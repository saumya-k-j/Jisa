"""Deployment-phase tests (written BEFORE the implementation, per the
project's test-first convention): the /health endpoint and the file-backed
live engine source that bridges the C++ coinbase_daemon to the FastAPI
service.

PINNED CONTRACT
===============

python/api/app.py (extended, existing /healthz /status /alerts unchanged):

  GET /health -> machine health for container orchestration (Docker
    HEALTHCHECK hits this). 200 whenever engine stats are readable, with:
      {"uptime_seconds": float,
       "total_messages_processed": int,
       "last_message_timestamp": str | None}   # ISO8601 UTC, None if no
                                                # message seen yet
    503 (same JSON shape, zeros/None) when the stats source is unavailable
    (e.g. the daemon has not written stats.json yet). A stale-but-readable
    stats file is still 200: a feed drop is the daemon's job to survive, not
    a reason to restart the container; last_message_timestamp is exposed so
    external monitoring can alert on staleness.

  Endpoints call runner.refresh() (if it exists) at request time so a live
  source re-reads its files; EngineRunner has no refresh and static fields.

python/api/live.py:

  class LiveEngineSource:
      def __init__(self, data_dir: str, db_path: str): ...
      # Duck-types the EngineRunner surface consumed by app.py:
      #   start_time, messages_processed, msgs_per_sec, streams, db_path,
      #   last_message_unix (float | None), healthy (bool)
      def refresh(self) -> None:
          # Re-reads <data_dir>/stats.json (written atomically by
          # coinbase_daemon) and syncs NEW lines of <data_dir>/alerts.jsonl
          # into the alerts table at db_path (same schema as EngineRunner),
          # tracking the consumed byte offset in a sync_state table so
          # restarts do not duplicate rows.

stats.json shape (produced by src/feed/coinbase_daemon_main.cpp):
  {"start_time_unix": float_s, "now_unix": float_s,
   "messages_received": int, "messages_processed": int, "ticks_pushed": int,
   "parse_failures": int, "gaps": int, "reconnects": int, "alerts": int,
   "last_message_unix_ms": int (0 = none), "last_message_ts_ns": int,
   "streams": [int, ...]}

alerts.jsonl: one object per line:
  {"stream_id": int, "ts_ns": int, "layer": str, "detail": str,
   "created_at": str}
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys

import pytest

fastapi = pytest.importorskip("fastapi")

_API_DIR = os.path.dirname(__file__)
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from fastapi.testclient import TestClient  # noqa: E402

import app as app_module  # noqa: E402

try:
    import live as live_module
except ModuleNotFoundError:
    live_module = None


STATS = {
    "start_time_unix": 1000.0,
    "now_unix": 1060.0,
    "messages_received": 120,
    "messages_processed": 100,
    "ticks_pushed": 100,
    "parse_failures": 0,
    "gaps": 1,
    "reconnects": 2,
    "alerts": 3,
    "last_message_unix_ms": 1059_500,
    "last_message_ts_ns": 1059_400_000_000,
    "streams": [10, 11],
}

ALERT_LINES = [
    {"stream_id": 10, "ts_ns": 1, "layer": "rules", "detail": "out_of_bounds",
     "created_at": "2026-07-08T12:00:00.000Z"},
    {"stream_id": 11, "ts_ns": 2, "layer": "feed", "detail": "gap seq 5->9",
     "created_at": "2026-07-08T12:00:01.000Z"},
]


def _write_stats(data_dir, stats=STATS):
    with open(os.path.join(data_dir, "stats.json"), "w") as f:
        json.dump(stats, f)


def _append_alerts(data_dir, alerts):
    with open(os.path.join(data_dir, "alerts.jsonl"), "a") as f:
        for a in alerts:
            f.write(json.dumps(a) + "\n")


@pytest.fixture
def live_source(tmp_path):
    if live_module is None:
        pytest.fail("python/api/live.py not implemented yet")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir)
    return live_module.LiveEngineSource(
        data_dir=data_dir, db_path=str(tmp_path / "alerts.db")), data_dir


class TestLiveEngineSource:
    def test_reads_stats_json(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        src.refresh()
        assert src.healthy is True
        assert src.messages_processed == 100
        assert src.start_time == 1000.0
        assert sorted(src.streams) == [10, 11]
        assert src.last_message_unix == pytest.approx(1059.5)

    def test_unhealthy_when_stats_missing(self, live_source):
        src, _ = live_source
        src.refresh()
        assert src.healthy is False
        assert src.messages_processed == 0
        assert src.last_message_unix is None

    def test_zero_last_message_means_none(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir, {**STATS, "last_message_unix_ms": 0})
        src.refresh()
        assert src.healthy is True
        assert src.last_message_unix is None

    def test_syncs_alert_lines_into_sqlite_without_duplicates(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        _append_alerts(data_dir, ALERT_LINES)
        src.refresh()
        src.refresh()  # second refresh must NOT duplicate rows

        conn = sqlite3.connect(src.db_path)
        rows = conn.execute(
            "SELECT stream_id, ts_ns, layer, detail, created_at FROM alerts "
            "ORDER BY id").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0] == (10, 1, "rules", "out_of_bounds",
                           "2026-07-08T12:00:00.000Z")
        assert rows[1][2] == "feed"

    def test_sync_resumes_after_restart_without_duplicates(self, live_source, tmp_path):
        src, data_dir = live_source
        _write_stats(data_dir)
        _append_alerts(data_dir, ALERT_LINES[:1])
        src.refresh()

        # New process (fresh source object, same db): must pick up ONLY the
        # line appended after the first sync.
        if live_module is None:
            pytest.fail("live module missing")
        src2 = live_module.LiveEngineSource(data_dir=data_dir, db_path=src.db_path)
        _append_alerts(data_dir, ALERT_LINES[1:])
        src2.refresh()

        conn = sqlite3.connect(src.db_path)
        n = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        conn.close()
        assert n == 2

    def test_malformed_alert_line_is_skipped_not_fatal(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        with open(os.path.join(data_dir, "alerts.jsonl"), "a") as f:
            f.write("this is not json\n")
        _append_alerts(data_dir, ALERT_LINES[:1])
        src.refresh()
        conn = sqlite3.connect(src.db_path)
        n = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        conn.close()
        assert n == 1


class TestHealthEndpoint:
    def test_health_200_with_live_source(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        client = TestClient(app_module.create_app(src))
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_messages_processed"] == 100
        assert body["uptime_seconds"] > 0
        # ISO8601 UTC string derived from last_message_unix_ms
        assert isinstance(body["last_message_timestamp"], str)
        assert body["last_message_timestamp"].startswith("1970-01-01T00:17:39")

    def test_health_503_when_daemon_has_not_written_stats(self, live_source):
        src, _ = live_source
        client = TestClient(app_module.create_app(src))
        resp = client.get("/health")
        assert resp.status_code == 503
        assert resp.json()["last_message_timestamp"] is None

    def test_alerts_endpoint_serves_synced_live_alerts(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        _append_alerts(data_dir, ALERT_LINES)
        client = TestClient(app_module.create_app(src))
        alerts = client.get("/alerts").json()
        assert len(alerts) == 2
        assert alerts[0]["layer"] == "feed"  # newest first

    def test_status_still_works_with_live_source(self, live_source):
        src, data_dir = live_source
        _write_stats(data_dir)
        client = TestClient(app_module.create_app(src))
        body = client.get("/status").json()
        assert body["messages_processed"] == 100
        assert 10 in body["streams"]
        assert "latency" not in body


class TestHealthWithEngineRunner:
    """The replay-based EngineRunner (existing tests' fixture) must also serve
    /health: always 200, last_message_timestamp from its run."""

    def test_health_200_with_engine_runner(self, tmp_path):
        telemetry_engine = pytest.importorskip("telemetry_engine")
        import engine as engine_module

        trec = str(tmp_path / "s.trec")
        rec = telemetry_engine.StreamRecorder(trec)
        for i in range(10):
            rec.write(telemetry_engine.Message(
                stream_id=1, ts_ns=(i + 1) * 10**9, value=50.0, seq=i + 1))
        rec.close()

        runner = engine_module.EngineRunner(
            replay_path=trec,
            rule_configs=[telemetry_engine.RuleConfig(
                stream_id=1, min=45.0, max=55.0, max_rate_of_change=0.5)],
            ewma_alpha=0.05, cusum_alpha=0.1, cusum_k=0.5, cusum_h=3.0,
            conformal_window=20, conformal_alpha=0.05,
            db_path=str(tmp_path / "a.db"))
        runner.run()

        client = TestClient(app_module.create_app(runner))
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_messages_processed"] == 10
        assert body["uptime_seconds"] >= 0.0
        assert body["last_message_timestamp"] is not None
