"""RED-stage tests for python/api (SPEC 3.11: "Reads engine metrics + alerts
over a local socket. Serves a status page: uptime, messages processed,
latency histograms, live alert feed. SQLite or Postgres for alert history.").

PINNED CONTRACT + DOCUMENTED DEVIATIONS FROM SPEC 3.11's LITERAL WORDING
(the implementer must build to this; deviations are deliberate, honest
simplifications for a portfolio system with no separate long-running C++
daemon -- NOT silently faked capability):

  * SPEC 3.11 literally says the API "reads engine metrics + alerts over a
    local socket," implying a separate long-running engine process the API
    connects to. THIS PROJECT HAS NO SEPARATE C++ DAEMON. Instead, the
    engine (replay + rules + baseline + cusum + conformal) is hosted
    IN-PROCESS inside the Python API process via the pybind11 bindings
    (SPEC 3.10). The FastAPI app itself is what serves on a local socket
    (uvicorn's TCP/unix socket), not a proxy to a separate engine socket.
    This is a documented deviation (D-candidate) -- flag for DECISIONS.md.
  * Latency histograms (p50/p99/p99.9, SPEC 1) are OMITTED from the /status
    response at the API layer for now: the C++ hot path does not yet
    export a latency histogram through the bindings. Do NOT fake this data.
    `GET /status` therefore has no `latency` key until that export exists;
    tests below assert its ABSENCE, not a placeholder value.
  * "Live alert feed": no websocket/streaming endpoint is pinned yet; only
    the polling `GET /alerts?limit=N` is pinned for this phase.

  Pinned module surface:

    python/api/engine.py
      class EngineRunner:
          def __init__(self, replay_path: str,
                       rule_configs: list["telemetry_engine.RuleConfig"],
                       ewma_alpha: float, cusum_alpha: float, cusum_k: float,
                       cusum_h: float, conformal_window: int,
                       conformal_alpha: float, db_path: str): ...
          def run(self) -> None:
              # Replays replay_path fully through rules -> baseline -> cusum
              # -> conformal (REAL pipeline via telemetry_engine bindings,
              # not mocks), accumulating:
              #   self.messages_processed: int
              #   self.alerts: list[dict] each
              #       {"stream_id": int, "ts_ns": int, "layer": str,
              #        "detail": str}
              #   self.wall_time_s: float
              #   self.msgs_per_sec: float
              # and PERSISTING every alert as a row in the SQLite file at
              # db_path (schema below) as it runs.

    SQLite schema (table `alerts`):
      CREATE TABLE alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stream_id INTEGER NOT NULL,
        ts_ns INTEGER NOT NULL,
        layer TEXT NOT NULL,
        detail TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

    python/api/app.py
      def create_app(runner_or_state) -> fastapi.FastAPI:
          # Accepts either an already-.run() EngineRunner, or some other
          # state object exposing the same fields (messages_processed,
          # msgs_per_sec, db_path, start_time / uptime source). Pinned here
          # as accepting an EngineRunner instance directly for these tests.

      Endpoints:
        GET /healthz -> {"ok": true}
        GET /status  -> {"uptime_s": float, "messages_processed": int,
                          "msgs_per_sec": float, "streams": [int, ...]}
                         NO "latency" key (see deviation above).
        GET /alerts?limit=N -> list of alert dicts, NEWEST FIRST
          (ORDER BY id DESC / created_at DESC), each:
            {"id": int, "stream_id": int, "ts_ns": int, "layer": str,
             "detail": str, "created_at": str}
          `limit` defaults to some reasonable value (pinned as 100 here);
          the endpoint must respect an explicit smaller limit.

These tests only count once BOTH `telemetry_engine` (bindings) AND
`fastapi`/`httpx` are importable; otherwise they SKIP loudly. Run:
    python/.venv/bin/python -m pytest python/api/test_api.py -v
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

telemetry_engine = pytest.importorskip(
    "telemetry_engine",
    reason=(
        "telemetry_engine pybind11 extension is not built/importable yet "
        "(SPEC 3.10). The API's EngineRunner depends on it directly (real "
        "pipeline, not mocks) -- these API tests cannot run until it is "
        "built with -DBUILD_PYTHON_BINDINGS=ON."
    ),
)
fastapi = pytest.importorskip(
    "fastapi",
    reason=(
        "fastapi is not installed in python/.venv yet (SPEC 3.11). Run "
        "`python/.venv/bin/pip install -r python/requirements.txt` once "
        "python/requirements.txt is added by the implementer."
    ),
)

# Both dependencies present: locate python/api on sys.path for `import app`
# and `import engine` (module names pinned by the task).
_API_DIR = os.path.dirname(__file__)
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

try:
    from fastapi.testclient import TestClient
except Exception as exc:  # pragma: no cover - defensive, httpx missing etc.
    pytest.skip(f"fastapi.testclient unavailable: {exc}", allow_module_level=True)

try:
    import engine as engine_module  # python/api/engine.py -- does not exist yet
    import app as app_module  # python/api/app.py -- does not exist yet
except ModuleNotFoundError as exc:
    pytest.skip(
        f"python/api engine.py/app.py not implemented yet (SPEC 3.11): {exc}",
        allow_module_level=True,
    )


GRID_STREAM_ID = 1


def _write_stream_with_spike(path, n=50, stream_id=GRID_STREAM_ID):
    """Builds a small .trec fixture via the REAL bindings (StreamRecorder),
    with an injected spike so the pipeline is guaranteed to raise at least
    one alert (rules layer: out-of-bounds, since grid freq bounds are
    45..55 Hz and the spike pushes far outside that)."""
    rec = telemetry_engine.StreamRecorder(path)
    assert rec.is_open()
    for i in range(n):
        ts = 1_000_000_000 * (i + 1)
        value = 50.0  # nominal grid frequency, well within [45, 55]
        if i == 25:
            value = 1000.0  # spike: violates rules-layer bounds
        rec.write(telemetry_engine.Message(
            stream_id=stream_id, ts_ns=ts, value=value, seq=i + 1))
    rec.close()
    return path


@pytest.fixture
def rule_configs():
    return [telemetry_engine.RuleConfig(
        stream_id=GRID_STREAM_ID, min=45.0, max=55.0, max_rate_of_change=0.5)]


@pytest.fixture
def primed_runner(tmp_path, rule_configs):
    trec_path = str(tmp_path / "fixture.trec")
    _write_stream_with_spike(trec_path)
    db_path = str(tmp_path / "alerts.db")

    runner = engine_module.EngineRunner(
        replay_path=trec_path,
        rule_configs=rule_configs,
        ewma_alpha=0.05,
        cusum_alpha=0.1, cusum_k=0.5, cusum_h=3.0,
        conformal_window=20, conformal_alpha=0.05,
        db_path=db_path,
    )
    runner.run()
    return runner


class TestEngineRunner:
    def test_run_processes_all_messages(self, primed_runner):
        assert primed_runner.messages_processed == 50

    def test_run_raises_at_least_one_alert_for_the_spike(self, primed_runner):
        assert len(primed_runner.alerts) >= 1
        alert = primed_runner.alerts[0]
        assert alert["stream_id"] == GRID_STREAM_ID
        assert alert["layer"] in {"rules", "baseline", "cusum", "conformal"}
        assert "detail" in alert

    def test_run_reports_throughput(self, primed_runner):
        assert primed_runner.wall_time_s >= 0.0
        assert primed_runner.msgs_per_sec >= 0.0

    def test_run_persists_alerts_to_sqlite(self, primed_runner):
        conn = sqlite3.connect(primed_runner.db_path)
        rows = conn.execute(
            "SELECT id, stream_id, ts_ns, layer, detail, created_at "
            "FROM alerts").fetchall()
        conn.close()
        assert len(rows) == len(primed_runner.alerts)
        assert len(rows) >= 1
        first = rows[0]
        assert first[1] == GRID_STREAM_ID  # stream_id column


class TestApiEndpoints:
    def test_healthz(self, primed_runner):
        app = app_module.create_app(primed_runner)
        client = TestClient(app)
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    def test_status_reports_counts_and_no_fake_latency(self, primed_runner):
        app = app_module.create_app(primed_runner)
        client = TestClient(app)
        resp = client.get("/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["messages_processed"] == 50
        assert body["msgs_per_sec"] >= 0.0
        assert body["uptime_s"] >= 0.0
        assert GRID_STREAM_ID in body["streams"]
        # Documented deviation: no fabricated latency histogram at this
        # phase (C++ side does not export one yet).
        assert "latency" not in body

    def test_alerts_endpoint_returns_the_spike_alert(self, primed_runner):
        app = app_module.create_app(primed_runner)
        client = TestClient(app)
        resp = client.get("/alerts")
        assert resp.status_code == 200
        alerts = resp.json()
        assert len(alerts) >= 1
        assert alerts[0]["stream_id"] == GRID_STREAM_ID
        assert "layer" in alerts[0]
        assert "detail" in alerts[0]
        assert "created_at" in alerts[0]

    def test_alerts_limit_param_respected(self, tmp_path, rule_configs):
        # Prime a runner with MANY spikes so multiple alerts exist, then
        # confirm ?limit=N actually truncates and returns newest-first.
        trec_path = str(tmp_path / "many_spikes.trec")
        rec = telemetry_engine.StreamRecorder(trec_path)
        n = 60
        for i in range(n):
            ts = 1_000_000_000 * (i + 1)
            value = 1000.0 if i % 5 == 0 else 50.0  # frequent spikes
            rec.write(telemetry_engine.Message(
                stream_id=GRID_STREAM_ID, ts_ns=ts, value=value, seq=i + 1))
        rec.close()

        db_path = str(tmp_path / "alerts_many.db")
        runner = engine_module.EngineRunner(
            replay_path=trec_path,
            rule_configs=rule_configs,
            ewma_alpha=0.05,
            cusum_alpha=0.1, cusum_k=0.5, cusum_h=3.0,
            conformal_window=20, conformal_alpha=0.05,
            db_path=db_path,
        )
        runner.run()
        assert len(runner.alerts) >= 3  # sanity: multiple spikes -> multiple alerts

        app = app_module.create_app(runner)
        client = TestClient(app)

        resp_all = client.get("/alerts")
        resp_limited = client.get("/alerts", params={"limit": 2})
        assert resp_limited.status_code == 200
        limited = resp_limited.json()
        assert len(limited) == 2
        all_alerts = resp_all.json()
        # Newest-first: the first two of the unlimited (newest-first) list
        # equal the limited response.
        assert limited == all_alerts[:2]

    def test_alerts_survive_across_app_recreation(self, primed_runner):
        # SQLite persistence: a brand new app built from the SAME db_path
        # (simulating an API process restart) still sees the alerts written
        # during .run().
        app1 = app_module.create_app(primed_runner)
        client1 = TestClient(app1)
        before = client1.get("/alerts").json()
        assert len(before) >= 1

        app2 = app_module.create_app(primed_runner)
        client2 = TestClient(app2)
        after = client2.get("/alerts").json()
        assert after == before
