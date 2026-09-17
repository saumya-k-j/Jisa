"""Tests for the DogStatsD exporter (SPEC 3.14).

Metrics go over a real UDP socket bound to an ephemeral port, so the wire
format is asserted as the Agent would receive it rather than through a mock.
"""
from __future__ import annotations

import json
import os
import socket

import pytest

from python.obs.dogstatsd import Exporter, StatsdClient


@pytest.fixture
def agent():
    """A UDP socket standing in for the local Datadog Agent."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.settimeout(0.5)
    yield sock
    sock.close()


def drain(sock) -> list[str]:
    out = []
    while True:
        try:
            out.append(sock.recv(65535).decode())
        except socket.timeout:
            return out


def client_for(agent, tags=None) -> StatsdClient:
    host, port = agent.getsockname()
    return StatsdClient(host=host, port=port, tags=tags)


def write_stats(tmp_path, **overrides) -> str:
    stats = {
        "start_time_unix": 1000.0,
        "now_unix": 1100.0,
        "messages_received": 0,
        "messages_processed": 0,
        "ticks_pushed": 0,
        "parse_failures": 0,
        "gaps": 0,
        "reconnects": 0,
        "alerts": 0,
        "last_message_unix_ms": 1100_000,
        "last_message_ts_ns": 0,
        "streams": [1, 2],
    }
    stats.update(overrides)
    path = tmp_path / "stats.json"
    path.write_text(json.dumps(stats))
    return str(tmp_path)


class TestWireFormat:
    def test_gauge_uses_dogstatsd_syntax(self, agent):
        client_for(agent).gauge("jisa.up", 1)
        assert drain(agent) == ["jisa.up:1|g"]

    def test_count_uses_dogstatsd_syntax(self, agent):
        client_for(agent).count("jisa.messages.received", 42)
        assert drain(agent) == ["jisa.messages.received:42|c"]

    def test_constructor_tags_are_appended(self, agent):
        client_for(agent, tags=["service:jisa", "env:vps"]).gauge("jisa.up", 1)
        assert drain(agent) == ["jisa.up:1|g|#service:jisa,env:vps"]

    def test_per_call_tags_merge_with_constructor_tags(self, agent):
        client_for(agent, tags=["service:jisa"]).count("jisa.alerts", 3, tags=["layer:cusum"])
        assert drain(agent) == ["jisa.alerts:3|c|#service:jisa,layer:cusum"]

    def test_unreachable_agent_does_not_raise(self):
        # Nothing is listening on this port; the engine must not care.
        StatsdClient(host="127.0.0.1", port=1).gauge("jisa.up", 1)


class TestCounterDeltas:
    def test_first_tick_establishes_a_baseline(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, messages_received=500)
        emitted = Exporter(data_dir, client_for(agent), now=lambda: 1100.0).tick()
        assert emitted["jisa.messages.received"] == 0

    def test_second_tick_reports_the_delta(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, messages_received=500)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        write_stats(tmp_path, messages_received=650)
        assert exporter.tick()["jisa.messages.received"] == 150

    def test_counter_reset_reports_the_new_value_not_a_negative(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, gaps=90)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        write_stats(tmp_path, gaps=4)          # daemon restarted
        assert exporter.tick()["jisa.feed.gaps"] == 4


class TestLiveness:
    def test_fresh_stats_report_up(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        assert Exporter(data_dir, client_for(agent), now=lambda: 1100.0).tick()["jisa.up"] == 1

    def test_stale_stats_report_down(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, now_unix=1100.0)
        # Wall clock far ahead of the daemon's last write.
        assert Exporter(data_dir, client_for(agent), now=lambda: 1300.0).tick()["jisa.up"] == 0

    def test_missing_stats_file_reports_down_without_raising(self, tmp_path, agent):
        emitted = Exporter(str(tmp_path), client_for(agent)).tick()
        assert emitted == {"jisa.up": 0}
        assert drain(agent) == ["jisa.up:0|g"]

    def test_last_message_age_is_measured_against_wall_clock(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, last_message_unix_ms=1_100_000)
        emitted = Exporter(data_dir, client_for(agent), now=lambda: 1130.0).tick()
        assert emitted["jisa.feed.last_message_age_seconds"] == pytest.approx(30.0)

    def test_throughput_uses_the_daemons_own_clock(self, tmp_path, agent):
        data_dir = write_stats(tmp_path, start_time_unix=1000.0, now_unix=1100.0,
                               messages_processed=5000)
        emitted = Exporter(data_dir, client_for(agent), now=lambda: 9999.0).tick()
        assert emitted["jisa.throughput.msgs_per_sec"] == pytest.approx(50.0)


class TestAlertTailing:
    def _append(self, tmp_path, *layers):
        with open(tmp_path / "alerts.jsonl", "a") as f:
            for layer in layers:
                f.write(json.dumps({"stream_id": 1, "ts_ns": 1, "layer": layer,
                                    "detail": "x", "created_at": "now"}) + "\n")

    def test_alerts_written_before_startup_are_not_counted(self, tmp_path, agent):
        self._append(tmp_path, "cusum", "cusum")
        data_dir = write_stats(tmp_path)
        assert Exporter(data_dir, client_for(agent), now=lambda: 1100.0).tick()["jisa.alerts"] == {}

    def test_new_alerts_are_counted_by_layer(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        self._append(tmp_path, "cusum", "ewma", "cusum", "conformal")
        assert exporter.tick()["jisa.alerts"] == {"conformal": 1, "cusum": 2, "ewma": 1}

    def test_each_alert_is_counted_once(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        self._append(tmp_path, "rules")
        exporter.tick()
        assert exporter.tick()["jisa.alerts"] == {}

    def test_partial_trailing_line_waits_for_the_next_tick(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        with open(tmp_path / "alerts.jsonl", "a") as f:
            f.write('{"layer":"cusum"}\n{"layer":"ew')   # torn write
        assert exporter.tick()["jisa.alerts"] == {"cusum": 1}
        with open(tmp_path / "alerts.jsonl", "a") as f:
            f.write('ma"}\n')
        assert exporter.tick()["jisa.alerts"] == {"ewma": 1}

    def test_malformed_line_is_counted_not_fatal(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        exporter = Exporter(data_dir, client_for(agent), now=lambda: 1100.0)
        exporter.tick()
        with open(tmp_path / "alerts.jsonl", "a") as f:
            f.write("not json at all\n")
        assert exporter.tick()["jisa.alerts"] == {"malformed": 1}

    def test_alert_counts_reach_the_agent_with_layer_tags(self, tmp_path, agent):
        data_dir = write_stats(tmp_path)
        exporter = Exporter(data_dir, client_for(agent, tags=["service:jisa"]),
                            now=lambda: 1100.0)
        exporter.tick()
        drain(agent)
        self._append(tmp_path, "cusum")
        exporter.tick()
        assert "jisa.alerts:1|c|#service:jisa,layer:cusum" in drain(agent)
