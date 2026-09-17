"""DogStatsD export of the live daemon's counters (SPEC 3.14).

The C++ daemon already publishes everything worth graphing: it rewrites
``stats.json`` atomically every second and appends one JSON object per alert
to ``alerts.jsonl``. This exporter reads those two files and forwards them to
a local Datadog Agent over UDP, so nothing here touches the ingestion hot
path, the detection layers, or the daemon itself (CLAUDE.md forbids
allocation and locks on the hot path; a UDP send there would violate both).

Counters in stats.json are monotonic. DogStatsD counts are deltas, so each
counter is diffed against the previous sample. A daemon restart resets the
counters to zero; a negative delta is therefore treated as a restart and the
new absolute value is sent rather than a negative one.

Failure to reach the Agent is never fatal: metrics are best-effort telemetry
about the engine, and losing them must not take the engine down.
"""
from __future__ import annotations

import json
import os
import socket
import time

DEFAULT_HOST = os.environ.get("DD_AGENT_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("DD_DOGSTATSD_PORT", "8125"))

# A stats.json older than this means the daemon has stopped rewriting it.
STALE_AFTER_S = 15.0


class StatsdClient:
    """Minimal DogStatsD client over UDP. Standard library only."""

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
                 tags: list[str] | None = None) -> None:
        self.address = (host, port)
        self.tags = list(tags or [])
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)

    def gauge(self, name: str, value: float, tags: list[str] | None = None) -> None:
        self._send(f"{name}:{value:g}|g", tags)

    def count(self, name: str, delta: int, tags: list[str] | None = None) -> None:
        self._send(f"{name}:{int(delta)}|c", tags)

    def _send(self, payload: str, tags: list[str] | None) -> None:
        merged = self.tags + list(tags or [])
        if merged:
            payload = f"{payload}|#{','.join(merged)}"
        try:
            self._sock.sendto(payload.encode("utf-8"), self.address)
        except OSError:
            # Agent down, socket buffer full, DNS gone: drop the sample. The
            # engine keeps running; a gap in the graph is the correct symptom.
            pass

    def close(self) -> None:
        self._sock.close()


# stats.json key -> emitted metric name, for the monotonic counters.
_COUNTERS = {
    "messages_received": "jisa.messages.received",
    "messages_processed": "jisa.messages.processed",
    "ticks_pushed": "jisa.ticks.pushed",
    "parse_failures": "jisa.feed.parse_failures",
    "gaps": "jisa.feed.gaps",
    "reconnects": "jisa.feed.reconnects",
}


class Exporter:
    """Samples the daemon's files and emits one batch of metrics per tick."""

    def __init__(self, data_dir: str, client: StatsdClient,
                 now=time.time) -> None:
        self.stats_path = os.path.join(data_dir, "stats.json")
        self.alerts_path = os.path.join(data_dir, "alerts.jsonl")
        self.client = client
        self._now = now
        self._previous: dict[str, int] = {}
        # Start at end-of-file: alerts written before the exporter started are
        # history, not a burst to report now.
        self._alerts_offset = self._file_size(self.alerts_path)

    @staticmethod
    def _file_size(path: str) -> int:
        try:
            return os.path.getsize(path)
        except OSError:
            return 0

    def _delta(self, key: str, value: int) -> int:
        previous = self._previous.get(key)
        self._previous[key] = value
        if previous is None:
            return 0          # first sample establishes the baseline
        if value < previous:
            return value      # counter reset: daemon restarted
        return value - previous

    def tick(self) -> dict:
        """One sampling pass. Returns what was emitted, for tests and logs."""
        emitted: dict = {}

        try:
            with open(self.stats_path) as f:
                stats = json.load(f)
        except (OSError, ValueError):
            # Daemon not up, or mid-rename. Report down and stop here.
            self.client.gauge("jisa.up", 0)
            return {"jisa.up": 0}

        now = self._now()
        age = now - float(stats.get("now_unix", 0.0))
        up = 1 if age <= STALE_AFTER_S else 0
        self.client.gauge("jisa.up", up)
        emitted["jisa.up"] = up

        uptime = max(0.0, float(stats.get("now_unix", 0.0))
                     - float(stats.get("start_time_unix", 0.0)))
        self.client.gauge("jisa.uptime_seconds", uptime)
        emitted["jisa.uptime_seconds"] = uptime

        for key, metric in _COUNTERS.items():
            delta = self._delta(key, int(stats.get(key, 0)))
            self.client.count(metric, delta)
            emitted[metric] = delta

        # Lifetime average from the daemon's own clock, matching live.py.
        elapsed = uptime
        processed = int(stats.get("messages_processed", 0))
        rate = processed / elapsed if elapsed > 0 else 0.0
        self.client.gauge("jisa.throughput.msgs_per_sec", rate)
        emitted["jisa.throughput.msgs_per_sec"] = rate

        # The liveness signal that matters: a feed can be "connected" and
        # silent, and only the age of the last message shows it.
        last_ms = int(stats.get("last_message_unix_ms", 0))
        if last_ms > 0:
            last_age = max(0.0, now - last_ms / 1000.0)
            self.client.gauge("jisa.feed.last_message_age_seconds", last_age)
            emitted["jisa.feed.last_message_age_seconds"] = last_age

        streams = stats.get("streams", []) or []
        self.client.gauge("jisa.streams.active", len(streams))
        emitted["jisa.streams.active"] = len(streams)

        by_layer = self._drain_alerts()
        for layer, n in sorted(by_layer.items()):
            self.client.count("jisa.alerts", n, tags=[f"layer:{layer}"])
        emitted["jisa.alerts"] = by_layer
        return emitted

    def _drain_alerts(self) -> dict[str, int]:
        """Count alerts appended since the last tick, grouped by layer."""
        size = self._file_size(self.alerts_path)
        if size <= self._alerts_offset:
            if size < self._alerts_offset:
                self._alerts_offset = size   # file rotated or truncated
            return {}

        try:
            with open(self.alerts_path, "rb") as f:
                f.seek(self._alerts_offset)
                chunk = f.read(size - self._alerts_offset)
        except OSError:
            return {}

        # Consume whole lines only; a half-written trailing line is picked up
        # on the next tick (same rule live.py uses).
        last_newline = chunk.rfind(b"\n")
        if last_newline < 0:
            return {}
        self._alerts_offset += last_newline + 1

        counts: dict[str, int] = {}
        for line in chunk[: last_newline + 1].splitlines():
            try:
                layer = str(json.loads(line)["layer"])
            except (ValueError, KeyError, TypeError):
                counts["malformed"] = counts.get("malformed", 0) + 1
                continue
            counts[layer] = counts.get(layer, 0) + 1
        return counts


def main() -> None:
    data_dir = os.environ.get("JISA_DATA_DIR", "/data")
    interval = float(os.environ.get("JISA_DD_INTERVAL_S", "10"))
    tags = [t for t in os.environ.get("JISA_DD_TAGS", "service:jisa").split(",") if t]

    client = StatsdClient(tags=tags)
    exporter = Exporter(data_dir, client)
    while True:
        exporter.tick()
        time.sleep(interval)


if __name__ == "__main__":
    main()
