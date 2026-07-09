"""File-backed live engine source for 24/7 deployment.

Bridges the C++ ``coinbase_daemon`` (which owns the WebSocket, the ring
buffer, and the four detection layers) to the FastAPI service in app.py.
The daemon writes two files under its --data-dir:

  stats.json    atomic (tmp+rename) snapshot, rewritten every second
  alerts.jsonl  append-only, one JSON alert per line

``LiveEngineSource`` duck-types the EngineRunner surface that app.py reads
(start_time, messages_processed, msgs_per_sec, streams, db_path,
last_message_unix, healthy) and adds ``refresh()``, which app.py calls at
request time: it re-reads stats.json and syncs any NEW alerts.jsonl lines
into the SQLite alerts table (same schema as EngineRunner), tracking the
consumed byte offset in a ``sync_state`` row so process restarts never
duplicate rows. SQLite lives on the deployment volume; the JSONL/stats files
are daemon-owned working files on the same volume.
"""
from __future__ import annotations

import json
import os
import sqlite3

_CREATE_ALERTS = """
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id INTEGER NOT NULL,
  ts_ns INTEGER NOT NULL,
  layer TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""

_CREATE_SYNC = """
CREATE TABLE IF NOT EXISTS sync_state (
  source TEXT PRIMARY KEY,
  byte_offset INTEGER NOT NULL
);
"""


class LiveEngineSource:
    def __init__(self, data_dir: str, db_path: str):
        self.data_dir = data_dir
        self.db_path = db_path
        self._stats_path = os.path.join(data_dir, "stats.json")
        self._alerts_path = os.path.join(data_dir, "alerts.jsonl")

        # EngineRunner-compatible surface (refreshed from stats.json).
        self.healthy = False
        self.start_time = 0.0
        self.messages_processed = 0
        self.msgs_per_sec = 0.0
        self.streams: list[int] = []
        self.last_message_unix: float | None = None

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(_CREATE_ALERTS)
            conn.execute(_CREATE_SYNC)
            conn.commit()
        finally:
            conn.close()

    def refresh(self) -> None:
        self._read_stats()
        self._sync_alerts()

    def _read_stats(self) -> None:
        try:
            with open(self._stats_path) as f:
                stats = json.load(f)
        except (OSError, ValueError):
            # Daemon not started yet / mid-crash: report unhealthy, keep
            # zeros. The atomic tmp+rename on the writer side means a partial
            # read should not happen in steady state.
            self.healthy = False
            self.start_time = 0.0
            self.messages_processed = 0
            self.msgs_per_sec = 0.0
            self.streams = []
            self.last_message_unix = None
            return

        self.healthy = True
        self.start_time = float(stats.get("start_time_unix", 0.0))
        self.messages_processed = int(stats.get("messages_processed", 0))
        self.streams = [int(s) for s in stats.get("streams", [])]
        last_ms = int(stats.get("last_message_unix_ms", 0))
        self.last_message_unix = (last_ms / 1000.0) if last_ms > 0 else None
        # The daemon reports counters, not a rate: derive the lifetime
        # average from its own clock snapshot (not this process's clock).
        elapsed = float(stats.get("now_unix", 0.0)) - self.start_time
        self.msgs_per_sec = (self.messages_processed / elapsed
                             if elapsed > 0 else 0.0)

    def _sync_alerts(self) -> None:
        try:
            size = os.path.getsize(self._alerts_path)
        except OSError:
            return  # no alerts yet

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT byte_offset FROM sync_state WHERE source = 'alerts_jsonl'"
            ).fetchone()
            offset = row[0] if row else 0
            if size <= offset:
                return

            with open(self._alerts_path, "rb") as f:
                f.seek(offset)
                chunk = f.read()
            # Only consume complete lines; a partially-written trailing line
            # is picked up by the next refresh.
            last_nl = chunk.rfind(b"\n")
            if last_nl < 0:
                return
            consumed = chunk[: last_nl + 1]

            for line in consumed.splitlines():
                try:
                    a = json.loads(line)
                    params = (int(a["stream_id"]), int(a["ts_ns"]),
                              str(a["layer"]), str(a["detail"]),
                              str(a["created_at"]))
                except (ValueError, KeyError, TypeError):
                    continue  # malformed line: skip, never fatal
                conn.execute(
                    "INSERT INTO alerts (stream_id, ts_ns, layer, detail, "
                    "created_at) VALUES (?, ?, ?, ?, ?)", params)

            conn.execute(
                "INSERT INTO sync_state (source, byte_offset) "
                "VALUES ('alerts_jsonl', ?) "
                "ON CONFLICT(source) DO UPDATE SET byte_offset = excluded.byte_offset",
                (offset + len(consumed),))
            conn.commit()
        finally:
            conn.close()
