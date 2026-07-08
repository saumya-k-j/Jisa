"""SPEC-3.11 FastAPI status page.

Serves the in-process engine's metrics + alert feed (see engine.py for the
documented deviation from the literal "reads over a local socket" wording).

Endpoints:
  GET /healthz        -> {"ok": true}
  GET /status         -> uptime_s, messages_processed, msgs_per_sec, streams.
                         NO "latency" key: the C++ hot path does not export a
                         latency histogram through the bindings yet, and this
                         layer does NOT fabricate one (SPEC 1 / documented
                         omission).
  GET /alerts?limit=N -> newest-first alert rows from the runner's SQLite DB
                         (default limit 100).
"""
from __future__ import annotations

import sqlite3
import time

import fastapi


def create_app(runner) -> fastapi.FastAPI:
    app = fastapi.FastAPI(title="Telemetry Anomaly Engine")

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/status")
    def status():
        return {
            "uptime_s": max(0.0, time.time() - runner.start_time),
            "messages_processed": runner.messages_processed,
            "msgs_per_sec": runner.msgs_per_sec,
            "streams": list(runner.streams),
        }

    @app.get("/alerts")
    def alerts(limit: int = 100):
        conn = sqlite3.connect(runner.db_path)
        try:
            rows = conn.execute(
                "SELECT id, stream_id, ts_ns, layer, detail, created_at "
                "FROM alerts ORDER BY id DESC LIMIT ?",
                (limit,)).fetchall()
        finally:
            conn.close()
        return [
            {"id": r[0], "stream_id": r[1], "ts_ns": r[2], "layer": r[3],
             "detail": r[4], "created_at": r[5]}
            for r in rows
        ]

    return app
