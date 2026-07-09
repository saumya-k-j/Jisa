"""SPEC-3.11 FastAPI status page.

Serves the in-process engine's metrics + alert feed (see engine.py for the
documented deviation from the literal "reads over a local socket" wording).

Endpoints:
  GET /healthz        -> {"ok": true} (liveness of the API process only)
  GET /health         -> machine health for container orchestration (the
                         Docker HEALTHCHECK target): 200 with uptime_seconds,
                         total_messages_processed, last_message_timestamp
                         (ISO8601 UTC or null) while engine stats are
                         readable; 503 when they are not (daemon not up yet).
                         A stale-but-readable feed is still 200 — surviving
                         feed drops is the daemon's job (auto-reconnect), not
                         a reason to restart the container; monitors can
                         alert on last_message_timestamp instead.
  GET /status         -> uptime_s, messages_processed, msgs_per_sec, streams.
                         NO "latency" key: the C++ hot path does not export a
                         latency histogram through the bindings yet, and this
                         layer does NOT fabricate one (SPEC 1 / documented
                         omission).
  GET /alerts?limit=N -> newest-first alert rows from the runner's SQLite DB
                         (default limit 100).
"""
from __future__ import annotations

import datetime
import sqlite3
import time

import fastapi


def create_app(runner) -> fastapi.FastAPI:
    app = fastapi.FastAPI(title="Telemetry Anomaly Engine")

    def refresh():
        # Live sources (python/api/live.py) re-read their files at request
        # time; the replay-based EngineRunner has no refresh (static fields).
        hook = getattr(runner, "refresh", None)
        if hook is not None:
            hook()

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/health")
    def health(response: fastapi.Response):
        refresh()
        healthy = getattr(runner, "healthy", True)
        if not healthy:
            response.status_code = 503
        last_unix = getattr(runner, "last_message_unix", None)
        last_iso = (
            datetime.datetime.fromtimestamp(
                last_unix, datetime.timezone.utc
            ).isoformat().replace("+00:00", "Z")
            if last_unix is not None else None
        )
        return {
            "uptime_seconds": (max(0.0, time.time() - runner.start_time)
                               if healthy else 0.0),
            "total_messages_processed": runner.messages_processed,
            "last_message_timestamp": last_iso,
        }

    @app.get("/status")
    def status():
        refresh()
        return {
            "uptime_s": max(0.0, time.time() - runner.start_time),
            "messages_processed": runner.messages_processed,
            "msgs_per_sec": runner.msgs_per_sec,
            "streams": list(runner.streams),
        }

    @app.get("/alerts")
    def alerts(limit: int = 100):
        refresh()
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
