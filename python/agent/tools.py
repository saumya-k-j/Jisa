"""SPEC-3.12 deterministic data tools for the post-alert triage agent.

These are the NO-LLM data-access tools the triage agent calls to investigate a
confirmed alert. They are POST-ALERT only and never touch the hot path. Data
comes from the real ``telemetry_engine`` bindings (StreamReplayer, EwmaBaseline)
and the SQLite alert history (schema from python/api/engine.py). Pure stdlib +
bindings; no framework, no network.

See python/agent/test_tools.py for the pinned contract this builds to.
"""
from __future__ import annotations

import os
import sqlite3

import telemetry_engine as te


class DomainCardNotFoundError(Exception):
    """Raised when get_domain_card is asked for a domain with no card file.

    A specific, documented type (message names the domain and the path tried)
    so callers never confuse "no card exists" with a silent None or a raw
    FileNotFoundError escaping uncaught.
    """


class AlertContext:
    """Bundles everything the triage tools need for one alert investigation."""

    def __init__(self, replay_path: str, db_path: str, domain: str,
                 cards_dir: str, alert_ts_ns: int, ewma_alpha: float = 0.3):
        self.replay_path = replay_path
        self.db_path = db_path
        self.domain = domain
        self.cards_dir = cards_dir
        self.alert_ts_ns = alert_ts_ns
        self.ewma_alpha = ewma_alpha

    def get_window(self, stream_id: int, t0_ns: int, t1_ns: int) -> list[dict]:
        """Samples for ``stream_id`` with t0_ns <= ts_ns <= t1_ns (CLOSED
        interval, both ends inclusive), in ascending ts_ns (stream) order."""
        out: list[dict] = []
        replayer = te.StreamReplayer(self.replay_path)
        for msg in replayer:
            if msg.stream_id == stream_id and t0_ns <= msg.ts_ns <= t1_ns:
                out.append({"ts_ns": msg.ts_ns, "value": msg.value})
        return out

    def get_baseline(self, stream_id: int) -> dict:
        """EWMA {"mean", "variance"} as-of the alert instant: fed every message
        for ``stream_id`` with ts_ns <= self.alert_ts_ns (inclusive), so the
        memo reflects what was known at detection time, not future data."""
        baseline = te.EwmaBaseline(alpha=self.ewma_alpha)
        replayer = te.StreamReplayer(self.replay_path)
        for msg in replayer:
            if msg.stream_id == stream_id and msg.ts_ns <= self.alert_ts_ns:
                baseline.update(stream_id=stream_id, value=msg.value)
        return {"mean": baseline.mean(stream_id=stream_id),
                "variance": baseline.variance(stream_id=stream_id)}

    def get_concurrent_alerts(self, ts_ns: int, window_ns: int) -> list[dict]:
        """Alert rows with ts_ns in [ts_ns - window_ns, ts_ns + window_ns]
        (CLOSED, symmetric), ascending by ts_ns. All six schema columns."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT id, stream_id, ts_ns, layer, detail, created_at "
                "FROM alerts WHERE ts_ns BETWEEN ? AND ? ORDER BY ts_ns ASC",
                (ts_ns - window_ns, ts_ns + window_ns))
            return [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

    def get_domain_card(self, domain: str | None = None) -> str:
        """Raw markdown of <cards_dir>/<domain or self.domain>.md."""
        name = domain if domain is not None else self.domain
        path = os.path.join(self.cards_dir, f"{name}.md")
        try:
            with open(path, "r") as f:
                return f.read()
        except FileNotFoundError as exc:
            raise DomainCardNotFoundError(
                f"no domain card for '{name}' (tried {path})") from exc
