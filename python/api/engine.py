"""SPEC-3.11 in-process engine host.

Deviation from SPEC 3.11's literal "reads engine metrics + alerts over a local
socket": this project has NO separate long-running C++ daemon. The engine
(replay + rules + baseline + cusum + conformal) runs IN-PROCESS via the
pybind11 bindings (SPEC 3.10, module ``telemetry_engine``). ``EngineRunner``
replays a recorded ``.trec`` stream through the REAL C++ pipeline (no Python
reimplementation of detector math), accumulates throughput metrics, and
persists every alert to a SQLite file that the FastAPI app (app.py) serves.
"""
from __future__ import annotations

import datetime as _dt
import sqlite3
import time
from typing import List

import telemetry_engine as te


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id INTEGER NOT NULL,
  ts_ns INTEGER NOT NULL,
  layer TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""

# Human-readable detail strings for rule violations (RuleResult enum -> text).
_RULE_DETAIL = {
    te.RuleResult.kOutOfBounds: "out_of_bounds",
    te.RuleResult.kRateViolation: "rate_violation",
}


class EngineRunner:
    def __init__(self, replay_path: str,
                 rule_configs: List["te.RuleConfig"],
                 ewma_alpha: float, cusum_alpha: float, cusum_k: float,
                 cusum_h: float, conformal_window: int,
                 conformal_alpha: float, db_path: str):
        self.replay_path = replay_path
        self.rule_configs = list(rule_configs)
        self.ewma_alpha = ewma_alpha
        self.cusum_alpha = cusum_alpha
        self.cusum_k = cusum_k
        self.cusum_h = cusum_h
        self.conformal_window = conformal_window
        self.conformal_alpha = conformal_alpha
        self.db_path = db_path

        self.start_time = time.time()
        self.messages_processed = 0
        self.alerts: List[dict] = []
        self.wall_time_s = 0.0
        self.msgs_per_sec = 0.0
        self.streams: List[int] = [cfg.stream_id for cfg in self.rule_configs]

    def run(self) -> None:
        # Build the REAL detector pipeline from the bindings.
        checker = te.RuleChecker()
        for cfg in self.rule_configs:
            checker.add_rule(cfg)
        baseline = te.EwmaBaseline(alpha=self.ewma_alpha)
        cusum = te.CusumDetector(alpha=self.cusum_alpha, k=self.cusum_k,
                                 h=self.cusum_h)
        conformal = te.ConformalThreshold(window_capacity=self.conformal_window)

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(_CREATE_TABLE)
            conn.commit()

            seen_streams = set(self.streams)

            t0 = time.perf_counter()
            replayer = te.StreamReplayer(self.replay_path)
            for msg in replayer:
                self.messages_processed += 1
                sid = msg.stream_id
                if sid not in seen_streams:
                    seen_streams.add(sid)

                # Layer 1: hard bounds / rate.
                rr = checker.check(stream_id=sid, value=msg.value,
                                   ts_ns=msg.ts_ns)
                if rr != te.RuleResult.kOk:
                    self._alert(conn, sid, msg.ts_ns, "rules",
                                _RULE_DETAIL.get(rr, "violation"))

                # Layer 2: baseline z-score (out-of-sample: score BEFORE update).
                z = baseline.zscore(stream_id=sid, value=msg.value)
                baseline.update(stream_id=sid, value=msg.value)
                score = abs(z)

                # Layer 3: CUSUM changepoint.
                if cusum.update_and_check(stream_id=sid, value=msg.value):
                    self._alert(conn, sid, msg.ts_ns, "cusum",
                                "changepoint")

                # Layer 4: conformal threshold (out-of-sample decision, then
                # fold the score into the window).
                if conformal.is_anomalous(stream_id=sid, score=score,
                                          alpha=self.conformal_alpha):
                    thr = conformal.threshold(stream_id=sid,
                                              alpha=self.conformal_alpha)
                    self._alert(conn, sid, msg.ts_ns, "conformal",
                                f"score={score:.4f}>thr={thr:.4f}")
                conformal.update(stream_id=sid, score=score)

            conn.commit()
            self.wall_time_s = time.perf_counter() - t0
        finally:
            conn.close()

        self.streams = sorted(seen_streams)
        self.msgs_per_sec = (self.messages_processed / self.wall_time_s
                             if self.wall_time_s > 0.0 else 0.0)

    def _alert(self, conn, stream_id: int, ts_ns: int, layer: str,
               detail: str) -> None:
        created_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO alerts (stream_id, ts_ns, layer, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (int(stream_id), int(ts_ns), layer, detail, created_at))
        self.alerts.append({"stream_id": int(stream_id), "ts_ns": int(ts_ns),
                            "layer": layer, "detail": detail})
