"""SPEC-3.12 memo evaluation: score triage memos against injected ground truth
(labels + LLM-as-judge).

Pure label-matching and scoring are stdlib-only; build_eval_cases drives the
REAL bindings (FaultInjector) and the REAL in-process pipeline
(python/api/engine.py::EngineRunner) so the evaluation exercises the actual
detector stack, not a Python reimplementation.

See python/agent/test_evaluate.py for the pinned contract this builds to.
"""
from __future__ import annotations

import json
import os
import sys

import telemetry_engine as te

# python/api/engine.py holds the real EngineRunner. It is a sibling dir, not on
# sys.path by default (the agent tests only add python/agent/). Following the
# python/api/test_api.py precedent, engine.py is imported as a top-level module
# with its own directory on sys.path.
_API_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api")
if _API_DIR not in sys.path:
    sys.path.insert(0, _API_DIR)

from engine import EngineRunner  # noqa: E402


# Hypothesis vocabulary -> injected fault taxonomy. Keys are exactly the
# hypothesis words used in docs/domain_cards/*.md "Memo = {...hypothesis...}"
# lines. Anything not a key here (incl. the "unknown"/"no_conclusion"
# sentinels, and "false_alarm" which has no hypothesis word) maps to None and
# can never be scored correct -- a deliberate, visible gap in the vocabulary.
HYPOTHESIS_TO_FAULT_TYPE = {
    "bad_print": "spike", "sensor_glitch": "spike",
    "telemetry_glitch": "spike",
    "market_move": "drift", "generation_loss": "drift",
    "real_maneuver": "drift",
    "stuck_sensor": "stuck_at_value", "stuck_upstream": "stuck_at_value",
    "feed_gap_artifact": "dropout", "receiver_outage": "dropout",
    "coverage_dropout": "dropout",
}

# telemetry_engine.FaultType enum -> normalized lowercase strings.
_FAULT_TYPE_TO_STR = {
    te.FaultType.kSpike: "spike",
    te.FaultType.kDrift: "drift",
    te.FaultType.kStuckAtValue: "stuck_at_value",
    te.FaultType.kDropout: "dropout",
}


def match_alert_to_label(alert: dict, labels: list[dict]) -> str:
    """Ground-truth type of the same-stream label whose CLOSED
    [t_start_ns, t_end_ns] contains alert["ts_ns"]; earliest t_start_ns wins a
    tie; "false_alarm" if none match."""
    ts = alert["ts_ns"]
    sid = alert["stream_id"]
    matches = [
        lbl for lbl in labels
        if lbl["stream_id"] == sid
        and lbl["t_start_ns"] <= ts <= lbl["t_end_ns"]
    ]
    if not matches:
        return "false_alarm"
    return min(matches, key=lambda lbl: lbl["t_start_ns"])["type"]


def score_memos(memos: list[dict], labels: list[dict], judge) -> dict:
    """Score memos[i] against labels[i]={"type": <fault-type>} using a
    label-correctness check plus an LLM-as-judge quality score.

    Malformed judge output -> score 0.0, reason "malformed_judge_response"
    (best-effort, offline; no retry -- unlike the agent's own tool loop).
    """
    per_memo: list[dict] = []
    for memo, label in zip(memos, labels):
        true_type = label["type"]
        correct = HYPOTHESIS_TO_FAULT_TYPE.get(memo["hypothesis"]) == true_type

        score, reason = _judge_one(memo, true_type, judge)
        per_memo.append({
            "correct": correct,
            "judge_score": score,
            "judge_reason": reason,
        })

    n = len(per_memo)
    accuracy = sum(1 for pm in per_memo if pm["correct"]) / n if n else 0.0
    mean_judge = sum(pm["judge_score"] for pm in per_memo) / n if n else 0.0
    return {"per_memo": per_memo, "accuracy": accuracy,
            "mean_judge_score": mean_judge}


def _judge_one(memo: dict, true_type: str, judge):
    system_prompt = (
        "You are grading a telemetry incident triage memo against the known "
        "ground-truth fault type. Respond with ONE JSON object: "
        '{"score": <0..1 quality>, "reason": "<short>"}. Score how well the '
        "memo's hypothesis and evidence explain the true fault.")
    conversation = [{
        "role": "user",
        "content": json.dumps({"memo": memo, "true_fault_type": true_type}),
    }]
    try:
        raw = judge.complete(system_prompt, conversation)
        obj = json.loads(raw)
        return float(obj["score"]), str(obj["reason"])
    except (ValueError, TypeError, KeyError, RuntimeError):
        return 0.0, "malformed_judge_response"


def build_eval_cases(seed: int, work_dir: str) -> list[dict]:
    """Record a synthetic grid-frequency stream, inject all four labeled fault
    types at well-separated onsets (deterministic for a given seed), run the
    REAL EngineRunner over the faulted stream, and return
    [{"alert": <alerts-row>, "label_type": <resolved ground-truth>}, ...]."""
    os.makedirs(work_dir, exist_ok=True)
    in_path = os.path.join(work_dir, "clean.trec")
    out_path = os.path.join(work_dir, "faulted.trec")
    labels_path = os.path.join(work_dir, "labels.txt")
    db_path = os.path.join(work_dir, "alerts.db")

    stream_id = 1
    n = 200
    rec = te.StreamRecorder(in_path)
    for i in range(n):
        rec.write(te.Message(stream_id=stream_id, ts_ns=1_000_000_000 * (i + 1),
                             value=50.0, seq=i + 1))
    rec.close()

    # Well-separated onsets, one per fault type.
    specs = [
        te.FaultSpec(type=te.FaultType.kSpike, stream_id=stream_id,
                     onset_index=30, duration=2, magnitude=100.0),
        te.FaultSpec(type=te.FaultType.kDrift, stream_id=stream_id,
                     onset_index=70, duration=20, magnitude=10.0),
        te.FaultSpec(type=te.FaultType.kStuckAtValue, stream_id=stream_id,
                     onset_index=120, duration=20, magnitude=0.0),
        te.FaultSpec(type=te.FaultType.kDropout, stream_id=stream_id,
                     onset_index=160, duration=20, magnitude=0.0),
    ]
    injector = te.FaultInjector(seed=seed)
    if not injector.inject(in_path, out_path, labels_path, specs):
        raise RuntimeError("fault injection failed")

    label_dicts = [
        {"stream_id": lbl.stream_id,
         "type": _FAULT_TYPE_TO_STR[lbl.type],
         "t_start_ns": lbl.t_start_ns, "t_end_ns": lbl.t_end_ns}
        for lbl in te.read_labels(labels_path)
    ]

    rule_cfg = te.RuleConfig(stream_id=stream_id, min=45.0, max=55.0,
                             max_rate_of_change=0.5)
    runner = EngineRunner(
        replay_path=out_path, rule_configs=[rule_cfg], ewma_alpha=0.05,
        cusum_alpha=0.5, cusum_k=0.5, cusum_h=3.0, conformal_window=50,
        conformal_alpha=0.01, db_path=db_path)
    runner.run()

    return [
        {"alert": alert,
         "label_type": match_alert_to_label(alert, label_dicts)}
        for alert in runner.alerts
    ]
