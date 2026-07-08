"""RED-stage tests for python/agent/evaluate.py (SPEC 3.12: "Evaluate memos
against injected ground truth (LLM-as-judge + labels)." and SPEC 7 phase 8:
"python/agent triage + evaluation against injected faults.").

PINNED CONTRACT (written BEFORE python/agent/evaluate.py exists):

  Module: python/agent/evaluate.py (colocated with its test; no package
  __init__.py -- `import evaluate` after inserting this dir onto sys.path).

  FAULT-TYPE NORMALIZATION: telemetry_engine.FaultType enum values are
  normalized to these exact lowercase strings (pinned mapping, matches the
  fault-injection skill's taxonomy):
      kSpike -> "spike", kDrift -> "drift",
      kStuckAtValue -> "stuck_at_value", kDropout -> "dropout"
  An alert with no overlapping label of any type gets ground-truth type
  "false_alarm" (pinned literal).

  HYPOTHESIS -> FAULT-TYPE MAPPING TABLE (pinned; drawn from the taxonomy
  words actually used in docs/domain_cards/*.md's "Memo = {...hypothesis:
  ...}" lines):
      evaluate.HYPOTHESIS_TO_FAULT_TYPE = {
          "bad_print": "spike", "sensor_glitch": "spike",
          "telemetry_glitch": "spike",
          "market_move": "drift", "generation_loss": "drift",
          "real_maneuver": "drift",
          "stuck_sensor": "stuck_at_value", "stuck_upstream": "stuck_at_value",
          "feed_gap_artifact": "dropout", "receiver_outage": "dropout",
          "coverage_dropout": "dropout",
      }
  Any hypothesis NOT a key in this table (including the reserved
  "unknown" and "no_conclusion" sentinels) maps to None and can never be
  scored "correct" against any ground-truth type (including "false_alarm",
  which has no hypothesis mapping to it in this table -- a memo is only
  "correct" for a false alarm if... there is in fact no hypothesis string
  that maps to "false_alarm" in this table, so a false_alarm case can never
  be marked correct by hypothesis-mapping alone. This is a documented,
  deliberate asymmetry: the agent has no "this was nothing" hypothesis
  vocabulary in the current domain cards; that gap is left visible rather
  than silently scored as correct.)

  LABEL-MATCHING RULE (pinned): given an alert {"stream_id", "ts_ns"} and a
  list of labels {"stream_id", "type", "t_start_ns", "t_end_ns"}, the
  matching ground-truth type is the type of the label with the SAME
  stream_id whose [t_start_ns, t_end_ns] CLOSED interval contains
  alert["ts_ns"]. If multiple labels match, the one with the EARLIEST
  t_start_ns wins (pinned tie-break). If none match, "false_alarm".

      def evaluate.match_alert_to_label(alert: dict, labels: list[dict]) -> str

  def evaluate.build_eval_cases(seed: int, work_dir: str) -> list[dict]:
      Generates a recorded stream via the REAL bindings, injects labeled
      faults (all four FaultType values, well-separated onsets) via
      FaultInjector(seed), runs python/api/engine.py's EngineRunner over
      the faulted stream to produce REAL alerts (writing to
      <work_dir>/alerts.db), reads back the labels file, and returns
      [{"alert": {...alerts-table row...}, "label_type": str}, ...] using
      match_alert_to_label above. Every returned label_type is one of
      {"spike","drift","stuck_at_value","dropout","false_alarm"}.

  def evaluate.score_memos(memos: list[dict], labels: list[dict],
                            judge) -> dict:
      memos[i] corresponds to labels[i] (labels[i] == {"type": <fault-type
      str>}, i.e. just the ground-truth type, already resolved -- this
      function does NOT do label matching itself, that is
      match_alert_to_label's job). judge is an triage.LLMClient-shaped
      object: judge.complete(system_prompt, conversation) -> str, expected
      to return JSON {"score": 0..1, "reason": str} when prompted with the
      memo + ground truth. PINNED malformed-judge-response handling: score
      = 0.0, reason = "malformed_judge_response" (no retry -- judging is
      best-effort and offline, unlike the agent's own tool loop).

      Returns:
          {"per_memo": [{"correct": bool, "judge_score": float,
                         "judge_reason": str}, ...],
           "accuracy": float,        # mean of per_memo[i]["correct"]
           "mean_judge_score": float}  # mean of per_memo[i]["judge_score"]
      "correct" is
          evaluate.HYPOTHESIS_TO_FAULT_TYPE.get(memo["hypothesis"]) == labels[i]["type"]

Run: python/.venv/bin/python -m pytest python/agent/test_evaluate.py -v
Expected RED result right now: ModuleNotFoundError for `evaluate`
(python/agent/evaluate.py does not exist yet). Only test_evaluate.py's
build_eval_cases test touches real bindings/engine.py; score_memos and
match_alert_to_label tests are pure/mocked, zero LLM/network calls.
"""
from __future__ import annotations

import os
import sys

import pytest

_AGENT_DIR = os.path.dirname(__file__)
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

import evaluate  # python/agent/evaluate.py -- does not exist yet (RED)

import triage  # for MockLLMClient reuse as the judge double (also RED until
                # python/agent/triage.py exists)


# ---------------------------------------------------------------------------
# Pure label-matching rule (no bindings, no I/O)
# ---------------------------------------------------------------------------

class TestMatchAlertToLabel:
    def test_alert_inside_single_label_window_matches_its_type(self):
        alert = {"stream_id": 1, "ts_ns": 5_000_000_000}
        labels = [
            {"stream_id": 1, "type": "spike",
             "t_start_ns": 4_000_000_000, "t_end_ns": 6_000_000_000},
        ]
        assert evaluate.match_alert_to_label(alert, labels) == "spike"

    def test_boundary_ts_inclusive_both_ends(self):
        labels = [
            {"stream_id": 1, "type": "drift",
             "t_start_ns": 1_000, "t_end_ns": 2_000},
        ]
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 1_000}, labels) == "drift"
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 2_000}, labels) == "drift"
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 999}, labels) == "false_alarm"
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 2_001}, labels) == "false_alarm"

    def test_different_stream_id_does_not_match(self):
        labels = [
            {"stream_id": 2, "type": "dropout",
             "t_start_ns": 0, "t_end_ns": 10_000},
        ]
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 5_000}, labels) == "false_alarm"

    def test_alert_outside_any_label_window_is_false_alarm(self):
        labels = [
            {"stream_id": 1, "type": "spike",
             "t_start_ns": 0, "t_end_ns": 100},
        ]
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 999_999}, labels) == "false_alarm"

    def test_no_labels_at_all_is_false_alarm(self):
        assert evaluate.match_alert_to_label(
            {"stream_id": 1, "ts_ns": 1}, []) == "false_alarm"

    def test_overlapping_labels_earliest_t_start_wins(self):
        alert = {"stream_id": 1, "ts_ns": 150}
        labels = [
            {"stream_id": 1, "type": "stuck_at_value",
             "t_start_ns": 100, "t_end_ns": 200},
            {"stream_id": 1, "type": "spike",
             "t_start_ns": 50, "t_end_ns": 200},
        ]
        assert evaluate.match_alert_to_label(alert, labels) == "spike"


# ---------------------------------------------------------------------------
# score_memos: fully mocked (MockLLMClient as judge), hand-computed math
# ---------------------------------------------------------------------------

class TestScoreMemos:
    def test_hand_computed_accuracy_and_mean_judge_score(self):
        memos = [
            {"stream_id": 1, "hypothesis": "bad_print", "confidence": 0.9,
             "evidence": ["e"]},   # maps to "spike" -> correct
            {"stream_id": 1, "hypothesis": "market_move", "confidence": 0.7,
             "evidence": ["e"]},   # maps to "drift" -> correct
            {"stream_id": 2, "hypothesis": "bad_print", "confidence": 0.5,
             "evidence": ["e"]},   # maps to "spike", true type false_alarm
                                    # -> incorrect
        ]
        labels = [{"type": "spike"}, {"type": "drift"},
                  {"type": "false_alarm"}]
        judge = triage.MockLLMClient(responses=[
            '{"score": 0.9, "reason": "matches ground truth"}',
            '{"score": 0.6, "reason": "plausible"}',
            '{"score": 0.1, "reason": "wrong fault type"}',
        ])

        result = evaluate.score_memos(memos, labels, judge)

        assert [pm["correct"] for pm in result["per_memo"]] == [
            True, True, False]
        assert [pm["judge_score"] for pm in result["per_memo"]] == [
            pytest.approx(0.9), pytest.approx(0.6), pytest.approx(0.1)]
        assert result["per_memo"][0]["judge_reason"] == "matches ground truth"

        assert result["accuracy"] == pytest.approx(2.0 / 3.0)
        assert result["mean_judge_score"] == pytest.approx(
            (0.9 + 0.6 + 0.1) / 3.0)

    def test_unmapped_hypothesis_is_never_correct(self):
        memos = [{"stream_id": 1, "hypothesis": "unknown", "confidence": 0.0,
                   "evidence": ["max_turns_exceeded"]}]
        labels = [{"type": "false_alarm"}]
        judge = triage.MockLLMClient(
            responses=['{"score": 0.2, "reason": "no info"}'])

        result = evaluate.score_memos(memos, labels, judge)
        assert result["per_memo"][0]["correct"] is False

    def test_malformed_judge_response_scores_zero_with_pinned_reason(self):
        memos = [{"stream_id": 1, "hypothesis": "bad_print",
                   "confidence": 0.9, "evidence": ["e"]}]
        labels = [{"type": "spike"}]
        judge = triage.MockLLMClient(responses=["not json at all"])

        result = evaluate.score_memos(memos, labels, judge)
        assert result["per_memo"][0]["judge_score"] == 0.0
        assert result["per_memo"][0]["judge_reason"] == \
            "malformed_judge_response"
        # Correctness is independent of judge malfunction: hypothesis still
        # maps to the true type.
        assert result["per_memo"][0]["correct"] is True


# ---------------------------------------------------------------------------
# build_eval_cases: end-to-end with REAL bindings + EngineRunner
# ---------------------------------------------------------------------------

telemetry_engine = pytest.importorskip(
    "telemetry_engine",
    reason=(
        "telemetry_engine pybind11 extension is not built/importable yet "
        "(SPEC 3.10); evaluate.build_eval_cases needs it to drive "
        "FaultInjector + EngineRunner."
    ),
)


class TestBuildEvalCases:
    def test_returns_well_formed_cases_with_at_least_one_spike_match(
            self, tmp_path):
        work_dir = str(tmp_path)
        cases = evaluate.build_eval_cases(seed=1234, work_dir=work_dir)

        assert isinstance(cases, list)
        assert len(cases) >= 1
        allowed_types = {"spike", "drift", "stuck_at_value", "dropout",
                          "false_alarm"}
        for case in cases:
            assert set(case.keys()) == {"alert", "label_type"}
            assert case["label_type"] in allowed_types
            assert {"stream_id", "ts_ns", "layer", "detail"} <= set(
                case["alert"].keys())

        # A large-magnitude spike fault is guaranteed to violate the rules
        # layer's bounds IMMEDIATELY at the injected sample (no detection
        # delay for Layer 1), so at least one case must be spike-labeled.
        assert any(c["label_type"] == "spike" for c in cases)

    def test_same_seed_is_reproducible(self, tmp_path):
        work_dir_a = str(tmp_path / "a")
        work_dir_b = str(tmp_path / "b")
        os.makedirs(work_dir_a)
        os.makedirs(work_dir_b)

        cases_a = evaluate.build_eval_cases(seed=99, work_dir=work_dir_a)
        cases_b = evaluate.build_eval_cases(seed=99, work_dir=work_dir_b)

        types_a = [c["label_type"] for c in cases_a]
        types_b = [c["label_type"] for c in cases_b]
        assert types_a == types_b
