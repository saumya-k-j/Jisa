"""SPEC-3.12 real end-to-end triage evaluation CLI (NOT under pytest).

Builds seeded eval cases through the REAL bindings + EngineRunner, runs the
TriageAgent (backed by the local ``claude`` CLI) on each alert using the
grid_frequency domain card, scores each memo against the injected ground truth
with an LLM-as-judge, and prints an auditable table plus aggregate accuracy and
mean judge score.

The LLM side is inherently nondeterministic; the stream/fault side is seeded and
deterministic. Raw memos are printed so every run is auditable.

Usage:
    python/.venv/bin/python python/agent/run_eval.py [--seed N] [--model M]
        [--max-cases K] [--max-turns T]
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile

_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

import evaluate  # noqa: E402
import tools as tools_mod  # noqa: E402
import triage  # noqa: E402

_REPO_ROOT = os.path.dirname(os.path.dirname(_AGENT_DIR))
_CARDS_DIR = os.path.join(_REPO_ROOT, "docs", "domain_cards")

# Grid-frequency hypothesis taxonomy (from docs/domain_cards/grid_frequency.md).
_GRID_ALLOWED = {"stuck_sensor", "generation_loss", "telemetry_glitch"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--model", type=str, default="haiku")
    parser.add_argument("--max-cases", type=int, default=0,
                        help="0 = all cases; otherwise cap for a quick smoke.")
    parser.add_argument("--max-turns", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args(argv)

    work_dir = tempfile.mkdtemp(prefix="triage_eval_")
    cases = evaluate.build_eval_cases(seed=args.seed, work_dir=work_dir)
    if args.max_cases > 0:
        cases = cases[:args.max_cases]

    print(f"seed={args.seed} model={args.model} cases={len(cases)} "
          f"work_dir={work_dir}")

    db_path = os.path.join(work_dir, "alerts.db")
    replay_path = os.path.join(work_dir, "faulted.trec")

    client = triage.ClaudeCliClient(model=args.model, timeout=args.timeout)

    memos: list[dict] = []
    labels: list[dict] = []
    rows: list[dict] = []
    for case in cases:
        alert = case["alert"]
        ctx = tools_mod.AlertContext(
            replay_path=replay_path, db_path=db_path, domain="grid_frequency",
            cards_dir=_CARDS_DIR, alert_ts_ns=alert["ts_ns"], ewma_alpha=0.05)
        agent_tools = {
            "get_window": ctx.get_window,
            "get_baseline": ctx.get_baseline,
            "get_concurrent_alerts": ctx.get_concurrent_alerts,
            "get_domain_card": ctx.get_domain_card,
        }
        agent = triage.TriageAgent(llm=client, tools=agent_tools,
                                   max_turns=args.max_turns)
        memo = agent.investigate(alert, domain="grid_frequency",
                                 allowed_hypotheses=_GRID_ALLOWED)
        memos.append(memo)
        labels.append({"type": case["label_type"]})
        rows.append({"alert": alert, "true": case["label_type"], "memo": memo})

    result = evaluate.score_memos(memos, labels, client)

    _print_table(rows, result)
    return 0


def _print_table(rows: list[dict], result: dict) -> None:
    header = ("alert(ts_ns/layer)", "true_fault", "hypothesis", "conf",
              "correct", "judge")
    print()
    print("{:<24} {:<12} {:<16} {:>5} {:>7} {:>6}".format(*header))
    print("-" * 76)
    for row, pm in zip(rows, result["per_memo"]):
        alert = row["alert"]
        memo = row["memo"]
        print("{:<24} {:<12} {:<16} {:>5.2f} {:>7} {:>6.2f}".format(
            f"{alert['ts_ns']}/{alert['layer']}",
            row["true"], memo["hypothesis"], float(memo["confidence"]),
            str(pm["correct"]), float(pm["judge_score"])))
        # Raw memo evidence for auditability.
        print(f"    evidence={memo['evidence']} judge_reason={pm['judge_reason']!r}")
    print("-" * 76)
    print(f"accuracy={result['accuracy']:.3f} "
          f"mean_judge_score={result['mean_judge_score']:.3f}")


if __name__ == "__main__":
    raise SystemExit(main())
