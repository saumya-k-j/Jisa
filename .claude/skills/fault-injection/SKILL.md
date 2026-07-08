---
name: fault-injection
description: SCADA/telemetry fault taxonomy and how to inject each with known ground truth for evaluation. Load when building the replay/fault-injection module or the agent evaluation.
---
# Fault injection taxonomy (with ground truth)

Inject into replayed REAL streams; write a labels file (stream, t_start, t_end, type).

- spike: single extreme point. Tests Layer 1 (bounds) and Layer 2 (z-score).
- drift: slow linear/exponential ramp over N samples. Tests Layer 3 (CUSUM)
  detection DELAY — record samples from t_start to alert.
- stuck-at-value: sensor freezes at last value for N samples. Tests Layer 2
  (variance collapse) and simple flatline rules.
- dropout: data stops for a gap. Tests feed/handler sequence-gap detection.

For each injected fault, the engine's alert (or lack of one) is scored against
the label: precision, recall, detection delay. The triage agent's memo is scored
for correct fault-type classification and correct stream localization
(LLM-as-judge + the ground-truth labels).
