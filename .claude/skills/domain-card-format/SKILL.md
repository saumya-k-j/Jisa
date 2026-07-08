---
name: domain-card-format
description: Structure for per-domain knowledge cards the triage agent reads. Load when creating a domain adapter or card in docs/domain_cards/.
---
# Domain card format

One markdown file per domain in docs/domain_cards/<domain>.md:

- Sensors/streams: what each stream is, units, expected range.
- Physical relationships: how streams relate (e.g. regional frequency sensors
  move together during real grid events).
- Known failure modes: for each, the SIGNATURE across streams and the likely
  cause. Example: "single stream flatline while neighbors normal => stuck sensor,
  not a real event; multi-stream frequency dip + high rate-of-change =>
  likely generation loss."
- What the agent should do: which tools to call, what to put in the memo.
