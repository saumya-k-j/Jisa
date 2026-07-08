# Domain card: grid frequency
## Streams
Regional grid frequency sensors, units Hz, nominal 50.00 (EU) / 60.00 (US),
normal band roughly +/- 0.2 Hz.
## Physical relationships
Regional sensors move TOGETHER during real grid events (shared frequency).
Divergence between one sensor and its neighbors implies a local/sensor issue.
## Known failure modes
- single stream flatline, neighbors normal => stuck sensor (not a real event)
- multi-stream dip + high rate-of-change => likely generation loss (real event)
- single spike, isolated => likely telemetry glitch
## Agent actions
Call get_concurrent_alerts to check neighbors; get_window for context; classify
into the failure modes above; memo = {stream, hypothesis, confidence}.
