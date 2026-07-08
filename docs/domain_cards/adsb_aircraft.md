# Domain card: ADS-B aircraft

## Streams
One stream per aircraft (ICAO hex), dynamically registered first-seen
(base_stream_id + index, hard cap). Metric is chosen at adapter construction:
barometric altitude (ft) or ground speed (knots). Samples arrive via polled
snapshots (~2 s cadence); per-aircraft timestamps derive from poll time minus
per-aircraft staleness ("seen").

## Physical relationships
- Altitude and ground speed are physically coupled: climb/descent at roughly
  constant IAS changes ground speed slowly; a jump in BOTH simultaneously is
  suspect.
- Real aircraft dynamics are bounded: transport aircraft climb/descend
  <= ~6000 fpm (100 ft/s), accelerate a few knots/s. Violations are data
  artifacts, not flight.
- alt_baro == "ground" (mapped to 0.0) is a legitimate state transition around
  takeoff/landing, not an anomaly.
- Streams are independent aircraft: unlike grid sensors, cross-stream
  correlation is weak — EXCEPT receiver-side problems, which hit many aircraft
  at once.

## Known failure modes
- one aircraft's value flatlines with growing "seen" staleness => aircraft left
  receiver coverage (dropout), not an event; adapter dedup stops emitting.
- MANY streams go quiet simultaneously => receiver/feeder outage (data-side),
  not an aviation event.
- single-sample altitude spike (e.g. 35000 -> 0 -> 35000) => barometric/MLAT
  glitch or transponder error; signature: isolated spike with instant
  reversion, physically impossible rate.
- steady altitude ramp during cruise phase => real climb/descent (normal); the
  interesting anomaly is UNCOMMANDED drift, only judgeable with route context —
  keep confidence low.
- value stuck exactly constant for many minutes while position (lat/lon) moves
  => stuck field from upstream aggregator.

## Agent actions
get_window(stream, t0, t1) to test spike-reversion vs persistence against the
physical rate bounds above; get_concurrent_alerts() to separate single-aircraft
events from receiver-wide artifacts (many concurrent alerts => data-side);
get_baseline(stream) for the aircraft's recent regime. Memo = {stream,
hypothesis: sensor_glitch | coverage_dropout | receiver_outage | real_maneuver
| stuck_upstream, confidence} — bias to low confidence for "real_maneuver"
absent route context.
