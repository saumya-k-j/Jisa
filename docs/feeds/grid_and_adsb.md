# Grid-frequency + ADS-B feed research (for Phase 6 adapters)

Researched 2026-07-07 (researcher agent; adsb.fi and OpenSky responses fetched
live and verified).

## Grid frequency (anchor domain)
ALL live grid-frequency APIs found require auth (free registration tiers):
- Gridradar (recommended): POST https://api.gridradar.net/query, Bearer token
  (free research tier). Body: {"metric":"frequency-ucte-median-1s","format":"json",
  "ts":"rfc3339","aggr":"1s"}; response {"data":[{"ts":"<RFC3339>","value":49.98},...]}.
  Continental-Europe 50 Hz zone, 1 s median. Docs: https://service.gridradar.net/index.php?menu=doc
- Fingrid open data (alternative): https://data.fingrid.fi/api dataset 177,
  free API key, updates every 3 min, 10k req/day. Docs: https://data.fingrid.fi/en/datasets/177
CONSEQUENCE: without a user-provided key, the grid adapter is implemented and
tested against recorded/synthetic fixtures matching the Gridradar JSON shape and
marked needs-live-validation in VERIFICATION.md.

## ADS-B aircraft (verified live, no auth)
- adsb.fi (recommended): GET https://opendata.adsb.fi/api/v3/lat/{lat}/lon/{lon}/dist/{distNM}
  No auth; rate limit 1 req/s. Response: {"ac":[{aircraft...}],"now":<epoch ms>,
  "total":N,...}. Aircraft fields: hex (icao24), type, flight (callsign),
  lat/lon (float deg), alt_baro (ft int or "ground"), alt_geom (ft), gs (knots
  float), track, baro_rate/geom_rate (fpm), squawk, messages (count), rssi,
  seen (s since last msg). Docs: https://github.com/adsbfi/opendata/blob/main/README.md
- OpenSky (fallback): GET https://opensky-network.org/api/states/all?lamin=..&lomin=..&lamax=..&lomax=..
  Anonymous: 400 credits/day, 10 s resolution. Response "states" is an
  ORDER-DEPENDENT array (0=icao24, 1=callsign, 3=time_position, 5=lon, 6=lat,
  7=baro alt m, 9=velocity m/s, ...). Docs: https://openskynetwork.github.io/opensky-api/rest.html

## Adapter implications
- Both new domains are POLLED REST feeds (vs Coinbase's push WSS): adapters
  parse a polled JSON snapshot into multiple Messages (one per aircraft/sample).
- ADS-B has no feed sequence number: per-aircraft `messages` count and `now`
  timestamps can drive staleness/dropout ("seen" field) but gap detection
  semantics differ from crypto — pin per-adapter semantics at test time.
- Real adsb.fi fixture can and should be recorded (curl polling) before writing
  tests, same pattern as the Coinbase fixture.
