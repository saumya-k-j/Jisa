# Fixture provenance

| File | Provenance | Recorded |
|---|---|---|
| coinbase_ticker_raw.jsonl | REAL — wss://ws-feed.exchange.coinbase.com, 500 messages (ticker+heartbeat, BTC-USD/ETH-USD), line 1 = subscriptions ack | 2026-07-07 |
| adsb_fi_raw.jsonl | REAL — https://opendata.adsb.fi/api/v3/lat/51.47/lon/-0.45/dist/40 (Heathrow area), 60 polls at 2 s spacing, 18-25 aircraft/poll, one raw response per line | 2026-07-08 |
| gridradar_synthetic.jsonl | SYNTHETIC — format-faithful to the Gridradar API response shape ({"data":[{"ts":RFC3339,"value":Hz},...]}, see docs/feeds/grid_and_adsb.md) because all live grid-frequency APIs require a registered key. Deterministic OU mean-reverting process around 50 Hz (fixed LCG seed), 60 poll responses x 60 one-second samples, range 49.9797-50.0226 Hz. Generated 2026-07-08; the grid adapter is therefore needs-live-validation. | n/a |
