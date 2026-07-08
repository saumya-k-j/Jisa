# Coinbase Exchange WebSocket feed (public market data) — reference

Researched 2026-07-07 from Coinbase docs (researcher agent). Sources:
- https://docs.cdp.coinbase.com/exchange/websocket-feed/overview
- https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
- https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits

## Endpoint
- Production: `wss://ws-feed.exchange.coinbase.com` (no auth for market data).
- Must send a `subscribe` message within 5 seconds of connecting or the server
  closes the connection.

## Subscribe (ticker + heartbeat)
```json
{"type":"subscribe","product_ids":["BTC-USD","ETH-USD"],"channels":["ticker","heartbeat"]}
```
Server replies with a `subscriptions` ack message.

## Ticker message fields
`type`="ticker", `sequence` (integer, per-product monotonic), `product_id`,
`price` (STRING decimal), `time` (RFC3339 UTC with microseconds, e.g.
"2022-10-19T23:28:22.061769Z"), `trade_id` (int), `side`, `last_size`,
plus open_24h/volume_24h/low_24h/high_24h/volume_30d/best_bid(_size)/best_ask(_size)
(all string decimals).

## Heartbeat message fields
`type`="heartbeat", `sequence`, `last_trade_id` (int), `product_id`, `time`.
Sent ~1/sec per product; shares the product's sequence space; used to detect
feed silence.

## Sequence semantics (drives gap detection)
- Per-product, monotonically increasing by exactly 1 per message in the
  product's full stream. NOTE: a channel subscription (e.g. ticker) sees a
  SUBSET of the product's sequence space, so consecutive ticker messages
  normally have sequence jumps > 1; a "gap" for our purposes is detected
  relative to the semantics the handler pins (see tests).
- sequence < last seen: out-of-order/duplicate → ignore.
- Recommended recovery on gap: REST snapshot + replay queued messages
  (order-book use case); for a value stream, resync = reset expected sequence.

## Rate/connection limits
- 8 RPS standard / 20 RPS burst per IP (requests); inbound client messages
  10 RPS / 1000 burst; 10 subscriptions per product-channel baseline.

## Recorded fixture
`tests/feed/fixtures/coinbase_ticker_raw.jsonl` — 500 real messages recorded
2026-07-07 from the production endpoint (BTC-USD + ETH-USD, ticker+heartbeat,
line 1 is the subscriptions ack). Recorder script: python websockets, see
scratchpad/record_coinbase.py (session scratchpad; re-record with any WS client).
