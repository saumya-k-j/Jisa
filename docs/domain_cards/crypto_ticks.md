# Domain card: crypto ticks (Coinbase)

## Streams
Per-product last-trade price streams (BTC-USD, ETH-USD), units USD. seq =
exchange trade_id (contiguous per product, D-010); ts from the exchange's
trade timestamp. Update rate is trade-driven and bursty: quiet minutes to
hundreds of msgs/sec.

## Physical relationships
- BTC and ETH (and most majors) are strongly correlated on large moves: a real
  market-wide move shows in both; a violent move in ONE product only is more
  likely a product-specific event (listing, depeg, fat finger) or data issue.
- Price is a martingale-ish series: baselines drift constantly; volatility
  regime shifts are normal, so changepoint alerts alone are weak evidence.
- Heartbeats arrive ~1/sec per product even when no trades print.

## Known failure modes
- seq (trade_id) gap reported by on_gap => feed dropped messages (Coinbase WS
  drops under load); NOT a market event. Signature: gap event, then prices
  resume from a slightly different level.
- flatline (no ticks) while heartbeats continue => quiet market or one-sided
  book, normal for small products; flatline AND no heartbeats => connection
  stall (client-side reconnect issue).
- single tick far off-market, immediately reverting => stale/out-of-order or
  fat-finger print; check whether neighbors (other products) moved.
- sustained drift in one product with the other flat => product-specific
  event (real); both drifting together => market-wide move (real).
- price exactly repeating for many ticks with rising seq => possible stuck
  upstream cache; distinguish from a pegged/stable pair which is legitimately
  near-constant.

## Agent actions
Call get_concurrent_alerts() — same-direction alerts in correlated products
support a real market move; get_window(stream, t0, t1) to check for reversion
(glitch) vs persistence (regime); note any gap events near the alert time in
the memo. Memo = {stream, hypothesis: market_move | feed_gap_artifact |
bad_print | stuck_upstream, confidence}.
