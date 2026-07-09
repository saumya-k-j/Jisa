#!/bin/bash
# Container entrypoint: runs the C++ ingest/detection daemon and the FastAPI
# status service side by side. If EITHER process dies, the container exits
# nonzero so the compose restart policy (unless-stopped) restarts BOTH — the
# feed-drop case never gets here (the daemon reconnects internally); this is
# for genuine crashes.
set -euo pipefail

DATA_DIR="${JISA_DATA_DIR:-/data}"
mkdir -p "$DATA_DIR"

/app/bin/coinbase_daemon \
  --data-dir "$DATA_DIR" \
  --rules /app/config/crypto_ticks.yaml \
  --rules /app/config/crypto_eth_usd.yaml &
DAEMON_PID=$!

python /app/api/serve.py &
API_PID=$!

shutdown() {
  kill -TERM "$DAEMON_PID" "$API_PID" 2>/dev/null || true
}
trap shutdown TERM INT

status=0
wait -n "$DAEMON_PID" "$API_PID" || status=$?
shutdown
wait "$DAEMON_PID" "$API_PID" 2>/dev/null || true
exit "$status"
