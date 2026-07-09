# Deploying to a single Linux VPS (Ubuntu 24.04, x86_64)

The deployment is one Docker container running two processes (see
`deploy/entrypoint.sh`):

- `coinbase_daemon` — the C++ engine: live Coinbase WebSocket ingestion
  (BTC-USD, ETH-USD; no API key needed — the public ticker/heartbeat feed),
  gap detection, and the four detection layers. Survives feed drops via
  automatic reconnection with bounded backoff; it never exits on a
  disconnect.
- FastAPI status service on port 8000 — `/health` (machine health, used by
  the Docker HEALTHCHECK), `/status` (human-readable summary JSON),
  `/alerts` (alert history from SQLite).

Alert history (SQLite) and engine stats live on the named volume
`jisa-data`, so they survive container restarts and image updates.

## 1. Fresh VPS: install Docker

```bash
# as root, or prefix with sudo
apt-get update
apt-get install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu noble stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin
```

Optional (run docker as a non-root user):

```bash
usermod -aG docker $USER   # then log out and back in
```

## 2. Clone and start

```bash
git clone <your-repo-url> jisa
cd jisa
docker compose up -d --build
```

The first build compiles the C++ engine inside the build stage (a few
minutes). `restart: unless-stopped` in docker-compose.yml means the
container restarts on crash and on VPS reboot (as long as the Docker
daemon is enabled, which the packages above do by default).

Verify:

```bash
docker compose ps            # STATUS should become "Up ... (healthy)"
curl -s localhost:8000/health
# {"uptime_seconds": ..., "total_messages_processed": ...,
#  "last_message_timestamp": "..."}
curl -s localhost:8000/status
curl -s "localhost:8000/alerts?limit=10"
```

`/health` returns 503 until the daemon has written its first stats snapshot
(about a second after start); the HEALTHCHECK's `start-period` covers this.
A dropped Coinbase feed does NOT make the container unhealthy — the daemon
reconnects on its own; monitor `last_message_timestamp` externally if you
want staleness alerts.

## 3. Read logs

```bash
docker compose logs -f            # both processes, follow
docker compose logs --since 1h    # recent window
```

Daemon lines (connects, resubscribes, shutdown) and uvicorn access logs are
interleaved in the same stream.

## 4. Restart

```bash
docker compose restart            # restart the container
docker compose down && docker compose up -d   # recreate it
```

Alert history survives both (named volume). To wipe all state too:
`docker compose down -v` (destructive: deletes the alert history).

## 5. Update to a new version

```bash
cd jisa
git pull
docker compose up -d --build      # rebuild image, recreate container
docker image prune -f             # drop the old dangling image layers
```

The volume is untouched by updates; the SQLite history carries over.

## Notes

- The image is pinned to `linux/amd64` (Dockerfile `FROM --platform` and
  compose `platform:`) because the VPS is x86_64 while the dev machine is
  ARM. On the VPS itself this is a no-op; on an ARM dev machine the same
  compose file builds and runs the amd64 image under emulation.
- No API keys or secrets are needed: the Coinbase Exchange public WebSocket
  feed is unauthenticated.
- Port 8000 is exposed on all interfaces by the compose file. If the VPS has
  no firewall in front of it, either restrict it (e.g.
  `ufw allow from <your-ip> to any port 8000`) or change the mapping to
  `127.0.0.1:8000:8000` and front it with a reverse proxy.
