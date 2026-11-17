# go/ — alert delivery sidecar

Takes confirmed alerts off the engine's critical path and delivers them to HTTP
subscribers with at-least-once semantics. Contract: SPEC 3.13 (in-memory mode)
and SPEC 3.15 (durable mode). Rationale and limits: DECISIONS D-049 through
D-052 and D-055.

One dependency: the official MongoDB Go driver v2, pinned in go.mod/go.sum.
With `MONGODB_URI` unset the binary runs the original in-memory mode and every
Mongo-backed test skips, so `go test ./...` works offline.

## Layout

    cmd/alertd/main.go     binary: env config, graceful shutdown, mode select
    cmd/faultcheck/        fault harness: primary kill / worker kill / replay
    cmd/alertbench/        accept-path benchmark (w:majority on the path)
    delivery/signature.go  HMAC-SHA256 over "<t>.<body>", constant-time verify
    delivery/backoff.go    exponential backoff with full jitter, seeded
    delivery/dedupe.go     bounded idempotency-key set (in-memory mode)
    delivery/dispatcher.go retry policy, dead-letter, counters
    delivery/server.go     POST /v1/alerts, GET /healthz
    delivery/store.go      durable store: accept, atomic claim, outcomes, watch
    delivery/worker.go     claim loop: change-stream wakeup + ticker sweep
    verification/          measured CI results (mongo_results.json)

## Test

    go test ./...                 # Mongo tests skip without MONGODB_URI
    go test -race -count=3 ./...
    go vet ./... && gofmt -l .

Against the replica set (deploy/docker-compose.mongo.yml; add
`127.0.0.1 mongo1 mongo2 mongo3` to /etc/hosts once):

    export MONGODB_URI='mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0'
    go test -race -count=1 ./...  # 53 cases, durable-store suite included

Fault injection and benchmarks (what CI runs in mongo-verify.yml):

    go build -o /tmp/alertd ./cmd/alertd
    go run ./cmd/faultcheck -scenario primary -n 200 -alertd /tmp/alertd
    go run ./cmd/faultcheck -scenario worker  -n 200 -alertd /tmp/alertd
    go run ./cmd/faultcheck -scenario replay  -n 200 -alertd /tmp/alertd
    go run ./cmd/alertbench -n 5000 -c 32

## Smoke test

Verifies the signature in Python rather than through this package's own
`Verify`, so the check is independent of the code that produced it.

    cat > /tmp/receiver.py <<'EOF'
    import hmac, hashlib, json
    from http.server import BaseHTTPRequestHandler, HTTPServer
    SECRET = b"whsec_smoke"
    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            parts = dict(p.split("=", 1) for p in self.headers.get("Jisa-Signature", "").split(",") if "=" in p)
            expect = hmac.new(SECRET, f"{parts.get('t')}.".encode() + body, hashlib.sha256).hexdigest()
            print(json.dumps({"verified": hmac.compare_digest(expect, parts.get("v1", "")),
                              "body": body.decode()}), flush=True)
            self.send_response(200); self.end_headers()
        def log_message(self, *a): pass
    HTTPServer(("127.0.0.1", 9099), H).serve_forever()
    EOF

    python3 /tmp/receiver.py &
    go build -o /tmp/alertd ./cmd/alertd
    JISA_DELIVERY_SECRET=whsec_smoke \
    JISA_DELIVERY_SUBSCRIBERS=http://127.0.0.1:9099/hook \
    JISA_DELIVERY_DEAD_LETTER=/tmp/dead.jsonl /tmp/alertd &

    A='{"stream_id":1,"ts_ns":1750000000000,"layer":"cusum","detail":"score=9.2>thr=8.0"}'
    curl -s -XPOST -d "$A" localhost:8081/v1/alerts   # 202, {"delivery_id":"dlv_...","duplicate":false}
    curl -s -XPOST -d "$A" localhost:8081/v1/alerts   # 200, same delivery_id, "duplicate":true
    curl -s -XPOST -d '{oops'  localhost:8081/v1/alerts   # 400
    curl -s localhost:8081/healthz

Expected receiver line: `{"verified": true, "body": "{...}"}`.

## Configuration

| Variable | Required | Default |
|---|---|---|
| `JISA_DELIVERY_SECRET` | yes | — |
| `JISA_DELIVERY_SUBSCRIBERS` | yes | — (comma-separated URLs) |
| `JISA_DELIVERY_ADDR` | no | `:8081` |
| `JISA_DELIVERY_MAX_ATTEMPTS` | no | `6` |
| `JISA_DELIVERY_DEAD_LETTER` | no | `dead_letter.jsonl` |
| `JISA_DELIVERY_DEDUPE_MAX` | no | `8192` (in-memory mode) |
| `MONGODB_URI` | no | — (set = durable mode; never committed) |
| `JISA_DELIVERY_DB` | no | `jisa` |
| `JISA_DELIVERY_WORKERS` | no | `4` |
| `JISA_DELIVERY_TTL_SECONDS` | no | `604800` (expire delivered alerts) |
| `JISA_DELIVERY_LEASE_SECONDS` | no | `30` (reclaim in_flight after) |

## Retry policy

Network errors, 429 and 5xx are retried; every other 4xx is permanent and is
dead-lettered on the first response. Backoff for attempt *n* is drawn uniformly
from `[0, min(base * 2^n, cap)]` — full jitter, because synchronised retries
from many senders re-converge into the burst that caused the failure. Anything
that exhausts its attempts is appended to the dead-letter JSONL file and
counted; nothing is dropped silently.
