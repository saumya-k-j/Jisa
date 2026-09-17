# go/ — alert delivery sidecar

Takes confirmed alerts off the engine's critical path and delivers them to HTTP
subscribers with at-least-once semantics. Contract: SPEC 3.13. Rationale and
limits: DECISIONS D-049 through D-052.

Standard library only. There is no `go.sum` and nothing is fetched at build
time.

## Layout

    cmd/alertd/main.go     binary: env config, graceful shutdown
    delivery/signature.go  HMAC-SHA256 over "<t>.<body>", constant-time verify
    delivery/backoff.go    exponential backoff with full jitter, seeded
    delivery/dedupe.go     bounded idempotency-key set
    delivery/dispatcher.go retry policy, dead-letter, counters
    delivery/server.go     POST /v1/alerts, GET /healthz

## Test

    go test ./...                 # 39 cases
    go test -race -count=3 ./...
    go test -cover ./delivery/    # 89.7% of statements
    go vet ./... && gofmt -l .

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
| `JISA_DELIVERY_DEDUPE_MAX` | no | `8192` |

## Retry policy

Network errors, 429 and 5xx are retried; every other 4xx is permanent and is
dead-lettered on the first response. Backoff for attempt *n* is drawn uniformly
from `[0, min(base * 2^n, cap)]` — full jitter, because synchronised retries
from many senders re-converge into the burst that caused the failure. Anything
that exhausts its attempts is appended to the dead-letter JSONL file and
counted; nothing is dropped silently.
