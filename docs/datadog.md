# Datadog export (SPEC 3.14)

The daemon already publishes every counter worth graphing. `python/obs/dogstatsd.py`
reads `stats.json` and `alerts.jsonl` and forwards them to a local Datadog Agent
over UDP. Nothing is collected on the hot path: no allocation, no locks, and no
change to the C++ daemon at all.

## Metrics

| Metric | Type | Notes |
|---|---|---|
| `jisa.up` | gauge | 1 while `stats.json` is fresher than 15s, else 0 |
| `jisa.uptime_seconds` | gauge | daemon clock, not the exporter's |
| `jisa.messages.received` | count | delta per tick |
| `jisa.messages.processed` | count | delta per tick |
| `jisa.ticks.pushed` | count | delta per tick |
| `jisa.feed.parse_failures` | count | delta per tick |
| `jisa.feed.gaps` | count | `trade_id` sequence gaps (D-010) |
| `jisa.feed.reconnects` | count | bounded-backoff reconnects |
| `jisa.feed.last_message_age_seconds` | gauge | **the liveness signal** |
| `jisa.throughput.msgs_per_sec` | gauge | lifetime average, daemon clock |
| `jisa.streams.active` | gauge | streams seen since start |
| `jisa.alerts` | count | tagged `layer:rules\|ewma\|cusum\|conformal` |

`last_message_age_seconds` is the metric that earns its place. A process check
says the daemon is alive and a socket check says the connection is open; neither
notices a feed that has gone quiet. That distinction is the reason this export
exists.

## Run it

The Agent is a sibling container. Add to `docker-compose.yml`:

```yaml
  datadog:
    image: gcr.io/datadoghq/agent:7
    environment:
      - DD_API_KEY=${DD_API_KEY}
      - DD_SITE=${DD_SITE:-datadoghq.com}
      - DD_DOGSTATSD_NON_LOCAL_TRAFFIC=true
      - DD_HOSTNAME=jisa-vps
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - /proc/:/host/proc/:ro
      - /sys/fs/cgroup/:/host/sys/fs/cgroup:ro
```

Then set on the `jisa` service:

```yaml
    environment:
      - DD_AGENT_HOST=datadog
      - JISA_DD_TAGS=service:jisa,env:vps
```

`deploy/entrypoint.sh` starts the exporter only when `DD_AGENT_HOST` is set, so
an unconfigured deployment behaves exactly as before.

Standalone, without Docker:

```
DD_AGENT_HOST=127.0.0.1 JISA_DATA_DIR=/data python -m python.obs.dogstatsd
```

| Variable | Default |
|---|---|
| `DD_AGENT_HOST` | `127.0.0.1` (exporter only starts if set, under the entrypoint) |
| `DD_DOGSTATSD_PORT` | `8125` |
| `JISA_DATA_DIR` | `/data` |
| `JISA_DD_INTERVAL_S` | `10` |
| `JISA_DD_TAGS` | `service:jisa` |

## Dashboard and monitors

`deploy/datadog/dashboard.json` imports through **Dashboards → New → Import
JSON**. `deploy/datadog/monitors.json` holds four monitors; create them through
the API:

```
curl -X POST "https://api.${DD_SITE}/api/v1/monitor" \
  -H "DD-API-KEY: ${DD_API_KEY}" -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
  -H "Content-Type: application/json" \
  -d @<(python3 -c "import json,sys; [print(json.dumps(m)) for m in json.load(open('deploy/datadog/monitors.json'))]" | head -1)
```

or paste each object into the monitor UI's JSON editor.

## Two detectors on one signal

The third monitor runs Datadog's `anomalies()` on the engine's own alert rate.
That is deliberate. The engine's conformal layer holds a measured false-alarm
rate of 0.0116 against a 0.01 target (VERIFICATION.md), derived from a
pre-registered binomial band. Datadog's anomaly detection reaches its own
verdict on the same stream by a different method.

They will not always agree. The disagreements are the interesting part, and the
fault-injection harness makes them reproducible:

```
ctest --test-dir build -R FaultInjection --output-on-failure
```

Inject a known fault, record when the engine's CUSUM layer fires against when
the Datadog monitor transitions, and the gap between the two is a measurement
rather than an opinion.

## Not done

- Detection-latency comparison between the two detectors has not been run on the
  live deployment. The harness exists and the monitors exist; the measurement
  does not. Marked `needs-live-validation` in VERIFICATION.md.
- No trace or log export. The daemon writes structured lines already, so Log
  Management would be a small addition, but metrics are the honest scope today.
