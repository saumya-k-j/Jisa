# SPEC: Real-Time Telemetry Anomaly Engine

## 0. Purpose and audience
A domain-agnostic engine that ingests high-rate telemetry streams and raises
calibrated anomaly alerts with bounded detection latency. Domains are config,
not code. Built as a recruiting portfolio artifact: correctness, measured
performance, and honesty about what is verified matter more than feature count.

## 1. Measurable objectives (these ARE the project)
- Throughput: sustain high message rates on replay; report measured msgs/sec.
- Latency: report per-message processing latency as p50 / p99 / p99.9 histograms.
- Detection delay: for injected drift faults, report samples between true
  regime change and alert.
- False-alarm rate: hold a target false-positive budget via conformal thresholds
  and verify it empirically.
- Determinism: identical checksummed end-state on replay of a recorded stream.

## 2. Architecture (one generic core + thin domain adapters)
The core knows only "streams of timestamped numeric values with schemas."
Each domain is an adapter: a feed parser + a YAML config + a domain card.
Ship THREE adapters to prove the core is generic:
  (a) crypto ticks (Coinbase WSS) — highest message rate, easiest API
  (b) grid frequency (public TSO/frequency endpoint) — the anchor domain
  (c) ADS-B aircraft (community API) — high rate, different shape

## 3. Modules and interfaces (build each as a separately-tested unit)

### 3.1 core/message
Normalized struct: { uint32 stream_id; int64 ts_ns; double value; uint64 seq }.
Cache-line aligned. No dynamic allocation.

### 3.2 core/ring_buffer
Lock-free SPSC ring buffer of messages. Interface:
  bool try_push(const Message&);  bool try_pop(Message&);
Explicit std::atomic memory ordering (acquire/release). Fixed capacity, power of two.
Also provide an MPSC variant if multiple feeds share one consumer.

### 3.3 core/pool_allocator
Fixed-size block pool for hot-path message objects. Zero allocation after warmup.
  void* allocate();  void deallocate(void*);

### 3.4 feed/handler
WebSocket ingestion (per adapter) -> parse (simdjson) -> Message -> ring buffer.
Detects sequence gaps (missing seq) and handles reconnect/resync. Interface:
  virtual Message parse(std::string_view raw) = 0;   // per-adapter
  void on_gap(uint64 from, uint64 to);

### 3.5 detect/rules  (Layer 1)
Per-stream hard bounds from YAML: min, max, max_rate_of_change.
  enum class RuleResult { OK, OUT_OF_BOUNDS, RATE_VIOLATION };
  RuleResult check(stream_id, value, ts_ns);

### 3.6 detect/baseline  (Layer 2)
Per-stream online EWMA of mean and variance (O(1) update). Emits z-score.
Optional time-of-day/context bucketing.
  void update(stream_id, value);  double zscore(stream_id, value) const;

### 3.7 detect/changepoint  (Layer 3)
CUSUM on the hot path (detects sustained mean shift, reports detection delay).
BOCPD prototype lives in python/research for comparison.
  bool update_and_check(stream_id, value);  // true = changepoint fired

### 3.8 detect/conformal  (Layer 4)
Sets thresholds from a window of recent nonconformity scores to hold a target
false-alarm rate (e.g. threshold = (1-alpha) empirical quantile). Per stream,
adaptive.
  double threshold(stream_id, double alpha) const;

### 3.9 replay + fault injection
Record streams to disk; replay deterministically; checksum end-state.
Inject faults with known ground truth. Fault taxonomy (see fault-injection skill):
spike, drift, stuck-at-value, dropout. Emits labeled ground-truth file for scoring.

### 3.10 bindings (pybind11)
Expose replay + detectors to Python so research/experiments can drive them.

### 3.11 python/api (FastAPI)
Reads engine metrics + alerts over a local socket. Serves a status page:
uptime, messages processed, latency histograms, live alert feed. SQLite or
Postgres for alert history.

### 3.12 python/agent (triage — POST-ALERT ONLY, never on hot path)
On a confirmed alert, an LLM agent with TOOLS investigates and writes an
incident memo. Tools: get_window(stream,t0,t1), get_baseline(stream),
get_concurrent_alerts(), get_domain_card(domain). Reads the per-domain card
for known failure modes. Output: structured memo {stream, hypothesis, confidence}.
Evaluate memos against injected ground truth (LLM-as-judge + labels).

## 4. Verification requirements
- Unit tests per module, written from this spec by the test-writer agent.
- Replay-determinism test in CI (checksum match).
- Sanitizers (ASan/TSan/UBSan) clean in CI.
- Measured numbers recorded in VERIFICATION.md, each mapped to a rerunnable command.
- Anything not runnable in this environment (live feeds, VPS soak, real latency
  on target hardware): implement against recorded fixtures and mark
  "needs-live-validation". DO NOT simulate success.

## 5. Repo layout
As given in the project skeleton (src/{feed,core,detect,bindings}, tests/,
python/{research,agent,api}, config/, docs/domain_cards/, .github/workflows/).

## 6. Non-goals (do NOT build)
- No Kubernetes, no multi-node distribution, no service mesh.
- No authentication / user accounts.
- No monorepo / package workspaces.
- No security/exploit tooling of any kind.
- No speculative abstraction for "future" domains beyond the three adapters.

## 7. Build order (phases; verify each before moving on)
1. core/message, ring_buffer, pool_allocator + tests + benchmarks.
2. feed/handler for ONE adapter (crypto) end-to-end + gap detection + tests.
3. replay + fault injection + determinism test.
4. detect layers 1-3 + tests, with detection-delay measurement.
5. detect layer 4 (conformal) + false-alarm-rate verification.
6. remaining two adapters (parallel).
7. bindings + python/api status page.
8. python/agent triage + evaluation against injected faults.
9. README + architecture diagram + fill VERIFICATION.md.
