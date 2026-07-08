# DECISIONS (living)
Record every non-obvious architectural choice with a one-line rationale.

## D-001: Message padding layout
Fields sum to 28 bytes (uint32+pad4+int64+double+uint64); explicit `uint8_t _pad1[32]` fills to 64 so sizeof==64 without relying on implicit trailing padding from alignas, which is compiler-defined behavior.

## D-002: SpscRingBuffer — unbounded monotonic head/tail counters
Head and tail are unbounded `size_t` counters masked with `(Capacity-1)` on each slot access. This allows full-capacity occupancy (N items when N == Capacity) because fullness is `tail - head >= Capacity`, not `(tail+1) & mask == head`. The alternative (waste one slot) would break the SPEC-3.2-SPSC-FILL contract pinned by the tests.

## D-003: SpscRingBuffer — acquire/release only, no seq_cst
Producer stores slot data then does `tail_.store(release)`. Consumer does `tail_.load(acquire)`, which synchronizes-with the producer's release store, guaranteeing the slot data is visible. Symmetric for head_. seq_cst would add a full fence on ARM and is unnecessary for the SPSC case with a single synchronization edge in each direction.

## D-004: MpscRingBuffer — Dmitry Vyukov MPMC adapted for MPSC
Producers claim slots via a `compare_exchange_weak(relaxed)` loop on write_cursor_ (the slot's sequence is checked first, so a full buffer is detected without claiming an unusable slot) then publish via a per-slot `sequence.store(release)`. Consumer does `sequence.load(acquire)` and advances read_cursor_ without CAS (single consumer, no contention). Relaxed on the CAS is safe because the RMW's atomicity alone provides mutual exclusion for slot claiming; payload ordering comes from the slot sequence acquire/release pair.

## D-004a: MPSC slots are NOT cache-line padded (reviewer finding)
A slot's data and sequence are touched by one thread at a time (ownership handed off via the sequence acquire/release), so padding each slot to 64B would double memory (128 B/slot for Message) without preventing any real false sharing. Only write_cursor_/read_cursor_ get alignas(64).

## D-005: MpscRingBuffer — no seq_cst on per-slot sequence
The acquire on `sequence` in `try_pop` synchronizes-with the release store in `try_push`, establishing the happens-before edge for the payload. seq_cst is not needed; the pair is sufficient. Any use of seq_cst on the hot path without this justification would violate the conventions in SKILL.md.

## D-006: PoolAllocator — single aligned_alloc at construction only
All memory is reserved in one `aligned_alloc` call in the constructor. `allocate()`/`deallocate()` manipulate an intrusive singly-linked free-list stored in the free blocks themselves (first `sizeof(void*)` bytes). Block stride is rounded up to `alignof(std::max_align_t)` so every block in the contiguous array starts at an aligned address.

## D-007: Sanitizer builds use Homebrew LLVM clang++ (Apple Clang 14 runtime broken on this macOS)
Apple Clang 14's ASan/TSan runtimes abort at startup on this macOS (malloc zone change, `sanitizer_malloc_mac.inc:189`). Resolved by installing Homebrew LLVM and configuring sanitizer trees with `-DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++`. ASan+UBSan and TSan now run clean (43/43) — see VERIFICATION.md for the exact commands.

## D-008: Benchmark harness is hand-rolled percentiles, not Google Benchmark + HdrHistogram
The spec's measurable objective is per-op p50/p99/p99.9; a sorted-sample vector computes exact empirical percentiles with zero dependencies, whereas Google Benchmark reports mean/median per-iteration and would need HdrHistogram bolted on via custom counters. Simplest thing that meets the spec (CLAUDE.md rule); revisit if benchmarks grow. Timer overhead (~40 ns steady_clock) is included in every sample and stated in the output — uncontended p50s at ~41 ns are timer-floor readings, i.e. upper bounds.

## D-010: Coinbase adapter uses trade_id (not sequence) for Message.seq and gap detection
Coinbase's `sequence` spans the product's FULL event feed; the ticker channel sees a subset, so consecutive ticker sequences jump by >1 routinely (fixture: deltas 2..2541, never 1) and expected=seq+1 gap detection would false-fire on every message. `trade_id` increments by exactly 1 per ticker per product (verified on 422 real deltas), so trade_id is the contiguous counter; heartbeats carry last_trade_id and let us detect missed tickers between trades.

## D-009: Warnings (-Wall -Wextra -Werror) scoped to project targets via telemetry_warnings INTERFACE target
Newer clang's -Wcharacter-conversion fires inside googletest headers; third-party code is included via -isystem (FetchContent SYSTEM) and built without -Werror, project code keeps the full strict set.

## D-011: Gap-detection state is an unordered_map keyed by stream_id; product lookup relies on std::string SSO
Per-stream expected-seq state lives in `std::unordered_map<uint32_t,uint64_t>` in FeedHandler. It inserts only the first time each stream_id is seen (bounded by the number of subscribed streams == warmup); steady-state accept_seq() lookups do not allocate. The Coinbase product->stream_id map is `unordered_map<std::string,uint32_t>`; parse() constructs a temporary `std::string(product_id)` as the lookup key, but Coinbase product ids ("BTC-USD") are <= 15 chars and fit libc++ short-string optimization, so the temporary does not touch the heap. Simplest thing that meets the no-per-message-alloc rule without a custom transparent hash.

## D-012: simdjson DOM error-code API, one parser instance reused per handler
CoinbaseTickerHandler owns a single `simdjson::dom::parser`. `parser_.parse(ptr,len)` (realloc_if_needed default) copies the input into the parser's internally-owned padded buffer and reuses/grows it across calls, so there is no per-message tape/buffer allocation after the first few messages warm the buffer to max line length. DOM (not On-Demand) is used so ticker/heartbeat fields can be read in any order without O(n^2) rescans, and because DOM's `.get()`/`.get_string()` error-code API never throws (satisfies the noexcept parse() contract; the throwing operator API is never used). Price/size are JSON *strings*: converted via strtod on a 64-byte stack buffer (null-terminated copy), no heap, exact nearest-double so EXPECT_DOUBLE_EQ holds.

## D-013: RFC3339 -> epoch-ns via days-from-civil, no std::get_time / no allocation
`time` ("2026-07-08T01:56:01.617651Z") is parsed by fixed-field digit scanning + Howard Hinnant's constexpr days_from_civil(); the fractional part is read as up to 9 digits and scaled to nanoseconds. No std::string, no std::get_time/std::istringstream (which allocate and are locale-dependent). Verified exact against three known fixture timestamps (ticker BTC/ETH + heartbeat).

## D-014: telemetry_feed is a separate INTERFACE target; telemetry_core stays dependency-free
simdjson is pulled via FetchContent (pinned v3.10.1, SYSTEM like googletest so -Werror does not apply to its headers). telemetry_core remains an INTERFACE lib with zero third-party deps; the feed adapter's simdjson dependency is isolated in `telemetry_feed` (INTERFACE: include path + simdjson::simdjson + telemetry_core). The feed-test helper in tests/CMakeLists.txt was extended to link telemetry_feed (build glue only — no test assertion changed); the RED-state helper omitted it because the library did not yet exist.

## D-015: Live smoke tool uses IXWebSocket, off by default, out of hot-path claims
`coinbase_live_smoke` (src/feed/coinbase_live_main.cpp) is gated behind `-DBUILD_LIVE_SMOKE=ON` so the core build/test/ASan/UBSan/TSan trees never depend on IXWebSocket or TLS. IXWebSocket is fetched (pinned v11.4.5, SYSTEM, USE_TLS=ON -> Secure Transport on macOS, no OpenSSL). The tool subscribes within 5s of Open, reconnects with bounded backoff (500ms..5s) and calls handler.on_reconnect() on Close/Error to drop stale gap baselines. It is validation scaffolding, not hot-path code, so it is NOT built with telemetry_warnings and its double-parse (one parse() to classify malformed + handle() to push) is acceptable. Verified with a single ~8s live run: 60 messages received, 43 ticks pushed (38 BTC / 5 ETH), 0 gaps, 0 parse failures.

## D-016: Replay file format — self-describing TREC header + fixed 64-byte little-endian records
An 8-byte header (`char magic[4]="TREC"` + `uint32_t version=1` LE) precedes zero or more 64-byte records. Records are serialized field-by-field explicitly (stream_id@0 LE, ts_ns@8 LE, value@16 IEEE-754 LE bits via `std::bit_cast`, seq@24 LE, bytes 4..7 and 32..63 zero) rather than a raw `memcpy` of the `Message` struct: this makes output byte-identical regardless of in-memory alignment-padding contents (required for fault-injection determinism) and pins a portable byte order. This machine is little-endian; the on-disk policy is LE, and the fixed 64-byte stride lets a truncated/corrupt file be detected by simple size arithmetic (partial trailing bytes => `truncated()`), matching the tests' record math.

## D-017: Checksum hashes 28 content bytes, excluding Message padding
`StreamChecksum` is FNV-1a-64 (offset `0xcbf29ce484222325`, prime `0x100000001b3`) folded over exactly 28 bytes per message: `stream_id(4) || ts_ns(8) || value(8) || seq(8)`, all LE, with the struct's `_pad0`/`_pad1` alignment filler explicitly NOT hashed (padding is not stream content, so it must not perturb the deterministic end-state digest). Note this differs from the on-disk record layout (D-016), which keeps the 4-byte gap after stream_id; the checksum concatenates the four content fields with no gap.

## D-018: Fault-injection RNG is hand-rolled SplitMix64, reduced by modulo, not std:: distributions
Auto-resolved onsets (`kAutoOnset`) draw from a seeded SplitMix64 engine (state seeded directly with the ctor seed; classic constants `0x9E3779B97F4A7C15`, `0xBF58476D1CE4E5B9`, `0x94D049BB133111EB`). Onset = `next() % (max_onset+1)`. std library distributions (`std::uniform_int_distribution`) are implementation-defined and would break byte-identical output across stdlibs; a hand-rolled engine + modulo keeps `same seed+input => byte-identical output` portable. Modulo bias is irrelevant here (onset selection, not statistical sampling).

## D-019: Fault semantics — pre-fault value snapshot; timestamps/seq never altered; dropout removes records
Original values are snapshotted before any spec is applied, so stuck-at-value freezes at the true pre-fault onset value and overlapping specs read unmodified data. spike adds `magnitude` to each affected sample; drift adds `magnitude*(j-onset+1)/duration` (ramp reaching exactly `magnitude` at the last affected sample, no persistence past the window); stuck-at-value freezes at `orig_value[onset]`; dropout deletes the affected records entirely from the output while every other message stays bit-identical and in order. ts_ns/seq are never modified by any fault type. Ground-truth labels always carry the ORIGINAL onset/last-affected timestamps (even for dropout, whose samples no longer exist on disk). Label file: one `"<stream_id>|<type_name>|<t_start_ns>|<t_end_ns>"` line per fault, type_name in {spike,drift,stuck_at_value,dropout}.

## D-020: Determinism test's volatile busy-loop suppressed per-target, not globally
`test_replay_determinism.cpp` deliberately uses a `volatile int` accumulator (`busy += k`) to simulate uneven caller pacing between `next()` pulls. C++20 deprecates compound assignment to a volatile lvalue (`-Wdeprecated-volatile`), which is fatal under the project `-Werror`. The test is owned by the test-writer and must not be edited, so the suppression is scoped to that single test target in tests/CMakeLists.txt (`-Wno-deprecated-volatile`); project and hot-path code keep the full strict warning set. Analogous to D-009's scoping around third-party header quirks.

## D-021: read_labels malformed-line policy — skip and continue; inject() catch-all keeps noexcept truthful
`read_labels` never throws on bad content: numeric fields are parsed with strict whole-field `std::from_chars` (non-throwing, rejects empty/partial/non-numeric/overflow), and any line with the wrong field count, unknown type name, or bad numeric field is SKIPPED, parsing continuing with the next line (mirrors the hardened .trec reader's tolerance). Separately, `FaultInjector::inject()` keeps its pinned `noexcept` signature truthful by delegating to a throwing `inject_impl` inside `try { } catch (...) { return false; }` — the body allocates (vector growth, ofstream), and this is offline tooling off the hot path, so a catch-all returning false is the correct failure channel instead of std::terminate on bad_alloc.

## D-022: Rule config YAML loader is a minimal hand-rolled flat-mapping parser, not yaml-cpp
The rule config schema is a FLAT `key: value` mapping (see config/grid_eu_freq.yaml and tests/detect/fixtures/*.yaml): no nesting, no lists, no anchors. Per CLAUDE.md "simplest thing that meets the spec", `load_rule_config` strips inline `#` comments, splits on the first `:`, trims whitespace, and reads only the four required numeric fields (stream_id, min, max, max_rate_of_change) into std::optional; any other keys (name, units, ewma_alpha, conformal_alpha) are ignored. A missing required field, an unopenable file, or min >= max throws std::runtime_error. This runs at startup only (off the hot path), so allocation/exceptions are fine. Adding a yaml-cpp FetchContent dependency would be unjustified weight for a flat schema; revisit only if a future config needs real YAML features (nesting/lists).
Reviewer fix: numeric values are parsed with strict whole-token consumption — `std::from_chars` for stream_id (rejects negatives/trailing garbage, no sign wrap) and `std::strtod` with endptr==token-end for doubles (Apple Clang 14's libc++ lacks floating-point from_chars; strtod's decimal-separator locale dependence is documented in-header and moot because the project never leaves the "C" locale) — with any parse failure thrown as `std::runtime_error` naming the offending key, matching the documented contract (std::stod/stoul previously leaked `std::invalid_argument`, a logic_error).

## D-023: CUSUM uses a self-contained per-stream EWMA standardization (not detect/baseline)
SPEC-3.7 leaves the standardization source open; this repo pins CusumDetector owning its OWN running mean/variance (same EWMA recursion as detect/baseline but private state) rather than depending on an external EwmaBaseline instance. This keeps the changepoint detector a single self-contained hot-path object with no cross-module coupling or ordering hazard between "who updated the baseline first". The residual is standardized against the PRE-update running variance (var_prev); when var_prev == 0 (immediately after the seed) the residual is defined as 0.0 to avoid division by zero / NaN propagation into the accumulators.

## D-024: CUSUM warmup_n arms the detector only after variance is meaningfully estimated
The first call per stream_id is a pure seed (sets mean = x, var = 0, never fires). warmup_n additional-count updates refine mean/variance via the EWMA recursion WITHOUT evaluating S+/S- or firing, so the detector does not raise spurious changepoints while its running variance is still near zero (which would make the standardized residual explode). count is compared as `count < warmup_n` with the seed counted as update #1, matching the pinned recursion in tests/detect/test_cusum.cpp. S+/S- use fmax/fmin and reset to 0 on fire (reset-after-fire) so a sustained new regime does not re-fire every subsequent sample.

## D-025: Per-stream detector state containers follow the D-011 unordered_map pattern
RuleChecker, EwmaBaseline, and CusumDetector each keep per-stream state in `std::unordered_map<uint32_t, State>` keyed by stream_id, inserting on first sight (warmup) and doing allocation-free lookups in steady state — identical to the feed handler's gap-state map (D-011). Const accessors (EwmaBaseline::mean/variance/zscore) use find() and never insert, so querying a never-seen stream_id returns the documented fresh-stream defaults (0.0) without mutating the map.

## D-026: ConformalThreshold sorts into a preallocated mutable scratch buffer; single-threaded detection path
`threshold()` is const (a pure query) but must produce a sorted copy of the per-stream window to pick an order statistic. Rather than allocate a temporary each call (a heap allocation on the hot path, forbidden), it sorts into a `mutable std::vector<double> scratch_` sized to W once in the constructor (startup allocation) and reused every call — the same fixed-capacity-preallocated pattern the pool allocator enforces elsewhere. The `mutable` member means `threshold()`/`is_anomalous()` are NOT safe to call concurrently on the same instance from multiple threads (two const calls race on scratch_); this is acceptable because the detection path is single-threaded per the architecture (one consumer drains the SPSC ring and runs all detect layers in order). The per-stream ring itself is allocated once on the first `update()` for a stream_id (first-sight warmup, D-025/D-011) and never reallocated; `capacity_ == 0` is legal and keeps every window empty (threshold == +infinity). The ring uses modulo indexing (not the power-of-two bitmask of the SPSC ingestion ring) because W is an arbitrary caller-chosen window size (500 here), not a power of two — this is a per-stream statistics window off the lock-free ingestion path, so modulo is the honest, simplest choice.

## D-027: Conformal quantile uses full std::sort into scratch (O(W log W)), not nth_element
`threshold()` computes the "higher order statistic, clamped" quantile s[clamp(ceil((1-alpha)*n)-1, 0, n-1)] via `std::sort` over the n<=W copied scores. W<=500 and threshold() is called at most once per classified score per stream, so O(W log W) (~500*9 ~ 4.5k comparisons worst case) is negligible against the per-message budget; the FPR test drives 50k*2 threshold evaluations through it in under 1s. `std::nth_element` would give O(W) but two alpha values per sample would need two partial selections (or a re-partition of the already-partitioned buffer), and the sorted buffer is trivially reusable/inspectable and matches the pinned ascending-sort quantile convention one-to-one. Chose sort for clarity and exact convention-fidelity; revisit to nth_element only if a benchmark shows this on a hot budget. Verified empirically: seed 0xC0FFEE1234, N=50k, W=500 lands at FPR 0.011580 (alpha=0.01) and 0.051840 (alpha=0.05), inside the pinned bands [0.006441,0.013559] and [0.042203,0.057797].

## D-028: Gridradar seq is adapter-assigned
Gridradar has no native sequence number, so GridradarHandler assigns a monotonic seq from 1, one per ACCEPTED sample, across the handler's whole lifetime (spanning parse_response calls) — gives downstream gap/ordering machinery a dense per-stream sequence the raw feed lacks.

## D-029: Gridradar dedup by timestamp
A sample with ts_ns <= last accepted ts_ns is dropped silently before seq assignment or gap evaluation: polled REST windows overlap and re-send already-seen samples; ts is the only monotonic key available.

## D-030: Gridradar gap detection over wall-clock, not seq
Gap detection compares successive ACCEPTED ts_ns against the 1 s nominal cadence (fires once per gapped pair, from=last, to=new) rather than seq deltas as Coinbase does, because the feed carries no native sequence. Mirrors handler.hpp's GapCallback shape.

## D-031: RFC3339 helper duplication in gridradar_detail (parallel-safe isolation)
The RFC3339/days-from-civil helpers are duplicated from coinbase.hpp rather than shared, to keep the parallel Phase-6 adapter work file-isolated (no cross-header edits). Accepted duplication; single-source refactor deferred to post-Phase-6 cleanup if warranted.

## D-032: ADS-B noexcept field access via simdjson_result .get() only
AdsbHandler uses the same reused-DOM-parser, error-code, noexcept pattern as D-012, with one critical addition: `element["field"]` returns a simdjson_result whose implicit conversion to element THROWS on a missing field, so every access goes through .get()/simdjson_result-taking overloads to keep the batch-parse contract genuinely noexcept.

## D-033: ADS-B permissive numeric reads
Numeric fields go through a read_number that tries get_double, then get_int64, then get_uint64: the real adsb.fi feed mixes integer alt_baro with floating gs/seen.

## D-034: ADS-B "ground" altitude maps to 0.0 and is emitted
alt_baro=="ground" is a valid on-the-ground reading (value 0.0), not a missing field; any other string value is invalid and skips the aircraft.

## D-035: ADS-B stream registry: hex-keyed map, first-seen ids, hard cap
unordered_map keyed by ICAO hex (6 chars, SSO — D-011 precedent); stream_id = base_stream_id + first-seen index, capped at max_streams with over-cap hexes permanently ignored (never inserted). Field validation precedes registration so a malformed aircraft never consumes a stream slot.

## D-036: ADS-B seq adapter-assigned per aircraft; ts-based dedup
The feed's cumulative `messages` field is a per-receiver-network counter (not reliable per-aircraft); seq is adapter-assigned from 1 per aircraft, and dedup is per-aircraft ts_ns-based (ts_ns <= last emitted skipped).

## D-037: pybind11 bindings gated behind BUILD_PYTHON_BINDINGS (OFF by default)
SPEC-3.10 module `telemetry_engine` lives in src/bindings/ and is built only when `-DBUILD_PYTHON_BINDINGS=ON`; the option defaults OFF so the core build/test/sanitizer trees never require a Python interpreter or pybind11 (confirmed: build-asan/build-tsan report "no work to do" after the root CMake change; canonical `build` stays 177/177 with the option OFF and with it ON). pybind11 v3.0.1 pinned via FetchContent URL, marked SYSTEM (headers via -isystem so -Wall -Wextra -Werror does not apply to them); v3.0.1 is the first line with Python 3.14 support (venv is 3.14.6). bindings.cpp itself IS compiled under telemetry_warnings (-Werror clean). Build uses `-DPython3_EXECUTABLE=python/.venv/bin/python` so it targets the venv interpreter.

## D-038: venv .so install = plain copy into site-packages (no global pollution)
The extension is made importable by copying the freshly built `build/src/bindings/telemetry_engine.cpython-314-darwin.so` into `python/.venv/lib/python3.14/site-packages/` — no `pip install`, no global site-packages pollution, no editable install. Rebuild command:
  cmake -B build -G Ninja -DBUILD_PYTHON_BINDINGS=ON -DPython3_EXECUTABLE=$(pwd)/python/.venv/bin/python
  cmake --build build --target telemetry_engine
  cp build/src/bindings/telemetry_engine.cpython-314-darwin.so python/.venv/lib/python3.14/site-packages/
Simplest honest mechanism; the .so is a build artifact, not committed.

## D-039: FastAPI status page hosts the engine IN-PROCESS (no separate daemon)
SPEC-3.11 literally says the API "reads engine metrics + alerts over a local socket," implying a separate long-running C++ engine process. This project has NO such daemon. Instead python/api/engine.py::EngineRunner drives the REAL pipeline (rules -> EWMA baseline -> CUSUM -> conformal) via the pybind11 bindings IN-PROCESS, persisting alerts to SQLite; python/api/app.py::create_app serves /healthz, /status, /alerts from that state. uvicorn's own socket is the "local socket." Documented deviation, not a faked capability.

## D-040: /status omits latency histogram (not faked)
SPEC-1 calls for p50/p99/p99.9 latency histograms, but the C++ hot path does not yet export a latency histogram through the bindings. `GET /status` therefore has NO `latency` key (tests assert its ABSENCE). We do not fabricate placeholder latency numbers; the export is deferred until the hot path measures and exposes real per-message latency.

## D-041: CUSUM binding test contradicted the pinned C++ algorithm (RESOLVED by test author)
RESOLUTION (2026-07-08): the test-writer independently recomputed the pinned recursion, confirmed the implementer's analysis (S+ peaks at ~1.203, never crosses h=3.0), and replaced its own wrong test with two correct ones reusing the C++-pinned known-answer sequence (fire exactly at index 7, reset-after-fire, per-stream independence). python suite now 28/28. Original finding kept below for the record.

## D-041 (original finding): CUSUM binding test contradicts the pinned C++ algorithm (LEFT FAILING)
python/tests/test_bindings.py::TestCusumDetectorFromPython::test_fires_on_sustained_shift expects a fire for CusumDetector(alpha=0.3,k=0.5,h=3.0,warmup_n=2) over [0,0,0,10,10,10,10,10]. The binding faithfully wraps detect/cusum.hpp (no reimplementation); simulating the pinned standardized-CUSUM byte-for-byte shows S+ peaks at ~1.20 < h=3.0, so it never fires. Root cause is inherent to the pinned algorithm: the pre-jump baseline has ZERO variance (three identical 0.0 samples), so the first step to 10 yields residual 0 (var_prev==0) AND inflates the running variance to ~30, damping every subsequent standardized residual below the k=0.5 slack accumulation needed to reach h over only 5 post-warmup samples. Per the TDD contract (implementer must not weaken tests) and CLAUDE.md honesty rules, this test is LEFT FAILING rather than patched; the parameters chosen by the test-writer are inconsistent with the pinned CUSUM (which the 177 C++ tests, incl. tests/detect/test_cusum.cpp, pin to the current behavior). Fixing it requires either the test-writer adjusting the sequence (e.g. a noisy non-zero-variance baseline before the jump, mirroring the C++ InRegime->jump test) or changing the core algorithm (which would break the C++ suite). Not silently worked around.

## D-042: Triage LLM client = local `claude` CLI subprocess (no SDK, no API key)
SPEC-3.12's triage agent needs an LLM, but this environment has no Anthropic API
key and CLAUDE.md forbids unnecessary dependencies. `triage.ClaudeCliClient`
therefore shells out to the already-authenticated local `claude -p --model
<model>` CLI: the rendered prompt (system + conversation) goes in on stdin,
stdout is captured, and a surrounding ```json fence is stripped if the model
wraps its answer. Any subprocess failure (nonzero exit, OSError, timeout) is
raised as RuntimeError, which the agent loop treats as a malformed response
(one retry, then no_conclusion) rather than crashing the run. The LLM contract
is a minimal `LLMClient` Protocol so the loop is unit-tested against a
deterministic `MockLLMClient` with zero network calls; `ClaudeCliClient` is
never exercised by pytest.

## D-043: Hand-rolled JSON tool-call loop, no agent framework
`TriageAgent` is a plain Python loop over a two-shape JSON protocol
({"tool":...,"args":...} and {"memo":...}), not LangChain/an SDK agent runtime
(CLAUDE.md: no unnecessary abstraction/future-proofing). The domain card is
fetched ONCE up front and folded into the system prompt (every investigation
needs it) rather than exposed as a mid-loop tool. Malformed output gets exactly
one in-turn retry (not counted against max_turns) then a `no_conclusion` memo;
max_turns exhaustion also yields `no_conclusion`. Bad kwargs to a KNOWN tool are
caught and fed back to the LLM as a JSON error result so it can self-correct
(the tested paths use valid args, so this guard never changes their behavior);
this is the only deviation beyond the pinned contract and is confined to
robustness of the un-tested real run.

## D-044: LLM-as-judge is best-effort/offline (malformed => 0.0, no retry)
`evaluate.score_memos` grades each memo two ways: a deterministic
label-correctness check (`HYPOTHESIS_TO_FAULT_TYPE[hypothesis] == true_type`,
independent of the LLM) plus an LLM-as-judge quality score in [0,1]. Unlike the
agent's own tool loop, the judge is offline and non-interactive, so a malformed
judge response scores 0.0 with reason "malformed_judge_response" and is NOT
retried. `false_alarm` cases can never be scored "correct" by hypothesis mapping
(no hypothesis word maps to it in the domain cards) -- a deliberate, visible gap
left un-papered-over. `build_eval_cases` is seeded/deterministic on the
stream+fault side; only the LLM calls are nondeterministic, so run_eval.py prints
raw memos and evidence for auditability.
