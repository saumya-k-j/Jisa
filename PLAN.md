# PLAN (living)

Current phase: 9 — DONE (2026-07-08). ALL PHASES COMPLETE.

## Phase 9 — README + diagram + CI + final VERIFICATION (DONE 2026-07-08)
- README.md rewritten (architecture Mermaid, measured-results tables lifted from
  VERIFICATION.md with caveats attached, honest-unverified section, full build/
  run commands, decision highlights); docs/architecture.md (diagram + life-of-a-
  message walkthrough + determinism story); .github/workflows/ci.yml (4 jobs:
  Release+ctest incl. explicit ReplayDeterminism step, ASan+UBSan with
  .ubsan-ignorelist for simdjson's pre-existing __builtin_clz(0), TSan, python
  bindings+pytest) — YAML validated locally, AUTHORED-BUT-UNVERIFIED until
  pushed (needs-live-validation, honestly labeled).
- Bonus: zero-heap-allocation claim upgraded from design-verified to
  TEST-VERIFIED (tests/core/test_hot_path_allocation.cpp, operator new/delete
  counting; 3 scenarios, 0 steady-state allocations; caveats: aligned new not
  intercepted, informational under sanitizers). 180/180 C++ tests.
- Final reviewer (fresh context) audited Phase 9 + whole-SPEC completeness:
  engineering verdict PASS (all 12 modules, 3 complete adapters, all 5
  measurable objectives measured or honestly labeled, non-goals respected).
  Doc-consistency majors found and fixed same-day: stale 177 counts → 180,
  stale zero-alloc caveat rewritten, three ctest -R tokens corrected to
  ReplayDeterminism/DetectionDelay/ConformalFalseAlarmRate (all three verified
  to match tests now), sonnet/6 judge score shown with caveat instead of
  suppressed, .so copy command made version-agnostic, D-007 42/42→43/43.
- Remaining reviewer minors/nits on record (accepted): CI ignorelist wiring
  asymmetric with local recipe (contained — CI row is needs-live-validation);
  allocation audit doesn't intercept aligned operator new (documented in
  README caveat).

## Phase 8 — triage agent + evaluation (DONE 2026-07-08)
- TDD flow: test-writer (RED, 33 tests, all mock-LLM deterministic), implementer
  (separate), reviewer PASSED in fresh context (no blockers/majors; coverage
  minor fixed by test author: +8 tests → 69/69 python total; nits on record:
  evidence-as-string explosion, unclamped confidence, zip truncation).
- python/agent: tools.py (get_window/get_baseline/get_concurrent_alerts/
  get_domain_card via real bindings + SQLite), triage.py (hand-rolled JSON
  tool-loop, MockLLMClient + ClaudeCliClient subprocess — no API key in this
  env, D-042/D-043), evaluate.py (label matching, hypothesis→fault mapping,
  LLM-as-judge D-044), run_eval.py CLI.
- REAL LLM evaluation runs (seed 42, 10 cases, grid domain; full auditable
  tables in docs/eval_runs/):
  - haiku,  max_turns=6:  accuracy 0.400, mean judge 0.295 (3 max-turns fails)
  - sonnet, max_turns=6:  accuracy 0.000 — ALL 10 hit max-turns (sonnet
    investigates longer than 6 turns; turn budget dominates model choice)
  - sonnet, max_turns=12: accuracy 0.400, mean judge 0.290, 0 max-turns fails,
    all memos substantive
  Honest known limits: false_alarm asymmetry (no hypothesis maps to it);
  hypothesis vocab (telemetry_glitch→spike etc.) makes drift under-diagnosed;
  judge occasionally returns malformed responses (scored 0.0, best-effort).
- Verified: 69/69 python tests, 177/177 C++ untouched.

## Phase 7 — bindings + FastAPI status page (DONE 2026-07-08)
- TDD flow: test-writer (RED: loud skips), implementer (separate), reviewer
  PASSED in fresh context: CLEAN (1 minor + nits on record: "baseline" layer
  label advertised but unreachable by design — baseline feeds conformal
  scores, doesn't alert; negative /alerts limit unvalidated; no negative
  binding test for load_rule_config→RuntimeError, verified manually).
- TDD held under pressure: one binding test asserted a fire the pinned CUSUM
  cannot produce; implementer refused to weaken it (D-041), test AUTHOR
  independently re-derived the recursion, confirmed, and replaced with
  known-answer tests mirroring the C++ suite. Reviewer judged the resolution
  sound.
- Module `telemetry_engine` (pybind11 v3.0.1, -DBUILD_PYTHON_BINDINGS, OFF by
  default; D-037/D-038): full replay + detect surface. python/api: EngineRunner
  (real pipeline via bindings) + FastAPI /status /alerts /healthz, SQLite
  history. Honest deviations: in-process engine (D-039), latency histograms
  omitted not faked (D-040), polling alert feed.
- Verified: 28/28 python tests, C++ 177/177 unaffected (bindings ON in build/).

## Phase 6 — grid + ADS-B adapters (DONE 2026-07-08)
- TDD flow: ONE test-writer wrote 31 tests for both adapters from spec +
  fixtures (RED verified); TWO implementers ran in PARALLEL (separate agents,
  strict file isolation, own build trees; D-031 documents the accepted helper
  duplication); reviewer PASSED in fresh context: CLEAN, 2 nits (adsb NaN-cast
  UB impossible via JSON — nit; pre-existing simdjson __builtin_clz(0) UBSan
  note — carried to Phase 9 CI work, needs a suppression file).
- Reviewer independently recomputed all fixture-derived assertions (3600 grid
  samples, 26 hexes, 1124/291 emitted, first-5 hex order) — all match.
- Fixtures: adsb_fi_raw.jsonl REAL (60 polls, Heathrow); gridradar_synthetic
  SYNTHETIC format-faithful (grid APIs need auth keys — needs-live-validation);
  provenance in tests/feed/fixtures/README.md.
- Adapter trio completed per SPEC 2 (parser + YAML config + domain card for
  ALL THREE domains): config/{crypto_ticks,adsb_alt_baro}.yaml verified to
  load via load_rule_config; docs/domain_cards/{crypto_ticks,adsb_aircraft}.md
  added (grid card already existed).
- Verified: 177/177 in Release, ASan+UBSan, TSan (canonical trees, centrally
  rerun after parallel merge). D-028..D-036 recorded.

## Phase 9 carry-forward notes
- CI sanitizer job needs a UBSan suppression for simdjson's internal
  __builtin_clz(0) (pre-existing, not ours — reviewer phase 6).

## Phase 5 — conformal + FPR verification (DONE 2026-07-08)
- TDD flow: test-writer (RED, 12 tests incl. statistical FPR test with
  pre-derived binomial bands, Python-validated before implementation);
  implementer (separate) made them pass; reviewer PASSED in fresh context
  (no blockers/majors). Reviewer independently recomputed bands, quantile
  known answers, AND byte-reproduced the FPR pipeline (0.011580/0.051840).
- Reviewer minor fixed: W==0 edge case now has a regression test
  (Conformal.ZeroCapacityWindowStaysEmptyForever); stale W>0 comment fixed.
- Module: include/detect/conformal.hpp — per-stream ring window, (1-alpha)
  empirical quantile via preallocated mutable scratch (D-026, D-027).
- Interview-ready note from review: we use the n-convention quantile (SPEC
  literal); split-conformal n+1 convention has the exact finite-sample
  guarantee — empirical budget verified held regardless.
- Verified: 146/146 in Release, ASan+UBSan, TSan (sanitizer trees rebuilt
  after the W=0 test addition).

## Phase 2 — feed/handler crypto adapter (DONE 2026-07-07)
- TDD flow: test-writer wrote 26 tests from SPEC 3.4 (RED verified: 4 targets
  fail on missing feed/ headers, 43 phase-1 tests unaffected); implementer
  (separate agent) made them pass; reviewer passed in fresh context with
  3 nits, no blockers/majors/minors (nits on record in review output:
  rfc3339 parser ignores non-Z timezones, strtod lexical laxity, smoke-tool
  SPSC handoff not TSan-covered — all documented, no live impact).
- Key decision D-010: Coinbase trade_id (not sequence) is the contiguous
  per-product counter → Message.seq and gap detection use trade_id;
  heartbeat last_trade_id also drives gap detection. Verified on real data.
- Real fixture: tests/feed/fixtures/coinbase_ticker_raw.jsonl (500 live
  messages). Feed reference: docs/feeds/coinbase_ws.md.
- Verified: 69/69 tests in Release, ASan+UBSan, TSan. Live smoke
  (coinbase_live_smoke, IXWebSocket, BUILD_LIVE_SMOKE=ON): 20s live run,
  157 ticks pushed, 0 parse failures, 0 gaps. VERIFICATION.md updated.

## Phase 3 — replay + fault injection (DONE 2026-07-08)
- TDD flow: test-writer wrote 33 replay tests from SPEC 3.9 + fault-injection
  skill (RED verified); implementer (separate) made them pass; reviewer passed
  in fresh context (no blockers/majors; 2 minors + 2 nits).
- Both minors fixed and re-verified: inject() noexcept made truthful via
  internal try/catch (D-021), read_labels hardened with from_chars
  skip-malformed-line policy (D-021).
- Module: include/replay/ — TREC self-describing format (magic+version,
  explicit LE, 64B records, D-016), FNV-1a-64 end-state checksum over 28
  content bytes with known-answer tests (D-017), pull-based non-throwing
  replayer with truncation handling, seeded SplitMix64 fault injector for
  spike/drift/stuck-at/dropout + pipe-delimited ground-truth labels (D-018,
  D-019). Reviewer-noted nits on record: seed-collision fragility in one test,
  one near-tautological determinism test (covered by stronger tests elsewhere).
- Verified: 102/102 tests in Release, ASan+UBSan, TSan (all re-run after the
  minor fixes).

## Phase 4 — detect layers 1-3 (DONE 2026-07-08)
- TDD flow: test-writer wrote 31 detect tests from SPEC 3.5-3.7 (RED verified);
  implementer (separate) made them pass; reviewer PASSED in fresh context
  (no blockers/majors; 1 minor + nits). Reviewer independently recomputed the
  EWMA/CUSUM known answers and the detection-delay result in Python — all match.
- Minor fixed & re-verified: load_rule_config now parses via from_chars
  (stream_id) + strict strtod (doubles; Apple Clang 14 lacks FP from_chars),
  full-token consumption, all parse failures throw runtime_error (D-022 note).
- Modules: include/detect/{rules,baseline,cusum}.hpp — YAML hard bounds +
  rate-of-change (D-022 hand-rolled flat parser), per-stream EWMA mean/var
  zscore, two-sided CUSUM with warmup arming + reset-after-fire (D-023..D-025).
- Detection delay (objective #3): 12 samples on injected drift (seed 12345,
  CUSUM alpha=.01 k=3 h=8), zero pre-onset false alarms; recorded via gtest
  RecordProperty (reviewer nit: XML not stdout — acceptable).
- Reviewer nits on record, not fixed by choice: zero-variance zscore test
  duplicates the uninitialized case (documented in test comment).
- BOCPD python/research half: see entry above (4/4 pytest, comparison table).
- Verified: 133/133 in Release (clean rebuild), ASan+UBSan, TSan.
- BOCPD prototype (SPEC 3.7 python/research half): DONE — bocpd.py
  (Adams & MacKay, Normal-Gamma prior, truncated run-length posterior),
  compare_cusum.py harness, 4/4 pytest (python/.venv: numpy+pytest).
  Seed-12345 comparison: step delay 0 (both), drift delay BOCPD 34 vs
  CUSUM 28 samples; CUSUM's one pre-change false alarm at ARL0≈900 is
  pinned in the test, not tuned away.

Phases: see SPEC.md section 7. Mark each done only when reviewer passes it.

## Phase 1 — core/message, ring_buffer, pool_allocator (DONE 2026-07-07)
- TDD flow followed: test-writer agent wrote 43 tests from SPEC.md (RED
  confirmed: compile failure with no headers), implementer agent (separate)
  made them pass, reviewer agent reviewed in fresh context (pass-with-nits).
- Reviewer findings addressed: MPSC per-slot alignas(64) removed (memory
  bloat, D-004a), stale fetch_add comments corrected, deterministic MPSC
  wraparound test added by test-writer, D-008/D-009 decision entries added.
- Verified: 43/43 tests pass in Release, ASan+UBSan, and TSan builds
  (Homebrew LLVM clang++ for sanitizer trees — see DECISIONS D-007).
- Benchmarks in benchmarks/bench_core.cpp report p50/p99/p99.9 + throughput;
  numbers recorded in VERIFICATION.md.

## Build trees
- build/       Release, Apple clang (canonical: `cmake -B build -G Ninja`)
- build-asan/  Debug + ASan+UBSan, Homebrew LLVM clang++
- build-tsan/  Debug + TSan, Homebrew LLVM clang++
