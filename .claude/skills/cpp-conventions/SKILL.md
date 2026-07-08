---
name: cpp-conventions
description: C++20 conventions for the telemetry engine hot path — memory ordering, lock-free patterns, allocation rules, error handling. Load when writing or reviewing any C++ in src/.
---
# C++ conventions (hot path)

- C++20. Compile clean under -Wall -Wextra -Werror. Sanitizers must pass.
- NO heap allocation after startup on the ingestion/detection path. Use the pool allocator.
- No exceptions on the hot path; use error codes / expected-style returns.
- Lock-free structures: use std::atomic with EXPLICIT memory ordering.
  - Producer publishes with release; consumer reads with acquire. Never default seq_cst "just in case" on the hot path without justifying it in DECISIONS.md.
- Prevent false sharing: pad/align shared atomics to cache lines (alignas(64)).
- Ring buffer capacity is a power of two; use bitmask indexing, not modulo.
- Every hot-path structure needs a Google Benchmark microbenchmark reporting p50/p99/p99.9 via HdrHistogram.
- Prefer simple, readable code. No template metaprogramming unless it earns its place in DECISIONS.md.
