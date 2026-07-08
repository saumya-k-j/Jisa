// tests/core/test_hot_path_allocation.cpp
//
// ALLOCATION-AUDIT: upgrades the CLAUDE.md hot-path claim ("NO heap
// allocation after startup" / VERIFICATION.md row "Zero heap allocation on
// hot path after warmup", currently "design-verified ... no
// allocation-counting test yet") from design-verified to test-verified by
// actually COUNTING heap allocations via global operator new/delete
// overrides, rather than reasoning about the code.
//
// This test is derived from the documented warmup-allocation model in
// DECISIONS.md, NOT from re-reading the implementation for correctness:
//   D-006  PoolAllocator: single aligned_alloc at CONSTRUCTION only; allocate()/
//          deallocate() manipulate an intrusive free list and never touch the
//          heap afterwards.
//   D-011  FeedHandler gap-state map + Coinbase product->stream_id map insert
//          only on first sight of a stream_id / product (bounded warmup);
//          steady-state lookups do not allocate.
//   D-012  CoinbaseTickerHandler reuses one simdjson::dom::parser; its
//          internal tape/input buffer grows to accommodate the longest line
//          seen and is not (re)allocated once it has warmed up to that size.
//   D-025  RuleChecker / EwmaBaseline / CusumDetector keep per-stream state in
//          unordered_map<uint32_t, State>, inserting only the first time a
//          stream_id is seen; steady-state update()/check()/zscore() lookups
//          are allocation-free.
//   D-026  ConformalThreshold allocates each per-stream ring (a
//          std::vector<double>) once on the stream's first update() and sorts
//          into a scratch buffer preallocated (sized W) at construction; both
//          are one-time / construction-time costs, never repeated in steady
//          state.
//
// Spec / CLAUDE.md requirements covered:
//   HOTPATH-ALLOC-PARSE   : CoinbaseTickerHandler::handle() (parse + gap
//                           detection) + SpscRingBuffer try_push/try_pop, in
//                           steady state (after the parser buffer and
//                           product/gap maps have warmed up), performs 0 heap
//                           allocations over a real recorded fixture stream.
//   HOTPATH-ALLOC-DETECT  : RuleChecker::check(), EwmaBaseline::update()/
//                           zscore(), CusumDetector::update_and_check(), and
//                           ConformalThreshold::update()/threshold()/
//                           is_anomalous(), in steady state (after every
//                           stream_id has been seen once), perform 0 heap
//                           allocations over 10k samples per stream.
//   HOTPATH-ALLOC-POOL    : PoolAllocator::allocate()/deallocate(), after the
//                           single construction-time aligned_alloc, perform 0
//                           heap allocations over many alloc/dealloc cycles.
//
// Technique: this translation unit replaces the global operator
// new/new[]/delete/delete[] (including the sized-delete overloads) with
// versions that atomically increment a counter and otherwise forward to
// malloc/free. This is a standard, well-defined technique for a single
// dedicated test binary (the override has external linkage and applies
// process-wide for this executable only; no other test target links this
// file). The nothrow forms are NOT separately overridden because the
// standard library's default nothrow operator new is specified to call the
// throwing form in a try/catch, so it is still captured here.
//
// ASan caveat: this technique can, in principle, run under ASan (the counter
// simply wraps whatever underlying malloc the runtime ends up using), but
// ASan's own allocator may perform bookkeeping allocations that are not
// contract violations of the code under test. To avoid a flaky false failure
// on a sanitizer build, the zero-allocation assertion is gated: under
// __SANITIZE_ADDRESS__ (or __SANITIZE_THREAD__, same rationale) a non-zero
// count is reported as an informational message rather than a hard test
// failure. On a plain (non-sanitizer) Release/Debug build, a non-zero count
// is a genuine, hard test failure — do not weaken that path.
//
// IMPORTANT: per the task, if the steady-state assertion genuinely fails on
// a non-sanitizer build, that is a valuable finding to report as-is (with the
// allocation count), NOT something to fix by widening the warmup window.

#include <core/message.hpp>
#include <core/pool_allocator.hpp>
#include <core/ring_buffer.hpp>
#include <detect/baseline.hpp>
#include <detect/conformal.hpp>
#include <detect/cusum.hpp>
#include <detect/rules.hpp>
#include <feed/coinbase.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

// ---------------------------------------------------------------------------
// Global allocation counter: overrides operator new/new[]/delete/delete[]
// (unsized and sized) for this test binary only.
// ---------------------------------------------------------------------------
namespace {
std::atomic<std::uint64_t> g_alloc_count{0};

std::uint64_t alloc_count() {
    return g_alloc_count.load(std::memory_order_relaxed);
}

void reset_alloc_count() {
    g_alloc_count.store(0, std::memory_order_relaxed);
}
}  // namespace

void* operator new(std::size_t sz) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz == 0 ? 1 : sz);
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}

void* operator new[](std::size_t sz) {
    g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    void* p = std::malloc(sz == 0 ? 1 : sz);
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

// ---------------------------------------------------------------------------
// Assertion macro: hard failure on plain builds, informational-only report
// under ASan/TSan where the runtime's own bookkeeping can add noise that is
// not attributable to the code under test (see header comment).
// ---------------------------------------------------------------------------
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TELEMETRY_EXPECT_ZERO_ALLOC(count, label)                                \
    do {                                                                         \
        if ((count) != 0) {                                                      \
            std::cerr << "[hot-path-alloc][" << (label) << "] sanitizer build: "  \
                       << (count)                                                 \
                       << " allocations counted in steady state (informational " \
                          "only under ASan/TSan; see test_hot_path_allocation.cpp)"\
                       << std::endl;                                             \
        }                                                                         \
    } while (0)
#else
#define TELEMETRY_EXPECT_ZERO_ALLOC(count, label)                                \
    EXPECT_EQ((count), 0u)                                                       \
        << "[" << (label) << "] expected 0 heap allocations in steady state; "   \
        << "got " << (count)
#endif

namespace {

// ---------------------------------------------------------------------------
// HOTPATH-ALLOC-PARSE: CoinbaseTickerHandler::handle() + SpscRingBuffer,
// steady state over the recorded fixture (D-011, D-012).
// ---------------------------------------------------------------------------
TEST(HotPathAllocation, CoinbaseParseAndRingSteadyStateZeroAlloc) {
    using telemetry::Message;
    using telemetry::SpscRingBuffer;
    using telemetry::feed::CoinbaseTickerHandler;
    using telemetry::test::kCoinbaseFixture;
    using telemetry::test::LoadFixtureLines;

    std::unordered_map<std::string, std::uint32_t> product_to_stream{
        {"BTC-USD", 1}, {"ETH-USD", 2}};
    CoinbaseTickerHandler handler(product_to_stream);
    SpscRingBuffer<Message, 512> ring;

    const std::vector<std::string> lines = LoadFixtureLines(kCoinbaseFixture);
    ASSERT_GE(lines.size(), 500u)
        << "fixture shrank below the 50-warmup/450-steady split this test relies on";

    constexpr std::size_t kWarmupLines = 50;
    Message drained{};

    // WARMUP (not measured): parser buffer grows to max line length seen so
    // far, and the product/gap maps insert on first sight of each stream_id
    // (D-011, D-012). Draining after every line keeps the ring from filling.
    for (std::size_t i = 0; i < kWarmupLines; ++i) {
        handler.handle(lines[i], ring);
        while (ring.try_pop(drained)) {
            // drain
        }
    }

    reset_alloc_count();

    // STEADY STATE (measured window): no gtest assertions or string building
    // inside this loop, only plain locals, per the task's noise-avoidance
    // instruction.
    for (std::size_t i = kWarmupLines; i < lines.size(); ++i) {
        handler.handle(lines[i], ring);
        while (ring.try_pop(drained)) {
            // drain
        }
    }

    const std::uint64_t steady_allocs = alloc_count();
    TELEMETRY_EXPECT_ZERO_ALLOC(steady_allocs, "coinbase-parse-and-ring");
}

// ---------------------------------------------------------------------------
// HOTPATH-ALLOC-DETECT: RuleChecker + EwmaBaseline + CusumDetector +
// ConformalThreshold, steady state over synthetic samples on 2 stream_ids
// (D-025, D-026).
// ---------------------------------------------------------------------------
TEST(HotPathAllocation, DetectLayersSteadyStateZeroAlloc) {
    using telemetry::detect::ConformalThreshold;
    using telemetry::detect::CusumDetector;
    using telemetry::detect::EwmaBaseline;
    using telemetry::detect::RuleChecker;
    using telemetry::detect::RuleConfig;

    constexpr std::uint32_t kStreamA = 1;
    constexpr std::uint32_t kStreamB = 2;

    RuleChecker rules;
    // add_rule() is explicitly off the hot path (D-025 comment); registering
    // both rules here, before any counter reset, is not part of the measured
    // steady-state window either way.
    rules.add_rule(RuleConfig{kStreamA, /*min=*/0.0, /*max=*/1e9, /*max_rate_of_change=*/1e9});
    rules.add_rule(RuleConfig{kStreamB, /*min=*/0.0, /*max=*/1e9, /*max_rate_of_change=*/1e9});

    EwmaBaseline baseline(0.1);
    CusumDetector cusum(/*alpha=*/0.1, /*k=*/0.5, /*h=*/5.0, /*warmup_n=*/10);
    ConformalThreshold conformal(/*window_capacity=*/100);

    auto drive_one = [&](std::uint32_t stream_id, double value, std::int64_t ts_ns) {
        rules.check(stream_id, value, ts_ns);
        baseline.update(stream_id, value);
        const double z = baseline.zscore(stream_id, value);
        cusum.update_and_check(stream_id, value);
        conformal.update(stream_id, z);
        conformal.threshold(stream_id, 0.05);
        conformal.is_anomalous(stream_id, z, 0.05);
    };

    // WARMUP (not measured): first sight of each stream_id in
    // baseline/cusum/conformal's unordered_maps inserts a fresh State
    // (D-025), and ConformalThreshold additionally allocates the per-stream
    // ring vector on its first update() for that stream_id (D-026).
    std::int64_t ts_ns = 0;
    constexpr int kWarmupSamplesPerStream = 100;
    for (int i = 0; i < kWarmupSamplesPerStream; ++i) {
        drive_one(kStreamA, 100.0 + static_cast<double>(i % 7), ts_ns);
        drive_one(kStreamB, 50.0 + static_cast<double>(i % 5), ts_ns);
        ts_ns += 1'000'000;
    }

    reset_alloc_count();

    // STEADY STATE (measured window): 10k more samples per stream. Plain
    // locals only; no assertions/string building inside the loop.
    constexpr int kSteadySamplesPerStream = 10000;
    for (int i = 0; i < kSteadySamplesPerStream; ++i) {
        drive_one(kStreamA, 100.0 + static_cast<double>(i % 7), ts_ns);
        drive_one(kStreamB, 50.0 + static_cast<double>(i % 5), ts_ns);
        ts_ns += 1'000'000;
    }

    const std::uint64_t steady_allocs = alloc_count();
    TELEMETRY_EXPECT_ZERO_ALLOC(steady_allocs, "detect-layers");
}

// ---------------------------------------------------------------------------
// HOTPATH-ALLOC-POOL: PoolAllocator allocate()/deallocate() cycles never
// touch the heap after the single construction-time aligned_alloc (D-006).
// ---------------------------------------------------------------------------
TEST(HotPathAllocation, PoolAllocatorSteadyStateZeroAlloc) {
    using telemetry::PoolAllocator;

    constexpr std::size_t kBlockSize = sizeof(telemetry::Message);
    constexpr std::size_t kBlockCount = 64;

    // Construction performs the single aligned_alloc (D-006); not measured.
    PoolAllocator pool(kBlockSize, kBlockCount);

    reset_alloc_count();

    // STEADY STATE (measured window): allocate-then-immediately-deallocate so
    // the pool never exhausts; plain locals only, no assertions inside.
    constexpr int kCycles = 100000;
    for (int i = 0; i < kCycles; ++i) {
        void* p = pool.allocate();
        pool.deallocate(p);
    }

    const std::uint64_t steady_allocs = alloc_count();
    TELEMETRY_EXPECT_ZERO_ALLOC(steady_allocs, "pool-allocator");
}

}  // namespace
