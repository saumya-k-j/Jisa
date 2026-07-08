// tests/feed/test_gridradar.cpp
//
// Tests derived from SPEC.md section 2(b) ("grid frequency (public TSO/
// frequency endpoint) - the anchor domain"), section 3.4 (feed/handler), and
// section 7 phase 6 ("remaining two adapters"). NO implementation exists yet
// under include/feed/gridradar.hpp -- this is the RED state: this translation
// unit is expected to FAIL TO BUILD (missing header) until the implementer
// agent adds it.
//
// docs/feeds/grid_and_adsb.md: Gridradar is a POLLED REST feed (unlike
// Coinbase's push WSS). One raw poll response yields MANY Messages (one per
// {"ts","value"} sample). This adapter therefore does NOT use FeedHandler's
// single-message parse()/handle() (include/feed/handler.hpp); it pins its
// own batch-parse shape, which the ADS-B adapter (test_adsb.cpp) mirrors:
//
//   template <typename F>
//   std::size_t parse_response(std::string_view raw, F&& emit) noexcept;
//
//   - `emit` is called once per accepted sample as emit(const Message&).
//   - Returns the number of times `emit` was invoked (0 on any malformed
//     input -- bad JSON, missing "data" array, non-numeric "value", bad
//     "ts" -- rejected cleanly, never throws).
//   - noexcept end-to-end: no exceptions escape the parse path, per
//     CLAUDE.md / cpp-conventions.
//
// PINNED interface (include/feed/gridradar.hpp), from this spec:
//
//   namespace telemetry::feed {
//   // Fired when the gap between two successive ACCEPTED samples on this
//   // stream exceeds the nominal 1s cadence. `from_ts_ns` is the ts_ns of
//   // the last accepted sample; `to_ts_ns` is the ts_ns of the newly
//   // accepted sample that revealed the gap (to_ts_ns - from_ts_ns > 1s in
//   // ns). Missing seconds are (from_ts_ns, to_ts_ns) exclusive-exclusive.
//   // Fires exactly once per gapped sample (mirrors GapCallback in
//   // feed/handler.hpp, but over wall-clock time rather than seq, since
//   // Gridradar has no native sequence number -- see
//   // docs/feeds/grid_and_adsb.md).
//   using TimeGapCallback =
//       std::function<void(std::uint32_t stream_id,
//                           std::int64_t from_ts_ns, std::int64_t to_ts_ns)>;
//
//   class GridradarHandler {
//   public:
//       // Constructed with a single stream_id: one frequency stream per
//       // handler instance (SPEC 2(b): grid frequency is a single anchor
//       // stream, unlike ADS-B's dynamic multi-stream registry).
//       explicit GridradarHandler(std::uint32_t stream_id,
//                                  TimeGapCallback on_gap = nullptr) noexcept;
//
//       template <typename F>
//       std::size_t parse_response(std::string_view raw, F&& emit) noexcept;
//   };
//   }  // namespace telemetry::feed
//
// PINNED semantics (documented per test below):
//   - seq: successive ACCEPTED samples (across the handler's whole lifetime,
//     i.e. across multiple parse_response() calls) get consecutive seq
//     values assigned BY THE ADAPTER, starting at 1. The feed has no native
//     sequence number (docs/feeds/grid_and_adsb.md).
//   - ts_ns: RFC3339 "...Z" (whole seconds, no fractional part in this feed)
//     -> epoch nanoseconds. Same days-from-civil convention as
//     feed/coinbase.hpp's detail::parse_rfc3339_ns (verified independently
//     in Python: 2026-07-08T00:00:00Z == 1783468800 epoch seconds).
//   - Dedup: a sample with ts_ns <= the LAST ACCEPTED ts_ns on this stream is
//     dropped silently (no emit, no seq consumed, no gap check). This covers
//     both exact duplicates (re-polling the same window) and overlapping
//     poll windows.
//   - Gap detection: see TimeGapCallback above. A poll returning perfectly
//     contiguous 1s-cadence samples never fires on_gap.
//   - Malformed input (bad JSON syntax, missing "data" array, a sample
//     object missing "ts" or "value", "value" not numeric, "ts" not a valid
//     RFC3339 string) causes zero emissions for the malformed sample(s) and
//     never throws. Well-formed samples in the same response are still
//     processed (partial-response tolerance is NOT required by spec, but a
//     malformed sample must not crash the whole parse).
//
// Fixture ground truth (tests/feed/fixtures/gridradar_synthetic.jsonl,
// SYNTHETIC per tests/feed/fixtures/README.md -- needs-live-validation),
// computed independently via a one-off Python pass over the raw JSON lines:
//   - 60 lines, each {"data":[...60 samples...]}: 3600 samples total.
//   - Samples are perfectly contiguous 1s cadence across the WHOLE file:
//     line 0 starts 2026-07-08T00:00:00Z, line 59 ends 2026-07-08T00:59:59Z,
//     zero gaps > 1s anywhere (0 gap events expected end-to-end).
//   - value range across all 3600 samples: [49.9797, 50.0226] Hz, i.e.
//     comfortably within the SPEC-required assertion range [49.9, 50.1].
//
// Spec requirements covered in this file:
//   SPEC-GRID-CTOR       : single stream_id per handler instance.
//   SPEC-GRID-SEQ        : adapter-assigned seq, consecutive from 1, across
//                          calls to parse_response.
//   SPEC-GRID-TS         : RFC3339 (whole seconds, "Z") -> epoch ns.
//   SPEC-GRID-DEDUP      : ts_ns <= last accepted ts_ns dropped silently;
//                          re-feeding the identical poll response yields no
//                          new messages; overlapping poll windows only yield
//                          the strictly-newer tail.
//   SPEC-GRID-GAP        : gap callback fires with (stream_id, from, to) on
//                          a >1s jump between accepted samples; does NOT
//                          fire on contiguous 1s-cadence data.
//   SPEC-GRID-MALFORMED  : bad JSON / missing data array / non-numeric value
//                          / bad ts -> 0 emissions, no throw.
//   SPEC-GRID-E2E        : entire 60-line fixture -> exactly 3600 messages,
//                          ts strictly increasing, values in [49.9, 50.1],
//                          zero gap events (healthy synthetic capture).
// ---------------------------------------------------------------------------

#include <feed/gridradar.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

namespace {

using telemetry::Message;
using telemetry::feed::GridradarHandler;

constexpr std::uint32_t kStreamId = 42;

// Independently verified (Python datetime, UTC):
//   2026-07-08T00:00:00Z -> 1783468800 epoch seconds
//   2026-07-08T00:00:05Z -> 1783468805 epoch seconds
constexpr std::int64_t kTsEpoch00 = 1783468800LL * 1'000'000'000LL;
constexpr std::int64_t kTsEpoch05 = 1783468805LL * 1'000'000'000LL;

std::vector<Message> RunParse(GridradarHandler& handler, std::string_view raw) {
    std::vector<Message> out;
    handler.parse_response(raw, [&](const Message& m) { out.push_back(m); });
    return out;
}

// ---------------------------------------------------------------------------
// SPEC-GRID-CTOR / SPEC-GRID-SEQ / SPEC-GRID-TS: a single valid sample
// produces one Message with the constructed stream_id, correctly-derived
// ts_ns, the raw Hz value, and seq starting at 1.
// ---------------------------------------------------------------------------
TEST(Gridradar, SingleValidSampleEmitsOneMessage) {
    GridradarHandler handler(kStreamId);
    const std::string raw = R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.998}]})";

    std::vector<Message> msgs;
    const std::size_t n = handler.parse_response(raw, [&](const Message& m) { msgs.push_back(m); });

    ASSERT_EQ(n, std::size_t{1});
    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_EQ(msgs[0].stream_id, kStreamId);
    EXPECT_EQ(msgs[0].ts_ns, kTsEpoch00);
    EXPECT_DOUBLE_EQ(msgs[0].value, 49.998);
    EXPECT_EQ(msgs[0].seq, std::uint64_t{1});
}

TEST(Gridradar, SeqIsConsecutiveAcrossSamplesAndAcrossCalls) {
    GridradarHandler handler(kStreamId);
    const std::string raw1 =
        R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.99},)"
        R"({"ts":"2026-07-08T00:00:01Z","value":50.00}]})";
    const std::string raw2 = R"({"data":[{"ts":"2026-07-08T00:00:02Z","value":50.01}]})";

    auto batch1 = RunParse(handler, raw1);
    auto batch2 = RunParse(handler, raw2);

    ASSERT_EQ(batch1.size(), std::size_t{2});
    ASSERT_EQ(batch2.size(), std::size_t{1});
    EXPECT_EQ(batch1[0].seq, std::uint64_t{1});
    EXPECT_EQ(batch1[1].seq, std::uint64_t{2});
    EXPECT_EQ(batch2[0].seq, std::uint64_t{3});  // seq continues across calls
}

// ---------------------------------------------------------------------------
// SPEC-GRID-DEDUP: identical / overlapping polls.
// ---------------------------------------------------------------------------
TEST(Gridradar, FeedingIdenticalPollTwiceYieldsMessagesOnlyOnce) {
    GridradarHandler handler(kStreamId);
    const std::string raw =
        R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.99},)"
        R"({"ts":"2026-07-08T00:00:01Z","value":50.00}]})";

    auto first = RunParse(handler, raw);
    auto second = RunParse(handler, raw);  // exact same poll response again

    EXPECT_EQ(first.size(), std::size_t{2});
    EXPECT_EQ(second.size(), std::size_t{0})
        << "re-polling an identical response must not re-emit already-accepted samples";
}

TEST(Gridradar, OverlappingPollWindowOnlyEmitsStrictlyNewerTail) {
    GridradarHandler handler(kStreamId);

    // First poll: ts 0..2 (3 samples, whole seconds from 00:00:00).
    const std::string raw1 =
        R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.99},)"
        R"({"ts":"2026-07-08T00:00:01Z","value":50.00},)"
        R"({"ts":"2026-07-08T00:00:02Z","value":50.01}]})";
    // Second poll overlaps: ts 1..3 (re-sends ts 1,2, adds ts 3).
    const std::string raw2 =
        R"({"data":[{"ts":"2026-07-08T00:00:01Z","value":50.00},)"
        R"({"ts":"2026-07-08T00:00:02Z","value":50.01},)"
        R"({"ts":"2026-07-08T00:00:03Z","value":50.02}]})";

    auto first = RunParse(handler, raw1);
    auto second = RunParse(handler, raw2);

    ASSERT_EQ(first.size(), std::size_t{3});
    ASSERT_EQ(second.size(), std::size_t{1})
        << "only ts=3 (strictly newer than last accepted ts=2) should be emitted";
    EXPECT_DOUBLE_EQ(second[0].value, 50.02);
    EXPECT_EQ(second[0].seq, std::uint64_t{4});  // seq continues: 1,2,3 then 4
}

// ---------------------------------------------------------------------------
// SPEC-GRID-GAP: time-gap detection between consecutive accepted samples.
// ---------------------------------------------------------------------------
TEST(Gridradar, GapFiresWhenMissingSecondsBetweenAcceptedSamples) {
    std::vector<std::tuple<std::uint32_t, std::int64_t, std::int64_t>> gaps;
    GridradarHandler handler(kStreamId,
        [&](std::uint32_t sid, std::int64_t from, std::int64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    RunParse(handler, R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.99}]})");
    RunParse(handler, R"({"data":[{"ts":"2026-07-08T00:00:05Z","value":50.00}]})");

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<0>(gaps[0]), kStreamId);
    EXPECT_EQ(std::get<1>(gaps[0]), kTsEpoch00);
    EXPECT_EQ(std::get<2>(gaps[0]), kTsEpoch05);
}

TEST(Gridradar, NoGapOnContiguousOneSecondCadence) {
    std::vector<std::tuple<std::uint32_t, std::int64_t, std::int64_t>> gaps;
    GridradarHandler handler(kStreamId,
        [&](std::uint32_t sid, std::int64_t from, std::int64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    const std::string raw =
        R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":49.99},)"
        R"({"ts":"2026-07-08T00:00:01Z","value":50.00},)"
        R"({"ts":"2026-07-08T00:00:02Z","value":50.01}]})";
    RunParse(handler, raw);

    EXPECT_TRUE(gaps.empty());
}

// ---------------------------------------------------------------------------
// SPEC-GRID-MALFORMED: malformed input rejected cleanly, no throw.
// ---------------------------------------------------------------------------
TEST(Gridradar, BadJsonSyntaxEmitsNothing) {
    GridradarHandler handler(kStreamId);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, "{not valid json");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Gridradar, MissingDataArrayEmitsNothing) {
    GridradarHandler handler(kStreamId);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, R"({"foo":"bar"})");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Gridradar, NonNumericValueEmitsNothing) {
    GridradarHandler handler(kStreamId);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, R"({"data":[{"ts":"2026-07-08T00:00:00Z","value":"fifty"}]})");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Gridradar, BadTimestampEmitsNothing) {
    GridradarHandler handler(kStreamId);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, R"({"data":[{"ts":"not-a-date","value":50.0}]})");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Gridradar, SampleMissingTsOrValueFieldEmitsNothing) {
    GridradarHandler handler(kStreamId);
    ASSERT_NO_THROW({
        auto msgs1 = RunParse(handler, R"({"data":[{"value":50.0}]})");
        EXPECT_TRUE(msgs1.empty());
    });

    GridradarHandler handler2(kStreamId);
    ASSERT_NO_THROW({
        auto msgs2 = RunParse(handler2, R"({"data":[{"ts":"2026-07-08T00:00:00Z"}]})");
        EXPECT_TRUE(msgs2.empty());
    });
}

// ---------------------------------------------------------------------------
// SPEC-GRID-E2E: entire recorded (synthetic, format-faithful) fixture.
// ---------------------------------------------------------------------------
TEST(Gridradar, EntireFixtureDrivesExpectedCountMonotonicityAndRange) {
    auto lines = telemetry::test::LoadFixtureLines("gridradar_synthetic.jsonl");
    ASSERT_EQ(lines.size(), std::size_t{60});

    std::vector<std::tuple<std::uint32_t, std::int64_t, std::int64_t>> gaps;
    GridradarHandler handler(kStreamId,
        [&](std::uint32_t sid, std::int64_t from, std::int64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    std::vector<Message> all;
    for (const auto& raw : lines) {
        handler.parse_response(raw, [&](const Message& m) { all.push_back(m); });
    }

    // SPEC-GRID-E2E: 60 lines x 60 samples = 3600, verified independently in
    // Python over the raw fixture.
    EXPECT_EQ(all.size(), std::size_t{3600});

    // Healthy contiguous synthetic capture: zero gap events.
    EXPECT_TRUE(gaps.empty());

    std::int64_t last_ts = -1;
    for (const auto& m : all) {
        EXPECT_EQ(m.stream_id, kStreamId);
        EXPECT_GT(m.ts_ns, last_ts) << "ts must be strictly increasing";
        last_ts = m.ts_ns;
        EXPECT_GE(m.value, 49.9);
        EXPECT_LE(m.value, 50.1);
    }

    // seq strictly consecutive from 1 across the whole fixture.
    for (std::size_t i = 0; i < all.size(); ++i) {
        EXPECT_EQ(all[i].seq, static_cast<std::uint64_t>(i + 1));
    }
}

}  // namespace
