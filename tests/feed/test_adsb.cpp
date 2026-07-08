// tests/feed/test_adsb.cpp
//
// Tests derived from SPEC.md section 2(c) ("ADS-B aircraft (community API) -
// high rate, different shape"), section 3.4 (feed/handler), and section 7
// phase 6 ("remaining two adapters"). NO implementation exists yet under
// include/feed/adsb.hpp -- this is the RED state: this translation unit is
// expected to FAIL TO BUILD (missing header) until the implementer agent
// adds it.
//
// This file is intentionally fully independent of test_gridradar.cpp (both
// adapters' implementations proceed in parallel): no shared adapter-specific
// helpers beyond the generic fixture_utils.hpp loader used by every feed
// test.
//
// docs/feeds/grid_and_adsb.md: adsb.fi is a POLLED REST feed. One raw poll
// response ({"ac":[...aircraft...],"now":<epoch ms>,...}) yields MANY
// Messages -- one per aircraft with a usable sample -- mirroring the same
// batch-parse shape pinned for Gridradar (test_gridradar.cpp):
//
//   template <typename F>
//   std::size_t parse_response(std::string_view raw, F&& emit) noexcept;
//
//   - `emit` is called once per accepted aircraft sample as
//     emit(const Message&). Returns the number of times `emit` was invoked
//     (0 on malformed input, never throws).
//
// PINNED interface (include/feed/adsb.hpp), from this spec:
//
//   namespace telemetry::feed {
//   enum class AdsbMetric : std::uint8_t { kAltBaro, kGroundSpeed };
//
//   class AdsbHandler {
//   public:
//       // base_stream_id: the stream_id assigned to the FIRST distinct
//       //   aircraft (hex) seen, in first-seen order; subsequent distinct
//       //   aircraft get base_stream_id+1, +2, ... .
//       // max_streams: registry cap. Once max_streams distinct hexes have
//       //   been registered, any FURTHER new hex is ignored entirely (no
//       //   stream_id assigned, no Message emitted for it), for the
//       //   lifetime of this handler instance.
//       // metric: which field becomes Message.value for every aircraft.
//       AdsbHandler(std::uint32_t base_stream_id, std::size_t max_streams,
//                   AdsbMetric metric) noexcept;
//
//       template <typename F>
//       std::size_t parse_response(std::string_view raw, F&& emit) noexcept;
//   };
//   }  // namespace telemetry::feed
//
// PINNED semantics (documented per test below):
//   - Multi-stream registry: one stream_id per distinct aircraft hex,
//     assigned dynamically in first-seen order starting at base_stream_id,
//     bounded by max_streams (see ctor doc above).
//   - Metric selection at construction: kAltBaro -> Message.value =
//     alt_baro as double; kGroundSpeed -> Message.value = gs as double.
//     alt_baro may be the JSON STRING "ground" for a grounded aircraft
//     (verified in the real fixture: e.g. hex "424a61", alt_baro:"ground").
//     PINNED CHOICE: when metric==kAltBaro and alt_baro=="ground", the
//     sample IS emitted with Message.value = 0.0 (grounded aircraft is a
//     real, valid altitude-adjacent reading of "on the ground", not a
//     missing-field condition -- documented choice, see test below).
//   - ts_ns = (response "now", epoch ms) - (per-aircraft "seen", seconds
//     since that aircraft's last message), converted to nanoseconds:
//       ts_ns = now_ms * 1'000'000 - (int64)(seen * 1e9)
//     so per-aircraft ts_ns approximates that aircraft's last-message wall
//     clock time, independent of poll cadence.
//   - Dedup per aircraft: a sample whose computed ts_ns <= the last EMITTED
//     ts_ns for that hex is skipped (aircraft unchanged / stale between
//     polls -- e.g. same "seen" value re-polled).
//   - seq: a per-aircraft counter assigned BY THE ADAPTER, starting at 1 for
//     that aircraft's first accepted sample and incrementing by 1 per
//     accepted (non-deduped) sample for that aircraft. The feed's
//     cumulative `messages` field is a per-receiver-network counter, NOT
//     reliable per SPEC/docs, and is never used for seq.
//   - Missing fields: an aircraft missing "hex", "lat"/"lon", or the field
//     required by the selected metric (alt_baro for kAltBaro, gs for
//     kGroundSpeed) is skipped entirely (no Message, no stream registered
//     if hex was never seen before) -- no throw.
//
// Fixture ground truth (tests/feed/fixtures/adsb_fi_raw.jsonl, REAL capture
// per tests/feed/fixtures/README.md: adsb.fi, Heathrow area, 60 polls at 2s
// spacing), computed independently via a one-off Python pass over the raw
// JSON lines (simulating the pinned algorithm above):
//   - 60 lines (poll responses), 18-25 aircraft per line, 1182 total
//     aircraft-observations across the file.
//   - 26 DISTINCT hex values across the whole file. First-seen order (first
//     5): 40683e, aa7f5e, 424a61, 42584c, 42584b.
//   - No aircraft in this fixture is ever missing hex/gs/alt_baro/lat/lon
//     and no aircraft is ever missing "seen" -- the missing-field tests
//     below therefore use small hand-built synthetic single-poll JSON
//     (documented per test), while the end-to-end test uses the real
//     fixture for volume/shape.
//   - Simulated with a GENEROUS cap (>= 26, so the cap never binds): total
//     emitted messages (after per-aircraft ts_ns dedup) == 1124 for EITHER
//     metric (dedup is ts_ns-based, not value-based, so both metrics drop
//     the exact same samples), distinct streams == 26.
//   - Simulated with max_streams == 5: total emitted == 291, distinct
//     streams == 5 (aircraft #6 onward across the whole file are ignored).
//   - alt_baro (numeric, "ground" mapped to 0.0) range across the fixture:
//     [0.0, 40000.0] ft. gs range: [0.0, 517.8] kn.
//
// Spec requirements covered in this file:
//   SPEC-ADSB-REGISTRY    : dynamic hex -> stream_id, first-seen order,
//                          base_stream_id offset, max_streams cap enforced.
//   SPEC-ADSB-METRIC      : kAltBaro / kGroundSpeed selection at construction.
//   SPEC-ADSB-GROUND      : alt_baro=="ground" -> value 0.0 (pinned choice).
//   SPEC-ADSB-TS          : ts_ns = now_ms*1e6 - seen*1e9, exact formula.
//   SPEC-ADSB-DEDUP       : per-aircraft ts_ns <= last emitted ts_ns skipped.
//   SPEC-ADSB-SEQ         : per-aircraft adapter-assigned seq from 1,
//                          independent per aircraft, ignores `messages`.
//   SPEC-ADSB-MISSING     : missing hex/lat/lon/metric-field -> aircraft
//                          skipped, no throw.
//   SPEC-ADSB-MALFORMED   : bad JSON / missing "ac" array -> 0 emissions,
//                          no throw.
//   SPEC-ADSB-E2E         : entire real 60-line fixture -> per-stream ts
//                          monotonicity, plausible value ranges, distinct
//                          stream count == distinct hexes (up to cap), and a
//                          floor on total emitted messages.
// ---------------------------------------------------------------------------

#include <feed/adsb.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

namespace {

using telemetry::Message;
using telemetry::feed::AdsbHandler;
using telemetry::feed::AdsbMetric;

constexpr std::uint32_t kBaseStreamId = 100;

std::vector<Message> RunParse(AdsbHandler& handler, std::string_view raw) {
    std::vector<Message> out;
    handler.parse_response(raw, [&](const Message& m) { out.push_back(m); });
    return out;
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-REGISTRY / SPEC-ADSB-METRIC / SPEC-ADSB-TS / SPEC-ADSB-SEQ: a
// single well-formed aircraft, first poll.
// ---------------------------------------------------------------------------
TEST(Adsb, SingleAircraftFirstSeenGetsBaseStreamIdAndSeqOne) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);

    // now = 1700000000000 ms, seen = 2.5 s ->
    //   ts_ns = 1700000000000 * 1'000'000 - 2'500'000'000
    //         = 1699999997500000000
    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":123.4,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":2.5}],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_EQ(msgs[0].stream_id, kBaseStreamId);
    EXPECT_EQ(msgs[0].ts_ns, 1699999997500000000LL);
    EXPECT_DOUBLE_EQ(msgs[0].value, 123.4);  // kGroundSpeed -> gs
    EXPECT_EQ(msgs[0].seq, std::uint64_t{1});
}

TEST(Adsb, AltBaroMetricUsesAltBaroField) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kAltBaro);

    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":123.4,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":1.0}],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_DOUBLE_EQ(msgs[0].value, 5000.0);
}

// SPEC-ADSB-GROUND: pinned choice -- alt_baro=="ground" emits value 0.0
// under kAltBaro (does not skip the aircraft).
TEST(Adsb, GroundedAircraftAltBaroMetricEmitsZero) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kAltBaro);

    const std::string raw =
        R"({"ac":[{"hex":"424a61","gs":0.0,"alt_baro":"ground",)"
        R"("lat":51.46,"lon":-0.45,"seen":0.9}],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_DOUBLE_EQ(msgs[0].value, 0.0);
}

// Grounded aircraft under kGroundSpeed metric: gs is present and numeric
// (0.0 while grounded in the real feed), so it is emitted normally --
// "ground" only affects the alt_baro field/metric, never gs.
TEST(Adsb, GroundedAircraftGroundSpeedMetricEmitsGsValue) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);

    const std::string raw =
        R"({"ac":[{"hex":"424a61","gs":0.0,"alt_baro":"ground",)"
        R"("lat":51.46,"lon":-0.45,"seen":0.9}],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_DOUBLE_EQ(msgs[0].value, 0.0);
}

TEST(Adsb, MultipleAircraftFirstSeenOrderAssignsConsecutiveStreamIds) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);

    const std::string raw =
        R"({"ac":[)"
        R"({"hex":"aaa111","gs":100.0,"alt_baro":1000,"lat":51.0,"lon":-0.4,"seen":0.0},)"
        R"({"hex":"bbb222","gs":200.0,"alt_baro":2000,"lat":52.0,"lon":-0.5,"seen":0.0})"
        R"(],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    ASSERT_EQ(msgs.size(), std::size_t{2});
    std::set<std::uint32_t> ids{msgs[0].stream_id, msgs[1].stream_id};
    std::set<std::uint32_t> expected{kBaseStreamId, kBaseStreamId + 1};
    EXPECT_EQ(ids, expected);
    // First-seen order: the first aircraft in "ac" gets base_stream_id.
    EXPECT_EQ(msgs[0].stream_id, kBaseStreamId);
    EXPECT_EQ(msgs[1].stream_id, kBaseStreamId + 1);
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-REGISTRY: max_streams cap.
// ---------------------------------------------------------------------------
TEST(Adsb, AircraftBeyondMaxStreamsCapAreIgnored) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/1, AdsbMetric::kGroundSpeed);

    const std::string raw =
        R"({"ac":[)"
        R"({"hex":"aaa111","gs":100.0,"alt_baro":1000,"lat":51.0,"lon":-0.4,"seen":0.0},)"
        R"({"hex":"bbb222","gs":200.0,"alt_baro":2000,"lat":52.0,"lon":-0.5,"seen":0.0})"
        R"(],"now":1700000000000})";

    auto msgs = RunParse(handler, raw);

    // Only the first-seen aircraft (cap == 1) is registered/emitted.
    ASSERT_EQ(msgs.size(), std::size_t{1});
    EXPECT_EQ(msgs[0].stream_id, kBaseStreamId);
}

TEST(Adsb, AircraftBeyondCapStillIgnoredOnSubsequentPolls) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/1, AdsbMetric::kGroundSpeed);

    RunParse(handler,
        R"({"ac":[{"hex":"aaa111","gs":100.0,"alt_baro":1000,)"
        R"("lat":51.0,"lon":-0.4,"seen":0.0}],"now":1700000000000})");

    // "bbb222" never seen before, but the registry is already at cap.
    auto msgs = RunParse(handler,
        R"({"ac":[{"hex":"bbb222","gs":200.0,"alt_baro":2000,)"
        R"("lat":52.0,"lon":-0.5,"seen":0.0}],"now":1700000002000})");

    EXPECT_TRUE(msgs.empty());
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-DEDUP / SPEC-ADSB-SEQ across polls.
// ---------------------------------------------------------------------------
TEST(Adsb, UnchangedAircraftBetweenPollsIsDeduped) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);

    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":123.4,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":2.5}],"now":1700000000000})";

    auto first = RunParse(handler, raw);
    auto second = RunParse(handler, raw);  // identical poll: same now, same seen

    EXPECT_EQ(first.size(), std::size_t{1});
    EXPECT_EQ(second.size(), std::size_t{0})
        << "identical now/seen -> identical computed ts_ns -> deduped";
}

TEST(Adsb, ChangedAircraftAcrossPollsIsEmittedWithIncrementingSeq) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);

    auto first = RunParse(handler,
        R"({"ac":[{"hex":"abc123","gs":123.4,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":2.5}],"now":1700000000000})");
    // 2 seconds later; aircraft has a fresher "seen" (0.5s since its own last
    // message at the new poll time) -> strictly newer ts_ns.
    auto second = RunParse(handler,
        R"({"ac":[{"hex":"abc123","gs":150.0,"alt_baro":5100,)"
        R"("lat":51.01,"lon":-0.41,"seen":0.5}],"now":1700000002000})");

    ASSERT_EQ(first.size(), std::size_t{1});
    ASSERT_EQ(second.size(), std::size_t{1});
    EXPECT_EQ(first[0].seq, std::uint64_t{1});
    EXPECT_EQ(second[0].seq, std::uint64_t{2});
    EXPECT_GT(second[0].ts_ns, first[0].ts_ns);
    EXPECT_EQ(second[0].stream_id, first[0].stream_id);  // same aircraft, same stream
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-MISSING: missing fields -> aircraft skipped, no throw.
// ---------------------------------------------------------------------------
TEST(Adsb, AircraftMissingGroundSpeedIsSkippedUnderGroundSpeedMetric) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    // No "gs" field at all.
    const std::string raw =
        R"({"ac":[{"hex":"abc123","alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":1.0}],"now":1700000000000})";

    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, raw);
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, AircraftMissingAltBaroIsSkippedUnderAltBaroMetric) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kAltBaro);
    // No "alt_baro" field at all.
    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":100.0,)"
        R"("lat":51.0,"lon":-0.4,"seen":1.0}],"now":1700000000000})";

    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, raw);
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, AircraftMissingLatLonIsSkipped) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":100.0,"alt_baro":5000,)"
        R"("seen":1.0}],"now":1700000000000})";

    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, raw);
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, AircraftMissingHexIsSkipped) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    const std::string raw =
        R"({"ac":[{"gs":100.0,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4,"seen":1.0}],"now":1700000000000})";

    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, raw);
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, AircraftMissingSeenIsSkipped) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    const std::string raw =
        R"({"ac":[{"hex":"abc123","gs":100.0,"alt_baro":5000,)"
        R"("lat":51.0,"lon":-0.4}],"now":1700000000000})";

    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, raw);
        EXPECT_TRUE(msgs.empty());
    });
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-MALFORMED.
// ---------------------------------------------------------------------------
TEST(Adsb, BadJsonSyntaxEmitsNothing) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, "{not valid json");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, MissingAcArrayEmitsNothing) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler, R"({"now":1700000000000})");
        EXPECT_TRUE(msgs.empty());
    });
}

TEST(Adsb, MissingNowFieldEmitsNothingForThatPoll) {
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/10, AdsbMetric::kGroundSpeed);
    // "now" is required to compute ts_ns; its absence makes the whole poll
    // response unusable (cannot derive any per-aircraft timestamp).
    ASSERT_NO_THROW({
        auto msgs = RunParse(handler,
            R"({"ac":[{"hex":"abc123","gs":100.0,"alt_baro":5000,)"
            R"("lat":51.0,"lon":-0.4,"seen":1.0}]})");
        EXPECT_TRUE(msgs.empty());
    });
}

// ---------------------------------------------------------------------------
// SPEC-ADSB-E2E: entire real fixture.
// ---------------------------------------------------------------------------
TEST(Adsb, EntireFixtureGenerousCapProducesExpectedShapeAndCounts) {
    auto lines = telemetry::test::LoadFixtureLines("adsb_fi_raw.jsonl");
    ASSERT_EQ(lines.size(), std::size_t{60});

    // Cap well above the 26 distinct hexes verified in the fixture, so the
    // cap never binds.
    AdsbHandler handler(kBaseStreamId, /*max_streams=*/1000, AdsbMetric::kGroundSpeed);

    std::unordered_map<std::uint32_t, std::int64_t> last_ts_per_stream;
    std::unordered_map<std::uint32_t, std::uint64_t> last_seq_per_stream;
    std::set<std::uint32_t> distinct_streams;
    std::size_t total = 0;

    for (const auto& raw : lines) {
        handler.parse_response(raw, [&](const Message& m) {
            ++total;
            distinct_streams.insert(m.stream_id);

            auto ts_it = last_ts_per_stream.find(m.stream_id);
            if (ts_it != last_ts_per_stream.end()) {
                EXPECT_GT(m.ts_ns, ts_it->second)
                    << "per-stream ts_ns must be strictly increasing, stream "
                    << m.stream_id;
            }
            last_ts_per_stream[m.stream_id] = m.ts_ns;

            auto seq_it = last_seq_per_stream.find(m.stream_id);
            if (seq_it != last_seq_per_stream.end()) {
                EXPECT_EQ(m.seq, seq_it->second + 1)
                    << "per-stream seq must be consecutive, stream " << m.stream_id;
            } else {
                EXPECT_EQ(m.seq, std::uint64_t{1});
            }
            last_seq_per_stream[m.stream_id] = m.seq;

            // Ground speed plausible range (kn): [0, 700] is generous
            // (fixture's true range is [0.0, 517.8]).
            EXPECT_GE(m.value, 0.0);
            EXPECT_LE(m.value, 700.0);
        });
    }

    // Ground truth from independent Python simulation of the pinned
    // algorithm over the raw fixture.
    EXPECT_EQ(distinct_streams.size(), std::size_t{26});
    EXPECT_EQ(total, std::size_t{1124});
    EXPECT_GT(total, std::size_t{500}) << "floor: plenty of messages emitted";
}

TEST(Adsb, EntireFixtureSmallCapBoundsDistinctStreamsAndTotal) {
    auto lines = telemetry::test::LoadFixtureLines("adsb_fi_raw.jsonl");

    AdsbHandler handler(kBaseStreamId, /*max_streams=*/5, AdsbMetric::kAltBaro);

    std::set<std::uint32_t> distinct_streams;
    std::size_t total = 0;
    for (const auto& raw : lines) {
        handler.parse_response(raw, [&](const Message& m) {
            ++total;
            distinct_streams.insert(m.stream_id);
            EXPECT_GE(m.stream_id, kBaseStreamId);
            EXPECT_LT(m.stream_id, kBaseStreamId + 5);
        });
    }

    // Ground truth from independent Python simulation with max_streams=5.
    EXPECT_EQ(distinct_streams.size(), std::size_t{5});
    EXPECT_EQ(total, std::size_t{291});
}

}  // namespace
