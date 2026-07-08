// tests/feed/test_coinbase_parse.cpp
//
// Tests derived from SPEC.md section 3.4 (feed/handler) applied to the
// crypto (Coinbase) adapter, section 7 phase 2. NO implementation exists
// yet under include/feed/ — this is the RED state (build must fail on
// missing headers, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/feed/handler.hpp, include/feed/coinbase.hpp,
// neither of which exist yet):
//
//   namespace telemetry::feed {
//     enum class ParseResult : uint8_t { kOk, kHeartbeat, kSkipped, kMalformed };
//     using GapCallback = std::function<void(uint32_t stream_id,
//                                             uint64_t from, uint64_t to)>;
//     class FeedHandler {
//     public:
//       explicit FeedHandler(GapCallback on_gap = nullptr) noexcept;
//       virtual ~FeedHandler() = default;
//       virtual ParseResult parse(std::string_view raw, Message& out) noexcept = 0;
//       template<typename RingBuffer> bool handle(std::string_view, RingBuffer&) noexcept;
//       void on_disconnect() noexcept;
//       void on_reconnect() noexcept;
//       void reset() noexcept;
//     };
//     class CoinbaseTickerHandler : public FeedHandler {
//     public:
//       CoinbaseTickerHandler(std::unordered_map<std::string, uint32_t> product_to_stream,
//                              GapCallback on_gap = nullptr);
//       ParseResult parse(std::string_view raw, Message& out) noexcept override;
//     };
//   }
//
// DEVIATION FROM SPEC-3.4 LITERAL SIGNATURE:
//   SPEC-3.4 pins `virtual Message parse(std::string_view raw) = 0;` which
//   cannot signal parse failure without throwing, and CLAUDE.md / the
//   cpp-conventions skill forbid exceptions on the hot path. Pinned instead:
//   `ParseResult parse(std::string_view raw, Message& out) noexcept`, an
//   error-code/out-param style consistent with SPEC-3.5's `RuleResult check(...)`
//   pattern used elsewhere in the same spec.
//
// DEVIATION / EXTENSION: ParseResult has FOUR variants, not a bool. `kOk`
// (ticker -> full Message with a real tick value), `kHeartbeat` (heartbeat ->
// Message.stream_id/seq populated for gap tracking ONLY, no tick value, must
// not be pushed to the ring buffer -- see SPEC-3.4 gap-detection requirement
// "heartbeat sequence numbers also participate in gap detection"), `kSkipped`
// (recognized-but-irrelevant, e.g. "subscriptions" ack, or a ticker for an
// unmapped product_id), `kMalformed` (invalid JSON / missing required fields).
//
// SEQUENCE-FIELD DEVIATION (researched + independently verified against the
// real fixture, see docs/feeds/coinbase_ws.md): Coinbase's `sequence` field
// counts ALL events per product across the full feed; the ticker/heartbeat
// channels see only a SUBSET, so consecutive ticker messages normally have
// `sequence` deltas > 1 (verified: fixture deltas range from 2 to 2541,
// NEVER 1). Naive expected=last+1 gap detection on `sequence` would false-fire
// on every message. `trade_id`, however, increments by EXACTLY 1 per ticker
// message per product (verified: all 422 consecutive-ticker-pair deltas in
// the fixture equal 1). Therefore Message.seq is pinned to `trade_id` (not
// `sequence`), and heartbeat's `last_trade_id` feeds the same per-product
// gap-detection state (see test_gap_detection.cpp).
//
// Spec requirements covered in this file:
//   SPEC-3.4-PARSE-TICKER   : raw ticker line -> Message{stream_id, ts_ns,
//                             value, seq} with exact field values.
//   SPEC-3.4-PARSE-PRODUCT-MAP: stream_id comes from a product_id->stream_id
//                             mapping supplied at construction.
//   SPEC-3.4-PARSE-TS       : ISO8601-with-microseconds `time` field converts
//                             to exact epoch nanoseconds.
//   SPEC-3.4-PARSE-REJECT   : non-ticker (heartbeat, subscriptions) and
//                             malformed/unknown JSON are rejected without
//                             exceptions and without producing a pushed
//                             Message.
//   SPEC-3.4-PARSE-NOEXCEPT : parse() is noexcept (no exceptions on the
//                             hot path, per CLAUDE.md / cpp-conventions).
// ---------------------------------------------------------------------------

#include <feed/coinbase.hpp>
#include <feed/handler.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <string>
#include <type_traits>
#include <unordered_map>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

namespace {

using telemetry::Message;
using telemetry::feed::CoinbaseTickerHandler;
using telemetry::feed::ParseResult;

std::unordered_map<std::string, std::uint32_t> DefaultProductMap() {
    return {{"BTC-USD", 7u}, {"ETH-USD", 9u}};
}

// ---------------------------------------------------------------------------
// SPEC-3.4-PARSE-NOEXCEPT
// ---------------------------------------------------------------------------
TEST(CoinbaseParseNoexcept, ParseIsNoexcept) {
    EXPECT_TRUE(noexcept(std::declval<CoinbaseTickerHandler&>().parse(
        std::string_view{}, std::declval<Message&>())));
}

// ---------------------------------------------------------------------------
// SPEC-3.4-PARSE-TICKER / SPEC-3.4-PARSE-PRODUCT-MAP / SPEC-3.4-PARSE-TS
//
// Fixture line 2 (1-based): a real BTC-USD ticker message.
//   trade_id=1052192039, price="62975.91",
//   time="2026-07-08T01:56:01.617651Z" -> exactly 1783475761617651000 ns
//   (computed independently via Python datetime against the Unix epoch;
//   see PLAN.md/session notes for the derivation).
// ---------------------------------------------------------------------------
TEST(CoinbaseParseTicker, BtcTickerParsesExactFields) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    ASSERT_GE(lines.size(), std::size_t{2});
    const std::string& raw = lines[1];  // line 2, 0-indexed
    ASSERT_NE(raw.find("\"type\":\"ticker\""), std::string::npos);
    ASSERT_NE(raw.find("\"product_id\":\"BTC-USD\""), std::string::npos);

    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r = handler.parse(raw, out);

    ASSERT_EQ(r, ParseResult::kOk);
    EXPECT_EQ(out.stream_id, 7u);
    EXPECT_EQ(out.seq, 1052192039ull);          // trade_id, NOT sequence
    EXPECT_DOUBLE_EQ(out.value, 62975.91);      // price parsed as double
    EXPECT_EQ(out.ts_ns, 1783475761617651000LL);
}

TEST(CoinbaseParseTicker, EthTickerParsesExactFields) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    ASSERT_GE(lines.size(), std::size_t{3});
    const std::string& raw = lines[2];  // line 3
    ASSERT_NE(raw.find("\"type\":\"ticker\""), std::string::npos);
    ASSERT_NE(raw.find("\"product_id\":\"ETH-USD\""), std::string::npos);

    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r = handler.parse(raw, out);

    ASSERT_EQ(r, ParseResult::kOk);
    EXPECT_EQ(out.stream_id, 9u);
    EXPECT_EQ(out.seq, 825499712ull);           // trade_id, NOT sequence
    EXPECT_DOUBLE_EQ(out.value, 1753.68);
    EXPECT_EQ(out.ts_ns, 1783475761380254000LL);
}

// Different construction-time mapping must produce different stream_ids for
// identical raw input, proving stream_id is derived from the supplied map
// and not hardcoded.
TEST(CoinbaseParseTicker, StreamIdComesFromConstructorMapping) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    const std::string& raw = lines[1];  // BTC-USD ticker

    std::unordered_map<std::string, std::uint32_t> custom_map{{"BTC-USD", 42u}, {"ETH-USD", 43u}};
    CoinbaseTickerHandler handler(custom_map);
    Message out{};
    ASSERT_EQ(handler.parse(raw, out), ParseResult::kOk);
    EXPECT_EQ(out.stream_id, 42u);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-PARSE-REJECT: heartbeat is not a tick -- kHeartbeat, not kOk.
// Fixture line 4: a real ETH-USD heartbeat.
//   last_trade_id=825499712, time="2026-07-08T01:56:02.000000Z"
//   -> exactly 1783475762000000000 ns.
// ---------------------------------------------------------------------------
TEST(CoinbaseParseReject, HeartbeatIsNotOkAndCarriesGapTrackingFields) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    ASSERT_GE(lines.size(), std::size_t{4});
    const std::string& raw = lines[3];  // line 4
    ASSERT_NE(raw.find("\"type\":\"heartbeat\""), std::string::npos);
    ASSERT_NE(raw.find("\"product_id\":\"ETH-USD\""), std::string::npos);

    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r = handler.parse(raw, out);

    ASSERT_EQ(r, ParseResult::kHeartbeat);
    EXPECT_EQ(out.stream_id, 9u);
    EXPECT_EQ(out.seq, 825499712ull);  // last_trade_id feeds gap-detection state
}

// ---------------------------------------------------------------------------
// SPEC-3.4-PARSE-REJECT: "subscriptions" ack (fixture line 1) is skipped.
// ---------------------------------------------------------------------------
TEST(CoinbaseParseReject, SubscriptionsAckIsSkipped) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    ASSERT_GE(lines.size(), std::size_t{1});
    const std::string& raw = lines[0];  // line 1
    ASSERT_NE(raw.find("\"type\":\"subscriptions\""), std::string::npos);

    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r = handler.parse(raw, out);
    EXPECT_EQ(r, ParseResult::kSkipped);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-PARSE-REJECT: malformed / non-JSON input never throws and never
// reports kOk.
// ---------------------------------------------------------------------------
TEST(CoinbaseParseReject, TruncatedJsonIsMalformedNotThrown) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse("{\"type\":\"ticker\",\"product_id\":", out));
    EXPECT_EQ(r, ParseResult::kMalformed);
}

TEST(CoinbaseParseReject, NonJsonGarbageIsMalformedNotThrown) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse("not json at all !!!", out));
    EXPECT_EQ(r, ParseResult::kMalformed);
}

TEST(CoinbaseParseReject, EmptyStringIsMalformedNotThrown) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse("", out));
    EXPECT_EQ(r, ParseResult::kMalformed);
}

TEST(CoinbaseParseReject, TickerMissingPriceFieldIsMalformed) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    const std::string raw =
        R"({"type":"ticker","sequence":1,"product_id":"BTC-USD",)"
        R"("time":"2026-07-08T01:56:01.617651Z","trade_id":1})";
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse(raw, out));
    EXPECT_EQ(r, ParseResult::kMalformed);
}

TEST(CoinbaseParseReject, ValidJsonUnknownTypeIsSkipped) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    const std::string raw = R"({"type":"snapshot","product_id":"BTC-USD"})";
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse(raw, out));
    EXPECT_EQ(r, ParseResult::kSkipped);
}

// A ticker for a product NOT present in the constructor's mapping is
// unroutable (no stream_id) -- must be skipped, not fabricated as stream 0.
TEST(CoinbaseParseReject, TickerForUnmappedProductIsSkipped) {
    CoinbaseTickerHandler handler(DefaultProductMap());
    Message out{};
    const std::string raw =
        R"({"type":"ticker","sequence":1,"product_id":"LTC-USD","price":"70.5",)"
        R"("time":"2026-07-08T01:56:01.617651Z","trade_id":1})";
    ParseResult r{};
    EXPECT_NO_THROW(r = handler.parse(raw, out));
    EXPECT_EQ(r, ParseResult::kSkipped);
}

} // namespace
