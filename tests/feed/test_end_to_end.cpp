// tests/feed/test_end_to_end.cpp
//
// Tests derived from SPEC.md section 3.4 (feed/handler) and section 7 phase
// 2: "feed/handler for ONE adapter (crypto) end-to-end + gap detection".
// NO implementation exists yet under include/feed/ -- this is the RED state.
//
// Drives the ENTIRE recorded fixture (500 real messages, tests/feed/fixtures/
// coinbase_ticker_raw.jsonl) through CoinbaseTickerHandler::handle() into a
// real telemetry::SpscRingBuffer<Message, N> (include/core/ring_buffer.hpp,
// a DONE Phase-1 dependency -- not re-tested here).
//
// Ground truth (computed directly from the fixture, independently of any
// implementation, via a one-off Python pass over the raw JSON lines):
//   - 500 total lines: 424 "ticker", 75 "heartbeat", 1 "subscriptions".
//   - Of the 424 ticker lines: 283 BTC-USD, 141 ETH-USD (both mapped below),
//     so ALL 424 ticker lines should produce a pushed Message.
//   - Per-product trade_id (== Message.seq, see test_coinbase_parse.cpp's
//     seq-field deviation note) is strictly increasing across the fixture
//     for both products (BTC-USD and ETH-USD each independently verified
//     strictly increasing over their 283 / 141 ticker messages).
//   - Under trade_id-based gap semantics, this fixture is a HEALTHY capture:
///    zero gap events occur across the whole file (verified independently
//     by replaying trade_id/last_trade_id transitions in Python: 0 gaps).
//
// Spec requirements covered in this file:
//   SPEC-3.4-E2E-COUNT      : number of Messages pushed == number of valid
//                             ticker lines for mapped products (424).
//   SPEC-3.4-E2E-MONOTONIC  : seq is strictly increasing per stream_id in
//                             the popped output.
//   SPEC-3.4-E2E-NO-LOSS    : no message lost when the ring buffer has
//                             adequate capacity (try_push never fails).
//   SPEC-3.4-E2E-NO-SPURIOUS-GAP: a healthy real capture produces zero
//                             on_gap calls (sanity check that gap detection
//                             isn't trigger-happy on real, valid data --
//                             this is what would catch a bug that used the
//                             `sequence` field instead of `trade_id`).
// ---------------------------------------------------------------------------

#include <feed/coinbase.hpp>
#include <feed/handler.hpp>

#include <core/message.hpp>
#include <core/ring_buffer.hpp>

#include <cstdint>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

namespace {

using telemetry::Message;
using telemetry::SpscRingBuffer;
using telemetry::feed::CoinbaseTickerHandler;

constexpr std::uint32_t kBtcStreamId = 1;
constexpr std::uint32_t kEthStreamId = 2;

std::unordered_map<std::string, std::uint32_t> ProductMap() {
    return {{"BTC-USD", kBtcStreamId}, {"ETH-USD", kEthStreamId}};
}

TEST(EndToEnd, EntireFixtureDrivesCountMonotonicityAndNoLoss) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    ASSERT_EQ(lines.size(), std::size_t{500});

    std::vector<std::tuple<std::uint32_t, std::uint64_t, std::uint64_t>> gaps;
    CoinbaseTickerHandler handler(ProductMap(),
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    // Capacity must be a power of two >= 424 pushed messages.
    SpscRingBuffer<Message, 512> buf;

    int pushed = 0;
    for (const auto& raw : lines) {
        if (handler.handle(raw, buf)) {
            ++pushed;
        }
    }

    // SPEC-3.4-E2E-COUNT
    EXPECT_EQ(pushed, 424);

    // SPEC-3.4-E2E-NO-SPURIOUS-GAP
    EXPECT_TRUE(gaps.empty())
        << "healthy real fixture should not report any sequence gaps "
           "under trade_id-based gap semantics";

    // SPEC-3.4-E2E-MONOTONIC / SPEC-3.4-E2E-NO-LOSS
    std::unordered_map<std::uint32_t, std::uint64_t> last_seq_per_stream;
    std::unordered_map<std::uint32_t, int> count_per_stream;
    Message m;
    int popped = 0;
    while (buf.try_pop(m)) {
        ++popped;
        ++count_per_stream[m.stream_id];
        auto it = last_seq_per_stream.find(m.stream_id);
        if (it != last_seq_per_stream.end()) {
            EXPECT_GT(m.seq, it->second)
                << "seq must be strictly increasing within stream_id " << m.stream_id;
        }
        last_seq_per_stream[m.stream_id] = m.seq;
    }

    EXPECT_EQ(popped, 424) << "no message lost: everything pushed must be poppable";
    EXPECT_EQ(count_per_stream[kBtcStreamId], 283);
    EXPECT_EQ(count_per_stream[kEthStreamId], 141);
}

// Only mapped products should ever be pushed; an adapter constructed with
// just ONE product mapped must silently skip the other product's tickers
// (kSkipped path in parse()), not crash / not fabricate stream_id 0.
TEST(EndToEnd, OnlyMappedProductIsPushedWhenOtherIsUnmapped) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);

    CoinbaseTickerHandler handler({{"BTC-USD", kBtcStreamId}});  // ETH-USD NOT mapped
    SpscRingBuffer<Message, 512> buf;

    int pushed = 0;
    for (const auto& raw : lines) {
        if (handler.handle(raw, buf)) {
            ++pushed;
        }
    }

    EXPECT_EQ(pushed, 283);  // BTC-USD ticker count only

    Message m;
    while (buf.try_pop(m)) {
        EXPECT_EQ(m.stream_id, kBtcStreamId);
    }
}

// If the ring buffer capacity is deliberately too small, try_push must
// simply fail (return false) for the overflow messages -- no crash, no UB,
// no exception -- exercising the SPSC ring buffer's documented full-buffer
// behavior (Phase-1 dependency) end-to-end through the feed handler.
TEST(EndToEnd, UndersizedBufferDropsOverflowWithoutCrashing) {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);

    CoinbaseTickerHandler handler(ProductMap());
    SpscRingBuffer<Message, 16> tiny_buf;  // far smaller than 424 ticker messages

    int pushed = 0;
    for (const auto& raw : lines) {
        if (handler.handle(raw, tiny_buf)) {
            ++pushed;
        }
    }

    EXPECT_LE(pushed, 16);
    EXPECT_GT(pushed, 0);
}

} // namespace
