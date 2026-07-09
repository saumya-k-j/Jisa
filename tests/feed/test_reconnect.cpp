// tests/feed/test_reconnect.cpp
//
// Tests derived from SPEC.md section 3.4: "handles reconnect/resync."
// NO implementation exists yet under include/feed/ -- this is the RED state.
//
// These are offline, state-machine-level tests: no real sockets are opened.
// A "disconnect" is simulated by directly calling the pinned lifecycle hooks;
// see PINNED INTERFACE below. Live WebSocket reconnection itself is
// needs-live-validation and explicitly out of scope for unit tests (per
// task instructions).
//
// PINNED INTERFACE (see include/feed/handler.hpp):
//   void FeedHandler::on_disconnect() noexcept;
//   void FeedHandler::on_reconnect() noexcept;  // resets ALL per-stream gap
//                                                // state (equivalent to reset())
//   void FeedHandler::reset() noexcept;         // same effect as on_reconnect();
//                                                // exposed directly so tests
//                                                // (and the implementation)
//                                                // can invoke the reset
//                                                // behavior without pretending
//                                                // a disconnect happened.
//
// Rationale for pinning on_reconnect()==reset(): the adapter's WebSocket
// client is a single connection covering all subscribed products (per
// docs/feeds/coinbase_ws.md); on resubscribe there is no way to know how
// many events were missed for any product, so the only sound behavior is to
// forget all per-stream expected-sequence state and let the first message
// after resubscribe seed a fresh baseline (no spurious gap), while a genuine
// gap AFTER that point still fires normally.
//
// Spec requirements covered in this file:
//   SPEC-3.4-RECONNECT-NO-SPURIOUS-GAP : first message after on_reconnect()
//                             for a stream never fires on_gap, even if its
//                             seq is far from the pre-disconnect last-seen
//                             value.
//   SPEC-3.4-RECONNECT-RESUME-DETECTION: a genuine gap AFTER the post-reconnect
//                             baseline message still fires on_gap normally.
//   SPEC-3.4-RECONNECT-ALL-STREAMS: reconnect resets state for ALL streams,
//                             not just one.
//   SPEC-3.4-RESET-ALIAS    : reset() alone (without on_disconnect() first)
//                             has the identical effect to on_reconnect().
// ---------------------------------------------------------------------------

#include <feed/coinbase.hpp>
#include <feed/handler.hpp>

#include <core/message.hpp>
#include <core/ring_buffer.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::SpscRingBuffer;
using telemetry::feed::FeedHandler;
using telemetry::feed::GapCallback;
using telemetry::feed::ParseResult;

struct CannedEvent {
    ParseResult result;
    std::uint32_t stream_id;
    std::uint64_t seq;
};

// Same fake adapter pattern as test_gap_detection.cpp: isolates the
// reconnect/resync state machine (a FeedHandler base-class concern) from
// JSON parsing.
class FakeHandler : public FeedHandler {
public:
    explicit FakeHandler(std::vector<CannedEvent> events, GapCallback on_gap = nullptr)
        : FeedHandler(std::move(on_gap)), events_(std::move(events)) {}

    ParseResult parse(std::string_view /*raw*/, Message& out) noexcept override {
        if (next_ >= events_.size()) {
            return ParseResult::kMalformed;
        }
        const CannedEvent& e = events_[next_++];
        out = Message{};
        out.stream_id = e.stream_id;
        out.seq = e.seq;
        return e.result;
    }

private:
    std::vector<CannedEvent> events_;
    std::size_t next_ = 0;
};

using GapCall = std::tuple<std::uint32_t, std::uint64_t, std::uint64_t>;

// ---------------------------------------------------------------------------
// SPEC-3.4-RECONNECT-NO-SPURIOUS-GAP / SPEC-3.4-RECONNECT-RESUME-DETECTION
// ---------------------------------------------------------------------------
TEST(Reconnect, ResetSuppressesOneSpuriousGapThenResumesDetection) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kOk, 1, 100},  // pre-disconnect
            {ParseResult::kOk, 1, 101},  // pre-disconnect, contiguous
            // -- simulated disconnect + reconnect happens here --
            {ParseResult::kOk, 1, 500},  // post-reconnect baseline: must NOT gap
            {ParseResult::kOk, 1, 505},  // genuine gap after baseline: MUST fire
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));  // 100
    EXPECT_TRUE(handler.handle("ignored", buf));  // 101

    handler.on_disconnect();
    handler.on_reconnect();

    EXPECT_TRUE(handler.handle("ignored", buf));  // 500: baseline, no gap expected
    EXPECT_TRUE(gaps.empty())
        << "first message after reconnect must not fire a spurious gap";

    EXPECT_TRUE(handler.handle("ignored", buf));  // 505: genuine gap after baseline

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<0>(gaps[0]), 1u);
    EXPECT_EQ(std::get<1>(gaps[0]), 500ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 505ull);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-RECONNECT-ALL-STREAMS
// ---------------------------------------------------------------------------
TEST(Reconnect, ResetClearsStateForAllStreamsNotJustOne) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kOk, 1, 100},
            {ParseResult::kOk, 2, 900},
            // -- reconnect --
            {ParseResult::kOk, 1, 300},  // far from 100, must not gap
            {ParseResult::kOk, 2, 50},   // far below 900 (and lower!), must not gap either:
                                         // post-reconnect the state is gone entirely, so
                                         // this is treated as a fresh first message.
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));  // stream 1: 100
    EXPECT_TRUE(handler.handle("ignored", buf));  // stream 2: 900

    handler.on_disconnect();
    handler.on_reconnect();

    EXPECT_TRUE(handler.handle("ignored", buf));  // stream 1: 300, no gap
    EXPECT_TRUE(handler.handle("ignored", buf));  // stream 2: 50, no gap

    EXPECT_TRUE(gaps.empty());
}

// ---------------------------------------------------------------------------
// SPEC-3.4-RESET-ALIAS: reset() alone (no on_disconnect()) has the same
// effect as on_reconnect().
// ---------------------------------------------------------------------------
TEST(Reconnect, ResetAloneHasSameEffectAsOnReconnect) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kOk, 1, 100},
            // -- reset() only, no on_disconnect() call --
            {ParseResult::kOk, 1, 900},
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));  // 100

    handler.reset();

    EXPECT_TRUE(handler.handle("ignored", buf));  // 900: must not gap after reset()
    EXPECT_TRUE(gaps.empty());
}

// A genuine gap must still be detectable entirely WITHOUT ever calling
// reset/on_disconnect/on_reconnect (baseline sanity check that this test
// file's fake isn't accidentally masking gap detection generally).
TEST(Reconnect, GapDetectionWorksNormallyWithoutAnyReconnectCalls) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {{ParseResult::kOk, 1, 100}, {ParseResult::kOk, 1, 110}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    handler.handle("ignored", buf);
    handler.handle("ignored", buf);

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<1>(gaps[0]), 100ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 110ull);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-RECONNECT on the REAL adapter (deployment coverage, 2026-07-08).
//
// The tests above pin the reconnect state machine on a FakeHandler; the state
// machine lives in the FeedHandler base, but nothing above proves the LIVE
// Coinbase adapter (CoinbaseTickerHandler) inherits it correctly end to end
// through real JSON parsing. That is exactly the path the 24/7 daemon takes
// on a feed drop, so it gets its own tests here. Live socket-level reconnect
// remains needs-live-validation (exercised by the deployment feed-drop check
// in VERIFICATION.md).
// ---------------------------------------------------------------------------

std::string ticker_json(const char* product, std::uint64_t trade_id, const char* price) {
    std::string s = R"({"type":"ticker","product_id":")";
    s += product;
    s += R"(","price":")";
    s += price;
    s += R"(","time":"2026-07-08T12:00:00.000000Z","trade_id":)";
    s += std::to_string(trade_id);
    s += "}";
    return s;
}

TEST(Reconnect, CoinbaseHandlerReconnectSuppressesSpuriousGapThenResumes) {
    std::vector<GapCall> gaps;
    telemetry::feed::CoinbaseTickerHandler handler(
        {{"BTC-USD", 1u}, {"ETH-USD", 2u}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 1000, "62000.10"), buf));
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 1001, "62000.20"), buf));
    EXPECT_TRUE(handler.handle(ticker_json("ETH-USD", 7000, "3400.00"), buf));

    // -- feed drop: what the live daemon does on Close/Error --
    handler.on_disconnect();
    handler.on_reconnect();

    // First message per stream after resubscribe: trade_ids far from the
    // pre-disconnect values must NOT fire a spurious gap.
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 5000, "62010.00"), buf));
    EXPECT_TRUE(handler.handle(ticker_json("ETH-USD", 100, "3401.00"), buf));
    EXPECT_TRUE(gaps.empty())
        << "real adapter fired a spurious gap after reconnect";

    // A genuine gap AFTER the fresh baseline must still fire.
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 5005, "62011.00"), buf));
    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<0>(gaps[0]), 1u);
    EXPECT_EQ(std::get<1>(gaps[0]), 5000ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 5005ull);

    // All six accepted ticks made it into the ring buffer.
    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 6);
}

TEST(Reconnect, CoinbaseHandlerHeartbeatSeedsBaselineAfterReconnect) {
    // After a reconnect, a HEARTBEAT (last_trade_id) may be the first event
    // seen for a product; it must seed the fresh baseline without a gap, and
    // the next ticker continues from it.
    std::vector<GapCall> gaps;
    telemetry::feed::CoinbaseTickerHandler handler(
        {{"BTC-USD", 1u}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 1000, "62000.10"), buf));

    handler.on_disconnect();
    handler.on_reconnect();

    const std::string hb =
        R"({"type":"heartbeat","product_id":"BTC-USD","last_trade_id":4999,)"
        R"("sequence":123456,"time":"2026-07-08T12:00:01.000000Z"})";
    EXPECT_FALSE(handler.handle(hb, buf));  // heartbeat: never pushed
    EXPECT_TRUE(gaps.empty()) << "heartbeat after reconnect fired a spurious gap";

    // Contiguous ticker after the heartbeat baseline: no gap.
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 5000, "62010.00"), buf));
    EXPECT_TRUE(gaps.empty());

    // Genuine gap relative to the heartbeat-seeded sequence still fires.
    EXPECT_TRUE(handler.handle(ticker_json("BTC-USD", 5010, "62011.00"), buf));
    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<1>(gaps[0]), 5000ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 5010ull);
}

} // namespace
