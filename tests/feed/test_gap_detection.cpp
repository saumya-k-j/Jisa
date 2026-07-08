// tests/feed/test_gap_detection.cpp
//
// Tests derived from SPEC.md section 3.4: "Detects sequence gaps (missing
// seq) and handles reconnect/resync." NO implementation exists yet under
// include/feed/ -- this is the RED state.
//
// These tests exercise FeedHandler::handle()'s gap-detection logic in
// isolation from Coinbase-specific JSON parsing, via a small test-only fake
// adapter (FakeHandler below) that returns pre-canned ParseResult/Message
// pairs. This keeps the pinned gap-detection semantics testable without
// depending on real JSON strings, per SPEC-3.4's generic (per-adapter)
// framing: "detects sequence gaps" is a FeedHandler-base-class concern, not
// a Coinbase-specific one.
//
// PINNED SEMANTICS (see also test_coinbase_parse.cpp's deviation note on
// Message.seq == trade_id, not Coinbase's `sequence` field):
//   Let last = last-accepted seq for stream_id (absent on first message ever
//   seen for that stream_id -- no gap check fires on the first message).
//     seq <= last            : duplicate / out-of-order-older -> DROP.
//                               No on_gap call. Not pushed to ring buffer.
//     seq == last + 1        : contiguous -> accept, no gap.
//     seq >  last + 1        : gap -> on_gap(stream_id, /*from=*/last,
//                               /*to=*/seq) fires exactly once, then the
//                               message *is still accepted* (a gap is a
//                               notification of missing data in between, not
//                               a reason to discard the new, valid message).
//   Heartbeat (ParseResult::kHeartbeat) messages carry a seq (last_trade_id
//   for Coinbase) that participates in the SAME per-stream_id gap-detection
//   state as ticker (kOk) messages, but a heartbeat itself is never pushed
//   to the ring buffer (handle() returns false for it).
//
// Spec requirements covered in this file:
//   SPEC-3.4-GAP-BASIC      : gap (e.g. last=101, incoming=105) invokes
//                             on_gap(from=101, to=105) exactly once.
//   SPEC-3.4-GAP-CONTIGUOUS : contiguous seq stream never invokes on_gap.
//   SPEC-3.4-GAP-DUP        : seq == last is dropped silently (no on_gap,
//                             not pushed).
//   SPEC-3.4-GAP-OLDER      : seq < last (out-of-order/older) is dropped
//                             silently (no on_gap, not pushed).
//   SPEC-3.4-GAP-PERSTREAM  : gap state is tracked independently per
//                             stream_id; a gap on one stream does not affect
//                             another.
//   SPEC-3.4-GAP-HEARTBEAT  : heartbeat seq (last_trade_id) participates in
//                             the same per-product gap-detection state as
//                             ticker seq (trade_id), and can itself trigger
//                             on_gap, without being pushed as a Message.
// ---------------------------------------------------------------------------

#include <feed/handler.hpp>

#include <core/message.hpp>
#include <core/ring_buffer.hpp>

#include <cstdint>
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

// A canned (result, stream_id, seq) triple; value/ts_ns are irrelevant to
// gap-detection tests and left at 0.
struct CannedEvent {
    ParseResult result;
    std::uint32_t stream_id;
    std::uint64_t seq;
};

// Test-only fake adapter: ignores the raw string entirely and returns the
// next canned event in sequence. This isolates gap-detection logic (a
// FeedHandler base-class concern per SPEC-3.4) from JSON parsing.
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

using GapCall = std::tuple<std::uint32_t, std::uint64_t, std::uint64_t>;  // stream_id, from, to

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-CONTIGUOUS
// ---------------------------------------------------------------------------
TEST(GapDetection, ContiguousSequenceNeverFiresGap) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {{ParseResult::kOk, 1, 100}, {ParseResult::kOk, 1, 101}, {ParseResult::kOk, 1, 102}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(handler.handle("ignored", buf));
    }
    EXPECT_TRUE(gaps.empty());

    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 3);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-BASIC: 100, 101, 105 -> on_gap(from=101, to=105) exactly once.
// The gapped message (105) is still accepted and pushed.
// ---------------------------------------------------------------------------
TEST(GapDetection, GapFiresWithCorrectFromAndTo) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {{ParseResult::kOk, 1, 100}, {ParseResult::kOk, 1, 101}, {ParseResult::kOk, 1, 105}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));   // 100: first ever, no check
    EXPECT_TRUE(handler.handle("ignored", buf));   // 101: contiguous
    EXPECT_TRUE(handler.handle("ignored", buf));   // 105: gap, but still pushed

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<0>(gaps[0]), 1u);
    EXPECT_EQ(std::get<1>(gaps[0]), 101ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 105ull);

    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 3);  // gap does not drop the new message
}

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-DUP: exact duplicate seq is dropped, no on_gap, not pushed.
// ---------------------------------------------------------------------------
TEST(GapDetection, DuplicateSeqIsDroppedSilently) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {{ParseResult::kOk, 1, 100}, {ParseResult::kOk, 1, 101}, {ParseResult::kOk, 1, 101}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));    // 100
    EXPECT_TRUE(handler.handle("ignored", buf));    // 101
    EXPECT_FALSE(handler.handle("ignored", buf));   // 101 again: dropped

    EXPECT_TRUE(gaps.empty());

    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 2);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-OLDER: out-of-order older seq (after a gap jump) is dropped,
// no on_gap, not pushed.
// ---------------------------------------------------------------------------
TEST(GapDetection, OutOfOrderOlderSeqIsDroppedSilently) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {{ParseResult::kOk, 1, 100}, {ParseResult::kOk, 1, 105}, {ParseResult::kOk, 1, 103}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));    // 100: first, no check
    EXPECT_TRUE(handler.handle("ignored", buf));    // 105: gap fires once
    EXPECT_FALSE(handler.handle("ignored", buf));   // 103 < 105: dropped, no 2nd gap

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<1>(gaps[0]), 100ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 105ull);

    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 2);  // 100 and 105 only; 103 dropped
}

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-PERSTREAM: independent gap state per stream_id.
// ---------------------------------------------------------------------------
TEST(GapDetection, GapStateIsIndependentPerStream) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kOk, 1, 100},  // stream 1: first
            {ParseResult::kOk, 2, 500},  // stream 2: first (different stream, no gap even
                                         // though 500 != 100+1 -- independent state)
            {ParseResult::kOk, 1, 101},  // stream 1: contiguous
            {ParseResult::kOk, 2, 510},  // stream 2: gap (500 -> 510)
            {ParseResult::kOk, 1, 200},  // stream 1: gap (101 -> 200)
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    for (int i = 0; i < 5; ++i) {
        handler.handle("ignored", buf);
    }

    ASSERT_EQ(gaps.size(), std::size_t{2});
    EXPECT_EQ(std::get<0>(gaps[0]), 2u);
    EXPECT_EQ(std::get<1>(gaps[0]), 500ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 510ull);
    EXPECT_EQ(std::get<0>(gaps[1]), 1u);
    EXPECT_EQ(std::get<1>(gaps[1]), 101ull);
    EXPECT_EQ(std::get<2>(gaps[1]), 200ull);
}

// ---------------------------------------------------------------------------
// SPEC-3.4-GAP-HEARTBEAT: heartbeat seq (last_trade_id) participates in the
// same gap-detection state as ticker seq (trade_id); heartbeat itself is
// never pushed.
// ---------------------------------------------------------------------------
TEST(GapDetection, HeartbeatParticipatesInGapDetectionButIsNeverPushed) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kOk, 1, 200},         // ticker, trade_id 200
            {ParseResult::kHeartbeat, 1, 205},  // heartbeat, last_trade_id 205: gap!
            {ParseResult::kOk, 1, 206},         // ticker, trade_id 206: contiguous after 205
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_TRUE(handler.handle("ignored", buf));    // ticker 200: pushed
    EXPECT_FALSE(handler.handle("ignored", buf));   // heartbeat 205: gap fires, not pushed
    EXPECT_TRUE(handler.handle("ignored", buf));    // ticker 206: contiguous, pushed

    ASSERT_EQ(gaps.size(), std::size_t{1});
    EXPECT_EQ(std::get<0>(gaps[0]), 1u);
    EXPECT_EQ(std::get<1>(gaps[0]), 200ull);
    EXPECT_EQ(std::get<2>(gaps[0]), 205ull);

    Message m;
    int popped = 0;
    while (buf.try_pop(m)) ++popped;
    EXPECT_EQ(popped, 2);  // ticker 200 and ticker 206 only, NOT the heartbeat
}

// A kSkipped/kMalformed event must not touch gap-detection state at all
// (e.g. must not be treated as seq 0).
TEST(GapDetection, SkippedAndMalformedDoNotAffectGapState) {
    std::vector<GapCall> gaps;
    FakeHandler handler(
        {
            {ParseResult::kSkipped, 1, 0},
            {ParseResult::kMalformed, 1, 0},
            {ParseResult::kOk, 1, 100},
            {ParseResult::kOk, 1, 101},
        },
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            gaps.emplace_back(sid, from, to);
        });

    SpscRingBuffer<Message, 16> buf;
    EXPECT_FALSE(handler.handle("ignored", buf));  // skipped
    EXPECT_FALSE(handler.handle("ignored", buf));  // malformed
    EXPECT_TRUE(handler.handle("ignored", buf));   // 100: first real message, no gap
    EXPECT_TRUE(handler.handle("ignored", buf));   // 101: contiguous

    EXPECT_TRUE(gaps.empty());
}

} // namespace
