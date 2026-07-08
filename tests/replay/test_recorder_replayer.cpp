// tests/replay/test_recorder_replayer.cpp
//
// Tests derived from SPEC.md section 3.9 (replay + fault injection), section
// 7 phase 3. NO implementation exists yet under include/replay/ -- this is
// the RED state (build must fail on missing headers, not on logic).
//
// This file links telemetry_feed (not just telemetry_core) because it
// exercises the round-trip requirement with a mix of synthetic Messages AND
// real ones parsed from the recorded Coinbase fixture via
// CoinbaseTickerHandler (see tests/CMakeLists.txt, following the same
// linking pattern as telemetry_add_feed_test).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/replay/recorder.hpp, include/replay/replayer.hpp,
// neither of which exist yet):
//
//   namespace telemetry::replay {
//     inline constexpr char kMagic[4] = {'T', 'R', 'E', 'C'};
//     inline constexpr std::uint32_t kFormatVersion = 1;
//
//     class StreamRecorder {
//     public:
//       explicit StreamRecorder(const std::string& path) noexcept;
//       bool is_open() const noexcept;
//       bool write(const Message& msg) noexcept;      // appends one record
//       std::size_t count() const noexcept;            // # records written
//       void close() noexcept;                          // flush + finalize
//       ~StreamRecorder();                              // closes if open
//     };
//
//     class StreamReplayer {
//     public:
//       enum class Status : std::uint8_t {
//         kOk, kFileNotFound, kBadMagic, kUnsupportedVersion
//       };
//       explicit StreamReplayer(const std::string& path) noexcept;
//       Status status() const noexcept;
//       bool next(Message& out) noexcept;               // pull one message
//       std::size_t messages_replayed() const noexcept;
//       bool truncated() const noexcept;
//     };
//   }
//
// FILE FORMAT (pinned so a garbage/corrupt file can be detected cleanly --
// see test_replay_errors.cpp): an 8-byte header {char magic[4] = "TREC";
// uint32_t version = 1;} followed by zero or more 64-byte raw Message
// records in write order. StreamRecorder writes the header on construction
// (if is_open()) and appends one 64-byte record per write().
//
// Spec requirements covered in this file:
//   SPEC-3.9-ROUNDTRIP: recording N messages (synthetic + real, parsed via
//     CoinbaseTickerHandler from the Coinbase fixture) and replaying them
//     back yields exact field-level equality (stream_id, ts_ns, value, seq)
//     AND preserves order.
// ---------------------------------------------------------------------------

#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <feed/coinbase.hpp>
#include <feed/handler.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_utils.hpp"

namespace {

using telemetry::Message;
using telemetry::feed::CoinbaseTickerHandler;
using telemetry::feed::ParseResult;
using telemetry::replay::StreamRecorder;
using telemetry::replay::StreamReplayer;

std::string TempPath(const std::string& suffix) {
    const std::string test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    return (std::filesystem::path(::testing::TempDir()) /
            ("replay_test_" + test_name + "_" + suffix))
        .string();
}

Message MakeMessage(std::uint32_t stream_id, std::int64_t ts_ns, double value,
                     std::uint64_t seq) {
    Message m{};
    m.stream_id = stream_id;
    m.ts_ns = ts_ns;
    m.value = value;
    m.seq = seq;
    return m;
}

void ExpectFieldsEqual(const Message& a, const Message& b) {
    EXPECT_EQ(a.stream_id, b.stream_id);
    EXPECT_EQ(a.ts_ns, b.ts_ns);
    EXPECT_DOUBLE_EQ(a.value, b.value);
    EXPECT_EQ(a.seq, b.seq);
}

std::vector<Message> RealCoinbaseTickerMessages() {
    auto lines = telemetry::test::LoadFixtureLines(telemetry::test::kCoinbaseFixture);
    std::unordered_map<std::string, std::uint32_t> product_to_stream{
        {"BTC-USD", 7u}, {"ETH-USD", 9u}};
    CoinbaseTickerHandler handler(product_to_stream);
    std::vector<Message> out;
    for (const auto& line : lines) {
        Message m{};
        if (handler.parse(line, m) == ParseResult::kOk) {
            out.push_back(m);
        }
    }
    return out;
}

// ---------------------------------------------------------------------------
// SPEC-3.9-ROUNDTRIP: synthetic messages only.
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, SyntheticMessagesRoundTripExactlyInOrder) {
    const std::string path = TempPath("synthetic.trec");
    std::vector<Message> written = {
        MakeMessage(1, 100, 1.5, 1),
        MakeMessage(1, 200, 2.5, 2),
        MakeMessage(2, 150, -9.75, 1),
        MakeMessage(1, 300, 3.5, 3),
    };

    {
        StreamRecorder rec(path);
        ASSERT_TRUE(rec.is_open());
        for (const auto& m : written) {
            EXPECT_TRUE(rec.write(m));
        }
        EXPECT_EQ(rec.count(), written.size());
        rec.close();
    }

    StreamReplayer replayer(path);
    ASSERT_EQ(replayer.status(), StreamReplayer::Status::kOk);

    std::vector<Message> read_back;
    Message out{};
    while (replayer.next(out)) {
        read_back.push_back(out);
    }

    ASSERT_EQ(read_back.size(), written.size());
    for (std::size_t i = 0; i < written.size(); ++i) {
        SCOPED_TRACE(i);
        ExpectFieldsEqual(read_back[i], written[i]);
    }
    EXPECT_EQ(replayer.messages_replayed(), written.size());
}

// ---------------------------------------------------------------------------
// SPEC-3.9-ROUNDTRIP: real messages parsed from the Coinbase fixture via
// CoinbaseTickerHandler.
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, RealCoinbaseMessagesRoundTripExactlyInOrder) {
    std::vector<Message> written = RealCoinbaseTickerMessages();
    ASSERT_GE(written.size(), std::size_t{2});

    const std::string path = TempPath("real.trec");
    {
        StreamRecorder rec(path);
        ASSERT_TRUE(rec.is_open());
        for (const auto& m : written) {
            ASSERT_TRUE(rec.write(m));
        }
        rec.close();
    }

    StreamReplayer replayer(path);
    ASSERT_EQ(replayer.status(), StreamReplayer::Status::kOk);

    std::vector<Message> read_back;
    Message out{};
    while (replayer.next(out)) {
        read_back.push_back(out);
    }

    ASSERT_EQ(read_back.size(), written.size());
    for (std::size_t i = 0; i < written.size(); ++i) {
        SCOPED_TRACE(i);
        ExpectFieldsEqual(read_back[i], written[i]);
    }
}

// ---------------------------------------------------------------------------
// SPEC-3.9-ROUNDTRIP: mixed synthetic + real, interleaved, order preserved.
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, MixedSyntheticAndRealPreserveOrder) {
    std::vector<Message> real = RealCoinbaseTickerMessages();
    ASSERT_GE(real.size(), std::size_t{2});

    std::vector<Message> written;
    written.push_back(MakeMessage(100, 1, 0.0, 0));
    written.push_back(real[0]);
    written.push_back(MakeMessage(101, 2, -1.0, 1));
    written.push_back(real[1]);

    const std::string path = TempPath("mixed.trec");
    {
        StreamRecorder rec(path);
        for (const auto& m : written) {
            ASSERT_TRUE(rec.write(m));
        }
        rec.close();
    }

    StreamReplayer replayer(path);
    std::vector<Message> read_back;
    Message out{};
    while (replayer.next(out)) {
        read_back.push_back(out);
    }
    ASSERT_EQ(read_back.size(), written.size());
    for (std::size_t i = 0; i < written.size(); ++i) {
        SCOPED_TRACE(i);
        ExpectFieldsEqual(read_back[i], written[i]);
    }
}

// ---------------------------------------------------------------------------
// Edge case: an empty recorded stream (header only, zero records) replays
// zero messages without error.
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, EmptyStreamReplaysZeroMessages) {
    const std::string path = TempPath("empty.trec");
    {
        StreamRecorder rec(path);
        ASSERT_TRUE(rec.is_open());
        EXPECT_EQ(rec.count(), std::size_t{0});
        rec.close();
    }

    StreamReplayer replayer(path);
    ASSERT_EQ(replayer.status(), StreamReplayer::Status::kOk);
    Message out{};
    EXPECT_FALSE(replayer.next(out));
    EXPECT_EQ(replayer.messages_replayed(), std::size_t{0});
}

// ---------------------------------------------------------------------------
// Recorder.count() tracks exactly the number of successful writes.
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, RecorderCountMatchesNumberOfWrites) {
    const std::string path = TempPath("count.trec");
    StreamRecorder rec(path);
    EXPECT_EQ(rec.count(), std::size_t{0});
    rec.write(MakeMessage(1, 1, 1.0, 1));
    EXPECT_EQ(rec.count(), std::size_t{1});
    rec.write(MakeMessage(1, 2, 2.0, 2));
    EXPECT_EQ(rec.count(), std::size_t{2});
    rec.close();
}

// ---------------------------------------------------------------------------
// After all messages are consumed, next() keeps returning false (does not
// wrap around, crash, or resurrect stale data in `out`).
// ---------------------------------------------------------------------------
TEST(RecorderReplayerRoundTrip, NextReturnsFalseRepeatedlyAfterExhaustion) {
    const std::string path = TempPath("exhaustion.trec");
    {
        StreamRecorder rec(path);
        rec.write(MakeMessage(1, 1, 1.0, 1));
        rec.close();
    }
    StreamReplayer replayer(path);
    Message out{};
    EXPECT_TRUE(replayer.next(out));
    EXPECT_FALSE(replayer.next(out));
    EXPECT_FALSE(replayer.next(out));
    EXPECT_EQ(replayer.messages_replayed(), std::size_t{1});
}

}  // namespace
