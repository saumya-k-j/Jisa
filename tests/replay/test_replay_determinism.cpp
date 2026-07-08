// tests/replay/test_replay_determinism.cpp
//
// Tests derived from SPEC.md section 1 measurable objective #5
// ("Determinism: identical checksummed end-state on replay of a recorded
// stream"), section 3.9, and section 4 ("Replay-determinism test in CI
// (checksum match)"). NO implementation exists yet under include/replay/ --
// this is the RED state (build must fail on missing headers, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface: see include/replay/recorder.hpp, include/replay/replayer.hpp,
// include/replay/checksum.hpp (documented fully in test_recorder_replayer.cpp
// and test_checksum.cpp; not repeated here).
//
// PINNED REPLAY MODE: StreamReplayer is constructible from ONLY a path
// (`explicit StreamReplayer(const std::string&)`) -- i.e. "as-fast-as-
// possible" iteration is the only mode in scope; there is no timing/pacing
// parameter to pin or test (see task instructions: timing-paced modes are
// out of test scope). Iteration is single-threaded and pull-based via
// next(Message&) -- not callback-based (that choice is pinned by this file
// and by test_recorder_replayer.cpp).
//
// Spec requirements covered in this file:
//   SPEC-3.9-DETERMINISM: replaying the same recorded file twice yields a
//     byte-identical (here: checksum-identical, covering every replayed
//     message's stream_id/ts_ns/value/seq) end state.
//   SPEC-3.9-REPLAY-MODE: replay is single-threaded, deterministic, pull-based
//     iteration; "as-fast-as-possible" (no timing parameter) is the default
//     and only in-scope mode.
// ---------------------------------------------------------------------------

#include <replay/checksum.hpp>
#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <filesystem>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::replay::StreamChecksum;
using telemetry::replay::StreamRecorder;
using telemetry::replay::StreamReplayer;

std::string TempPath(const std::string& suffix) {
    const std::string test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    return (std::filesystem::path(::testing::TempDir()) /
            ("replay_determinism_" + test_name + "_" + suffix))
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

std::string RecordFixtureStream(const std::string& path) {
    StreamRecorder rec(path);
    for (std::uint64_t i = 0; i < 50; ++i) {
        rec.write(MakeMessage(static_cast<std::uint32_t>(i % 3), 1000 + static_cast<std::int64_t>(i),
                               static_cast<double>(i) * 1.25, i));
    }
    rec.close();
    return path;
}

std::uint64_t ChecksumEntireStream(const std::string& path) {
    StreamReplayer replayer(path);
    StreamChecksum cs;
    Message m{};
    while (replayer.next(m)) {
        cs.update(m);
    }
    return cs.digest();
}

// ---------------------------------------------------------------------------
// SPEC-3.9-DETERMINISM: replaying the same file twice (two independent
// StreamReplayer instances, no shared state) produces an identical checksum
// covering the full replayed content.
// ---------------------------------------------------------------------------
TEST(ReplayDeterminism, ReplayingSameFileTwiceYieldsIdenticalChecksum) {
    const std::string path = TempPath("stream.trec");
    RecordFixtureStream(path);

    const std::uint64_t digest_first = ChecksumEntireStream(path);
    const std::uint64_t digest_second = ChecksumEntireStream(path);

    EXPECT_EQ(digest_first, digest_second);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-DETERMINISM: a checksum computed while interleaving replay calls
// with other work (i.e. no timing dependence) still matches a checksum
// computed via straight-line iteration -- pull-based replay has no implicit
// clock/timer coupling.
// ---------------------------------------------------------------------------
TEST(ReplayDeterminism, PullBasedReplayHasNoTimingDependence) {
    const std::string path = TempPath("no_timing.trec");
    RecordFixtureStream(path);

    StreamReplayer replayer(path);
    StreamChecksum cs;
    Message m{};
    int i = 0;
    while (replayer.next(m)) {
        cs.update(m);
        // Simulate uneven caller-side pacing between pulls; must not affect
        // the deterministic digest since StreamReplayer has no timer.
        if (i % 7 == 0) {
            volatile int busy = 0;
            for (int k = 0; k < 1000; ++k) busy += k;
        }
        ++i;
    }

    EXPECT_EQ(cs.digest(), ChecksumEntireStream(path));
}

// ---------------------------------------------------------------------------
// SPEC-3.9-REPLAY-MODE: StreamReplayer's only required constructor takes
// just a path -- "as-fast-as-possible" is the default with no timing
// parameter to configure.
// ---------------------------------------------------------------------------
TEST(ReplayDeterminism, ReplayerIsConstructibleFromPathAlone) {
    static_assert(std::is_constructible_v<StreamReplayer, const std::string&>);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-REPLAY-MODE: iteration order via next() is exactly recorded
// order across a full pass, and repeating the pass from a fresh instance
// reproduces the identical sequence of stream_id/seq pairs.
// ---------------------------------------------------------------------------
TEST(ReplayDeterminism, IterationOrderIsIdenticalAcrossFreshInstances) {
    const std::string path = TempPath("order.trec");
    RecordFixtureStream(path);

    auto collect_seqs = [&]() {
        StreamReplayer replayer(path);
        std::vector<std::pair<std::uint32_t, std::uint64_t>> seen;
        Message m{};
        while (replayer.next(m)) {
            seen.emplace_back(m.stream_id, m.seq);
        }
        return seen;
    };

    const auto pass1 = collect_seqs();
    const auto pass2 = collect_seqs();
    ASSERT_EQ(pass1.size(), std::size_t{50});
    EXPECT_EQ(pass1, pass2);
}

}  // namespace
