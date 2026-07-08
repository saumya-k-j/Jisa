// tests/replay/test_replay_errors.cpp
//
// Tests derived from SPEC.md section 3.9 (replay + fault injection) and
// CLAUDE.md ("no exceptions on the hot path"). NO implementation exists yet
// under include/replay/ -- this is the RED state (build must fail on
// missing headers, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface: see include/replay/recorder.hpp, include/replay/replayer.hpp
// (documented fully in test_recorder_replayer.cpp; not repeated here).
//
// PINNED ERROR-HANDLING CONTRACT:
//   - StreamReplayer's constructor NEVER throws, even for a nonexistent
//     file, wrong magic, or unsupported version. Failure is reported via
//     status() (Status::kFileNotFound / kBadMagic / kUnsupportedVersion).
//   - next(Message&) NEVER throws, including on a truncated file (cut
//     mid-record). It returns false once no complete record remains.
//   - messages_replayed() reports exactly how many COMPLETE 64-byte records
//     were successfully replayed before EOF/truncation/error.
//   - truncated() is true iff the file ended with a partial (non-64-byte)
//     tail after the header, distinguishing "clean EOF at a record
//     boundary" from "cut mid-record".
//   - FILE FORMAT self-description: an 8-byte header {char magic[4] =
//     "TREC"; uint32_t version = 1 LE}. A file with the wrong magic, or a
//     magic-correct-but-unsupported version, is rejected cleanly (no crash,
//     no throw, no partial garbage treated as a Message).
//
// Spec requirements covered in this file:
//   SPEC-3.9-TRUNCATED-REPLAY: replaying a truncated file does not crash or
//     throw; reports how many complete messages were replayed.
//   SPEC-3.9-SELF-DESCRIBING-FORMAT: magic number + version header; garbage
//     / wrong-magic / unsupported-version input is rejected cleanly.
// ---------------------------------------------------------------------------

#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <core/message.hpp>

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <type_traits>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::replay::StreamRecorder;
using telemetry::replay::StreamReplayer;

std::string TempPath(const std::string& suffix) {
    const std::string test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    return (std::filesystem::path(::testing::TempDir()) /
            ("replay_errors_" + test_name + "_" + suffix))
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

// Truncates `path` to `new_size` bytes (simulating a connection/disk cut
// mid-write). Requires the file to already exist and be >= new_size.
void TruncateFileTo(const std::string& path, std::uintmax_t new_size) {
    std::filesystem::resize_file(path, new_size);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-TRUNCATED-REPLAY: file cut off partway through the 3rd of 5
// records. next() must not throw, must return exactly 2 complete messages,
// then false; messages_replayed() == 2; truncated() == true.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, TruncatedFileDoesNotThrowAndReportsPartialCount) {
    const std::string path = TempPath("truncated.trec");
    {
        StreamRecorder rec(path);
        for (std::uint64_t i = 0; i < 5; ++i) {
            rec.write(MakeMessage(1, static_cast<std::int64_t>(i), static_cast<double>(i), i));
        }
        rec.close();
    }

    // Header (8 bytes) + 2 full records (64 bytes each) + 30 stray bytes of
    // a 3rd record that never completes.
    const std::uintmax_t header_size = 8;
    const std::uintmax_t record_size = 64;
    const std::uintmax_t cut_at = header_size + 2 * record_size + 30;
    TruncateFileTo(path, cut_at);

    StreamReplayer replayer(path);
    ASSERT_EQ(replayer.status(), StreamReplayer::Status::kOk);

    Message out{};
    int replayed = 0;
    EXPECT_NO_THROW({
        while (replayer.next(out)) {
            ++replayed;
        }
    });

    EXPECT_EQ(replayed, 2);
    EXPECT_EQ(replayer.messages_replayed(), std::size_t{2});
    EXPECT_TRUE(replayer.truncated());
}

// ---------------------------------------------------------------------------
// A file truncated EXACTLY at a record boundary (no partial trailing bytes)
// is NOT reported as truncated -- it is a clean, complete (if short) stream.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, FileCutExactlyAtRecordBoundaryIsNotTruncated) {
    const std::string path = TempPath("clean_cut.trec");
    {
        StreamRecorder rec(path);
        for (std::uint64_t i = 0; i < 5; ++i) {
            rec.write(MakeMessage(1, static_cast<std::int64_t>(i), static_cast<double>(i), i));
        }
        rec.close();
    }
    const std::uintmax_t header_size = 8;
    const std::uintmax_t record_size = 64;
    TruncateFileTo(path, header_size + 3 * record_size);

    StreamReplayer replayer(path);
    Message out{};
    int replayed = 0;
    while (replayer.next(out)) {
        ++replayed;
    }
    EXPECT_EQ(replayed, 3);
    EXPECT_FALSE(replayer.truncated());
}

// ---------------------------------------------------------------------------
// SPEC-3.9-SELF-DESCRIBING-FORMAT: wrong magic number is rejected cleanly.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, WrongMagicIsRejectedCleanly) {
    const std::string path = TempPath("bad_magic.trec");
    {
        std::ofstream out(path, std::ios::binary);
        const char bad_magic[4] = {'X', 'X', 'X', 'X'};
        std::uint32_t version = 1;
        out.write(bad_magic, sizeof(bad_magic));
        out.write(reinterpret_cast<const char*>(&version), sizeof(version));
        // A full 64-byte record's worth of arbitrary payload -- must NOT be
        // interpreted as a Message once the magic check has failed.
        char payload[64] = {};
        out.write(payload, sizeof(payload));
    }

    StreamReplayer replayer(path);
    EXPECT_EQ(replayer.status(), StreamReplayer::Status::kBadMagic);

    Message out{};
    EXPECT_NO_THROW(EXPECT_FALSE(replayer.next(out)));
    EXPECT_EQ(replayer.messages_replayed(), std::size_t{0});
}

// ---------------------------------------------------------------------------
// SPEC-3.9-SELF-DESCRIBING-FORMAT: correct magic, unsupported version.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, UnsupportedVersionIsRejectedCleanly) {
    const std::string path = TempPath("bad_version.trec");
    {
        std::ofstream out(path, std::ios::binary);
        const char magic[4] = {'T', 'R', 'E', 'C'};
        std::uint32_t version = 9999;
        out.write(magic, sizeof(magic));
        out.write(reinterpret_cast<const char*>(&version), sizeof(version));
    }

    StreamReplayer replayer(path);
    EXPECT_EQ(replayer.status(), StreamReplayer::Status::kUnsupportedVersion);

    Message out{};
    EXPECT_NO_THROW(EXPECT_FALSE(replayer.next(out)));
    EXPECT_EQ(replayer.messages_replayed(), std::size_t{0});
}

// ---------------------------------------------------------------------------
// A nonexistent file is reported via status(), not an exception.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, NonexistentFileReturnsFileNotFoundStatus) {
    const std::string path = TempPath("does_not_exist.trec");
    ASSERT_FALSE(std::filesystem::exists(path));

    StreamReplayer replayer(path);
    EXPECT_EQ(replayer.status(), StreamReplayer::Status::kFileNotFound);

    Message out{};
    EXPECT_NO_THROW(EXPECT_FALSE(replayer.next(out)));
}

// ---------------------------------------------------------------------------
// A completely garbage (non-telemetry, e.g. plain-text) file is rejected as
// bad magic, never crashes, never reports kOk-shaped data.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, ArbitraryGarbageFileIsRejectedCleanly) {
    const std::string path = TempPath("garbage.trec");
    {
        std::ofstream out(path, std::ios::binary);
        out << "this is not a telemetry replay file at all, just text\n";
    }

    StreamReplayer replayer(path);
    EXPECT_NE(replayer.status(), StreamReplayer::Status::kOk);
    Message out{};
    EXPECT_NO_THROW(EXPECT_FALSE(replayer.next(out)));
}

// ---------------------------------------------------------------------------
// Hot-path noexcept pinning: write()/next() never throw by signature.
// ---------------------------------------------------------------------------
TEST(ReplayErrors, WriteAndNextAreNoexcept) {
    EXPECT_TRUE(noexcept(std::declval<StreamRecorder&>().write(std::declval<const Message&>())));
    EXPECT_TRUE(noexcept(std::declval<StreamReplayer&>().next(std::declval<Message&>())));
    EXPECT_TRUE(noexcept(StreamReplayer(std::declval<const std::string&>())));
    EXPECT_TRUE(noexcept(StreamRecorder(std::declval<const std::string&>())));
}

}  // namespace
