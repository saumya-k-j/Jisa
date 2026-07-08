// tests/replay/test_checksum.cpp
//
// Tests derived from SPEC.md section 3.9 (replay + fault injection) and
// section 1 measurable objective #5 ("Determinism: identical checksummed
// end-state on replay of a recorded stream"), section 7 phase 3. NO
// implementation exists yet under include/replay/ -- this is the RED state
// (build must fail on missing headers, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/replay/checksum.hpp, which does not exist
// yet):
//
//   namespace telemetry::replay {
//     class StreamChecksum {
//     public:
//       StreamChecksum() noexcept;
//       void update(const Message& msg) noexcept;
//       std::uint64_t digest() const noexcept;
//     };
//   }
//
// PINNED ALGORITHM (must not silently change -- see known-answer tests
// below): FNV-1a, 64-bit, offset basis 0xcbf29ce484222325, prime
// 0x100000001b3. Each call to update() feeds the accumulator EXACTLY 28
// bytes, built by concatenating the raw little-endian byte representation
// of four Message fields IN THIS ORDER:
//   1. stream_id  (uint32_t,  4 bytes, LE)
//   2. ts_ns      (int64_t,   8 bytes, LE two's complement)
//   3. value      (double,    8 bytes, LE IEEE-754 bit pattern)
//   4. seq        (uint64_t,  8 bytes, LE)
// The struct's padding bytes (_pad0, _pad1) are explicitly NOT hashed --
// they are alignment filler, not stream content, and must not affect the
// checksum (see PaddingBytesAreExcludedFromDigest below).
//
// Known-answer values below were computed independently in Python (FNV-1a
// reference implementation over struct.pack('<I' / '<q' / '<d' / '<Q', ...)
// concatenated bytes), NOT derived from any C++ implementation.
// ---------------------------------------------------------------------------

#include <replay/checksum.hpp>

#include <core/message.hpp>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::replay::StreamChecksum;

Message MakeMessage(std::uint32_t stream_id, std::int64_t ts_ns, double value,
                     std::uint64_t seq) {
    Message m{};
    m.stream_id = stream_id;
    m.ts_ns = ts_ns;
    m.value = value;
    m.seq = seq;
    return m;
}

// ---------------------------------------------------------------------------
// digest() returns exactly uint64_t (pinned return type).
// ---------------------------------------------------------------------------
TEST(StreamChecksumApi, DigestReturnsUint64) {
    static_assert(std::is_same_v<decltype(std::declval<const StreamChecksum&>().digest()),
                                  std::uint64_t>);
}

TEST(StreamChecksumApi, UpdateAndDigestAreNoexcept) {
    EXPECT_TRUE(noexcept(std::declval<StreamChecksum&>().update(std::declval<const Message&>())));
    EXPECT_TRUE(noexcept(std::declval<const StreamChecksum&>().digest()));
}

// ---------------------------------------------------------------------------
// Initial state (no updates) equals the bare FNV-1a 64 offset basis -- this
// pins that update() is a pure fold-left over the offset basis with no other
// hidden seeding.
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, InitialDigestEqualsFnvOffsetBasis) {
    StreamChecksum cs;
    EXPECT_EQ(cs.digest(), 0xcbf29ce484222325ULL);
}

// ---------------------------------------------------------------------------
// Known-answer: single message.
//   stream_id=1, ts_ns=1000, value=2.5, seq=42
//   bytes (28, hex): 01000000 e803000000000000 0000000000000440 2a00000000000000
//   FNV-1a64(bytes) = 0xd3d19d036981dbd3
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, SingleMessageDigestMatchesReferenceFnv1a) {
    StreamChecksum cs;
    cs.update(MakeMessage(1, 1000, 2.5, 42));
    EXPECT_EQ(cs.digest(), 0xd3d19d036981dbd3ULL);
}

// ---------------------------------------------------------------------------
// Known-answer: two messages fed in order, accumulator chained (FNV-1a over
// msg1_bytes || msg2_bytes, i.e. update() folds into the running state, it
// does not reset between calls).
//   msg1: stream_id=1, ts_ns=1000,  value=2.5,   seq=42
//   msg2: stream_id=7, ts_ns=2000,  value=-3.25, seq=100
//   chained FNV-1a64 = 0x835b3c96476fd867
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, TwoMessagesChainAccumulatorState) {
    StreamChecksum cs;
    cs.update(MakeMessage(1, 1000, 2.5, 42));
    cs.update(MakeMessage(7, 2000, -3.25, 100));
    EXPECT_EQ(cs.digest(), 0x835b3c96476fd867ULL);
}

// ---------------------------------------------------------------------------
// Order sensitivity: feeding the same two messages in the opposite order
// must produce a DIFFERENT digest (proves the checksum covers sequence, not
// just a set/sum of message content). Reference value computed the same way
// as above with msg2 first, msg1 second.
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, SwappedOrderProducesDifferentDigest) {
    StreamChecksum forward;
    forward.update(MakeMessage(1, 1000, 2.5, 42));
    forward.update(MakeMessage(7, 2000, -3.25, 100));

    StreamChecksum swapped;
    swapped.update(MakeMessage(7, 2000, -3.25, 100));
    swapped.update(MakeMessage(1, 1000, 2.5, 42));

    EXPECT_EQ(swapped.digest(), 0xe4d9d88238c36d93ULL);
    EXPECT_NE(forward.digest(), swapped.digest());
}

// ---------------------------------------------------------------------------
// Padding bytes (_pad0, _pad1) must NOT be hashed: two messages with
// identical stream_id/ts_ns/value/seq but different padding content produce
// the SAME digest.
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, PaddingBytesAreExcludedFromDigest) {
    Message a = MakeMessage(3, 5000, 9.75, 17);
    Message b = a;
    a._pad0 = 0x11111111u;
    std::fill(std::begin(a._pad1), std::end(a._pad1), std::uint8_t{0xAA});
    b._pad0 = 0x22222222u;
    std::fill(std::begin(b._pad1), std::end(b._pad1), std::uint8_t{0xBB});

    StreamChecksum cs_a;
    cs_a.update(a);
    StreamChecksum cs_b;
    cs_b.update(b);

    EXPECT_EQ(cs_a.digest(), cs_b.digest());
}

// ---------------------------------------------------------------------------
// Sanity: differing content-bearing fields do change the digest (basic
// diffusion, not a security property -- just guards against a no-op stub).
// ---------------------------------------------------------------------------
TEST(StreamChecksumKnownAnswer, DifferingValueChangesDigest) {
    StreamChecksum cs_a;
    cs_a.update(MakeMessage(1, 1000, 2.5, 42));
    StreamChecksum cs_b;
    cs_b.update(MakeMessage(1, 1000, 2.5000001, 42));
    EXPECT_NE(cs_a.digest(), cs_b.digest());
}

}  // namespace
