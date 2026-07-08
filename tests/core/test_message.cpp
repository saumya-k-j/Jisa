// tests/core/test_message.cpp
//
// Tests derived exclusively from SPEC.md sections 3.1 and the interface
// contract. NO implementation exists yet — this is the RED state.
//
// Spec requirements covered:
//   SPEC-3.1-FIELDS       : struct has stream_id, ts_ns, value, seq fields
//   SPEC-3.1-TYPES        : field types are uint32, int64, double, uint64
//   SPEC-3.1-ALIGN        : cache-line aligned (alignas(64), sizeof == 64)
//   SPEC-3.1-NO-DYNALLOC  : trivially copyable, standard layout
//   SPEC-3.1-ROUNDTRIP    : all fields survive assignment and copy

#include <core/message.hpp>

#include <cstdint>
#include <type_traits>

#include <gtest/gtest.h>

namespace {

// ---------------------------------------------------------------------------
// SPEC-3.1-ALIGN: cache-line aligned, sizeof == 64
// ---------------------------------------------------------------------------
TEST(MessageLayout, SizeIs64Bytes) {
    EXPECT_EQ(sizeof(telemetry::Message), std::size_t{64});
}

TEST(MessageLayout, AlignmentIs64Bytes) {
    EXPECT_EQ(alignof(telemetry::Message), std::size_t{64});
}

// ---------------------------------------------------------------------------
// SPEC-3.1-NO-DYNALLOC: trivially copyable and standard layout
// (also verifies no dynamic allocation can occur from the type itself)
// ---------------------------------------------------------------------------
TEST(MessageLayout, IsTriviallyCopyable) {
    EXPECT_TRUE(std::is_trivially_copyable_v<telemetry::Message>);
}

TEST(MessageLayout, IsStandardLayout) {
    EXPECT_TRUE(std::is_standard_layout_v<telemetry::Message>);
}

// Sanity: struct is not polymorphic (no vtable, no heap via new in hot path)
TEST(MessageLayout, IsNotPolymorphic) {
    EXPECT_FALSE(std::is_polymorphic_v<telemetry::Message>);
}

// ---------------------------------------------------------------------------
// SPEC-3.1-TYPES: verify field types exactly
// ---------------------------------------------------------------------------
TEST(MessageFields, StreamIdIsUint32) {
    EXPECT_TRUE((std::is_same_v<decltype(telemetry::Message::stream_id), std::uint32_t>));
}

TEST(MessageFields, TsNsIsInt64) {
    EXPECT_TRUE((std::is_same_v<decltype(telemetry::Message::ts_ns), std::int64_t>));
}

TEST(MessageFields, ValueIsDouble) {
    EXPECT_TRUE((std::is_same_v<decltype(telemetry::Message::value), double>));
}

TEST(MessageFields, SeqIsUint64) {
    EXPECT_TRUE((std::is_same_v<decltype(telemetry::Message::seq), std::uint64_t>));
}

// ---------------------------------------------------------------------------
// SPEC-3.1-ROUNDTRIP: all fields survive write and read-back
// ---------------------------------------------------------------------------
TEST(MessageRoundtrip, AllFieldsWriteAndRead) {
    telemetry::Message m;
    m.stream_id = 0xDEAD'BEEFu;
    m.ts_ns     = -9'000'000'000'000LL;
    m.value     = 3.14159265358979;
    m.seq       = 0xFFFF'FFFF'FFFF'FFFFull;

    EXPECT_EQ(m.stream_id, 0xDEAD'BEEFu);
    EXPECT_EQ(m.ts_ns,     -9'000'000'000'000LL);
    EXPECT_DOUBLE_EQ(m.value, 3.14159265358979);
    EXPECT_EQ(m.seq,       0xFFFF'FFFF'FFFF'FFFFull);
}

TEST(MessageRoundtrip, ZeroInitialization) {
    telemetry::Message m{};
    EXPECT_EQ(m.stream_id, std::uint32_t{0});
    EXPECT_EQ(m.ts_ns,     std::int64_t{0});
    EXPECT_DOUBLE_EQ(m.value, 0.0);
    EXPECT_EQ(m.seq,       std::uint64_t{0});
}

TEST(MessageRoundtrip, CopyPreservesFields) {
    telemetry::Message src;
    src.stream_id = 42u;
    src.ts_ns     = 1'700'000'000'000'000'000LL;
    src.value     = -273.15;
    src.seq       = 999'999ull;

    telemetry::Message dst = src;   // trivial copy
    EXPECT_EQ(dst.stream_id, src.stream_id);
    EXPECT_EQ(dst.ts_ns,     src.ts_ns);
    EXPECT_DOUBLE_EQ(dst.value, src.value);
    EXPECT_EQ(dst.seq,       src.seq);
}

TEST(MessageRoundtrip, NegativeTimestampRoundtrip) {
    telemetry::Message m{};
    m.ts_ns = std::numeric_limits<std::int64_t>::min();
    EXPECT_EQ(m.ts_ns, std::numeric_limits<std::int64_t>::min());
}

TEST(MessageRoundtrip, MaxSeqRoundtrip) {
    telemetry::Message m{};
    m.seq = std::numeric_limits<std::uint64_t>::max();
    EXPECT_EQ(m.seq, std::numeric_limits<std::uint64_t>::max());
}

// ---------------------------------------------------------------------------
// SPEC-3.1-ALIGN: runtime alignment of a heap-allocated instance also
// satisfies 64-byte alignment (the allocator will honour alignas).
// ---------------------------------------------------------------------------
TEST(MessageLayout, HeapAlignedTo64) {
    // operator new respects alignas in C++17+.
    auto* p = new telemetry::Message{};
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(p) % 64u, std::uintptr_t{0});
    delete p;
}

} // namespace
