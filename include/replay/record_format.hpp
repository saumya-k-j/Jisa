#pragma once

// SPEC-3.9 replay file format primitives.
//
// On-disk layout (little-endian, this repo's byte-order policy — see D-016):
//   Header (8 bytes): char magic[4] = "TREC" ; uint32_t version (LE).
//   Records (kRecordSize == 64 bytes each), in write order:
//     offset  0: stream_id   uint32_t  (LE)
//     offset  4: (zero pad, 4 bytes)
//     offset  8: ts_ns       int64_t   (LE two's complement)
//     offset 16: value       double    (LE IEEE-754 bit pattern)
//     offset 24: seq         uint64_t  (LE)
//     offset 32..63: zero padding (fills a record to one cache line, mirrors
//                    the in-memory Message layout so a record is 64 bytes).
//
// Serialization is explicit byte-by-byte (not a raw struct memcpy) so the
// output is byte-identical regardless of any in-memory padding contents and
// portable across endianness; the content padding is written as zeros.

#include <core/message.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace telemetry::replay {

inline constexpr char kMagic[4] = {'T', 'R', 'E', 'C'};
inline constexpr std::uint32_t kFormatVersion = 1;
inline constexpr std::size_t kHeaderSize = 8;
inline constexpr std::size_t kRecordSize = 64;

namespace detail {

inline void put_u32(std::uint8_t* p, std::uint32_t v) noexcept {
    p[0] = static_cast<std::uint8_t>(v);
    p[1] = static_cast<std::uint8_t>(v >> 8);
    p[2] = static_cast<std::uint8_t>(v >> 16);
    p[3] = static_cast<std::uint8_t>(v >> 24);
}

inline void put_u64(std::uint8_t* p, std::uint64_t v) noexcept {
    for (int i = 0; i < 8; ++i) {
        p[i] = static_cast<std::uint8_t>(v >> (8 * i));
    }
}

inline std::uint32_t get_u32(const std::uint8_t* p) noexcept {
    return static_cast<std::uint32_t>(p[0]) |
           (static_cast<std::uint32_t>(p[1]) << 8) |
           (static_cast<std::uint32_t>(p[2]) << 16) |
           (static_cast<std::uint32_t>(p[3]) << 24);
}

inline std::uint64_t get_u64(const std::uint8_t* p) noexcept {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

// Encodes exactly kRecordSize bytes into buf.
inline void encode_record(const Message& m, std::uint8_t* buf) noexcept {
    std::memset(buf, 0, kRecordSize);
    put_u32(buf + 0, m.stream_id);
    put_u64(buf + 8, static_cast<std::uint64_t>(m.ts_ns));
    put_u64(buf + 16, std::bit_cast<std::uint64_t>(m.value));
    put_u64(buf + 24, m.seq);
}

// Decodes kRecordSize bytes into m (content fields only; caller's padding is
// left untouched).
inline void decode_record(const std::uint8_t* buf, Message& m) noexcept {
    m.stream_id = get_u32(buf + 0);
    m.ts_ns = static_cast<std::int64_t>(get_u64(buf + 8));
    m.value = std::bit_cast<double>(get_u64(buf + 16));
    m.seq = get_u64(buf + 24);
}

}  // namespace detail
}  // namespace telemetry::replay
