#pragma once

#include <cstdint>
#include <type_traits>

namespace telemetry {

// SPEC-3.1: Normalized message struct.
// alignas(64): cache-line aligned to prevent false sharing on the hot path.
// sizeof == 64: uint32(4) + pad(4) + int64(8) + double(8) + uint64(8) = 32 bytes
// of data + 32 bytes of explicit padding = 64 total.
// trivially copyable + standard layout: no virtual, no user-defined ctor/dtor.
struct alignas(64) Message {
    std::uint32_t stream_id;  //  4 bytes
    std::uint32_t _pad0;      //  4 bytes (alignment gap for ts_ns)
    std::int64_t  ts_ns;      //  8 bytes
    double        value;      //  8 bytes
    std::uint64_t seq;        //  8 bytes
    std::uint8_t  _pad1[32]; //  32 bytes — total = 64
};

static_assert(sizeof(Message) == 64,    "Message must be exactly one cache line");
static_assert(alignof(Message) == 64,   "Message must be cache-line aligned");
static_assert(std::is_trivially_copyable_v<Message>, "Message must be trivially copyable");
static_assert(std::is_standard_layout_v<Message>,    "Message must be standard layout");

} // namespace telemetry
