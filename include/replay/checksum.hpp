#pragma once

// SPEC-3.9 / measurable objective #5: deterministic checksum of a replayed
// stream. FNV-1a, 64-bit. Each update() folds EXACTLY 28 content bytes into
// the running accumulator, in field order stream_id||ts_ns||value||seq (all
// little-endian). The Message's alignment padding is deliberately excluded —
// it is filler, not stream content.

#include <core/message.hpp>
#include <replay/record_format.hpp>

#include <bit>
#include <cstdint>

namespace telemetry::replay {

class StreamChecksum {
public:
    StreamChecksum() noexcept = default;

    void update(const Message& msg) noexcept {
        std::uint8_t bytes[28];
        detail::put_u32(bytes + 0, msg.stream_id);
        detail::put_u64(bytes + 4, static_cast<std::uint64_t>(msg.ts_ns));
        detail::put_u64(bytes + 12, std::bit_cast<std::uint64_t>(msg.value));
        detail::put_u64(bytes + 20, msg.seq);
        for (unsigned char b : bytes) {
            hash_ ^= static_cast<std::uint64_t>(b);
            hash_ *= kFnvPrime;
        }
    }

    std::uint64_t digest() const noexcept { return hash_; }

private:
    static constexpr std::uint64_t kFnvOffsetBasis = 0xcbf29ce484222325ULL;
    static constexpr std::uint64_t kFnvPrime = 0x100000001b3ULL;
    std::uint64_t hash_ = kFnvOffsetBasis;
};

}  // namespace telemetry::replay
