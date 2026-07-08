#pragma once

// SPEC-3.9: pull-based, single-threaded, "as-fast-as-possible" replay of a
// recorded TREC stream. The constructor never throws (failures are reported
// via status()); next() never throws, including on a truncated file.

#include <core/message.hpp>
#include <replay/record_format.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>

namespace telemetry::replay {

class StreamReplayer {
public:
    enum class Status : std::uint8_t {
        kOk,
        kFileNotFound,
        kBadMagic,
        kUnsupportedVersion,
    };

    explicit StreamReplayer(const std::string& path) noexcept
        : file_(path, std::ios::binary) {
        if (!file_.is_open()) {
            status_ = Status::kFileNotFound;
            return;
        }
        std::uint8_t header[kHeaderSize];
        file_.read(reinterpret_cast<char*>(header), kHeaderSize);
        if (file_.gcount() != static_cast<std::streamsize>(kHeaderSize)) {
            status_ = Status::kBadMagic;
            return;
        }
        if (std::memcmp(header, kMagic, sizeof(kMagic)) != 0) {
            status_ = Status::kBadMagic;
            return;
        }
        if (detail::get_u32(header + 4) != kFormatVersion) {
            status_ = Status::kUnsupportedVersion;
            return;
        }
        status_ = Status::kOk;
    }

    Status status() const noexcept { return status_; }

    bool next(Message& out) noexcept {
        if (status_ != Status::kOk) return false;
        std::uint8_t buf[kRecordSize];
        file_.read(reinterpret_cast<char*>(buf), kRecordSize);
        const std::streamsize got = file_.gcount();
        if (got == static_cast<std::streamsize>(kRecordSize)) {
            detail::decode_record(buf, out);
            ++messages_replayed_;
            return true;
        }
        if (got > 0) {
            truncated_ = true;
        }
        return false;
    }

    std::size_t messages_replayed() const noexcept { return messages_replayed_; }

    bool truncated() const noexcept { return truncated_; }

private:
    std::ifstream file_;
    Status status_ = Status::kFileNotFound;
    std::size_t messages_replayed_ = 0;
    bool truncated_ = false;
};

}  // namespace telemetry::replay
