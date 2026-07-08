#pragma once

// SPEC-3.9: record a stream of Messages to disk in the self-describing TREC
// format (see record_format.hpp). Not on the ingestion hot path, so buffered
// std::ofstream is fine; write() stays noexcept and allocation-light.

#include <core/message.hpp>
#include <replay/record_format.hpp>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>

namespace telemetry::replay {

class StreamRecorder {
public:
    explicit StreamRecorder(const std::string& path) noexcept
        : file_(path, std::ios::binary | std::ios::trunc) {
        if (file_.is_open()) {
            std::uint8_t header[kHeaderSize];
            header[0] = static_cast<std::uint8_t>(kMagic[0]);
            header[1] = static_cast<std::uint8_t>(kMagic[1]);
            header[2] = static_cast<std::uint8_t>(kMagic[2]);
            header[3] = static_cast<std::uint8_t>(kMagic[3]);
            detail::put_u32(header + 4, kFormatVersion);
            file_.write(reinterpret_cast<const char*>(header), kHeaderSize);
            header_ok_ = file_.good();
        }
    }

    StreamRecorder(const StreamRecorder&) = delete;
    StreamRecorder& operator=(const StreamRecorder&) = delete;

    ~StreamRecorder() { close(); }

    bool is_open() const noexcept { return file_.is_open() && header_ok_; }

    bool write(const Message& msg) noexcept {
        if (!is_open()) return false;
        std::uint8_t buf[kRecordSize];
        detail::encode_record(msg, buf);
        file_.write(reinterpret_cast<const char*>(buf), kRecordSize);
        if (!file_.good()) return false;
        ++count_;
        return true;
    }

    std::size_t count() const noexcept { return count_; }

    void close() noexcept {
        if (file_.is_open()) {
            file_.flush();
            file_.close();
        }
    }

private:
    std::ofstream file_;
    std::size_t count_ = 0;
    bool header_ok_ = false;
};

}  // namespace telemetry::replay
