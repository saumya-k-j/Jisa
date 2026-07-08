#pragma once

// feed/coinbase.hpp
//
// SPEC-3.4 crypto adapter: parses Coinbase Exchange WebSocket ticker/heartbeat
// messages (see docs/feeds/coinbase_ws.md) into telemetry::Message.
//
// Design (see DECISIONS D-010, D-011, D-012):
//   - Message.seq == trade_id (ticker) / last_trade_id (heartbeat). Coinbase's
//     `sequence` spans the product's FULL event feed, so consecutive ticker
//     sequences jump by >1 and would false-fire naive gap detection; trade_id
//     increments by exactly 1 per ticker per product.
//   - Parsing uses simdjson's DOM error-code API (never the throwing API); the
//     parser instance is reused per handler so there is no per-message tape
//     allocation after the buffer warms up.
//   - `time` (RFC3339 UTC, microseconds) -> int64 epoch nanoseconds via
//     days-from-civil arithmetic, no allocation, no std::get_time.

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <simdjson.h>

#include <core/message.hpp>
#include <feed/handler.hpp>

namespace telemetry::feed {

namespace detail {

// Howard Hinnant's days-from-civil: days since 1970-01-01 for a proleptic
// Gregorian (y, m, d). Constexpr, no allocation, no <ctime>.
constexpr std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) noexcept {
    y -= m <= 2;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);              // [0, 399]
    const unsigned doy = (153u * (m + (m > 2 ? -3u : 9u)) + 2u) / 5u + d - 1;  // [0, 365]
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;              // [0, 146096]
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

inline bool read_uint(std::string_view s, std::size_t& i, std::size_t count,
                      std::int64_t& out) noexcept {
    if (i + count > s.size()) {
        return false;
    }
    std::int64_t v = 0;
    for (std::size_t k = 0; k < count; ++k) {
        const char c = s[i + k];
        if (c < '0' || c > '9') {
            return false;
        }
        v = v * 10 + (c - '0');
    }
    i += count;
    out = v;
    return true;
}

// Parses "YYYY-MM-DDTHH:MM:SS[.frac][Z]" (UTC) to epoch nanoseconds. The
// fractional part may have up to 9 digits; it is scaled to nanoseconds.
inline bool parse_rfc3339_ns(std::string_view s, std::int64_t& ns) noexcept {
    std::size_t i = 0;
    std::int64_t year, mon, day, hour, min, sec;
    if (!read_uint(s, i, 4, year) || i >= s.size() || s[i++] != '-') return false;
    if (!read_uint(s, i, 2, mon) || i >= s.size() || s[i++] != '-') return false;
    if (!read_uint(s, i, 2, day) || i >= s.size() || (s[i] != 'T' && s[i] != 't' && s[i] != ' ')) return false;
    ++i;
    if (!read_uint(s, i, 2, hour) || i >= s.size() || s[i++] != ':') return false;
    if (!read_uint(s, i, 2, min) || i >= s.size() || s[i++] != ':') return false;
    if (!read_uint(s, i, 2, sec)) return false;
    if (mon < 1 || mon > 12 || day < 1 || day > 31) return false;

    const std::int64_t days = days_from_civil(year, static_cast<unsigned>(mon),
                                              static_cast<unsigned>(day));
    const std::int64_t secs = days * 86400 + hour * 3600 + min * 60 + sec;

    std::int64_t frac_ns = 0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        int digits = 0;
        while (i < s.size() && s[i] >= '0' && s[i] <= '9' && digits < 9) {
            frac_ns = frac_ns * 10 + (s[i] - '0');
            ++i;
            ++digits;
        }
        // consume any excess sub-nanosecond digits (none expected here)
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
        while (digits < 9) {
            frac_ns *= 10;
            ++digits;
        }
    }

    ns = secs * 1000000000LL + frac_ns;
    return true;
}

// Converts a Coinbase decimal string (e.g. "62975.91") to double without
// allocation. strtod requires null termination; a small stack buffer suffices
// (Coinbase price/size strings are far shorter than this).
inline bool parse_decimal(std::string_view s, double& out) noexcept {
    char buf[64];
    if (s.empty() || s.size() >= sizeof(buf)) {
        return false;
    }
    std::memcpy(buf, s.data(), s.size());
    buf[s.size()] = '\0';
    char* end = nullptr;
    errno = 0;
    const double v = std::strtod(buf, &end);
    if (end != buf + s.size()) {
        return false;  // trailing garbage / not a full number
    }
    out = v;
    return true;
}

}  // namespace detail

class CoinbaseTickerHandler : public FeedHandler {
public:
    CoinbaseTickerHandler(std::unordered_map<std::string, std::uint32_t> product_to_stream,
                          GapCallback on_gap = nullptr)
        : FeedHandler(std::move(on_gap)),
          product_to_stream_(std::move(product_to_stream)) {}

    ParseResult parse(std::string_view raw, Message& out) noexcept override {
        simdjson::dom::element doc;
        if (parser_.parse(raw.data(), raw.size()).get(doc)) {
            return ParseResult::kMalformed;
        }
        std::string_view type;
        if (doc["type"].get_string().get(type)) {
            return ParseResult::kMalformed;
        }
        if (type == "ticker") {
            return parse_ticker(doc, out);
        }
        if (type == "heartbeat") {
            return parse_heartbeat(doc, out);
        }
        return ParseResult::kSkipped;  // subscriptions ack, snapshot, etc.
    }

private:
    ParseResult parse_ticker(simdjson::dom::element doc, Message& out) noexcept {
        std::string_view product;
        if (doc["product_id"].get_string().get(product)) {
            return ParseResult::kMalformed;
        }
        const std::uint32_t* stream_id = lookup(product);
        if (stream_id == nullptr) {
            return ParseResult::kSkipped;  // unmapped product: unroutable
        }

        std::string_view price_sv;
        if (doc["price"].get_string().get(price_sv)) {
            return ParseResult::kMalformed;
        }
        double price = 0.0;
        if (!detail::parse_decimal(price_sv, price)) {
            return ParseResult::kMalformed;
        }

        std::string_view time_sv;
        if (doc["time"].get_string().get(time_sv)) {
            return ParseResult::kMalformed;
        }
        std::int64_t ts_ns = 0;
        if (!detail::parse_rfc3339_ns(time_sv, ts_ns)) {
            return ParseResult::kMalformed;
        }

        std::uint64_t trade_id = 0;
        if (doc["trade_id"].get_uint64().get(trade_id)) {
            return ParseResult::kMalformed;
        }

        out.stream_id = *stream_id;
        out.ts_ns = ts_ns;
        out.value = price;
        out.seq = trade_id;
        return ParseResult::kOk;
    }

    ParseResult parse_heartbeat(simdjson::dom::element doc, Message& out) noexcept {
        std::string_view product;
        if (doc["product_id"].get_string().get(product)) {
            return ParseResult::kMalformed;
        }
        const std::uint32_t* stream_id = lookup(product);
        if (stream_id == nullptr) {
            return ParseResult::kSkipped;
        }
        std::uint64_t last_trade_id = 0;
        if (doc["last_trade_id"].get_uint64().get(last_trade_id)) {
            return ParseResult::kMalformed;
        }
        // stream_id + seq only: feeds gap detection, never pushed.
        out.stream_id = *stream_id;
        out.seq = last_trade_id;
        return ParseResult::kHeartbeat;
    }

    // Product ids ("BTC-USD") fit std::string SSO (<= 15 chars), so this
    // temporary key construction does not allocate. See DECISIONS D-011.
    const std::uint32_t* lookup(std::string_view product) const noexcept {
        auto it = product_to_stream_.find(std::string(product));
        if (it == product_to_stream_.end()) {
            return nullptr;
        }
        return &it->second;
    }

    simdjson::dom::parser parser_;
    std::unordered_map<std::string, std::uint32_t> product_to_stream_;
};

}  // namespace telemetry::feed
