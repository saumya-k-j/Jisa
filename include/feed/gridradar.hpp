#pragma once

// feed/gridradar.hpp
//
// SPEC-2(b)/3.4 grid-frequency adapter (anchor domain): parses polled Gridradar
// REST responses (see docs/feeds/grid_and_adsb.md) of the shape
//   {"data":[{"ts":"<RFC3339Z>","value":<Hz>}, ...]}
// into telemetry::Message. Unlike Coinbase's push WSS single-message parse(),
// one poll response yields MANY Messages (one per accepted {ts,value} sample),
// so this adapter pins its own batch-parse shape:
//   template <typename F> std::size_t parse_response(std::string_view, F&&) noexcept;
//
// Design (mirrors DECISIONS D-010..D-012 for Coinbase; duplication of the
// RFC3339/days-from-civil helpers is intentional for parallel-safe file
// isolation -- do NOT reach into coinbase.hpp to share):
//   - simdjson DOM error-code API only (never the throwing accessors); the
//     parser instance is reused per handler so there is no per-message tape
//     allocation in steady state.
//   - Gridradar has no native sequence number, so the adapter assigns a
//     consecutive seq from 1 across the handler's whole lifetime, one per
//     ACCEPTED sample.
//   - Dedup: a sample whose ts_ns <= the last accepted ts_ns is dropped
//     silently (re-polls / overlapping windows), before any seq/gap work.
//   - Gap detection is over wall-clock time (>1s between successive accepted
//     samples) rather than seq, since the feed has no sequence number.

#include <cstdint>
#include <functional>
#include <string_view>

#include <simdjson.h>

#include <core/message.hpp>

namespace telemetry::feed {

namespace gridradar_detail {

// Howard Hinnant's days-from-civil: days since 1970-01-01 for a proleptic
// Gregorian (y, m, d). Constexpr, no allocation, no <ctime>. (Duplicated from
// feed/coinbase.hpp on purpose -- see file header.)
constexpr std::int64_t days_from_civil(std::int64_t y, unsigned m, unsigned d) noexcept {
    y -= m <= 2;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);                  // [0, 399]
    const unsigned doy = (153u * (m + (m > 2 ? -3u : 9u)) + 2u) / 5u + d - 1;    // [0, 365]
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;                  // [0, 146096]
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

// Parses "YYYY-MM-DDTHH:MM:SS[.frac][Z]" (UTC) to epoch nanoseconds. Gridradar
// samples are whole seconds ("...Z"), but the optional fractional part is
// tolerated for robustness.
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
        while (i < s.size() && s[i] >= '0' && s[i] <= '9') ++i;
        while (digits < 9) {
            frac_ns *= 10;
            ++digits;
        }
    }

    ns = secs * 1000000000LL + frac_ns;
    return true;
}

}  // namespace gridradar_detail

// Fired when the gap between two successive ACCEPTED samples exceeds the nominal
// 1s cadence. `from_ts_ns` is the last accepted sample's ts; `to_ts_ns` is the
// newly accepted sample that revealed the gap (to - from > 1s in ns).
using TimeGapCallback =
    std::function<void(std::uint32_t stream_id,
                       std::int64_t from_ts_ns, std::int64_t to_ts_ns)>;

class GridradarHandler {
public:
    explicit GridradarHandler(std::uint32_t stream_id,
                              TimeGapCallback on_gap = nullptr) noexcept
        : stream_id_(stream_id), on_gap_(std::move(on_gap)) {}

    // Parses one poll response, invoking emit(const Message&) once per accepted
    // sample. Returns the number of emit invocations. Any malformed input (bad
    // JSON, missing "data" array, a sample missing/mistyped "ts"/"value") yields
    // no emission for that sample and never throws. Well-formed samples in the
    // same response are still processed.
    template <typename F>
    std::size_t parse_response(std::string_view raw, F&& emit) noexcept {
        simdjson::dom::element doc;
        if (parser_.parse(raw.data(), raw.size()).get(doc)) {
            return 0;
        }
        simdjson::dom::array data;
        if (doc["data"].get_array().get(data)) {
            return 0;
        }

        std::size_t emitted = 0;
        for (simdjson::dom::element sample : data) {
            std::string_view ts_sv;
            if (sample["ts"].get_string().get(ts_sv)) {
                continue;  // missing / mistyped "ts"
            }
            double value = 0.0;
            if (sample["value"].get_double().get(value)) {
                continue;  // missing / non-numeric "value"
            }
            std::int64_t ts_ns = 0;
            if (!gridradar_detail::parse_rfc3339_ns(ts_sv, ts_ns)) {
                continue;  // unparseable timestamp
            }

            // Dedup before assigning seq / checking for gaps.
            if (has_last_ && ts_ns <= last_ts_ns_) {
                continue;
            }

            if (has_last_ && on_gap_ && (ts_ns - last_ts_ns_) > kCadenceNs) {
                on_gap_(stream_id_, last_ts_ns_, ts_ns);
            }

            Message m;
            m.stream_id = stream_id_;
            m._pad0 = 0;
            m.ts_ns = ts_ns;
            m.value = value;
            m.seq = ++seq_;

            last_ts_ns_ = ts_ns;
            has_last_ = true;
            emit(static_cast<const Message&>(m));
            ++emitted;
        }
        return emitted;
    }

private:
    static constexpr std::int64_t kCadenceNs = 1'000'000'000LL;  // 1s

    std::uint32_t stream_id_;
    TimeGapCallback on_gap_;
    simdjson::dom::parser parser_;
    std::uint64_t seq_ = 0;
    std::int64_t last_ts_ns_ = 0;
    bool has_last_ = false;
};

}  // namespace telemetry::feed
