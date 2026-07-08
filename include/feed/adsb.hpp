#pragma once

// feed/adsb.hpp
//
// SPEC-2c / SPEC-3.4 ADS-B aircraft adapter: parses a polled adsb.fi REST
// snapshot ({"ac":[...aircraft...],"now":<epoch ms>,...}, see
// docs/feeds/grid_and_adsb.md) into telemetry::Message samples, one per
// accepted aircraft.
//
// Design (mirrors the Coinbase adapter precedents):
//   - simdjson DOM error-code API only (never the throwing API); the parser
//     instance is reused per handler, so there is no per-poll tape allocation
//     after warm-up (D-012 precedent).
//   - Per-aircraft state keyed by the 6-char ICAO hex in an unordered_map;
//     hex fits std::string SSO, so key construction does not allocate
//     (D-011 precedent).
//   - Dynamic hex -> stream_id registry: first-seen order from base_stream_id,
//     bounded by max_streams. Once the cap is reached, any further new hex is
//     never registered (permanently ignored) because it is never inserted.
//   - ts_ns = now_ms * 1'000'000 - (int64)(seen * 1e9): approximates each
//     aircraft's own last-message wall-clock time, independent of poll cadence.
//   - Per-aircraft ts_ns dedup and per-aircraft adapter-assigned seq from 1.

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include <simdjson.h>

#include <core/message.hpp>

namespace telemetry::feed {

enum class AdsbMetric : std::uint8_t { kAltBaro, kGroundSpeed };

namespace adsb_detail {

// Reads a JSON number as double, accepting integer or floating literals
// (simdjson stores integer-valued literals as int64/uint64). Returns false
// only when the element is missing or not a number.
inline bool read_number(simdjson::dom::element e, double& out) noexcept {
    double d;
    if (!e.get_double().get(d)) {
        out = d;
        return true;
    }
    std::int64_t i;
    if (!e.get_int64().get(i)) {
        out = static_cast<double>(i);
        return true;
    }
    std::uint64_t u;
    if (!e.get_uint64().get(u)) {
        out = static_cast<double>(u);
        return true;
    }
    return false;
}

// Overload over the field-lookup result: treats a missing field as "no
// number" (never throws).
inline bool read_number(simdjson::simdjson_result<simdjson::dom::element> r,
                        double& out) noexcept {
    simdjson::dom::element e;
    if (r.get(e)) {
        return false;  // missing / error
    }
    return read_number(e, out);
}

}  // namespace adsb_detail

class AdsbHandler {
public:
    AdsbHandler(std::uint32_t base_stream_id, std::size_t max_streams,
                AdsbMetric metric) noexcept
        : base_stream_id_(base_stream_id),
          max_streams_(max_streams),
          metric_(metric) {}

    template <typename F>
    std::size_t parse_response(std::string_view raw, F&& emit) noexcept {
        simdjson::dom::element doc;
        if (parser_.parse(raw.data(), raw.size()).get(doc)) {
            return 0;  // malformed JSON
        }

        // "now" (epoch ms) is required to derive any per-aircraft timestamp.
        std::int64_t now_ms = 0;
        if (!read_int64(doc["now"], now_ms)) {
            return 0;
        }

        simdjson::dom::array ac;
        if (doc["ac"].get_array().get(ac)) {
            return 0;  // missing "ac" array
        }

        std::size_t emitted = 0;
        for (simdjson::dom::element aircraft : ac) {
            if (accept(aircraft, now_ms, emit)) {
                ++emitted;
            }
        }
        return emitted;
    }

private:
    struct StreamState {
        std::uint32_t stream_id;
        std::int64_t last_ts_ns;
        std::uint64_t seq;
    };

    static bool read_int64(simdjson::simdjson_result<simdjson::dom::element> r,
                           std::int64_t& out) noexcept {
        simdjson::dom::element e;
        if (r.get(e)) {
            return false;  // missing / error
        }
        std::int64_t i;
        if (!e.get_int64().get(i)) {
            out = i;
            return true;
        }
        std::uint64_t u;
        if (!e.get_uint64().get(u)) {
            out = static_cast<std::int64_t>(u);
            return true;
        }
        return false;
    }

    // Extracts Message.value for the selected metric. Returns false when the
    // required field is missing/invalid (aircraft is then skipped). Under
    // kAltBaro, the JSON string "ground" maps to 0.0 (pinned choice).
    bool read_value(simdjson::dom::element aircraft, double& out) const noexcept {
        if (metric_ == AdsbMetric::kGroundSpeed) {
            return adsb_detail::read_number(aircraft["gs"], out);
        }
        simdjson::dom::element alt;
        if (aircraft["alt_baro"].get(alt)) {
            return false;
        }
        std::string_view s;
        if (!alt.get_string().get(s)) {
            if (s == "ground") {
                out = 0.0;
                return true;
            }
            return false;
        }
        return adsb_detail::read_number(alt, out);
    }

    template <typename F>
    bool accept(simdjson::dom::element aircraft, std::int64_t now_ms,
                F&& emit) noexcept {
        std::string_view hex;
        if (aircraft["hex"].get_string().get(hex)) {
            return false;
        }
        double lat = 0.0;
        double lon = 0.0;
        if (!adsb_detail::read_number(aircraft["lat"], lat) ||
            !adsb_detail::read_number(aircraft["lon"], lon)) {
            return false;
        }
        double seen = 0.0;
        if (!adsb_detail::read_number(aircraft["seen"], seen)) {
            return false;
        }
        double value = 0.0;
        if (!read_value(aircraft, value)) {
            return false;
        }

        const std::int64_t ts_ns =
            now_ms * 1'000'000 - static_cast<std::int64_t>(seen * 1e9);

        auto it = streams_.find(std::string(hex));
        if (it == streams_.end()) {
            if (streams_.size() >= max_streams_) {
                return false;  // registry at cap: permanently ignore new hex
            }
            const std::uint32_t stream_id = base_stream_id_ +
                static_cast<std::uint32_t>(streams_.size());
            it = streams_.emplace(std::string(hex),
                                  StreamState{stream_id, ts_ns, 0}).first;
        } else if (ts_ns <= it->second.last_ts_ns) {
            return false;  // stale / unchanged since last emitted sample
        } else {
            it->second.last_ts_ns = ts_ns;
        }

        StreamState& st = it->second;
        ++st.seq;

        Message m{};
        m.stream_id = st.stream_id;
        m.ts_ns = ts_ns;
        m.value = value;
        m.seq = st.seq;
        emit(static_cast<const Message&>(m));
        return true;
    }

    std::uint32_t base_stream_id_;
    std::size_t max_streams_;
    AdsbMetric metric_;
    simdjson::dom::parser parser_;
    std::unordered_map<std::string, StreamState> streams_;
};

}  // namespace telemetry::feed
