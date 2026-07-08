#pragma once

// detect/rules.hpp
//
// SPEC-3.5 (Layer 1): per-stream hard bounds + rate-of-change limits loaded
// from a flat YAML config (see config/grid_eu_freq.yaml).
//
// Hot-path notes:
//   - RuleChecker::check() is O(1), noexcept, no allocation in steady state.
//     Per-stream state (bound rule + previous sample) lives in an
//     unordered_map keyed by stream_id; it inserts only when a rule is
//     registered (add_rule, off the hot path) or the first time a sample is
//     seen for a configured stream. Steady-state lookups do not allocate.
//     Same pattern as feed/handler.hpp (see DECISIONS D-011).
//   - load_rule_config() runs at startup only and MAY throw / allocate.

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>

namespace telemetry::detect {

enum class RuleResult : std::uint8_t { kOk, kOutOfBounds, kRateViolation };

struct RuleConfig {
    std::uint32_t stream_id;
    double min;
    double max;
    double max_rate_of_change;  // value units PER SECOND
};

namespace internal {

// Strict whole-token uint32 parse via std::from_chars (locale-independent,
// no sign wrap: a leading '-' is rejected because from_chars for unsigned
// types does not accept it). Trailing garbage ("7x") is rejected by
// requiring full-token consumption. Throws std::runtime_error naming `key`.
inline std::uint32_t parse_u32_or_throw(const std::string& key,
                                        const std::string& val) {
    std::uint32_t out = 0;
    const char* first = val.data();
    const char* last = first + val.size();
    const auto [ptr, ec] = std::from_chars(first, last, out);
    if (ec != std::errc{} || ptr != last) {
        throw std::runtime_error("load_rule_config: invalid value for key '" +
                                 key + "': " + val);
    }
    return out;
}

// Strict whole-token double parse. Apple Clang 14's libc++ (the Release-tree
// toolchain) does not provide std::from_chars for floating-point types
// (integral only), so this uses std::strtod with strict full-consumption:
// endptr must land exactly at the end of the token, so trailing garbage
// ("5.0xyz") and empty tokens are rejected. Caveat: strtod is
// locale-dependent for the decimal separator; this project never changes the
// default "C" locale, where '.' is the separator used by the pinned YAML
// schema. Throws std::runtime_error naming `key` on any parse failure.
inline double parse_double_or_throw(const std::string& key,
                                    const std::string& val) {
    const char* first = val.c_str();  // trimmed by caller; null-terminated
    char* end = nullptr;
    const double out = std::strtod(first, &end);
    if (end != first + val.size() || val.empty()) {
        throw std::runtime_error("load_rule_config: invalid value for key '" +
                                 key + "': " + val);
    }
    return out;
}

}  // namespace internal

// Off the hot path: parses one stream's rule config from a flat-mapping YAML
// file shaped like config/grid_eu_freq.yaml. Only the four required fields are
// read; any other keys (name, units, ewma_alpha, ...) are ignored. Throws
// std::runtime_error if the file cannot be opened, a required field is
// missing, a value fails to parse (non-numeric / trailing garbage / negative
// stream_id), or min >= max.
inline RuleConfig load_rule_config(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("load_rule_config: cannot open file: " + path);
    }

    std::optional<std::uint32_t> stream_id;
    std::optional<double> min;
    std::optional<double> max;
    std::optional<double> max_rate;

    std::string line;
    while (std::getline(in, line)) {
        // Strip inline comments.
        const auto hash = line.find('#');
        if (hash != std::string::npos) {
            line.erase(hash);
        }
        const auto colon = line.find(':');
        if (colon == std::string::npos) {
            continue;  // blank / non key:value line
        }
        std::string key = line.substr(0, colon);
        std::string val = line.substr(colon + 1);

        auto trim = [](std::string& s) {
            const auto b = s.find_first_not_of(" \t\r\n");
            if (b == std::string::npos) {
                s.clear();
                return;
            }
            const auto e = s.find_last_not_of(" \t\r\n");
            s = s.substr(b, e - b + 1);
        };
        trim(key);
        trim(val);
        if (key.empty() || val.empty()) {
            continue;
        }

        if (key == "stream_id") {
            stream_id = internal::parse_u32_or_throw(key, val);
        } else if (key == "min") {
            min = internal::parse_double_or_throw(key, val);
        } else if (key == "max") {
            max = internal::parse_double_or_throw(key, val);
        } else if (key == "max_rate_of_change") {
            max_rate = internal::parse_double_or_throw(key, val);
        }
    }

    if (!stream_id || !min || !max || !max_rate) {
        throw std::runtime_error("load_rule_config: missing required field in " + path);
    }
    if (*min >= *max) {
        throw std::runtime_error("load_rule_config: min >= max in " + path);
    }

    RuleConfig cfg{};
    cfg.stream_id = *stream_id;
    cfg.min = *min;
    cfg.max = *max;
    cfg.max_rate_of_change = *max_rate;
    return cfg;
}

class RuleChecker {
public:
    RuleChecker() noexcept = default;

    // Off hot path: register or replace the bound rule for cfg.stream_id.
    void add_rule(const RuleConfig& cfg) { rules_[cfg.stream_id] = State{cfg}; }

    // HOT PATH: O(1), noexcept. Unknown stream_id -> kOk.
    RuleResult check(std::uint32_t stream_id, double value,
                     std::int64_t ts_ns) noexcept {
        auto it = rules_.find(stream_id);
        if (it == rules_.end()) {
            return RuleResult::kOk;  // domain-agnostic core: never reject unknowns
        }
        State& st = it->second;

        // Bounds take precedence over rate.
        RuleResult result = RuleResult::kOk;
        if (value < st.cfg.min || value > st.cfg.max) {
            result = RuleResult::kOutOfBounds;
        } else if (st.has_prev && ts_ns > st.prev_ts_ns) {
            const double dt = static_cast<double>(ts_ns - st.prev_ts_ns) / 1e9;
            const double dv = value - st.prev_value;
            const double rate = (dv < 0.0 ? -dv : dv) / dt;
            if (rate > st.cfg.max_rate_of_change) {
                result = RuleResult::kRateViolation;
            }
        }

        // Update previous-sample state for the next call, regardless of result.
        st.prev_value = value;
        st.prev_ts_ns = ts_ns;
        st.has_prev = true;
        return result;
    }

private:
    struct State {
        RuleConfig cfg{};
        double prev_value = 0.0;
        std::int64_t prev_ts_ns = 0;
        bool has_prev = false;
    };

    std::unordered_map<std::uint32_t, State> rules_;
};

}  // namespace telemetry::detect
