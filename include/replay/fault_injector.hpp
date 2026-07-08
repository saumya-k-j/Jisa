#pragma once

// SPEC-3.9 + fault-injection skill: inject faults with known ground truth
// into a recorded stream, emit a labeled ground-truth file for scoring.
//
// Fault taxonomy (per-stream, indexed among that stream's messages):
//   spike          value += magnitude on every affected sample (isolated).
//   drift          value += magnitude * (j-onset+1)/duration — a linear ramp
//                  reaching exactly `magnitude` at the last affected sample.
//   stuck-at-value value frozen at the pre-fault value at `onset`.
//   dropout        affected samples removed from the output entirely.
// Timestamps and seq are never altered; non-faulted samples stay bit-identical.
//
// Determinism: onsets given as kAutoOnset are resolved from a seeded
// SplitMix64 (hand-rolled so results are byte-identical across stdlibs — see
// D-018). Same seed + same input => byte-identical output + labels.

#include <core/message.hpp>
#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <system_error>
#include <limits>
#include <string>
#include <vector>

namespace telemetry::replay {

enum class FaultType : std::uint8_t {
    kSpike,
    kDrift,
    kStuckAtValue,
    kDropout,
};

inline constexpr std::size_t kAutoOnset = std::numeric_limits<std::size_t>::max();

struct FaultSpec {
    FaultType type;
    std::uint32_t stream_id;
    std::size_t onset_index;
    std::size_t duration;
    double magnitude;
};

struct FaultLabel {
    std::uint32_t stream_id;
    FaultType type;
    std::int64_t t_start_ns;
    std::int64_t t_end_ns;
};

inline bool operator==(const FaultLabel& a, const FaultLabel& b) noexcept {
    return a.stream_id == b.stream_id && a.type == b.type &&
           a.t_start_ns == b.t_start_ns && a.t_end_ns == b.t_end_ns;
}

namespace detail {

inline const char* fault_type_name(FaultType t) noexcept {
    switch (t) {
        case FaultType::kSpike: return "spike";
        case FaultType::kDrift: return "drift";
        case FaultType::kStuckAtValue: return "stuck_at_value";
        case FaultType::kDropout: return "dropout";
    }
    return "spike";
}

inline bool parse_fault_type(const std::string& name, FaultType& out) noexcept {
    if (name == "spike") { out = FaultType::kSpike; return true; }
    if (name == "drift") { out = FaultType::kDrift; return true; }
    if (name == "stuck_at_value") { out = FaultType::kStuckAtValue; return true; }
    if (name == "dropout") { out = FaultType::kDropout; return true; }
    return false;
}

}  // namespace detail

class FaultInjector {
public:
    explicit FaultInjector(std::uint64_t seed) noexcept : rng_state_(seed) {}

    // The noexcept promise is kept truthful by catching everything the
    // allocating body (vector growth, ofstream) could throw and reporting
    // failure via the return value. Offline tooling, off the hot path, so a
    // catch-all is fine per cpp-conventions.
    bool inject(const std::string& input_path, const std::string& output_path,
                const std::string& labels_path,
                const std::vector<FaultSpec>& specs) noexcept {
        try {
            return inject_impl(input_path, output_path, labels_path, specs);
        } catch (...) {
            return false;
        }
    }

private:
    bool inject_impl(const std::string& input_path,
                     const std::string& output_path,
                     const std::string& labels_path,
                     const std::vector<FaultSpec>& specs) {
        // Read the whole input stream in write order.
        std::vector<Message> msgs;
        {
            StreamReplayer replayer(input_path);
            if (replayer.status() != StreamReplayer::Status::kOk) return false;
            Message m{};
            while (replayer.next(m)) {
                msgs.push_back(m);
            }
        }

        // Snapshot original values so overlapping/stuck faults read pre-fault
        // data.
        std::vector<double> orig_value(msgs.size());
        for (std::size_t i = 0; i < msgs.size(); ++i) {
            orig_value[i] = msgs[i].value;
        }

        std::vector<char> dropped(msgs.size(), 0);
        std::vector<FaultLabel> labels;
        labels.reserve(specs.size());

        for (const FaultSpec& spec : specs) {
            // Global indices of this stream's messages, in order.
            std::vector<std::size_t> idx;
            for (std::size_t i = 0; i < msgs.size(); ++i) {
                if (msgs[i].stream_id == spec.stream_id) idx.push_back(i);
            }
            const std::size_t n = idx.size();
            if (n == 0 || spec.duration == 0) continue;

            std::size_t onset = spec.onset_index;
            if (onset == kAutoOnset) {
                const std::size_t max_onset =
                    (n >= spec.duration) ? (n - spec.duration) : 0;
                onset = static_cast<std::size_t>(next_rand() % (max_onset + 1));
            }
            if (onset >= n) continue;
            std::size_t last = onset + spec.duration - 1;
            if (last >= n) last = n - 1;

            const std::size_t onset_global = idx[onset];
            const std::size_t last_global = idx[last];
            labels.push_back(FaultLabel{spec.stream_id, spec.type,
                                        msgs[onset_global].ts_ns,
                                        msgs[last_global].ts_ns});

            const double frozen = orig_value[onset_global];
            const auto dur = static_cast<double>(spec.duration);
            for (std::size_t j = onset; j <= last; ++j) {
                const std::size_t g = idx[j];
                switch (spec.type) {
                    case FaultType::kSpike:
                        msgs[g].value = orig_value[g] + spec.magnitude;
                        break;
                    case FaultType::kDrift: {
                        const double frac =
                            static_cast<double>(j - onset + 1) / dur;
                        msgs[g].value = orig_value[g] + spec.magnitude * frac;
                        break;
                    }
                    case FaultType::kStuckAtValue:
                        msgs[g].value = frozen;
                        break;
                    case FaultType::kDropout:
                        dropped[g] = 1;
                        break;
                }
            }
        }

        // Write the faulted output stream (dropped samples removed).
        {
            StreamRecorder rec(output_path);
            if (!rec.is_open()) return false;
            for (std::size_t i = 0; i < msgs.size(); ++i) {
                if (dropped[i]) continue;
                if (!rec.write(msgs[i])) return false;
            }
            rec.close();
        }

        // Write the ground-truth labels file.
        {
            std::ofstream out(labels_path, std::ios::trunc);
            if (!out.is_open()) return false;
            for (const FaultLabel& l : labels) {
                out << l.stream_id << '|' << detail::fault_type_name(l.type)
                    << '|' << l.t_start_ns << '|' << l.t_end_ns << '\n';
            }
            out.flush();
            if (!out.good()) return false;
        }

        return true;
    }

    // SplitMix64: deterministic, portable, no dependence on stdlib
    // distributions.
    std::uint64_t next_rand() noexcept {
        rng_state_ += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = rng_state_;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    std::uint64_t rng_state_;
};

namespace detail {

// Strict whole-field integer parse via std::from_chars: never throws, fails
// on empty/partial/non-numeric/overflowing input.
template <typename Int>
inline bool parse_int_field(const std::string& s, Int& out) noexcept {
    const char* first = s.data();
    const char* last = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(first, last, out);
    return ec == std::errc{} && ptr == last && !s.empty();
}

}  // namespace detail

// Malformed-line policy: any line with the wrong field count, an unknown
// type name, or non-numeric/overflowing numeric fields is SKIPPED (parsing
// continues with the next line); read_labels never throws on bad content.
inline std::vector<FaultLabel> read_labels(const std::string& labels_path) {
    std::vector<FaultLabel> labels;
    std::ifstream in(labels_path);
    if (!in.is_open()) return labels;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        // Split on '|' into exactly 4 fields.
        std::size_t p0 = line.find('|');
        if (p0 == std::string::npos) continue;
        std::size_t p1 = line.find('|', p0 + 1);
        if (p1 == std::string::npos) continue;
        std::size_t p2 = line.find('|', p1 + 1);
        if (p2 == std::string::npos) continue;

        const std::string id_s = line.substr(0, p0);
        const std::string type_s = line.substr(p0 + 1, p1 - p0 - 1);
        const std::string start_s = line.substr(p1 + 1, p2 - p1 - 1);
        const std::string end_s = line.substr(p2 + 1);

        FaultType type{};
        if (!detail::parse_fault_type(type_s, type)) continue;

        FaultLabel label{};
        label.type = type;
        if (!detail::parse_int_field(id_s, label.stream_id) ||
            !detail::parse_int_field(start_s, label.t_start_ns) ||
            !detail::parse_int_field(end_s, label.t_end_ns)) {
            continue;
        }
        labels.push_back(label);
    }
    return labels;
}

}  // namespace telemetry::replay
