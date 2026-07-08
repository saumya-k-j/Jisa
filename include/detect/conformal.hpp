#pragma once

// detect/conformal.hpp
//
// SPEC-3.8 (Layer 4): per-stream conformal anomaly threshold. Maintains a
// sliding window of the most recent W nonconformity scores per stream and
// returns the (1 - alpha) empirical quantile as an adaptive alarm threshold,
// so the false-alarm rate tracks the target alpha (measurable objective #4).
//
// Hot-path notes:
//   - update()/threshold()/is_anomalous() are noexcept and, in steady state,
//     allocation-free. Per-stream state lives in an unordered_map keyed by
//     stream_id and inserts only the first time a stream is updated; that
//     first-sight insertion allocates the fixed-capacity ring once and never
//     (re)allocates thereafter (same first-touch pattern as EwmaBaseline /
//     CusumDetector, D-025 / D-011).
//   - threshold() sorts a COPY of the window into a preallocated mutable
//     scratch buffer (sized once at construction); it never allocates per
//     call. See DECISIONS D-026 for the mutable-member / single-threaded
//     thread-model caveat and D-027 for the quantile algorithm choice.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <unordered_map>
#include <vector>

namespace telemetry::detect {

class ConformalThreshold {
public:
    // window_capacity (W): fixed per-stream ring capacity for recent
    // nonconformity scores. W == 0 is legal (every window stays empty, so
    // every threshold() is +infinity).
    explicit ConformalThreshold(std::size_t window_capacity) noexcept
        : capacity_(window_capacity), scratch_(window_capacity) {}

    // HOT PATH. Appends `score` to stream_id's sliding window, evicting the
    // oldest score (FIFO) once the window already holds window_capacity
    // scores. The per-stream ring is allocated on the first update() for a
    // stream_id and reused thereafter.
    void update(std::uint32_t stream_id, double score) noexcept {
        State& st = state_[stream_id];  // inserts a fresh State on first sight
        if (st.buf.size() != capacity_) {
            st.buf.resize(capacity_);  // one-time first-sight ring allocation
        }
        if (capacity_ == 0) {
            return;
        }
        if (st.count < capacity_) {
            st.buf[(st.head + st.count) % capacity_] = score;
            ++st.count;
        } else {
            // Window full: overwrite the oldest slot and advance head.
            st.buf[st.head] = score;
            st.head = (st.head + 1) % capacity_;
        }
    }

    // HOT PATH. Returns the (1 - alpha) empirical quantile of stream_id's
    // CURRENT window ("higher order statistic, clamped"):
    //   n == 0 -> +infinity; otherwise sort ascending and return
    //   s[clamp(ceil((1 - alpha) * n) - 1, 0, n - 1)].
    double threshold(std::uint32_t stream_id, double alpha) const noexcept {
        auto it = state_.find(stream_id);
        if (it == state_.end() || it->second.count == 0) {
            return std::numeric_limits<double>::infinity();
        }
        const State& st = it->second;
        const std::size_t n = st.count;
        for (std::size_t i = 0; i < n; ++i) {
            scratch_[i] = st.buf[(st.head + i) % capacity_];
        }
        std::sort(scratch_.begin(), scratch_.begin() + static_cast<std::ptrdiff_t>(n));

        const double raw = std::ceil((1.0 - alpha) * static_cast<double>(n)) - 1.0;
        std::ptrdiff_t idx = static_cast<std::ptrdiff_t>(raw);
        const std::ptrdiff_t last = static_cast<std::ptrdiff_t>(n) - 1;
        if (idx < 0) {
            idx = 0;
        } else if (idx > last) {
            idx = last;
        }
        return scratch_[static_cast<std::size_t>(idx)];
    }

    // HOT PATH. Pure query (does NOT mutate any window):
    //   is_anomalous == score > threshold(stream_id, alpha)
    // computed against the window as it currently stands. The caller must
    // check BEFORE calling update() with the same score to get the correct
    // out-of-sample decision.
    bool is_anomalous(std::uint32_t stream_id, double score,
                      double alpha) const noexcept {
        return score > threshold(stream_id, alpha);
    }

private:
    struct State {
        std::vector<double> buf;   // fixed-capacity ring (size == capacity_)
        std::size_t head = 0;      // index of the oldest score
        std::size_t count = 0;     // number of scores currently held (<= W)
    };

    std::size_t capacity_;
    // Preallocated sorted-copy scratch, sized W once at construction. Mutable
    // so threshold() can stay const while reusing it (no per-call allocation);
    // safe only on the single-threaded detection path (D-026).
    mutable std::vector<double> scratch_;
    std::unordered_map<std::uint32_t, State> state_;
};

}  // namespace telemetry::detect
