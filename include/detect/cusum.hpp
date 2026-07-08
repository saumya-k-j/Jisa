#pragma once

// detect/cusum.hpp
//
// SPEC-3.7 (Layer 3): CUSUM changepoint detector on the hot path (detects a
// sustained mean shift). Self-contained per-stream running mean/variance
// (EWMA standardization) -- independent of detect/baseline; this is the
// documented design choice pinned by the tests (see DECISIONS entry).
//
// Hot-path notes:
//   - update_and_check() is O(1), noexcept, no allocation in steady state.
//     Per-stream state lives in an unordered_map keyed by stream_id; it
//     inserts only the first time a stream is seen. Same pattern as
//     feed/handler.hpp (D-011).

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace telemetry::detect {

class CusumDetector {
public:
    // alpha: EWMA rate for this detector's own running mean/variance.
    // k: CUSUM slack. h: decision threshold (|S| > h fires).
    // warmup_n: number of updates (per stream_id, counting the first seed
    //   update as update #1) during which the detector only refines its
    //   running estimate and never evaluates/fires.
    CusumDetector(double alpha, double k, double h,
                  std::size_t warmup_n = 1) noexcept
        : alpha_(alpha), k_(k), h_(h), warmup_n_(warmup_n) {}

    // HOT PATH. Returns true exactly on the call where a changepoint fires.
    bool update_and_check(std::uint32_t stream_id, double value) noexcept {
        State& st = state_[stream_id];  // inserts a zero-initialized state

        if (st.count == 0) {
            // Seed sample: initialize mean, never fires.
            st.mean = value;
            st.var = 0.0;
            st.s_pos = 0.0;
            st.s_neg = 0.0;
            st.count = 1;
            return false;
        }

        const double mean_prev = st.mean;
        const double var_prev = st.var;

        if (st.count < warmup_n_) {
            // Still warming up: refine estimate, never evaluate S+/S-.
            const double diff = value - mean_prev;
            st.mean = (1.0 - alpha_) * mean_prev + alpha_ * value;
            st.var = (1.0 - alpha_) * var_prev + alpha_ * diff * diff;
            st.count += 1;
            return false;
        }

        const double residual =
            (var_prev > 0.0) ? (value - mean_prev) / std::sqrt(var_prev) : 0.0;

        const double diff = value - mean_prev;
        st.mean = (1.0 - alpha_) * mean_prev + alpha_ * value;
        st.var = (1.0 - alpha_) * var_prev + alpha_ * diff * diff;

        st.s_pos = std::fmax(0.0, st.s_pos + residual - k_);
        st.s_neg = std::fmin(0.0, st.s_neg + residual + k_);

        const bool fired = (st.s_pos > h_) || (st.s_neg < -h_);
        if (fired) {
            st.s_pos = 0.0;
            st.s_neg = 0.0;
        }
        st.count += 1;
        return fired;
    }

private:
    struct State {
        std::size_t count = 0;
        double mean = 0.0;
        double var = 0.0;
        double s_pos = 0.0;
        double s_neg = 0.0;
    };

    double alpha_;
    double k_;
    double h_;
    std::size_t warmup_n_;
    std::unordered_map<std::uint32_t, State> state_;
};

}  // namespace telemetry::detect
