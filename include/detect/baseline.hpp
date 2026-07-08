#pragma once

// detect/baseline.hpp
//
// SPEC-3.6 (Layer 2): per-stream online EWMA of mean and variance (O(1)
// update), emitting a z-score.
//
// Hot-path notes:
//   - update()/zscore()/mean()/variance() are O(1), noexcept, no allocation
//     in steady state. Per-stream state lives in an unordered_map keyed by
//     stream_id; it inserts only the first time a stream is updated. Const
//     accessors never insert. Same pattern as feed/handler.hpp (D-011).

#include <cmath>
#include <cstdint>
#include <unordered_map>

namespace telemetry::detect {

class EwmaBaseline {
public:
    // alpha in (0, 1]; fixed for the life of the instance.
    explicit EwmaBaseline(double alpha) noexcept : alpha_(alpha) {}

    // HOT PATH. Recursion (mean_prev/var_prev are the PRE-update state):
    //   diff      = x - mean_prev
    //   mean_next = (1 - alpha) * mean_prev + alpha * x
    //   var_next  = (1 - alpha) * var_prev  + alpha * diff * diff
    void update(std::uint32_t stream_id, double value) noexcept {
        State& st = state_[stream_id];  // inserts {0.0, 0.0} on first sight
        const double diff = value - st.mean;
        st.mean = (1.0 - alpha_) * st.mean + alpha_ * value;
        st.var = (1.0 - alpha_) * st.var + alpha_ * diff * diff;
    }

    // HOT PATH. Uses the CURRENT mean/var for stream_id. Returns 0.0 if the
    // stream has never been updated or var == 0.0; never NaN/Inf.
    double zscore(std::uint32_t stream_id, double value) const noexcept {
        auto it = state_.find(stream_id);
        if (it == state_.end() || it->second.var == 0.0) {
            return 0.0;
        }
        return (value - it->second.mean) / std::sqrt(it->second.var);
    }

    double mean(std::uint32_t stream_id) const noexcept {
        auto it = state_.find(stream_id);
        return it == state_.end() ? 0.0 : it->second.mean;
    }

    double variance(std::uint32_t stream_id) const noexcept {
        auto it = state_.find(stream_id);
        return it == state_.end() ? 0.0 : it->second.var;
    }

private:
    struct State {
        double mean = 0.0;
        double var = 0.0;
    };

    double alpha_;
    std::unordered_map<std::uint32_t, State> state_;
};

}  // namespace telemetry::detect
