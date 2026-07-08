// tests/detect/test_cusum.cpp
//
// Tests derived from SPEC.md section 3.7 (detect/changepoint, Layer 3). NO
// implementation exists yet under include/detect/ -- this is the RED state
// (build must fail on the missing header, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/detect/cusum.hpp, which does not exist yet):
//
//   namespace telemetry::detect {
//     class CusumDetector {
//      public:
//       // alpha in (0,1]: internal EWMA rate for this detector's OWN
//       // self-contained running mean/variance (independent of
//       // detect/baseline -- see SPEC-3.7 note: "self-contained running
//       // mean" is one of the two documented design choices; this repo
//       // pins that one).
//       // k: CUSUM slack (subtracted from the standardized residual before
//       //    accumulating into S+, added before accumulating into S-).
//       // h: CUSUM decision threshold; |S| > h fires.
//       // warmup_n: number of updates (per stream_id, counting the very
//       //    first "seed" update as update #1) during which the detector
//       //    only refines its running mean/variance estimate and NEVER
//       //    evaluates S+/S- or fires. Defaults to 1 (i.e. only the very
//       //    first sample -- which seeds mean/var and can never fire
//       //    anyway -- is skipped).
//       CusumDetector(double alpha, double k, double h,
//                     std::size_t warmup_n = 1) noexcept;
//
//       // HOT PATH: O(1), noexcept. Returns true exactly on the call where
//       // a changepoint fires.
//       //
//       // Per-stream state: count, mean, var, s_pos, s_neg (all start at
//       // 0 / uninitialized).
//       //
//       // On update_and_check(stream_id, x):
//       //   if this is the FIRST call ever for stream_id (count == 0):
//       //     mean = x; var = 0; s_pos = 0; s_neg = 0; count = 1;
//       //     return false.                                    // seed
//       //   mean_prev = mean; var_prev = var
//       //   if count < warmup_n:
//       //     diff = x - mean_prev
//       //     mean = (1 - alpha) * mean_prev + alpha * x
//       //     var  = (1 - alpha) * var_prev  + alpha * diff * diff
//       //     count += 1
//       //     return false                       // still warming up
//       //   residual = (var_prev > 0) ? (x - mean_prev) / sqrt(var_prev)
//       //                              : 0.0
//       //   diff = x - mean_prev
//       //   mean = (1 - alpha) * mean_prev + alpha * x
//       //   var  = (1 - alpha) * var_prev  + alpha * diff * diff
//       //   s_pos = max(0.0, s_pos + residual - k)
//       //   s_neg = min(0.0, s_neg + residual + k)
//       //   fired = (s_pos > h) || (s_neg < -h)
//       //   if fired: s_pos = 0; s_neg = 0     // reset-after-fire
//       //   count += 1
//       //   return fired
//       bool update_and_check(std::uint32_t stream_id, double value) noexcept;
//     };
//   }
//
// Spec requirements covered in this file:
//   SPEC-3.7-KNOWN-ANSWER: hand-derived (via an independent Python
//     reimplementation of the exact pinned recursion above -- see
//     test-writer report for the derivation) sequence where the exact
//     update at which S+ crosses h is known.
//   SPEC-3.7-RESET-AFTER-FIRE: the call immediately following a fire does
//     NOT immediately re-fire on an in-regime sample.
//   SPEC-3.7-FALSE-ALARM-SANITY: constant-amplitude oscillation strictly
//     inside the slack k never fires over a long fixed sequence.
//   SPEC-3.7-PER-STREAM-INDEPENDENCE: firing on one stream_id does not
//     affect another stream_id's accumulators.
// ---------------------------------------------------------------------------

#include <detect/cusum.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

namespace {

using telemetry::detect::CusumDetector;

// ---------------------------------------------------------------------------
// SPEC-3.7-KNOWN-ANSWER
//
// alpha = 0.5, k = 0.5, h = 3.0, warmup_n = 1 (default: only the seed
// sample is skipped). Input sequence (index: value):
//   0: 10.0  (seed sample -- mean=10, var=0, never fires)
//   1: 10.5  2: 9.5  3: 10.5  4: 9.5  5: 10.5  6: 9.5   (in-control
//      oscillation +/-0.5 around 10 -- must never fire)
//   7: 20.0  (sustained jump to 20 -- must fire on THIS exact update, per
//      the independent Python reimplementation of the recursion above,
//      which reports s_pos == 15.35163... > h == 3.0 at this step, having
//      been 0.0 immediately prior)
//   8..11: 20.0 (four more samples at the new level -- must NOT
//      immediately re-fire on index 8 (reset-after-fire), consistent with
//      the reimplementation showing s_pos resets to 0 at index 7 and only
//      slowly re-accumulates afterward, never re-crossing h in this
//      sequence)
// ---------------------------------------------------------------------------
TEST(Cusum, KnownAnswerFiresExactlyAtTheJumpSample) {
    CusumDetector cusum(/*alpha=*/0.5, /*k=*/0.5, /*h=*/3.0);
    const std::vector<double> values = {
        10.0, 10.5, 9.5, 10.5, 9.5, 10.5, 9.5,  // indices 0..6
        20.0, 20.0, 20.0, 20.0, 20.0,            // indices 7..11
    };

    std::vector<bool> fired(values.size());
    for (std::size_t i = 0; i < values.size(); ++i) {
        fired[i] = cusum.update_and_check(1, values[i]);
    }

    for (std::size_t i = 0; i <= 6; ++i) {
        SCOPED_TRACE(i);
        EXPECT_FALSE(fired[i]);
    }
    EXPECT_TRUE(fired[7]);
    for (std::size_t i = 8; i < fired.size(); ++i) {
        SCOPED_TRACE(i);
        EXPECT_FALSE(fired[i]);
    }
}

// ---------------------------------------------------------------------------
// SPEC-3.7-RESET-AFTER-FIRE: a dedicated, isolated check that the sample
// immediately after a fire does not itself immediately re-fire, using a
// fresh detector and a shorter sequence for clarity.
// ---------------------------------------------------------------------------
TEST(Cusum, DoesNotImmediatelyRefireOnNextInRegimeSample) {
    CusumDetector cusum(/*alpha=*/0.5, /*k=*/0.5, /*h=*/3.0);
    const std::vector<double> values = {10.0, 10.5, 9.5, 10.5,
                                         9.5,  10.5, 9.5, 20.0};
    bool fired_at_jump = false;
    for (std::size_t i = 0; i < values.size(); ++i) {
        fired_at_jump = cusum.update_and_check(1, values[i]);
    }
    ASSERT_TRUE(fired_at_jump);
    // Next sample stays at the new (now in-regime) level 20.0 -- must not
    // immediately refire.
    EXPECT_FALSE(cusum.update_and_check(1, 20.0));
}

// ---------------------------------------------------------------------------
// SPEC-3.7-FALSE-ALARM-SANITY: a fixed, longer in-control oscillation
// (+/-0.5 around 10, strictly inside slack k = 0.5 once the running
// variance stabilizes) never fires.
// ---------------------------------------------------------------------------
TEST(Cusum, ConstantAmplitudeOscillationBelowSlackNeverFires) {
    CusumDetector cusum(/*alpha=*/0.5, /*k=*/0.5, /*h=*/3.0);
    std::vector<double> values;
    values.push_back(10.0);
    for (int i = 0; i < 30; ++i) {
        values.push_back((i % 2 == 0) ? 10.5 : 9.5);
    }

    bool any_fired = false;
    for (double v : values) {
        if (cusum.update_and_check(1, v)) any_fired = true;
    }
    EXPECT_FALSE(any_fired);
}

// ---------------------------------------------------------------------------
// SPEC-3.7-PER-STREAM-INDEPENDENCE: a changepoint on stream 1 must not
// perturb stream 2's independent accumulators/state.
// ---------------------------------------------------------------------------
TEST(Cusum, PerStreamStateIsIndependent) {
    CusumDetector cusum(/*alpha=*/0.5, /*k=*/0.5, /*h=*/3.0);
    const std::vector<double> jump_seq = {
        10.0, 10.5, 9.5, 10.5, 9.5, 10.5, 9.5, 20.0,
    };
    for (double v : jump_seq) {
        cusum.update_and_check(1, v);
    }
    // Stream 2 has never been touched -- its first call must be treated as
    // a fresh seed sample (never fires), regardless of stream 1's history.
    EXPECT_FALSE(cusum.update_and_check(2, 999.0));
}

}  // namespace
