// tests/detect/test_baseline.cpp
//
// Tests derived from SPEC.md section 3.6 (detect/baseline, Layer 2). NO
// implementation exists yet under include/detect/ -- this is the RED state
// (build must fail on the missing header, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/detect/baseline.hpp, which does not exist
// yet):
//
//   namespace telemetry::detect {
//     class EwmaBaseline {
//      public:
//       // alpha in (0, 1]; fixed for the life of the instance.
//       explicit EwmaBaseline(double alpha) noexcept;
//
//       // HOT PATH: O(1), noexcept.
//       // Per-stream state before any update(): mean = 0.0, var = 0.0.
//       // Recursion (mean_prev/var_prev are the PRE-update state):
//       //   diff      = x - mean_prev
//       //   mean_next = (1 - alpha) * mean_prev + alpha * x
//       //   var_next  = (1 - alpha) * var_prev  + alpha * diff * diff
//       void update(std::uint32_t stream_id, double value) noexcept;
//
//       // HOT PATH: O(1), noexcept. Uses the CURRENT (post the most recent
//       // update()) mean/var for stream_id -- does not itself call update().
//       //   zscore = (value - mean) / sqrt(var)
//       // except:
//       //   - stream_id has never been updated -> 0.0
//       //   - var == 0.0                       -> 0.0
//       // Never returns NaN or +/-Inf.
//       double zscore(std::uint32_t stream_id, double value) const noexcept;
//
//       // Accessors for the current per-stream state (0.0/0.0 if the
//       // stream_id has never been updated).
//       double mean(std::uint32_t stream_id) const noexcept;
//       double variance(std::uint32_t stream_id) const noexcept;
//     };
//   }
//
// Spec requirements covered in this file:
//   SPEC-3.6-EWMA-RECURSION: hand-computed known-answer sequence for mean
//     and variance (alpha = 0.5).
//   SPEC-3.6-ZSCORE-UNINITIALIZED: zscore before any update() -> 0.0.
//   SPEC-3.6-ZSCORE-ZERO-VARIANCE: zscore with var == 0 (e.g. exactly one
//     update) -> 0.0, never NaN/Inf.
//   SPEC-3.6-CONSTANT-STREAM: a long constant stream drives variance -> 0
//     and zscore of the constant value -> 0.
//   SPEC-3.6-STEP-CHANGE: a sudden step change produces a large-magnitude
//     zscore (computed against the PRE-jump baseline).
//   SPEC-3.6-PER-STREAM-INDEPENDENCE: state for one stream_id does not leak
//     into another.
// ---------------------------------------------------------------------------

#include <detect/baseline.hpp>

#include <cmath>
#include <cstdint>

#include <gtest/gtest.h>

namespace {

using telemetry::detect::EwmaBaseline;

// ---------------------------------------------------------------------------
// SPEC-3.6-ZSCORE-UNINITIALIZED
// ---------------------------------------------------------------------------
TEST(Baseline, ZscoreBeforeAnyUpdateIsZero) {
    EwmaBaseline baseline(0.5);
    EXPECT_DOUBLE_EQ(baseline.zscore(1, 12345.0), 0.0);
    EXPECT_DOUBLE_EQ(baseline.mean(1), 0.0);
    EXPECT_DOUBLE_EQ(baseline.variance(1), 0.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.6-ZSCORE-ZERO-VARIANCE: exactly one update -> var stays 0 only if
// the very first update's diff is against mean_prev == 0. Pin: a single
// update never leaves NaN/Inf, and zscore reads back a finite number.
// ---------------------------------------------------------------------------
TEST(Baseline, ZscoreNeverNanOrInfAfterSingleUpdate) {
    EwmaBaseline baseline(0.5);
    baseline.update(1, 10.0);
    const double z = baseline.zscore(1, 10.0);
    EXPECT_FALSE(std::isnan(z));
    EXPECT_FALSE(std::isinf(z));
}

TEST(Baseline, ZscoreIsZeroWheneverVarianceIsZero) {
    EwmaBaseline baseline(0.5);
    // Repeatedly feeding the same value from a zero start eventually drives
    // variance arbitrarily close to (but not exactly) zero; instead, force
    // the exact-zero-variance case directly via the documented pin: query
    // zscore for a fresh, never-updated stream_id (variance is exactly 0.0).
    EXPECT_DOUBLE_EQ(baseline.zscore(99, 42.0), 0.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.6-EWMA-RECURSION: hand-computed known-answer sequence.
// alpha = 0.5, values = [4, 6, 5, 7], starting mean = 0, var = 0.
//   t1: x=4  diff=4-0=4    mean=0.5*0+0.5*4=2      var=0.5*0+0.5*16=8
//   t2: x=6  diff=6-2=4    mean=0.5*2+0.5*6=4      var=0.5*8+0.5*16=12
//   t3: x=5  diff=5-4=1    mean=0.5*4+0.5*5=4.5    var=0.5*12+0.5*1=6.5
//   t4: x=7  diff=7-4.5=2.5 mean=0.5*4.5+0.5*7=5.75 var=0.5*6.5+0.5*6.25=6.375
// zscore(1, 7.0) after t4 = (7 - 5.75) / sqrt(6.375) = 0.49507377148834...
// (verified independently via a Python reimplementation of the same
// recursion; see test-writer report).
// ---------------------------------------------------------------------------
TEST(Baseline, EwmaKnownAnswerSequence) {
    EwmaBaseline baseline(0.5);
    baseline.update(1, 4.0);
    EXPECT_NEAR(baseline.mean(1), 2.0, 1e-9);
    EXPECT_NEAR(baseline.variance(1), 8.0, 1e-9);

    baseline.update(1, 6.0);
    EXPECT_NEAR(baseline.mean(1), 4.0, 1e-9);
    EXPECT_NEAR(baseline.variance(1), 12.0, 1e-9);

    baseline.update(1, 5.0);
    EXPECT_NEAR(baseline.mean(1), 4.5, 1e-9);
    EXPECT_NEAR(baseline.variance(1), 6.5, 1e-9);

    baseline.update(1, 7.0);
    EXPECT_NEAR(baseline.mean(1), 5.75, 1e-9);
    EXPECT_NEAR(baseline.variance(1), 6.375, 1e-9);

    EXPECT_NEAR(baseline.zscore(1, 7.0), 0.49507377148834, 1e-9);
}

// ---------------------------------------------------------------------------
// SPEC-3.6-CONSTANT-STREAM: a long constant stream drives variance -> 0 and
// zscore(constant) -> 0.
// ---------------------------------------------------------------------------
TEST(Baseline, ConstantStreamConvergesVarianceAndZscoreToZero) {
    EwmaBaseline baseline(0.1);
    for (int i = 0; i < 500; ++i) {
        baseline.update(1, 10.0);
    }
    EXPECT_NEAR(baseline.mean(1), 10.0, 1e-6);
    EXPECT_NEAR(baseline.variance(1), 0.0, 1e-6);
    EXPECT_NEAR(baseline.zscore(1, 10.0), 0.0, 1e-3);
}

// ---------------------------------------------------------------------------
// SPEC-3.6-STEP-CHANGE: after a long constant in-control regime, a sudden
// step change produces a large-magnitude zscore (measured against the
// PRE-jump mean/variance, i.e. queried before update() assimilates the
// jump).
// ---------------------------------------------------------------------------
TEST(Baseline, StepChangeProducesLargeMagnitudeZscore) {
    EwmaBaseline baseline(0.1);
    for (int i = 0; i < 100; ++i) {
        baseline.update(1, 10.0);
    }
    // Query zscore for the jumped value BEFORE assimilating it via update().
    const double z = baseline.zscore(1, 50.0);
    EXPECT_GT(std::fabs(z), 10.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.6-PER-STREAM-INDEPENDENCE
// ---------------------------------------------------------------------------
TEST(Baseline, PerStreamStateIsIndependent) {
    EwmaBaseline baseline(0.5);
    baseline.update(1, 100.0);
    baseline.update(1, 100.0);
    // Stream 2 has never been updated -- must read back the fresh-stream
    // defaults regardless of stream 1's history.
    EXPECT_DOUBLE_EQ(baseline.mean(2), 0.0);
    EXPECT_DOUBLE_EQ(baseline.variance(2), 0.0);
    EXPECT_DOUBLE_EQ(baseline.zscore(2, 100.0), 0.0);
}

}  // namespace
