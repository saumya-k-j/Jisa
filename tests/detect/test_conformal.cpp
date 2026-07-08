// tests/detect/test_conformal.cpp
//
// Tests derived from SPEC.md section 3.8 (detect/conformal, Layer 4) and
// section 7 phase 5 ("detect layer 4 (conformal) + false-alarm-rate
// verification"). NO implementation exists yet under include/detect/ -- this
// is the RED state (build must fail on the missing detect/conformal.hpp
// header, not on logic). The statistical empirical-false-alarm-rate
// verification (SPEC section 1, measurable objective #4) lives in the
// sibling file test_false_alarm_rate.cpp; this file pins the
// ConformalThreshold interface, its exact quantile convention, adaptivity,
// the is_anomalous call-order contract, per-stream independence, and
// hot-path discipline.
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/detect/conformal.hpp, which does not exist
// yet):
//
//   namespace telemetry::detect {
//     class ConformalThreshold {
//      public:
//       // window_capacity (W): fixed per-stream ring capacity for recent
//       // nonconformity scores, given once at construction. W == 0 is
//       // legal (D-026): every window stays permanently empty, so every
//       // threshold() is +infinity and is_anomalous() is always false.
//       explicit ConformalThreshold(std::size_t window_capacity) noexcept;
//
//       // HOT PATH: noexcept, no allocation once a stream_id's ring has
//       // been created (creation happens lazily on the FIRST update() for
//       // that stream_id -- same per-stream-on-first-touch pattern as
//       // EwmaBaseline/CusumDetector -- and is a fixed-capacity ring
//       // reused thereafter; no further (re)allocation).
//       //
//       // Appends `score` to stream_id's window. If the window already
//       // holds window_capacity scores, the OLDEST score (FIFO) is evicted
//       // to make room -- i.e. it is a sliding window of the most recent
//       // window_capacity scores.
//       void update(std::uint32_t stream_id, double score) noexcept;
//
//       // HOT PATH: noexcept. Returns the (1 - alpha) empirical quantile
//       // of stream_id's CURRENT window contents (i.e. as of the most
//       // recent update() for that stream_id -- threshold() itself never
//       // mutates state).
//       //
//       // PINNED QUANTILE CONVENTION ("higher order statistic, clamped"):
//       //   Let n = number of scores currently in stream_id's window.
//       //   - n == 0 (stream never updated, or window_capacity == 0):
//       //       threshold = +infinity (std::numeric_limits<double>::infinity())
//       //       so nothing alarms before any data has been observed.
//       //   - n > 0: let s[0..n-1] be the window's scores sorted ASCENDING.
//       //       idx = ceil((1 - alpha) * n) - 1, clamped to [0, n - 1]
//       //       threshold = s[idx]
//       //   This gives: alpha == 0.0 -> s[n-1] (the maximum); alpha == 1.0
//       //   (or any alpha close to 1) -> s[0] (the minimum, via idx
//       //   clamped up from a negative/zero value).
//       //
//       //   Complexity: O(W) or O(W log W) per call is acceptable, done
//       //   into a preallocated per-call scratch buffer (a member of the
//       //   implementation) -- no heap allocation on this call.
//       double threshold(std::uint32_t stream_id, double alpha) const noexcept;
//
//       // HOT PATH: noexcept, does NOT mutate any window. Convenience:
//       //   is_anomalous(stream_id, score, alpha) == score > threshold(stream_id, alpha)
//       // computed against stream_id's window AS IT CURRENTLY STANDS --
//       // i.e. NOT including `score` itself.
//       //
//       // PINNED CALL-ORDER CONTRACT: to get the correct "out-of-sample"
//       // alarm decision for a freshly observed score, the caller MUST
//       // call is_anomalous(stream_id, score, alpha) BEFORE calling
//       // update(stream_id, score) with that same score. update() always
//       // appends unconditionally, independent of is_anomalous's result.
//       bool is_anomalous(std::uint32_t stream_id, double score,
//                          double alpha) const noexcept;
//     };
//   }
//
// Spec requirements covered in this file:
//   SPEC-3.8-QUANTILE-KNOWN-ANSWER: hand-computed known-answer window
//     (scores 1..10, alpha = 0.2 -> pinned expected threshold = 8.0), plus
//     alpha = 0.0 (max), alpha = 1.0 (min), and an eviction-after-full-window
//     variant of the same known answer (+100 offset).
//   SPEC-3.8-EMPTY-WINDOW: threshold() for a never-updated stream_id is
//     +infinity.
//   SPEC-3.8-SINGLE-ELEMENT: threshold() with exactly one score in the
//     window returns that score for any alpha in (0, 1).
//   SPEC-3.8-DUPLICATE-SCORES: a window of identical scores returns that
//     score for any alpha.
//   SPEC-3.8-EVICTION: pushing more than window_capacity scores evicts the
//     oldest (FIFO), verified via the resulting quantile.
//   SPEC-3.8-ADAPTIVITY: after a full window turnover from one score
//     regime to another, the threshold converges to the new regime's
//     known-answer quantile (not the old regime's).
//   SPEC-3.8-IS-ANOMALOUS-CALL-ORDER: is_anomalous() checked BEFORE
//     update() reflects the pre-update window; the identical raw score,
//     re-checked AFTER update() has assimilated it, can flip the decision
//     -- pinning that update() must be called AFTER the check, never
//     before.
//   SPEC-3.8-PER-STREAM-INDEPENDENCE: two streams with different score
//     distributions get independent thresholds; an unseen stream_id
//     behaves like an empty window (+infinity).
//   SPEC-3.8-HOT-PATH-NOEXCEPT: update()/threshold()/is_anomalous() are all
//     noexcept (compile-time pin via the noexcept operator).
//   SPEC-3.8-ZERO-CAPACITY-WINDOW: window_capacity == 0 is a documented
//     legal edge case (D-026 / the header doc comment above), not an
//     error: every window stays permanently empty, threshold() stays
//     +infinity, is_anomalous() stays false, and update() must not crash
//     (e.g. via a modulo-by-zero on the ring index).
// ---------------------------------------------------------------------------

#include <detect/conformal.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

namespace {

using telemetry::detect::ConformalThreshold;

// ---------------------------------------------------------------------------
// SPEC-3.8-EMPTY-WINDOW
// ---------------------------------------------------------------------------
TEST(Conformal, EmptyWindowThresholdIsPositiveInfinity) {
    ConformalThreshold ct(/*window_capacity=*/10);
    EXPECT_EQ(ct.threshold(1, 0.2), std::numeric_limits<double>::infinity());
    EXPECT_EQ(ct.threshold(1, 0.0), std::numeric_limits<double>::infinity());
    EXPECT_EQ(ct.threshold(1, 0.99), std::numeric_limits<double>::infinity());
}

// ---------------------------------------------------------------------------
// SPEC-3.8-QUANTILE-KNOWN-ANSWER
//
// Window = {1, 2, ..., 10} (n = 10), pushed in ascending order (order does
// not matter for the sorted quantile computation).
//   alpha = 0.2: idx = ceil(0.8 * 10) - 1 = ceil(8.0) - 1 = 7
//     -> s[7] in ascending-sorted {1..10} = 8.0
//   alpha = 0.0 (max of window): idx = ceil(1.0 * 10) - 1 = 9 -> s[9] = 10.0
//   alpha = 0.99 (near 1, low order statistic): idx = ceil(0.01000...*10)-1
//     = ceil(0.1000...0009) - 1 = 1 - 1 = 0 -> s[0] = 1.0
//   alpha = 1.0 (exactly): idx = ceil(0.0 * 10) - 1 = 0 - 1 = -1, clamped to
//     0 -> s[0] = 1.0
// (Hand-verified independently via a Python reimplementation of the exact
// pinned formula; see test-writer report.)
// ---------------------------------------------------------------------------
TEST(Conformal, QuantileKnownAnswerOneToTen) {
    ConformalThreshold ct(/*window_capacity=*/10);
    for (int v = 1; v <= 10; ++v) {
        ct.update(1, static_cast<double>(v));
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 8.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.0), 10.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.99), 1.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 1.0), 1.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-SINGLE-ELEMENT
// ---------------------------------------------------------------------------
TEST(Conformal, SingleElementWindowReturnsThatScoreForAnyAlpha) {
    ConformalThreshold ct(/*window_capacity=*/10);
    ct.update(1, 42.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.01), 42.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.5), 42.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.99), 42.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-DUPLICATE-SCORES
// ---------------------------------------------------------------------------
TEST(Conformal, DuplicateScoresReturnTheSameValueForAnyAlpha) {
    ConformalThreshold ct(/*window_capacity=*/5);
    for (int i = 0; i < 5; ++i) {
        ct.update(1, 5.0);
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.4), 5.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.0), 5.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.99), 5.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-EVICTION
//
// window_capacity = 5. Push 1, 2, 3, 4, 5 (window full: {1,2,3,4,5}), then
// push 100 -> the oldest score (1) is evicted -> window = {2,3,4,5,100}.
//   alpha = 0.0 (max): idx = ceil(1.0*5)-1 = 4 -> s[4] = 100.0
//   alpha = 0.99 (min, low order statistic): (1-0.99)*5 =
//     0.010000000000000009 * 5 = 0.05000000000000004, ceil = 1, idx = 0
//     -> s[0] = 2.0 (NOT 1.0 -- 1 has been evicted)
// ---------------------------------------------------------------------------
TEST(Conformal, EvictsOldestWhenWindowIsFull) {
    ConformalThreshold ct(/*window_capacity=*/5);
    for (int v = 1; v <= 5; ++v) {
        ct.update(1, static_cast<double>(v));
    }
    ct.update(1, 100.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.0), 100.0);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.99), 2.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-ADAPTIVITY
//
// window_capacity = 10. Regime A: ten identical scores of 1.0 -> any
// alpha's threshold is trivially 1.0. Then push regime B: 101..110 (ten
// ascending scores) one at a time -- this is exactly window_capacity
// pushes, so by the time all ten are in, the window has fully turned over
// and contains ONLY {101..110}; regime A has been completely evicted.
//
// After full turnover, threshold(alpha=0.2) is the same known-answer
// computation as QuantileKnownAnswerOneToTen, offset by +100:
//   idx = ceil(0.8*10)-1 = 7 -> s[7] in ascending {101..110} = 108.0
//
// A midway checkpoint (after 5 of the 10 regime-B pushes) additionally
// pins that the window is a genuine sliding window (not an instant
// atomic swap): window = {1,1,1,1,1, 101,102,103,104,105} (5 regime-A
// scores remain), sorted ascending is the same list (already sorted);
// idx = ceil(0.8*10)-1 = 7 -> s[7] = 103.0 -- neither the pure old-regime
// value (1.0) nor the pure new-regime post-turnover value (108.0).
// ---------------------------------------------------------------------------
TEST(Conformal, ThresholdConvergesToNewRegimeAfterFullWindowTurnover) {
    ConformalThreshold ct(/*window_capacity=*/10);
    for (int i = 0; i < 10; ++i) {
        ct.update(1, 1.0);
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 1.0);

    for (int v = 101; v <= 105; ++v) {
        ct.update(1, static_cast<double>(v));
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 103.0);

    for (int v = 106; v <= 110; ++v) {
        ct.update(1, static_cast<double>(v));
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 108.0);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-IS-ANOMALOUS-CALL-ORDER
//
// Window = {1..10} (n = 10), alpha = 0.2 -> threshold = 8.0 (as pinned
// above). Candidate score = 8.5:
//   - is_anomalous(1, 8.5, 0.2) BEFORE update(1, 8.5): compares 8.5 against
//     the window EXCLUDING 8.5 -> threshold = 8.0 -> 8.5 > 8.0 -> true.
//   - After calling update(1, 8.5): the oldest score (1.0) is evicted and
//     8.5 is inserted -> window = {2,3,4,5,6,7,8,8.5,9,10}. New threshold
//     at alpha=0.2: idx = ceil(0.8*10)-1 = 7 -> sorted {2,3,4,5,6,7,8,8.5,
//     9,10}[7] = 8.5.
//   - Re-checking the SAME raw score 8.5 now (is_anomalous(1, 8.5, 0.2))
//     against the POST-update window: 8.5 > 8.5 is false. This flip from
//     true -> false for the identical raw input, purely as a function of
//     whether update() has been called yet, pins that the caller must
//     check BEFORE updating to get the correct out-of-sample decision.
// ---------------------------------------------------------------------------
TEST(Conformal, IsAnomalousReflectsPreUpdateWindowNotPostUpdate) {
    ConformalThreshold ct(/*window_capacity=*/10);
    for (int v = 1; v <= 10; ++v) {
        ct.update(1, static_cast<double>(v));
    }

    EXPECT_TRUE(ct.is_anomalous(1, 8.5, 0.2));

    ct.update(1, 8.5);

    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 8.5);
    EXPECT_FALSE(ct.is_anomalous(1, 8.5, 0.2));
}

// is_anomalous() must not itself mutate the window (it is a pure query).
TEST(Conformal, IsAnomalousDoesNotMutateWindow) {
    ConformalThreshold ct(/*window_capacity=*/10);
    for (int v = 1; v <= 10; ++v) {
        ct.update(1, static_cast<double>(v));
    }
    const double before = ct.threshold(1, 0.2);
    // Call is_anomalous() several times with different candidate scores;
    // none of these may be absorbed into the window.
    (void)ct.is_anomalous(1, 999.0, 0.2);
    (void)ct.is_anomalous(1, -999.0, 0.2);
    (void)ct.is_anomalous(1, 5.0, 0.2);
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), before);
}

// ---------------------------------------------------------------------------
// SPEC-3.8-PER-STREAM-INDEPENDENCE
// ---------------------------------------------------------------------------
TEST(Conformal, PerStreamThresholdsAreIndependent) {
    ConformalThreshold ct(/*window_capacity=*/10);
    for (int v = 1; v <= 10; ++v) {
        ct.update(1, static_cast<double>(v));       // stream 1: {1..10}
        ct.update(2, static_cast<double>(v * 10));  // stream 2: {10,20,..,100}
    }
    EXPECT_DOUBLE_EQ(ct.threshold(1, 0.2), 8.0);
    EXPECT_DOUBLE_EQ(ct.threshold(2, 0.2), 80.0);

    // stream 3 has never been touched -- behaves exactly like the empty
    // window regardless of what streams 1/2 have accumulated.
    EXPECT_EQ(ct.threshold(3, 0.2), std::numeric_limits<double>::infinity());
    EXPECT_FALSE(ct.is_anomalous(3, -1e300, 0.2));  // nothing exceeds +inf
    EXPECT_FALSE(ct.is_anomalous(3, 1e300, 0.2));   // not even a huge score
}

// ---------------------------------------------------------------------------
// SPEC-3.8-HOT-PATH-NOEXCEPT
// ---------------------------------------------------------------------------
TEST(Conformal, HotPathMethodsAreNoexcept) {
    ConformalThreshold ct(16);
    static_assert(noexcept(ct.update(1, 1.0)));
    static_assert(noexcept(ct.threshold(1, 0.5)));
    static_assert(noexcept(ct.is_anomalous(1, 1.0, 0.5)));
    SUCCEED();
}

// ---------------------------------------------------------------------------
// SPEC-3.8-ZERO-CAPACITY-WINDOW
//
// window_capacity == 0 is documented as legal (D-026 / header doc comment
// above), NOT an error case to reject: a zero-capacity ring can never hold
// any score, so the window stays permanently empty no matter how many
// times update() is called for a stream_id. Per the pinned empty-window
// rule (n == 0 -> threshold = +infinity), threshold() must stay +infinity
// for every alpha, and is_anomalous() (score > +infinity) must stay false
// for every finite score and every alpha. This also pins that update()
// itself must not crash (e.g. a modulo/divide by the zero capacity when
// computing a ring index) when window_capacity == 0.
// ---------------------------------------------------------------------------
TEST(Conformal, ZeroCapacityWindowStaysEmptyForever) {
    ConformalThreshold ct(/*window_capacity=*/0);

    // Before any update(): empty window, as documented.
    EXPECT_EQ(ct.threshold(1, 0.2), std::numeric_limits<double>::infinity());
    EXPECT_FALSE(ct.is_anomalous(1, 0.0, 0.2));

    // update() must not crash with a zero-capacity ring, and must leave
    // the window (still) empty regardless of how many scores are pushed.
    for (int v = 1; v <= 10; ++v) {
        ct.update(1, static_cast<double>(v));
    }

    EXPECT_EQ(ct.threshold(1, 0.0), std::numeric_limits<double>::infinity());
    EXPECT_EQ(ct.threshold(1, 0.5), std::numeric_limits<double>::infinity());
    EXPECT_EQ(ct.threshold(1, 1.0), std::numeric_limits<double>::infinity());

    // Nothing exceeds +infinity: is_anomalous() stays false for every
    // finite score, including implausibly large ones.
    EXPECT_FALSE(ct.is_anomalous(1, 1e300, 0.2));
    EXPECT_FALSE(ct.is_anomalous(1, -1e300, 0.2));
}

}  // namespace
