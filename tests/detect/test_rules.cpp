// tests/detect/test_rules.cpp
//
// Tests derived from SPEC.md section 3.5 (detect/rules, Layer 1). NO
// implementation exists yet under include/detect/ -- this is the RED state
// (build must fail on the missing header, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/detect/rules.hpp, which does not exist yet):
//
//   namespace telemetry::detect {
//     enum class RuleResult : std::uint8_t { kOk, kOutOfBounds, kRateViolation };
//
//     struct RuleConfig {
//       std::uint32_t stream_id;
//       double min;
//       double max;
//       double max_rate_of_change;  // value units PER SECOND
//     };
//
//     // Off the hot path: parses one stream's rule config from a YAML file
//     // shaped like config/grid_eu_freq.yaml (flat mapping, not a list):
//     //   stream_id: <uint>
//     //   min: <double>
//     //   max: <double>
//     //   max_rate_of_change: <double>   # units per second
//     // Extra keys (name, units, ewma_alpha, conformal_alpha, ...) are
//     // ignored by this loader -- it only reads the four fields above.
//     // Throws std::runtime_error if: the file does not exist, is not valid
//     // YAML, is missing any of the four required fields, or has
//     // min >= max. May allocate/throw -- this runs at startup only.
//     RuleConfig load_rule_config(const std::string& path);
//
//     class RuleChecker {
//      public:
//       RuleChecker() noexcept;
//
//       // Off hot path: registers (or replaces) the bound rule for
//       // cfg.stream_id.
//       void add_rule(const RuleConfig& cfg);
//
//       // HOT PATH: O(1), noexcept. A stream_id with no registered rule
//       // always yields kOk (domain-agnostic core must not reject unknown
//       // streams).
//       //
//       // Rate-of-change: on the first sample ever seen for a stream_id (no
//       // previous sample), or when ts_ns does not strictly advance past the
//       // previous sample's ts_ns for that stream (delta_seconds <= 0), the
//       // rate check is SKIPPED for this call (never divides by zero,
//       // never flags a rate violation on that call). Otherwise:
//       //   rate = |value - prev_value| / ((ts_ns - prev_ts_ns) / 1e9)
//       //   rate violation iff rate > max_rate_of_change.
//       // Bounds check: OUT_OF_BOUNDS iff value < min || value > max.
//       // PRECEDENCE: if both an out-of-bounds condition and a rate
//       // violation would fire on the same call, kOutOfBounds is returned.
//       // In all cases the previous-sample state used for the NEXT call's
//       // rate check is updated to (value, ts_ns), regardless of the
//       // result.
//       RuleResult check(std::uint32_t stream_id, double value,
//                         std::int64_t ts_ns) noexcept;
//     };
//   }
//
// Spec requirements covered in this file:
//   SPEC-3.5-BOUNDS: value within [min, max] -> kOk; outside -> kOutOfBounds.
//   SPEC-3.5-RATE: |dv/dt| (value units per second, from ts_ns deltas)
//     exceeding max_rate_of_change -> kRateViolation.
//   SPEC-3.5-RATE-FIRST-SAMPLE: no previous sample -> no rate check.
//   SPEC-3.5-RATE-ZERO-DT: non-advancing timestamp -> no rate check (no
//     divide-by-zero / no spurious violation).
//   SPEC-3.5-PRECEDENCE: bounds violation takes precedence over rate
//     violation when both fire.
//   SPEC-3.5-UNKNOWN-STREAM: unconfigured stream_id always passes (kOk).
//   SPEC-3.5-CONFIG-LOAD: load_rule_config() reads the pinned flat YAML
//     schema (matches config/grid_eu_freq.yaml's shape) and the loaded
//     values then drive check() as configured via add_rule().
//   SPEC-3.5-CONFIG-LOAD-FAILURE: missing file / missing required field /
//     inverted bounds -> load_rule_config() throws cleanly.
// ---------------------------------------------------------------------------

#include <detect/rules.hpp>

#include <cstdint>
#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

#ifndef DETECT_FIXTURE_DIR
#define DETECT_FIXTURE_DIR "."
#endif
#ifndef CONFIG_DIR
#define CONFIG_DIR "."
#endif

namespace {

using telemetry::detect::load_rule_config;
using telemetry::detect::RuleChecker;
using telemetry::detect::RuleConfig;
using telemetry::detect::RuleResult;

RuleConfig MakeConfig(std::uint32_t stream_id, double min, double max,
                       double max_rate_of_change) {
    RuleConfig cfg{};
    cfg.stream_id = stream_id;
    cfg.min = min;
    cfg.max = max;
    cfg.max_rate_of_change = max_rate_of_change;
    return cfg;
}

// ---------------------------------------------------------------------------
// SPEC-3.5-BOUNDS
// ---------------------------------------------------------------------------
TEST(Rules, ValueWithinBoundsIsOk) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 1000.0));
    EXPECT_EQ(checker.check(1, 50.0, 1'000'000'000), RuleResult::kOk);
}

TEST(Rules, ValueBelowMinIsOutOfBounds) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 1000.0));
    EXPECT_EQ(checker.check(1, -1.0, 1'000'000'000), RuleResult::kOutOfBounds);
}

TEST(Rules, ValueAboveMaxIsOutOfBounds) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 1000.0));
    EXPECT_EQ(checker.check(1, 101.0, 1'000'000'000), RuleResult::kOutOfBounds);
}

TEST(Rules, BoundaryValuesAreOk) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 1000.0));
    EXPECT_EQ(checker.check(1, 0.0, 1'000'000'000), RuleResult::kOk);
    EXPECT_EQ(checker.check(1, 100.0, 2'000'000'000), RuleResult::kOk);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-RATE-FIRST-SAMPLE: no previous sample -> no rate check, even for
// a value whose absolute magnitude alone would look like a huge rate if
// (incorrectly) compared against an implicit zero baseline.
// ---------------------------------------------------------------------------
TEST(Rules, FirstSampleNeverTriggersRateViolation) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.1));
    // Huge value, but it's the very first sample for stream 1.
    EXPECT_EQ(checker.check(1, 500.0, 1'000'000'000), RuleResult::kOk);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-RATE: units are value-units per SECOND, computed from the ts_ns
// delta. 1 second apart, max_rate_of_change = 0.5 -> a 0.6 jump violates.
// ---------------------------------------------------------------------------
TEST(Rules, RateViolationDetectedAcrossOneSecondGap) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    // 1,000,000,000 ns later, value moves by 0.6 -> rate = 0.6 Hz/s > 0.5.
    EXPECT_EQ(checker.check(1, 50.6, 1'000'000'000), RuleResult::kRateViolation);
}

TEST(Rules, RateWithinLimitAcrossOneSecondGapIsOk) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    // Exactly at the limit (rate == max_rate_of_change) is NOT a violation
    // (strict inequality: rate > max_rate_of_change fires).
    EXPECT_EQ(checker.check(1, 50.5, 1'000'000'000), RuleResult::kOk);
}

TEST(Rules, RateComputedOverHalfSecondGapDoublesEffectiveRate) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    // 0.3 change over 0.5s -> rate = 0.6 Hz/s > 0.5 -> violation, even
    // though the raw delta (0.3) alone would look "under" 0.5.
    EXPECT_EQ(checker.check(1, 50.3, 500'000'000), RuleResult::kRateViolation);
}

TEST(Rules, RateViolationSymmetricForNegativeChange) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    EXPECT_EQ(checker.check(1, 49.4, 1'000'000'000), RuleResult::kRateViolation);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-RATE-ZERO-DT: a non-advancing (equal or reversed) timestamp never
// produces a rate check (no divide-by-zero, no spurious violation).
// ---------------------------------------------------------------------------
TEST(Rules, NonAdvancingTimestampSkipsRateCheck) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 1'000'000'000), RuleResult::kOk);
    // Same timestamp, huge value jump: must not divide by zero / must not
    // report kRateViolation from this call.
    EXPECT_EQ(checker.check(1, 999.0, 1'000'000'000), RuleResult::kOk);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-PRECEDENCE: out-of-bounds wins when both conditions fire on the
// same call.
// ---------------------------------------------------------------------------
TEST(Rules, OutOfBoundsTakesPrecedenceOverRateViolation) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 0.1));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    // 200.0 is both out-of-bounds (>100) AND a huge rate change over 1s.
    EXPECT_EQ(checker.check(1, 200.0, 1'000'000'000), RuleResult::kOutOfBounds);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-UNKNOWN-STREAM: a stream_id with no registered rule always passes,
// no matter how extreme the value -- the domain-agnostic core must not
// reject unknown streams.
// ---------------------------------------------------------------------------
TEST(Rules, UnconfiguredStreamAlwaysOk) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, 0.0, 100.0, 0.1));
    EXPECT_EQ(checker.check(42, 1e300, 0), RuleResult::kOk);
    EXPECT_EQ(checker.check(42, -1e300, 1), RuleResult::kOk);
}

// Independent per-stream state: a rate violation on stream 1 must not affect
// stream 2's first-sample (no-previous-sample) exemption, and vice versa.
TEST(Rules, PerStreamStateIsIndependent) {
    RuleChecker checker;
    checker.add_rule(MakeConfig(1, -1000.0, 1000.0, 0.5));
    checker.add_rule(MakeConfig(2, -1000.0, 1000.0, 0.5));
    ASSERT_EQ(checker.check(1, 50.0, 0), RuleResult::kOk);
    ASSERT_EQ(checker.check(1, 100.0, 1'000'000'000), RuleResult::kRateViolation);
    // Stream 2's first sample is unaffected by stream 1's history.
    EXPECT_EQ(checker.check(2, 900.0, 1'000'000'000), RuleResult::kOk);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-CONFIG-LOAD: load a single stream's rule config from the pinned
// flat YAML schema (matches config/grid_eu_freq.yaml's shape) and drive
// check() with it.
// ---------------------------------------------------------------------------
TEST(RulesConfigLoad, LoadsGoodFixtureYamlFields) {
    const RuleConfig cfg =
        load_rule_config(std::string(DETECT_FIXTURE_DIR) + "/good_rule.yaml");
    EXPECT_EQ(cfg.stream_id, 7u);
    EXPECT_DOUBLE_EQ(cfg.min, -10.0);
    EXPECT_DOUBLE_EQ(cfg.max, 10.0);
    EXPECT_DOUBLE_EQ(cfg.max_rate_of_change, 2.0);
}

TEST(RulesConfigLoad, LoadedConfigDrivesRuleChecker) {
    const RuleConfig cfg =
        load_rule_config(std::string(DETECT_FIXTURE_DIR) + "/good_rule.yaml");
    RuleChecker checker;
    checker.add_rule(cfg);
    EXPECT_EQ(checker.check(7, 0.0, 0), RuleResult::kOk);
    EXPECT_EQ(checker.check(7, 20.0, 1'000'000'000), RuleResult::kOutOfBounds);
}

// Real project config -- confirms the pinned schema matches the actual
// domain config shipped in the repo (config/grid_eu_freq.yaml), not just a
// hand-crafted test fixture.
TEST(RulesConfigLoad, LoadsRealGridEuFreqConfig) {
    const RuleConfig cfg =
        load_rule_config(std::string(CONFIG_DIR) + "/grid_eu_freq.yaml");
    EXPECT_EQ(cfg.stream_id, 1u);
    EXPECT_DOUBLE_EQ(cfg.min, 45.0);
    EXPECT_DOUBLE_EQ(cfg.max, 55.0);
    EXPECT_DOUBLE_EQ(cfg.max_rate_of_change, 0.5);
}

// ---------------------------------------------------------------------------
// SPEC-3.5-CONFIG-LOAD-FAILURE
// ---------------------------------------------------------------------------
TEST(RulesConfigLoad, MissingFileThrows) {
    EXPECT_THROW(
        load_rule_config(std::string(DETECT_FIXTURE_DIR) + "/does_not_exist.yaml"),
        std::runtime_error);
}

TEST(RulesConfigLoad, MissingRequiredFieldThrows) {
    EXPECT_THROW(
        load_rule_config(std::string(DETECT_FIXTURE_DIR) + "/missing_field_rule.yaml"),
        std::runtime_error);
}

TEST(RulesConfigLoad, InvertedBoundsThrows) {
    EXPECT_THROW(
        load_rule_config(std::string(DETECT_FIXTURE_DIR) + "/bad_bounds_rule.yaml"),
        std::runtime_error);
}

}  // namespace
