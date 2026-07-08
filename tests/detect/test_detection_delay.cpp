// tests/detect/test_detection_delay.cpp
//
// End-to-end test tying replay + fault injection (SPEC 3.9, DONE) to the
// changepoint detector (SPEC 3.7, NOT YET IMPLEMENTED) to measure detection
// delay for an injected drift fault -- SPEC.md section 1, measurable
// objective #3 ("Detection delay: for injected drift faults, report samples
// between true regime change and alert") and section 7 phase 4
// ("detect layers 1-3 + tests, with detection-delay measurement").
//
// NO implementation exists yet under include/detect/ -- this is the RED
// state (build must fail on the missing detect/cusum.hpp header, not on
// logic). replay::FaultInjector / StreamRecorder / StreamReplayer already
// exist and are used here exactly as in tests/replay/test_fault_injector.cpp.
//
// ---------------------------------------------------------------------------
// Determinism: the synthetic in-control stream is generated with a
// hand-rolled SplitMix64 generator (byte-identical to
// replay::FaultInjector's internal RNG algorithm, reimplemented here
// independently so this test does not depend on FaultInjector's onset
// auto-resolution RNG draws) mapped to a small, bounded, zero-mean noise
// term. This avoids std::mt19937_64 / std::normal_distribution so the
// synthetic sequence's exact values are reproducible byte-for-byte across
// standard library implementations. The drift fault's onset is given
// EXPLICITLY (not kAutoOnset), so the injected fault is fully deterministic
// independent of FaultInjector's seed.
//
// Parameters (chosen so the test is a clear, non-flaky pass once
// detect/cusum.hpp is implemented per its pinned recursion in
// tests/detect/test_cusum.cpp):
//   N = 2000 samples, stream_id = 1, 1 ms spacing (ts_ns step = 1,000,000).
//   base value = 50.0, noise = uniform in [-0.05, +0.05] (deterministic,
//     derived from SplitMix64).
//   drift: onset_index = 1000, duration = 300, magnitude = 5.0 (a
//     sustained ramp ~100x the noise amplitude -- unambiguous once it's
//     underway).
//   CusumDetector(alpha = 0.01, k = 3.0, h = 8.0, warmup_n = 50).
//   Detection-delay bound: fires within 100 samples of the true onset
//     (independently verified via a Python reimplementation of the exact
//     recursion pinned in test_cusum.cpp against this exact synthetic
//     sequence: the true delay is 12 samples with these parameters -- the
//     100-sample bound leaves generous headroom against an
//     equivalent-but-not-bit-identical correct implementation).
// ---------------------------------------------------------------------------

#include <detect/cusum.hpp>

#include <core/message.hpp>
#include <replay/fault_injector.hpp>
#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::detect::CusumDetector;
using telemetry::replay::FaultInjector;
using telemetry::replay::FaultLabel;
using telemetry::replay::FaultSpec;
using telemetry::replay::FaultType;
using telemetry::replay::read_labels;
using telemetry::replay::StreamRecorder;
using telemetry::replay::StreamReplayer;

std::string TempPath(const std::string& suffix) {
    const std::string test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    return (std::filesystem::path(::testing::TempDir()) /
            ("detection_delay_" + test_name + "_" + suffix))
        .string();
}

// Hand-rolled SplitMix64 (same algorithm as replay::FaultInjector's internal
// RNG -- reimplemented independently here so this generator has no
// dependency on FaultInjector's private state).
std::uint64_t SplitMix64Next(std::uint64_t& state) noexcept {
    state += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

// Maps a raw 64-bit draw to a double in [0, 1) using the top 53 bits
// (standard technique; deterministic, no stdlib distribution involved).
double ToUnitInterval(std::uint64_t u64) noexcept {
    return static_cast<double>(u64 >> 11) * (1.0 / static_cast<double>(1ULL << 53));
}

// Generates a deterministic, seeded, stationary (in-control) synthetic
// stream: N samples of stream_id, 1 ms apart, value = base +/- noise_scale
// (uniform, symmetric).
std::vector<Message> GenerateStationaryStream(std::uint64_t seed, std::size_t n,
                                               std::uint32_t stream_id, double base,
                                               double noise_scale) {
    std::vector<Message> msgs;
    msgs.reserve(n);
    std::uint64_t state = seed;
    for (std::size_t i = 0; i < n; ++i) {
        const std::uint64_t draw = SplitMix64Next(state);
        const double unit = ToUnitInterval(draw);
        const double noise = (unit - 0.5) * 2.0 * noise_scale;
        Message m{};
        m.stream_id = stream_id;
        m.ts_ns = static_cast<std::int64_t>(i) * 1'000'000;
        m.value = base + noise;
        m.seq = i;
        msgs.push_back(m);
    }
    return msgs;
}

// Measures detection delay in SAMPLES: given the ground-truth label's
// t_start_ns and the sequence of (ts_ns, fired) observations in replay
// order for the SAME stream, returns the number of samples between the
// onset sample (inclusive, index 0) and the first fire at or after onset,
// or -1 if no such fire exists.
//
// Also reports (via out-params) whether any fire occurred STRICTLY before
// the onset sample (false alarm on the pre-onset segment).
long DetectionDelaySamples(const std::vector<std::int64_t>& ts_ns_in_order,
                           const std::vector<bool>& fired_in_order,
                           std::int64_t onset_ts_ns, bool* any_false_alarm_before_onset) {
    *any_false_alarm_before_onset = false;
    long onset_pos = -1;
    for (std::size_t i = 0; i < ts_ns_in_order.size(); ++i) {
        if (ts_ns_in_order[i] < onset_ts_ns && fired_in_order[i]) {
            *any_false_alarm_before_onset = true;
        }
        if (onset_pos < 0 && ts_ns_in_order[i] == onset_ts_ns) {
            onset_pos = static_cast<long>(i);
        }
    }
    if (onset_pos < 0) return -1;
    for (std::size_t i = static_cast<std::size_t>(onset_pos); i < ts_ns_in_order.size(); ++i) {
        if (fired_in_order[i]) {
            return static_cast<long>(i) - onset_pos;
        }
    }
    return -1;
}

TEST(DetectionDelay, CusumFiresAfterInjectedDriftOnsetWithinBoundAndNoEarlyFalseAlarm) {
    constexpr std::size_t kN = 2000;
    constexpr std::uint32_t kStreamId = 1;
    constexpr double kBase = 50.0;
    constexpr double kNoiseScale = 0.05;
    constexpr std::size_t kOnsetIndex = 1000;
    constexpr std::size_t kDuration = 300;
    constexpr double kMagnitude = 5.0;
    constexpr long kDelayBoundSamples = 100;

    const std::vector<Message> original =
        GenerateStationaryStream(/*seed=*/12345ULL, kN, kStreamId, kBase, kNoiseScale);

    const std::string input_path = TempPath("input.trec");
    {
        StreamRecorder rec(input_path);
        ASSERT_TRUE(rec.is_open());
        for (const auto& m : original) {
            ASSERT_TRUE(rec.write(m));
        }
        rec.close();
    }

    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    FaultSpec spec{FaultType::kDrift, kStreamId, kOnsetIndex, kDuration, kMagnitude};
    FaultInjector injector(/*seed=*/999ULL);  // irrelevant: onset is explicit, not kAutoOnset
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{1});
    EXPECT_EQ(labels[0].type, FaultType::kDrift);
    const std::int64_t onset_ts_ns = labels[0].t_start_ns;
    EXPECT_EQ(onset_ts_ns, original[kOnsetIndex].ts_ns);

    // Replay the faulted stream through the CUSUM changepoint detector.
    CusumDetector cusum(/*alpha=*/0.01, /*k=*/3.0, /*h=*/8.0, /*warmup_n=*/50);

    std::vector<std::int64_t> ts_ns_in_order;
    std::vector<bool> fired_in_order;
    {
        StreamReplayer replayer(output_path);
        ASSERT_EQ(replayer.status(), StreamReplayer::Status::kOk);
        Message m{};
        while (replayer.next(m)) {
            const bool fired = cusum.update_and_check(m.stream_id, m.value);
            ts_ns_in_order.push_back(m.ts_ns);
            fired_in_order.push_back(fired);
        }
    }

    bool any_false_alarm_before_onset = false;
    const long delay = DetectionDelaySamples(ts_ns_in_order, fired_in_order, onset_ts_ns,
                                              &any_false_alarm_before_onset);

    // (c) No false alarm before the true onset.
    EXPECT_FALSE(any_false_alarm_before_onset);

    // (a)+(b) A changepoint fires, strictly after the true onset sample,
    // within the generous pinned delay bound.
    ASSERT_GE(delay, 0) << "CUSUM never fired on the injected drift";
    EXPECT_GT(delay, 0) << "fire must be AFTER the true onset sample, not AT it";
    EXPECT_LE(delay, kDelayBoundSamples)
        << "detection delay of " << delay << " samples exceeds the pinned bound";

    // Report the measured detection delay for visibility (objective #3:
    // "report samples between true regime change and alert").
    RecordProperty("detection_delay_samples", static_cast<int>(delay));
}

}  // namespace
