// tests/replay/test_fault_injector.cpp
//
// Tests derived from SPEC.md section 3.9 (replay + fault injection) and the
// fault-injection skill (.claude/skills/fault-injection/SKILL.md): fault
// taxonomy {spike, drift, stuck-at-value, dropout}, ground-truth labeling
// ("stream, t_start, t_end, type"). NO implementation exists yet under
// include/replay/ -- this is the RED state (build must fail on missing
// headers, not on logic).
//
// ---------------------------------------------------------------------------
// Pinned interface (see include/replay/fault_injector.hpp, which does not
// exist yet):
//
//   namespace telemetry::replay {
//     enum class FaultType : std::uint8_t {
//       kSpike, kDrift, kStuckAtValue, kDropout
//     };
//
//     // Sentinel: FaultSpec::onset_index == kAutoOnset means "the injector
//     // picks a pseudorandom onset (in-range for the target stream) driven
//     // by FaultInjector's seed", used for the seed-determinism tests below.
//     inline constexpr std::size_t kAutoOnset = <implementation-defined max>;
//
//     struct FaultSpec {
//       FaultType type;
//       std::uint32_t stream_id;   // which stream (by Message.stream_id) to
//                                  // inject into
//       std::size_t onset_index;  // 0-based index among THAT stream's
//                                  // messages where the fault begins, or
//                                  // kAutoOnset
//       std::size_t duration;     // number of consecutive samples (of that
//                                  // stream) affected, >= 1
//       double magnitude;         // spike: additive offset applied to every
//                                  //   affected sample's value
//                                  // drift: total additive ramp reached at
//                                  //   the LAST affected sample (linear from
//                                  //   a 1/duration fraction up to 1.0)
//                                  // stuck-at-value / dropout: unused
//     };
//
//     struct FaultLabel {
//       std::uint32_t stream_id;
//       FaultType type;
//       std::int64_t t_start_ns;  // ts_ns of the onset sample (from the
//                                 // ORIGINAL, unfaulted stream)
//       std::int64_t t_end_ns;    // ts_ns of the last affected sample (from
//                                 // the ORIGINAL, unfaulted stream)
//     };
//     bool operator==(const FaultLabel&, const FaultLabel&) noexcept;
//
//     class FaultInjector {
//     public:
//       explicit FaultInjector(std::uint64_t seed) noexcept;
//       // Reads the recorded stream at input_path (StreamRecorder/
//       // StreamReplayer format), applies every spec in `specs`, writes the
//       // faulted stream to output_path (same file format) and the
//       // ground-truth labels to labels_path (one FaultLabel per line, see
//       // pinned line format below). Returns true on success.
//       bool inject(const std::string& input_path,
//                   const std::string& output_path,
//                   const std::string& labels_path,
//                   const std::vector<FaultSpec>& specs) noexcept;
//     };
//
//     // Pinned label file format (the skill leaves the file format open, so
//     // this repo pins one): one label per line, pipe-delimited, no header
//     // row:
//     //   <stream_id>|<type_name>|<t_start_ns>|<t_end_ns>
//     // where <type_name> in {spike, drift, stuck_at_value, dropout}.
//     std::vector<FaultLabel> read_labels(const std::string& labels_path);
//   }
//
// PINNED PER-FAULT-TYPE SEMANTICS (from the skill's taxonomy, made concrete
// and testable):
//   spike: for j in [onset, onset+duration): faulted.value = original.value
//     + magnitude. Isolated / short by convention (duration typically 1).
//   drift: for j in [onset, onset+duration): faulted.value = original.value
//     + magnitude * (j - onset + 1) / duration -- a linear ramp starting
//     just above 0 at onset and reaching exactly `magnitude` at the last
//     affected sample. Samples outside the window are unaffected (the ramp
//     does not persist past onset+duration).
//   stuck-at-value: for j in [onset, onset+duration): faulted.value =
//     original.value AT INDEX onset (frozen at the pre-fault value).
//   dropout: samples at indices [onset, onset+duration) for the targeted
//     stream_id are REMOVED from the output stream entirely (not merely
//     zeroed); all other messages (other indices of that stream, and all
//     messages of other streams) are preserved, bit-identical, and in
//     order.
//   In every case, the ground-truth label's t_start_ns/t_end_ns are the
//   ORIGINAL (pre-fault) timestamps of the onset and last-affected samples,
//   even for dropout where those samples no longer exist in the output.
//
// Spec requirements covered in this file:
//   SPEC-3.9-FAULT-SPIKE / SPEC-3.9-FAULT-DRIFT / SPEC-3.9-FAULT-STUCK /
//   SPEC-3.9-FAULT-DROPOUT: each fault type transforms exactly the labeled
//     window and leaves everything else bit-identical.
//   SPEC-3.9-LABELS-ROUNDTRIP: the ground-truth label file round-trips
//     (write via inject(), read via read_labels()).
//   SPEC-3.9-INJECTION-DETERMINISM: same seed + same input -> bit-identical
//     faulted stream + labels; different seed -> different result (for
//     auto-resolved onsets).
// ---------------------------------------------------------------------------

#include <replay/fault_injector.hpp>
#include <replay/recorder.hpp>
#include <replay/replayer.hpp>

#include <core/message.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using telemetry::Message;
using telemetry::replay::FaultInjector;
using telemetry::replay::FaultLabel;
using telemetry::replay::FaultSpec;
using telemetry::replay::FaultType;
using telemetry::replay::kAutoOnset;
using telemetry::replay::read_labels;
using telemetry::replay::StreamRecorder;
using telemetry::replay::StreamReplayer;

std::string TempPath(const std::string& suffix) {
    const std::string test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    return (std::filesystem::path(::testing::TempDir()) /
            ("fault_injector_" + test_name + "_" + suffix))
        .string();
}

Message MakeMessage(std::uint32_t stream_id, std::int64_t ts_ns, double value,
                     std::uint64_t seq) {
    Message m{};
    m.stream_id = stream_id;
    m.ts_ns = ts_ns;
    m.value = value;
    m.seq = seq;
    return m;
}

// A deterministic base stream: two interleaved streams (1 and 2), 10
// samples each, evenly spaced timestamps, monotonically increasing values.
// Returns the FULL interleaved vector in write order.
std::vector<Message> BaseStream() {
    std::vector<Message> msgs;
    for (std::uint64_t i = 0; i < 10; ++i) {
        msgs.push_back(MakeMessage(1, 1000 + static_cast<std::int64_t>(i) * 10,
                                    10.0 + static_cast<double>(i), i));
        msgs.push_back(MakeMessage(2, 5000 + static_cast<std::int64_t>(i) * 10,
                                    100.0 + static_cast<double>(i) * 2.0, i));
    }
    return msgs;
}

std::vector<Message> OnlyStream(const std::vector<Message>& all, std::uint32_t stream_id) {
    std::vector<Message> out;
    for (const auto& m : all) {
        if (m.stream_id == stream_id) out.push_back(m);
    }
    return out;
}

std::string RecordStream(const std::string& path, const std::vector<Message>& msgs) {
    StreamRecorder rec(path);
    for (const auto& m : msgs) {
        rec.write(m);
    }
    rec.close();
    return path;
}

std::vector<Message> ReadAllMessages(const std::string& path) {
    StreamReplayer replayer(path);
    std::vector<Message> out;
    Message m{};
    while (replayer.next(m)) {
        out.push_back(m);
    }
    return out;
}

void ExpectMessageFieldsEqual(const Message& a, const Message& b) {
    EXPECT_EQ(a.stream_id, b.stream_id);
    EXPECT_EQ(a.ts_ns, b.ts_ns);
    EXPECT_DOUBLE_EQ(a.value, b.value);
    EXPECT_EQ(a.seq, b.seq);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-FAULT-SPIKE
// ---------------------------------------------------------------------------
TEST(FaultInjector, SpikeAddsMagnitudeOnlyToAffectedSampleOnTargetStream) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    const std::vector<Message> stream1 = OnlyStream(original, 1);
    ASSERT_GE(stream1.size(), std::size_t{4});

    FaultSpec spec{FaultType::kSpike, /*stream_id=*/1, /*onset_index=*/3,
                    /*duration=*/1, /*magnitude=*/1000.0};
    FaultInjector injector(/*seed=*/42);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<Message> faulted = ReadAllMessages(output_path);
    ASSERT_EQ(faulted.size(), original.size());

    for (std::size_t i = 0; i < original.size(); ++i) {
        SCOPED_TRACE(i);
        const bool is_spiked_sample =
            (original[i].stream_id == 1 && original[i].seq == stream1[3].seq);
        if (is_spiked_sample) {
            EXPECT_EQ(faulted[i].stream_id, original[i].stream_id);
            EXPECT_EQ(faulted[i].ts_ns, original[i].ts_ns);
            EXPECT_EQ(faulted[i].seq, original[i].seq);
            EXPECT_DOUBLE_EQ(faulted[i].value, original[i].value + 1000.0);
        } else {
            ExpectMessageFieldsEqual(faulted[i], original[i]);
        }
    }

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{1});
    EXPECT_EQ(labels[0].stream_id, 1u);
    EXPECT_EQ(labels[0].type, FaultType::kSpike);
    EXPECT_EQ(labels[0].t_start_ns, stream1[3].ts_ns);
    EXPECT_EQ(labels[0].t_end_ns, stream1[3].ts_ns);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-FAULT-DRIFT: linear ramp over the window, magnitude reached
// exactly at the last affected sample; unaffected elsewhere.
// ---------------------------------------------------------------------------
TEST(FaultInjector, DriftRampsLinearlyOverWindowThenStops) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    const std::vector<Message> stream1 = OnlyStream(original, 1);
    ASSERT_GE(stream1.size(), std::size_t{6});

    const std::size_t onset = 2;
    const std::size_t duration = 4;
    const double magnitude = 8.0;
    FaultSpec spec{FaultType::kDrift, 1, onset, duration, magnitude};
    FaultInjector injector(7);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<Message> faulted_stream1 = OnlyStream(ReadAllMessages(output_path), 1);
    ASSERT_EQ(faulted_stream1.size(), stream1.size());

    for (std::size_t j = 0; j < stream1.size(); ++j) {
        SCOPED_TRACE(j);
        if (j >= onset && j < onset + duration) {
            const double fraction = static_cast<double>(j - onset + 1) / static_cast<double>(duration);
            EXPECT_DOUBLE_EQ(faulted_stream1[j].value, stream1[j].value + magnitude * fraction);
        } else {
            EXPECT_DOUBLE_EQ(faulted_stream1[j].value, stream1[j].value);
        }
    }

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{1});
    EXPECT_EQ(labels[0].type, FaultType::kDrift);
    EXPECT_EQ(labels[0].t_start_ns, stream1[onset].ts_ns);
    EXPECT_EQ(labels[0].t_end_ns, stream1[onset + duration - 1].ts_ns);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-FAULT-STUCK: value frozen at the pre-fault (onset) value for the
// whole window.
// ---------------------------------------------------------------------------
TEST(FaultInjector, StuckAtValueFreezesAtOnsetValueForDuration) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    const std::vector<Message> stream2 = OnlyStream(original, 2);
    ASSERT_GE(stream2.size(), std::size_t{5});

    const std::size_t onset = 1;
    const std::size_t duration = 3;
    FaultSpec spec{FaultType::kStuckAtValue, 2, onset, duration, /*magnitude=*/0.0};
    FaultInjector injector(99);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<Message> faulted_stream2 = OnlyStream(ReadAllMessages(output_path), 2);
    ASSERT_EQ(faulted_stream2.size(), stream2.size());

    const double frozen_value = stream2[onset].value;
    for (std::size_t j = 0; j < stream2.size(); ++j) {
        SCOPED_TRACE(j);
        if (j >= onset && j < onset + duration) {
            EXPECT_DOUBLE_EQ(faulted_stream2[j].value, frozen_value);
        } else {
            EXPECT_DOUBLE_EQ(faulted_stream2[j].value, stream2[j].value);
        }
        // Timestamps and seq are never altered by any fault type.
        EXPECT_EQ(faulted_stream2[j].ts_ns, stream2[j].ts_ns);
        EXPECT_EQ(faulted_stream2[j].seq, stream2[j].seq);
    }

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{1});
    EXPECT_EQ(labels[0].stream_id, 2u);
    EXPECT_EQ(labels[0].type, FaultType::kStuckAtValue);
    EXPECT_EQ(labels[0].t_start_ns, stream2[onset].ts_ns);
    EXPECT_EQ(labels[0].t_end_ns, stream2[onset + duration - 1].ts_ns);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-FAULT-DROPOUT: affected samples are removed entirely; everything
// else (both the rest of that stream and the other stream) survives
// bit-identical and in order.
// ---------------------------------------------------------------------------
TEST(FaultInjector, DropoutRemovesMessagesInWindowLeavesRestIntact) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    const std::vector<Message> stream1 = OnlyStream(original, 1);
    ASSERT_GE(stream1.size(), std::size_t{8});

    const std::size_t onset = 5;
    const std::size_t duration = 2;
    FaultSpec spec{FaultType::kDropout, 1, onset, duration, /*magnitude=*/0.0};
    FaultInjector injector(5);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<Message> faulted = ReadAllMessages(output_path);
    EXPECT_EQ(faulted.size(), original.size() - duration);

    const std::vector<Message> faulted_stream1 = OnlyStream(faulted, 1);
    ASSERT_EQ(faulted_stream1.size(), stream1.size() - duration);

    // Every remaining stream-1 sample is bit-identical to some original
    // stream-1 sample, and none of them are the two dropped seqs.
    std::vector<std::uint64_t> remaining_seqs;
    for (const auto& m : faulted_stream1) remaining_seqs.push_back(m.seq);
    for (std::size_t j = onset; j < onset + duration; ++j) {
        EXPECT_EQ(std::find(remaining_seqs.begin(), remaining_seqs.end(), stream1[j].seq),
                  remaining_seqs.end());
    }
    // Order preserved for the surviving samples.
    std::size_t k = 0;
    for (std::size_t j = 0; j < stream1.size(); ++j) {
        if (j >= onset && j < onset + duration) continue;
        SCOPED_TRACE(j);
        ExpectMessageFieldsEqual(faulted_stream1[k], stream1[j]);
        ++k;
    }

    // Stream 2 is completely untouched (same count, all bit-identical).
    const std::vector<Message> stream2 = OnlyStream(original, 2);
    const std::vector<Message> faulted_stream2 = OnlyStream(faulted, 2);
    ASSERT_EQ(faulted_stream2.size(), stream2.size());
    for (std::size_t j = 0; j < stream2.size(); ++j) {
        SCOPED_TRACE(j);
        ExpectMessageFieldsEqual(faulted_stream2[j], stream2[j]);
    }

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{1});
    EXPECT_EQ(labels[0].type, FaultType::kDropout);
    EXPECT_EQ(labels[0].t_start_ns, stream1[onset].ts_ns);
    EXPECT_EQ(labels[0].t_end_ns, stream1[onset + duration - 1].ts_ns);
}

// ---------------------------------------------------------------------------
// SPEC-3.9-LABELS-ROUNDTRIP: labels written by inject() and re-read by
// read_labels() compare field-equal (multiple faults across streams).
// ---------------------------------------------------------------------------
TEST(FaultInjector, LabelFileRoundTripsMultipleFaults) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    const std::vector<Message> stream1 = OnlyStream(original, 1);
    const std::vector<Message> stream2 = OnlyStream(original, 2);

    std::vector<FaultSpec> specs = {
        {FaultType::kSpike, 1, 0, 1, 50.0},
        {FaultType::kStuckAtValue, 2, 3, 2, 0.0},
    };
    FaultInjector injector(1);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, specs));

    const std::vector<FaultLabel> labels = read_labels(labels_path);
    ASSERT_EQ(labels.size(), std::size_t{2});

    const FaultLabel expected_spike{1u, FaultType::kSpike, stream1[0].ts_ns, stream1[0].ts_ns};
    const FaultLabel expected_stuck{2u, FaultType::kStuckAtValue, stream2[3].ts_ns, stream2[4].ts_ns};

    EXPECT_EQ(labels[0], expected_spike);
    EXPECT_EQ(labels[1], expected_stuck);
}

// ---------------------------------------------------------------------------
// A stream not targeted by any FaultSpec is entirely unaffected (belt and
// suspenders alongside the per-fault-type checks above).
// ---------------------------------------------------------------------------
TEST(FaultInjector, UntargetedStreamIsBitIdenticalToOriginal) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);
    const std::string output_path = TempPath("output.trec");
    const std::string labels_path = TempPath("labels.txt");

    FaultSpec spec{FaultType::kSpike, /*stream_id=*/1, 0, 1, 999.0};
    FaultInjector injector(3);
    ASSERT_TRUE(injector.inject(input_path, output_path, labels_path, {spec}));

    const std::vector<Message> faulted_stream2 = OnlyStream(ReadAllMessages(output_path), 2);
    const std::vector<Message> stream2 = OnlyStream(original, 2);
    ASSERT_EQ(faulted_stream2.size(), stream2.size());
    for (std::size_t j = 0; j < stream2.size(); ++j) {
        ExpectMessageFieldsEqual(faulted_stream2[j], stream2[j]);
    }
}

// ---------------------------------------------------------------------------
// SPEC-3.9-INJECTION-DETERMINISM: same seed + same input + an
// auto-resolved onset -> bit-identical faulted stream and labels across two
// independent runs.
// ---------------------------------------------------------------------------
TEST(FaultInjector, SameSeedSameInputProducesByteIdenticalOutputAndLabels) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);

    FaultSpec spec{FaultType::kSpike, 1, kAutoOnset, 1, 500.0};

    const std::string out_a = TempPath("out_a.trec");
    const std::string labels_a = TempPath("labels_a.txt");
    FaultInjector injector_a(0xABCDEF1234ULL);
    ASSERT_TRUE(injector_a.inject(input_path, out_a, labels_a, {spec}));

    const std::string out_b = TempPath("out_b.trec");
    const std::string labels_b = TempPath("labels_b.txt");
    FaultInjector injector_b(0xABCDEF1234ULL);
    ASSERT_TRUE(injector_b.inject(input_path, out_b, labels_b, {spec}));

    // Byte-identical output stream files.
    std::ifstream fa(out_a, std::ios::binary);
    std::ifstream fb(out_b, std::ios::binary);
    std::vector<char> bytes_a((std::istreambuf_iterator<char>(fa)), std::istreambuf_iterator<char>());
    std::vector<char> bytes_b((std::istreambuf_iterator<char>(fb)), std::istreambuf_iterator<char>());
    EXPECT_EQ(bytes_a, bytes_b);

    EXPECT_EQ(read_labels(labels_a), read_labels(labels_b));
}

// ---------------------------------------------------------------------------
// SPEC-3.9-INJECTION-DETERMINISM: a different seed with the same input and
// an auto-resolved onset produces a DIFFERENT result (either a different
// onset choice, reflected in the faulted stream bytes or the labels).
// ---------------------------------------------------------------------------
TEST(FaultInjector, DifferentSeedProducesDifferentResult) {
    const std::vector<Message> original = BaseStream();
    const std::string input_path = RecordStream(TempPath("input.trec"), original);

    FaultSpec spec{FaultType::kSpike, 1, kAutoOnset, 1, 500.0};

    const std::string out_a = TempPath("out_a.trec");
    const std::string labels_a = TempPath("labels_a.txt");
    FaultInjector injector_a(1);
    ASSERT_TRUE(injector_a.inject(input_path, out_a, labels_a, {spec}));

    const std::string out_b = TempPath("out_b.trec");
    const std::string labels_b = TempPath("labels_b.txt");
    FaultInjector injector_b(2);
    ASSERT_TRUE(injector_b.inject(input_path, out_b, labels_b, {spec}));

    std::ifstream fa(out_a, std::ios::binary);
    std::ifstream fb(out_b, std::ios::binary);
    std::vector<char> bytes_a((std::istreambuf_iterator<char>(fa)), std::istreambuf_iterator<char>());
    std::vector<char> bytes_b((std::istreambuf_iterator<char>(fb)), std::istreambuf_iterator<char>());

    const bool streams_differ = (bytes_a != bytes_b);
    const bool labels_differ = !(read_labels(labels_a) == read_labels(labels_b));
    EXPECT_TRUE(streams_differ || labels_differ);
}

}  // namespace
