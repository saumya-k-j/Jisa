"""RED-stage tests for the pybind11 bindings module `telemetry_engine`
(SPEC 3.10: "Expose replay + detectors to Python so research/experiments can
drive them.").

PINNED CONTRACT (documented here because SPEC 3.10 is a one-line spec; the
implementer must build to this contract, not to whatever is easiest):

  Module name: `telemetry_engine` (pybind11 extension, built by CMake with a
  new `-DBUILD_PYTHON_BINDINGS=ON` option; NOT built by default, so these
  tests SKIP loudly -- see `_require_bindings()` below -- until the
  implementer adds src/bindings/ + pybind11 FetchContent).

  Exposed symbols (all pythonic: keyword args accepted, no C++-only handles
  leaking into Python beyond simple readonly attribute access):

    telemetry_engine.Message
        Readonly fields: stream_id (int), ts_ns (int), value (float),
        seq (int). Constructible with keyword args for round-tripping in
        tests: Message(stream_id=.., ts_ns=.., value=.., seq=..).

    telemetry_engine.StreamRecorder(path: str)
        .write(msg: Message) -> bool
        .is_open() -> bool
        .count() -> int
        .close() -> None

    telemetry_engine.StreamReplayer(path: str)
        .status -> StreamReplayer.Status  (property or method; pinned as a
          property below since C++ status() is a pure accessor)
        StreamReplayer.Status: kOk, kFileNotFound, kBadMagic,
          kUnsupportedVersion (enum, mirrors C++ enum class Status)
        .truncated -> bool (property)
        .messages_replayed -> int (property)
        PINNED iteration protocol: StreamReplayer is ITERABLE (`for msg in
          replayer: ...`), yielding Message objects until exhausted. This is
          the "replayer iterable" arm of the two options offered by the
          task; the alternative (`next() -> Optional[Message]`) is NOT
          implemented, so tests below only exercise iteration. If the
          implementer instead chooses the Optional[Message] arm, this test
          file's iteration-based tests will need a corresponding update --
          that deviation must be called out explicitly in review, not
          silently patched around.

    telemetry_engine.StreamChecksum()
        .update(msg: Message) -> None
        .digest() -> int  (uint64 FNV-1a digest, matches C++ StreamChecksum)

    telemetry_engine.FaultType
        Enum: kSpike, kDrift, kStuckAtValue, kDropout (mirrors C++).

    telemetry_engine.FaultSpec
        Constructible via kwargs: FaultSpec(type=FaultType.kSpike,
          stream_id=1, onset_index=.., duration=.., magnitude=..)
        kAutoOnset exposed as telemetry_engine.kAutoOnset (int constant).

    telemetry_engine.FaultLabel
        Readonly fields: stream_id, type, t_start_ns, t_end_ns.

    telemetry_engine.FaultInjector(seed: int)
        .inject(input_path, output_path, labels_path, specs: list[FaultSpec])
          -> bool

    telemetry_engine.read_labels(labels_path: str) -> list[FaultLabel]

    telemetry_engine.RuleResult
        Enum: kOk, kOutOfBounds, kRateViolation (mirrors C++ enum class).

    telemetry_engine.RuleConfig
        Fields: stream_id, min, max, max_rate_of_change (kwargs-constructible).

    telemetry_engine.load_rule_config(path: str) -> RuleConfig

    telemetry_engine.RuleChecker()
        .add_rule(cfg: RuleConfig) -> None
        .check(stream_id: int, value: float, ts_ns: int) -> RuleResult

    telemetry_engine.EwmaBaseline(alpha: float)
        .update(stream_id: int, value: float) -> None
        .zscore(stream_id: int, value: float) -> float
        .mean(stream_id: int) -> float
        .variance(stream_id: int) -> float

    telemetry_engine.CusumDetector(alpha: float, k: float, h: float,
        warmup_n: int = 1)
        .update_and_check(stream_id: int, value: float) -> bool

    telemetry_engine.ConformalThreshold(window_capacity: int)
        .update(stream_id: int, score: float) -> None
        .threshold(stream_id: int, alpha: float) -> float
        .is_anomalous(stream_id: int, score: float, alpha: float) -> bool

These tests only COUNT as verifying the bindings when `telemetry_engine`
successfully imports (i.e. after the implementer builds the extension and
installs/copies it onto the venv's import path). Until then, every test
here is expected to SKIP with a loud, explicit reason -- NOT silently pass,
NOT fail with a generic collection error. Run:
    python/.venv/bin/python -m pytest python/tests/test_bindings.py -v
"""
from __future__ import annotations

import math
import os
import struct

import pytest

telemetry_engine = pytest.importorskip(
    "telemetry_engine",
    reason=(
        "telemetry_engine pybind11 extension is not built/importable yet "
        "(SPEC 3.10). Implementer must add src/bindings/, wire pybind11 via "
        "FetchContent, and build with -DBUILD_PYTHON_BINDINGS=ON. These "
        "tests are RED-stage placeholders until then."
    ),
)


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------

class TestMessage:
    def test_construct_and_readback_fields(self):
        m = telemetry_engine.Message(stream_id=7, ts_ns=123456789, value=3.5, seq=42)
        assert m.stream_id == 7
        assert m.ts_ns == 123456789
        assert m.value == pytest.approx(3.5)
        assert m.seq == 42


# ---------------------------------------------------------------------------
# Recorder / Replayer round-trip (SPEC 3.9 exposed via bindings)
# ---------------------------------------------------------------------------

class TestRecordReplayRoundTrip:
    def _make_messages(self):
        return [
            telemetry_engine.Message(stream_id=1, ts_ns=1_000_000_000 * i,
                                      value=float(i) * 1.5, seq=i)
            for i in range(1, 11)
        ]

    def test_record_then_replay_matches_checksum(self, tmp_path):
        path = str(tmp_path / "stream.trec")
        msgs = self._make_messages()

        rec = telemetry_engine.StreamRecorder(path)
        assert rec.is_open()
        for m in msgs:
            assert rec.write(m) is True
        assert rec.count() == len(msgs)
        rec.close()

        # Checksum computed directly over the in-memory messages.
        direct = telemetry_engine.StreamChecksum()
        for m in msgs:
            direct.update(m)

        # Checksum computed by replaying the recorded file back through the
        # bindings -- this is the actual round-trip under test.
        replayed = telemetry_engine.StreamChecksum()
        replayer = telemetry_engine.StreamReplayer(path)
        assert replayer.status == telemetry_engine.StreamReplayer.Status.kOk
        count = 0
        for msg in replayer:
            replayed.update(msg)
            count += 1
        assert count == len(msgs)
        assert replayer.messages_replayed == len(msgs)
        assert replayer.truncated is False

        assert replayed.digest() == direct.digest()

    def test_replay_missing_file_reports_status(self, tmp_path):
        missing = str(tmp_path / "does_not_exist.trec")
        replayer = telemetry_engine.StreamReplayer(missing)
        assert replayer.status == telemetry_engine.StreamReplayer.Status.kFileNotFound
        # Exhausting immediately: no messages, no crash.
        assert list(replayer) == []

    def test_replay_truncated_file_sets_truncated_flag(self, tmp_path):
        path = str(tmp_path / "trunc.trec")
        rec = telemetry_engine.StreamRecorder(path)
        m = telemetry_engine.Message(stream_id=1, ts_ns=1, value=1.0, seq=1)
        assert rec.write(m)
        rec.close()

        # Truncate the file mid-record to force `truncated`.
        with open(path, "r+b") as f:
            f.truncate(os.path.getsize(path) - 10)

        replayer = telemetry_engine.StreamReplayer(path)
        assert replayer.status == telemetry_engine.StreamReplayer.Status.kOk
        msgs = list(replayer)
        assert msgs == []
        assert replayer.truncated is True


# ---------------------------------------------------------------------------
# Fault injection determinism (same seed -> byte-identical output + labels)
# ---------------------------------------------------------------------------

class TestFaultInjectorFromPython:
    def _record_stream(self, path, n=20, stream_id=1):
        rec = telemetry_engine.StreamRecorder(path)
        for i in range(n):
            rec.write(telemetry_engine.Message(
                stream_id=stream_id, ts_ns=1_000_000_000 * (i + 1),
                value=10.0, seq=i + 1))
        rec.close()

    def test_spike_fault_reproduces_known_answer(self, tmp_path):
        in_path = str(tmp_path / "in.trec")
        out_path = str(tmp_path / "out.trec")
        labels_path = str(tmp_path / "labels.txt")
        self._record_stream(in_path, n=10)

        spec = telemetry_engine.FaultSpec(
            type=telemetry_engine.FaultType.kSpike,
            stream_id=1, onset_index=3, duration=2, magnitude=100.0)

        injector = telemetry_engine.FaultInjector(seed=42)
        ok = injector.inject(in_path, out_path, labels_path, [spec])
        assert ok is True

        # Values at indices 3,4 (0-based) get +100; others unaffected.
        replayer = telemetry_engine.StreamReplayer(out_path)
        values = [m.value for m in replayer]
        expected = [10.0] * 10
        expected[3] += 100.0
        expected[4] += 100.0
        assert values == pytest.approx(expected)

        labels = telemetry_engine.read_labels(labels_path)
        assert len(labels) == 1
        assert labels[0].stream_id == 1
        assert labels[0].type == telemetry_engine.FaultType.kSpike
        # onset ts_ns = 4e9 (index 3, 1-based ts = (3+1)*1e9), end ts_ns = 5e9
        assert labels[0].t_start_ns == 4_000_000_000
        assert labels[0].t_end_ns == 5_000_000_000

    def test_auto_onset_same_seed_gives_byte_identical_output(self, tmp_path):
        in_path = str(tmp_path / "in.trec")
        self._record_stream(in_path, n=30)

        def run(seed):
            out_path = str(tmp_path / f"out_{seed}.trec")
            labels_path = str(tmp_path / f"labels_{seed}.txt")
            spec = telemetry_engine.FaultSpec(
                type=telemetry_engine.FaultType.kDrift, stream_id=1,
                onset_index=telemetry_engine.kAutoOnset, duration=5,
                magnitude=50.0)
            injector = telemetry_engine.FaultInjector(seed=seed)
            assert injector.inject(in_path, out_path, labels_path, [spec])
            with open(out_path, "rb") as f:
                data = f.read()
            with open(labels_path, "r") as f:
                labels_text = f.read()
            return data, labels_text

        data_a, labels_a = run(seed=1234)
        data_b, labels_b = run(seed=1234)
        assert data_a == data_b
        assert labels_a == labels_b

        # A different seed is permitted (not required) to differ; we only
        # assert same-seed determinism per the spec's explicit guarantee.


# ---------------------------------------------------------------------------
# Detectors: known-answer values reused from phase 4/5 C++ tests
# ---------------------------------------------------------------------------

class TestRuleCheckerFromPython:
    def test_out_of_bounds_and_ok(self):
        checker = telemetry_engine.RuleChecker()
        cfg = telemetry_engine.RuleConfig(
            stream_id=1, min=45.0, max=55.0, max_rate_of_change=0.5)
        checker.add_rule(cfg)
        assert checker.check(stream_id=1, value=50.0, ts_ns=0) == \
            telemetry_engine.RuleResult.kOk
        assert checker.check(stream_id=1, value=60.0, ts_ns=1_000_000_000) == \
            telemetry_engine.RuleResult.kOutOfBounds

    def test_load_rule_config_from_yaml(self):
        # Reuse the real pinned config fixture (SPEC 3.5 / config/*.yaml).
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        cfg_path = os.path.join(repo_root, "config", "grid_eu_freq.yaml")
        cfg = telemetry_engine.load_rule_config(cfg_path)
        assert cfg.stream_id == 1
        assert cfg.min == pytest.approx(45.0)
        assert cfg.max == pytest.approx(55.0)
        assert cfg.max_rate_of_change == pytest.approx(0.5)


class TestEwmaBaselineFromPython:
    def test_known_answer_alpha_half(self):
        # Known-answer sequence from tests/detect/test_baseline.cpp:
        # alpha=0.5, values=[4,6,5,7], mean0=var0=0 ->
        # zscore(7.0) after 4 updates == 0.49507377148834
        baseline = telemetry_engine.EwmaBaseline(alpha=0.5)
        for v in (4.0, 6.0, 5.0, 7.0):
            baseline.update(stream_id=1, value=v)
        assert baseline.zscore(stream_id=1, value=7.0) == pytest.approx(
            0.49507377148834, abs=1e-9)

    def test_unknown_stream_zscore_is_zero(self):
        baseline = telemetry_engine.EwmaBaseline(alpha=0.5)
        assert baseline.zscore(stream_id=999, value=1.0) == 0.0


class TestCusumDetectorFromPython:
    def test_known_answer_fires_exactly_at_jump_sample(self):
        # CORRECTED (previous version of this test used
        # alpha=0.3/k=0.5/h=3.0/warmup_n=2 on [0,0,0,10,10,10,10,10] and
        # asserted "fires eventually" -- that assertion was WRONG under the
        # recursion pinned in include/detect/cusum.hpp /
        # tests/detect/test_cusum.cpp: the first post-seed sample (count==1)
        # is a warmup update under warmup_n=2, so residual is only ever
        # evaluated once var_prev has already been inflated by the very
        # first 0->10 jump (var_prev jumps to ~30 after that update),
        # which damps every subsequent standardized residual. An
        # independent hand re-derivation of the pinned recursion (plain
        # Python, no C++ code involved) confirms s_pos plateaus at
        # ~1.20 < h=3.0 and NEVER crosses the threshold over this
        # sequence -- the test-writer's original assertion was simply
        # false under the pinned algorithm. Flagged by the implementer
        # (who correctly refused to special-case the binding to satisfy
        # a wrong test) and confirmed independently here.
        #
        # This replacement reuses the EXACT known-answer sequence already
        # independently verified in tests/detect/test_cusum.cpp
        # (Cusum.KnownAnswerFiresExactlyAtTheJumpSample): alpha=0.5, k=0.5,
        # h=3.0, warmup_n=1 (default). Index 0 is the seed sample (never
        # fires); indices 1..6 oscillate +/-0.5 around 10 (in-control,
        # must never fire); index 7 jumps to 20.0 and must fire EXACTLY
        # there (independently hand-traced: s_pos == 0 through index 6,
        # then residual at index 7 = (20 - 9.8359375) / sqrt(0.4383544921875)
        # ~= 15.352, so s_pos = 15.352 - 0.5 ~= 14.85 > h=3.0 -> fires,
        # then resets to 0 and stays quiet for indices 8..11 at the new
        # level -- reset-after-fire).
        det = telemetry_engine.CusumDetector(alpha=0.5, k=0.5, h=3.0)
        values = [
            10.0, 10.5, 9.5, 10.5, 9.5, 10.5, 9.5,   # indices 0..6
            20.0, 20.0, 20.0, 20.0, 20.0,             # indices 7..11
        ]
        fired = [det.update_and_check(stream_id=1, value=v) for v in values]

        for i in range(0, 7):
            assert fired[i] is False, f"unexpected fire at index {i}"
        assert fired[7] is True, "expected fire exactly at the jump sample (index 7)"
        for i in range(8, len(values)):
            assert fired[i] is False, f"unexpected re-fire at index {i}"

    def test_per_stream_state_independent_after_fire(self):
        # Mirrors tests/detect/test_cusum.cpp Cusum.PerStreamStateIsIndependent:
        # a changepoint fired on stream 1 must not perturb stream 2's
        # independent, freshly-seeded state.
        det = telemetry_engine.CusumDetector(alpha=0.5, k=0.5, h=3.0)
        jump_seq = [10.0, 10.5, 9.5, 10.5, 9.5, 10.5, 9.5, 20.0]
        for v in jump_seq:
            det.update_and_check(stream_id=1, value=v)
        # Stream 2's first-ever call is a seed sample regardless of stream
        # 1's history -- must never fire.
        assert det.update_and_check(stream_id=2, value=999.0) is False


class TestConformalThresholdFromPython:
    def test_known_answer_scores_1_to_10_alpha_0_2(self):
        # Known-answer from tests/detect/test_conformal.cpp:
        # window={1..10}, alpha=0.2 -> threshold == 8.0
        ct = telemetry_engine.ConformalThreshold(window_capacity=10)
        for v in range(1, 11):
            ct.update(stream_id=1, score=float(v))
        assert ct.threshold(stream_id=1, alpha=0.2) == pytest.approx(8.0)

    def test_is_anomalous_matches_threshold_comparison(self):
        ct = telemetry_engine.ConformalThreshold(window_capacity=10)
        for v in range(1, 11):
            ct.update(stream_id=1, score=float(v))
        assert ct.is_anomalous(stream_id=1, score=8.5, alpha=0.2) is True
        assert ct.is_anomalous(stream_id=1, score=7.5, alpha=0.2) is False

    def test_empty_window_threshold_is_infinity(self):
        ct = telemetry_engine.ConformalThreshold(window_capacity=5)
        assert math.isinf(ct.threshold(stream_id=1, alpha=0.2))
