"""Tests for the BOCPD prototype (SPEC 3.7). Deterministic, seeded, offline."""

import numpy as np
import pytest

from bocpd import BOCPD
from compare_cusum import cusum

SEED = 12345
ONSET = 400
N = 800


@pytest.fixture
def step_stream() -> np.ndarray:
    rng = np.random.default_rng(SEED)
    xs = rng.normal(0.0, 1.0, N)
    xs[ONSET:] += 3.0  # strong 3-sigma step
    return xs


@pytest.fixture
def stationary_stream() -> np.ndarray:
    rng = np.random.default_rng(SEED + 1)
    return rng.normal(0.0, 1.0, N)


def make_detector() -> BOCPD:
    return BOCPD(hazard_lambda=250.0, mu0=0.0, kappa0=1.0,
                 alpha0=1.0, beta0=1.0, trunc_thresh=1e-6)


def test_detects_strong_step_within_delay_and_no_false_alarms(step_stream):
    detections = make_detector().detect(step_stream, threshold=0.5)
    pre = [d for d in detections if d < ONSET]
    post = [d for d in detections if d >= ONSET]
    assert pre == [], f"false alarms before onset: {pre}"
    assert post, "missed the 3-sigma step change entirely"
    delay = post[0] - ONSET
    assert delay <= 30, f"detection delay {delay} exceeds generous budget of 30"


def test_run_length_posterior_is_valid_distribution(step_stream):
    b = make_detector()
    for x in step_stream:
        probs = b.update(x)
        assert np.all(probs >= 0.0)
        assert np.all(np.isfinite(probs))
        assert probs.sum() == pytest.approx(1.0, abs=1e-9)
        assert len(probs) == len(b.run_lengths)


def test_no_detection_on_stationary_stream(stationary_stream):
    assert make_detector().detect(stationary_stream, threshold=0.5) == []


def test_cusum_reference_detects_same_step(step_stream):
    # Sanity: the reference CUSUM sees the same 3-sigma step promptly. At
    # h=5, k=0.5 (ARL0 ~ 900) this seed produces exactly one pre-change false
    # alarm at t=318; we pin it deliberately -- it is the false-alarm/delay
    # trade-off the comparison harness is meant to expose.
    alarms = cusum(step_stream, k=0.5, h=5.0)
    pre = [a for a in alarms if a < ONSET]
    post = [a for a in alarms if a >= ONSET]
    assert pre == [318]
    assert post and post[0] - ONSET <= 30
