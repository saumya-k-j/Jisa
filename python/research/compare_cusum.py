"""BOCPD vs. reference CUSUM on seeded synthetic mean-shift streams.

Self-contained comparison harness for SPEC 3.7: the C++ hot path runs CUSUM;
this shows how the offline BOCPD prototype compares on the same faults.

Run:  python python/research/compare_cusum.py   (takes ~1-2 s)

Scenarios (numpy default_rng, fixed seed, unit-variance Gaussian noise):
  * step:  mean 0 -> 3 sigma at a known onset
  * drift: mean ramps from 0 at +0.05 sigma/sample after the onset

For each detector we report the first detection at/after the true onset, the
delay in samples, and the number of false alarms strictly before the onset.
"""

from __future__ import annotations

import numpy as np

from bocpd import BOCPD

SEED = 12345
N = 1000
ONSET = 400


def cusum(xs, k: float = 0.5, h: float = 5.0, mu: float = 0.0, sigma: float = 1.0,
          min_gap: int = 10) -> list[int]:
    """Reference two-sided CUSUM on standardized values.

    k = drift allowance, h = decision threshold (both in sigma units).
    Statistics reset after each alarm; alarms within min_gap are merged.
    """
    gp = gn = 0.0
    alarms: list[int] = []
    last = -min_gap - 1
    for t, x in enumerate(xs):
        z = (x - mu) / sigma
        gp = max(0.0, gp + z - k)
        gn = max(0.0, gn - z - k)
        if (gp > h or gn > h) and t - last > min_gap:
            alarms.append(t)
            last = t
            gp = gn = 0.0
    return alarms


def make_streams(seed: int = SEED) -> dict[str, np.ndarray]:
    rng = np.random.default_rng(seed)
    step = rng.normal(0.0, 1.0, N)
    step[ONSET:] += 3.0
    drift = rng.normal(0.0, 1.0, N)
    drift[ONSET:] += 0.05 * np.arange(N - ONSET)
    return {"step": step, "drift": drift}


def score(alarms: list[int], onset: int) -> tuple[int | None, int | None, int]:
    """(detection index, delay, false alarms before onset)."""
    false_alarms = sum(1 for a in alarms if a < onset)
    hits = [a for a in alarms if a >= onset]
    if not hits:
        return None, None, false_alarms
    return hits[0], hits[0] - onset, false_alarms


def main() -> None:
    streams = make_streams()
    rows = []
    for name, xs in streams.items():
        b = BOCPD(hazard_lambda=250.0, mu0=0.0, kappa0=1.0,
                  alpha0=1.0, beta0=1.0, trunc_thresh=1e-6)
        for det, alarms in (
            ("BOCPD", b.detect(xs, threshold=0.5)),
            ("CUSUM", cusum(xs, k=0.5, h=5.0)),
        ):
            det_idx, delay, fa = score(alarms, ONSET)
            rows.append((det, name, ONSET, det_idx, delay, fa))

    hdr = f"{'detector':<8} {'scenario':<9} {'true onset':>10} {'detected':>9} {'delay':>6} {'false alarms':>13}"
    print(hdr)
    print("-" * len(hdr))
    for det, name, onset, det_idx, delay, fa in rows:
        d = "miss" if det_idx is None else det_idx
        dl = "-" if delay is None else delay
        print(f"{det:<8} {name:<9} {onset:>10} {d!s:>9} {dl!s:>6} {fa:>13}")


if __name__ == "__main__":
    main()
