"""Bayesian Online Changepoint Detection (Adams & MacKay, 2007).

Univariate Gaussian observation model with a **Normal-Gamma prior** on
(mean, precision), i.e. both the mean AND the variance of each segment are
unknown. Rationale for choosing Normal-Gamma over a fixed-variance Normal
prior:

  * Real telemetry streams (ticks, grid frequency) do not come with a known
    noise variance, and regime changes often shift variance as well as mean.
    A fixed-variance model is miscalibrated the moment sigma is wrong.
  * The Normal-Gamma prior is still fully conjugate, so the run-length
    posterior update stays closed-form: the posterior predictive is a
    Student-t, which additionally gives heavier tails than a Gaussian and
    makes the detector less trigger-happy on single outliers (spikes), which
    is exactly the failure mode we want CUSUM/BOCPD to ignore in favour of
    *sustained* shifts.

Cost: the Student-t predictive needs log-gamma evaluations, which is fine
here — this is offline research code, not the C++ hot path.

Algorithm (Adams & MacKay 2007, "Bayesian Online Changepoint Detection"):
at each step t we maintain the joint P(r_t, x_1:t) over the current run
length r_t. With a constant hazard H(r) = 1/hazard_lambda:

    growth:      P(r_t = r+1) ∝ P(r_{t-1} = r) * pred(x_t | r) * (1 - H)
    changepoint: P(r_t = 0)   ∝ Σ_r P(r_{t-1} = r) * pred(x_t | r) * H

The update over all run lengths is vectorized with numpy. Worst case is
O(T^2) over a stream of length T; the standard truncation (drop run-length
probabilities below `trunc_thresh` and renormalize) keeps the retained state
small in practice.
"""

from __future__ import annotations

import math

import numpy as np

_LGAMMA = np.vectorize(math.lgamma, otypes=[np.float64])


class BOCPD:
    """Online BOCPD for a univariate Gaussian with Normal-Gamma prior.

    Parameters
    ----------
    hazard_lambda : expected run length between changepoints; constant
        hazard H(r) = 1 / hazard_lambda.
    mu0, kappa0, alpha0, beta0 : Normal-Gamma hyperparameters. mu0 is the
        prior mean, kappa0 the prior pseudo-count on the mean, alpha0/beta0
        shape/rate of the Gamma prior on precision (prior variance guess is
        roughly beta0 / alpha0).
    trunc_thresh : run-length probabilities below this are dropped (r = 0 is
        always kept) and the posterior renormalized.
    """

    def __init__(
        self,
        hazard_lambda: float = 250.0,
        mu0: float = 0.0,
        kappa0: float = 1.0,
        alpha0: float = 1.0,
        beta0: float = 1.0,
        trunc_thresh: float = 1e-6,
    ) -> None:
        if hazard_lambda <= 1.0:
            raise ValueError("hazard_lambda must be > 1")
        self.hazard = 1.0 / hazard_lambda
        self.prior = (float(mu0), float(kappa0), float(alpha0), float(beta0))
        self.trunc_thresh = float(trunc_thresh)
        self.reset()

    def reset(self) -> None:
        mu0, kappa0, alpha0, beta0 = self.prior
        # run_length_probs[i] is P(r_t = run_lengths[i] | x_1:t)
        self.run_lengths = np.array([0], dtype=np.int64)
        self.run_length_probs = np.array([1.0])
        # Sufficient statistics of the Normal-Gamma posterior, one entry per
        # retained run length (aligned with run_lengths).
        self._mu = np.array([mu0])
        self._kappa = np.array([kappa0])
        self._alpha = np.array([alpha0])
        self._beta = np.array([beta0])

    # ---- internals -------------------------------------------------------

    def _log_pred(self, x: float) -> np.ndarray:
        """Log posterior-predictive density of x for each retained run length.

        Predictive is Student-t with df = 2*alpha, loc = mu,
        scale^2 = beta * (kappa + 1) / (alpha * kappa).
        """
        df = 2.0 * self._alpha
        scale2 = self._beta * (self._kappa + 1.0) / (self._alpha * self._kappa)
        z2 = (x - self._mu) ** 2 / scale2
        return (
            _LGAMMA((df + 1.0) / 2.0)
            - _LGAMMA(df / 2.0)
            - 0.5 * np.log(df * np.pi * scale2)
            - (df + 1.0) / 2.0 * np.log1p(z2 / df)
        )

    # ---- public API ------------------------------------------------------

    def update(self, x: float) -> np.ndarray:
        """Ingest one observation; return the new run-length posterior.

        The returned array is aligned with `self.run_lengths` (both are
        pruned by truncation, so it is not dense in r).
        """
        x = float(x)
        pred = np.exp(self._log_pred(x))
        growth = self.run_length_probs * pred * (1.0 - self.hazard)
        cp = float(np.sum(self.run_length_probs * pred * self.hazard))

        new_probs = np.concatenate(([cp], growth))
        total = new_probs.sum()
        if total <= 0.0 or not np.isfinite(total):  # pragma: no cover
            self.reset()
            return self.run_length_probs
        new_probs /= total
        new_run_lengths = np.concatenate(([0], self.run_lengths + 1))

        # Posterior sufficient-stat update (conjugate Normal-Gamma), with the
        # fresh prior prepended for the r = 0 hypothesis.
        mu0, kappa0, alpha0, beta0 = self.prior
        kappa_new = self._kappa + 1.0
        mu_new = (self._kappa * self._mu + x) / kappa_new
        alpha_new = self._alpha + 0.5
        beta_new = self._beta + 0.5 * self._kappa * (x - self._mu) ** 2 / kappa_new
        self._mu = np.concatenate(([mu0], mu_new))
        self._kappa = np.concatenate(([kappa0], kappa_new))
        self._alpha = np.concatenate(([alpha0], alpha_new))
        self._beta = np.concatenate(([beta0], beta_new))

        # Truncation: drop negligible run lengths (always keep r = 0).
        keep = new_probs >= self.trunc_thresh
        keep[0] = True
        self.run_lengths = new_run_lengths[keep]
        self.run_length_probs = new_probs[keep]
        self.run_length_probs /= self.run_length_probs.sum()
        self._mu = self._mu[keep]
        self._kappa = self._kappa[keep]
        self._alpha = self._alpha[keep]
        self._beta = self._beta[keep]
        return self.run_length_probs

    @property
    def map_run_length(self) -> int:
        return int(self.run_lengths[np.argmax(self.run_length_probs)])

    def prob_recent_changepoint(self, r_window: int = 5) -> float:
        """P(current run length <= r_window), i.e. 'a changepoint just happened'."""
        return float(self.run_length_probs[self.run_lengths <= r_window].sum())

    def detect(
        self,
        xs,
        threshold: float = 0.5,
        r_window: int = 5,
        min_gap: int = 10,
        map_drop_min: int = 30,
    ) -> list[int]:
        """Run over a full stream; return indices where a changepoint fires.

        Two triggers, per the usual BOCPD readouts:
          1. posterior mass on short run lengths, P(r_t <= r_window),
             exceeds `threshold` (abrupt steps: mass jumps straight to r=0);
          2. the MAP run length collapses — drops by more than half and by at
             least `map_drop_min` samples (slow drifts: the posterior
             re-concentrates on a short-but-not-tiny run length, so the mass
             criterion alone can miss it).

        The first r_window + 1 samples are warmup (run length is trivially
        short at the start), and after a detection we hold off `min_gap`
        samples so one changepoint is not reported repeatedly while the
        posterior re-concentrates.
        """
        self.reset()
        detections: list[int] = []
        last = -min_gap - 1
        prev_map = 0
        for t, x in enumerate(np.asarray(xs, dtype=np.float64)):
            self.update(x)
            map_r = self.map_run_length
            fired = False
            if t > r_window and t - last > min_gap:
                if self.prob_recent_changepoint(r_window) > threshold:
                    fired = True
                elif prev_map - map_r >= max(map_drop_min, prev_map // 2):
                    fired = True
            if fired:
                detections.append(t)
                last = t
            prev_map = map_r
        return detections
