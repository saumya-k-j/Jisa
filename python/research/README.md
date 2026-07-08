# Research: BOCPD vs CUSUM (SPEC 3.7)

BOCPD (Bayesian Online Changepoint Detection, Adams & MacKay 2007) maintains a
full posterior over the current *run length* — time since the last regime
change — updating it recursively with a conjugate Normal-Gamma model and a
constant hazard. It is the offline comparison baseline for the C++ CUSUM
hot-path detector:

- **BOCPD**: probabilistic (calibrated changepoint probability, not just a
  binary alarm), O(retained run lengths) state per stream growing until
  truncation, Student-t predictive robust to single spikes — but needs
  log-gamma evaluations and dynamic arrays per update: too heavy for a
  no-allocation, lock-free hot path.
- **CUSUM**: two scalars per stream, O(1) update, provable bounded detection
  delay for a given mean shift — exactly what the hot path needs; the price is
  a hand-tuned k/h trade-off and no probabilistic output.

Run the tests:        `python/.venv/bin/python -m pytest python/research/test_bocpd.py`
Run the comparison:   `python/.venv/bin/python python/research/compare_cusum.py`
(Environment: `python3 -m venv python/.venv && python/.venv/bin/pip install numpy pytest`,
or `source python/.venv/bin/activate` and use `python`/`pytest` directly.)
