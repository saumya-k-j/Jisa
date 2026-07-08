"""RED-stage tests for python/agent/tools.py (SPEC 3.12: "Tools:
get_window(stream,t0,t1), get_baseline(stream), get_concurrent_alerts(),
get_domain_card(domain)." These are the deterministic, NO-LLM data tools the
triage agent calls; they are POST-ALERT only, never on the hot path).

PINNED CONTRACT (SPEC 3.12 is a one-line tool list; the implementer must
build to this contract, written here BEFORE python/agent/tools.py exists):

  Module: python/agent/tools.py (colocated with its test, following the
  python/api/{engine,app,test_api}.py precedent -- no package __init__.py;
  this test inserts its own directory onto sys.path and does
  `import tools`).

  class tools.AlertContext:

      def __init__(self, replay_path: str, db_path: str, domain: str,
                   cards_dir: str, alert_ts_ns: int, ewma_alpha: float = 0.3):
          ...

      PINNED DEVIATION from the task prompt's literal "window_index": we use
      an explicit `alert_ts_ns: int` instead. get_window() already takes
      explicit t0_ns/t1_ns per call, so a separate "window_index" would be
      redundant; get_baseline() is the one tool that needs a single fixed
      "as-of" instant bound to the AlertContext, and alert_ts_ns names that
      directly. This is a documented interface choice, not a silent
      deviation.

      def get_window(self, stream_id: int, t0_ns: int, t1_ns: int) -> list[dict]:
          # Replays replay_path (a .trec, via telemetry_engine.StreamReplayer)
          # and returns [{"ts_ns": int, "value": float}, ...] for messages
          # with the given stream_id AND t0_ns <= ts_ns <= t1_ns
          # -- PINNED: CLOSED interval, inclusive on BOTH ends (chosen over
          # half-open because callers naturally pass the alert's own ts_ns
          # as one of the bounds and expect that exact sample included).
          # Returned in ascending ts_ns order (natural stream order).

      def get_baseline(self, stream_id: int) -> dict:
          # Returns {"mean": float, "variance": float} from a FRESH
          # telemetry_engine.EwmaBaseline(alpha=self.ewma_alpha), updated
          # with every message for stream_id in replay_path with
          # ts_ns <= self.alert_ts_ns (INCLUSIVE of the alert instant
          # itself -- PINNED: "baseline state AT the alert ts", not
          # end-of-stream, so the memo reflects what was known at
          # detection time, not future data). Messages with
          # ts_ns > alert_ts_ns are never fed to the baseline.

      def get_concurrent_alerts(self, ts_ns: int, window_ns: int) -> list[dict]:
          # Queries the SQLite `alerts` table (schema from
          # python/api/engine.py: id, stream_id, ts_ns, layer, detail,
          # created_at) at self.db_path for rows with
          # ts_ns BETWEEN (ts_ns - window_ns) AND (ts_ns + window_ns)
          # -- PINNED: CLOSED interval, inclusive on both ends, symmetric
          # around ts_ns. Returns dicts with all six columns, ascending by
          # ts_ns. May include the alert being triaged itself if it is
          # already persisted in the table -- callers filter by id if they
          # need to exclude it (not this tool's job).

      def get_domain_card(self, domain: str | None = None) -> str:
          # Returns the raw markdown text of
          # <cards_dir>/<domain or self.domain>.md.
          # PINNED failure behavior: a missing card file raises
          # tools.DomainCardNotFoundError(domain) -- a SPECIFIC, documented
          # exception type (message includes the domain name and the path
          # tried), NOT a raw OSError/FileNotFoundError escaping uncaught,
          # and NOT a silent None return that a caller could mistake for
          # "card says nothing".

  class tools.DomainCardNotFoundError(Exception): ...

Run: python/.venv/bin/python -m pytest python/agent/test_tools.py -v
Expected RED result right now: collection error / ModuleNotFoundError for
`tools` (python/agent/tools.py does not exist yet).
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

telemetry_engine = pytest.importorskip(
    "telemetry_engine",
    reason=(
        "telemetry_engine pybind11 extension is not built/importable yet "
        "(SPEC 3.10); python/agent/tools.py fixtures need it."
    ),
)

_AGENT_DIR = os.path.dirname(__file__)
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

import tools  # python/agent/tools.py -- does not exist yet (RED)

_REPO_ROOT = os.path.dirname(os.path.dirname(_AGENT_DIR))
_CARDS_DIR = os.path.join(_REPO_ROOT, "docs", "domain_cards")

# Mirrors the exact schema in python/api/engine.py so fixtures built here are
# indistinguishable from a real EngineRunner-produced database.
_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id INTEGER NOT NULL,
  ts_ns INTEGER NOT NULL,
  layer TEXT NOT NULL,
  detail TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def _make_stream_with_spike(path, n=20, stream_id=1, spike_index=9):
    """Builds a .trec with n samples (value 50.0, grid-frequency-like),
    ts_ns = (i+1)*1e9 for i in range(n), seq = i+1, then injects a
    2-sample +100.0 spike at spike_index via the REAL FaultInjector
    (bindings), matching the fault-injection skill's ground-truth
    labeling."""
    rec = telemetry_engine.StreamRecorder(str(path / "in.trec"))
    for i in range(n):
        rec.write(telemetry_engine.Message(
            stream_id=stream_id, ts_ns=1_000_000_000 * (i + 1),
            value=50.0, seq=i + 1))
    rec.close()

    out_path = str(path / "out.trec")
    labels_path = str(path / "labels.txt")
    spec = telemetry_engine.FaultSpec(
        type=telemetry_engine.FaultType.kSpike, stream_id=stream_id,
        onset_index=spike_index, duration=2, magnitude=100.0)
    injector = telemetry_engine.FaultInjector(seed=7)
    assert injector.inject(str(path / "in.trec"), out_path, labels_path,
                            [spec])
    return out_path


def _two_stream_file(path):
    """A .trec interleaving two stream_ids so get_window's stream_id filter
    is exercised, not just a single-stream file."""
    p = str(path / "two.trec")
    rec = telemetry_engine.StreamRecorder(p)
    for i in range(10):
        rec.write(telemetry_engine.Message(
            stream_id=1, ts_ns=1_000_000_000 * (i + 1), value=1.0, seq=i + 1))
        rec.write(telemetry_engine.Message(
            stream_id=2, ts_ns=1_000_000_000 * (i + 1), value=2.0, seq=i + 1))
    rec.close()
    return p


def _make_alerts_db(path, rows):
    """rows: list of (stream_id, ts_ns, layer, detail, created_at)."""
    db_path = str(path / "alerts.db")
    conn = sqlite3.connect(db_path)
    conn.execute(_CREATE_TABLE)
    for r in rows:
        conn.execute(
            "INSERT INTO alerts (stream_id, ts_ns, layer, detail, created_at)"
            " VALUES (?, ?, ?, ?, ?)", r)
    conn.commit()
    conn.close()
    return db_path


class TestGetWindow:
    def test_closed_interval_both_ends_inclusive(self, tmp_path):
        replay_path = _two_stream_file(tmp_path)
        ctx = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=5_000_000_000)

        window = ctx.get_window(stream_id=1, t0_ns=5_000_000_000,
                                 t1_ns=8_000_000_000)
        assert [w["ts_ns"] for w in window] == [
            5_000_000_000, 6_000_000_000, 7_000_000_000, 8_000_000_000]
        assert all(w["value"] == pytest.approx(1.0) for w in window)

    def test_narrowing_bound_by_one_ns_excludes_boundary_sample(self, tmp_path):
        replay_path = _two_stream_file(tmp_path)
        ctx = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=5_000_000_000)

        left_excl = ctx.get_window(stream_id=1, t0_ns=5_000_000_001,
                                    t1_ns=8_000_000_000)
        assert [w["ts_ns"] for w in left_excl] == [
            6_000_000_000, 7_000_000_000, 8_000_000_000]

        right_excl = ctx.get_window(stream_id=1, t0_ns=5_000_000_000,
                                     t1_ns=7_999_999_999)
        assert [w["ts_ns"] for w in right_excl] == [
            5_000_000_000, 6_000_000_000, 7_000_000_000]

    def test_filters_by_stream_id(self, tmp_path):
        replay_path = _two_stream_file(tmp_path)
        ctx = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=5_000_000_000)

        window = ctx.get_window(stream_id=2, t0_ns=0, t1_ns=10_000_000_000)
        assert len(window) == 10
        assert all(w["value"] == pytest.approx(2.0) for w in window)

    def test_no_samples_in_range_returns_empty_list(self, tmp_path):
        replay_path = _two_stream_file(tmp_path)
        ctx = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=5_000_000_000)

        window = ctx.get_window(stream_id=1, t0_ns=100_000_000_000,
                                 t1_ns=200_000_000_000)
        assert window == []


class TestGetBaseline:
    def test_matches_direct_ewma_replay_up_to_alert_ts_inclusive(self, tmp_path):
        replay_path = _make_stream_with_spike(tmp_path, n=20, stream_id=1,
                                               spike_index=9)
        alert_ts_ns = 11_000_000_000  # ts of the 2nd spiked sample (index 10, 1-based ts)
        ewma_alpha = 0.4
        ctx = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=alert_ts_ns, ewma_alpha=ewma_alpha)

        result = ctx.get_baseline(stream_id=1)
        assert set(result.keys()) == {"mean", "variance"}

        # Cross-check by directly replaying the SAME .trec through the SAME
        # bindings primitive, feeding only messages with ts_ns <= alert_ts_ns.
        expected_baseline = telemetry_engine.EwmaBaseline(alpha=ewma_alpha)
        replayer = telemetry_engine.StreamReplayer(replay_path)
        for msg in replayer:
            if msg.stream_id == 1 and msg.ts_ns <= alert_ts_ns:
                expected_baseline.update(stream_id=1, value=msg.value)

        assert result["mean"] == pytest.approx(
            expected_baseline.mean(stream_id=1))
        assert result["variance"] == pytest.approx(
            expected_baseline.variance(stream_id=1))

    def test_messages_after_alert_ts_are_excluded(self, tmp_path):
        # Baseline computed as-of an EARLY alert_ts_ns must differ from one
        # computed as-of a LATE alert_ts_ns on a stream with a spike in
        # between -- proves future data is not leaking in.
        replay_path = _make_stream_with_spike(tmp_path, n=20, stream_id=1,
                                               spike_index=9)
        ctx_early = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=5_000_000_000, ewma_alpha=0.4)
        ctx_late = tools.AlertContext(
            replay_path=replay_path, db_path=str(tmp_path / "unused.db"),
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=20_000_000_000, ewma_alpha=0.4)

        early = ctx_early.get_baseline(stream_id=1)
        late = ctx_late.get_baseline(stream_id=1)
        # Early window never sees the spike (index 9 -> ts 10e9); late does.
        assert early["variance"] != pytest.approx(late["variance"])


class TestGetConcurrentAlerts:
    def test_symmetric_closed_window_around_ts(self, tmp_path):
        rows = [
            (1, 89, "rules", "out_of_bounds", "t0"),   # excluded (89 < 90)
            (1, 90, "rules", "out_of_bounds", "t1"),   # included (boundary)
            (2, 100, "cusum", "changepoint", "t2"),    # included (center)
            (3, 110, "conformal", "score=1>thr", "t3"),  # included (boundary)
            (1, 111, "rules", "out_of_bounds", "t4"),  # excluded (111 > 110)
        ]
        db_path = _make_alerts_db(tmp_path, rows)
        ctx = tools.AlertContext(
            replay_path=str(tmp_path / "unused.trec"), db_path=db_path,
            domain="grid_frequency", cards_dir=_CARDS_DIR,
            alert_ts_ns=100)

        result = ctx.get_concurrent_alerts(ts_ns=100, window_ns=10)
        assert [r["ts_ns"] for r in result] == [90, 100, 110]
        # Full column shape pinned to the engine.py schema.
        assert set(result[0].keys()) == {
            "id", "stream_id", "ts_ns", "layer", "detail", "created_at"}

    def test_no_alerts_in_window_returns_empty_list(self, tmp_path):
        rows = [(1, 1_000, "rules", "out_of_bounds", "t0")]
        db_path = _make_alerts_db(tmp_path, rows)
        ctx = tools.AlertContext(
            replay_path=str(tmp_path / "unused.trec"), db_path=db_path,
            domain="grid_frequency", cards_dir=_CARDS_DIR, alert_ts_ns=1_000)

        assert ctx.get_concurrent_alerts(ts_ns=0, window_ns=5) == []


class TestGetDomainCard:
    def test_returns_real_markdown_text_for_known_domain(self, tmp_path):
        ctx = tools.AlertContext(
            replay_path=str(tmp_path / "unused.trec"),
            db_path=str(tmp_path / "unused.db"), domain="grid_frequency",
            cards_dir=_CARDS_DIR, alert_ts_ns=0)

        text = ctx.get_domain_card()
        with open(os.path.join(_CARDS_DIR, "grid_frequency.md")) as f:
            expected = f.read()
        assert text == expected
        assert "Known failure modes" in text

    def test_explicit_domain_arg_overrides_constructor_domain(self, tmp_path):
        ctx = tools.AlertContext(
            replay_path=str(tmp_path / "unused.trec"),
            db_path=str(tmp_path / "unused.db"), domain="grid_frequency",
            cards_dir=_CARDS_DIR, alert_ts_ns=0)

        text = ctx.get_domain_card(domain="crypto_ticks")
        assert "Coinbase" in text

    def test_missing_domain_raises_specific_error_not_generic_exception(
            self, tmp_path):
        ctx = tools.AlertContext(
            replay_path=str(tmp_path / "unused.trec"),
            db_path=str(tmp_path / "unused.db"), domain="grid_frequency",
            cards_dir=_CARDS_DIR, alert_ts_ns=0)

        with pytest.raises(tools.DomainCardNotFoundError):
            ctx.get_domain_card(domain="no_such_domain_xyz")
