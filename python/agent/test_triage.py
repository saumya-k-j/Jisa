"""RED-stage tests for python/agent/triage.py (SPEC 3.12: "On a confirmed
alert, an LLM agent with TOOLS investigates and writes an incident memo...
Output: structured memo {stream, hypothesis, confidence}.").

PINNED PROTOCOL (written BEFORE python/agent/triage.py exists; the
implementer builds to this, not to whatever is easiest):

  Module: python/agent/triage.py (colocated with its test; no package
  __init__.py -- this test inserts its own directory onto sys.path and does
  `import triage`).

  class triage.LLMClient(Protocol):
      def complete(self, system_prompt: str, conversation: list[dict]) -> str:
          '''conversation is a list of {"role": "user"|"assistant",
          "content": str} dicts (minimal OpenAI/Anthropic-shaped messages,
          NO framework dependency -- this project does not use the
          anthropic SDK; see MockLLMClient below and ClaudeCliClient which
          is implemented separately and NEVER exercised by pytest). Returns
          the assistant's raw next-turn text, expected to parse as the
          tool-call JSON protocol below.'''

  class triage.MockLLMClient:
      MockLLMClient(responses: list[str]) -- a deterministic, NO-network
      test double. The Nth call to .complete() returns responses[N] (0
      indexed); if more calls are made than len(responses), the LAST
      response is repeated forever (PINNED: lets a test model "the LLM
      never returns a memo" with a short, finite script instead of an
      infinite list). Records every call on self.calls as
      (system_prompt, conversation) tuples (conversation is a deep-ish copy
      -- a new list -- so later mutation by the agent does not corrupt the
      recorded call) for assertions on exactly what the agent sent.

  TOOL-CALL JSON PROTOCOL (the only two shapes triage.py accepts from
  LLMClient.complete()'s return string, each parsed with json.loads):

      {"tool": "<tool_name>", "args": {...}}
          -- request to invoke one tool from the `tools` dict passed to
          TriageAgent (keys are tool names, values are zero-or-more-kwarg
          callables, e.g. {"get_window": ctx.get_window, ...}). The agent
          calls tools[name](**args), then appends the JSON-serialized
          result to the conversation as the next "user" message before
          asking the LLM again (this consumes one of max_turns).

      {"memo": {"hypothesis": str, "confidence": float,
                "evidence": [str, ...]}}
          -- final answer. The agent fills in "stream_id" from the alert
          being investigated and returns the memo (after hypothesis
          validation, below). Ends the loop successfully.

  Any other/malformed content (invalid JSON, JSON that is neither of the
  two shapes above, or {"tool": <unknown name not in `tools`>, ...}) is
  MALFORMED. PINNED malformed handling: the agent gives the LLM exactly ONE
  retry within the SAME turn (appends a "malformed response, respond with
  either {\"tool\":...} or {\"memo\":...}" user message and calls
  .complete() again -- this retry call does NOT consume a max_turns slot).
  If the retry is ALSO malformed, the agent stops immediately (does not
  keep looping) and returns a no_conclusion memo (see below) -- it does
  NOT retry a second time.

  class triage.TriageAgent:
      TriageAgent(llm: LLMClient, tools: dict[str, Callable],
                  max_turns: int)

      def investigate(self, alert: dict, domain: str,
                       allowed_hypotheses: set[str]) -> dict:
          '''alert: {"stream_id": int, "ts_ns": int, "layer": str,
          "detail": str} (the alerts-table row shape from
          python/api/engine.py). Fetches tools["get_domain_card"](domain)
          ONCE up front (before turn 1) and folds its text into the system
          prompt/first conversation turn as context -- it is NOT something
          the LLM has to explicitly request as a tool call mid-loop, since
          every investigation needs it. Then runs the tool-call loop above
          for up to max_turns LLM turns (each turn = one non-retry
          .complete() call). Returns a memo dict:
              {"stream_id": int, "hypothesis": str, "confidence": float,
               "evidence": [str, ...]}
          '''

  HYPOTHESIS VALIDATION (pinned): after the LLM returns a {"memo": ...},
  if memo["hypothesis"] is NOT in (allowed_hypotheses | {"unknown"}), the
  agent REWRITES hypothesis to "unknown" and appends
  f"original_hypothesis:{original}" to the evidence list (evidence is
  otherwise preserved verbatim). If hypothesis IS in the allowed set (or is
  already literally "unknown"), it passes through unchanged.

  NO-CONCLUSION SENTINEL (pinned): if max_turns is exhausted without the
  LLM ever returning a {"memo": ...} shape, OR the one-retry-then-stop
  malformed rule triggers, the agent returns:
      {"stream_id": <alert's stream_id>, "hypothesis": "no_conclusion",
       "confidence": 0.0, "evidence": [<a short machine-readable reason
       string -- pinned substrings: "max_turns_exceeded" for the turn-cap
       case, "malformed_llm_response" for the malformed-retry case>]}
  "no_conclusion" is a reserved sentinel, NEVER subject to the hypothesis
  validation/rewrite-to-"unknown" rule above (it is not a real hypothesis
  guess to validate).

Run: python/.venv/bin/python -m pytest python/agent/test_triage.py -v
Expected RED result right now: ModuleNotFoundError for `triage`
(python/agent/triage.py does not exist yet). Zero LLM/network calls in this
file -- MockLLMClient only.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

_AGENT_DIR = os.path.dirname(__file__)
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

import triage  # python/agent/triage.py -- does not exist yet (RED)


ALERT = {"stream_id": 1, "ts_ns": 5_000_000_000, "layer": "cusum",
          "detail": "changepoint"}
DOMAIN = "grid_frequency"
ALLOWED = {"stuck_sensor", "generation_loss", "telemetry_glitch"}


class _RecordingTool:
    """A fake tool callable that records the kwargs it was invoked with and
    returns a fixed canned payload -- used so triage.py tests are decoupled
    from tools.py (unit-tested separately in test_tools.py)."""

    def __init__(self, name, payload):
        self.name = name
        self.payload = payload
        self.calls = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return self.payload


def _make_tools():
    return {
        "get_window": _RecordingTool(
            "get_window", [{"ts_ns": 5_000_000_000, "value": 51.0}]),
        "get_baseline": _RecordingTool(
            "get_baseline", {"mean": 50.0, "variance": 0.01}),
        "get_concurrent_alerts": _RecordingTool(
            "get_concurrent_alerts", []),
        "get_domain_card": _RecordingTool(
            "get_domain_card", "# Domain card: grid frequency\n..."),
    }


class TestMultiTurnScriptedSession:
    def test_memo_parses_and_tools_called_with_requested_args(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"tool": "get_window",
                        "args": {"stream_id": 1, "t0_ns": 4_000_000_000,
                                 "t1_ns": 6_000_000_000}}),
            json.dumps({"tool": "get_concurrent_alerts",
                        "args": {"ts_ns": 5_000_000_000, "window_ns": 2}}),
            json.dumps({"memo": {"hypothesis": "generation_loss",
                                  "confidence": 0.7,
                                  "evidence": ["neighbor alert present"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=5)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo == {
            "stream_id": 1, "hypothesis": "generation_loss",
            "confidence": 0.7, "evidence": ["neighbor alert present"],
        }
        assert tools["get_window"].calls == [
            {"stream_id": 1, "t0_ns": 4_000_000_000, "t1_ns": 6_000_000_000}]
        assert tools["get_concurrent_alerts"].calls == [
            {"ts_ns": 5_000_000_000, "window_ns": 2}]
        # Domain card fetched exactly once, up front, not via the tool-call
        # JSON loop (the LLM script above never requests it explicitly).
        assert tools["get_domain_card"].calls == [{"domain": DOMAIN}]

    def test_llm_receives_domain_card_context_before_first_turn(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"memo": {"hypothesis": "stuck_sensor",
                                  "confidence": 0.9, "evidence": ["e"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=3)
        agent.investigate(ALERT, domain=DOMAIN, allowed_hypotheses=ALLOWED)

        assert len(llm.calls) == 1
        system_prompt, conversation = llm.calls[0]
        assert "grid frequency" in system_prompt or any(
            "grid frequency" in str(turn) for turn in conversation)


class TestMaxTurnsCap:
    def test_llm_never_returning_memo_stops_with_no_conclusion(self):
        tools = _make_tools()
        # Always asks for the window, never concludes.
        llm = triage.MockLLMClient(responses=[
            json.dumps({"tool": "get_window",
                        "args": {"stream_id": 1, "t0_ns": 0, "t1_ns": 1}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=4)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["stream_id"] == 1
        assert memo["hypothesis"] == "no_conclusion"
        assert memo["confidence"] == 0.0
        assert any("max_turns_exceeded" in e for e in memo["evidence"])
        # Exactly max_turns non-retry completions were made (the repeating
        # tool-call script never triggers the malformed-retry path).
        assert len(llm.calls) == 4


class TestMalformedLlmResponse:
    def test_retries_once_then_no_conclusion_if_still_malformed(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            "this is not json",
            "still not json",
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=5)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "no_conclusion"
        assert memo["confidence"] == 0.0
        assert any("malformed_llm_response" in e for e in memo["evidence"])
        # Exactly one retry call was made (2 total), the loop did NOT
        # continue to max_turns.
        assert len(llm.calls) == 2

    def test_recovers_if_retry_succeeds(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            "garbage, not json at all",
            json.dumps({"memo": {"hypothesis": "telemetry_glitch",
                                  "confidence": 0.4, "evidence": ["e1"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=5)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "telemetry_glitch"
        assert memo["confidence"] == pytest.approx(0.4)
        assert len(llm.calls) == 2

    def test_unknown_tool_name_treated_as_malformed(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"tool": "delete_all_data", "args": {}}),
            json.dumps({"tool": "delete_all_data", "args": {}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=5)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "no_conclusion"
        assert any("malformed_llm_response" in e for e in memo["evidence"])


class TestHypothesisRestrictedToAllowedTaxonomy:
    def test_invalid_hypothesis_rewritten_to_unknown_original_preserved(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"memo": {"hypothesis": "alien_intervention",
                                  "confidence": 0.5,
                                  "evidence": ["speculative"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=3)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "unknown"
        assert "speculative" in memo["evidence"]
        assert "original_hypothesis:alien_intervention" in memo["evidence"]

    def test_hypothesis_already_literally_unknown_passes_through(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"memo": {"hypothesis": "unknown",
                                  "confidence": 0.1, "evidence": ["e"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=3)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "unknown"
        assert memo["evidence"] == ["e"]

    def test_allowed_hypothesis_passes_through_unchanged(self):
        tools = _make_tools()
        llm = triage.MockLLMClient(responses=[
            json.dumps({"memo": {"hypothesis": "stuck_sensor",
                                  "confidence": 0.85, "evidence": ["e"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=3)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "stuck_sensor"


class TestMockLLMClientMechanics:
    def test_repeats_last_response_when_script_exhausted(self):
        llm = triage.MockLLMClient(responses=["a", "b"])
        assert llm.complete("sys", []) == "a"
        assert llm.complete("sys", []) == "b"
        assert llm.complete("sys", []) == "b"
        assert llm.complete("sys", []) == "b"

    def test_records_calls_immutably(self):
        llm = triage.MockLLMClient(responses=["a"])
        convo = [{"role": "user", "content": "hi"}]
        llm.complete("sys", convo)
        convo.append({"role": "user", "content": "mutated after the call"})
        assert llm.calls[0][1] == [{"role": "user", "content": "hi"}]


# ---------------------------------------------------------------------------
# Coverage-gap fill (phase-8 review): D-042/D-043 documented behaviors that
# had no pinning test yet. Written from DECISIONS.md D-042/D-043 and the
# module docstrings above/in triage.py -- NOT from reading implementation
# internals beyond those documented contracts.
#
# D-043 (excerpt): "Bad kwargs to a KNOWN tool are caught and fed back to the
# LLM as a JSON error result so it can self-correct ... this is the only
# deviation beyond the pinned contract and is confined to robustness of the
# un-tested real run." This is explicitly NOT the malformed-protocol retry
# path (unknown tool name / bad JSON shape) -- it is a normal tool-call turn
# whose result happens to be an error payload; it consumes one max_turns slot
# like any other tool call, and it does NOT trigger the one-shot malformed
# retry text.
#
# D-042 (excerpt): "ClaudeCliClient ... the rendered prompt (system +
# conversation) goes in on stdin, stdout is captured, and a surrounding
# ```json fence is stripped if the model wraps its answer." Both the fence
# stripping and the prompt rendering are pure, subprocess-free helpers and
# are pinned here directly (ClaudeCliClient itself is never exercised via a
# real subprocess call in this test suite, per D-042).
# ---------------------------------------------------------------------------

class TestBadKwargsToKnownToolFedBackAsError:
    def _strict_get_window(self, stream_id, t0_ns, t1_ns):
        return [{"ts_ns": t0_ns, "value": 1.0}]

    def test_bad_kwargs_do_not_crash_and_agent_continues_to_a_memo(self):
        tools = _make_tools()
        tools["get_window"] = self._strict_get_window
        llm = triage.MockLLMClient(responses=[
            json.dumps({"tool": "get_window", "args": {"bogus": 1}}),
            json.dumps({"memo": {"hypothesis": "stuck_sensor",
                                  "confidence": 0.6, "evidence": ["e"]}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=5)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        # Did not crash; the agent kept going and reached the scripted memo.
        assert memo["hypothesis"] == "stuck_sensor"
        assert memo["confidence"] == pytest.approx(0.6)
        assert len(llm.calls) == 2

        # The bad-kwargs result was fed back as a JSON error payload, not the
        # malformed-protocol retry prompt text.
        second_conversation = llm.calls[1][1]
        fed_back_raw = second_conversation[-1]["content"]
        fed_back = json.loads(fed_back_raw)
        assert fed_back["tool"] == "get_window"
        assert "error" in fed_back
        assert "malformed" not in fed_back_raw

    def test_bad_kwargs_consume_a_full_turn_not_a_free_retry(self):
        # With max_turns=1, a bad-kwargs tool call must exhaust the turn
        # budget (max_turns_exceeded) rather than get a free in-turn retry
        # the way a malformed JSON/unknown-tool response would.
        tools = _make_tools()
        tools["get_window"] = self._strict_get_window
        llm = triage.MockLLMClient(responses=[
            json.dumps({"tool": "get_window", "args": {"bogus": 1}}),
        ])
        agent = triage.TriageAgent(llm=llm, tools=tools, max_turns=1)

        memo = agent.investigate(ALERT, domain=DOMAIN,
                                  allowed_hypotheses=ALLOWED)

        assert memo["hypothesis"] == "no_conclusion"
        assert any("max_turns_exceeded" in e for e in memo["evidence"])
        # Exactly one completion call -- no bonus retry call was spent.
        assert len(llm.calls) == 1


class TestClaudeCliClientStripCodeFences:
    def test_json_fenced_block_returns_inner_json(self):
        text = '```json\n{"memo": {"hypothesis": "unknown"}}\n```'
        assert triage._strip_code_fences(text) == \
            '{"memo": {"hypothesis": "unknown"}}'

    def test_plain_fenced_block_returns_inner_text(self):
        text = '```\n{"a": 1}\n```'
        assert triage._strip_code_fences(text) == '{"a": 1}'

    def test_unfenced_text_is_returned_unchanged_aside_from_trim(self):
        text = '{"a": 1}'
        assert triage._strip_code_fences(text) == '{"a": 1}'

    def test_surrounding_whitespace_around_a_fence_is_trimmed(self):
        text = '  \n```json\n{"a": 1}\n```\n  '
        assert triage._strip_code_fences(text) == '{"a": 1}'


class TestClaudeCliClientPromptRendering:
    def test_renders_system_prompt_then_role_tagged_turns_in_order(self):
        system_prompt = "SYSTEM CONTEXT"
        conversation = [
            {"role": "user", "content": "Investigate the alert."},
            {"role": "assistant", "content": '{"tool": "get_window", '
                                              '"args": {}}'},
        ]
        prompt = triage.ClaudeCliClient._render_prompt(
            system_prompt, conversation)

        assert prompt.startswith(system_prompt)
        assert "[user]\nInvestigate the alert." in prompt
        assert ('[assistant]\n{"tool": "get_window", "args": {}}'
                in prompt)
        # A trailing bare "[assistant]" tag invites the model's next reply.
        assert prompt.rstrip().endswith("[assistant]")

    def test_tool_result_json_and_braced_domain_card_do_not_break_rendering(
            self):
        # A domain card containing literal curly braces (e.g. quoted JSON
        # examples or set notation) must pass through untouched -- prompt
        # rendering must not try to str.format()/interpret braces in
        # conversation content or the system prompt.
        system_prompt = (
            "Domain card:\nKnown failure modes include {\"weird\": "
            "{\"nested\": true}} and {curly braces} in general.")
        tool_result = json.dumps(
            {"tool": "get_window",
             "result": [{"ts_ns": 1, "value": 2.0}]})
        conversation = [{"role": "user", "content": tool_result}]

        prompt = triage.ClaudeCliClient._render_prompt(
            system_prompt, conversation)

        assert system_prompt in prompt
        assert tool_result in prompt
        # The embedded tool result is still valid, round-trippable JSON
        # inside the rendered prompt.
        user_section = prompt.split("[user]\n", 1)[1]
        user_section = user_section.split("\n\n[assistant]", 1)[0]
        assert json.loads(user_section) == {
            "tool": "get_window", "result": [{"ts_ns": 1, "value": 2.0}]}
