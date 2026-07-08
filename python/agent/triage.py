"""SPEC-3.12 post-alert triage agent: an LLM with TOOLS investigates a
confirmed alert and writes a structured incident memo {stream, hypothesis,
confidence}.

Deliberately a hand-rolled JSON tool-call loop -- NO agent framework, NO
anthropic SDK (CLAUDE.md: no unnecessary abstraction; no API key is available
in this environment). The LLM contract is a minimal Protocol so the loop is
unit-tested with a deterministic MockLLMClient, while the real
``ClaudeCliClient`` shells out to the local ``claude`` CLI and is never
exercised by pytest.

See python/agent/test_triage.py for the pinned protocol this builds to.
"""
from __future__ import annotations

import json
import subprocess
from typing import Callable, Protocol


class LLMClient(Protocol):
    """Minimal chat-completion contract. No framework/SDK dependency."""

    def complete(self, system_prompt: str, conversation: list[dict]) -> str:
        """conversation is a list of {"role": "user"|"assistant",
        "content": str} dicts. Returns the assistant's raw next-turn text,
        expected to parse as the tool-call JSON protocol."""
        ...


class MockLLMClient:
    """Deterministic, no-network test double.

    The Nth .complete() call returns responses[N]; once the script is
    exhausted the LAST response repeats forever (so a test can model "the LLM
    never concludes" with a finite script). Every call is recorded on
    self.calls as (system_prompt, conversation_copy) tuples -- the copy is a
    fresh list so later mutation by the agent cannot corrupt the record.
    """

    def __init__(self, responses: list[str]):
        self.responses = list(responses)
        self.calls: list[tuple[str, list[dict]]] = []
        self._n = 0

    def complete(self, system_prompt: str, conversation: list[dict]) -> str:
        self.calls.append((system_prompt, list(conversation)))
        if self._n < len(self.responses):
            resp = self.responses[self._n]
        else:
            resp = self.responses[-1]
        self._n += 1
        return resp


class ClaudeCliClient:
    """Real LLMClient backed by the local ``claude`` CLI (headless mode).

    Runs ``claude -p --model <model>`` with the prompt on stdin and captures
    stdout. No SDK, no API key handling -- authentication is whatever the CLI
    already has. On any subprocess failure a RuntimeError is raised; the agent
    loop treats that as a malformed response (one retry, then no_conclusion).
    """

    def __init__(self, model: str = "haiku", timeout: float = 120.0):
        self.model = model
        self.timeout = timeout

    def complete(self, system_prompt: str, conversation: list[dict]) -> str:
        prompt = self._render_prompt(system_prompt, conversation)
        try:
            proc = subprocess.run(
                ["claude", "-p", "--model", self.model],
                input=prompt, capture_output=True, text=True,
                timeout=self.timeout)
        except (OSError, subprocess.SubprocessError) as exc:
            raise RuntimeError(f"claude CLI invocation failed: {exc}") from exc
        if proc.returncode != 0:
            raise RuntimeError(
                f"claude CLI exited {proc.returncode}: {proc.stderr.strip()}")
        return _strip_code_fences(proc.stdout.strip())

    @staticmethod
    def _render_prompt(system_prompt: str, conversation: list[dict]) -> str:
        parts = [system_prompt, ""]
        for turn in conversation:
            parts.append(f"[{turn['role']}]\n{turn['content']}")
        parts.append("[assistant]")
        return "\n\n".join(parts)


def _strip_code_fences(text: str) -> str:
    """Remove a surrounding ```json ... ``` / ``` ... ``` markdown fence if the
    model wrapped its JSON answer in one."""
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    lines = lines[1:]  # drop opening ``` / ```json line
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


_SYSTEM_TEMPLATE = """You are a telemetry incident triage agent. A confirmed \
anomaly alert has fired and you must investigate it and write a structured \
incident memo.

Alert under investigation:
{alert_json}

Domain card (known failure modes and recommended actions for this domain):
{domain_card}

You have these tools (call ONE per turn):
{tool_descriptions}

RESPOND WITH EXACTLY ONE JSON OBJECT, one of these two shapes, and nothing else:
  {{"tool": "<tool_name>", "args": {{...kwargs...}}}}
      -- invoke a tool; its JSON result is fed back to you next turn.
  {{"memo": {{"hypothesis": "<one of: {allowed}>", "confidence": <0..1>, \
"evidence": ["...", ...]}}}}
      -- your final answer. Pick the single most likely hypothesis from the \
allowed set (or "unknown" if none fit). Ends the investigation.
"""


class TriageAgent:
    def __init__(self, llm: LLMClient, tools: dict[str, Callable],
                 max_turns: int):
        self.llm = llm
        self.tools = tools
        self.max_turns = max_turns

    def investigate(self, alert: dict, domain: str,
                    allowed_hypotheses: set[str]) -> dict:
        stream_id = alert["stream_id"]

        # Domain card is fetched ONCE up front (every investigation needs it)
        # and folded into the system prompt -- not something the LLM must
        # request mid-loop as a tool call.
        domain_card = self.tools["get_domain_card"](domain=domain)
        system_prompt = _SYSTEM_TEMPLATE.format(
            alert_json=json.dumps(alert),
            domain_card=domain_card,
            tool_descriptions=self._describe_tools(),
            allowed=", ".join(sorted(allowed_hypotheses)))

        conversation: list[dict] = [{
            "role": "user",
            "content": "Investigate the alert above and produce a memo."}]

        for _ in range(self.max_turns):
            raw = self.llm.complete(system_prompt, conversation)
            parsed = self._parse(raw)

            if parsed[0] == "malformed":
                # Exactly one in-turn retry (does NOT consume a max_turns slot).
                conversation.append({"role": "assistant", "content": raw})
                conversation.append({
                    "role": "user",
                    "content": ('malformed response, respond with either '
                                '{"tool":...} or {"memo":...}')})
                raw = self.llm.complete(system_prompt, conversation)
                parsed = self._parse(raw)
                if parsed[0] == "malformed":
                    return self._no_conclusion(
                        stream_id, "malformed_llm_response")

            if parsed[0] == "memo":
                return self._finalize_memo(
                    parsed[1], stream_id, allowed_hypotheses)

            # parsed[0] == "tool". Execute; if the LLM passed bad kwargs the
            # error is fed back as a result so it can self-correct (this
            # consumes the turn like any other tool call). Tests only exercise
            # valid args, so this guard never changes their behavior.
            _, name, args = parsed
            try:
                result = self.tools[name](**args)
                content = json.dumps({"tool": name, "result": result})
            except Exception as exc:  # noqa: BLE001 - fed back to the LLM
                content = json.dumps(
                    {"tool": name, "error": f"{type(exc).__name__}: {exc}"})
            conversation.append({"role": "assistant", "content": raw})
            conversation.append({"role": "user", "content": content})

        return self._no_conclusion(stream_id, "max_turns_exceeded")

    def _parse(self, raw: str):
        try:
            obj = json.loads(raw)
        except (ValueError, TypeError):
            return ("malformed", None)
        if not isinstance(obj, dict):
            return ("malformed", None)
        if "memo" in obj and isinstance(obj["memo"], dict):
            return ("memo", obj["memo"])
        if "tool" in obj:
            name = obj["tool"]
            if name not in self.tools:
                return ("malformed", None)
            args = obj.get("args", {})
            if not isinstance(args, dict):
                return ("malformed", None)
            return ("tool", name, args)
        return ("malformed", None)

    def _describe_tools(self) -> str:
        return "\n".join(f"  - {name}" for name in self.tools)

    @staticmethod
    def _finalize_memo(memo: dict, stream_id: int,
                       allowed_hypotheses: set[str]) -> dict:
        hypothesis = memo.get("hypothesis", "unknown")
        evidence = list(memo.get("evidence", []))
        # "unknown" is always allowed; the reserved sentinel passes through.
        if hypothesis not in (allowed_hypotheses | {"unknown"}):
            evidence.append(f"original_hypothesis:{hypothesis}")
            hypothesis = "unknown"
        return {
            "stream_id": stream_id,
            "hypothesis": hypothesis,
            "confidence": memo.get("confidence", 0.0),
            "evidence": evidence,
        }

    @staticmethod
    def _no_conclusion(stream_id: int, reason: str) -> dict:
        # Reserved sentinel -- never subject to hypothesis validation.
        return {
            "stream_id": stream_id,
            "hypothesis": "no_conclusion",
            "confidence": 0.0,
            "evidence": [reason],
        }
