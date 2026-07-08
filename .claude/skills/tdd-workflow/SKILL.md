---
name: tdd-workflow
description: Test-driven workflow with a SEPARATE test author. Load for any module implementation task.
---
# TDD workflow (RED-GREEN-REFACTOR, separate authors)

1. The test-writer agent writes tests FROM SPEC.md — never from the implementation.
2. A DIFFERENT agent (implementer) writes code until those tests pass.
   Rationale: an agent that writes its own tests writes weak tests that
   rubber-stamp its own bugs. The authors must differ.
3. RED: tests exist and fail. GREEN: minimal code to pass. REFACTOR: clean up
   with tests green.
4. Tests live in tests/ mirroring src/. Each covers the spec'd interface,
   documented edge cases, and failure modes.
5. A module is not "done" until: independent tests pass, sanitizers clean,
   and the capability is recorded in VERIFICATION.md with a rerunnable command.
