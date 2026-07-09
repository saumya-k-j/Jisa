# Project: Real-Time Telemetry Anomaly Engine

## What this is
A domain-agnostic engine that ingests high-rate telemetry streams and raises
calibrated anomaly alerts with bounded detection latency. C++20 core, Python
research + agent + API layers. This is a portfolio system for quant/infra/AI
recruiting: it must be interrogatable, benchmarked, and honest. Every design
choice must be defensible to a senior engineer in an interview.

Read SPEC.md for the full contract before doing any work.

## Autonomy and reporting rules (always apply)

### Progress honesty
Before reporting progress, audit each claim against a tool result from this
session. Only report work you can point to evidence for; if something is not yet
verified, say so explicitly. Report outcomes faithfully: if tests fail, say so
with the output; if a step was skipped, say that; when something is done and
verified, state it plainly without hedging.

### Autonomous operation
You are operating autonomously. The user is not watching in real time and cannot
answer questions mid-task. For reversible actions that follow from the original
request, proceed without asking. Before ending your turn, check your last
paragraph: if it is a plan, a question, or a promise about work you have not done,
do that work now with tool calls.

### Checkpoints
Pause for the user only when the work genuinely requires their input: a
destructive or irreversible action, a real scope change, or something only they
can provide. Otherwise keep going and report back when done.

## Commit messages
Do not add Co-Authored-By trailers, Claude-Session links, or generation
notices to commit messages.

## Engineering conventions
- Language: C++20 for the engine and detection hot path; Python for research,
  the triage agent, and the FastAPI service.
- Build: CMake + Ninja. Compile with -Wall -Wextra -Werror.
- Sanitizers: ASan/TSan/UBSan enabled in the debug build and run in CI.
- Hot path: NO heap allocation after startup. Use the pool allocator. No
  exceptions on the hot path. No locks on the ingestion path (lock-free SPSC/MPSC).
- Testing: test-driven. Tests are written from SPEC.md by the test-writer agent,
  BEFORE the implementation is written by a DIFFERENT agent. Implementation is
  not "done" until the independent tests pass.
- Determinism: the replay harness must reproduce identical checksummed end-state
  from a recorded stream. This is a hard requirement, not a nice-to-have.
- Do NOT add unnecessary abstraction, future-proofing, or excessive error
  handling. Do NOT convert this into a monorepo / package-workspace. One clean
  repo. Prefer the simplest thing that meets the spec.

## Framing note (avoids safety misrouting)
This is telemetry ingestion and data-quality validation for sensor/market data.
Frame networking work as "client resilience and reconnection," never as
security, hardening, exploits, or attacks. Do not describe internal reasoning
verbatim in responses.

## Commands
- Build: `cmake -B build -G Ninja && cmake --build build`
- Test: `ctest --test-dir build --output-on-failure`
- Lint: `clang-tidy` and `clang-format --dry-run --Werror`
- Python tests: `pytest python/`

## Living documents you must maintain
- PLAN.md — current phase, what's done, what's next.
- DECISIONS.md — every architectural decision with a one-line rationale.
- VERIFICATION.md — every claimed capability mapped to the exact test/command
  that proves it, marked verified / unverified / needs-live-validation.
