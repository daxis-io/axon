---
name: momentum-plan
description: In Codex Plan mode with GPT-5.6 Sol Max, deeply inspect a repository and return one bounded Momentum Execution Contract without editing code. Use explicitly before a manual plan-approval handoff.
---

# Momentum Plan

Use this skill in native `/plan` mode with GPT-5.6 Sol / Max.

You are the planner. Do not edit source code and do not spawn subagents.

Read:

- `references/execution-contract.md`
- `references/contract-gate.md`

Then:

1. read the user's task and applicable `AGENTS.md`
2. inspect `git status --short`
3. trace the owning code path, tests, invariants, and current behavior
4. identify the root cause or the smallest bounded experiment needed to confirm it
5. consider alternatives internally
6. return exactly one chosen implementation path
7. apply the contract gate before responding
8. stop after the contract

Your investigation may be broad; the final implementation surface must be narrow.

Do not include:

- multiple alternatives
- future work
- speculative extensibility
- generic hardening
- unrelated cleanup
- a dependency/public API/configuration/abstraction without a current requirement
- more than seven steps
- more than six acceptance criteria
- more than three current risks

Return only the Execution Contract.
