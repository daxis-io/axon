---
name: codex-team
description: Orchestrate independent Codex GPT workers for repository investigation, architecture, implementation, debugging, testing, performance analysis, and adversarial review. Use when a task is ambiguous, high-risk, multi-part, or benefits from an independent model pass.
argument-hint: "[engineering task]"
---

# Codex Team

Claude is the lead orchestrator. Codex threads are specialist leaf workers.
Claude owns task decomposition, decisions, integration, and final verification.

Task:
$ARGUMENTS

## 0. Preflight

Confirm `~/.codex/agents/` exists before dispatching anything.

If it is missing, **stop and say so**. Do not substitute guessed models or inline role prompts —
this skill's roles are defined by those files, and dispatching without them sends wrong models with
wrong sandboxes. Tell the user the profile library is absent and what would be needed to restore it.

Tools required: `mcp__codex__codex` (start a thread) and `mcp__codex__codex-reply` (continue one).
If they are not available, the MCP server is not registered — say so rather than working around it.
MCP servers load at session start, so a server registered mid-session is not callable until restart.

### Capability preflight

Run this after any Codex CLI upgrade, profile change, or when a dispatch behaves unexpectedly.
"Stable" does not mean every model, sandbox, and role combination behaves identically.

1. `codex --version`, and compare against the `codex_version` in the harness manifest.
2. Dispatch one `scout` read-only, with a trivial question **and** a write probe:
   `touch <repo>/.codex-sandbox-probe`.
3. Confirm the probe reports `Operation not permitted` and that the file does not exist.
4. Confirm the returned thread ID continues correctly via one `codex-reply`.

If the write probe *succeeds*, stop immediately. Read-only is not being enforced, and every
read-only role in this skill is silently write-capable.

## 1. Define the work contract

Before delegating, state:

1. The objective.
2. Verifiable acceptance criteria.
3. Non-goals.
4. Relevant repository scope.
5. Whether code changes are allowed.
6. Required tests, benchmarks, logs, or other evidence.
7. Assumptions workers must **verify rather than inherit**.

Do not delegate trivial work merely to create agents. Use at most **three active workers** by
default. A single well-scoped worker beats three vague ones.

Delegate only when at least two workstreams can genuinely proceed independently. A rename, a local
bug, a single-file change, or one serial dependency chain stays here. Workers do their own model and
tool work, so delegation pays only when parallel time saved plus independent validation plus context
isolation exceeds the extra usage and the cost of reconciling the results.

When more than three facets exist, run **waves** — do not raise the worker count. A second wave
informed by the first is usually better than a wider first wave.

### The assignment envelope

Every dispatch over MCP starts a **fresh thread with no conversation history**. A worker that is not
told something does not know it. Fill in every field that applies; omitting one is how a worker
rediscovers scope you had already settled, or violates a constraint it was never given.

```text
Task ID:
Role:
Question / deliverable:
Why it matters:
Scope and relevant paths:
Known facts and accepted decisions:
Constraints and non-goals:
Allowed actions:
Forbidden actions:
Dependencies / named handoff recipient:
Required evidence:
Return format:
Stop condition:
```

Restate safety constraints explicitly in every envelope. The sandbox and approval policy are
inherited from the parameters you pass, but the *semantic* boundaries — do not change public
contracts, do not touch unrelated files, this is the sole writer — travel only in the prompt.

## 2. Role registry

Roles are defined by TOML profiles in `~/.codex/agents/`. That directory is the single source of
truth for model, reasoning effort, sandbox, and role instructions — this table is a routing index
only. **Read the profile at dispatch time; never hardcode its values from this table.**

Those files are installed from `~/gpt56-codex-engineering-harness` (`home/agents/`), which is the
version-controlled origin. If a profile looks wrong, fix it there and reinstall rather than editing
`~/.codex/` directly — a direct edit is drift that `scripts/capture-manifest.py` will flag but that
nothing will preserve.

### Read-only roles

| Role | Profile | Use for |
| --- | --- | --- |
| `scout` | `scout.toml` | One narrow evidence question. Leaf lookups. |
| `code_mapper` | `code_mapper.toml` | Execution paths, contracts, ownership, repo mapping. |
| `architect` | `architect.toml` | Invariants, alternatives, failure modes, migration, operability. |
| `docs_researcher` | `docs_researcher.toml` | Version-sensitive API/spec/changelog behavior (live web). |
| `test_auditor` | `test_auditor.toml` | Coverage gaps and high-value validation design. |
| `reviewer` | `reviewer.toml` | Independent owner-level review of a diff. |
| `security_reviewer` | `security_reviewer.toml` | Trust boundaries, authz, secrets, injection, isolation. |
| `performance_reviewer` | `performance_reviewer.toml` | Algorithmic cost, allocations, I/O, contention, benchmark validity. |

### Write-capable roles (worktree required)

| Role | Profile | Use for |
| --- | --- | --- |
| `debugger` | `debugger.toml` | Reproduce, minimize, test hypotheses, isolate root cause. |
| `implementer` | `implementer.toml` | Bounded patch against an accepted plan. |
| `worker` | `worker.toml` | Scoped, well-understood implementation. |
| `smart_worker` | `smart_worker.toml` | Difficult implementation or material ambiguity. |

Prefer the narrowest role that can answer the question. Reach for `architect` or `smart_worker` only
when the problem is genuinely ambiguous.

## 3. Dispatch procedure

For role `R`, read `~/.codex/agents/R.toml` and call `mcp__codex__codex` with:

| Parameter | Value |
| --- | --- |
| `model` | the profile's `model` |
| `config` | `{"model_reasoning_effort": <profile's model_reasoning_effort>}`, plus `"web_search": "live"` if the profile sets it |
| `sandbox` | the profile's `sandbox_mode` — **always passed explicitly** |
| `developer-instructions` | the profile's `developer_instructions`, verbatim |
| `approval-policy` | `"never"` |
| `cwd` | absolute path — repo root for read-only, the worktree for write-capable |
| `prompt` | the bounded task contract for this worker |

Then record the returned **thread ID**.

### Why `sandbox` is never omitted

`~/.codex/config.toml` sets `sandbox_mode = "workspace-write"` globally. A call that omits `sandbox`
inherits **write access**. Read-only is not the default — passing it explicitly is what makes a
read-only role read-only.

### Hard rules

- Never use `danger-full-access`. It is a valid enum value; it is not a valid choice here.
- Always pass an absolute `cwd`.
- Give each worker a bounded task, not the entire user request.
- Preserve thread ID, role, model, sandbox, and cwd for every worker.
- Never let two write-capable workers share a working directory.
- No recursive delegation — workers are leaves.
- Never pass secrets, `.env` contents, credentials, or unrelated personal context.
- Continue a thread only with `mcp__codex__codex-reply` and its exact `threadId`.
- Keep at most three worker threads active at once.

## 3a. What this transport does and does not give you

Codex's native multi-agent mode has features the MCP surface does not expose. Three of them appear
in the Codex-side orchestration guidance and **do not apply here**. Do not write prompts that assume
them.

| Codex-native mechanism | Over MCP |
| --- | --- |
| `fork_turns: "none"` for fresh context | Not needed and not available. Every `mcp__codex__codex` call is already a fresh thread. This is precisely why the assignment envelope is mandatory rather than advisory. |
| Peer agent-to-agent messaging | **Unavailable.** There is no inbox. A worker told to "message the test auditor" will not. Every handoff routes through Claude — collect the finding, then include it in the next worker's envelope. |
| `max_concurrent_threads_per_session = 3` | Governs Codex-native spawning only. It does not bound how many MCP calls Claude issues in parallel. The three-worker budget here is enforced by this skill, not by the runtime. |

Context modes available on this transport:

| Mode | How |
| --- | --- |
| Fresh context | `mcp__codex__codex` — the only way to start. Always fresh. |
| Bounded continuation | `mcp__codex__codex-reply` with the thread ID. The worker retains its own prior turns, nothing of Claude's. |
| Full conversation inheritance | Not available. Do not promise a worker context it cannot receive. |

The same caveat applies to `sandbox` as to concurrency: Claude Code permission rules match tool
names, not arguments, so nothing mechanically distinguishes a `read-only` call from a
`danger-full-access` one. Passing the profile's sandbox explicitly is a convention this skill
enforces by text. Treat it as load-bearing.

## 4. Worktree isolation

Every write-capable worker gets its own worktree. Read-only workers use the repo root.

```bash
RUN_ID="$(date +%Y%m%d-%H%M)-<topic>"
git worktree add -b "agent/${RUN_ID}-implementer" ".worktrees/${RUN_ID}-implementer" HEAD
```

Pass the resulting **absolute** path as `cwd`. Then:

- Keep the main checkout untouched while the worker operates.
- Review the diff from outside the worker thread.
- Never remove a worktree containing uncommitted work.
- Integrate only after independent review and Claude's own verification.

`.worktrees/` is gitignored and already holds many entries, so the run-id prefix matters — check
`git worktree list` before creating one.

Note that `[sandbox_workspace_write] network_access = false` applies: write workers have no network.
If a task genuinely needs the network, that is a signal to reconsider the decomposition, not to relax
the sandbox.

## 5. Required worker response

Every worker must return:

1. Conclusion.
2. Evidence — file paths, symbols, line ranges.
3. Commands run and relevant output.
4. Assumptions verified.
5. Assumptions **not** verified.
6. Risks and edge cases.
7. Recommended next action.
8. Confidence, with reasons for uncertainty.

Write-capable workers must additionally return changed files, a behavioral explanation, tests added
or modified, exact commands run, test and benchmark results, known limitations, and confirmation that
no unrelated changes were introduced.

Claims without reproducible evidence are not sufficient. A worker that reports a check passed without
having run it has failed the task.

## 6. Reconciliation

After workers return:

1. Compare findings; do not concatenate them.
2. Identify agreements, disagreements, and unsupported claims.
3. Challenge weak findings with a targeted `codex-reply` to that thread.
4. Never ask an implementation thread to be its own independent reviewer.
5. Prefer one additional targeted experiment over model voting.
6. Inspect the relevant files and diffs yourself.
7. Run or independently confirm the final verification commands yourself.

The final answer must distinguish:

- Directly observed evidence.
- Worker conclusions supported by evidence.
- Claude's own inference.
- Remaining uncertainty.

State unresolved uncertainty explicitly rather than smoothing it over.

## 7. Run ledger

For **multi-worker runs only**, maintain `.ai/runs/<run-id>/run.md`:

```markdown
# <run-id> — <objective>

harness_id: <from scripts/capture-manifest.py in the harness repo>

## Acceptance criteria
- ...

## Workers
| Role | Model | Sandbox | cwd | Thread ID | Status |
| --- | --- | --- | --- | --- | --- |

## Evidence
- commands run, key file:line references

## Decision
- outcome, and what remains uncertain
```

This exists so thread IDs and evidence survive a context compaction, and so a run can be audited
afterward. Skip it for single-worker dispatches — the overhead is not worth it there.
