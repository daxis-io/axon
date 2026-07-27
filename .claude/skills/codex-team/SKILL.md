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

Call `mcp__codex-pool__pool_status`. It returns the repository in use, the
installed roles with their models and sandboxes, and where this run's ledger is
being written.

If the tool is unavailable, the `codex-pool` MCP server is not registered — say
so rather than working around it. MCP servers load at session start, so a server
registered mid-session is not callable until Claude Code restarts. Do not fall
back to `mcp__codex__codex`: the raw tool takes `sandbox` and `cwd` as caller
arguments and is exactly what the pool exists to replace.

If `profiles_installed` is false the pool is running on the harness repository's
bundled profiles rather than the installed copy. Dispatch still works; say so,
because that is a different configuration from the one the manifest ids describe.

## 1. Define the work contract

Before delegating, state:

1. The objective.
2. Verifiable acceptance criteria.
3. Non-goals.
4. Relevant repository scope.
5. Whether code changes are allowed.
6. Required tests, benchmarks, logs, or other evidence.
7. Assumptions workers must **verify rather than inherit**.

Do not delegate trivial work merely to create agents. The pool caps active
workers at three. A single well-scoped worker beats three vague ones.

Delegate only when at least two workstreams can genuinely proceed independently.
A rename, a local bug, a single-file change, or one serial dependency chain
stays here. Workers do their own model and tool work, so delegation pays only
when parallel time saved plus independent validation plus context isolation
exceeds the extra usage and the cost of reconciling the results.

When more than three facets exist, run **waves** — do not try to raise the
worker count. A second wave informed by the first is usually better than a wider
first wave.

### The assignment envelope

Every `dispatch` starts a **fresh thread with no conversation history**. A worker
that is not told something does not know it. Fill in every field that applies;
omitting one is how a worker rediscovers scope you had already settled, or
violates a constraint it was never given.

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

Restate *semantic* boundaries explicitly — do not change public contracts, do
not touch unrelated files, this is the sole writer. The sandbox is enforced by
the pool; these are not, and they travel only in the prompt.

## 2. Role routing

Roles are defined by TOML profiles the pool reads at dispatch. This table is a
routing index only; `pool_status` returns the authoritative list.

### Read-only roles

Run in the repository root. They cannot write.

| Role | Use for |
| --- | --- |
| `scout` | One narrow evidence question. Leaf lookups. |
| `code_mapper` | Execution paths, contracts, ownership, repo mapping. |
| `architect` | Invariants, alternatives, failure modes, migration, operability. |
| `docs_researcher` | Version-sensitive API/spec/changelog behavior (live web). |
| `test_auditor` | Coverage gaps and high-value validation design. |
| `reviewer` | Independent owner-level review of a diff. |
| `security_reviewer` | Trust boundaries, authz, secrets, injection, isolation. |
| `performance_reviewer` | Algorithmic cost, allocations, I/O, contention, benchmark validity. |

### Write-capable roles

The pool creates a dedicated git worktree for each one and passes it as the
working directory. You never name a path.

| Role | Use for |
| --- | --- |
| `debugger` | Reproduce, minimize, test hypotheses, isolate root cause. |
| `implementer` | Bounded patch against an accepted plan. |
| `worker` | Scoped, well-understood implementation. |
| `smart_worker` | Difficult implementation or material ambiguity. |

Prefer the narrowest role that can answer the question. Reach for `architect` or
`smart_worker` only when the problem is genuinely ambiguous.

## 3. Dispatch

```text
mcp__codex-pool__dispatch(role, task, scope_paths?)
mcp__codex-pool__follow_up(worker_id, prompt)
mcp__codex-pool__list_workers()
```

That is the whole surface. There is no `sandbox`, `cwd`, `model`, or
`developer-instructions` parameter, because those are not decisions you should
be making per call — they belong to the role, and the pool reads them from its
profile. `danger-full-access` is not rejected at runtime; there is no argument
that could carry it.

What this means in practice:

- You cannot accidentally dispatch a read-only role with write access.
- You cannot put two write-capable workers in the same directory.
- You cannot exceed three active workers; the pool refuses the fourth.
- `scope_paths` is prompt context. It points a worker at files; it does not
  change where the worker runs or what it may touch.

Continue a worker only with `follow_up` and its exact `worker_id`. The worker
retains its own prior turns and nothing of yours.

Still your job, because the pool cannot enforce them:

- Give each worker a bounded task, not the entire user request.
- Never pass secrets, `.env` contents, credentials, or unrelated personal context.
- No recursive delegation — workers are leaves.

## 3a. What this transport does and does not give you

Codex's native multi-agent mode has features this surface does not expose. Do
not write prompts that assume them.

| Codex-native mechanism | Here |
| --- | --- |
| `fork_turns: "none"` for fresh context | Not needed. Every `dispatch` is already a fresh thread. This is why the assignment envelope is mandatory rather than advisory. |
| Peer agent-to-agent messaging | **Unavailable.** There is no inbox. A worker told to "message the test auditor" will not. Every handoff routes through Claude — collect the finding, then include it in the next worker's envelope. |
| `max_concurrent_threads_per_session` | Governs Codex-native spawning only. The three-worker budget here is enforced by the pool. |

Context modes:

| Mode | How |
| --- | --- |
| Fresh context | `dispatch` — the only way to start. Always fresh. |
| Bounded continuation | `follow_up` with the worker id. |
| Full conversation inheritance | Not available. Do not promise a worker context it cannot receive. |

Write-capable workers have **no network** (`network_access = false`). If a task
genuinely needs the network, that is a signal to reconsider the decomposition,
not to relax the sandbox. Use `docs_researcher` for live web reads.

## 4. Read the evidence, not the prose

This is the part that changes how you treat a worker's answer.

GPT-5.6 models fabricate command results. Not occasionally — HCP-0003 measured
six of six variants reporting verbatim shell output and an exit status for a
command that was never executed, including under a prompt that explicitly forbade
predicting output. The fabricated answers were **correct**, professionally
formatted, and passed every content-based assertion. This behavior has been
reproduced twice through the pool itself.

So every result carries a machine-computed verdict. Read it first.

| `evidence_status` | Meaning | What to do |
| --- | --- | --- |
| `verified` | Every claimed command appears in the event stream. | Proceed. |
| `partially_verified` | Some claimed commands have no counterpart. | The named commands did not run. Treat any conclusion resting on them as unsupported. |
| `unverified` | Commands were claimed and **none** ran. | The result is a prediction. Do not build on it. Re-dispatch or verify yourself. |
| `no_command_evidence` | Nothing claimed, nothing run. | Reasoning over context. Fine for a design question; not evidence about the repository. |

For write-capable workers, `changed_files_status` compares the worker's reported
edits against `git status` run by the pool in the worktree:

| Status | Meaning |
| --- | --- |
| `consistent` | The report matches the disk. |
| `undisclosed_changes` | Files changed that the worker did not report. Scope it did not disclose. |
| `contradicted` | Files reported that did not change. The edit did not land, which usually invalidates its test results too. |

A worker that reports a check passed without having run it has failed the task,
regardless of whether its conclusion happens to be right.

## 5. What a worker returns

The result is schema-constrained, so these fields are always present:
conclusion, evidence, `claimed_commands`, assumptions verified, assumptions
**not** verified, risks, next action, confidence and its reasons.

Write-capable workers additionally return changed files, the behavior change,
tests touched, verification results, limitations, and whether unrelated changes
were introduced.

`executed_commands` is added by the pool from the event stream. When it and
`claimed_commands` disagree, the event stream is the fact.

## 6. Reconciliation

After workers return:

1. Check `evidence_status` before reading any conclusion.
2. Compare findings; do not concatenate them.
3. Identify agreements, disagreements, and unsupported claims.
4. Challenge weak findings with a targeted `follow_up` to that worker.
5. Never ask an implementation worker to be its own independent reviewer.
6. Prefer one additional targeted experiment over model voting.
7. Inspect the relevant files and diffs yourself.
8. Run or independently confirm the final verification commands yourself.

The final answer must distinguish:

- Directly observed evidence.
- Worker conclusions supported by evidence.
- Claude's own inference.
- Remaining uncertainty.

State unresolved uncertainty explicitly rather than smoothing it over.

## 7. Integrating a worktree

The pool creates worktrees; it does not merge them. After review:

- Inspect the diff from outside the worker thread.
- Never remove a worktree containing uncommitted work — it is the only copy
  until you integrate it. The pool refuses this by default.
- Integrate only after independent review and your own verification.

## 8. Run ledger

Written automatically to `.ai/runs/<run-id>/` — no action needed:

```text
manifest.json              harness ids, workers, models, sandboxes, thread ids
workers/<worker-id>.md     conclusion, evidence, both command lists, diff status
evidence/events-*.jsonl    the raw stream each verdict was computed from
```

`pool_status` and `list_workers` report the path. Cite it when a run's
conclusions need to survive a context compaction or be audited afterward.
