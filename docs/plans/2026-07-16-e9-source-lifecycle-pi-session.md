# E9 Slice 1 — Selected-source integrity and authoritative execution lifecycle

- Date: 2026-07-16
- Branch: `feat/e9-source-lifecycle-pi`
- Worktree: `.worktrees/e9-source-lifecycle-pi`
- Baseline: `origin/main` at
  `c56a038e5e27151da997b56d85abc9a44820bf3e`
- Status: implementation in progress
- Scope: E9 Slice 1 only. No push and no protobuf or generated-contract change.

## Preflight

`git fetch origin main` completed before the worktree was created. The fetched
`origin/main` and live `refs/heads/main` both resolved to
`c56a038e5e27151da997b56d85abc9a44820bf3e`. The prerequisite E0 run-state,
E3A data-access and execution contracts, browser metrics, bounded readahead, E9
vertical-slice plan, and architecture-reconciliation commits are ancestors of
that ref. No local or remote `feat/e9-source-lifecycle-pi` branch and no
`.worktrees/e9-source-lifecycle-pi` directory existed. A source search found no
landed `QuerySourceSelection` or execution-lifecycle implementation.

The root checkout was already dirty and divergent. It is not the implementation
workspace and remains untouched. All evidence below comes from the isolated
worktree created directly from `origin/main`.

## Current behavior to remove

### Source substitution and sample defaults

`apps/axon-web/src/services/query-source.ts` contains both substitution paths:

- `querySourceFromConnectedCatalogs()` first tries the active ref, then scans
  catalogs and schemas for the first queryable table, then returns
  `SAMPLE_QUERY_SOURCE`.
- `resolveActiveTableRef()` keeps a resolvable active ref but otherwise calls
  `firstQueryableTableRef()`.
- `querySourceForTable()` returns `SAMPLE_QUERY_SOURCE` from its final arm even
  though its caller is expected to have proved queryability.

The connection slice compounds this behavior. `upsertCatalog()` always selects
the first queryable incoming table. `removeCatalog()` selects another queryable
table when the active catalog is removed, and `resolveActiveTableRef()` may
replace any stale selection.

The sample fixture is a legitimate explicit catalog in
`editor/connect/store.ts`, where `SAMPLE_CONNECTED_CATALOG` is constructed from
`SAMPLE_QUERY_SOURCE`. It must remain available only through exact sample-table
selection.

Execution-facing APIs also inject sample implicitly:

- `services/query.ts`: `getSession()`, `getCurrentSession()`, and `runQuery()`;
- `services/catalog.ts`: `loadCatalog()` and `snapshotCatalog()`;
- `services/snapshot.ts`: `loadCommits()`;
- `query/catalog.ts`, `editor/App.tsx`, and `editor/CatalogsPage.tsx` accept only
  an openable `QueryTableSource`, so their callers cannot represent unavailable
  selection without substituting a source first.

The Slice 1 target is a closed `QuerySourceSelection` with `resource`, `sample`,
and `unavailable` arms. `resolveQuerySourceSelection()` resolves only the exact
selected catalog/schema/table. Missing, empty, stale, and unqueryable states
use stable disabled query keys and never call a loader, session, table opener,
lazy worker import, or SQL function.

### Current identifier and cancellation flow

`services/query.ts` owns a module `requestCounter`. `createQueryClient()` gives
the generic SDK `editor-request-*` IDs. `ensureTable()` independently creates
`editor-open-*`, and `runQuery()` independently creates `editor-query-*`.
`AxonBrowserClient.cancelQuery()` creates another transport request ID unless a
caller supplies one and sends the query ID separately. The editor does not call
it at all.

`editor/App.tsx` owns an `AbortController`. Cancel aborts local awaits, clears the
elapsed timer, and calls the run slice's `cancelRun()`. That action immediately
resets accepted work to `{status: 'idle'}`. It does not record a cancellation
request or wait for worker confirmation. Progress callbacks and success/error
completion are not guarded by one execution identity.

The Slice 1 target creates the execution ID and absolute deadline in the run
controller before admission. SQL worker `request_id` and cancellation
`query_id` are that execution ID. Open and cancel-command request IDs are
internal spans. Cancellation is attached before session/open/query work,
`cancel_requested` is nonterminal, and the first completed, failed, or cancelled
transition is authoritative. Late callbacks become recorded invariant
violations and cannot update UI or emit another frame.

### Current deadlines, budgets, and Arrow delivery

The browser runtime's established execution timeout is 120,000 ms in
`crates/wasm-query-runtime`. The editor does not derive one end-to-end deadline
or recalculate remaining time before session, open, and SQL.

Existing browser-safe limits are:

- 501 result rows, including the pagination sentinel;
- 8 MiB Arrow IPC;
- 256 KiB preview strings; and
- the optional existing `max_scan_bytes` field when supplied.

The SDK applies these defaults, but `queryCommand()` unconditionally requests
`chunked_buffers`. The worker supports both delivery modes and posts chunks
before the success envelope. Slice 1 retains the generic SDK's chunked default
while adding an explicit delivery choice. The editor requests `single_buffer`
and carries the admitted Arrow maximum. Before any Arrow result or success is
published, the worker must compare the actual encoded byte length with that
maximum. Oversize output is a structured resource-limit failure with no Arrow
publication. The worker's 750 KB artifact budget is unchanged.

## Lifecycle contract for this slice

Admission input is immutable and contains the canonical selected-source
identity, exact SQL, execution target, absolute deadline, result-row maximum,
Arrow-byte maximum, preview-string maximum, and optional scan-byte maximum.
IDs, descriptors, cancellation handles, listeners, timers, and any
capability-bearing values remain memory-only and are never logged or persisted.

The lifecycle supports:

- deterministic caller-created IDs and clocks in tests;
- identical admission replay without relaunch;
- mismatched ID reuse rejection for every immutable field without changing the
  original record;
- invalid-limit and expired-deadline rejection without an execution stream;
- cancel-before-admit tombstones retained through deadline processing;
- one launch for accepted work and exactly one terminal outcome;
- idempotent cancellation before, during, and after execution;
- first-terminal-wins across success, worker error, cancellation, and deadline;
- ordered nonterminal frames, at most one terminal delivery, and no frame after
  terminal;
- bounded records, timers, cancellation handles, and subscriptions; and
- a fresh ID and deadline for each pagination request in addition to existing
  source/query/page stale-result guards.

Run UI state becomes `idle | created | running | cancel_requested | rejected |
completed | failed | cancelled`, carries `executionId` after creation, and
guards every mutation by it. Accepted work never returns directly to idle.

## TDD and evidence log

Baseline focused command:

```text
npm exec -- vitest run src/services/query-source.test.ts src/services/query.test.ts src/state/slices/connections.test.ts src/state/slices/run.test.ts src/editor/App.test.ts
```

Baseline result: exit 0; 4 files and 17 tests passed. `query.test.ts` did not
exist on the baseline. Node emitted the pre-existing invalid
`--localstorage-file` warning.

The following sections are filled with the first expected red output and the
focused green result before each implementation commit.

### Authoritative selection

- Red: `npm exec -- vitest run src/services/query-source.test.ts
src/query/catalog.test.ts src/state/slices/connections.test.ts
src/editor/App.test.ts` exited 1. Vitest ran 32 tests: 16 failed and 16
  passed. The failures were the missing resolver/App guard, absent disabled query
  options, first-table selection for multi-table Connect, next-table selection
  after removal, and retained active selection after multi-table replacement.
- Green: the expanded focused command covering selection, catalog adapters,
  connections, existing run state, and App exited 0 with 5 files and 40 tests
  passed. The SDK query-source Playwright contract exited 0 with 11 tests
  passed. Fresh `tsc --noEmit` and ESLint both exited 0 after generating the
  missing WASM bindings in the new worktree. The pre-existing Node
  `--localstorage-file` warning remains non-fatal. A follow-up compatibility
  case first failed because a sole table loaded at startup had no exact ref; the
  connection slice now materializes that ref only for exactly one queryable
  table, and the focused connection suite passed all 11 tests. Multiple-table
  startup remains unselected.

### Execution lifecycle and guarded run state

- Red: `npm exec -- vitest run src/services/execution-lifecycle.test.ts
  src/state/slices/run.test.ts` exited 1. The lifecycle suite could not load the
  not-yet-created authority module, and the run-state regression failed because
  `createRun` did not exist. The seven baseline run tests remained green.
- Green: the same command exited 0 with 2 files and 21 tests passed. A broader
  focused run covering lifecycle, guarded run state, App, selection, catalog,
  and connection behavior exited 0 with 6 files and 54 tests passed;
  `npm exec -- tsc --noEmit` also exited 0. The controller creates an immutable
  ID/deadline/budget admission before launch, identical replay is nonlaunching,
  mismatched reuse preserves the original, records are bounded, UI transitions
  are execution-ID guarded, every page request receives a new ID, SQL uses the
  domain ID, and table-open requests use an internal execution span.

### Cancellation and terminal races

- Red: pending
- Green: pending

### Deadline and Arrow bounds

- Red: pending
- Green: pending

## Commit boundaries

The final branch must contain exactly these six commits, in order:

1. `docs: plan e9 source lifecycle pi`
2. `fix(web): require authoritative query selection`
3. `refactor(web): model one execution lifecycle`
4. `fix(web): route cancellation through execution lifecycle`
5. `fix(web): enforce execution deadline and buffer bounds`
6. `docs: document e9 slice one handoff`

Blocker or high audit findings are amended into the owning implementation
commit so this history does not grow. No commit is pushed.

## Planned test surface

Focused tests cover missing, empty, stale, unqueryable, explicit sample,
single-table and multi-table Connect, replacement/removal, local Delta, GCS, S3,
and disabled-query negative spies. Lifecycle tests cover deterministic IDs,
response loss and identical replay, every immutable mismatch, cancel before
admit, cancel during open, repeated and post-terminal cancel, worker failure,
deadline/cancel/success races, late callbacks, terminal-frame ordering, bounded
records, and persistence exclusion. Query/SDK/worker tests cover fresh page IDs,
stale page suppression, impossible limits, 120-second remaining-deadline
propagation, no accepted-work retry, explicit editor single-buffer delivery,
generic SDK chunked delivery, and oversize-result suppression.

The final verification bundle is the exact web, Playwright, Rust, WASM, artifact,
security, generated-diff, status, and six-commit log matrix in the execution
prompt. Live GCS and S3 tests run only when their required environment inputs
exist; otherwise the exact missing variable or external condition is recorded.

## Non-goals

- No E9 Slice 2 provider seams or generated-contract adoption.
- No mandatory E3A correction in this branch.
- No protobuf, generated TypeScript, or generated Rust edits.
- No settings UI or generated configuration change.
- No remote executor, automatic browser-to-remote fallback, query queue,
  resume/status protocol, retry of accepted work, or exactly-once claim.
- No credit-based Arrow chunking protocol and no removal of generic SDK chunk
  support.
- No capability, descriptor, signed URL, grant, or lifecycle persistence or
  logging.
- No unrelated Rust product change. Rust changes require a failing lower-layer
  bounds test and must retain the 750 KB worker artifact budget.

## Exit gates and handoff

Slice 1 passes only if all of the following are proved:

- missing or invalid selection cannot run the wrong table or implicit sample;
- unavailable query options cannot construct a session, open a table, import a
  worker, or run SQL;
- one lifecycle owns admission, cancellation, progress, metrics, and exactly
  one completed, failed, or cancelled outcome;
- one deadline and admitted budgets reach the editor worker path;
- E9 uses one bounded Arrow buffer and cannot publish oversize bytes;
- existing explicit sample, local Delta, GCS, S3, pagination, and generic SDK
  chunk paths remain supported;
- the requested verification bundle passes or any live-only skip is recorded
  precisely;
- protobuf and generated-contract diff is empty;
- the worktree is clean and the branch has exactly the six named commits.

Only if every gate passes should the next PI be the mandatory E3A correction.
E9 Slice 2 remains blocked until that correction lands.

## Final evidence

- Commit SHAs: pending
- Focused and full verification: pending
- Live smoke inputs/skips: pending
- Audit findings and resolutions: pending
- Exit-gate verdict: pending
