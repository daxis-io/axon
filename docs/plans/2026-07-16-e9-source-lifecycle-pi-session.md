# E9 Slice 1 — Selected-source integrity and authoritative execution lifecycle

- Date: 2026-07-16
- Branch: `feat/e9-source-lifecycle-pi`
- Worktree: `.worktrees/e9-source-lifecycle-pi`
- Baseline: `origin/main` at
  `c56a038e5e27151da997b56d85abc9a44820bf3e`
- Final integration base: `origin/main` at
  `f8e530c0d9422b8e970e397472147fbf965aa6ef`
- Status: Slice 1 implementation and verification complete
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

During the feedback closeout, `origin/main` advanced by five non-overlapping
browser-cache/readahead implementation, test, and planning commits ending at
`f8e530c0d9422b8e970e397472147fbf965aa6ef`. The final autosquash replayed the
same six branch commits directly onto that ref. The complete post-rewrite web,
browser, Rust, artifact, security, diff, and history verification below ran on
that final integration base.

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

- Red: `npm exec -- vitest run src/services/execution-lifecycle.test.ts
src/state/slices/run.test.ts` exited 1 with 8 expected lifecycle failures and
  21 passing admission/run-state tests. The missing controller subscription,
  cancellation-handle, cancellation-request, and terminal-transition APIs were
  the failures.
- Green: `npm exec -- vitest run src/services/execution-lifecycle.test.ts
src/services/query.test.ts src/state/slices/run.test.ts
src/editor/App.test.ts` exited 0 with 4 files and 36 tests passed;
  `npm exec -- tsc --noEmit` also exited 0. Coverage includes response-loss
  replay, cancel-before-admit, cancellation during session/open/query stages,
  repeated and post-terminal cancellation, worker failure, all three
  first-terminal winners, late-frame suppression, and fresh page-execution
  guards. The editor now publishes worker events and terminal outcomes only
  through the lifecycle, and controller cancellation aborts local waits while
  targeting SQL with `query_id = execution_id` once SQL has started.

### Deadline and Arrow bounds

- Red: `npm exec -- vitest run src/services/execution-lifecycle.test.ts
src/services/query.test.ts` exited 1. Ten deadline/admission tests failed
  because invalid inputs were still accepted, no authority timer existed, and
  expired records were not sweepable; the query suite could not load the
  not-yet-created worker bounds module. The 21 earlier lifecycle tests remained
  green.
- Green: the focused lifecycle/query/run/App command exited 0 with 4 files and
  55 tests passed, and `npm exec -- tsc --noEmit` exited 0. Two targeted SDK
  Playwright tests proved the generic query path remains `chunked_buffers` and
  explicit editor selection is `single_buffer`. Real Chromium worker probes
  passed for cancellation terminal ordering and oversized-output suppression.
  The latter initially proved the lower runtime already withholds oversize
  output but labeled it `fallback_required`; the TypeScript worker now
  normalizes that exact `max_output_ipc_bytes` path to a structured
  `execution_failed` resource-limit error and publishes neither Arrow chunks nor
  success. No Rust product change was required.

### Fresh-session review execution

- Red: the independent review regressions ran with `npm exec -- vitest run
src/services/execution-lifecycle.test.ts src/services/query.test.ts
src/state/slices/run.test.ts src/editor/App.test.ts` and exited 1 with 44
  failures and 23 passes. The failures reproduced missing selected-ref/version
  identity, deadline-before-reuse mutation, non-finite fingerprint collisions,
  duplicate-terminal silence, active-run reset, local cancellation winning a
  running-SQL race, and stale timer ownership. A follow-up cancellation test
  timed out because an already-cancelled, hung SQL promise could not be released
  when the lifecycle deadline later won.
- Green: the final focused six-file command exited 0 with 91 tests. The full web
  unit suite exited 0 with 32 files and 240 tests. The source identity now owns
  the exact selected ref, source access identity, catalog snapshot, and requested
  snapshot pin. Admission equality is structural, checks ID reuse before deadline
  processing, and distinguishes every JavaScript numeric value with `Object.is`.
  Running SQL waits for worker completion, failure, or cancellation confirmation;
  the existing authoritative lifecycle timer separately signals deadline cleanup
  without creating another timer.
- Integration red/green: the first full unit run exposed two persisted catalog
  restore failures because the query-cache allowlist still recognized the old
  short identity tuples. The allowlist and cache schema version were updated;
  its focused 19-test suite and the subsequent 240-test full suite passed. Two
  codegen cleanup tests timed out only during that first parallel run and passed
  in isolation. Buf-backed checks initially could not reach `buf.build` inside
  the network sandbox and passed on the required network-enabled rerun.

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
At this 2026-07-16 handoff, E9 Slice 2 remained blocked until that correction;
the 2026-07-23 addendum records that the gate is now satisfied.

## Final evidence

### Commit identities

The five implementation predecessors are immutable at handoff:

1. `4c02123b82eb8e8e2363161811841f04fad5a531` —
   `docs: plan e9 source lifecycle pi`
2. `530f1adb1ece07ac89998cb62a90ade253a79037` —
   `fix(web): require authoritative query selection`
3. `89a6fb1b88998ddbb5c8aef8fdee9a69756ac60f` —
   `refactor(web): model one execution lifecycle`
4. `8367e90f710d499c313f3e057044b72233d3dec6` —
   `fix(web): route cancellation through execution lifecycle`
5. `61072237163b806e64c60d2276cc7a6d277edee7` —
   `fix(web): enforce execution deadline and buffer bounds`

The sixth identity is this handoff commit itself and is therefore recorded by
`git rev-parse HEAD` in the delivery evidence rather than embedded
self-referentially in its own contents. Its subject is
`docs: document e9 slice one handoff`.

### Focused and full verification

All requested deterministic checks passed:

- All three configuration/contract/codegen checks exited 0.
- The final focused Vitest command passed 6 files and 91 tests. The full web
  unit suite passed 32 files and 240 tests.
- `npm run test:sdk` passed all 147 SDK tests, including generic chunked
  delivery and explicit single-buffer delivery.
- The full Playwright matrix passed 24 tests across Chromium, Firefox, and
  WebKit after building the repository fixture and installing the exact
  Playwright browser revisions required by this fresh worktree.
- `npm run build:fixture` and `npm run build:wasm` passed. The latter emitted
  only the existing dependency deprecation warning.
- `tsc --noEmit`, ESLint with zero warnings, and Prettier verification passed.
- With `npm run dev:server` serving HTTPS on port 5173, the editor smoke passed
  42 tests and skipped only its 2 explicitly gated stress cases. The local
  Delta smoke passed all 10 tests. Its first sandboxed rerun was denied by the
  macOS Chromium Mach-port sandbox; the required unsandboxed rerun passed. The
  server was stopped afterward.
- `cargo test -p browser-sdk --locked` passed 27 tests,
  `cargo test -p browser-engine-worker --locked` passed 14 tests, and
  `cargo test -p axon-contract-proto --locked` passed 6 tests.
- The WASM-target worker check and `cargo fmt --check` passed. The worker check
  emitted only the existing `page_index` deprecation warning.
- The worker artifact report passed at 622,354 bytes against the unchanged
  750,000-byte budget. The browser dependency and bundle guardrail script
  passed when run sequentially after the artifact existed.
- `git diff --check origin/main...HEAD` passed, and the protobuf/generated
  contract path diff was empty.
- The final fetched `origin/main`, branch merge-base, and first branch parent all
  resolved to `f8e530c0d9422b8e970e397472147fbf965aa6ef`; exactly six branch
  commits remain above it.

The browser matrix first exposed two stale test probes rather than product
regressions: a raw-dispose query omitted the already-required browser-safe
limits, and an unsupported-partition probe expected a lazy schema error during
open. The probes now supply the required limits and issue a query against a
deterministic partition-only fixture, respectively. Both then passed in all
three engines. The initial guardrail invocation was parallel with artifact
creation and observed no artifact; its required sequential rerun passed.

### Live smoke inputs and skips

No live public-object result is claimed. The environment contained none of the
required inputs:

- GCS skipped because `AXON_LIVE_PUBLIC_GCS_TABLE_URI` was missing.
- S3 skipped because `AXON_LIVE_PUBLIC_S3_TABLE_URI` and
  `AXON_LIVE_PUBLIC_S3_REGION` were both missing.

The deterministic GCS and S3 selection/SDK coverage and the local Delta smoke
passed, but they do not substitute for those gated live checks.

### Final audit

The final `origin/main...HEAD` audit covered fallback paths, stale callbacks,
race handling, record/listener bounds, duplicate terminal delivery, logging,
unused code, and claim accuracy. It found and resolved the following before
handoff:

- Exact sample classification now requires the repository sample fixture
  identity; a non-fixture source with similar shape cannot acquire sample
  semantics.
- Replacing a selected resource clears that stale selection instead of
  auto-selecting a sole replacement table.
- History and metadata callbacks carry the execution context they were started
  for, so a late callback cannot toast or clean up a newer tab.
- Unused lifecycle wrapper APIs were removed rather than leaving parallel
  mutation paths.
- Unavailable-selection UI now states the reason and asks for an explicit table
  instead of presenting a misleading loading state.
- The two browser probes described above were aligned with the current lazy
  execution and required-limit contracts.
- Public worker error codes again match the Rust/protobuf contract; `deadline`
  remains a local lifecycle failure and is not a wire `QueryErrorCode`.
- Canonical admission identity includes the exact selected catalog ref, every
  source field used to reuse a query session, the catalog snapshot, and the
  requested snapshot pin. Pagination retains that same authoritative selection.
- Structural admission comparison replaces JSON fingerprints, so `NaN`,
  infinities, and other numeric inputs cannot collide. A mismatched expired retry
  is rejected before it can deadline-fail the original execution.
- `resetRun()` cannot return created, running, or cancel-requested work to idle.
  Duplicate same-terminal callbacks are retained as bounded invariant violations.
- Once SQL starts, local abort requests worker cancellation but does not confirm
  cancellation. Worker success or failure may win the race, and the lifecycle
  deadline releases adapter listeners if the worker never confirms.
- Each elapsed timer is cleared by its owning execution, so an older completion
  cannot stop a newer run's timer. The unused controller field was removed from
  active cancellation state.
- The dormant `editor-request-*` fallback generator was removed. Any editor
  worker command that omits its execution ID or execution-derived transport span
  now fails closed instead of manufacturing a second identifier.
- The persisted query-cache schema was advanced for the expanded canonical key;
  local capability-bearing source identities remain excluded from persistence.

No blocker or high-severity audit finding remains. Lifecycle records,
listeners, timers, cancellation handles, and invariant history are bounded;
capability-bearing inputs remain memory-only and are not logged or persisted.

### Exit-gate verdict

**Pass.** Exact-source tests and negative spies prove that missing, stale,
empty, or unqueryable selection cannot run a replacement table, inject sample
data, construct a session or lazy worker, open a table, or execute SQL. The
execution tests prove that one caller-created ID controls admission, worker
correlation, cancellation, deadline handling, progress/metrics, and exactly one
completed, failed, or cancelled terminal outcome. The worker path uses the
admitted single-buffer maximum and withholds oversized Arrow output, while the
generic SDK retains its chunked default.

This was the Slice 1 handoff verdict on 2026-07-16. The 2026-07-23 integration
addendum below supersedes its correction gate.

## Current-main and E3A correction integration addendum (2026-07-23)

The fetched `origin/main`, final merge base, and first rewritten parent are all
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f`
(`perf(ipc): stream browser query results through coordinator`). The E9 stack
was replayed from its historical `f8e530c` base and now has these six commit
identities:

1. `59df620` — `docs: plan e9 source lifecycle pi`
2. `d2fc39e` — `fix(web): require authoritative query selection`
3. `8424e6d` — `refactor(web): model one execution lifecycle`
4. `4519d42` — `fix(web): route cancellation through execution lifecycle`
5. `b78deb5` — `fix(web): enforce execution deadline and buffer bounds`
6. `0572b32` — `docs: document e9 slice one handoff`

The replay conflicted only in `sandbox-query-worker.ts` and
`browser-worker-matrix.spec.ts`. The worker resolution keeps `6cca364`'s
one-child, credit-controlled private stream and atomic `QueryStage` commit,
while retaining Slice 1's admitted byte maximum, execution-scoped cancellation,
absolute lifecycle deadline, and public single-buffer result. A private
`deadline_exceeded` remains a domain `failed(deadline)` outcome. The test
resolution keeps both the coordinator-stream coverage and Slice 1's
cancellation/output-limit assertions. The integration repair also makes child
crash injection browser-neutral and prevents Firefox from promoting a handled
child error into a second outer-worker error.

The fresh-review focused bundle passed 123 Vitest tests. The final full web
matrix passed 258 Vitest tests, 147 generic SDK Playwright tests, and 37 worker
tests with only 2 Firefox nested-worker routing probes skipped. The generic SDK
retains its hand-written chunked behavior; private stream chunks, credits,
stages, and child-worker topology are absent from protobuf and generated public
contracts.

The mandatory correction now follows the six commits as four implementation
commits plus this final documentation commit. It establishes canonical
resource identity, the four-arm read-resolution algebra, caller-created
execution admission and cancellation, three authoritative terminal outcomes,
and one byte-budgeted public Arrow buffer. No E9 Slice 2 provider interface or
runtime adoption was started. Once the final documentation commit is present
and the worktree/history gates are clean, E9 Slice 2 planning is unblocked.

### Fresh-review lifecycle hardening (2026-07-23)

The correction review found that a child blocked inside stream advancement
could outlive a cancellation, and that replacing a crashed child left the
editor's cached `tableOpened` state referring to the old child. The rewritten
fifth Slice 1 commit now caps the coordinator at 32 outstanding commands,
waits one bounded confirmation grace, discards staged bytes, settles pending
requests once, and recycles a nonresponsive child. Child replacement is marked
as an internal session invalidation, so the editor terminates the stale session
and the next execution reopens. Authoritative deadline cleanup also disposes
the session immediately. No accepted execution retries, no private frame enters
protobuf, and the domain terminal remains completed, failed, or cancelled.
