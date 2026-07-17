# E3A Pre-Adoption Contract Correction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the one intentional E3A compatibility window by replacing ambiguous resource, resolution, and execution wire shapes before E9 Slice 2 becomes their first app-layer consumer.

**Architecture:** Keep protobuf as message-only contract substrate and keep the existing `QueryEngine` declaration classified as browser-worker compatibility IPC. Introduce one canonical resource tuple, one closed data-access resolution algebra, and disjoint admission/nonterminal/terminal execution messages. Regenerate only the TypeScript packages and the already-proven Buffa common/data-access/exec outputs; do not adopt provider seams or change the live editor runtime in this PI.

**Tech Stack:** Buf `FILE` compatibility, protobuf edition proto3, `@bufbuild/protobuf` TypeScript output, Buffa Rust output, Vitest, Cargo host/WASM checks, and checked-in JSON canonicalization fixtures.

---

## Preflight and fixed boundaries

- Root checkout: dirty and divergent; read-only for this PI.
- Live `origin/main`: `f8e530c0d9422b8e970e397472147fbf965aa6ef`.
- Required predecessor: E9 Slice 1 branch `feat/e9-source-lifecycle-pi` at
  `d9a5ffb7edd914bfc46b13ffa1d090e991cc0cee`, exactly six commits above
  `origin/main` with a clean worktree.
- Branch: `chore/e3a-pre-adoption-correction-pi`.
- Worktree: `.worktrees/e3a-pre-adoption-correction-pi`.
- Rollback point: the branch parent above. No corrected contract has an adopted
  app-layer consumer, so reverting the four correction commits before E9 Slice
  2 is the bounded rollback.
- Publication: local only. Do not push or open a PR.

The fresh-worktree baseline generated the ignored web WASM prerequisite, then
passed 240 Vitest tests and all six `axon-contract-proto` host tests. A no-diff
`buf breaking` probe against
`../../.git#ref=origin/main,subdir=apps/axon-web` passed, proving the comparison
input works before the intentional breaks are introduced.

## Current-main integration addendum (2026-07-23)

The fetched `origin/main` is
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f`
(`perf(ipc): stream browser query results through coordinator`). It is the
direct successor of this stack's original `f8e530c` base. The required
`6cca364` ancestry check passes, no E9 Slice 1 or E3A correction commit is
already present on main, and the new main commit does not change protobuf or
generated contract files. The reviewed intentional Buf break set therefore
remains grounded in the same pre-adoption wire baseline.

At continuation, the branch still ends at `4feb709` and has the expected six
E9 commits plus the first two E3A correction commits. Task 3 remains an
uncommitted ten-file change: `dataaccess.proto`, the data-access and exec
codegen tests, generated TypeScript data-access output, five generated Buffa
data-access files, and `exec_smoke.rs`. `git diff --check` passes. No app-layer
provider adoption, E8 work, remote execution, or unrelated runtime change is
present in that dirty state.

`6cca364` introduces an internal Arrow IPC cursor, one child worker, and a
private coordinator protocol with fixed data/control credit windows. The child
may transfer credit-controlled schema, data, and end-of-stream fragments, while
the coordinator stages those fragments in `QueryStage`. A successful public
result is published only after a validated successful terminal commits the
whole stage; cancellation, deadline, worker failure, limit failure, and
protocol failure discard staged bytes. The protocol, chunk counters, credits,
stages, and child-worker topology remain private TypeScript/Rust implementation
details and must not enter protobuf or generated public contracts.

The replay must preserve these lifecycle and streaming invariants:

- One domain `execution_id` owns admission, progress, metrics, cancellation,
  the absolute deadline, and the visible terminal result. Open and transport
  request IDs may remain internal spans but cannot become alternate execution
  identities.
- The first domain terminal transition wins and remains exactly `completed`,
  `failed`, or `cancelled`. Private `deadline_exceeded` maps to domain
  `failed(deadline)` and cannot create a fourth state or a second terminal
  outcome.
- The admitted E9 absolute deadline controls the run. The private 120-second
  timer may remain an upper bound, but a shorter admitted deadline must cancel
  running SQL first and stale private timers or frames cannot affect a newer
  execution.
- Cancellation targets running SQL and waits for authoritative worker
  confirmation or lifecycle-deadline cleanup. Accepted work is never retried
  automatically.
- Staged private chunks become visible only after atomic success. Failure or
  cancellation discards them. The editor keeps bounded `single_buffer`
  delivery, while the existing generic SDK may retain its hand-written
  `chunked_buffers` delivery after the coordinator commits.
- Late callbacks, private frames, timers, and worker responses cannot mutate a
  newer execution, and all records, listeners, stages, timers, and byte buffers
  remain bounded.

The merge simulation against `6cca364` reports conflicts only in
`apps/axon-web/src/sandbox-query-worker.ts` and
`apps/axon-web/tests/browser-worker-matrix.spec.ts`. Resolution must retain the
coordinator/child staging path and its two landed matrix probe updates while
reapplying E9's structured resource-limit error, execution-derived worker
identity, cancellation semantics, and deadline behavior. Focused integration
proof is:

```text
npm run build:wasm
npm exec -- vitest run \
  src/services/execution-lifecycle.test.ts \
  src/services/query.test.ts \
  src/state/slices/run.test.ts \
  src/editor/App.test.ts \
  src/sandbox-query-stream-protocol.test.ts
npm exec -- playwright test --config=playwright.config.ts \
  tests/internal-arrow-ipc-stream.spec.ts \
  tests/browser-worker-matrix.spec.ts
npm exec -- tsc --noEmit
```

After Task 3 is committed and the worktree is clean, record the pre-rebase head
and create a unique local safety branch. Verify `f8e530c` is still the common
base, then run an interactive autosquash replay with
`--onto origin/main f8e530c`. This addendum's fixup folds into `ab9c506`.
Integration regressions are written red-first and committed only as fixups for
the E9 implementation commit that owns the behavior; they are autosquashed
before final verification. The finished history retains the six prescribed E9
subjects followed by exactly five E3A correction subjects. Nothing is pushed,
merged, or removed.

## Corrected contract shape

### Common and catalog

`axon.common.v1.CanonicalResourceRef` is the sole cross-seam resource identity:

```proto
enum ResourceKind {
  RESOURCE_KIND_UNSPECIFIED = 0;
  RESOURCE_KIND_TABLE = 1;
  RESOURCE_KIND_VOLUME = 2;
}

message CanonicalResourceRef {
  string connection_id = 1;
  string provider_namespace = 2;
  ResourceKind kind = 3;
  oneof identity {
    string provider_object_id = 4;
    string canonical_locator = 5;
  }
}
```

The namespace is versioned; adapters reject empty tuple members, unspecified
kinds, unset or empty identity arms, capabilities in locators, and namespace or
kind mismatches. Locator fixtures cover public GCS, public S3, and the explicit
sample fixture. Local Delta uses its opaque registry ID as `provider_object_id`.

`PageInfo` keeps only `optional string next_cursor = 1`. Remove `has_more`,
`ProviderCapabilities`, and `ProviderAuthority`. `ProviderError` remains the
typed operational-error value.

Catalog table and volume nodes use `CanonicalResourceRef` plus display names.
Remove function/model nodes and list responses. Remove `ColumnNode.partition`;
ordered `TableMetadata.partition_columns` is the only partition-membership
source. Retain volume references because E8 has a named future consumer, but do
not add an E8 runtime path.

### Data access

Replace string-keyed capability maps with:

```proto
message CapabilityReport {
  repeated CapabilityEntry capabilities = 1;
}

message CapabilityEntry {
  CapabilityKey key = 1;
  CapabilityState state = 2;
}
```

Move the existing typed `CapabilityKey` enum from exec into data access so
descriptors and execution gates share one key space. Boundary fixtures reject
unspecified keys/states and duplicate keys.

Move the directly openable descriptor oneof into data access as
`BrowserReadDescriptor`. Define `ResolvedBrowserRead` with exactly one
`CanonicalResourceRef`, descriptor, `BrowserAccessClass`, optional
`google.protobuf.Timestamp not_after`, correlation ID, and resolution
provenance. `not_after` is required and must be the earliest finite expiry for
local-handle, signed-URL, and session-proxy access; public access omits it.

Replace `ReadAccessPlan` and the nested/fallback/blocked variants with one
`ReadResolution` outcome:

```proto
message ReadResolution {
  oneof outcome {
    ResolvedBrowserRead browser_read = 1;
    RemoteRequired remote_required = 2;
    ReadDenied denied = 3;
    axon.common.v1.ProviderError error = 4;
  }
}
```

Each non-error outcome carries the same canonical resource directly. Remove
the brokered/delta-sharing/fallback plan hierarchy and its unused authority
enums. Keep the object-grant messages used by the separately gated filesystem
substrate. Replace audit `query_id`/`request_id` with one `execution_id`.

### Execution

Remove the duplicate exec capability key, fallback reasons/events, chunked
Arrow contract, and table URI/snapshot fields from `QueryRequest`. A query is
bound only by its request binding:

```proto
message ExecuteRequest {
  string execution_id = 1;
  oneof binding {
    axon.dataaccess.v1.ResolvedBrowserRead browser_read = 2;
    axon.common.v1.CanonicalResourceRef logical_resource = 3;
  }
  QueryRequest query = 4;
  google.protobuf.Timestamp deadline = 5;
}
```

Browser execution accepts `browser_read`; native or future remote execution
accepts `logical_resource` and resolves inside its trust boundary. Validation
rejects no binding, conflicting/wrong-target binding, empty IDs/SQL, expired
deadlines, and non-positive or browser-unsafe budgets. Runtime limits remain in
`QueryExecutionOptions` and preserve explicit presence.

Admission is a closed `ExecutionAdmission` with accepted or rejected arms.
Accepted records expose the authoritative state and whether this identical
admission should launch; rejection distinguishes invalid input, expired
deadline, cancel-before-admit, and mismatched ID reuse. Nonterminal
`BrowserWorkerEventEnvelope` contains progress/log/metrics/cache only.

`ExecutionTerminalState` has exactly one completed, failed, or cancelled arm.
`ExecutionTerminalFrame` is the at-most-once delivery wrapper, so authoritative
state and transport delivery are not conflated. `ExecuteResponse` carries one
admission, nonterminal event, or terminal frame. The contract comments require
admission first, no event after terminal, and no execution event stream for a
rejected admission.

Cancellation is keyed only by `execution_id`. `CancelResponse` returns the
recorded lifecycle state, making cancel-before-admit tombstones, repeated
cancel, post-terminal cancel, and response-loss retries idempotent. Worker
transport commands retain internal `request_id` spans but SQL/event/cancel
correlation uses `execution_id`.

The browser result is one opaque byte-budgeted Arrow IPC buffer. Remove
chunk-delivery enums, chunk events, delivery metadata, and chunk metrics from
the corrected contract. The unrelated hand-written generic SDK may retain its
current chunk support; this PI does not adopt or change that runtime boundary.

## Task 1: Commit the PI plan

**Files:**

- Create: `docs/plans/2026-07-16-e3a-pre-adoption-contract-correction-pi.md`

**Step 1: Verify the document is the only tracked change**

Run: `git status --short`

Expected: only this plan document.

**Step 2: Format and validate the document**

Run from `apps/axon-web`: `npm exec -- prettier --check ../../docs/plans/2026-07-16-e3a-pre-adoption-contract-correction-pi.md`

Expected: PASS.

**Step 3: Commit**

```bash
git add docs/plans/2026-07-16-e3a-pre-adoption-contract-correction-pi.md
git commit -m "docs: plan e3a pre-adoption contract correction"
```

## Task 2: Canonical resource identity and minimal catalog surface

**Files:**

- Modify: `apps/axon-web/proto/axon/common/v1/common.proto`
- Modify: `apps/axon-web/proto/axon/catalog/v1/catalog.proto`
- Create: `apps/axon-web/src/generated/contracts/fixtures/canonical-resource-locators.json`
- Modify: `apps/axon-web/src/generated/contracts/contracts-codegen.test.ts`
- Modify: `crates/contract-proto/src/lib.rs`
- Regenerate: `apps/axon-web/src/generated/contracts/protobuf/axon/common/v1/common_pb.ts`
- Regenerate: `apps/axon-web/src/generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts`
- Regenerate: `crates/contract-proto/src/generated/axon.common.v1.*`

**Step 1: Write failing identity/catalog tests**

Add reflection and round-trip tests proving the exact canonical tuple and
identity oneof, optional cursor presence, absent `has_more`, absent provider
authority/capabilities declarations, canonical locator fixtures, table/volume
resource fields, no function/model declarations, and no column partition flag.

**Step 2: Verify RED**

Run: `npm exec -- vitest run src/generated/contracts/contracts-codegen.test.ts`

Expected: FAIL because `CanonicalResourceRef`, `ResourceKind`, and corrected
catalog fields do not exist.

**Step 3: Implement the minimal proto changes and regenerate**

Keep `ObjectRef` temporarily only while the old data-access and exec messages
still compile; remove it in Task 4. Run:

```bash
npm run codegen:contracts
npm run codegen:contracts:rust
```

**Step 4: Verify GREEN**

```bash
npm exec -- vitest run src/generated/contracts/contracts-codegen.test.ts
npm run codegen:contracts:check
npm run codegen:contracts:rust:check
cargo test -p axon-contract-proto --locked
```

**Step 5: Commit**

```bash
git add apps/axon-web/proto/axon/common/v1/common.proto apps/axon-web/proto/axon/catalog/v1/catalog.proto apps/axon-web/src/generated/contracts crates/contract-proto
git commit -m "fix(contract): define canonical resource identity"
```

## Task 3: One closed data-access resolution algebra

**Files:**

- Modify: `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
- Modify: `apps/axon-web/src/generated/contracts/dataaccess-codegen.test.ts`
- Modify: `apps/axon-web/src/generated/contracts/exec-codegen.test.ts`
- Modify: `crates/contract-proto/tests/exec_smoke.rs`
- Regenerate: `apps/axon-web/src/generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts`
- Regenerate: `apps/axon-web/src/generated/contracts/protobuf/axon/exec/v1/exec_pb.ts`
- Regenerate: `crates/contract-proto/src/generated/axon.dataaccess.v1.*`
- Regenerate: `crates/contract-proto/src/generated/axon.exec.v1.*`

**Step 1: Write failing resolution tests**

Test all four resolution arms, exact self-identifying browser binding fields,
descriptor oneof, access classes and conditional expiry examples, typed
capability entries, duplicate-key rejection fixtures, provenance/correlation,
and audit `execution_id`. Assert the old nested plan hierarchy is absent after
the final Task 4 cleanup.

**Step 2: Verify RED**

Run: `npm exec -- vitest run src/generated/contracts/dataaccess-codegen.test.ts`

Expected: FAIL because the closed resolution and binding messages do not exist.

**Step 3: Add the corrected messages and regenerate**

Introduce the new algebra alongside the old exec-dependent compatibility
messages so this commit stays green. Update existing descriptor and capability
JSON normalizers to the typed repeated entry shape.

**Step 4: Verify GREEN**

```bash
npm exec -- vitest run src/generated/contracts/dataaccess-codegen.test.ts src/generated/contracts/exec-codegen.test.ts
npm run codegen:contracts:check
npm run codegen:contracts:rust:check
cargo test -p axon-contract-proto --locked
```

**Step 5: Commit**

```bash
git add apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto apps/axon-web/src/generated/contracts crates/contract-proto
git commit -m "fix(contract): close data access resolution"
```

## Task 4: Align the execution compatibility surface with Slice 1

**Files:**

- Modify: `apps/axon-web/proto/axon/common/v1/common.proto`
- Modify: `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
- Modify: `apps/axon-web/proto/axon/exec/v1/exec.proto`
- Modify: `apps/axon-web/src/generated/contracts/contracts-codegen.test.ts`
- Modify: `apps/axon-web/src/generated/contracts/dataaccess-codegen.test.ts`
- Modify: `apps/axon-web/src/generated/contracts/exec-codegen.test.ts`
- Modify: `crates/contract-proto/src/lib.rs`
- Modify: `crates/contract-proto/tests/exec_smoke.rs`
- Regenerate: all common/data-access/exec TypeScript and Buffa outputs

**Step 1: Write failing lifecycle wire tests**

Assert exact execute binding arms, caller-created IDs, absolute deadline,
positive budget presence, accepted/rejected admission, cancel keyed by execution
ID, the seven authoritative lifecycle states, three terminal outcomes,
admission/event/terminal stream arms, nonterminal-only worker events, and one
single-buffer Arrow result. Add negative reflection assertions for old table
URI/snapshot binding, fallback, query/request audit IDs, chunk delivery/events,
provider authority, `ObjectRef`, and old resolution-plan declarations.

**Step 2: Verify RED**

Run:

```bash
npm exec -- vitest run src/generated/contracts/contracts-codegen.test.ts src/generated/contracts/dataaccess-codegen.test.ts src/generated/contracts/exec-codegen.test.ts
cargo test -p axon-contract-proto --locked
```

Expected: FAIL on the old execute request, cancellation, fallback, chunk, and
terminal shapes.

**Step 3: Implement the corrected exec surface and remove temporary legacy messages**

Regenerate TypeScript and Buffa output after the proto compiles. Update Rust
exports and smoke construction to the canonical ref, resolved binding,
admission, cancellation, terminal-state, timestamp, and single-buffer shapes.

**Step 4: Verify GREEN and capture the intentional break report**

```bash
buf lint
buf format --diff --exit-code
buf breaking . --against '../../.git#ref=origin/main,subdir=apps/axon-web' --error-format=json
npm run codegen:contracts:check
npm run codegen:contracts:rust:check
npm exec -- vitest run src/generated/contracts
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
```

`buf breaking` must fail only with the reviewed pre-adoption changes named in
this plan. Save its structured findings for the final handoff; any unrelated
package, filesystem, field-type, or service-cardinality break is a blocker.

**Step 5: Commit**

```bash
git add apps/axon-web/proto apps/axon-web/src/generated/contracts crates/contract-proto
git commit -m "fix(contract): align execution lifecycle wire shape"
```

## Task 5: Establish the corrected baseline and hand off to E9 Slice 2

**Files:**

- Modify: `docs/plans/2026-07-16-e3a-pre-adoption-contract-correction-pi.md`
- Modify only if needed for truthful architecture state:
  `docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md`
- Modify only if needed for truthful gate state:
  `docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md`

**Step 1: Audit the final diff**

Review `d9a5ffb...HEAD` for speculative remote/E8 messages, duplicate identity
or terminal states, capability leakage, string-keyed capability maps, hidden
fallback, chunk-without-credit claims, unbounded values, unused generated Rust,
and any runtime/provider adoption.

**Step 2: Run the complete verification matrix**

From `apps/axon-web`:

```bash
buf lint
buf format --diff --exit-code
npm run codegen:config:check
npm run codegen:contracts:check
npm run codegen:contracts:rust:check
npm run codegen:check
npm exec -- vitest run src/generated/contracts
npm test
npm run test:sdk
npm exec -- playwright test --config=playwright.config.ts
npm exec -- tsc --noEmit
npm run lint
npm run format:check
```

From the worktree root:

```bash
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
cargo check -p browser-engine-worker --target wasm32-unknown-unknown --locked
cargo fmt --check
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check d9a5ffb...HEAD
git status --short
git log --oneline --reverse d9a5ffb..HEAD
```

The worker/runtime checks are regression guards: this message-only correction
must not change the 750 KB worker budget or the existing generic SDK behavior.
No browser live-cloud claim is required because no runtime path changes.

**Step 3: Record the accepted breaks and corrected-baseline rule**

Record every structured Buf finding by package/file/rule, explain why it is one
of the approved pre-adoption corrections, and state that future changes resume
Buf `FILE` compatibility from the correction commit. Record final SHAs and exact
verification counts. E9 Slice 2 may start only if every gate is green.

**Step 4: Commit**

```bash
git add docs/plans/2026-07-16-e3a-pre-adoption-contract-correction-pi.md docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md
git commit -m "docs: document e3a correction handoff"
```

Final history must contain exactly these five correction commits above the six
E9 Slice 1 commits. The correction branch remains local and clean. A passing
handoff unblocks planning E9 Slice 2; it does not implement Slice 2.

## Non-goals

- No E9 Slice 2 resolver/executor interfaces or app-layer generated-message adoption.
- No changes to selected-source behavior or the Slice 1 memory-only lifecycle.
- No `axon/fs/v1` schema, generated output, E8 UI, adapter, or runtime change.
- No catalog functions/models, Cedar decision package, Connect/Tauri transport,
  remote executor/service, service stubs, auth metadata, or credential fields.
- No protobuf Arrow rows, automatic retry after acceptance, or creditless
  chunked Arrow contract.
- No catalog Rust generation; it still has no Rust consumer.
- No push, PR, merge, or root-checkout edit.

## Exit verdict

The mandatory correction is complete only when the canonical identity is the
sole cross-seam identity, resolution/admission/terminal algebras are disjoint,
capability lifetime and execution budgets are explicit, browser execution can
carry only one byte-budgeted buffer, every intentional Buf break is recorded,
generated TS/Rust drift checks pass, and the five-commit correction stack is
clean. Only then is E9 Slice 2 unblocked.
