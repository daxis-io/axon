# E9 Slice 2 — Local and Public Provider Contract Adoption

- Date: 2026-07-23
- Branch: `feat/e9-local-public-provider-adoption-pi`
- Worktree: `.worktrees/e9-local-public-provider-adoption-pi`
- Audited baseline:
  `62d4c465e10dc329221023eaaf2c67c542c408ce`
- Publication: local commits only; do not push or open a pull request
- Scope: adopt the corrected generated provider contracts for local Delta,
  public GCS, public S3, and the explicit sample fixture

## Preflight and prerequisite evidence

The mandatory preflight audited `origin/main` at
`62d4c465e10dc329221023eaaf2c67c542c408ce`. E9 Slice 1 is present through
`59df620`, `d2fc39e`, `8424e6d`, `4519d42`, `b78deb5`, and `0572b32`. The
corrected E3A contract stack is present through `1a57235`, `fe15caf`, `0569dce`,
`51873a3`, and `a262401`.

The clean-snapshot audit passed:

- `npm run codegen:check`;
- `cargo test -p axon-contract-proto --locked`; and
- 112 focused Vitest tests across the eight prerequisite files.

Before creating this worktree, `git fetch origin main` confirmed that
`origin/main` remained at the audited SHA. The requested branch and worktree
were absent, and `.worktrees/` was already ignored. In the new worktree,
`npm install`, `npm run build:fixture`, and `npm run build:wasm` completed. The
required five-file baseline passed 78 tests:

```text
npm exec -- vitest run \
  src/services/query-source.test.ts \
  src/services/execution-lifecycle.test.ts \
  src/services/query.test.ts \
  src/state/slices/run.test.ts \
  src/services/local-delta.test.ts
```

The root checkout was already dirty and behind `origin/main`; it is not the
implementation workspace and must remain untouched.

## Current and target call graphs

The current local path constructs and opens a descriptor inside the query
connector:

```text
exact selection
  -> runQuery
  -> getSession / buildSession
  -> loadLocalDeltaRuntime
  -> handwritten SDK descriptor
  -> openDeltaTable
  -> SQL
```

The current public path also resolves and opens directly:

```text
exact selection
  -> runQuery
  -> descriptor cache OR public list / snapshot / preflight resolution
  -> handwritten SDK descriptor
  -> openDeltaTable
  -> SQL
```

Slice 2 replaces both paths with one generated-contract boundary:

```text
exact selection
  -> generated TableNode plus CanonicalResourceRef
  -> profile-scoped DataAccessResolver
  -> closed ReadResolution
  -> generated browser-bound ExecuteRequest
  -> validate and admit
  -> open the supplied descriptor
  -> SQL
  -> generated ExecuteResponse events
  -> exactly one generated terminal frame
```

Resolution happens once before admission. Accepted browser execution neither
refreshes nor re-resolves access, and it never falls back to another source or
execution target.

## Provider seams

Introduce only these two app-layer provider modules:

```ts
interface DataAccessResolver {
  resolve(
    resource: CanonicalResourceRef,
    context: {
      executionId: string;
      deadline: Timestamp;
      snapshotVersion?: number;
      signal: AbortSignal;
    },
  ): Promise<ReadResolution>;
}

type BrowserExecuteInput = Readonly<{
  table: TableNode;
  request: ExecuteRequest;
}>;

interface ExecutionProvider {
  execute(input: BrowserExecuteInput): AsyncIterable<ExecuteResponse>;
  cancel(request: CancelRequest): CancelResponse;
}
```

Each resolver instance is scoped to the exact selected source. It captures that
source's runtime configuration, but the generated canonical resource supplied
to `resolve()` is the authority. Selection uses a direct source-kind switch;
this slice does not add a registry or universal provider abstraction.

`BrowserExecuteInput` contains only generated contract types.
`TableNode.name` supplies the existing SQL table alias. The table resource must
equal `ExecuteRequest.browserRead.resource`.

The application adopts generated `CanonicalResourceRef`, `TableNode`,
`ReadResolution`, `ResolvedBrowserRead`, descriptor, access, provenance,
query-option, admission, lifecycle, rejection, response, terminal,
cancellation, `QueryResponse`, `ResultPreview`, and single-buffer
`ArrowIpcResult` types. Existing snake-case SDK descriptor and result shapes are
confined to one compatibility adapter inside the executor boundary.

## Canonical profile mappings

| Profile         | Canonical mapping                                                                                                                                          | Access lifetime                                                           |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| Local Delta     | `connectionId = axon-connection://local-delta/<encoded registry id>`, namespace `axon.local-delta/v1`, kind `TABLE`, `providerObjectId = localRegistryId`  | `LOCAL_HANDLE`; required `notAfter` exactly equals the execution deadline |
| Public GCS      | Connection root derived from the normalized bucket, namespace `axon.public-gcs/v1`, kind `TABLE`, `canonicalLocator = normalized gs://bucket/prefix`       | `PUBLIC`; no expiry                                                       |
| Public S3       | Connection root derived from normalized region and bucket, namespace `axon.public-s3/v1`, kind `TABLE`, `canonicalLocator = normalized s3://bucket/prefix` | `PUBLIC`; no expiry                                                       |
| Explicit sample | Fixed sample connection, namespace `axon.sample-fixture/v1`, kind `TABLE`, locator `axon-fixture://sample-lake/prod_like/events`                           | Fixture-only `PUBLIC` envelope; never a fallback or product provider      |

Local expiry is an execution-scoped lease, not an intrinsic browser-permission
expiry. The resolver rechecks the retained handle or regrant state for every
new execution, keeps the runtime/session and Blob URLs when registry identity
and snapshot are unchanged, and mints a lease ending at the already-created
execution deadline. The executor validates the lease before admission and
again immediately before opening. Revocation may fail earlier.

Public descriptor-cache identity remains provider, normalized region,
canonical table locator, and requested snapshot. Reuse remains contingent on
path, size, and strong ETag validation. Every execution wraps a validated
descriptor in a fresh resolution envelope.

## Fail-closed validation

The executor rejects before opening for any of these conditions:

- missing or ambiguous canonical identity, descriptor, descriptor oneof,
  access class, correlation, or provenance;
- resource mismatch between exact selection, resolution, `TableNode`, and
  execution binding;
- expired or invalid local lifetime;
- public access carrying capability material;
- a non-snapshot descriptor in this Delta-only slice;
- a local descriptor containing anything except current registry-backed
  `blob:` URLs;
- a public descriptor containing credentials, signed parameters, non-HTTPS
  object URLs, objects outside the canonical root, or unstable identity when
  cache reuse is requested;
- a missing binding, malformed JSON carrying both bindings, or a
  logical-resource binding supplied to the browser executor; or
- an invalid execution ID, deadline, query, or browser budget.

`remote_required`, `denied`, and provider-error resolution outcomes remain
distinct and terminate before admission. They do not trigger sample selection,
browser-to-remote fallback, or another resolution attempt.

## Implementation and commit boundaries

1. `docs(plan): define E9 local and public provider adoption`
   - Record this audited plan before source edits.
2. `refactor(web): route local Delta through provider seams`
   - Add `browser-read-resolution.ts` and
     `browser-execution-provider.ts` with focused tests.
   - Update source mapping, local runtime resolution, generated lifecycle
     storage/equality, query execution, editor construction, and tests.
   - Extend `apps/axon-web/src/lib.rs` manifest resolution with an optional
     validated JavaScript-safe snapshot version so a requested snapshot is
     materialized into the bound descriptor.
3. `refactor(web): route public objects through provider seams`
   - Resolve GCS and S3 through the profile resolver while preserving
     descriptor-cache, preflight, metrics, opened-table, and range-cache
     behavior.
4. `refactor(web): close direct browser execution bypasses`
   - Route the explicit sample and pagination through the same validated
     executor.
   - Remove handwritten execution/admission/lifecycle mirrors where generated
     contracts now express the domain value.
   - Add a narrow architecture guard: app query, state, and editor code cannot
     call `openDeltaTable` or open a source/URL directly; only the internal
     executor runtime in `query.ts` may perform the SDK call after validation.
5. `docs: close E9 local and public provider adoption`
   - Update the provider model and rich-lakehouse strategy only after all
     implementation verification is green.

Blocker findings are fixed in the commit that owns the behavior. No protobuf or
tracked generated-contract file may change. If the snapshot bridge or either
profile cannot be implemented without a contract change, stop and report the
contract insufficiency.

## Test strategy

Use red-green-refactor within each implementation commit. Focused coverage
must prove:

- all canonical mappings and every `ReadResolution` arm;
- local permission revalidation, regrant, expiry, and retained runtime/Blob URL
  reuse;
- public expiry omission and capability-material rejection;
- exact supplied-descriptor opening and rejection before any SDK open;
- missing, ambiguous, logical, or mismatched bindings;
- exact generated-request replay, caller-created IDs, cancel-before-admit
  tombstones, cancellation/deadline races, first-terminal authority, bounded
  records/listeners, and one terminal frame;
- bounded generated Arrow output and result preview projection;
- requested snapshot materialization for local, GCS, and S3;
- pagination with fresh execution identity and stale-result protection;
- session, opened-table, descriptor, and range-cache reuse plus setup metrics;
- explicit sample execution through the same boundary; and
- stale/unavailable source behavior with no implicit sample or remote fallback.

Before each commit, format touched files, run its focused tests, run
`git diff --check`, inspect the staged diff, and prove no protobuf/generated
drift.

## Final verification

Run the complete web matrix:

```text
npm run codegen:contracts:check
npm run codegen:check
npm test
npm exec -- tsc --noEmit
npm run lint
npm run format:check
npm run test:sdk
npm run test:browser:editor-smoke
npm run test:browser:local-delta
npm run test:e2e
npm run test:browser:public-gcs-live
npm run test:browser:public-s3-live
```

Run the repository checks:

```text
cargo test -p axon-web-wasm --locked
cargo test -p wasm-delta-snapshot --locked
cargo test -p browser-sdk --locked
cargo test -p axon-contract-proto --locked
bash tests/security/verify_browser_dependency_guardrails.sh

git status --short
git diff --check
git log --oneline --decorate origin/main..HEAD
git diff --stat origin/main...HEAD
git diff --exit-code origin/main -- \
  apps/axon-web/proto \
  apps/axon-web/src/generated/contracts \
  crates/contract-proto/src/generated
```

The current environment has none of the three public-live variables. Both live
suites are still invoked, but a missing-variable skip is readiness evidence,
not live proof.

Finish with a fresh branch-versus-`origin/main` audit for unrelated changes,
dead interfaces, handwritten contract mirrors, descriptor-opening bypasses,
transient authority in persistence or cache keys, and accidental remote, Unity
Catalog, E8/filesystem, write, or UI scope.

## Non-goals

- No protobuf or generated-contract changes.
- No `CatalogProvider`, `FileSystemProvider`, or universal provider registry.
- No Unity Catalog, governed-read, signed-grant, or session-proxy adoption.
- No logical-resource/native executor or remote execution.
- No browser-to-remote fallback, retry of accepted work, or sample fallback.
- No E8/filesystem, write, settings, connection-flow, or new UI work.
- No persistence or logging of descriptors, capabilities, handles, signed
  values, execution envelopes, or cancellation authority.
- No replacement of the internal streaming coordinator or the generic SDK's
  separately scoped compatibility behavior.
