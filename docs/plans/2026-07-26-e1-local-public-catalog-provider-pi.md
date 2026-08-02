# E1 Local and Public CatalogProvider Adoption Implementation Plan

> **For Codex:** Use the executing-plans skill to implement this plan task by task.

**Goal:** Route local Delta and anonymous public GCS/S3 discovery through one generated-contract-backed `CatalogProvider` seam without changing access resolution or execution behavior.

**Architecture:** Add one discovery-only interface with source-specific immutable adapters. The adapters return generated catalog/common messages, reuse E9 canonical table identity, and remain below TanStack Query. Existing local handle acquisition, public descriptor resolution, runtime caches, and the sole validated SDK open remain outside this seam.

**Tech Stack:** TypeScript, React, TanStack Query, protobuf-es generated messages, Vitest, Playwright, Rust/WASM.

---

## Execution record and boundaries

- Execution date: 2026-07-26.
- Audited and fetched `origin/main`: `d1a31ec22479bb7d2fb380bfd61e00fd2f7881e8`.
- Branch: `feat/e1-local-public-catalog-provider-pi`.
- Worktree: `.worktrees/e1-local-public-catalog-provider-pi`.
- Root checkout at start: `3e5aceda0c1eb2c0dea983c0e5849200447a363f`, dirty and 24 commits behind `origin/main`.
- Preserved worktree: `.worktrees/live-connection-session-foundation` at `ba7c1eb5f6d0fc38ea8ae87cd95be477c48c43ba`, dirty.
- One sequential writer only. Do not delegate or edit files concurrently.
- Local commits only. Do not push, open or modify a PR, create a remote branch, deploy, publish, or modify `origin/main`.
- Do not modify protobufs, generated contract output, Rust source, dependencies, the root checkout, or any pre-existing worktree.
- Do not invoke `codegen:uc:check`.

### Phase 0 evidence

The mandatory fresh audit completed before worktree creation:

- `git fetch origin main` succeeded.
- `ee6a430afe99144c5e5780952b45a335d15e89c3` (E9 Slice 2) is an ancestor of `origin/main`.
- `a262401` (E3A correction) is an ancestor of `origin/main`.
- `0572b32` (E9 Slice 1) is an ancestor of `origin/main`.
- Post-E9 application/proto/generated paths have no overlap; later relevant-path changes are documentation-only POC/performance work.
- `gh pr list --state open --limit 100` returned no open PRs.
- The target branch and worktree were absent.
- No reachable branch defines `interface CatalogProvider`, `createLocalDeltaCatalogProvider`, or `createPublicObjectStorageCatalogProvider`.
- Catalog/provider-named historical branches contain routing, documentation, E9 execution, or old local-runtime work, not this E1 M0 discovery seam.
- `origin/main` contains exactly one app-layer `.openDeltaTable(...)`, at `apps/axon-web/src/services/query.ts:788`.
- `apps/axon-web/src/services/browser-provider-architecture.test.ts` guards that open behind executor-envelope validation.
- The generated catalog/common contracts already represent the required nodes, metadata presence, canonical resource, pagination termination, and safe structured error.

### Approved decisions

Cancellation and deadline failures use precise in-process kinds without a protobuf change:

| In-process kind     | Error name             | Generated code | Retryable                                         |
| ------------------- | ---------------------- | -------------- | ------------------------------------------------- |
| `cancelled`         | `AbortError`           | `UNSPECIFIED`  | No                                                |
| `deadline_exceeded` | `CatalogProviderError` | `UNSPECIFIED`  | No                                                |
| `invalid_request`   | `CatalogProviderError` | `INVALID`      | No                                                |
| `not_found`         | `CatalogProviderError` | `NOT_FOUND`    | No                                                |
| `unavailable`       | `CatalogProviderError` | `UNAVAILABLE`  | Yes, only within caller retry limits and deadline |

Synthetic hierarchy names are presentation-only:

| Source      | Catalog       | Schema                       | Table                            |
| ----------- | ------------- | ---------------------------- | -------------------------------- |
| Local Delta | `local-delta` | Existing runtime schema name | Existing runtime table name      |
| Public GCS  | `public-gcs`  | Existing `default` schema    | Existing root-derived table name |
| Public S3   | `public-s3`   | Existing `default` schema    | Existing root-derived table name |

These names never enter canonical table identity. Connected-catalog aliases remain user-facing presentation.

### Green bootstrap baseline

The isolated worktree was created at the audited SHA. These commands passed before the plan commit:

```bash
npm install --prefix apps/axon-web
cd apps/axon-web
npm run build:fixture
npm run build:wasm
npm exec -- vitest run \
  src/query/catalog.test.ts \
  src/query/keys.test.ts \
  src/query/persistence.test.ts \
  src/services/query-source.test.ts \
  src/services/browser-read-resolution.test.ts \
  src/services/browser-provider-architecture.test.ts \
  src/services/local-delta.test.ts \
  src/state/slices/connections.test.ts \
  src/editor/connect/store.test.ts \
  src/services/query.test.ts
```

Baseline result: 10 test files passed, 100 tests passed. The tracked worktree was clean.

## Required interface and invariants

Create `apps/axon-web/src/services/catalog-provider.ts` with:

```ts
export type CatalogDiscoveryContext = Readonly<{
  signal: AbortSignal;
  correlationId: string;
  deadlineEpochMs?: number;
}>;

export interface CatalogProvider {
  listCatalogs(
    page: PageRequest,
    context: CatalogDiscoveryContext,
  ): Promise<ListCatalogsResponse>;
  listSchemas(
    catalog: CatalogNode,
    page: PageRequest,
    context: CatalogDiscoveryContext,
  ): Promise<ListSchemasResponse>;
  listTables(
    schema: SchemaNode,
    page: PageRequest,
    context: CatalogDiscoveryContext,
  ): Promise<ListTablesResponse>;
  getTableMetadata(
    table: TableNode,
    context: CatalogDiscoveryContext,
  ): Promise<GetTableMetadataResponse>;
}

export type CatalogDiscoverySnapshot = Readonly<{
  catalog: CatalogNode;
  schema: SchemaNode;
  table: TableNode;
  metadata: TableMetadata;
}>;
```

`discoverFlatCatalog()` calls all four methods, requires exactly one catalog/schema/table, validates every parent, requires metadata-table equality, rejects any non-empty continuation cursor, and returns generated values.

Providers:

- Return plain promises and import neither React nor TanStack Query.
- Accept caller-owned cancellation, deadline, and correlation context.
- Reject an already-aborted signal before reading input.
- Reject a malformed, non-finite, or elapsed deadline before doing work.
- Recheck signal/deadline after asynchronous boundaries.
- Never retry, cache, execute, resolve access, build descriptors, inspect handles, or select fallback data.
- Return cloned generated messages so callers cannot mutate adapter state.
- Reject a non-empty M0 cursor.
- Treat page size zero as the one-item default and reject invalid numeric sizes.
- Create `PageInfo` without setting `nextCursor`.

The only allowed in-process error wrapper is:

```ts
export type CatalogProviderFailureKind =
  | "cancelled"
  | "deadline_exceeded"
  | "invalid_request"
  | "not_found"
  | "unavailable";

export class CatalogProviderError extends Error {
  readonly kind: CatalogProviderFailureKind;
  readonly detail: ProviderError;
  readonly retryable: boolean;
}
```

Every provider error carries a non-empty correlation ID and safe message. It must not expose handles, descriptors, signed URLs, query strings, credentials, grants, sessions, tokens, or raw unsafe input.

## Canonical identity

Create `apps/axon-web/src/services/canonical-table-identity.ts` as the only production implementation that constructs local/public `CanonicalResourceRef` and `TableNode` values.

Local identity:

- Existing encoded `axon-connection://local-delta/<registry ID>` connection ID.
- Namespace `axon.local-delta/v1`.
- `ResourceKind.TABLE`.
- `providerObjectId` containing only the opaque registry ID.
- Existing table name and `TableType.TABLE`.

Public identity:

- Existing `publicObjectStorageConnectionId(normalizedRoot)`.
- Namespace `axon.public-gcs/v1` or `axon.public-s3/v1`.
- `ResourceKind.TABLE`.
- `canonicalLocator` containing the normalized `gs://` or `s3://` table root.
- Existing table name and `TableType.TABLE`.

Reject empty IDs, unversioned namespaces, unsupported providers, unsafe or unnormalized roots, unspecified kinds, empty/wrong identity arms, and identity mismatches.

Aliases, display paths, handles, endpoints, descriptors, signed values, region labels beyond the existing connection canonicalizer, snapshots, metrics, and session/principal values never affect identity.

Keep `canonicalTableForSelection` exported from `browser-read-resolution.ts`; its local/public branches delegate to the shared builders.

## Generated metadata mapping

Local acquisition remains in `local-delta.ts`. It continues to own handles, permission/regrant, log parsing, WASM snapshot resolution, descriptor construction, persistence classification, and unsupported-feature validation. Convert normalized facts to a generated metadata seed:

- Parsed fields to `ColumnNode`.
- Partition names to `partitionColumns`.
- Optional snapshot, row, size, and file counts with explicit zero preserved.
- Reader/writer versions only when present.
- Protocol features to generated features.
- Logical local table URI to `storageLocation`.
- No handle, Blob URL, descriptor, permission, alias, or capability.

Public acquisition remains in `object-storage.ts`. It continues to own URI normalization, region handling, manifest/snapshot resolution, descriptor construction, range preflight, strong-ETag caching, expiry, and metrics. Move only the pure descriptor-to-generated-metadata mapping out of React:

- Normalized table root to `storageLocation`.
- Optional snapshot, row, size, and file counts with explicit zero preserved.
- Descriptor partition names to `partitionColumns`.
- Existing `r1/w2` compatibility presentation to explicit generated versions.
- Metrics remain a sibling runtime/view value, never generated metadata or identity.

Only GCS and S3 are supported. Reject ABFSS, R2, Unity Catalog, Delta Sharing, unknown providers, wrong schemes, userinfo, query/fragment values, credential markers, empty table paths, and malformed/escaping paths.

## Persistence and query identity

`ConnectedCatalog` remains the persisted connection/view model, not a second discovery owner. Store normalized generated metadata as:

```ts
catalogMetadataJson?: JsonValue;
```

Use protobuf `toJson(TableMetadataSchema, metadata)` and `fromJson(...)`. Validate canonical identity and safe storage location before write and after read. Preserve optional absence and explicit zero. Old records without this field remain readable through one explicitly named legacy compatibility projection that never derives identity from labels or falls back to sample.

Use this local/public query-key prefix:

```text
catalog/provider/<provider namespace>/connection/<connection ID>/authority/non-session
```

Table leaves add resource kind, identity arm/value, requested snapshot including `null`, and the resource leaf. Aliases, display labels, metrics, and descriptors never enter keys. Snapshot zero differs from latest. The sample fixture uses a fixed internal `fixture` namespace.

Disconnect/replacement ordering is:

1. Cancel the entire connection prefix.
2. Remove it.
3. Invalidate it.
4. Clear matching runtime presentation state.
5. Release/unregister runtime resources only after cancellation/removal begins.

Set `AXON_QUERY_CACHE_SCHEMA_VERSION = 3`. Local catalog entries remain non-persisted. Valid anonymous GCS/S3 and explicit fixture catalog entries remain persistable. Reject session-scoped/unknown providers, unsafe locators, malformed identities, unknown leaves, and all capability-bearing data.

## Task 1: Route local discovery through CatalogProvider

**Commit:** `refactor(web): route local discovery through CatalogProvider`

**Files:**

- Create: `apps/axon-web/src/services/canonical-table-identity.ts`
- Create: `apps/axon-web/src/services/catalog-provider.ts`
- Create: `apps/axon-web/src/services/catalog-provider.test.ts`
- Modify: `apps/axon-web/src/services/browser-read-resolution.ts`
- Modify: `apps/axon-web/src/services/browser-read-resolution.test.ts`
- Modify: `apps/axon-web/src/services/local-delta.ts`
- Modify: `apps/axon-web/src/services/local-delta.test.ts`
- Modify: `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- Modify: `apps/axon-web/src/editor/connect/types.ts`
- Modify: `apps/axon-web/src/editor/connect/store.ts`
- Modify: `apps/axon-web/src/editor/connect/store.test.ts`
- Modify: `apps/axon-web/src/state/slices/connections.test.ts`

### Red-green steps

1. Add tests importing the missing modules and asserting generated local hierarchy, pagination absence, zero/absence presence, cancellation, deadline, safe errors, parent validation, metadata facts, exact canonical identity, E9 equality, active provider use in connect/store, and no fallback.
2. Run the focused suite and record the expected missing-module or semantic failures.
3. Implement the minimal provider primitives, flat helper, local adapter, canonical builders, and local metadata seed.
4. Delegate E9 local identity construction to the canonical module without changing its public symbol.
5. In `ConnectModal`, cancel superseded/unmounted local discovery, call the adapter with a fresh correlation ID, and keep discovery separate from runtime access.
6. Carry `CatalogDiscoverySnapshot` through `ConnectResult` and one generated-to-view projection into `ConnectedCatalog`.
7. Persist validated generated metadata JSON while retaining safe compatibility fields.
8. Remove active local dependence on handwritten discovery production.
9. Run:

```bash
npm exec -- vitest run \
  src/services/catalog-provider.test.ts \
  src/services/local-delta.test.ts \
  src/services/browser-read-resolution.test.ts \
  src/editor/connect/store.test.ts \
  src/state/slices/connections.test.ts \
  src/services/browser-provider-architecture.test.ts \
  src/services/query.test.ts
```

10. Format only touched files, run `git diff --check`, inspect the staged diff, and commit.

Do not modify `services/query.ts`, `browser-execution-provider.ts`, generated files, protobufs, or Rust.

## Task 2: Route public GCS/S3 discovery through CatalogProvider

**Commit:** `refactor(web): route public discovery through CatalogProvider`

**Files:**

- Modify: `apps/axon-web/src/services/catalog-provider.ts`
- Modify: `apps/axon-web/src/services/catalog-provider.test.ts`
- Modify: `apps/axon-web/src/services/object-storage.ts`
- Modify: `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- Modify: `apps/axon-web/src/editor/connect/types.ts`
- Modify: `apps/axon-web/src/editor/connect/store.ts`
- Modify: `apps/axon-web/src/editor/connect/store.test.ts`
- Modify: `apps/axon-web/src/services/browser-read-resolution.test.ts`
- Modify: `apps/axon-web/src/services/query-source.test.ts`
- Create `apps/axon-web/src/services/object-storage.test.ts` only if public mapping cannot be covered without duplication.

### Red-green steps

1. Add tests for GCS/S3 hierarchy, identity, normalization, metadata presence, pagination, cancellation/deadline, unsafe input, unsupported providers, E9 parity, unchanged cache/metrics behavior, and no fallback.
2. Run the focused suite and record the expected unsupported-source and identity failures.
3. Add a public adapter factory to the existing provider module; do not add a registry.
4. Move the pure descriptor-to-metadata conversion to a service helper.
5. Use only the shared public canonical builder.
6. Preserve descriptor resolution, preflight, and runtime-cache registration order in `ConnectModal.runTest`, then call provider discovery.
7. Route the generated snapshot through the same generated-to-connected-catalog projection as local.
8. Remove React-owned public handwritten discovery production.
9. Run:

```bash
npm exec -- vitest run \
  src/services/catalog-provider.test.ts \
  src/services/object-storage.test.ts \
  src/services/browser-read-resolution.test.ts \
  src/services/query-source.test.ts \
  src/editor/connect/store.test.ts \
  src/state/slices/connections.test.ts \
  src/services/query.test.ts
```

Omit `object-storage.test.ts` only if it was intentionally not created and its assertions are present in `catalog-provider.test.ts`.

10. Format only touched files, run `git diff --check`, inspect the staged diff, and commit.

## Task 3: Consolidate catalog queries, persistence, invalidation, and guards

**Commit:** `refactor(web): consolidate catalog queries and identity`

**Files:**

- Create: `apps/axon-web/src/services/catalog.test.ts`
- Create: `apps/axon-web/src/services/catalog-provider-architecture.test.ts`
- Modify: `apps/axon-web/src/services/catalog.ts`
- Modify: `apps/axon-web/src/services/catalog-provider.ts`
- Modify: `apps/axon-web/src/services/catalog-provider.test.ts`
- Modify: `apps/axon-web/src/services/canonical-table-identity.ts`
- Modify: `apps/axon-web/src/services/browser-read-resolution.test.ts`
- Modify: `apps/axon-web/src/services/query-source.ts`
- Modify: `apps/axon-web/src/services/query-source.test.ts`
- Modify: `apps/axon-web/src/query/catalog.ts`
- Modify: `apps/axon-web/src/query/catalog.test.ts`
- Modify: `apps/axon-web/src/query/keys.ts`
- Modify: `apps/axon-web/src/query/keys.test.ts`
- Modify: `apps/axon-web/src/query/persistence.ts`
- Modify: `apps/axon-web/src/query/persistence.test.ts`
- Modify: `apps/axon-web/src/query/README.md`
- Modify: `apps/axon-web/src/editor/connect/data.ts`
- Modify: `apps/axon-web/src/editor/connect/types.ts`
- Modify: `apps/axon-web/src/editor/connect/store.ts`
- Modify: `apps/axon-web/src/editor/connect/store.test.ts`
- Modify: `apps/axon-web/src/state/slices/connections.test.ts`

### Red-green steps

1. Add tests for provider dispatch from `loadCatalog`, exact TanStack signal propagation, deadline/retry rules, canonical key grammar, snapshot zero, alias independence, connection-prefix cancellation/removal, late-completion races, auth purges, schema version 3, persistence allow/block rules, metadata JSON compatibility/tamper failure, unsupported providers, explicit fixture behavior, and unchanged execution.
2. Add the architecture guard before implementation and verify its intended failures.
3. Change `loadCatalog(source, context)` to route local/public discovery through a small source-kind switch and project generated discovery into the existing UI catalog.
4. Pass TanStack’s exact signal and a fresh correlation ID from `catalogQueryOptions`.
5. Implement full connection-prefix keying and cancellation/removal ordering.
6. Update cache persistence to schema version 3 and the strict new-key parser.
7. Keep explicit fixture handling fixed and isolated; fail closed for unavailable or unsupported sources.
8. Remove only local/public discovery and identity code made redundant by the adopted seam.
9. Run:

```bash
npm exec -- vitest run \
  src/services/catalog-provider.test.ts \
  src/services/catalog-provider-architecture.test.ts \
  src/services/catalog.test.ts \
  src/services/object-storage.test.ts \
  src/services/local-delta.test.ts \
  src/services/query-source.test.ts \
  src/services/browser-read-resolution.test.ts \
  src/services/browser-provider-architecture.test.ts \
  src/query/catalog.test.ts \
  src/query/keys.test.ts \
  src/query/persistence.test.ts \
  src/state/slices/connections.test.ts \
  src/editor/connect/store.test.ts \
  src/services/query.test.ts
npm exec -- tsc --noEmit
npm run lint
```

10. Format only touched files, run `git diff --check`, inspect the staged diff, and commit.

The new architecture guard must reject React/TanStack, access/execution, browser descriptor, session/credential/grant/worker, SDK-open, generic `Page<T>`, handwritten contract-mirror, UC/Delta Sharing/ABFSS/R2, codegen, and access-bearing persistence/logging dependencies below the discovery seam.

## Task 4: Verify and close documentation

**Commit:** `docs: close E1 local and public catalog adoption`

Run the complete source matrix before editing documentation:

```bash
cd apps/axon-web
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
cd ../..
cargo test -p browser-sdk --locked
cargo test -p axon-contract-proto --locked
bash tests/security/verify_browser_dependency_guardrails.sh
```

Before live suites, check only whether these names are set; never print values:

- `AXON_LIVE_PUBLIC_GCS_TABLE_URI`
- `AXON_LIVE_PUBLIC_S3_TABLE_URI`
- `AXON_LIVE_PUBLIC_S3_REGION`

Report live executions as live proof and absent-env skips only as readiness checks.

Verify zero contract drift:

```bash
git diff --exit-code origin/main -- \
  apps/axon-web/proto \
  apps/axon-web/src/generated/contracts \
  crates/contract-proto/src/generated
git diff --name-only origin/main...HEAD -- \
  apps/axon-web/proto \
  apps/axon-web/src/generated/contracts \
  crates/contract-proto/src/generated
```

Run the canonical identity and architecture searches described by this plan. Require generated `equals(TableNodeSchema, ...)` parity for local/GCS/S3 and deterministic protobuf-byte parity as a supplemental assertion.

Fetch `origin/main` again. If it moved, inspect ancestry and relevant path overlap. Stop rather than rebasing if overlap exists.

Only after the source branch is green, update:

- `docs/plans/2026-07-26-e1-local-public-catalog-provider-pi.md`
- `docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md`
- `docs/program/provider-model.md`
- `docs/program/rich-lakehouse-workbench-strategy.md`

Record exact commits, verification results, live/readiness classification, zero contract drift, unchanged E9 open boundary, and remaining E1 M1/M2, E6, and E9 Slice 3 prerequisites. Correct the older E1 plan narrowly: M0 is landed using generated responses; M1/M2 remain proposed; M0 does not use a handwritten generic page or UC codegen.

Format the four docs, inspect the staged diff, and commit. Then repeat the complete final matrix against the exact final `HEAD`.

## M0 closure evidence

M0 landed on `origin/main` as the exact five-commit range
`de8855f^..b0a7e1c` (inclusive):

1. `de8855f` — `docs(plan): define E1 local and public catalog adoption`
2. `eeaa11f` — `refactor(web): route local discovery through CatalogProvider`
3. `6e252e1` — `refactor(web): route public discovery through CatalogProvider`
4. `9e21b1a` — `refactor(web): consolidate catalog queries and identity`
5. `b0a7e1c` — `fix(web): harden catalog discovery lifecycle`

Local Delta and anonymous public GCS/S3 discovery now flow through the same
generated-contract-backed `CatalogProvider` shape and shared canonical identity
builders. E9 still owns access resolution and execution, and the sole
application-layer SDK table open remains in `services/query.ts:788`. The range
changes no protobuf, generated-contract, Rust-contract, Cargo manifest/lock,
web package manifest, or web lockfile.

The research verification recorded 15 focused files and 177 tests passing,
`tsc --noEmit` passing, and `codegen:check` passing. The clean M1 bootstrap on
2026-07-27 reran the current 15-file baseline with 176 tests passing after
building the fixture, application WASM, and worker WASM. The remote
`codegen:contracts:check` gate remains an environment-policy block because
`buf.build` was unreachable and an escalated descriptor-disclosing retry was
not authorized. The original security guard run stopped on the then-missing
prebuilt worker WASM artifact and is not reported green. The GCS URI, S3 URI,
and S3 region live variables were all unset, so public live browser suites were
readiness skips rather than live proof.

M0 added no Unity Catalog implementation, provider registry framework, data
access, descriptor resolution, or execution behavior. E1 M1 table-first
identity/Explorer work and M2 session-proxied Unity Catalog work remain
separate, as do E6 and E9 Slice 3.

## Final audit

The branch must contain these five local commits:

1. `docs(plan): define E1 local and public catalog adoption`
2. `refactor(web): route local discovery through CatalogProvider`
3. `refactor(web): route public discovery through CatalogProvider`
4. `refactor(web): consolidate catalog queries and identity`
5. `docs: close E1 local and public catalog adoption`

Before completion:

```bash
git status --short
git diff --check
git log --oneline --decorate origin/main..HEAD
git diff --stat origin/main...HEAD
git -C . rev-parse HEAD
git -C . status --short --branch
git -C .worktrees/live-connection-session-foundation rev-parse HEAD
git -C .worktrees/live-connection-session-foundation status --short --branch
```

Audit the full diff for duplicate canonicalizers, handwritten transport mirrors, unused adapters, UI/session/access/execution dependencies below discovery, extra SDK opens, capability-bearing persistence, alias/descriptor cache identity, fallback behavior, UC/Delta Sharing/ABFSS/R2 scope, generated/protobuf/Rust/dependency changes, unrelated cleanup, and root/worktree changes.

Do not hide necessary repairs with a history rewrite. Do not push or open a PR. Leave the isolated worktree intact and report exact deterministic, browser, live/readiness, inherited-failure, remote-drift, root-integrity, and publication evidence.
