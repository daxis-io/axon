# E1 Table-First Explorer Completion Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish E1 M1 by making stable generated connection and table identity
authoritative in persistence, routes, Catalog Explorer detail, and the logical
handoff to the existing SQL editor.

**Architecture:** Keep `CatalogProvider` discovery separate from E9 access
resolution and execution. Persist and route only validated generated logical
identity, resolve one exact current discovery location for presentation, and
pass the selected `TableNode` directly to the editor. Retain the old E0
display-coordinate route only as an exact-match compatibility consumer.

**Tech Stack:** React, TypeScript, TanStack Router, TanStack Query, Zustand,
Buf-generated protobuf-es messages, Vitest, Playwright, Rust/Cargo, CSS design
tokens.

---

## Baseline and integrity gate

- Fetched `origin/main` is exactly
  `b0a7e1c05fe4f5ace64aa095d11eeb1a4bf68ba0`.
- `b0a7e1c` is the landed M0 head and an ancestor of `origin/main`; there are no
  intervening commits.
- The exact M0 range is `de8855f^..b0a7e1c`:
  1. `de8855f` — `docs(plan): define E1 local and public catalog adoption`
  2. `eeaa11f` — `refactor(web): route local discovery through CatalogProvider`
  3. `6e252e1` — `refactor(web): route public discovery through CatalogProvider`
  4. `9e21b1a` — `refactor(web): consolidate catalog queries and identity`
  5. `b0a7e1c` — `fix(web): harden catalog discovery lifecycle`
- M0 changes no protobuf, generated-contract, Rust-contract, Cargo
  manifest/lock, or web dependency files.
- The sole application-layer SDK table open remains
  `apps/axon-web/src/services/query.ts:788`.
- The isolated branch is `feat/e1-table-first-explorer-pi` at
  `/Users/ethanurbanski/axon/.worktrees/e1-table-first-explorer-pi`.
- Bootstrap completed with `npm ci`, `npm run build:fixture`,
  `npm run build:wasm`, and the release worker-WASM build.
- The current 15-file inherited baseline passes 176 tests. This is baseline
  evidence, not M1 completion evidence.
- The root checkout remains intentionally dirty at
  `3e5aceda0c1eb2c0dea983c0e5849200447a363f`; it is evidence only and must not
  be modified.

Stop before implementation if M0 ceases to be an ancestor, relevant remote
overlap appears, the target branch/worktree becomes occupied, or the inherited
focused baseline fails.

## Requirement matrix and cut list

| M1 requirement             | M0 state                                                                                                                                    | Minimal M1 completion                                                                                                                                                                     |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stable connection identity | `CanonicalResourceRef.connectionId` is stable, but `ConnectedCatalog.id` is alias-derived and alias collisions merge sources                | Use validated generated connection ID as `ConnectedCatalog.id`; keep `alias` presentation-only; migrate legacy alias-grouped records by generated connection and canonical table identity |
| Route owns selection       | Encoded reload/history works, but routes carry alias-derived catalog ID and display names; the editor initially reads prior store selection | Route by connection ID and canonical resource; resolve exactly one current location; pass route selection into the editor on first render before mirroring it into the store              |
| Table/view Explorer        | Existing tree lists catalogs, schemas, and tables but discards `TableType` and has no detail surface                                        | Preserve the Catalogs page and add generated overview, semantic columns, table/view labels, and explicit loading/empty/error states                                                       |
| Invalid/stale states       | Query-source selection fails closed, but the route has one generic unavailable state and malformed persistence may restore sample           | Distinguish malformed, disconnected, stale, ambiguous legacy, non-queryable, loading, error, and empty; never substitute sample for present malformed data                                |
| Logical editor handoff     | Table clicks write a name-based store reference and E9 reconstructs the generated table later                                               | Make active selection the generated `TableNode`; navigate through an explicit canonical `/sql` route and hand only that logical table to E9                                               |

Retain without redesign:

- E0 router, `/catalogs`, sidebar, connected panel, encoded paths, reload and
  history behavior, and native-button keyboard semantics.
- E9 closed selection algebra, explicit sample profile, validated resolver and
  executor path, and one app-layer SDK open.
- M0 local/public providers, generated metadata, shared canonical identity,
  cancellation/deadline handling, bounded retry, connection-prefix query keys,
  persistence allowlist, cache invalidation, and late-completion protection.
- Existing design tokens and editor layout.

Do not add a provider registry, preview, descriptor resolution, access
resolution, execution work, protobufs, generated output, dependencies, Unity
Catalog, E6, E8, or E9 Slice 3 behavior.

## Interfaces and invariants

1. `ConnectedCatalog.id` is the validated generated
   `CatalogNode.connectionId`. Add `catalogName` from `CatalogNode.name`.
   `alias` is mutable presentation only and never participates in routing,
   equality, query keys, or deduplication.
2. The active catalog selection is a generated `TableNode`, not
   `{catalogId, schemaName, tableName}`.
3. Exact `CanonicalResourceRef` equality selects a table. Catalog, schema, and
   display table names are recovered from the one matching discovery location.
4. Move local/public source-to-`TableNode` projection into
   `apps/axon-web/src/services/canonical-table-identity.ts`; query source,
   Explorer routing, and E9 use that implementation.
5. Canonical Explorer routes have this shape:

   ```text
   /catalog/$connectionId/table/$providerNamespace/$identityArm/$identityValue
   ```

   `$identityArm` is exactly `provider-object-id` or `canonical-locator`.
   Resource kind is fixed to table.

6. The canonical editor route appends `/sql`. “Open in SQL editor” performs
   only navigation and logical selection.
7. The legacy E0 three-segment route remains a compatibility consumer. It
   replace-navigates to canonical `/sql` only when display coordinates resolve
   to exactly one current table. Missing or ambiguous links remain unavailable.
   No M1 code generates legacy URLs.
8. Display renames preserve canonical routes. Identity-arm value changes are a
   different logical table.
9. Persistence keeps the existing storage key and a backward-readable
   superset. Split alias-merged records by generated connection ID and merge
   only equal canonical tables.
10. A present malformed record produces unavailable/empty state, never sample.
    Only a genuinely missing first-run record may use the explicit fixture.
11. Local handles, descriptors, signed URLs, grants, resolved access, metrics,
    sessions, and selected capabilities remain non-persisted.
12. Query-key shape and cache schema stay unchanged.
13. Disconnect/replacement cleanup awaits prefix cancellation, removes and
    invalidates the prefix, clears matching presentation state, then discards
    the query session and unregisters local runtime resources.
14. Explorer/routing imports may use generated catalog/common contracts and
    query-source selection. They may not import data-access/exec contracts,
    descriptors, worker/session modules, `services/query.ts`, or
    `openDeltaTable`.

## Task 1: Stabilize connection identity and persistence

**Files:**

- Modify: `apps/axon-web/src/editor/connect/types.ts`
- Modify: `apps/axon-web/src/editor/connect/store.ts`
- Test: `apps/axon-web/src/editor/connect/store.test.ts`
- Modify: `apps/axon-web/src/persistence/key-value.ts`
- Test: `apps/axon-web/src/persistence/key-value.test.ts`
- Modify: `apps/axon-web/src/services/canonical-table-identity.ts`
- Modify: `apps/axon-web/src/services/query-source.ts`
- Test: `apps/axon-web/src/services/query-source.test.ts`
- Modify: `apps/axon-web/src/state/slices/connections.ts`
- Test: `apps/axon-web/src/state/slices/connections.test.ts`

### Red-green sequence

1. Add one failing test at a time for alias-independent identity, same-alias
   connection separation, same-name locator separation, legacy mixed-record
   splitting, reconnect metadata update, exact selected-resource clearing,
   malformed persistence, and blocked runtime values.
2. Run the smallest affected file and confirm each failure names the missing
   semantic behavior.
3. Implement only enough production behavior to pass that test.
4. Rerun the smallest file, then the task suite:

   ```bash
   npm exec -- vitest run \
     src/editor/connect/store.test.ts \
     src/persistence/key-value.test.ts \
     src/services/query-source.test.ts \
     src/state/slices/connections.test.ts
   ```

5. Format only touched files, run `git diff --check`, inspect the diff, and
   commit:

   ```text
   refactor(web): stabilize catalog connection identity
   ```

## Task 2: Make canonical routes and generated selection authoritative

**Files:**

- Modify: `apps/axon-web/src/editor/catalog-navigation.ts`
- Test: `apps/axon-web/src/editor/catalog-navigation.test.ts`
- Modify: `apps/axon-web/src/editor/router.tsx`
- Test: `apps/axon-web/src/editor/router.test.ts`
- Modify: `apps/axon-web/src/editor/App.tsx`
- Modify: `apps/axon-web/src/editor/components/Sidebar.tsx`
- Modify: `apps/axon-web/src/editor/connect/ConnectedCatalogs.tsx`
- Modify: `apps/axon-web/src/services/query-source.ts`
- Test: `apps/axon-web/src/services/query-source.test.ts`
- Modify: `apps/axon-web/src/services/browser-read-resolution.ts`
- Test: `apps/axon-web/src/services/browser-read-resolution.test.ts`
- Modify: `apps/axon-web/src/state/slices/connections.ts`
- Test: `apps/axon-web/src/state/slices/connections.test.ts`
- Modify: `apps/axon-web/src/query/keys.ts`
- Test: `apps/axon-web/src/query/keys.test.ts`

### Red-green sequence

1. Add failing pure route tests for provider-object-ID and canonical-locator
   round trips, percent encoding, identity-arm mismatch, exact selection under
   display collisions, disconnected/stale/non-queryable outcomes, and unique
   versus ambiguous legacy routes.
2. Add failing component/state tests proving the route selection is used on
   the editor’s first render and back/forward changes exact generated tables.
3. Add failing E9 parity tests for local Delta, GCS, and S3.
4. Implement canonical parsing/formatting and exact discovery lookup without
   resolving access or executing.
5. Run:

   ```bash
   npm exec -- vitest run \
     src/editor/catalog-navigation.test.ts \
     src/editor/router.test.ts \
     src/services/query-source.test.ts \
     src/services/browser-read-resolution.test.ts \
     src/state/slices/connections.test.ts \
     src/query/keys.test.ts
   ```

6. Format touched files, run `git diff --check`, inspect the diff, and commit:

   ```text
   refactor(web): make catalog routes resource authoritative
   ```

## Task 3: Complete the existing Catalog Explorer

**Files:**

- Modify: `apps/axon-web/src/editor/CatalogsPage.tsx`
- Modify: `apps/axon-web/src/editor/catalog-navigation.ts`
- Test: `apps/axon-web/src/editor/catalog-navigation.test.ts`
- Modify: `apps/axon-web/src/editor/components/Sidebar.tsx`
- Modify: `apps/axon-web/src/editor/connect/ConnectedCatalogs.tsx`
- Modify: `apps/axon-web/src/editor/styles/design-tokens.css`
- Create: `apps/axon-web/src/editor/catalog-explorer-architecture.test.ts`
- Test: `apps/axon-web/tests/editor-smoke.spec.ts`

### Required behavior

- Render connection → catalog → schema → table/view using the existing page and
  design system.
- For the exact route-selected table, show table kind, comment, storage
  location, snapshot, rows, files, size, protocol/features, partitions, and a
  semantic columns table with nullability, comments, and partition status.
- Distinguish no connections, no selected table, loading, metadata unavailable,
  stale/disconnected selection, no columns, and non-queryable table.
- Keep unsupported views browseable while disabling editor handoff.
- Show “Open in SQL editor” only for an exact queryable `TableNode`; navigate to
  canonical `/sql` without preview, descriptor creation, access resolution, SDK
  open, or execution.
- Remove active `Resync` and `Edit session` stubs and copy that falsely implies
  brokered ownership for local/public sources.
- Use native headings, lists, buttons, and table semantics with accessible
  names, `aria-current`/`aria-disabled`, deterministic keyboard activation, and
  focus-visible styling.

### Red-green sequence

1. Add pure Explorer-model failures before rendering changes.
2. Implement the smallest model/state presentation for each case.
3. Add targeted Playwright failures for overview/columns, view distinction,
   unavailable states, reload, disconnect, back/forward, and logical editor
   handoff.
4. Assert editor navigation does not issue a table-open or query request before
   the user runs SQL.
5. Run focused model tests and:

   ```bash
   npm run test:browser:editor-smoke
   ```

6. Format touched files, run the architecture guard and `git diff --check`,
   inspect the diff, and commit:

   ```text
   feat(web): complete table-first catalog explorer
   ```

## Task 4: Centralize connection cleanup

**Files:**

- Create: `apps/axon-web/src/editor/connect/connection-lifecycle.ts`
- Create: `apps/axon-web/src/editor/connect/connection-lifecycle.test.ts`
- Modify: `apps/axon-web/src/editor/App.tsx`
- Test: `apps/axon-web/src/editor/App.test.ts`
- Modify: `apps/axon-web/src/editor/ConnectPage.tsx`
- Replace: `apps/axon-web/src/editor/ConnectPage.test.ts`
- Modify: `apps/axon-web/src/query/catalog.ts`
- Test: `apps/axon-web/src/query/catalog.test.ts`

### Red-green sequence

1. Add behavior failures proving exact ordering:
   - cancel the complete canonical connection prefix;
   - remove and invalidate matching queries;
   - reject late repopulation;
   - clear matching presentation state;
   - only then dispose the active query session and unregister local runtime;
   - leave other connections, local metadata queries, and explicit sample
     state untouched.
2. Run the lifecycle/query/App tests and confirm semantic failures.
3. Implement one shared cleanup function and replace both handlers.
4. Delete the source-text ordering assertion; retain behavior tests.
5. Run:

   ```bash
   npm exec -- vitest run \
     src/editor/connect/connection-lifecycle.test.ts \
     src/editor/App.test.ts \
     src/editor/ConnectPage.test.ts \
     src/query/catalog.test.ts
   ```

6. Format touched files, run `git diff --check`, inspect the diff, and commit:

   ```text
   refactor(web): centralize catalog connection cleanup
   ```

## Task 5: Verify, review, repair, and close M1

Run the focused suite:

```bash
npm exec -- vitest run \
  src/editor/catalog-navigation.test.ts \
  src/editor/router.test.ts \
  src/editor/connect/store.test.ts \
  src/editor/connect/connection-lifecycle.test.ts \
  src/state/slices/connections.test.ts \
  src/services/query-source.test.ts \
  src/services/catalog-provider.test.ts \
  src/services/catalog-provider-architecture.test.ts \
  src/services/catalog.test.ts \
  src/services/local-delta.test.ts \
  src/services/object-storage.test.ts \
  src/services/browser-read-resolution.test.ts \
  src/query/catalog.test.ts \
  src/query/keys.test.ts \
  src/query/persistence.test.ts
```

Run the complete web gate from `apps/axon-web`:

```bash
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
```

Run repository gates:

```bash
cargo test -p browser-sdk --locked
cargo test -p axon-contract-proto --locked
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check
```

Print only set/unset status for:

- `AXON_LIVE_PUBLIC_GCS_TABLE_URI`
- `AXON_LIVE_PUBLIC_S3_TABLE_URI`
- `AXON_LIVE_PUBLIC_S3_REGION`

Run the public GCS live suite only with the GCS URI set. Run the public S3 live
suite only with both S3 variables set. Classify absent variables as readiness
skips and browser/cloud/policy restrictions as environment blocks; neither is
live proof.

Run authority and drift checks:

```bash
git diff --name-only origin/main...HEAD -- \
  apps/axon-web/proto \
  apps/axon-web/src/generated/contracts \
  crates/contract-proto/src/generated

git diff --name-only origin/main...HEAD -- \
  Cargo.toml Cargo.lock \
  apps/axon-web/package.json apps/axon-web/package-lock.json

rg -n "openDeltaTable\\(" apps/axon-web/src
rg -n "openDeltaTable\\(" \
  apps/axon-web/src/editor \
  apps/axon-web/src/query \
  apps/axon-web/src/services
```

Expected: no contract/dependency drift and exactly one application-layer call
in `services/query.ts`; SDK implementation methods are reported separately.

Request a fresh principal/maintainer review of `b0a7e1c..HEAD` against this
plan. Fix every Critical and Important finding using new failing tests and
rerun affected verification. Request follow-up review for architectural or
route-behavior repairs.

Fetch `origin/main` again. Prove ancestry and inspect relevant overlap if it
moved; do not rebase automatically.

Update this document with exact commits, test counts, review verdict and
repairs, live/readiness classification, drift result, rollback notes, and the
remaining M2/E6/E8/E9 gates. Commit:

```text
docs: close E1 table-first explorer completion
```

Repeat the complete final matrix against that exact commit.

## Commit sequence

This worktree must contain these seven local-only commits above `b0a7e1c`:

1. `docs: close E1 local and public catalog adoption`
2. `docs(plan): define E1 table-first explorer completion`
3. `refactor(web): stabilize catalog connection identity`
4. `refactor(web): make catalog routes resource authoritative`
5. `feat(web): complete table-first catalog explorer`
6. `refactor(web): centralize catalog connection cleanup`
7. `docs: close E1 table-first explorer completion`

Necessary review repairs remain separate, ordinary commits unless they fit
cleanly before the named task commit. Do not rewrite history to hide them.

## Rollback and publication boundary

- Roll back with ordinary commit reverts, never reset or history rewriting.
- Keep identity, routing, Explorer, and lifecycle behavior in separate commits.
- Preserve the existing persistence key and backward-readable superset. Older
  code may treat canonical connection IDs as opaque and ignore `catalogName`.
  New canonical URLs are not guaranteed after application rollback.
- Retain the old E0 route bridge only for exact legacy bookmarks; do not create
  a new producer.
- Leave query cache keys/schema, protobufs, generated output, Rust contracts,
  dependencies, descriptors, and execution behavior unchanged.
- Current M0 providers expose one generated catalog per connection. M2 owns any
  multi-catalog or session-proxied restructuring.
- A blocked remote Buf check is recorded as an environment-policy block and
  supplemented by exact zero-diff evidence; it is never reported green.
- Leave this isolated worktree intact and clean.
- Local commits only: no push, remote branch, pull request, merge, deployment,
  release, or tag.
