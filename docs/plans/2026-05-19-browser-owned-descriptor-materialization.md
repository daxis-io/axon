# Browser-Owned Descriptor Materialization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Axon's default browser product path produce a `BrowserHttpSnapshotDescriptor` in the browser, either by reconstructing a Delta snapshot from table-root access or by materializing a descriptor from manifests, Delta Sharing file actions, or brokered file lists.

**Architecture:** `openDeltaTable(name, descriptor)` remains the single execution handoff. Access brokers may authenticate, authorize, and vend browser-safe access material, but they do not reconstruct snapshots or execute SQL by default. Server snapshot resolution and server query fallback are explicit modes, not implicit behavior.

**Tech Stack:** `apps/axon-web`, TypeScript SDK and Playwright SDK tests, React connect UI, `query-contract`, `wasm-delta-snapshot`, `wasm-http-object-store`, browser dependency guardrails, Delta Sharing protocol URL/dir access semantics, Unity Catalog `ReadAccessPlan` and credential-vending contracts.

---

## Product Contract

Default architecture:

```text
browser-safe access material
  -> browser reconstructs or materializes the snapshot/descriptor
  -> BrowserHttpSnapshotDescriptor
  -> openDeltaTable()
  -> local query execution
```

When the source exposes table-root object access:

```text
browser reads/lists _delta_log
  -> wasm-delta-snapshot reconstructs snapshot
  -> BrowserHttpSnapshotDescriptor
```

When the source exposes file actions, manifests, or descriptors:

```text
browser-safe manifest / Delta Sharing file actions / provider file list
  -> browser materializes BrowserHttpSnapshotDescriptor directly
```

Core invariants:

- BFF/access broker does not query by default.
- BFF/access broker does not reconstruct snapshots by default.
- Browser/runtime owns local descriptor production when it has safe material.
- Server snapshot mode is an explicit enterprise/resolver mode.
- Server query mode is only explicit fallback, such as `sql_fallback_required`.
- Raw long-lived cloud credentials and provider secrets do not enter browser packages.

Use these terms consistently:

- **Access broker:** authenticates, authorizes, and vends browser-safe access material.
- **Browser snapshot resolver:** browser-owned `_delta_log` reconstruction over table-root/object-grant access.
- **Descriptor materializer:** browser-owned conversion from manifests, Delta Sharing file actions, file lists, or resolver-produced descriptors into `BrowserHttpSnapshotDescriptor`.
- **Server snapshot resolver:** optional enterprise mode that returns a descriptor.
- **Server fallback:** server-side SQL execution for governed assets.

## Source Matrix

| Source | Auth Owner | Access Broker | Snapshot Owner | Descriptor Material Source | Query Owner |
| --- | --- | --- | --- | --- | --- |
| Local files | Browser/user grant | None | Browser | Local file handles | Browser |
| Public bucket / HTTP | Browser-readable storage | None | Browser | HTTP table root / `_delta_log` when list/head/range are available | Browser |
| Private bucket | BFF brokers narrow access | BFF | Browser by default | Object grants, signed URLs, proxy URLs, manifests, or brokered descriptor | Browser |
| Delta Sharing URL mode | Sharing provider | Sharing server or app broker | Browser descriptor materialization | Sharing query response / file URLs | Browser |
| Delta Sharing dir mode | Sharing provider / BFF | Sharing server or BFF | Browser by default if safe object grants exist | Temporary directory-scoped grants, object grants, or brokered descriptor | Browser |
| Unity Catalog | UC/BFF | BFF | Browser by default | `ReadAccessPlan`, object grants, manifests, or descriptors | Browser unless fallback required |
| Governed UC fallback | UC/BFF | BFF | Server | SQL result batches | Databricks SQL / server fallback |

External references to recheck during implementation:

- Delta Sharing protocol access modes: URL mode returns presigned file URLs; directory mode issues temporary cloud credentials for table-root access.
- Databricks Unity Catalog credential vending: direct external-engine access requires eligible table capabilities, short-lived credentials, and excludes governed shapes such as row filters, column masks, views, shared tables, materialized views, streaming tables, online tables, and foreign tables.

## Task 1: Document The Browser-Owned Descriptor Contract

**Files:**

- Create: `docs/program/browser-owned-descriptor-materialization.md`
- Modify: `docs/program/browser-delta-compatibility-matrix.md`
- Modify: `docs/program/browser-uc-brokered-runtime-contract.md`
- Modify: `docs/program/browser-embedding-deployment.md`

**Step 1: Write the contract doc**

Create `docs/program/browser-owned-descriptor-materialization.md` with:

- the product contract above
- the source matrix above
- a section distinguishing table-root reconstruction from manifest/file-action materialization
- a section saying access brokers do not reconstruct snapshots or execute SQL by default
- a section defining server snapshot resolver and server query fallback as explicit modes
- a references section linking Delta Sharing protocol access modes and Databricks UC credential vending

**Step 2: Update existing docs**

Update:

- `browser-delta-compatibility-matrix.md` so supported browser paths include both browser snapshot reconstruction and browser descriptor materialization.
- `browser-uc-brokered-runtime-contract.md` so `ReadAccessPlan` can lead to object-grant reconstruction, descriptor materialization, `sql_fallback_required`, or `blocked`.
- `browser-embedding-deployment.md` so `openDeltaLocation` is no longer described as only a trusted resolver wrapper once the SDK API changes land.

**Step 3: Verify docs**

Run:

```bash
rg -n "resolver|BFF|browser-local|descriptor material" docs/program
git diff --check docs/program
```

Expected:

- No generic "resolver" wording remains where "access broker", "browser snapshot resolver", or "server snapshot resolver" is more precise.
- No doc says the BFF queries or reconstructs snapshots by default.

**Step 4: Commit**

```bash
git add docs/program/browser-owned-descriptor-materialization.md \
  docs/program/browser-delta-compatibility-matrix.md \
  docs/program/browser-uc-brokered-runtime-contract.md \
  docs/program/browser-embedding-deployment.md
git commit -m "docs: define browser-owned descriptor materialization"
```

## Task 2: Normalize SDK Source Types

**Files:**

- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk-shape.ts`

**Step 1: Add failing type-shape tests**

In `apps/axon-web/tests/axon-browser-sdk-shape.ts`, add compile-time examples for:

```ts
type BrowserDeltaSource =
  | { kind: 'local_files'; files: File[] }
  | { kind: 'http_manifest'; manifestUrl: string }
  | { kind: 'cors_http_table'; tableRootUrl: string }
  | { kind: 'brokered_manifest'; manifestUrl: string; grantId?: string }
  | { kind: 'brokered_object_grants'; grant: BrowserObjectGrantDescriptor }
  | { kind: 'delta_sharing_url_files'; files: DeltaSharingFileAction[] }
  | { kind: 'trusted_descriptor'; descriptor: BrowserHttpSnapshotDescriptor };
```

Add examples showing:

- `openDeltaLocation(name, { source })` defaults to browser-owned descriptor production.
- `openDeltaLocation(name, { source, resolutionMode: 'server_snapshot' })` is explicit.
- `openDeltaLocationWithBroker(name, options)` or `openDeltaLocation(name, { resolutionMode: 'brokered_access' })` is explicit for access-broker integration.

Pick one API shape before implementation:

- Preferred if compatibility matters: keep `openDeltaLocation(...)` and add `resolutionMode`.
- Preferred if clarity matters more: keep `openDeltaLocation(...)` as browser-default and add `openDeltaLocationWithBroker(...)`.

**Step 2: Run the shape test**

Run:

```bash
cd apps/axon-web
npm exec -- tsc --noEmit tests/axon-browser-sdk-shape.ts
```

Expected: FAIL until the source union and API options exist.

**Step 3: Implement the source union and option parsing**

In `axon-browser-sdk.ts`:

- add `BrowserDeltaSource`
- add `DeltaLocationResolutionMode = 'browser_local' | 'brokered_access' | 'server_snapshot'`
- preserve `openDeltaTable(name, descriptor)` unchanged
- update `openDeltaLocation(...)` to dispatch by `source.kind` and `resolutionMode`
- preserve the existing trusted resolver path as explicit brokered/server integration, not the default path

**Step 4: Run tests**

```bash
cd apps/axon-web
npm exec -- tsc --noEmit
npm run test:sdk -- axon-browser-sdk.spec.ts
```

Expected: PASS.

**Step 5: Commit**

```bash
git add apps/axon-web/src/axon-browser-sdk.ts \
  apps/axon-web/tests/axon-browser-sdk.spec.ts \
  apps/axon-web/tests/axon-browser-sdk-shape.ts
git commit -m "feat: add browser delta source options"
```

## Task 3: Split Snapshot Reconstruction From Descriptor Materialization

**Files:**

- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `apps/axon-web/src/lib.rs`
- Modify: `apps/axon-web/src/wasm/axon_web_wasm.d.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Test: `crates/wasm-delta-snapshot/tests/http_storage.rs`

**Step 1: Add failing SDK tests**

Add tests proving these source flows call `openDeltaTable` with a descriptor:

- `http_manifest`
- `brokered_manifest`
- `delta_sharing_url_files`
- `trusted_descriptor`

Add a separate test for table-root sources:

- `cors_http_table` dispatches to browser snapshot reconstruction only when list/head/range capability is present.

**Step 2: Add internal functions**

Implement two internal functions in `axon-browser-sdk.ts`:

```ts
async function resolveDeltaSnapshotBrowserLocal(
  source: BrowserDeltaSource,
  options: { snapshotVersion?: number },
): Promise<BrowserHttpSnapshotDescriptor> {
  // Only table-root or object-grant sources belong here.
}

async function materializeDescriptorFromBrowserSource(
  source: BrowserDeltaSource,
): Promise<BrowserHttpSnapshotDescriptor> {
  // Manifest, file-action, trusted-descriptor, and provider-file-list sources belong here.
}
```

Rules:

- `local_files`, `cors_http_table`, and capable `brokered_object_grants` go through `resolveDeltaSnapshotBrowserLocal`.
- `http_manifest`, `brokered_manifest`, `delta_sharing_url_files`, and `trusted_descriptor` go through `materializeDescriptorFromBrowserSource`.
- Do not force Delta Sharing URL mode or manifest sources through `_delta_log` listing.
- Do not accept credential-shaped URLs as source inputs unless they are broker-produced, scoped, and represented as descriptor file URLs.

**Step 3: Extend WASM bridge only where needed**

If the existing `resolve_delta_snapshot_from_manifest(...)` is sufficient, keep it. If table-root/object-grant reconstruction needs a new bridge, add a narrowly named bridge such as:

```rust
#[wasm_bindgen]
pub async fn resolve_delta_snapshot_from_browser_access(
    source_json: String,
    table_uri: String,
) -> Result<String, JsValue>
```

Do not put cloud SDK auth, provider bearer tokens, or production BFF behavior in this bridge.

**Step 4: Run focused tests**

```bash
cargo test -p wasm-delta-snapshot --locked --test http_storage
cd apps/axon-web
npm exec -- tsc --noEmit
npm run test:sdk -- axon-browser-sdk.spec.ts
```

Expected: PASS.

**Step 5: Commit**

```bash
git add apps/axon-web/src/axon-browser-sdk.ts \
  apps/axon-web/src/lib.rs \
  apps/axon-web/src/wasm/axon_web_wasm.d.ts \
  apps/axon-web/tests/axon-browser-sdk.spec.ts \
  crates/wasm-delta-snapshot/tests/http_storage.rs
git commit -m "feat: split browser snapshot and descriptor paths"
```

## Task 4: Reframe UC And Delta Sharing Around Access Plans

**Files:**

- Modify: `crates/query-contract/src/lib.rs`
- Modify: `crates/query-contract/tests/query_contract.rs`
- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Modify: `docs/program/browser-uc-brokered-runtime-contract.md`

**Step 1: Add failing contract tests**

Add or update tests so `ReadAccessPlan` supports these outcomes:

- `brokered_delta` with object-grant reconstruction capability
- descriptor materialization from brokered descriptor/file list
- `delta_sharing` URL-mode file actions
- `sql_fallback_required`
- `blocked`

Add negative tests:

- `sql_fallback_required` must not call `openDeltaTable`.
- `blocked` must not call `openDeltaTable`.
- Delta Sharing dir mode without broker/object grant must not expose raw cloud credentials.
- Private bucket source without broker must not attempt direct cloud SDK auth.

**Step 2: Run contract tests**

```bash
cargo test -p query-contract --locked
cd apps/axon-web
npm run test:sdk -- axon-browser-sdk.spec.ts
```

Expected: FAIL until contract and SDK parsing are aligned.

**Step 3: Implement minimal contract updates**

Keep the existing `ReadAccessPlan` family if it already expresses these states. Add fields only if tests prove a gap, such as:

- `descriptor_material` or `descriptorMaterial` for a brokered descriptor/file list
- explicit `accessMode: 'url' | 'dir'` for Delta Sharing-derived plans
- explicit `requiresServerSnapshot: true` only for server snapshot mode

Do not add browser-owned UC tokens, Databricks tokens, or cloud credentials.

**Step 4: Run focused tests**

```bash
cargo test -p query-contract --locked
cd apps/axon-web
npm exec -- tsc --noEmit
npm run test:sdk -- axon-browser-sdk.spec.ts
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/query-contract/src/lib.rs \
  crates/query-contract/tests/query_contract.rs \
  apps/axon-web/src/axon-browser-sdk.ts \
  apps/axon-web/tests/axon-browser-sdk.spec.ts \
  docs/program/browser-uc-brokered-runtime-contract.md
git commit -m "feat: align governed access plans with browser descriptors"
```

## Task 5: Clean Up Connect UI Terminology

**Files:**

- Modify: `apps/axon-web/src/editor/ConnectPage.tsx`
- Modify: `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- Modify: `apps/axon-web/src/editor/connect/ConnectedCatalogs.tsx`
- Modify: `apps/axon-web/src/editor/connect/data.ts`
- Modify: `apps/axon-web/tests/editor-smoke.spec.ts`
- Modify: `apps/axon-web/tests/sandbox-page.spec.ts`

**Step 1: Add failing UI assertions**

Update Playwright tests to assert status language like:

```text
Access: Browser
Snapshot: Browser
Query: Browser

Access: Brokered
Snapshot: Browser
Query: Browser

Access: UC brokered
Snapshot: Server governed
Query: Databricks SQL fallback
```

Tests should prove:

- "brokered access" does not imply server query.
- server compute appears only for explicit fallback.
- Delta Sharing URL mode is described as descriptor materialization, not `_delta_log` listing.

**Step 2: Run UI tests**

```bash
cd apps/axon-web
npm run test:e2e -- editor-smoke.spec.ts sandbox-page.spec.ts
```

Expected: FAIL until the UI terminology is updated.

**Step 3: Update the connect UI**

Use these labels:

- Local files: `Access: Browser`, `Snapshot: Browser`, `Query: Browser`
- Public HTTP Delta table: `Access: Browser`, `Snapshot: Browser`, `Query: Browser`
- Private bucket: `Access: Brokered`, `Snapshot: Browser`, `Query: Browser`
- Delta Sharing table: `Access: Provider brokered`, `Snapshot: Browser materialized`, `Query: Browser`
- Unity Catalog table: `Access: UC brokered`, `Snapshot: Browser`, `Query: Browser`
- Governed fallback: `Access: UC brokered`, `Snapshot: Server governed`, `Query: Server fallback`

Avoid generic "resolver" copy unless naming a specific server snapshot resolver mode.

**Step 4: Run focused UI checks**

```bash
cd apps/axon-web
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:e2e -- editor-smoke.spec.ts sandbox-page.spec.ts
```

Expected: PASS.

**Step 5: Commit**

```bash
git add apps/axon-web/src/editor/ConnectPage.tsx \
  apps/axon-web/src/editor/connect/ConnectModal.tsx \
  apps/axon-web/src/editor/connect/ConnectedCatalogs.tsx \
  apps/axon-web/src/editor/connect/data.ts \
  apps/axon-web/tests/editor-smoke.spec.ts \
  apps/axon-web/tests/sandbox-page.spec.ts
git commit -m "feat: clarify brokered access versus server compute"
```

## Task 6: Harden Credential And Dependency Guardrails

**Files:**

- Modify: `tests/security/verify_browser_dependency_guardrails.sh`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Modify: `crates/query-contract/tests/query_contract.rs`

**Step 1: Add failing secret-shape tests**

SDK and contract tests must reject browser source inputs containing:

- `accessKeyId`
- `secretAccessKey`
- `AWS_SECRET_ACCESS_KEY`
- service-account JSON
- `private_key`
- `client_secret`
- `refresh_token`
- broad SAS query strings
- signed URL query strings as table-root source inputs

Descriptor file URLs may contain broker-produced short-lived signed URLs, but diagnostics must redact query strings and fragments.

**Step 2: Add dependency denylist coverage**

Update guardrails so browser-target packages cannot import or depend on:

- `aws-sdk`
- `@aws-sdk/*`
- `@google-cloud/storage`
- `google-auth-library`
- `@azure/storage-blob`
- `azure-identity`
- `opendal`

Keep Rust browser-target denylist coverage for cloud SDK/auth crates.

**Step 3: Run expected failures**

```bash
cd apps/axon-web
npm run test:sdk -- axon-browser-sdk.spec.ts
cd ../..
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: FAIL until checks are implemented or test fixtures are updated.

**Step 4: Implement the minimal guardrail changes**

Keep the rejection logic in SDK parsing and contract validation. Do not add broad source scanning that flags safe redacted examples or brokered descriptor file URLs.

**Step 5: Run focused guardrails**

```bash
cargo test -p query-contract --locked
cd apps/axon-web
npm run test:sdk -- axon-browser-sdk.spec.ts
cd ../..
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: PASS.

**Step 6: Commit**

```bash
git add tests/security/verify_browser_dependency_guardrails.sh \
  apps/axon-web/tests/axon-browser-sdk.spec.ts \
  crates/query-contract/tests/query_contract.rs
git commit -m "test: harden browser credential boundaries"
```

## Task 7: Final Verification And Release Evidence

**Files:**

- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`
- Modify: `docs/program/browser-release-integration-runbook.md`

**Step 1: Run final verification**

```bash
cargo test -p query-contract --locked
cargo test -p wasm-delta-snapshot --locked
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-datafusion-session --locked
cd apps/axon-web
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:sdk
npm run test:e2e
cd ../..
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check
```

Expected: PASS, except for any pre-existing unrelated dirty-worktree failures that must be documented with exact commands and output.

**Step 2: Update release evidence**

Record:

- browser snapshot reconstruction path verified
- browser descriptor materialization path verified
- brokered access does not imply server query
- server snapshot mode remains explicit
- server query fallback remains explicit
- raw browser cloud credentials remain rejected

**Step 3: Commit**

```bash
git add docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md \
  docs/release-gates/browser-wasm-delta-gcs-release-evidence.md \
  docs/program/browser-release-integration-runbook.md
git commit -m "docs: record browser descriptor verification evidence"
```

## Stop Conditions

Stop and ask for direction if:

- implementation requires raw cloud credentials, UC tokens, Databricks tokens, or provider bearer tokens in browser package APIs
- Delta Sharing URL mode is being forced through `_delta_log` listing
- UC fallback starts calling `openDeltaTable`
- `openDeltaTable(name, descriptor)` stops being the single browser execution handoff
- production BFF/server code is required inside this repo
- existing dirty app files conflict with the intended slice

## Completion Criteria

The work is complete when:

- docs define access broker, browser snapshot resolver, descriptor materializer, server snapshot resolver, and server fallback distinctly
- SDK source types distinguish table-root reconstruction from manifest/file-action descriptor materialization
- local/public/brokered table-root sources can use browser snapshot reconstruction
- manifest, brokered file-list, Delta Sharing URL-mode, and trusted-descriptor sources can materialize descriptors directly
- UC and Delta Sharing access plans converge on `BrowserHttpSnapshotDescriptor -> openDeltaTable()` unless explicit fallback/block state applies
- UI shows access owner, snapshot/descriptor owner, and query owner separately
- tests prove server fallback is only used for explicit `sql_fallback_required`
- guardrails reject browser-owned raw credentials and browser-target cloud SDK dependencies
