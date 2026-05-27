# Production Public Object Storage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make a public GCS Delta table root usable as a production object-storage source in Axon without browser-held private credentials.

**Architecture:** Public object storage is browser-owned by default. The app accepts a logical `gs://bucket/table` URI, verifies that anonymous browser access can list/read `_delta_log` and range-read active data files, reconstructs a Delta snapshot in WASM, converts it to `BrowserHttpSnapshotDescriptor`, and opens it through the existing `openDeltaTable()` worker path. Manifest/materialized descriptors remain supported as a compatibility and fixture path, but direct public table-root access is the object-storage product path.

**Tech Stack:** `apps/axon-web`, TypeScript services, React connect UI, Playwright SDK/editor tests, `wasm-delta-snapshot`, `wasm-http-object-store`, GCS XML/JSON HTTP endpoints, existing `BrowserHttpSnapshotDescriptor` and worker query runtime.

---

## Non-Negotiable Invariants

- Browser code must never accept, persist, log, or send raw cloud credentials.
- Public object-storage reads must use `fetch(..., { credentials: 'omit' })`.
- Input table roots must be logical object-store URIs without userinfo, query strings, fragments, or signed URL parameters.
- Durable connection state may store only sanitized catalog metadata and the logical table URI.
- The existing manifest fixture path must continue to work.
- The direct table-root path must converge on `BrowserHttpSnapshotDescriptor -> openDeltaTable()`.

## Task 1: Add Public Object-Storage Resolver Service

**Files:**

- Create: `apps/axon-web/src/services/object-storage.ts`
- Create: `apps/axon-web/tests/object-storage.spec.ts`

**Step 1: Write failing tests for GCS URI validation and URL mapping**

Add tests that assert:

- `gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta` parses to bucket `axon-public-delta-fixture-20260522-6cf5c6` and prefix `axon-smoke-delta`.
- A relative data file maps to `https://storage.googleapis.com/<bucket>/<prefix>/<path>`.
- Userinfo, query strings, fragments, signed params, and credential-looking values are rejected.

Run:

```bash
cd apps/axon-web
npm run test:sdk -- --grep "public object storage"
```

Expected: FAIL because the service does not exist.

**Step 2: Implement minimal validation and mapping**

Export:

```ts
export type PublicObjectStorageTableRoot = {
  provider: 'gcs';
  tableUri: string;
  bucket: string;
  prefix: string;
  tableRootUrl: string;
};

export function parsePublicObjectStorageTableRoot(input: {
  provider: 'gcs';
  tableUri: string;
}): PublicObjectStorageTableRoot;

export function publicObjectUrl(root: PublicObjectStorageTableRoot, relativePath: string): string;
```

Validation must reject credential-shaped input and path traversal. Use structured errors with stable codes.

**Step 3: Verify tests pass**

Run the same Playwright SDK grep.

Expected: PASS.

**Step 4: Commit**

```bash
git add apps/axon-web/src/services/object-storage.ts apps/axon-web/tests/object-storage.spec.ts
git commit -m "feat: validate public GCS table roots"
```

## Task 2: Reconstruct Snapshot From Public GCS Table Root

**Files:**

- Modify: `apps/axon-web/src/services/object-storage.ts`
- Modify: `apps/axon-web/tests/object-storage.spec.ts`

**Step 1: Write failing tests for anonymous `_delta_log` listing**

Mock `fetch` and assert:

- The service lists `https://storage.googleapis.com/<bucket>?list-type=2&prefix=<prefix>/_delta_log/`.
- Requests use `credentials: 'omit'`.
- XML list results become manifest objects with HTTPS URLs.
- CORS/list failures produce a structured `public_storage_access_failed` error.

Run:

```bash
cd apps/axon-web
npm run test:sdk -- --grep "public object storage"
```

Expected: FAIL because listing is not implemented.

**Step 2: Implement GCS XML listing**

Export:

```ts
export type PublicDeltaLogManifest = {
  tableUri: string;
  objects: Array<{
    relative_path: string;
    url: string;
    size_bytes?: number;
    etag?: string;
  }>;
};

export async function buildPublicDeltaLogManifest(
  root: PublicObjectStorageTableRoot,
): Promise<PublicDeltaLogManifest>;
```

Use `DOMParser` for XML parsing in the browser/test runtime. Keep the implementation GCS-only.

**Step 3: Write failing tests for descriptor construction**

Mock `resolve_delta_snapshot_from_manifest` and a resolved snapshot with one active file. Assert:

- Active file URLs are mapped to public HTTPS object URLs.
- Descriptor table URI remains the logical `gs://...` URI.
- Partition values and stats are preserved.

**Step 4: Implement descriptor construction**

Export:

```ts
export async function resolvePublicObjectStorageDescriptor(input: {
  provider: 'gcs';
  tableUri: string;
  resolveDeltaSnapshotFromManifest: (manifestJson: string, tableUri: string) => Promise<string>;
}): Promise<BrowserHttpSnapshotDescriptor>;
```

**Step 5: Verify and commit**

Run the object-storage tests, then:

```bash
git add apps/axon-web/src/services/object-storage.ts apps/axon-web/tests/object-storage.spec.ts
git commit -m "feat: reconstruct public GCS Delta snapshots"
```

## Task 3: Wire Query Source And Runtime

**Files:**

- Modify: `apps/axon-web/src/services/query-source.ts`
- Modify: `apps/axon-web/src/services/query.ts`
- Test: existing SDK/query-source tests if present; otherwise add focused coverage to `apps/axon-web/tests/editor-smoke.spec.ts`

**Step 1: Write failing tests for query source selection**

Assert a connected object-store table with a table-root URI but no manifest URL becomes:

```ts
{
  kind: 'object_store_table_root',
  provider: 'gcs',
  tableUri: 'gs://bucket/table',
}
```

Expected: FAIL because only manifest/local sources are queryable.

**Step 2: Extend `QueryTableSource`**

Add `ObjectStoreTableRootQueryTableSource` and update:

- `querySourceFromConnectedCatalogs`
- `firstQueryableTableRef`
- `sameQuerySource`
- `isQueryableTable`

**Step 3: Wire `buildSession()`**

In `apps/axon-web/src/services/query.ts`, when source kind is `object_store_table_root`:

- call `ensureWasm()`
- call `resolvePublicObjectStorageDescriptor(...)`
- create the existing query client
- return `SessionState` with descriptor and snapshot metadata.

**Step 4: Verify and commit**

Run:

```bash
cd apps/axon-web
npm run test:sdk -- --grep "public object storage|query source"
```

Then commit:

```bash
git add apps/axon-web/src/services/query-source.ts apps/axon-web/src/services/query.ts apps/axon-web/tests
git commit -m "feat: query public object-store table roots"
```

## Task 4: Make Connect UI Perform Real Public-Storage Tests

**Files:**

- Modify: `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- Modify: `apps/axon-web/src/editor/connect/store.ts`
- Modify: `apps/axon-web/src/editor/connect/types.ts`
- Modify: `apps/axon-web/tests/editor-smoke.spec.ts`

**Step 1: Write failing Playwright test for object-storage connect**

Mock the GCS XML list and Delta log fetches. Assert:

- The object-storage form accepts the public fixture URI.
- `Test connection` performs network access.
- Success allows `Discover tables`.
- The connected catalog stores the logical URI and does not store HTTPS data-file URLs or signed params.

Expected: FAIL because object-storage test currently fails closed.

**Step 2: Add object-storage test state metadata**

Extend `ConnectForm` or local component state so object-storage test success carries discovered metadata:

- table name
- snapshot version
- row/file/size summaries
- protocol label when known
- sanitized logical URI

**Step 3: Implement real test connection**

For object storage:

- call the object-storage descriptor resolver
- summarize the returned descriptor
- avoid retaining signed/private URL fields in durable connection state
- show explicit failures for invalid URI, list blocked, missing Delta log, and CORS/range failures.

**Step 4: Wire catalog persistence**

When source is object storage, persist a connected table without `manifestUrl`; use `uri`/source metadata so `query-source.ts` chooses the table-root runtime path.

**Step 5: Verify and commit**

Run:

```bash
cd apps/axon-web
npm run test:browser:editor-smoke -- --grep "object storage"
```

Then commit:

```bash
git add apps/axon-web/src/editor/connect apps/axon-web/tests/editor-smoke.spec.ts
git commit -m "feat: connect public object storage"
```

## Task 5: Add Documentation And Live Smoke Script

**Files:**

- Modify: `docs/program/browser-owned-descriptor-materialization.md` if present, otherwise create it.
- Modify: `apps/axon-web/package.json`
- Create: `apps/axon-web/tests/public-gcs-live.spec.ts`

**Step 1: Document production requirements**

Document:

- anonymous browser-readable GCS only for the first production slice
- CORS requirements
- no private credentials
- manifest fallback
- verified fixture URI

**Step 2: Add opt-in live smoke**

Create an opt-in Playwright test gated by `AXON_LIVE_PUBLIC_GCS_TABLE_URI`. It should skip when the env var is absent.

**Step 3: Verify and commit**

Run:

```bash
cd apps/axon-web
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:sdk
```

Then commit:

```bash
git add docs/program apps/axon-web/package.json apps/axon-web/tests/public-gcs-live.spec.ts
git commit -m "docs: define public object-storage requirements"
```

## Final Verification

Run:

```bash
cd apps/axon-web
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:sdk
npm run test:browser:editor-smoke -- --grep "object storage|connect source flows"
```

Run a live smoke manually against the fixture:

```bash
curl -sS -o /dev/null -w 'list=%{http_code}\n' \
  'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6?list-type=2&prefix=axon-smoke-delta/_delta_log/&max-keys=5'

curl -sS -o /dev/null -w 'range=%{http_code} content_range=%header{content-range} allow_origin=%header{access-control-allow-origin}\n' \
  -H 'Origin: http://localhost:5173' \
  -H 'Range: bytes=0-15' \
  'https://storage.googleapis.com/axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta/part-00000-afc4ecda-691b-43d8-85cf-da31785877d2-c000.snappy.parquet'
```

Expected: `list=200`, `range=206`, `content_range=bytes 0-15/1128`, and `allow_origin=http://localhost:5173`.
