# Browser Delta Sandbox

This example proves that a real browser can load Axon's WASM Delta snapshot facade and resolve Delta logs over browser HTTP semantics.

It has two fixture paths:

- a tiny checked-in JSON log smoke fixture under `public/fixtures/table/_delta_log/`
- a generated prod-like fixture under `public/fixtures/prod-like/`

The prod-like fixture is generated with delta-rs before dev/build/test. It creates real partitioned Parquet data files, multiple Delta commits, a Snappy-compressed checkpoint parquet at version `2`, `_last_checkpoint`, stats-bearing add actions, and an overwrite commit at version `3` that removes old files and adds the latest active files.

Run it locally:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.114 --locked
npm install
npm run build:fixture
npm run build
npm run test:e2e
```

To use the interactive SQL workbench locally:

```bash
npm run dev
```

Open `https://127.0.0.1:5173` and run queries against `axon_table`. The workbench resolves the selected Delta fixture and opens the browser query session as part of query execution, so there is no separate snapshot step. The editor uses CodeMirror 6 with SQL highlighting and sample queries for row counts, category totals, and filtered top values. Results keep Arrow IPC as the canonical transport and render only a bounded preview in the page, alongside elapsed time, execution target, fallback reason, metrics, Arrow IPC byte length, row count, worker events, and structured errors.

The supported SQL shape is the current browser runtime envelope: read-only `SELECT` statements over the resolved `axon_table`, with the projection, filter, grouping, ordering, and limit forms covered by the sample queries and browser runtime tests. Unsupported statements, such as mutations, render structured browser errors rather than falling back silently.

The `/sandbox.html` route also includes an Object Store source mode. It models the browser-safe UX for S3, GCS, and Azure Blob locations by asking for a logical table URI, resolver access-mode preference, optional snapshot version, and a **Storage access profile** handle. It intentionally has no access-key, secret-key, SAS, bearer-token, or service-account JSON fields. The local implementation uses an injected mock Delta snapshot descriptor resolver that returns fixture-backed `BrowserHttpSnapshotDescriptor` payloads; production IAM, signing, proxying, CORS checks, audit logging, and request correlation remain trusted resolver responsibilities outside this browser package.

This SQL panel is an example-owned sandbox bridge in `src/sandbox-query-worker.ts` and `src/lib.rs`. It is not the production JavaScript worker bootstrap, not a production query API, and does not mint browser cloud credentials.

The E2E test starts Vite over HTTPS, opens Chromium, Firefox, and WebKit through Playwright, asks the WASM facade to resolve both fixture manifests, and asserts that the prod-like fixture resolves snapshot version `3` from checkpoint version `2` plus one replay commit. It also covers same-origin Parquet range requests and the browser worker envelope path for startup, Arrow IPC result bytes, structured fallback, and cancellation-shaped errors.

To run one browser while iterating:

```bash
npm run test:e2e -- --project=chromium
npm run test:e2e -- --project=firefox
npm run test:e2e -- --project=webkit
```

This Playwright browser matrix is a local/manual gate for now rather than a CI step because it adds Node, browser download, fixture generation, Vite, and three browser launches to the existing Rust-focused workflow.

## TypeScript worker SDK wrapper

The example package also carries the first TypeScript SDK wrapper for Axon's browser worker envelope in [`src/axon-browser-sdk.ts`](src/axon-browser-sdk.ts). It creates or accepts a browser `Worker`, sends the `open_delta_table`, `sql`, and `dispose` commands, normalizes Arrow IPC result bytes to `Uint8Array`, routes typed runtime event envelopes to an optional `onEvent` handler, and raises `AxonWorkerError` with the structured `fallback_reason` when the worker returns an error envelope.

`openDeltaLocation()` is a thin SDK wrapper around a trusted Delta snapshot descriptor resolver. It sends a logical object-store URI plus an opaque storage access profile, validates the resolver envelope, enforces descriptor expiry with `descriptor_expired`, asserts `resolved_snapshot_version === descriptor.snapshot_version`, and then forwards the returned `BrowserHttpSnapshotDescriptor` unchanged through the existing `openDeltaTable()` worker path. The resolved open result includes resolver metadata such as `resolved_snapshot_version`, `actual_access_mode`, `expires_at_epoch_ms`, and `correlation_id`, but it does not include descriptor file URLs. Resolver access modes are separate from runtime `BrowserAccessMode`: `auto`, `signed_url`, and `proxy` describe resolver behavior, while the worker still receives browser-safe HTTP descriptors.

```ts
import {
  AXON_BROWSER_BUNDLE_MANIFEST,
  createAxonBrowserClient,
  getPlatformFeatures,
  selectBundle,
  type BrowserHttpSnapshotDescriptor,
} from './src/axon-browser-sdk';

const snapshot: BrowserHttpSnapshotDescriptor = await fetch('/snapshot-descriptor.json').then(
  (response) => response.json(),
);

const platformFeatures = getPlatformFeatures();
const bundleSelection = selectBundle(AXON_BROWSER_BUNDLE_MANIFEST, platformFeatures);
const client = createAxonBrowserClient({
  workerUrl: bundleSelection.bundle.workerUrl,
});

await client.openDeltaTable('events', snapshot);

const result = await client.query('events', 'SELECT COUNT(*) AS row_count FROM events');
const arrowIpcBytes = result.result.bytes;
const fallbackReason = result.fallbackReason;

await client.dispose('events');
client.terminate();
```

Resolver-backed open:

```ts
const client = createAxonBrowserClient({ workerUrl: bundleSelection.bundle.workerUrl });

const opened = await client.openDeltaLocation('events', {
  provider: 'gcs',
  tableUri: 'gs://analytics-prod/events',
  credentialProfile: { id: 'prod-readonly' },
  requestedAccessMode: 'auto',
  resolverUrl: '/api/delta/snapshot-descriptor',
});

console.log(opened.location.resolved_snapshot_version, opened.location.actual_access_mode);
```

`AXON_BROWSER_BUNDLE_MANIFEST` ships the current single-threaded baseline bundle and records future
SIMD / threaded / SIMD-threaded bundle IDs as `status: 'future'`. `selectBundle` ignores future
entries until a deployment changes them to `available` and hosts the matching worker and WASM
assets. Threaded bundles require cross-origin isolation, `SharedArrayBuffer`, and shared WASM
memory; SIMD bundles require WASM SIMD; bundles can also declare a `BigInt64Array` requirement.

The SDK expects `BrowserHttpSnapshotDescriptor.active_files[*].url` to contain browser-safe object URLs supplied by a trusted control-plane seam. It does not mint cloud credentials or put cloud secrets in browser code.

See [Browser Embedding Deployment Guide](../../docs/program/browser-embedding-deployment.md) for worker/WASM asset hosting, CSP, COOP/COEP, CORS/range headers, signed URL constraints, cache behavior, and package export guidance.
