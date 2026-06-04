# Axon Web Runtime

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

The non-browser checks can run in a normal shell or agent sandbox:

```bash
npm exec -- tsc --noEmit
npm run format:check
npm run lint
npm run test:sdk
```

To use the interactive SQL workbench locally:

```bash
npm run dev
```

Open `https://127.0.0.1:5173` and run queries against the selected connected catalog. The default connected sample is `sample-lake.prod_like.events`, backed by the generated prod-like fixture. The workbench resolves that selected table source and opens the browser query session as part of query execution, so there is no separate snapshot step. The editor uses CodeMirror 6 with SQL highlighting and sample queries for row counts, category totals, and filtered top values. Results keep Arrow IPC as the canonical transport and render only a bounded preview in the page, alongside elapsed time, execution target, metrics, Arrow IPC byte length, row count, worker events, and structured errors.

The editor uses History API routes. Static deployments must rewrite `/connect` and any future editor routes to `index.html`. The root editor is the only product UI in the production build.

The supported SQL shape is the current browser runtime envelope: read-only `SELECT` statements over the selected connected table, with the projection, filter, grouping, ordering, and limit forms covered by the sample queries and browser runtime tests. Unsupported statements, such as mutations, render structured browser errors rather than routing silently.

Server query fallback is an opt-in build mode. The default app build has it disabled and does not show server fallback controls or labels. To expose that path in your own environment, build with `VITE_AXON_SERVER_QUERY_FALLBACK=server` and provide a separate authenticated server query module.

The Connect flow supports selected local Delta folders through browser-owned snapshot reconstruction and browser WASM query execution. It persists only local registry metadata in catalog state; local file bytes stay in browser storage where supported. ZIP import, object-registry import, and broad "any local table" guarantees are outside the current UI claim.

The browser query path still uses `src/sandbox-query-worker.ts` and `src/lib.rs` as the worker bridge. The duplicate piece was the old sandbox page, not the worker contract.

The E2E test starts Vite over HTTPS, opens Chromium, Firefox, and WebKit through Playwright, and covers the browser worker envelope path for startup, Arrow IPC result bytes, structured browser errors, Delta Sharing descriptor handoff, and cancellation-shaped errors. The editor smoke suite covers root UI catalog selection, query execution, and local-folder registry reload.

Browser Playwright gates launch real browsers and should run from a normal developer terminal or CI host with browser-launch permissions:

```bash
npm run test:browser
```

When validating the local Delta editor flow against an existing dev server, run `npm run dev` in one terminal and then run the focused browser gate from an unsandboxed terminal:

```bash
PLAYWRIGHT_BASE_URL=https://127.0.0.1:5173 npm run test:browser:local-delta -- --reporter=line
```

The public GCS live smoke is env-gated. Without `AXON_LIVE_PUBLIC_GCS_TABLE_URI`, it reports skips without starting Vite; with the variable set, Playwright starts Vite for the browser query path.

```bash
AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://bucket/table npm run test:browser:public-gcs-live -- --reporter=line
```

In the Codex macOS execution sandbox, Chromium can fail before any app code runs with `bootstrap_check_in ... MachPortRendezvousServer ... Permission denied`. Treat that as an environment failure and rerun the same Playwright browser command outside the sandbox or with elevated execution permissions before diagnosing app behavior.

To run one browser while iterating:

```bash
npm run test:e2e -- --project=chromium
npm run test:e2e -- --project=firefox
npm run test:e2e -- --project=webkit
```

This Playwright browser matrix is a local/manual gate for now rather than a CI step because it adds Node, browser download, fixture generation, Vite, and three browser launches to the existing Rust-focused workflow.

## TypeScript worker SDK wrapper

The app package also carries the first TypeScript SDK wrapper for Axon's browser worker envelope in [`src/axon-browser-sdk.ts`](src/axon-browser-sdk.ts). It creates or accepts a browser `Worker`, sends the `open_delta_table`, `sql`, fire-and-forget `cancel`, and `dispose` commands, normalizes Arrow IPC result bytes to `Uint8Array`, routes typed runtime event envelopes to an optional `onEvent` handler, and raises `AxonWorkerError` with the structured `fallback_reason` when the worker returns an error envelope.

`openDeltaLocation()` is a thin SDK wrapper around a trusted Delta snapshot descriptor resolver. It sends a logical object-store URI plus an opaque storage access profile, validates the resolver envelope, enforces descriptor expiry with `descriptor_expired`, asserts `resolved_snapshot_version === descriptor.snapshot_version`, and then forwards the returned `BrowserHttpSnapshotDescriptor` unchanged through the existing `openDeltaTable()` worker path. The resolved open result includes resolver metadata such as `resolved_snapshot_version`, `actual_access_mode`, `expires_at_epoch_ms`, and `correlation_id`, but it does not include descriptor file URLs. Resolver access modes are separate from runtime `BrowserAccessMode`: `auto`, `signed_url`, and `proxy` describe resolver behavior, while the worker still receives browser-safe HTTP descriptors.

`openUnityCatalogTable()` is contract-first for UC. It asks an authenticated app/BFF session for a `ReadAccessPlan`, consumes `brokered_delta` and `delta_sharing` plans by adapting them into the existing `openDeltaTable()` descriptor path, returns structured `sql_fallback_required` or `blocked` states before worker handoff, and never accepts browser-owned UC tokens or client secrets.

`openParquetDataset()` opens ordinary Parquet files without Delta log metadata. The caller supplies a `BrowserHttpParquetDatasetDescriptor` with browser-safe file URLs, optional partition metadata, and the logical dataset URI used by later `query(name, sql)` calls. Unlike Delta opens, Parquet dataset queries do not require or inject a `snapshot_version`.

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

await client.dispose('events');
client.terminate();
```

Plain Parquet open:

```ts
await client.openParquetDataset('events', {
  table_uri: 'https://data.example.test/events',
  files: [
    {
      path: 'part-000.parquet',
      url: 'https://data.example.test/events/part-000.parquet',
      size_bytes: 1048576,
      partition_values: {},
    },
  ],
});

const result = await client.query('events', 'SELECT COUNT(*) AS row_count FROM events');
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

The Daxis-specific descriptor resolver helper in [`examples/daxis-descriptor-resolver.ts`](examples/daxis-descriptor-resolver.ts) wraps the same SDK path for `POST /v1/query/delta/snapshot-descriptor`. It sends only the logical table URI, resolver URL, `x-daxis-request-id` header, and opaque credential profile reference to Daxis, preserves structured resolver error codes and correlation IDs, then opens the returned `BrowserHttpSnapshotDescriptor` through the worker without forwarding the credential profile.

The Daxis read-access-plan helper in [`examples/daxis-read-access-plan.ts`](examples/daxis-read-access-plan.ts) wraps `POST /v1/catalog/read-access-plan` through `openUnityCatalogTable()`. It sends the same `x-daxis-request-id` header when a request ID is provided, preserves structured Daxis read-plan errors with HTTP status and correlation IDs, consumes `brokered_delta` and `delta_sharing` plans through the existing descriptor handoff path, preserves `sql_fallback_required` and `blocked` states before worker handoff, and keeps Daxis session and grant identifiers out of worker commands.

The Daxis object-grant helper in [`examples/daxis-object-grant-adapter.ts`](examples/daxis-object-grant-adapter.ts) exposes a grant-scoped client for `list`, `head`, `batch-sign`, and `range` routes, plus a `BrokeredDeltaPlanAdapter` wrapper for the SDK batch-sign step. It keeps grant IDs in Daxis route calls instead of worker commands, forwards `x-daxis-request-id`, rejects expired grants, denied route capabilities, malformed route payloads, invalid ranges, and malformed range responses before handoff, returns range bytes through the grant route, and preserves structured grant errors with HTTP status and correlation IDs.

`AXON_BROWSER_BUNDLE_MANIFEST` ships the current single-threaded baseline bundle and records future
SIMD / threaded / SIMD-threaded bundle IDs as `status: 'future'`. `selectBundle` ignores future
entries until a deployment changes them to `available` and hosts the matching worker and WASM
assets. Threaded bundles require cross-origin isolation, `SharedArrayBuffer`, and shared WASM
memory; SIMD bundles require WASM SIMD; bundles can also declare a `BigInt64Array` requirement.

The SDK expects `BrowserHttpSnapshotDescriptor.active_files[*].url` to contain browser-safe object URLs supplied by a trusted control-plane seam. It does not mint cloud credentials or put cloud secrets in browser code.

See [Browser Embedding Deployment Guide](../../docs/program/browser-embedding-deployment.md) for worker/WASM asset hosting, CSP, COOP/COEP, CORS/range headers, signed URL constraints, cache behavior, and package export guidance.
