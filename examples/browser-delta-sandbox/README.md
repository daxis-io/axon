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

The E2E test starts Vite over HTTPS, opens Chromium through Playwright, asks the WASM facade to resolve both fixture manifests, and asserts that the prod-like fixture resolves snapshot version `3` from checkpoint version `2` plus one replay commit.

## TypeScript worker SDK wrapper

The example package also carries the first TypeScript SDK wrapper for Axon's browser worker envelope in [`src/axon-browser-sdk.ts`](src/axon-browser-sdk.ts). It creates or accepts a browser `Worker`, sends the `open_delta_table`, `sql`, and `dispose` commands, normalizes Arrow IPC result bytes to `Uint8Array`, and raises `AxonWorkerError` with the structured `fallback_reason` when the worker returns an error envelope.

```ts
import {
  createAxonBrowserClient,
  type BrowserHttpSnapshotDescriptor,
} from './src/axon-browser-sdk';

const snapshot: BrowserHttpSnapshotDescriptor = await fetch('/snapshot-descriptor.json').then(
  (response) => response.json(),
);

const client = createAxonBrowserClient({
  workerUrl: new URL('/workers/browser-engine-worker.js', window.location.href),
});

await client.openDeltaTable('events', snapshot);

const result = await client.query('events', 'SELECT COUNT(*) AS row_count FROM events');
const arrowIpcBytes = result.result.bytes;
const fallbackReason = result.fallbackReason;

await client.dispose('events');
client.terminate();
```

The SDK expects `BrowserHttpSnapshotDescriptor.active_files[*].url` to contain browser-safe object URLs supplied by a trusted control-plane seam. It does not mint cloud credentials or put cloud secrets in browser code.
