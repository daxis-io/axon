# Browser embedding sketch

Minimal embedding example for the Axon browser worker. It is intentionally
small: just enough to show how a host application wires up the SDK, the
module worker, and the WASM bundle manifest.

## Files

- [`index.html`](index.html) — standalone HTML page that boots the worker,
  resolves a Delta snapshot through a host-supplied descriptor URL, and runs
  a single SQL statement.

## Wiring

The host application must supply four things:

1. **SDK module URL** — bundled JavaScript that exports
   `createAxonBrowserClient()`. The current private source is
   [`apps/axon-web/src/axon-browser-sdk.ts`](../../apps/axon-web/src/axon-browser-sdk.ts).
2. **Worker module URL** — host-built JS that speaks `BrowserWorkerCommand` /
   `BrowserWorkerResponseEnvelope`. The reference implementation lives in
   [`apps/axon-web/src/sandbox-query-worker.ts`](../../apps/axon-web/src/sandbox-query-worker.ts).
3. **WASM artifact URL** — the worker's matching WASM asset. The reference app
   builds it with `npm run build:wasm` in `apps/axon-web`.
4. **Snapshot descriptor** — a `BrowserHttpSnapshotDescriptor` produced by a
   trusted control-plane resolver. See
   [`docs/program/browser-embedding-deployment.md`](../../docs/program/browser-embedding-deployment.md)
   for the contract.

This example is a sketch — it does not bundle the worker or WASM for you.
For a runnable application, use [`apps/axon-web/`](../../apps/axon-web/),
which builds and serves all three artifacts via Vite.
