# Browser embedding sketch

Minimal embedding example for the Axon browser worker. It is intentionally
small: just enough to show how a host application wires up the SDK, the
module worker, and the WASM bundle manifest.

## Files

- [`index.html`](index.html) — standalone HTML page that boots the worker,
  resolves a Delta snapshot through a host-supplied descriptor URL, and runs
  a single SQL statement.

## Wiring

The host application must supply three things:

1. **Worker module URL** — host-built JS that speaks `BrowserWorkerCommand` /
   `BrowserWorkerResponseEnvelope`. The reference implementation lives in
   [`apps/axon-web/src/sandbox-query-worker.ts`](../../apps/axon-web/src/sandbox-query-worker.ts).
2. **WASM artifact URL** — output of `cargo build -p axon-web-wasm
   --target wasm32-unknown-unknown --release --locked` plus `wasm-bindgen`.
3. **Snapshot descriptor** — a `BrowserHttpSnapshotDescriptor` produced by a
   trusted control-plane resolver. See
   [`docs/program/browser-embedding-deployment.md`](../../docs/program/browser-embedding-deployment.md)
   for the contract.

This example is a sketch — it does not bundle the worker or WASM for you.
For a runnable application, use [`apps/axon-web/`](../../apps/axon-web/),
which builds and serves all three artifacts via Vite.
