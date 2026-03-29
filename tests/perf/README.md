# Performance Tests

This directory defines the performance gate contract for the browser engine slice.

Current expectations:

- CI runs the browser-side crates on `wasm32-unknown-unknown` and reports a release-artifact size proxy for `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-sdk`.
- The size proxy is the crate `*.rlib` produced by `cargo build --release --target wasm32-unknown-unknown`; it is a stand-in for the final browser bundle until an application-level bundling step exists.
- The current browser smoke test remains `cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke`.
- Local crate coverage still matters for regression detection:
  - `cargo test -p wasm-parquet-engine --locked`
  - `cargo test -p wasm-delta-snapshot --locked`
  - `cargo test -p browser-sdk --locked`

Benchmark work that should live here once the repo grows a stable browser bundle:

- startup latency for the browser worker entrypoint
- cold and warm query-path latency
- bytes fetched versus bytes skipped
- candidate-file pruning effectiveness
- bundle-size trend lines for the release proxy and, later, the final shipped bundle
