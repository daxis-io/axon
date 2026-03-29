# Performance Tests

This directory defines the performance gate contract for the browser engine slice.

Current expectations:

- CI compiles `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-sdk` for `wasm32-unknown-unknown`, runs host tests for the split browser crates, and keeps the existing `wasm-query-runtime` `wasm32-unknown-unknown` smoke test.
- Dedicated wasm execution suites for `browser-sdk`, `wasm-parquet-engine`, and `wasm-delta-snapshot` remain future work.
- The size proxy is the crate `*.rlib` produced by `cargo build --release --target wasm32-unknown-unknown`; it is a provisional stand-in for the final browser bundle until an application-level bundling step exists.
- Local crate coverage still matters for regression detection:
  - `cargo test -p wasm-parquet-engine --locked`
  - `cargo test -p wasm-delta-snapshot --locked`
  - `cargo test -p browser-sdk --locked`

Benchmark work that should live here once the repo grows a stable browser bundle:

- startup latency for the browser worker entrypoint
- cold and warm query-path latency
- bytes fetched versus bytes skipped
- candidate-file pruning effectiveness
- bundle-size trend lines for the provisional release proxy and, later, the final shipped bundle
