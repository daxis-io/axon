# Performance Tests

This directory defines the performance gate contract for the browser engine slice.

Current expectations:

- CI compiles `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `browser-sdk`, and `browser-engine-worker` for `wasm32-unknown-unknown`, runs host tests for the split browser crates, and runs dedicated `wasm32-unknown-unknown` smoke suites for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- The blocking artifact budget now targets the real `target/wasm32-unknown-unknown/release/browser_engine_worker.wasm` bundle through `tests/perf/report_browser_worker_artifact.sh`.
- CI publishes a report-only host-proxy worker startup and memory baseline from `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`, while the wasm smoke validates the same report path on the browser target.
- Local crate coverage still matters for regression detection:
  - `cargo test -p wasm-parquet-engine --locked`
  - `cargo test -p wasm-delta-snapshot --locked`
  - `cargo test -p browser-sdk --locked`
  - `cargo test -p browser-engine-worker --locked`

Useful local commands:

- `bash tests/perf/report_browser_worker_artifact.sh`
- `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture`

Benchmark work that should live here once the repo grows a stable browser bundle:

- warm-start latency for the browser worker entrypoint after instantiation
- cold and warm query-path latency
- bytes fetched versus bytes skipped
- candidate-file pruning effectiveness
- bundle-size trend lines for the shipped worker artifact and any future app-level bundle
