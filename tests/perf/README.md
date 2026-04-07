# Performance Tests

This directory defines the performance gate contract for the browser engine slice.

Current expectations:

- CI compiles `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `browser-sdk`, and `browser-engine-worker` for `wasm32-unknown-unknown`, runs host tests for the split browser crates, and runs dedicated `wasm32-unknown-unknown` smoke suites for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- `crates/wasm-http-object-store` now exposes transport-local metrics for extent-body bytes fetched, bytes reused from cache, and validation misses caused by object-identity drift.
- The blocking artifact budget now targets the real `target/wasm32-unknown-unknown/release/browser_engine_worker.wasm` bundle through `tests/perf/report_browser_worker_artifact.sh`.
- CI publishes a report-only host-proxy worker startup and memory baseline from `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`, while the wasm smoke validates the same report path on the browser target.
- Local crate coverage still matters for regression detection:
  - `cargo test -p wasm-http-object-store --locked`
  - `cargo test -p wasm-parquet-engine --locked`
  - `cargo test -p wasm-delta-snapshot --locked`
  - `cargo test -p browser-sdk --locked`
  - `cargo test -p browser-engine-worker --locked`

Supporting docs:

- `README.md` documents the transport cache contract, cache-mode reporting, and the current persistent-cache deferral.
- `docs/program/browser-release-integration-runbook.md` covers the repo-owned transport troubleshooting path.

Useful local commands:

- `bash tests/perf/report_browser_worker_artifact.sh`
- `cargo test -p wasm-http-object-store --locked`
- `cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke`
- `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture`

Benchmark work that should live here once the repo grows a stable browser bundle:

- warm-start latency for the browser worker entrypoint after instantiation
- cold and warm query-path latency
- bytes fetched versus bytes reused for extent-cache hit ratio
- validation-miss rate when object identity drifts underneath cached extents
- candidate-file pruning effectiveness
- bundle-size trend lines for the shipped worker artifact and any future app-level bundle

These docs do not imply that live dashboards exist in this repository. Dashboarding remains external until a service-side telemetry pipeline exists.
