# Performance Tests

This directory defines the performance gate contract for the browser engine slice.

Current expectations:

- CI compiles `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, and `browser-engine-worker` for `wasm32-unknown-unknown`, runs host tests for the split browser crates, and runs dedicated `wasm32-unknown-unknown` smoke suites for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- `crates/wasm-http-object-store` now exposes transport-local metrics for extent-body bytes fetched, bytes reused from cache, and validation misses caused by object-identity drift.
- Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`.
- `crates/wasm-query-session` keeps repeated legacy narrow browser-query bootstrap costs in-memory only; persistent-cache hooks may exist lower in the stack, but OPFS / IndexedDB backends remain deferred. The Daxis-facing app worker is browser DataFusion-backed through `wasm-datafusion-session`.
- The blocking artifact budget now targets the real `target/wasm32-unknown-unknown/release/browser_engine_worker.wasm` bundle through `tests/perf/report_browser_worker_artifact.sh`.
- `tests/perf/report_exec_contract_worker_artifact.sh` enables the off-by-default `exec-contract-size-probe` feature, verifies the worker links `axon-contract-proto`, `buffa`, and `buffa-types`, confirms the probe export survives linking, and applies the same artifact budget. Normal worker builds do not enable this proof-only feature.
- CI publishes a report-only host-proxy worker startup and memory baseline from `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`, while the wasm smoke validates the same report path on the browser target and the legacy narrow worker artifact report states `session_shell = true` and `browser_datafusion = false`. The DataFusion default worker artifact report states `browser_datafusion = true`, and its size evidence is captured through the `axon-web-wasm` DataFusion size report in the release process.
- Local crate coverage still matters for regression detection:
  - `cargo test -p wasm-http-object-store --locked`
- `cargo test -p wasm-parquet-engine --locked`
- `cargo test -p wasm-delta-snapshot --locked`
- `cargo test -p wasm-query-session --locked`
- `cargo test -p browser-sdk --locked`
- `cargo test -p browser-engine-worker --locked`

Supporting docs:

- `README.md` documents the transport cache contract, cache-mode reporting, and the current persistent-cache deferral.
- `docs/program/browser-release-integration-runbook.md` covers the repo-owned transport troubleshooting path.

Useful local commands:

- `bash tests/perf/report_browser_worker_artifact.sh`
- `bash tests/perf/report_exec_contract_worker_artifact.sh`
- `cargo test -p wasm-http-object-store --locked`
- `cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke`
- `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture`
- `cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture`

## Browser DataFusion worker size

The repeatable DataFusion size report requires local `cargo`, `wasm-bindgen`, `wasm-opt`, `gzip`,
`brotli`, and `twiggy` tools. It writes generated artifacts under `target/df-size`.

The shipped `axon-web-wasm` worker and its 6 MiB Brotli budget are the defaults:

```bash
bash tests/perf/report_datafusion_wasm_size.sh
```

CI runs that exact default-SKU gate for pull requests and `main`. Set
`AXON_DF_SIZE_PACKAGE` and `AXON_DF_SIZE_WASM_STEM` only for exploratory
non-default reports; those reports are report-only unless they also provide an
explicit `AXON_DF_BROTLI_BUDGET_BYTES`.

```bash
AXON_DF_SIZE_PACKAGE=wasm-datafusion-planner-poc \
  bash tests/perf/report_datafusion_wasm_size.sh
AXON_DF_SIZE_PACKAGE=wasm-datafusion-planner-poc \
  AXON_DF_SIZE_CARGO_FLAGS="--features optimizer" \
  bash tests/perf/report_datafusion_wasm_size.sh
```

## Browser query performance

The release-facing browser probe builds the shipped WASM worker and runs a deterministic
Chromium query against the checked-in prod-like Delta/Parquet fixture:

```bash
bash tests/perf/browser_query_performance.sh
```

It writes `target/perf/browser-query-performance.json` with:

- worker/open startup duration;
- cold, warm, and five repeated query durations plus `arrow_ipc_ready`;
- result byte and chunk counts;
- exact coordinator staging and Rust cursor/transport peaks;
- a post-GC retained-heap delta for the page and public SDK;
- an over-limit query proving that no public Arrow IPC chunk escapes before failure.

The browser probe does not assert that the warm query is faster than the cold query. It records
both and applies independent checked-in ceilings, because the current public-storage evidence
does not prove a cache or readahead latency win. `browser_datafusion_engine_smoke.sh` remains a
Rust host diagnostic and is not browser-performance evidence.

Further benchmark work:

- bytes fetched versus bytes reused for extent-cache hit ratio
- validation-miss rate when object identity drifts underneath cached extents
- candidate-file pruning effectiveness
- bundle-size trend lines for the shipped worker artifact and any future app-level bundle

These docs do not imply that live dashboards exist in this repository. Dashboarding remains external until a service-side telemetry pipeline exists.
These docs also do not claim signed URL issuance, proxy-mode request issuance, audit logging, or production CORS/origin validation. Those remain external blockers outside repo-owned performance claims.
