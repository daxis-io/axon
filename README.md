# Axon

Axon is a Rust workspace for building a hybrid query platform with native and browser runtimes, shared query contracts, and supporting infrastructure for control-plane and UDF execution.

## Workspace

- `crates/query-contract` contains shared request and response types, capability flags, and fallback reasons.
- `crates/native-query-runtime` is the native execution runtime scaffold.
- `crates/wasm-query-runtime` is the browser-oriented runtime scaffold.
- `crates/delta-control-plane` is the control-plane scaffold for snapshot resolution and browser-safe access.
- `crates/query-router`, `crates/browser-sdk`, `crates/wasm-http-object-store`, `crates/udf-abi`, and `crates/udf-host-wasi` provide the supporting packages around routing, browser access, and hosted UDF execution.

## Getting Started

```bash
cargo check --workspace
cargo test -p query-contract
cargo test -p native-query-runtime
cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown
```

## Native Runtime Slice

`crates/native-query-runtime` now contains the first callable EPIC-02 slice:

- `bootstrap_table(table_uri)` opens a Delta table and validates the Sprint 1 compatibility envelope.
- `execute_query(request)` registers the table as `axon_table`, executes read-only SQL, and returns Arrow batches, core metrics, and optional explain output.

Local/offline validation:

```bash
cargo test -p native-query-runtime --locked
```

Env-gated GCS smoke validation:

```bash
AXON_GCS_TEST_TABLE_URI=gs://your-bucket/your-table \
cargo test -p native-query-runtime bootstrap_table_supports_env_gated_gcs_smoke -- --exact --nocapture
```

The GCS smoke path assumes standard Google ADC is already available in the shell or runner environment.

## Repository Layout

- `crates/` contains the Rust workspace packages.
- `tests/conformance/` contains scaffold checks plus the native SQL corpus and golden expectations.
- `tests/perf/` contains performance test scaffolding.
- `tests/security/` contains security test scaffolding.
- `.github/workflows/ci.yml` contains the CI configuration.
