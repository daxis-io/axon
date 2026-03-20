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
cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown
```

## Repository Layout

- `crates/` contains the Rust workspace packages.
- `tests/conformance/` contains scaffold and parity-oriented checks.
- `tests/perf/` contains performance test scaffolding.
- `tests/security/` contains security test scaffolding.
- `.github/workflows/ci.yml` contains the CI configuration.
