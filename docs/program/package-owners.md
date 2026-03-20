# Package Owners

This file records the role-based ownership model for the EPIC-01 scaffold. Replace role owners with named teams or individuals when the GitHub org mapping is finalized.

| Path | Owner | Responsibility |
| --- | --- | --- |
| `crates/query-contract` | Runtime / engine team | Shared request/response types, error taxonomy, capability flags, fallback reasons |
| `crates/query-router` | Runtime / engine team | Browser vs native route decisions and fallback capture |
| `crates/native-query-runtime` | Runtime / engine team | Authoritative native DataFusion + Delta execution path |
| `crates/delta-control-plane` | Storage platform team | Snapshot resolution, signed URL/proxy descriptors, policy hooks |
| `crates/wasm-query-runtime` | Runtime / engine team | Browser runtime, SQL execution envelope, browser-safe controls |
| `crates/wasm-http-object-store` | Runtime / engine team | HTTP range-read adapter for signed URL / proxy access |
| `crates/browser-sdk` | Web platform team | Public browser-facing SDK surface |
| `crates/udf-abi` | Runtime / engine team | Versioned WIT / WASI Preview 2 ABI contracts |
| `crates/udf-host-wasi` | Runtime / engine team | Hosted UDF runtime and capability mediation |
| `tests/conformance` | QA / performance engineering | Native vs browser parity harness and scaffold checks |
| `tests/perf` | QA / performance engineering | Benchmarks, budgets, regressions |
| `tests/security` | Security engineering | Secret leakage and origin/access policy verification |
