# EPIC-04: Browser DataFusion WASM Runtime And HTTP Object-Store Hardening

- Type: Epic
- Accountable: Query platform lead
- Delivery DRI: Runtime / engine team
- Depends on: EPIC-01, EPIC-02, EPIC-03
- Milestone: `M3`

## Goal

Build the browser runtime for supported SQL workloads over signed HTTPS or proxy access.

## Packages In Scope

- `crates/wasm-query-runtime`
- `crates/wasm-http-object-store`
- `crates/browser-sdk`
- `apps/web-console`
- `tests/conformance`
- `tests/perf`

## Deliverables

- SQL-first browser API
- descriptor-based table registration
- HTTP range-read adapter
- deterministic fallback to native runtime
- explain and metrics in browser
- single-partition execution by default
- size and startup budgets

## Current In-Repo Status

The repository now implements two thin in-repo EPIC-04 slices:

- `crates/wasm-http-object-store` provides a URL-only `HttpRangeReader` with exact full, bounded, from-offset, and suffix byte-range support plus deterministic local tests for footer-style reads and `401` / `403` / `404` / `416` / malformed partial-response handling
- `crates/wasm-query-runtime` now provides a constrained browser runtime envelope with runtime-owned config validation, an opaque `BrowserObjectSource` boundary for URL-backed reads, runtime-owned request and execution timeout policy for default readers, bounded-concurrency snapshot-preflight deadlines, descriptor materialization from HTTPS-only `BrowserHttpSnapshotDescriptor` inputs into runtime-owned validated object sources, a tiny async probe path built on `HttpRangeReader`, file-driven Parquet footer bootstrap with descriptor-size validation and captured bootstrap ETags, strongly typed Parquet footer-to-metadata decoding, per-file integer footer-stat extraction, validated bootstrapped snapshot/file constructors with read-only accessors, richer uniform-schema validation, deterministic query-shape analysis, a browser planning API that returns candidate-file sets plus partition/file-stat pruning summaries, explicit partition-column typing carried from the control plane into execution, a typed browser execution-plan lowering API with typed filters and required scan columns, and a narrow execution-plan interpreter over the curated supported SQL corpus for passthrough outputs plus grouped and ungrouped aggregates, while still rejecting `DISTINCT`, `HAVING`, wildcard projections, and non-output-aligned `ORDER BY` expressions and routing deferred aggregate functions such as `ARRAY_AGG`, `BOOL_AND`, and `BOOL_OR` to native fallback instead of overclaiming browser SQL execution
- EPIC-03 now also provides the shared browser HTTP snapshot descriptor contract plus deterministic control-plane-side URL attachment, and `wasm-query-runtime` can now consume that descriptor into runtime-owned object sources without yet attempting DataFusion table registration
- cross-crate tests in `crates/delta-control-plane/tests/browser_snapshot_preflight.rs` now prove the real in-repo seam from resolved Delta snapshots to browser preflight summaries, browser planning, typed browser execution-plan construction, and narrow browser execution over real local fixture Parquet files, including parity against the native `COUNT(*)`, `files_touched`, and `files_skipped` oracle on curated partition, numeric-string partition, and integer-stats pruning cases plus non-aggregate and aggregate native-result parity over the supported-browser SQL corpus and explicit native-only envelope divergences
- wasm-target compile coverage proves the browser crates remain compatible with `wasm32-unknown-unknown`

The following EPIC-04 work remains explicitly out of current in-repo scope:

- broad browser SQL / DataFusion execution outside the current execution-plan subset in `crates/wasm-query-runtime`
- descriptor-based table registration
- `crates/browser-sdk` public API work
- `crates/query-router` fallback orchestration
- bundle-size, startup, and memory benchmarking
- any service-backed browser access, signed URL issuance, proxy mode orchestration, or other work that depends on the still-missing `services/query-api`

## Child Issues

1. Wire `wasm-query-runtime` to descriptor-based registration.
2. Implement the HTTP range adapter in `wasm-http-object-store`.
3. Add range tests for footer, bounded, suffix, and offset reads.
4. Add the SQL API and JS/TS wrapper in `browser-sdk`.
5. Add structured fallback routing.
6. Add bundle-size reporting and cold-start benchmark.
7. Add browser memory benchmark.
8. Add explain-plan rendering and debug metrics.
9. Add deterministic error taxonomy for auth, range, and protocol failures.
10. Add a sample web console.

## Acceptance Criteria

- the browser runtime executes the supported SQL corpus with parity to native
- the browser defaults to one partition and reports this clearly
- range-read correctness tests pass
- bundle size and startup budgets are captured in CI
- the SDK returns structured fallback reasons
- no browser package depends on signing or service-account code

## Definition Of Done

A supported browser query can read a Delta-backed table through signed HTTPS or proxy descriptors and return the same result as native. The current in-repo slice now reaches a narrower but real milestone: it can validate supported browser SQL, bootstrap metadata with explicit partition typing and bootstrap ETags, deterministically plan/prune the candidate Parquet file set, lower that result into a typed browser execution plan, and execute the curated supported execution-plan corpus locally over loopback-served Parquet files while failing closed when identity or partition typing guarantees are absent. Broad browser SQL / DataFusion execution remains out of scope.
