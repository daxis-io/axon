# Browser DataFusion Runtime Parity

- Date: 2026-05-22
- Scope: `apps/axon-web` browser-owned DataFusion runtime over browser-safe Delta descriptors and HTTPS object URLs.
- Owner: Runtime / engine team

This document records what the browser DataFusion runtime proves in-repo and what it still rejects explicitly. It is release evidence, not a claim that every DataFusion, Arrow, Parquet, or Delta feature works in the browser.

## Supported SQL Classes

The supported browser SQL envelope is a single read-only `SELECT` scoped to one opened table. Current conformance covers:

- projection, filtering, ordering, and limit
- grouped aggregates such as `COUNT` and `SUM`
- arithmetic, `CAST`, and `CASE` expressions present in the runtime corpus
- Daxis-shaped order exploration queries over `order_id`, `order_date`, `customer_tier`, `status`, amount, discount, and priority fields
- descriptor-backed Delta/Parquet scans through `AxonParquetScanExec`
- Arrow IPC stream output through the worker and SDK boundary

Queries outside the opened table scope are rejected before execution as `InvalidRequest` with `browser DataFusion SQL scope does not allow ...`.

The Daxis query-shape corpus lives in [`../../tests/conformance/daxis-browser-datafusion-query-corpus.json`](../../tests/conformance/daxis-browser-datafusion-query-corpus.json). It is executed by `cargo test -p wasm-datafusion-poc --test daxis_query_corpus` through both an in-memory reference path and the descriptor-backed `AxonParquetScanExec` path.

## Daxis Query Corpus Coverage

Each Daxis corpus case declares `covered_sql_classes` so supported query claims stay tied to runtime evidence and this compatibility statement.

| Case                   | Covered SQL classes                                                                                   |
| ---------------------- | ----------------------------------------------------------------------------------------------------- |
| `recent_open_orders`   | projection, filtering, ordering, limit, descriptor-backed `AxonParquetScanExec`                       |
| `tier_revenue_summary` | projection, filtering, grouped aggregates, ordering, descriptor-backed `AxonParquetScanExec`          |
| `priority_net_amounts` | projection, ordering, arithmetic, `CAST`, `CASE` expressions, descriptor-backed `AxonParquetScanExec` |

## Supported Arrow And Parquet Field Types

The schema adapter admits only field shapes that have browser scan evidence:

- `Boolean`
- signed `Int32` and `Int64`
- `Float32` and `Float64`
- `Date32` from Parquet `DATE`
- UTC `Timestamp(Millisecond)` and `Timestamp(Microsecond)` from Parquet UTC timestamp annotations and legacy `TIMESTAMP_MILLIS`/`TIMESTAMP_MICROS`
- `Utf8` from Parquet UTF8/String and JSON byte-array annotations
- `Binary` from unannotated byte arrays plus raw BSON, ENUM, GEOMETRY, GEOGRAPHY, and unrecognized byte-array annotations
- optional nulls for the primitive/string/binary shapes covered by `parquet_scan_exec`

GEOMETRY and GEOGRAPHY annotations are carried as raw `Binary` payloads only. No geospatial expression semantics are claimed.

## Supported Scan Behavior

Current scan parity covers:

- browser HTTPS range reads over active Parquet file descriptors
- partition pruning for exact predicates, including `OR` and `IN` cases covered by `scan_pushdown`
- file stats and row-group min/max pruning while preserving DataFusion residual correctness
- query budgets for planned and actual scan bytes, output IPC bytes, and returned rows
- cancellation propagated as structured browser DataFusion cancellation errors
- browser-worker proof across Chromium, Firefox, and WebKit

## Daxis Budget Profile

The Daxis M3 browser DataFusion budget profile is checked in at [`../release-gates/daxis-browser-datafusion-budget-profile.json`](../release-gates/daxis-browser-datafusion-budget-profile.json). It records the current interactive query envelope for the Axon Daxis-facing default worker SKU:

- scan bytes: 64 MiB
- Arrow IPC output bytes: 16 MiB
- output batches in flight: 1
- returned rows: 100,000
- Brotli artifact budget: 6.0 MiB

`cargo test -p wasm-datafusion-poc --test daxis_budget_profile` keeps the profile parseable, bounded, and tied to the budget, size, smoke, and Daxis corpus verification commands. The full artifact-size command remains a release evidence command because it requires `wasm-bindgen`, `wasm-opt`, `brotli`, and `twiggy`. For the Daxis-facing default worker, run `AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh`.

## Daxis Runtime Isolation Plan

The Daxis M3 runtime isolation plan is checked in at [`../release-gates/daxis-browser-runtime-isolation-plan.json`](../release-gates/daxis-browser-runtime-isolation-plan.json). It records two separate runtime boundaries:

- the legacy `browser-engine-worker` narrow session as compatibility scaffolding that must not depend on DataFusion
- the `axon-web-wasm` browser DataFusion worker as the Daxis-facing default runtime path that must not depend on the legacy narrow session

`cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan` keeps the plan parseable and tied to the dependency-boundary commands. The plan is a runtime isolation record; Daxis production rollout, dashboards, and oncall readiness remain external M4 proof.

## Unsupported Features

Unsupported feature classes remain explicit:

- nested/list/map fields, variant, unknown logical type, decimals, fixed-size binary, UUID, interval, and other unproven Parquet/Arrow shapes
- local/non-UTC timestamps, nanosecond timestamps, INT96 timestamps, time-only fields, and timestamp fields with conflicting logical/converted annotations
- Delta deletion vectors in `AxonParquetScanExec`
- non-`SELECT` SQL, multi-statement SQL, joins/subqueries that read outside the opened table, and table-function forms outside the one-table scope
- Delta protocol/table features that the control plane or snapshot reconstruction classifies as native-only or terminal unsupported

Schema capability gaps use `UnsupportedFeature` with `browser DataFusion schema does not yet support ...`. Scan capability gaps use `browser DataFusion scan cannot execute ...`. SQL scope violations use `InvalidRequest`.

## Verification Matrix

The local verification set for this runtime is:

```bash
# From the repository root.
cargo test -p wasm-datafusion-session
cargo test -p wasm-datafusion-poc -- --test-threads=1
cargo test -p wasm-datafusion-poc --test daxis_query_corpus
cargo test -p wasm-datafusion-poc --test daxis_budget_profile
cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan
cargo test -p axon-web-wasm
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
cargo test -p wasm-query-session --tests
cargo test -p browser-engine-worker --tests
cargo test -p browser-sdk --tests

# From apps/axon-web.
npm run build:wasm
npm exec -- tsc --noEmit
npm run test:sdk
npm exec -- playwright test --config=playwright.config.ts tests/browser-worker-matrix.spec.ts
npm run format:check
npm run lint

# From the repository root.
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
cargo fmt --check
git diff --check
```

The browser-worker matrix proves:

- opening a Daxis descriptor-resolver table through the SDK helper reaches the real worker and browser DataFusion query path
- opening a browser-safe descriptor succeeds
- SQL returns Arrow IPC stream bytes, not row-shaped JSON
- binary, string, and integer columns survive through preview metadata
- stale cancels issued through the SDK do not poison later queries
- matching SDK cancels return structured cancellation errors
- unsupported features return structured `unsupported_feature` errors

## Artifact And Size Caveats

`apps/axon-web/src/wasm/` is generated by `npm run build:wasm` and is not the source of truth for this document. The Axon-side DataFusion worker identity is recorded in [`browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json`](./browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json) and checked by `cargo test -p axon-web-wasm`. The full `axon-web-wasm` app-size gate remains release-process evidence because it depends on the optional size toolchain; the exact command is recorded in [`../release-gates/daxis-browser-datafusion-budget-profile.json`](../release-gates/daxis-browser-datafusion-budget-profile.json). Size measurements are release evidence, not a correctness substitute.

This parity record does not remove external blockers for signed URL issuance, proxy reads, audit logging, origin policy, or production oncall readiness.
