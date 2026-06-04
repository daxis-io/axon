# Browser DataFusion Size Audit

- Status: Draft
- Date: 2026-05-04
- Scope: measurement-driven audit for making Axon's DataFusion-powered Delta/Parquet browser engine viable
- Related:
  - [DataFusion WASM Compile Spike](./datafusion-wasm-compile-spike.md)
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)

## Summary

Axon's browser DataFusion product goal is a browser-resident Delta/Parquet query engine powered by
Apache DataFusion:

> DataFusion is the browser query engine. Axon's strategic work is to make Delta Lake and Parquet
> data appear to DataFusion as browser-native, pushdown-capable tables.

This is not the same as using DataFusion only as a SQL planner and lowering into an Axon-owned query
IR. DataFusion should own SQL planning, logical optimization, physical planning, expression
evaluation, physical execution, and Arrow `RecordBatch` output. Axon should own Delta snapshot
resolution through a browser Delta Kernel engine, active Parquet file descriptors, browser-safe range
I/O, scan pushdown, query budgets, and the Arrow IPC worker boundary.

This audit also carries the DataFusion-specific strategy decision: DataFusion physical execution is
the target browser engine. Axon will minimize and adapt DataFusion for browser use, but not replace
DataFusion's execution model with a custom runtime as the destination architecture.

The likely product shape is:

- browser query engine: DataFusion `SessionContext`, SQL planner, optimizer, physical planner, and
  execution in a browser Worker
- Delta protocol layer: `delta-kernel-rs` core with Axon-supplied browser Engine handlers and no
  default Delta Kernel engine
- table access: `AxonDeltaTableProvider` registered with DataFusion
- scan node: `AxonParquetScanExec` over Kernel-derived Delta descriptors, Axon's browser Parquet
  engine, and HTTP range components
- result boundary: Arrow `RecordBatch` stream encoded as Arrow IPC to JavaScript
- legacy custom runtime: fallback, correctness scaffold, and migration bridge only

The important constraint is not "ship the full default `datafusion` crate and accept whatever it
pulls in." The constraint is: keep DataFusion as the execution engine while tailoring the table,
scan, feature, codec, and packaging surface to Axon's browser Delta/Parquet workload.

## Decision Table

| Decision | Current verdict |
| --- | --- |
| Use DataFusion in the browser strategy | Go |
| Make DataFusion physical execution the browser query engine target | Go |
| Use `delta-kernel-rs` core for browser Delta protocol semantics | Go |
| Implement an Axon browser Delta Kernel Engine over cached browser bytes | Go |
| Import high-level `deltalake` into the browser engine | No-go pending a serious feature audit |
| Enable Delta Kernel's default engine in the browser crate | No-go |
| Ship the unmodified full default DataFusion surface without browser-focused trimming | No-go |
| Build `AxonDeltaTableProvider` plus `AxonParquetScanExec` for browser Delta/Parquet access | Primary path |
| Replace DataFusion runtime execution with an Axon-owned query IR | No-go as product strategy |
| Keep the old custom runtime | Legacy fallback, correctness scaffold, and migration bridge |
| Make Substrait or DataFusion proto the hot-path browser IR | No-go for now; keep as optional diagnostics/interchange |

## Feature Floor Measurement Readout

| Variant | Measurement readout | Product implication |
| --- | --- | --- |
| full umbrella runtime POC | 5,775,497 Brotli bytes and 53,644,227 optimized wasm bytes | Encouraging for an initial serious browser analytics engine; optimize and budget rather than abandon DataFusion. |
| planner-only direct subcrates | 2,108,131 Brotli bytes | Useful floor, but no longer the target architecture by itself. |
| planner plus optimizer floor | 2,945,395 Brotli bytes | Confirms planner/optimizer pieces fit inside the engine budget. |
| planner plus physical-expr floor | 2,113,465 Brotli bytes | Useful compile and retention floor, not representative of full physical execution. |
| planner plus physical-plan floor | 2,104,480 Brotli bytes | Useful symbol floor; next measurements must retain real scan and operator paths. |

## 30-Day Gate Result

The 30-day browser DataFusion gate is complete. The recorded evidence is:

- full umbrella DataFusion POC size: raw `82,406,683`, `wasm-opt` `53,644,227`, gzip
  `11,282,641`, Brotli `5,775,497`
- planner-only direct-subcrate size: Brotli `2,108,131`
- planner-plus-optimizer size: Brotli `2,945,395`
- physical-expr floor: Brotli `2,113,465`
- physical-plan floor: Brotli `2,104,480`
- the previous projection, filter, and limit lowering spike works as scaffolding evidence
- the previous projected-output `ORDER BY` and aggregate/grouped aggregate summary corpus works through
  `tests/conformance/browser-datafusion-lowering-corpus.json`
- default worker/runtime DataFusion dependency guardrails still pass

Decision: Continue to 60-day DataFusion Delta/Parquet engine work. The next target is a custom
`AxonDeltaTableProvider` plus `AxonParquetScanExec` that feeds browser-native Arrow batches into
DataFusion physical execution.

This supersedes the prior "lazy planner/compiler into Axon IR" recommendation. Bundle size remains a
release gate, but it is no longer the deciding architecture principle. The product target is a
DataFusion-backed browser Delta/Parquet query engine.

## Strategic Recommendation

Use a DataFusion execution architecture:

```text
Delta table URL / trusted descriptor
  -> async browser fetch and manifest prefetch
  -> DeltaLogCache
  -> delta-kernel-rs core
  -> AxonBrowserKernelEngine handlers
  -> Delta snapshot / Kernel scan descriptors
  -> active Parquet file descriptors
  -> AxonDeltaTableProvider
  -> AxonParquetScanExec
  -> wasm-http-object-store range reads
  -> Arrow RecordBatch stream
  -> DataFusion physical execution
  -> Arrow IPC stream to browser app
```

SQL enters the same engine through DataFusion:

```text
SQL text
  -> DataFusion SQL planner
  -> DataFusion logical optimizer
  -> DataFusion physical planner
  -> AxonDeltaTableProvider scan
  -> AxonParquetScanExec batches
  -> DataFusion operators
  -> Arrow IPC result stream
```

This keeps the browser-owned table-access pieces in Axon:

- Delta Kernel Engine handlers, cached log/checkpoint bytes, and active file descriptors
- browser-safe HTTP range I/O
- Parquet metadata bootstrap and scan primitives
- query budgets, deterministic fallback, and browser metrics
- Arrow IPC result transport

DataFusion owns SQL planning, logical expressions, logical optimization, physical planning, physical
expression evaluation, streaming execution, and `RecordBatch` output. Axon owns table access. That is
the strategic boundary.

Delta Kernel owns the Delta protocol semantics. Axon should use `delta-kernel-rs` core with the
default engine disabled, implement `StorageHandler`, `JsonHandler`, `ParquetHandler`, and
`EvaluationHandler` for the browser, and keep async browser fetch outside synchronous Kernel
callbacks by prefetching into a cache first. High-level `deltalake` stays native: reference
implementation, correctness oracle, and DataFusion integration example.

## Axon Constraints

Axon's browser engine is not a generic database product. The browser SKU already has a repo-owned
lakehouse path, but its target Delta semantics should move behind Delta Kernel. The new center of
gravity is `wasm-delta-kernel-engine` for browser-safe Kernel handlers, `wasm-http-object-store` for
HTTP range reads, `wasm-parquet-engine` for Parquet metadata and scan primitives, and the DataFusion
engine wrapper for DataFusion session registration, execution, fallback, metrics, and Arrow IPC
output. The existing `wasm-delta-snapshot` crate remains useful as a compatibility scaffold and test
fixture source during the migration.

That means Axon does not need a browser SQL engine to solve broad catalog management, generic file
listing, generic object-store integration, CSV/JSON datasources, or bring-your-own-source
ergonomics. The production question is narrower: can Axon make Delta/Parquet tables readable by
DataFusion in a browser Worker while preserving pushdown, memory, cancellation, and range-read
constraints?

The initial browser SQL envelope should remain read-only and bounded: projection, aliases, filters,
`ORDER BY`, `LIMIT`, global aggregates, grouped aggregates, Arrow input/output, and eventual
Parquet-backed table access. Joins, recursive CTEs, `DISTINCT`, `HAVING`, broad scalar-function
catalogs, window functions, DDL/DML, and table functions remain outside the initial browser
execution envelope until product requirements and size budgets justify them.

Browser delivery also constrains the decision. The DataFusion engine path should run in a worker, use Arrow
IPC rather than row JSON for large results, load WASM through streaming instantiation where hosting
allows it, and treat single-threaded WASM as the product baseline. `SharedArrayBuffer`, shared WASM
memory, high-precision memory APIs, and threaded execution depend on cross-origin isolation, so they
are acceleration tiers rather than first-default-profile assumptions. OPFS-backed persistent caches may become useful
for metadata, footers, and selected byte ranges, but the engine should not require OPFS to be
correct.

## Candidate Architecture Matrix

| Candidate | Browser shape | Bundle outlook | Current recommendation |
| --- | --- | --- | --- |
| Delta Kernel core plus Axon browser Engine | Delta Kernel owns protocol semantics; Axon owns cached browser storage, JSON, Parquet, and evaluation handlers | Must be measured first; default engine must remain disabled | Primary Delta protocol architecture |
| DataFusion engine plus Axon table provider | DataFusion owns SQL and execution; Axon owns Delta table registration and browser scan execution | Current POC is 5.5 MiB Brotli before real Delta/Parquet scan integration | Primary production architecture |
| DataFusion built-in Parquet plus browser `object_store` adapter | DataFusion owns Parquet file source as well as execution | Unknown; likely more fragile in browser WASM first | Later evaluation path |
| High-level `deltalake` in browser | Native table/log/object-store stack enters the browser dependency graph | Likely pulls runtime, storage, TLS, and cloud surfaces unless heavily feature-gated | Avoid at first; use natively as oracle/reference |
| Full default DataFusion surface | Use broad upstream crate and defaults without browser trimming | Risky; likely pulls unneeded datasource/codecs/functions | Avoid as a packaging stance |
| DataFusion planner plus Axon IR | DataFusion plans; Axon replaces execution | Smallest potential bundle | Superseded; keep only as scaffold/test harness |
| Legacy custom runtime only | Axon owns planner and execution | Already exists for a narrow subset | Fallback and migration bridge, not the destination |

The repo should now point toward `wasm-datafusion-engine`: a browser Worker engine that registers
`AxonDeltaTableProvider` instances in a DataFusion `SessionContext`, executes SQL through DataFusion,
and streams Arrow IPC results back to JavaScript.

## Proposed Product Gates

The final budgets need product approval, but the size audit should use explicit working gates while
experiments run:

| Gate | Working target | Notes |
| --- | ---: | --- |
| DataFusion browser engine Brotli | Current 5.5 MiB class initially acceptable; reduce with measured feature work | Applies to the target query engine, not a separate optional experiment |
| Streaming init | Keep near current 83-87 ms or improve | Track cold and warm separately |
| First tiny query after warm worker | <= 150 ms p95 initially | Current full DataFusion POC first query is about 78-80 ms |
| Repeated tiny query | <= 2 ms p50 | Current full DataFusion POC is about 0.6 ms median |
| Cold init plus first tiny query | Track separately | Current full DataFusion POC is roughly 160-170 ms combined |
| First Delta/Parquet metadata query | Measure separately | Remote I/O and metadata decode dominate different budgets than engine init |
| First real Delta/Parquet query | Measure separately | Includes range reads, decompression, row-group pruning, aggregation, and result transfer |
| Browser query budgets | Required before default enablement | Enforce scan bytes, Arrow IPC bytes, output rows, and in-flight output batches with structured fallback errors |
| Peak browser working set | Bounded and streaming by default | Full global sort and high-cardinality group-by need explicit budgets or fallback |
| Worker-only execution | Required | DataFusion engine work stays off the main thread |
| Arrow IPC streaming | Required | Output should stream `RecordBatch` values, not force row JSON materialization |

These are audit gates, not a release promise. They exist to make future DataFusion feature-splitting
and Axon runtime experiments comparable.

## Current Baseline

DataFusion is now wired through the dedicated browser DataFusion session as the
`OpenDeltaTable` / SQL Arrow IPC path for UI/runtime builds. The implementation still lives under
`crates/wasm-datafusion-poc`, but `crates/wasm-datafusion-session` keeps that path separate from
the legacy narrow custom session so either side can be removed independently. The POC crate now has a product-shaped engine facade over
`SessionContext`, descriptor-backed table registration, custom scan execution, browser Parquet range
I/O, pushdown traces, query budgets, cancellation shape, and Arrow IPC output. The older
`wasm-datafusion-planner-poc` remains useful as a planner-only lower-bound measurement, not as the
target browser execution architecture.

The current POC proves:

- `datafusion = "=52.4.0"` compiles for `wasm32-unknown-unknown`.
- A long-lived `WasmDataFusionEngine` can register Arrow `RecordBatch` input under table names and
  run SQL repeatedly through DataFusion.
- Results can be returned as Arrow IPC bytes through a `wasm-bindgen` export.
- Delta active-file descriptors can be registered as DataFusion tables through
  `AxonDeltaTableProvider`.
- DataFusion can execute filter, order, limit, and aggregate operators above Axon's custom
  `AxonParquetScanExec`.
- `AxonParquetScanExec` can stream local/browser-style Parquet range-read batches through
  `wasm-parquet-engine` and `wasm-http-object-store`.
- Projection, limit, exact partition pruning, inexact residual predicates, and row-group pruning are
  reflected in scan planning and trace metrics.
- `wasm-query-session`, `browser-engine-worker`, and `browser-sdk` expose `OpenDeltaTable` plus SQL
  Arrow IPC stream commands without exposing DataFusion internals.
- Browser DataFusion query budgets now have explicit controls for `max_scan_bytes`,
  `max_output_ipc_bytes`, `max_batches_in_flight`, and `max_rows_returned`. The Arrow IPC path
  writes batches as DataFusion yields them instead of collecting the full output first. Budget
  failures return structured `QueryError` values with `FallbackReason::BrowserRuntimeConstraint`;
  cancellation returns a structured browser DataFusion cancellation error.
- `tests/perf/browser_datafusion_engine_smoke.sh` records optional smoke timings for streaming init,
  first and repeated tiny queries, first Parquet metadata query, first real Delta/Parquet query, and
  scan metrics. `tests/perf/report_datafusion_wasm_size.sh` remains the Brotli-size source of truth.
- `wasm-query-runtime` can opt into a compiler-boundary lowering spike through its
  `datafusion-planner-poc` feature. That spike is useful scaffolding and a corpus harness, but it is
  no longer the product destination.
- Task 6 added `tests/conformance/browser-datafusion-lowering-corpus.json` as the differential SQL
  corpus for the lowering spike. It covers projection/filter/limit, projected-output `ORDER BY`,
  unsupported join rejection before browser runtime planning, `COUNT(*)`, grouped `COUNT(*)`, and
  grouped `SUM`/`MIN`/`MAX`.

The current engine still does not yet prove full Delta Kernel snapshot resolution in the browser.
`wasm-delta-kernel-engine` and Kernel-derived descriptor conversion remain the next protocol-layer
work before this path can become the default Delta open implementation.

## Measured Artifact Sizes

Measured artifacts from the current release build and `wasm-bindgen` output:

| Artifact | Bytes | Size |
| --- | ---: | ---: |
| Raw Rust release wasm | 82,406,683 | 78.6 MiB |
| wasm-bindgen generated wasm | 76,742,693 | 73.2 MiB |
| `wasm-opt -Oz` generated wasm | 53,644,227 | 51.2 MiB |
| `gzip -9` of optimized wasm | 11,282,641 | 10.8 MiB |
| Brotli `-q 11` of optimized wasm | 5,775,497 | 5.5 MiB |

Release-profile matrix, measured with `tests/perf/report_datafusion_wasm_size.sh` on May 5, 2026:

| Variant | Raw wasm | wasm-opt | gzip | Brotli | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| umbrella current profile | 82,406,683 | 53,644,227 | 11,282,641 | 5,775,497 | Baseline; `wasm-bindgen` output was 76,742,693 bytes. |
| opt-level z | 41,031,540 | 21,594,406 | 5,601,010 | 3,504,722 | Independent temporary profile; `wasm-bindgen` output was 36,590,159 bytes; end-to-end run `real 130.74s`. |
| lto + codegen-units 1 | 53,597,447 | 37,021,684 | 9,192,354 | 5,141,782 | Independent temporary profile; `wasm-bindgen` output was 49,881,686 bytes; end-to-end run `real 360.17s`. |
| panic abort + strip | 71,416,095 | n/a | n/a | n/a | Independent temporary profile; `wasm-bindgen` output was 67,707,622 bytes, then the script failed at `wasm-opt -Oz` with Binaryen feature validation errors for stripped bulk-memory and nontrapping-float-to-int instructions; failing run `real 111.28s`. |
| planner-only direct subcrates | 29,029,645 | 19,363,360 | 3,901,323 | 2,108,131 | Isolated `wasm-datafusion-planner-poc` using `datafusion-common`, `datafusion-expr`, and `datafusion-sql` directly, with exported `plan_sql_to_display` retaining the planner surface; `wasm-bindgen` output was 26,732,143 bytes. Useful as a lower bound, but not the product engine by itself. |
| planner plus optimizer floor | 42,300,914 | 28,444,112 | 5,735,339 | 2,945,395 | Adds optional `datafusion-optimizer` and exported `optimizer_surface_marker_wasm`, retaining the default logical optimizer rule list; `wasm-bindgen` output was 39,236,537 bytes. Confirms planner and optimizer pieces fit inside the emerging engine budget. |
| planner plus physical-expr floor | 29,153,447 | 19,417,295 | 3,916,000 | 2,113,465 | Adds optional `datafusion-physical-expr` and exported `physical_expr_surface_marker_wasm`, retaining a column physical expression marker; `wasm-bindgen` output was 26,845,560 bytes. Inside the hard gate, but this is only a tiny symbol floor, not evidence for broad physical expression execution. |
| planner plus physical-plan floor | 29,186,424 | 19,422,327 | 3,907,415 | 2,104,480 | Adds optional `datafusion-physical-plan` and exported `physical_plan_surface_marker_wasm`, retaining `PlaceholderRowExec`; `wasm-bindgen` output was 26,875,287 bytes. Inside the hard gate, but this remains a narrow execution-plan symbol floor rather than a real physical runtime measurement. The wasm compile required a target-only `uuid/js` feature unifier because `datafusion-physical-plan` pulls `datafusion-functions` default string functions, which enable `uuid/v4`. |

Interpretation:

- Browser compile/instantiate cost is real and must be budgeted.
- Brotli transfer cost is much smaller than raw cost, so browser caching and streaming
  instantiation are important product tools.
- The current 5.5 MiB Brotli class is not disqualifying for a serious browser analytics engine; it
  says optimize, split, cache, and benchmark.
- The optimizer floor is the first measured feature row that materially increases retained planner
  size, but it remains inside the emerging DataFusion engine budget.
- The physical expression and physical plan rows are lower bounds only. They prove those optional
  subcrate surfaces can compile for wasm and be retained through browser exports, but they do not
  measure a representative DataFusion physical runtime.

## Required Browser SQL Profile

This profile should be treated as the minimum investigation target, not as a final public contract.

| Capability | Browser DataFusion need | Current Axon status |
| --- | --- | --- |
| Read-only `SELECT` | Required | Custom runtime already requires `SELECT` only |
| Single table source | Required for the first Daxis DataFusion default profile | Custom runtime requires one source relation |
| Projection and aliases | Required | Custom runtime supports passthrough projection and aliases |
| Filter predicates | Required | Custom runtime supports `AND`, comparison, `IN`, `IS NULL`, `IS NOT NULL` |
| Limit | Required | Custom runtime supports `LIMIT` |
| Order by output columns | Required | Custom runtime supports projected-output `ORDER BY` |
| Primitive and string columns | Required | Custom runtime handles integer, boolean, string, null scalar values |
| Arrow `RecordBatch` input | Required | POC proves in-memory `RecordBatch` input |
| Arrow IPC output | Required | POC and custom runtime use Arrow IPC output |
| Basic aggregates | Required | Custom runtime supports `COUNT`, `COUNT(*)`, `SUM`, integral `AVG`, `MIN`, `MAX`; DataFusion lowering now summarizes `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN`, and `MAX` |
| Group by | Required | Custom runtime has grouped aggregate execution coverage; DataFusion lowering now records grouped aggregate columns |
| Parquet-backed table registration | Required | Proven in `wasm-datafusion-poc` host tests through `AxonParquetScanExec` and browser range I/O |
| Delta active-file registration | Required | Proven for trusted descriptors through `AxonDeltaTableProvider`; Kernel-derived descriptors remain future protocol-layer work |

## Outside Browser DataFusion Default Profile

These should be treated as removable or feature-gatable unless a future product requirement says
otherwise:

| Capability | Current audit stance |
| --- | --- |
| CSV datasource | Not needed for Delta/Parquet browser SKU |
| JSON datasource | Not needed for Delta/Parquet browser SKU |
| Avro datasource | Not needed |
| Broad scalar-function registry | Not needed by the first Daxis DataFusion default profile unless individual functions are explicitly required |
| Regex/string/unicode-heavy function sets | Probably not needed by the first Daxis DataFusion default profile |
| Crypto functions | Not needed |
| Window functions | Not needed by the first Daxis DataFusion default profile |
| Table functions | Not needed by the first Daxis DataFusion default profile |
| Broad catalog/session features | Minimize; keep only what `MemTable` or table provider registration requires |
| Native filesystem integrations | Not needed in browser |
| Cloud SDKs, signing, OpenDAL service integrations | Not allowed in browser runtime |
| Compression codecs unrelated to browser Parquet profile | Gate by observed fixture/table codec needs |
| Verbose debug/format/backtrace paths | Reduce where possible without damaging diagnostics |

## Size Attribution Snapshot

`twiggy top -n 20 target/df-size/wasm-datafusion-poc/wasm_datafusion_poc_bg.wasm` shows that the
largest shallow rows are metadata, read-only data, and repeated SQL parser hash monomorphizations:

| Rank | Bytes | Percent | Item |
| ---: | ---: | ---: | --- |
| 1 | 9,077,342 | 11.83% | `"function names" subsection` |
| 2 | 4,148,291 | 5.41% | data segment `.rodata` |
| 3 | 138,903 | 0.18% | `elem[0]` |
| 4-20 | about 66 KiB each | 0.09% each | repeated `sqlparser::ast::Statement` hash variants |

`twiggy monos -g -m 20` identifies these monomorphization groups as notable bloat:

| Approx bloat bytes | Percent | Group |
| ---: | ---: | --- |
| 1,751,067 | 2.28% | `sqlparser::ast::Statement` `Hash::hash` |
| 1,082,191 | 1.41% | `core::fmt::Debug` for references |
| 906,831 | 1.18% | DataFusion expression tree `map_children` |
| 856,639 | 1.12% | `sqlparser::ast::Statement` `PartialEq::eq` |
| 843,867 | 1.10% | Arrow `PrimitiveArray<T>::try_unary` |
| 812,215 | 1.06% | `Vec<T>::from_iter` specializations |
| 682,126 | 0.89% | `sqlparser::ast::query::TableFactor` `Hash::hash` |
| 623,928 | 0.81% | Arrow primitive-array debug formatting |

This does not mean SQL parsing is removable. It means the exact parser/planner profile matters, and
that derive-heavy AST operations, debug formatting, and generic Arrow kernels are worth measuring
when profile reductions are tested.

## Dependency And Feature Observations

The current POC dependency is:

```toml
datafusion = { version = "=52.4.0", default-features = false, features = ["sql"] }
```

For DataFusion 52.4.0, `sql` enables `datafusion-common/sql`, `datafusion-sql`, and `sqlparser`. It
does not enable DataFusion's optional `parquet` or `compression` features. A wasm-target
`cargo tree -i parquet` does not find a `parquet` crate in the current POC graph.

However, the umbrella `datafusion` crate still pulls broad functionality even with only `sql`
enabled:

- `datafusion-datasource-csv`, `datafusion-datasource-json`, and `datafusion-datasource-arrow`
- `datafusion-catalog`, `datafusion-catalog-listing`, `datafusion-session`, and `object_store`
- physical plan, physical expression, optimizer, pruning, and execution crates
- aggregate, table, window, and scalar function crates
- `arrow` with default `csv`, `ipc`, and `json` features, plus `prettyprint` and `chrono-tz`
- `regex`, `chrono`, `chrono-tz`, `tokio`, `uuid`, and `tempfile`

That points to a feature-boundary problem more than a simple Axon dependency problem.

The reusable center of gravity is the DataFusion execution stack plus a browser-specific table and
scan layer:

- `datafusion-sql`, `datafusion-expr`, and `datafusion-optimizer` remain required for SQL semantics.
- `datafusion-physical-expr`, `datafusion-physical-plan`, `datafusion-execution`, and
  `datafusion-session` are part of the target engine surface, but must be measured and feature-gated
  to Axon's browser workload.
- DataFusion datasource crates for CSV, JSON, Avro, generic listing, and broad function catalogs are
  candidates for feature splitting because Axon's browser SKU is Delta/Parquet-first.
- The first production scan path should be an Axon `TableProvider` and `ExecutionPlan`, not a
  wholesale dependency on every built-in DataFusion file source.

## Profile Versus Retained Surface

| Retained surface | Needed by Axon profile? | Audit action |
| --- | --- | --- |
| `datafusion-sql` and `sqlparser` | Yes | Keep; measure parser breadth but do not replace DataFusion SQL ownership |
| `datafusion-datasource-csv` | No | Candidate for upstream feature split or direct subcrate usage |
| `datafusion-datasource-json` | No | Candidate for upstream feature split or direct subcrate usage |
| `datafusion-datasource-arrow` | Maybe | Needed only if it helps table registration or in-memory fixtures |
| `datafusion-functions` broad registry | Partially | Gate to required scalar functions, or avoid registering broad built-ins |
| aggregate functions | Yes, narrow subset | Gate to `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`; defer arrays/booleans unless required |
| window/table functions | No for the first Daxis DataFusion default profile | Candidate for feature split |
| `object_store` | Maybe | Needed only if DataFusion table registration requires it; browser transport should remain Axon-owned |
| Arrow CSV/JSON features | No | Candidate for Arrow default-feature reduction if DataFusion allows it |
| Arrow IPC | Yes | Keep for result boundary |
| Arrow cast/sort/take kernels | Yes, partially | Keep required primitive/string kernels, measure impact of unsupported type families |
| regex/unicode-heavy logic | Probably no | Gate to explicit query-function requirements |
| debug/format/backtrace paths | Mostly no | Measure release-profile and feature-level reductions |

## Runtime Performance Plan

The runtime plan should stay worker-first. Query execution should live off the main thread, WASM
should load through the browser streaming path when hosting supports the correct MIME type and CSP,
and large results should cross the worker boundary as Arrow IPC. JSON is acceptable for control
messages and small diagnostics, not for large result materialization.

The executor should be DataFusion's streaming `RecordBatch` pipeline with explicit browser budgets.
Filter and projection should run batch-by-batch, aggregates should keep partial state, and
`ORDER BY ... LIMIT` should prefer bounded `TopK` behavior where semantics allow it. Full global
`ORDER BY` without a limit, high-cardinality grouping, and unsupported expression/type combinations
should have observable fallback or failure modes rather than silently collecting unbounded data in
browser memory.

The scan boundary is the highest-leverage performance point. Lowered plans should annotate required
columns, exact and inexact pushdown predicates, limit hints, row-group pruning predicates, dictionary
filter opportunities, and output ordering when known. `AxonDeltaTableProvider::scan` should turn
DataFusion projection, filters, and limit into an `AxonParquetScanExec` that avoids decoding Parquet
columns, files, or row groups that DataFusion will discard later.

Caching should be layered but optional for correctness. Plan caches should key on normalized SQL,
schema fingerprint, planner version, and Delta snapshot identity. Metadata and footer caches can
later use OPFS or another browser storage backend with quota-aware eviction, but the first production
contract should work with memory-only caches and normal HTTP caching.

## Experiment Plan

Run each experiment in its own branch or worktree. After each change, rebuild, run bindgen, optimize,
compress, profile, and run correctness tests.

1. Establish repeatable size script.
   - Output raw, bindgen, `wasm-opt`, gzip, Brotli, `twiggy top`, `twiggy monos`, and cargo-tree
     summaries into package-scoped directories under `target/df-size`.

2. Try release profile knobs.
   - Test `opt-level = "z"`, `lto = true`, `codegen-units = 1`, `panic = "abort"`, and
     `strip = true`.
   - Compare size and build time before any dependency changes.

3. Remove Axon-side POC-only surface.
   - Verify whether direct `arrow-array`, `arrow-schema`, and `arrow-ipc` dependencies can use
     reduced default features without breaking the POC.
   - Measure whether the direct dependencies matter once umbrella DataFusion pulls `arrow`.

4. Prove Delta Kernel core as a browser dependency.
   - Add `crates/wasm-delta-kernel-engine` with `delta_kernel` core and default engine disabled.
   - Implement the smallest compile-only `AxonBrowserKernelEngine` shell.
   - Run wasm-target cargo-tree checks that `tokio`, `reqwest`, native TLS, cloud SDKs,
     `object_store`, and high-level `deltalake` are absent from this crate's Delta protocol graph.
   - Measure the incremental WASM artifact size before adding handlers.

5. Implement cached Delta Kernel snapshot resolution.
   - Add async prefetch outside Delta Kernel for `_last_checkpoint`, checkpoint parts, sidecars, and
     JSON commits when the descriptor or known version makes those files deterministic.
   - Implement cached `StorageHandler`, simple JSON parsing, checkpoint Parquet reads through
     `wasm-parquet-engine`, and a minimal Arrow-compatible evaluator for Delta metadata predicates.
   - Convert Kernel snapshot and scan output into the Axon Delta table descriptor consumed by the
     DataFusion engine.
   - Compare active file lists, schema, partition values, stats, and protocol feature handling
     against existing `wasm-delta-snapshot` fixtures and native `deltalake`.

6. Promote `wasm-datafusion-poc` toward `wasm-datafusion-engine`.
   - Keep `SessionContext`, SQL planning, optimizer, physical planning, and execution in the engine.
   - Replace the synthetic `MemTable` fixture with registered Kernel-derived Delta/Parquet table
     providers.
   - Preserve Arrow IPC output as the worker result boundary.

7. Build `AxonDeltaTableProvider`.
   - Consume Kernel-derived Delta table descriptors with schema, table version, partition columns,
     active files, file sizes, partition values, optional stats, and optional deletion-vector descriptors.
   - Implement DataFusion's scan contract so projection, filters, and limit become scan-planning
     inputs rather than post-scan guesses.
   - Classify pushdown as partition pruning, file-stat pruning, row-group pruning, projection
     pruning, early limit, and residual predicates that DataFusion applies above the scan.
   - Pass DataFusion pushed-down filters into Delta Kernel scans for file skipping where the Kernel
     API can preserve residual filter correctness.

8. Build `AxonParquetScanExec`.
   - Return a DataFusion `ExecutionPlan` that streams Arrow `RecordBatch` values from
     `wasm-parquet-engine` and `wasm-http-object-store`.
   - Enforce worker cancellation, memory budgets, range-read validation, row-group pruning, and
     projection pushdown. PR 8 added browser query-budget controls and structured cancellation
     shape for the current DataFusion POC.
   - Start with one browser partition and add partitioning only after memory and scheduling budgets
     are observable.

9. Compare against a later browser `object_store` path.
   - Keep the first production path on Axon's existing browser-safe Parquet/range stack.
   - Evaluate a DataFusion-native Parquet reader plus browser `object_store` adapter only if it
     reduces Axon-owned scan complexity or improves performance.

10. Test DataFusion feature splits locally.
   - Prototype optional gating for CSV/JSON datasource crates, window/table functions, broad scalar
     function registration, and Arrow default features.
   - Measure each split independently before combining them.

11. Add Delta/Parquet query profiles.
   - Measure first metadata query separately from first data query.
   - Gate codecs by actual browser table requirements.
   - Re-run size, memory, startup, and query-latency baselines because Parquet changes the byte and
     memory owners.

12. Re-run correctness and performance after every reduction.
   - Host DataFusion POC tests.
   - WASM DataFusion smoke tests.
   - Delta Kernel dependency and handler tests.
   - Native parity tests once Parquet/Delta integration exists.
   - Browser startup/instantiate timing, scan latency, peak memory, cancellation, and query latency
     for the DataFusion engine SKU.

## Decision Gates

| Result | Product decision |
| --- | --- |
| DataFusion engine size remains in the current 5.5 MiB Brotli class | Continue; optimize and cache rather than replace DataFusion execution |
| DataFusion engine grows materially after Delta/Parquet scan integration | Split features, codecs, and datasource surfaces before reconsidering architecture |
| Delta Kernel core brings unwanted browser dependencies | Keep it isolated, disable features, or pause the Kernel path before replacing browser snapshot code |
| Delta Kernel cached handler model cannot resolve required snapshots | Add an explicit cache-miss retry trampoline or constrain the first Daxis DataFusion default open path to trusted descriptors / known-version open |
| DataFusion scan pushdown cannot use Axon's Delta/Parquet facts | Improve `AxonDeltaTableProvider` and `AxonParquetScanExec`; do not fall back to an Axon IR by default |
| DataFusion physical execution cannot meet memory/cancellation budgets | Add browser budget controls, fallback rules, and operator limits |
| DataFusion-native Parquet/browser object-store path proves simpler and faster | Consider replacing `AxonParquetScanExec` later |
| DataFusion-native Parquet/browser object-store path is fragile | Keep custom scan ExecutionPlan over Axon's browser stack |
| Size is dominated by removable modules without feature boundaries | Prepare upstream feature-splitting proposals |
| Size reductions break correctness or broad type coercion unexpectedly | Keep the functionality and document why it is required |

## Roadmap Gates

### 30-Day Gate

- Add a repeatable DataFusion POC size-report script.
- Measure umbrella-min, planner-only, planner-plus-optimizer, and planner-plus-physical-expr builds.
- Prove DataFusion compiles and executes in browser wasm, with Arrow IPC output.
- Decide that DataFusion physical execution is the browser engine target, while Axon owns table
  access and browser scan integration.

### 60-Day Gate

- Prove `delta-kernel-rs` core compiles for `wasm32-unknown-unknown` without default engine,
  `tokio`, `reqwest`, native TLS, cloud SDKs, or high-level `deltalake` in the browser Delta crate.
- Resolve a read-only Delta snapshot through `AxonBrowserKernelEngine` over prefetched cached log and
  checkpoint bytes.
- Register a Kernel-derived Delta descriptor as an `AxonDeltaTableProvider` in DataFusion.
- Execute DataFusion SQL over an `AxonParquetScanExec` backed by `wasm-parquet-engine` and
  `wasm-http-object-store`.
- Stream Arrow `RecordBatch` output through Arrow IPC from the worker.
- Run native/DataFusion parity tests for projection, filter, limit, order-by-limit, global
  aggregates, and grouped aggregates over Delta-derived Parquet descriptors.
- Measure browser engine size, startup, first metadata query, first real query, repeated query,
  peak memory, and cancellation.

### 90-Day Gate

- Decide whether `wasm-datafusion-poc` is renamed/promoted to `wasm-datafusion-engine`.
- Decide whether the custom scan ExecutionPlan remains the production path or a browser
  `object_store` adapter should be prioritized.
- Add CI budget reporting for the DataFusion browser engine module.
- Open upstream issues or PRs only for measured blockers.

## Open Questions

- What exact SQL grammar should the DataFusion browser engine accept in the first Daxis default profile?
- Which scalar functions are product requirements rather than convenience?
- Which Parquet encodings and compression codecs are mandatory for target datasets?
- Should query-plan caching happen only in the browser, or can some deployments compile/cache plans
  outside the browser?
- Which DataFusion built-in datasources should be excluded from the browser engine package?
- Can DataFusion expose an in-memory/session profile that does not include CSV/JSON datasources?
- Can Arrow default features be narrowed through DataFusion without forking?
- Which Delta Kernel Arrow feature, if any, should be enabled so the Kernel and DataFusion agree on
  Arrow versions?
- Should the first browser Delta Kernel open path require trusted descriptors, known table versions,
  or manifest-backed `_delta_log` listings?
- What is the acceptable browser compile/instantiate budget for the DataFusion engine SKU?

## Next Concrete Task

Continue the 60-day engine work at the Delta protocol boundary. The DataFusion facade, trusted
descriptor registration, custom scan, browser Parquet range-read path, pushdown traces, worker Arrow
IPC flow, budgets, cancellation shape, and optional size/perf gates are now in place under
`crates/wasm-datafusion-poc`. The remaining critical path is proving `wasm-delta-kernel-engine` with
the default engine disabled, implementing cached read-only snapshot resolution, and converting Kernel
scan output into the descriptor contract consumed by `AxonDeltaTableProvider`.

## Primary References

- Local DataFusion POC: `crates/wasm-datafusion-poc`
- Local browser runtime: `crates/wasm-query-runtime`
- Local worker/release posture: [Browser Lakehouse Release Handoff](./browser-lakehouse-release-handoff.md)
- DataFusion 52.4.0 feature flags: <https://docs.rs/crate/datafusion/52.4.0/features>
- DataFusion custom table-provider layering: <https://datafusion.apache.org/blog/2026/03/31/writing-table-providers/>
- DataFusion optimizer docs: <https://datafusion.apache.org/library-user-guide/query-optimizer.html>
- DataFusion proto compatibility note: <https://docs.rs/datafusion-proto/52.4.0/datafusion_proto/>
- DataFusion Substrait scope note: <https://docs.rs/datafusion-substrait/latest/datafusion_substrait/>
- Delta Kernel Rust API docs: <https://docs.rs/delta_kernel/latest/delta_kernel/>
- Delta Kernel Rust README: <https://github.com/delta-io/delta-kernel-rs>
- Delta Kernel scan APIs: <https://docs.rs/delta_kernel/latest/delta_kernel/scan/index.html>
- delta-rs Rust crate docs: <https://docs.rs/deltalake/latest/deltalake/>
- Arrow IPC streaming docs: <https://arrow.apache.org/docs/11.0/python/ipc.html>
- Arrow Acero streaming execution docs: <https://arrow.apache.org/docs/20.0/cpp/streaming_execution.html>
- DuckDB-Wasm deployment docs: <https://duckdb.org/docs/stable/clients/wasm/deploying_duckdb_wasm>
- MDN Web Workers API: <https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API>
- MDN WebAssembly `instantiateStreaming`: <https://developer.mozilla.org/en-US/docs/WebAssembly/Reference/JavaScript_interface/instantiateStreaming_static>
- MDN OPFS sync access handles: <https://developer.mozilla.org/en-US/docs/Web/API/FileSystemFileHandle/createSyncAccessHandle>
- web.dev cross-origin isolation guide: <https://web.dev/articles/cross-origin-isolation-guide>
