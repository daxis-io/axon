# Browser DataFusion Size Audit

- Status: Draft
- Date: 2026-05-04
- Scope: measurement-driven audit for reducing an experimental Axon browser DataFusion WASM SKU
- Related:
  - [DataFusion WASM Compile Spike](./datafusion-wasm-compile-spike.md)
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)

## Summary

Axon's browser DataFusion hypothesis is narrower than general-purpose DataFusion:

> A browser DataFusion build for Axon probably does not need the full general-purpose DataFusion
> surface. If Axon defines the exact SQL, operator, and file-format profile it needs, then removes
> or feature-gates unused DataFusion functionality, it may materially reduce the WASM artifact while
> preserving performance for Axon's use case.

The current evidence supports investigating this, but not by deleting dependencies first. The first
step is to compare Axon's required browser SQL profile with measured retained WASM symbols and the
actual DataFusion feature boundaries.

This audit also carries the DataFusion-specific strategy decision: use DataFusion as a planning and
semantics substrate where measurements justify it, but keep the full umbrella runtime out of the
default browser core for now.

The likely product shape remains:

- default browser runtime: small custom/narrow engine
- lazy SQL compiler: DataFusion SQL/logical planning and selected optimizer rules, if its measured
  bundle is acceptable
- experimental SQL runtime: lazy-loaded full DataFusion WASM bundle for parity, benchmarks, and
  advanced-SQL experiments
- optimized DataFusion build: reduced feature set, `wasm-opt`, HTTP compression, and possibly
  upstream DataFusion feature splits

The strongest current recommendation is not to replace Axon's existing browser runtime with the full
umbrella `datafusion` runtime. Instead, treat DataFusion as the semantic/planning upstream and lower
the supported read-only SQL profile into Axon's compact browser execution plan. Full DataFusion stays
valuable as a conformance oracle and lazy experimental SKU, but it should not become an always-loaded
browser dependency without passing explicit size, startup, memory, and correctness gates.

## Decision Table

| Decision | Current verdict |
| --- | --- |
| Use DataFusion in the browser strategy | Go |
| Make full umbrella `datafusion` the default browser runtime core | No-go for now |
| Use narrow DataFusion crates for SQL planning and logical optimization | Go, behind measurement gates |
| Lower optimized logical plans into an Axon-owned browser execution IR | Recommended path |
| Keep full DataFusion WASM as a lazy advanced or fallback bundle | Conditional go |
| Make Substrait or DataFusion proto the hot-path browser IR | No-go for now; keep as optional diagnostics/interchange |

## Planner Feature Floor Decisions

| Variant | Lazy planner hard gate | Worth pursuing |
| --- | --- | --- |
| planner-only direct subcrates | Inside gate at 2,108,131 Brotli bytes | Yes; remains the preferred lazy SQL planning baseline. |
| planner plus optimizer floor | Inside gate at 2,945,395 Brotli bytes | Yes; optimizer rules are plausible for Axon logical-plan lowering. |
| planner plus physical-expr floor | Inside gate at 2,113,465 Brotli bytes | Conditional; useful as a compile and retention floor, but not enough to justify a DataFusion physical runtime. |
| planner plus physical-plan floor | Inside gate at 2,104,480 Brotli bytes | Conditional; keep measuring with real operators before treating this as runtime evidence. |

## Strategic Recommendation

Use a staged architecture:

```text
SQL text
  -> optional lazy DataFusion SQL/logical planner
  -> selected logical optimizer rules
  -> Axon lowering pass
  -> compact Axon browser execution plan
  -> wasm-query-runtime + wasm-parquet-engine + wasm-http-object-store
  -> Arrow IPC result stream
```

This keeps the hard browser-owned pieces in Axon:

- Delta snapshot facts and active file descriptors
- browser-safe HTTP range I/O
- Parquet metadata bootstrap and scan primitives
- query budgets, deterministic fallback, and browser metrics
- Arrow IPC result transport

DataFusion is still the right upstream to study for SQL semantics, type coercion, logical plan shape,
optimizer rules, and native parity. The measured POC shows the runtime is feasible, but the size and
cold-start costs are too high for the default SKU, especially because the current POC does not yet
include a Parquet-backed DataFusion table path.

The strongest boundary is the optimized logical plan. DataFusion's layering separates SQL planning,
logical expressions, logical optimization, physical planning, and streaming `RecordBatch` execution.
That maps well to Axon's current browser split: DataFusion can supply SQL semantics and selected
logical rewrites, while Delta snapshot resolution, Parquet/range I/O, query budgets, fallback, and
Arrow IPC output stay in Axon-owned crates.

## Axon Constraints

Axon's browser engine is not a generic database product. The default browser SKU already has a
repo-owned lakehouse path: `wasm-delta-snapshot` reconstructs browser-safe Delta snapshots,
`wasm-http-object-store` owns HTTP range reads, `wasm-parquet-engine` owns Parquet metadata and scan
primitives, and `wasm-query-runtime` owns query-shape analysis, pruning, fallback, metrics, and
Arrow IPC output.

That means Axon does not need a browser SQL engine to solve broad catalog management, generic file
listing, generic object-store integration, CSV/JSON datasources, or bring-your-own-source
ergonomics. The production question is narrower: can Axon reuse DataFusion's SQL planning semantics
without linking a general-purpose execution engine into the default browser core?

The V1 browser SQL envelope should remain read-only and bounded: projection, aliases, filters,
`ORDER BY`, `LIMIT`, global aggregates, grouped aggregates, Arrow input/output, and eventual
Parquet-backed table access. Joins, recursive CTEs, `DISTINCT`, `HAVING`, broad scalar-function
catalogs, window functions, DDL/DML, and table functions remain outside the default browser
execution envelope until product requirements and size budgets justify them.

Browser delivery also constrains the decision. The default path should run in a worker, use Arrow
IPC rather than row JSON for large results, load WASM through streaming instantiation where hosting
allows it, and treat single-threaded WASM as the product baseline. `SharedArrayBuffer`, shared WASM
memory, high-precision memory APIs, and threaded execution depend on cross-origin isolation, so they
are acceleration tiers rather than V1 assumptions. OPFS-backed persistent caches may become useful
for metadata, footers, and selected byte ranges, but the default engine should not require OPFS to
be correct.

## Candidate Architecture Matrix

| Candidate | Browser shape | Bundle outlook | Current recommendation |
| --- | --- | --- | --- |
| Full DataFusion runtime | Lazy worker module owns planning and execution | Poor to medium; current POC is 51.2 MiB raw after `wasm-opt` | Keep as prototype, benchmark, and conformance oracle |
| Narrow DataFusion subcrate runtime | Direct DataFusion subcrates plus selected physical execution | Unknown; must measure `physical-expr` and `physical-plan` separately | Measure seriously before committing |
| DataFusion planner plus Axon runtime | DataFusion emits logical plan; Axon lowers to compact execution plan | Best chance for a small default runtime; planner can be lazy | Primary production architecture candidate |
| Serialized plan boundary | DataFusion/server/lazy compiler emits Substrait or DataFusion proto | Good only with a restricted Axon consumer; full DataFusion serializers may be large | Spike after planner/runtime split is clearer |
| Hybrid staged runtime | Small default runtime, lazy planner, optional full DataFusion fallback | Best product flexibility | Recommended implementation path |

The repo already points toward the staged option: `crates/wasm-query-runtime` owns the constrained
execution envelope, `crates/wasm-delta-snapshot` owns browser-safe snapshot reconstruction, and the
DataFusion POC is isolated from the shipped worker/runtime dependency graph.

The hybrid path is the safest migration strategy. Supported queries should take the Axon IR fast
path. Unsupported queries can continue to use native fallback, and a fully lazy DataFusion worker can
remain available for developer conformance, benchmarks, or advanced SQL experiments. That keeps the
large runtime exceptional instead of making it the default cost of opening the browser product.

## Proposed Product Gates

The final budgets need product approval, but the size audit should use explicit working gates while
experiments run:

| Gate | Working target | Notes |
| --- | ---: | --- |
| Always-loaded browser query runtime Brotli | <= 2 MiB preferred, <= 3 MiB hard gate | Applies to the default narrow runtime, not the lazy DataFusion SKU |
| Lazy SQL planner/compiler Brotli | <= 1-2 MiB preferred, <= 5-6 MiB hard gate | Acceptable only if cached and not loaded for every page view |
| First tiny query after warm worker | <= 50 ms p50, <= 150 ms p95 | Current full DataFusion POC first query is about 78-80 ms |
| Repeated tiny query | <= 2 ms p50 | Current full DataFusion POC is about 0.6 ms median |
| Cold init plus first tiny query | Track separately | Current full DataFusion POC is roughly 160-170 ms combined |
| Peak browser working set | Bounded and streaming by default | Full global sort and high-cardinality group-by need explicit budgets or fallback |

These are audit gates, not a release promise. They exist to make future DataFusion feature-splitting
and Axon runtime experiments comparable.

## Current Baseline

DataFusion remains isolated from the shipped browser worker and default browser runtime, which still
report `browser_datafusion = false`. The workspace now has two browser WASM DataFusion experiments:
`crates/wasm-datafusion-poc`, a full umbrella-runtime POC that depends on `datafusion`, and
`crates/wasm-datafusion-planner-poc`, a planner-only POC that depends directly on
`datafusion-common`, `datafusion-expr`, and `datafusion-sql`.

The current POC proves:

- `datafusion = "=52.4.0"` compiles for `wasm32-unknown-unknown`.
- A synthetic Arrow `RecordBatch` can be registered through `datafusion::datasource::MemTable`.
- `datafusion::prelude::SessionContext` can execute SQL over that table.
- Results can be returned as Arrow IPC bytes through a `wasm-bindgen` export.
- `wasm-query-runtime` can opt into a compiler-boundary lowering spike through its
  `datafusion-planner-poc` feature without adding DataFusion crates to its default dependency
  graph. The spike uses direct DataFusion subcrates to plan SQL and summarize the supported
  projection, filter, limit, projected-output `ORDER BY`, global aggregate, and grouped aggregate
  shape into Axon-owned fields.
- Task 6 added `tests/conformance/browser-datafusion-lowering-corpus.json` as the differential SQL
  corpus for the lowering spike. It covers projection/filter/limit, projected-output `ORDER BY`,
  unsupported join rejection before browser runtime planning, `COUNT(*)`, grouped `COUNT(*)`, and
  grouped `SUM`/`MIN`/`MAX`.

The current POC does not yet prove:

- Delta active-file registration in DataFusion
- browser HTTP range reads feeding DataFusion directly
- Parquet-backed DataFusion table registration in WASM
- memory-budgeted DataFusion execution
- parity gates against the native DataFusion runtime
- execution of the `wasm-query-runtime` lowering output; joins and other broad SQL operators remain
  explicit unsupported-shape failures before browser runtime planning

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
| planner-only direct subcrates | 29,029,645 | 19,363,360 | 3,901,323 | 2,108,131 | Isolated `wasm-datafusion-planner-poc` using `datafusion-common`, `datafusion-expr`, and `datafusion-sql` directly, with exported `plan_sql_to_display` retaining the planner surface; `wasm-bindgen` output was 26,732,143 bytes. Passes the lazy planner/compiler hard Brotli gate, but is just over the 2 MiB preferred target. |
| planner plus optimizer floor | 42,300,914 | 28,444,112 | 5,735,339 | 2,945,395 | Adds optional `datafusion-optimizer` and exported `optimizer_surface_marker_wasm`, retaining the default logical optimizer rule list; `wasm-bindgen` output was 39,236,537 bytes. Inside the lazy planner/compiler hard Brotli gate and worth pursuing for Axon logical-plan lowering. |
| planner plus physical-expr floor | 29,153,447 | 19,417,295 | 3,916,000 | 2,113,465 | Adds optional `datafusion-physical-expr` and exported `physical_expr_surface_marker_wasm`, retaining a column physical expression marker; `wasm-bindgen` output was 26,845,560 bytes. Inside the hard gate, but this is only a tiny symbol floor, not evidence for broad physical expression execution. |
| planner plus physical-plan floor | 29,186,424 | 19,422,327 | 3,907,415 | 2,104,480 | Adds optional `datafusion-physical-plan` and exported `physical_plan_surface_marker_wasm`, retaining `PlaceholderRowExec`; `wasm-bindgen` output was 26,875,287 bytes. Inside the hard gate, but this remains a narrow execution-plan symbol floor rather than a real physical runtime measurement. The wasm compile required a target-only `uuid/js` feature unifier because `datafusion-physical-plan` pulls `datafusion-functions` default string functions, which enable `uuid/v4`. |

Interpretation:

- Browser compile/instantiate cost is still large even after `wasm-opt -Oz`.
- Brotli transfer cost is much smaller than raw cost, so lazy-loading is plausible.
- This is not suitable for Axon's default browser runtime SKU without an explicit opt-in.
- The optimizer floor is the first measured feature row that materially increases retained planner
  size, but it remains well inside the lazy planner hard gate.
- The physical expression and physical plan rows are lower bounds only. They prove those optional
  subcrate surfaces can compile for wasm and be retained through browser exports, but they do not
  measure a representative DataFusion physical runtime.

## Required Browser SQL Profile

This profile should be treated as the minimum investigation target, not as a final public contract.

| Capability | Browser DataFusion need | Current Axon status |
| --- | --- | --- |
| Read-only `SELECT` | Required | Custom runtime already requires `SELECT` only |
| Single table source | Required for V1 | Custom runtime requires one source relation |
| Projection and aliases | Required | Custom runtime supports passthrough projection and aliases |
| Filter predicates | Required | Custom runtime supports `AND`, comparison, `IN`, `IS NULL`, `IS NOT NULL` |
| Limit | Required | Custom runtime supports `LIMIT` |
| Order by output columns | Required | Custom runtime supports projected-output `ORDER BY` |
| Primitive and string columns | Required | Custom runtime handles integer, boolean, string, null scalar values |
| Arrow `RecordBatch` input | Required | POC proves in-memory `RecordBatch` input |
| Arrow IPC output | Required | POC and custom runtime use Arrow IPC output |
| Basic aggregates | Required | Custom runtime supports `COUNT`, `COUNT(*)`, `SUM`, integral `AVG`, `MIN`, `MAX`; DataFusion lowering now summarizes `COUNT(*)`, `COUNT(column)`, `SUM`, `MIN`, and `MAX` |
| Group by | Required | Custom runtime has grouped aggregate execution coverage; DataFusion lowering now records grouped aggregate columns |
| Parquet-backed table registration | Eventual | Not proven in DataFusion POC yet |
| Delta active-file registration | Eventual | Should come after Parquet-backed registration |

## Out Of Browser V1 Profile

These should be treated as removable or feature-gatable unless a future product requirement says
otherwise:

| Capability | Current audit stance |
| --- | --- |
| CSV datasource | Not needed for Delta/Parquet browser SKU |
| JSON datasource | Not needed for Delta/Parquet browser SKU |
| Avro datasource | Not needed |
| Broad scalar-function registry | Not needed by V1 unless individual functions are explicitly required |
| Regex/string/unicode-heavy function sets | Probably not needed by V1 |
| Crypto functions | Not needed |
| Window functions | Not needed by V1 |
| Table functions | Not needed by V1 |
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

The reusable center of gravity is narrower than the umbrella crate:

- `datafusion-sql` is the likely planner entry point for a DataFusion-backed SQL-to-logical-plan
  experiment.
- `datafusion-expr` owns `Expr` and `LogicalPlan` structures that can carry SQL semantics across a
  compiler boundary.
- `datafusion-optimizer` is valuable if Axon can apply selected logical rules before lowering to its
  own IR, but it must be measured because it pulls physical-expression machinery.
- `datafusion-physical-expr` is worth measuring only if direct batch expression evaluation saves
  enough implementation complexity to justify the extra bytes.
- `datafusion-physical-plan`, `datafusion-execution`, `datafusion-session`, DataFusion datasource
  crates, and broad function packages should not enter the default browser core until measurements
  prove they fit the budget.

## Profile Versus Retained Surface

| Retained surface | Needed by Axon profile? | Audit action |
| --- | --- | --- |
| `datafusion-sql` and `sqlparser` | Yes, but only SELECT profile is needed | Keep, investigate whether parser/planner can avoid broad statement operations |
| `datafusion-datasource-csv` | No | Candidate for upstream feature split or direct subcrate usage |
| `datafusion-datasource-json` | No | Candidate for upstream feature split or direct subcrate usage |
| `datafusion-datasource-arrow` | Maybe | Needed only if it is the cleanest path for Arrow table registration |
| `datafusion-functions` broad registry | Partially | Gate to required scalar functions, or avoid registering broad built-ins |
| aggregate functions | Yes, narrow subset | Gate to `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`; defer arrays/booleans unless required |
| window/table functions | No for V1 | Candidate for feature split |
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

The executor should remain a streaming `RecordBatch` pipeline with explicit budgets. Filter and
projection should run batch-by-batch, aggregates should keep partial state, and `ORDER BY ... LIMIT`
should prefer bounded `TopK` behavior where semantics allow it. Full global `ORDER BY` without a
limit, high-cardinality grouping, and unsupported expression/type combinations should have observable
fallback or failure modes rather than silently collecting unbounded data in browser memory.

The scan boundary is the highest-leverage performance point. Lowered plans should annotate required
columns, exact and inexact pushdown predicates, limit hints, row-group pruning predicates, dictionary
filter opportunities, and output ordering when known. The browser runtime should avoid decoding
Parquet columns or row groups that a later operator will discard.

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

4. Replace umbrella `datafusion` with direct subcrates where possible.
   - Start from the exact APIs needed by the POC: `SessionContext`, `MemTable`, SQL planning, and
     collect.
   - If the public API forces the umbrella crate, document that boundary as an upstream issue.

5. Measure planner/compiler-only DataFusion.
   - Build `datafusion-common`, `datafusion-expr`, `datafusion-sql`, and `datafusion-optimizer`
     without `datafusion-physical-plan`, datasource crates, or DataFusion Parquet.
   - Record raw, bindgen, `wasm-opt`, gzip, Brotli, init time, and `twiggy` output.
   - Treat this as the candidate lazy SQL compiler bundle, not the default runtime.

6. Measure reusable execution pieces.
   - Add `datafusion-physical-expr` alone and compare against an Axon expression evaluator.
   - Add `datafusion-physical-plan` separately to find the real floor for a narrow DataFusion
     runtime.
   - Stop pursuing the DataFusion physical runtime for browser V1 if the floor is close to the
     umbrella POC.

7. Build an Axon lowering spike.
   - Lower a DataFusion logical plan for projection, filter, limit, global aggregate, grouped
     aggregate, and sort/limit into `BrowserExecutionPlan` or its successor IR.
   - Compare outputs against native DataFusion and the existing custom runtime.

8. Test DataFusion feature splits locally.
   - Prototype optional gating for CSV/JSON datasource crates, window/table functions, broad scalar
     function registration, and Arrow default features.
   - Measure each split independently before combining them.

9. Add Parquet-backed profile only after the in-memory profile is understood.
   - Introduce the minimum Parquet registration path.
   - Gate codecs by actual browser table requirements.
   - Re-run size and performance baselines because Parquet will change the byte owners.

10. Re-run correctness and performance after every reduction.
   - Host DataFusion POC tests.
   - WASM DataFusion smoke tests.
   - Native parity tests once Parquet/Delta integration exists.
   - Browser startup/instantiate timing and query latency for the lazy-loaded SKU.

## Decision Gates

| Result | Product decision |
| --- | --- |
| Optimized DataFusion is small enough for default budgets | Consider an optional browser SQL SKU, not default replacement |
| Optimized DataFusion is large but compressed transfer is acceptable | Lazy-load only when DataFusion mode is requested |
| DataFusion remains large because needed planner/execution surface is broad | Keep custom runtime as default and DataFusion as advanced mode |
| Planner/compiler-only DataFusion is acceptable | Use DataFusion as a lazy SQL compiler into the Axon execution plan |
| Planner/compiler-only DataFusion is still too large | Keep SQL lowering on the current `sqlparser` path or compile plans outside the browser |
| DataFusion physical runtime is close to umbrella size | Do not use it for browser V1 execution |
| Axon lowering drifts semantically from DataFusion | Expand differential tests or narrow the browser SQL contract |
| Size is dominated by removable modules without feature boundaries | Prepare upstream feature-splitting proposals |
| Size reductions break correctness or broad type coercion unexpectedly | Keep the functionality and document why it is required |

## Roadmap Gates

### 30-Day Gate

- Add a repeatable DataFusion POC size-report script.
- Measure umbrella-min, planner-only, planner-plus-optimizer, and planner-plus-physical-expr builds.
- Prove SQL to logical plan to Axon lowering for projection, filter, limit, projected-output
  `ORDER BY`, and the required aggregate corpus.
- Publish a strict SQL/operator/type support matrix for the browser default SKU and experimental SQL
  SKU.

### 60-Day Gate

- Lower an optimized DataFusion logical plan into the existing Axon execution-plan model or a small
  successor IR.
- Run differential tests against native DataFusion or the native query runtime for the supported SQL
  subset.
- Decide whether `datafusion-physical-expr` is worth its measured size delta.
- Preserve current fallback behavior for unsupported SQL, unsupported Delta features, and browser
  runtime constraints.

### 90-Day Gate

- Decide whether the planner-only path becomes the default browser SQL frontend.
- Decide whether full DataFusion remains only a POC, becomes a lazy advanced module, or is dropped
  from browser product plans.
- Add CI budget reporting for the chosen browser query module.
- Open upstream issues or PRs only for measured blockers.

## Open Questions

- What exact SQL grammar should the experimental DataFusion SKU accept in browser V1?
- Which scalar functions are product requirements rather than convenience?
- Which Parquet encodings and compression codecs are mandatory for target datasets?
- Is browser-only SQL compilation required, or can some deployments compile/cache plans outside the
  browser?
- Is a full DataFusion lazy fallback acceptable as a product dependency, or should it remain a
  developer-only conformance tool?
- Can DataFusion expose an in-memory/session profile that does not include CSV/JSON datasources?
- Can Arrow default features be narrowed through DataFusion without forking?
- What is the acceptable browser compile/instantiate budget for a lazy-loaded SQL SKU?

## Next Concrete Task

Use `tests/perf/report_datafusion_wasm_size.sh` to emit the repeatable `wasm-datafusion-poc`
artifact table and profiler summaries in one command. Generated files live under package-scoped
directories in `target/df-size`, such as `target/df-size/wasm-datafusion-poc`, and should remain
uncommitted.

After that script exists, run the first three matrix rows in order:

1. current umbrella POC with current release profile
2. planner/compiler-only subcrate build
3. `datafusion-physical-expr` and `datafusion-physical-plan` size floors

Those measurements decide whether the next implementation task is a DataFusion-to-Axon lowering
spike or an upstream feature-splitting spike.

## Primary References

- Local DataFusion POC: `crates/wasm-datafusion-poc`
- Local browser runtime: `crates/wasm-query-runtime`
- Local worker/release posture: [Browser Lakehouse Release Handoff](./browser-lakehouse-release-handoff.md)
- DataFusion 52.4.0 feature flags: <https://docs.rs/crate/datafusion/52.4.0/features>
- DataFusion custom table-provider layering: <https://datafusion.apache.org/blog/2026/03/31/writing-table-providers/>
- DataFusion optimizer docs: <https://datafusion.apache.org/library-user-guide/query-optimizer.html>
- DataFusion proto compatibility note: <https://docs.rs/datafusion-proto/52.4.0/datafusion_proto/>
- DataFusion Substrait scope note: <https://docs.rs/datafusion-substrait/latest/datafusion_substrait/>
- Arrow IPC streaming docs: <https://arrow.apache.org/docs/11.0/python/ipc.html>
- Arrow Acero streaming execution docs: <https://arrow.apache.org/docs/20.0/cpp/streaming_execution.html>
- DuckDB-Wasm deployment docs: <https://duckdb.org/docs/stable/clients/wasm/deploying_duckdb_wasm>
- MDN Web Workers API: <https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API>
- MDN WebAssembly `instantiateStreaming`: <https://developer.mozilla.org/en-US/docs/WebAssembly/Reference/JavaScript_interface/instantiateStreaming_static>
- MDN OPFS sync access handles: <https://developer.mozilla.org/en-US/docs/Web/API/FileSystemFileHandle/createSyncAccessHandle>
- web.dev cross-origin isolation guide: <https://web.dev/articles/cross-origin-isolation-guide>
