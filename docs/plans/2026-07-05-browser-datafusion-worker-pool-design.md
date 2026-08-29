# Browser DataFusion Worker Pool Design

**Date:** 2026-07-05

**Status:** Proposed

**Owner:** Runtime / engine team

## Summary

Axon should support multi-partition browser DataFusion execution by moving
parallelism above a single WASM DataFusion instance and into a browser worker
pool. A coordinator worker should split candidate files into shards, send each
shard to a dedicated Web Worker that owns its own WASM module and
`BrowserDataFusionSession`, then merge shard results into the single Arrow IPC
response expected by the current worker protocol.

This keeps the existing public browser worker interface stable while avoiding
the current `wasm-bindgen-test` limitation where a multi-partition DataFusion
plan can try to spawn Tokio tasks without a Tokio reactor. Browser Web Workers
become the parallel execution runtime; DataFusion remains the SQL planner and
per-shard execution engine.

## Problem

The current browser DataFusion path can prove real `wasm32-unknown-unknown`
execution for a representative single-partition UAT slice, including
descriptor-backed `AxonParquetScanExec`. Multi-partition descriptor-backed proof
is blocked because DataFusion may wrap multi-partition output in execution
nodes that internally spawn Tokio tasks. That works in native host tests, but
it is not a browser-native parallelism model.

The product need is still valid: real browser queries over Delta or Parquet
tables need to scan multiple files without falling back to native execution.
The browser-safe implementation should use the browser's concurrency primitive,
Web Workers, instead of trying to emulate Tokio-driven parallel execution inside
one WASM instance.

## Current State

- `crates/wasm-datafusion-poc` owns `WasmDataFusionEngine`,
  `DeltaTableDescriptor`, `AxonDeltaTableProvider`, `AxonParquetScanExec`,
  budgets, cancellation, scan metrics, and Arrow IPC output.
- `crates/wasm-datafusion-session` owns `BrowserDataFusionSession`, which wraps
  `WasmDataFusionEngine` for UI/runtime builds over browser-safe descriptors.
- `apps/axon-web/src/lib.rs` exposes `SandboxQuerySession` through
  `wasm-bindgen`, and `apps/axon-web/src/sandbox-query-worker.ts` exposes the
  worker command protocol used by the web SDK.
- `tests/uat/run_axon_engine_uat.sh` is the deterministic operator-facing UAT
  runner. It now includes host-engine corpus coverage and an actual wasm32 UAT
  slice.

## Goals

- Execute multi-file browser queries through real WASM DataFusion workers.
- Preserve the existing public browser worker command and response protocol for
  callers.
- Keep DataFusion responsible for SQL planning and shard-local execution.
- Keep Axon responsible for descriptor sharding, worker orchestration, budgets,
  cancellation, metrics, compatibility classification, and Arrow IPC delivery.
- Start with query shapes that have straightforward distributed semantics.
- Make unsupported distributed SQL shapes explicit and conservative.

## Non-Goals

- Do not claim arbitrary distributed SQL support.
- Do not replace `WasmDataFusionEngine` or `AxonParquetScanExec`.
- Do not require native fallback to prove browser success.
- Do not require SharedArrayBuffer, cross-origin isolation, threads, or
  `wasm32` atomics for the first version.
- Do not push DataFusion's internal multi-partition Tokio execution path into
  the browser as the primary parallelism model.
- Do not change the public worker command protocol in the first slice unless a
  required capability cannot be represented through existing fields.

## Proposed Architecture

Introduce a deep module named `BrowserDataFusionWorkerPool` at the app worker
seam. Its interface should hide worker creation, sharding, fanout, cancellation,
partial-result collection, merge semantics, and metric aggregation behind a
small execution surface.

The coordinator should live in the JavaScript/TypeScript worker layer first,
because browser worker orchestration is naturally a JS responsibility. Each
child worker should load the existing `axon_web_wasm` package and create its
own `SandboxQuerySession`. Each child session owns an independent
`BrowserDataFusionSession` and therefore an independent `WasmDataFusionEngine`.

The coordinator remains the only worker visible to SDK callers. From the SDK's
perspective, `open_delta_table`, `open_parquet_dataset`, `sql`, `inspect`, and
`dispose` continue to behave as one worker-backed session. Internally, the
coordinator can maintain a table descriptor cache and lazily open shard
descriptors in child workers only when a parallel query is eligible.

```
SDK / UI
  |
  v
Coordinator Worker
  |-- query eligibility and sharding
  |-- cancellation and budgets
  |-- result merge and metrics
  |
  +--> Child Worker 1: axon_web_wasm + BrowserDataFusionSession + shard A
  +--> Child Worker 2: axon_web_wasm + BrowserDataFusionSession + shard B
  +--> Child Worker N: axon_web_wasm + BrowserDataFusionSession + shard N
```

## Query Eligibility

The first worker-pool version should support only SQL shapes whose distributed
semantics are obvious and cheap to validate.

Supported in the first slice:

- Projection.
- Filtering.
- Boolean logic.
- Arithmetic expressions.
- `CASE` expressions.
- `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG`.
- Grouped aggregation where the grouping keys and aggregate expressions can be
  merged from partial results.

Potentially supported in the second slice:

- Global `ORDER BY ... LIMIT ...` by asking each worker for local top-k, then
  doing a coordinator-level k-way merge.
- `DISTINCT` by shard-local distinct plus coordinator-level deduplication, if
  output cardinality budgets are enforced.

Unsupported until separately designed:

- Joins across independently sharded tables.
- Window functions.
- Set operations beyond simple `UNION ALL`.
- Queries that reference multiple open tables unless all referenced tables have
  an explicit compatible sharding strategy.
- SQL shapes requiring global state that is not represented in a coordinator
  merge plan.

Unsupported shapes should route to the existing single-worker browser
DataFusion path if they are supported there. They should not silently route to
native fallback as proof of worker-pool success.

## Sharding Model

The coordinator should shard at the descriptor-file level, not the row-group
level, for the first version. File-level sharding matches the existing
`BrowserHttpSnapshotDescriptor` and `BrowserHttpParquetDatasetDescriptor`
contracts and avoids introducing new object-range ownership concepts.

Shard assignment should be deterministic:

1. Start from the query's prebootstrap candidate file set.
2. Preserve existing partition pruning and file-stat pruning behavior.
3. Sort candidate files by stable path.
4. Assign files to workers using a simple balancing policy.

The first balancing policy can be greedy by `size_bytes`: place the next largest
file into the currently smallest shard. If size metadata is missing or suspect,
fall back to round-robin by path.

Each child worker receives a descriptor containing only its assigned files. The
table name and SQL remain stable, so shard execution still goes through
`BrowserDataFusionSession -> WasmDataFusionEngine -> AxonParquetScanExec`.

## Execution Flow

1. The UI sends a normal `sql` command to the visible worker.
2. The coordinator validates browser-safe limits and parses query eligibility.
3. If the query is not worker-pool eligible, the coordinator runs the existing
   single-worker path.
4. If eligible, the coordinator computes candidate files and shard descriptors.
5. The coordinator opens the table on each child worker using the shard
   descriptor.
6. The coordinator rewrites the SQL only when partial aggregation or local
   top-k semantics require it.
7. Each child worker executes SQL through its own real WASM DataFusion session.
8. The coordinator decodes Arrow IPC metadata needed for merge.
9. The coordinator emits one final Arrow IPC stream plus normal worker response
   metadata.

## Merge Semantics

### Projection And Filter Queries

For queries without global ordering, grouping, distinct, or aggregation, the
coordinator can concatenate shard Arrow IPC batches into one output stream.
Output order must be documented as unspecified unless the SQL requests an
ordering.

### Aggregates Without Grouping

Each worker should run a partial aggregate query. The coordinator should merge:

- `COUNT`: sum counts.
- `SUM`: sum sums, preserving null behavior.
- `MIN`: min of non-null partial mins.
- `MAX`: max of non-null partial maxes.
- `AVG`: merge as `SUM(value)` plus `COUNT(value)`, then divide.

### Grouped Aggregates

Each worker should return one row per group key with partial aggregate state.
The coordinator groups rows by serialized group-key values and applies the same
merge rules as ungrouped aggregates.

The coordinator should preserve Arrow output types exactly for supported
queries. If type preservation is ambiguous, the query should be rejected from
worker-pool execution.

### Order And Limit

Global ordering is intentionally deferred from the first slice unless the
implementation includes a focused top-k merge module. When implemented:

- Rewrite each shard query to preserve `ORDER BY` and `LIMIT`.
- Request at least global limit plus offset from every shard.
- Merge sorted shard streams in the coordinator.
- Apply final offset and limit after merge.

## Budgets And Cancellation

Budgets must be enforced at both levels:

- Child workers enforce scan bytes, output IPC bytes, rows returned, and
  per-worker batch limits.
- The coordinator enforces global output IPC bytes, global rows returned,
  preview bytes, worker count, and aggregate child scan bytes.

Cancellation should be fanout-first:

1. The coordinator receives the existing cancel command.
2. It marks the coordinator query as cancelled.
3. It sends cancel to every child worker participating in the query.
4. It rejects late child responses for the cancelled query id.
5. It emits one terminal cancellation response to the caller.

Timeouts should behave similarly. A child timeout should cancel all sibling
workers for the same query unless the query has an explicit partial-results
mode. The first version should not support partial results.

## Metrics And Observability

The final response should preserve existing query metrics and add enough detail
to prove worker-pool behavior:

- `execution_target = BrowserWasm`.
- `worker_pool_enabled = true`.
- `worker_count`.
- `candidate_file_count`.
- `shard_file_counts`.
- `bytes_fetched` summed across workers.
- `rows_emitted` final output rows.
- `child_rows_emitted` before coordinator merge.
- `coordinator_merge_duration_ms`.
- `worker_query_duration_ms` min, max, and total.
- `fallback_reason = None` for successful worker-pool execution.

If the existing `QueryMetricsSummary` cannot carry these without weakening the
contract, add a structured browser DataFusion metrics extension rather than
overloading unrelated fields.

## Module Shape

Recommended external interface:

```ts
type BrowserDataFusionWorkerPoolOptions = {
  workerFactory: () => Worker;
  maxWorkers: number;
  minFilesForParallelism: number;
  maxOpenTablesPerWorker: number;
};

class BrowserDataFusionWorkerPool {
  openDeltaTable(name: string, descriptor: BrowserHttpSnapshotDescriptor): Promise<void>;
  openParquetDataset(name: string, descriptor: BrowserHttpParquetDatasetDescriptor): Promise<void>;
  sql(command: BrowserWorkerSqlCommand): Promise<BrowserWorkerResponseEnvelope>;
  cancel(command: BrowserWorkerCancelCommand): Promise<void>;
  dispose(name: string): Promise<void>;
  terminate(): void;
}
```

This keeps the module deep: callers do not know about shard descriptors,
partial SQL rewrites, child worker lifetimes, retry rules, or merge operators.
Tests can exercise the worker-pool behavior through the same interface that the
coordinator worker uses.

## Testing Strategy

### Unit Tests

- Query eligibility accepts first-slice supported SQL and rejects unsupported
  global shapes.
- Descriptor sharding is deterministic and preserves all file identity fields.
- Aggregate merge handles nulls, empty shards, mixed group keys, and numeric
  type preservation.
- Cancellation rejects late child responses.
- Budget aggregation fails closed when child totals exceed global budgets.

### Rust/WASM Tests

- Keep the existing actual wasm32 UAT slice for single-worker proof.
- Add a wasm32 worker-pool contract test only when the worker-pool bridge is
  exposed through a stable wasm-bindgen interface.

### Browser Tests

Use Playwright for the real proof:

- Start the app worker in a browser.
- Open a table with at least two files.
- Execute an eligible projection/filter query.
- Assert more than one child worker executed.
- Assert `fallback_reason = None`.
- Assert result parity against the single-worker DataFusion path.
- Execute a grouped aggregate query.
- Assert merged results match the host UAT corpus oracle.
- Execute an unsupported query shape and assert it uses the documented
  single-worker path or returns a structured unsupported-worker-pool reason.

### UAT Integration

Add a new row to `tests/uat/run_axon_engine_uat.sh`:

```text
actual browser worker-pool UAT query slice
```

This row should be separate from:

- host-engine UAT query corpus,
- actual wasm32 single-worker query slice,
- native oracle corpora,
- browser/native parity,
- performance smoke.

## Rollout Plan

### Slice 1: Coordinator And Multi-Worker Projection/Aggregate Proof

Implement the worker pool behind an opt-in flag. Support projection/filter and
mergeable aggregates only. Add Playwright proof for at least two workers and
result parity.

Exit gate:

- Browser proof shows `worker_count >= 2`.
- Query response remains `BrowserWasm`.
- `fallback_reason = None`.
- Results match single-worker/browser or native oracle.

### Slice 2: Worker-Pool Budgets, Cancellation, And Metrics

Harden global budget aggregation, child cancellation, late-response rejection,
timeouts, and worker-pool metrics.

Exit gate:

- Cancellation test proves all child workers stop.
- Budget tests prove global limits apply across workers.
- Metrics expose worker counts and shard file counts.

### Slice 3: Global Order/Limit Top-K

Add top-k planning and merge support for `ORDER BY` / `LIMIT` / `OFFSET` query
shapes.

Exit gate:

- Ordered results match native oracle across skewed shards.
- Offset is applied globally, not per shard.

### Slice 4: UAT And Performance Gate

Add the worker-pool UAT row and performance measurements for first query,
repeated query, worker startup, scan bytes, and merge time.

Exit gate:

- `bash tests/uat/run_axon_engine_uat.sh` includes and passes the worker-pool
  row.
- Performance report separates scan time, child execution time, merge time, and
  startup overhead.

## Risks

- **WASM package duplication:** each child worker may load its own WASM module.
  Keep worker count bounded and measure startup plus memory before enabling by
  default.
- **Merge correctness:** global SQL semantics are easy to get wrong. Keep the
  first slice limited to mergeable shapes.
- **Protocol drift:** adding worker-pool-only fields to the public protocol can
  leak implementation detail. Prefer internal metrics extensions and preserve
  current command shapes.
- **Cache duplication:** each child worker can fetch overlapping metadata.
  Shard descriptors should avoid overlap, and later slices can add shared
  metadata coordination if needed.
- **Fallback ambiguity:** worker-pool ineligibility must not look like product
  success. Metrics and structured reasons must make the executed path explicit.

## Open Questions

- Should worker-pool execution be enabled automatically by file count, by data
  size, or by an explicit runtime option first?
- Where should extended worker-pool metrics live: `QueryMetricsSummary` or a
  browser DataFusion-specific metrics envelope?
- Should child workers be long-lived per visible worker, or created per query
  until startup and memory are measured?
- Should the first implementation live entirely in `apps/axon-web`, or should
  reusable coordination contracts be moved into `browser-sdk` once the shape is
  proven?

## Recommendation

Start with Slice 1 as an opt-in browser-only proof. Do not change the Rust
single-worker DataFusion execution model to chase internal parallelism. Use the
browser worker pool as the parallel runtime, keep each child worker's execution
path honest through `BrowserDataFusionSession -> WasmDataFusionEngine ->
AxonParquetScanExec`, and only broaden SQL support when coordinator merge
semantics are tested against oracle results.
