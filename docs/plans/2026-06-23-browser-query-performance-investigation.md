# Browser Query Performance Investigation

**Date:** 2026-06-23

**Goal:** Capture the browser Delta and Parquet performance investigation as a step-by-step reference. Each numbered area can become its own implementation slice, test plan, or design review.

**Scope:** Browser-side Delta snapshot resolution, public object-storage range reads, Parquet scan pruning, DataFusion custom scan integration, Arrow IPC output, and startup/runtime loading in `apps/axon-web` and the Rust WASM crates.

**Related framing:**

- [Axon workbench and query engine architecture](../program/axon-workbench-architecture.md)
- [Provider and host integration model](../program/provider-model.md)
- [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)
- [ADR-0010: Pluggable Catalog Providers And Table Read Resolution Seams](../adr/ADR-0010-pluggable-catalog-providers.md)

**Read-only status:** This investigation did not edit runtime code. The checkout had existing local changes in:

- `crates/wasm-datafusion-session/Cargo.toml`
- `crates/wasm-datafusion-session/tests/parquet_dataset.rs`
- `crates/wasm-parquet-engine/src/lib.rs`
- `crates/wasm-query-runtime/src/lib.rs`

Keep those files as user-owned state unless a later implementation plan claims them.

---

## Architecture Alignment

This plan is part of Axon's engine/runtime backlog. It should stay inside the boundary defined by ADR-0009 and ADR-0010:

- Axon owns browser/native execution performance: descriptor validation, browser-safe byte-range reads, Parquet/Delta scan planning, pruning, cache behavior, DataFusion-backed execution, Arrow IPC result transport, runtime budgets, metrics, and fallback taxonomy.
- E1 `CatalogProvider` remains discovery-only. It returns navigation nodes, metadata, refs, and capability facts. It must not mint browser descriptors, resolve grants, inspect table bytes, or call `openDeltaTable()`.
- E9 `DataAccessResolver` owns table read resolution. It returns `descriptor`, `read_access_plan`, `fallback`, or `blocked`.
- `ExecutionProvider` owns query execution and preview. Only openable E9 outcomes may reach `openDeltaTable()`.
- Durable catalog/navigation state may store non-secret locators, refs, display metadata, and refresh hints. It must not persist read resolutions, signed URLs, grants, or openable descriptors.

Performance work can cache resolved descriptors, access envelopes, and footer metadata in runtime/session scope when identity and expiry rules make that safe. That cache is an execution optimization, not durable catalog state.

---

## Current Status

Landed on `origin/main` before this alignment update:

- `perf(range): instrument duplicate descriptor/footer/range reads`
- `perf(range): preserve object identity in scans`
- `perf(range): share parquet metadata cache`

The next runtime slice is DataFusion prebootstrap pruning from partition values and Delta stats. After that, descriptor carryover should target the E9 read-resolution and query-session runtime cache, not the durable E1 catalog registry.

---

## Executive Ranking

1. **DataFusion prebootstrap pruning** should come next. Shared range/footer caching has landed; pruning now needs to avoid fetching candidate-file footers when partition values or Delta stats can decide the file set first.
2. **E9/session-scoped descriptor carryover** should follow. Public object-storage connect already resolves and preflights enough state to avoid repeating log reconstruction on the first query, but that state belongs in read-resolution/runtime cache, not durable catalog state.
3. **Arrow IPC streaming and default output budgets** should follow. The current DataFusion path streams internally, then materializes one IPC buffer across the WASM boundary.
4. **Runtime startup lazy loading** should remove WASM and worker creation from the first catalog render.
5. **DataFusion trace and scan clone cleanup** should come after the network and output-path fixes unless profiling proves trace cloning dominates a target workload.

Run this investigation as six separate workstreams. Share metrics and fixtures across them, but keep changes small enough that each workstream can prove a specific latency, memory, or byte-read improvement.

---

## Baseline Evidence

Focused tests passed during the investigation:

```bash
cargo test -p wasm-http-object-store --test extent_cache --locked
cargo test -p wasm-datafusion-poc --test scan_pushdown --test custom_scan_exec --locked
```

Observed local artifact sizes:

```text
apps/axon-web/dist/assets/axon_web_wasm_bg-B7VBuHgi.wasm       86M
apps/axon-web/src/wasm/axon_web_wasm_bg.wasm                   86M
target/wasm32-unknown-unknown/release/axon_web_wasm.wasm       93M
```

These artifact sizes came from existing local build outputs. Rebuild before treating them as release evidence for a changed branch.

---

## Shared Measurement Contract

Add counters before changing behavior. Each workstream should be able to answer these questions from logs, test probes, or query metrics:

- How many Delta log objects did the browser list or read?
- How many Parquet files did the browser inspect before SQL pruning?
- How many footer/trailer ranges did it fetch?
- How many exact duplicate ranges did it request?
- How many bytes came from footer, data pages, checkpoint/log materialization, and Arrow IPC output?
- How many rows and bytes crossed the worker boundary?
- Did the browser instantiate WASM before a user action required query execution?

Preferred metric fields:

- `delta_log_manifest_list_ms`
- `snapshot_resolve_ms`
- `descriptor_cache_hit`
- `footer_cache_hit`
- `range_cache_hit`
- `duplicate_range_reads`
- `prebootstrap_files_pruned`
- `footer_reads_avoided`
- `row_groups_skipped`
- `arrow_ipc_byte_length`
- `worker_created_before_query`
- `wasm_loaded_before_query`

---

## Area 1: HTTP Range-Read Planning

### Current State

Public object-storage connect resolves a descriptor and preflights one active Parquet file in the UI flow:

- `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- `apps/axon-web/src/services/object-storage.ts`

The connected catalog stores summary state, not an openable read resolution. First query builds a browser session from the table URI and resolves the descriptor in `apps/axon-web/src/services/query.ts`.

Delta log materialization reads `_delta_log` objects through `HttpRangeReader`. Parquet footer inspection reads a trailer range, then a footer payload range in `crates/wasm-parquet-engine/src/lib.rs`.

The active scan path uses `HttpRangeAsyncFileReader`. Each `get_bytes` call becomes one bounded HTTP range request. The richer extent cache lives in `crates/wasm-http-object-store/src/lib.rs`, but the Parquet/DataFusion scan path does not use it as the shared read layer.

### Main Gaps

- Range instrumentation, object identity propagation, and shared footer metadata reuse have landed. Remaining work should preserve those metrics and avoid weakening identity checks.
- The existing cache coalesces adjacent or overlapping extents, but it does not merge nearby small-gap reads.
- Public connect preflight only checks the first active file, and query execution does not consume that preflight result through the E9/runtime cache yet.

### Implementation Slices

1. Preserve `object_etag`, object size, and canonical resource identity from snapshot bootstrap into DataFusion scan targets. **Status: landed.**
2. Put Parquet range reads behind the existing object-store extent cache, or extract a shared range-cache interface used by both object-store reads and Parquet scan reads. **Status: partially addressed by session-scoped footer metadata reuse; small-gap range coalescing remains open.**
3. Add a Parquet metadata cache keyed by canonical object id plus immutable identity such as `ETag` and size. Cache trailer bytes, footer bytes, and decoded row-group metadata. **Status: landed for footer metadata reuse.**
4. Add small-gap coalescing for planned Parquet reads. Track coalesced read amplification so the browser does not fetch large gaps to save a small request count.
5. Carry connect-time descriptor and first-file preflight identity into the E9 read-resolution and query-session runtime cache, with expiration and identity refresh rules. Do not persist read resolutions or openable descriptors in durable catalog state.

### Correctness Constraints

- Cache keys need strong object identity: `ETag`, generation or hash, and size where available.
- Signed URLs cannot serve as the only cache key. They rotate and may include sensitive query parameters.
- Range reads need strict validation: `206`, valid `Content-Range`, expected body length, and consistent total size.
- Browser CORS must expose `Content-Length`, `Content-Range`, `Accept-Ranges`, and `ETag`.
- The browser should send `Range` and `If-Range` only when the provider allows them.
- Missing `ETag` should disable persistent identity reuse or emit an explicit degraded-mode metric.

### Validation Plan

- Add a failing-first Parquet test: metadata read followed by scan should not fetch the same trailer/footer ranges after caching lands.
- Add a DataFusion session test: open table then run SQL should not repeat bootstrap footer ranges.
- Add a test proving bootstrapped `ETag` reaches scan targets and scan requests send `If-Range`.
- Extend object-storage mocks to expose `ETag`. Add a negative case for missing or hidden `ETag`.
- Add connect-flow coverage proving connect plus first query does not redo descriptor resolution or preflight unless identity or TTL rules require refresh.

### Exit Criteria

- Metrics show duplicate footer/trailer ranges drop to zero for open-plus-query on the same table.
- Scan requests carry identity headers when the provider exposes identity.
- Tests cover `ETag` present, `ETag` missing, object identity drift, and signed URL refresh.

---

## Area 2: Parquet Scan Pruning

### Current State

Delta metadata carries partition values and stats into the browser descriptor through:

- `crates/query-contract/src/lib.rs`
- `crates/wasm-delta-snapshot/src/lib.rs`

The legacy runtime can prune files before opening Parquet metadata. The DataFusion browser session currently bootstraps Parquet metadata for all active files before registering the table in `crates/wasm-datafusion-session/src/lib.rs`.

After registration, `crates/wasm-datafusion-poc/src/lib.rs` prunes planned files and row groups. Row-group pruning happens after the engine has opened Parquet metadata in `crates/wasm-parquet-engine/src/lib.rs`.

### Existing Pruning

- Partition pruning handles equality, non-null `IN`, `IS NULL`, `AND`, and same-column `OR` in the DataFusion path.
- Legacy runtime also supports partition `IS NOT NULL`.
- Delta stats pruning handles integer min/max.
- Row-group pruning handles one integer comparison predicate.
- DataFusion marks stats and row-group pruning as inexact, so residual filters remain above the scan.

### Main Gaps

- DataFusion session opens all active file footers before SQL pruning can use partition values or Delta stats.
- DataFusion partition pruning lacks `IS NOT NULL`.
- Partition pruning stringifies literals and uses limited `PartitionColumnType` normalization. Mixed typed and string partition values can miss pruning.
- File and row-group stats do not use null counts for `IS NULL` and `IS NOT NULL`.
- Stats pruning does not cover string, bool, float, decimal, date, timestamp, `BETWEEN`, `NOT IN`, or compound compatible predicates.
- Row-group pruning uses the first compatible integer predicate rather than composing ranges.

### Implementation Slices

1. Move DataFusion session toward prebootstrap pruning. Apply partition and Delta stats before `bootstrap_snapshot_metadata`.
2. Fetch Parquet footers only for candidate files that survive prebootstrap pruning.
3. Feed bootstrapped Parquet file stats into DataFusion provider-level pruning.
4. Normalize partition pruning across legacy and DataFusion paths, including `IS NOT NULL`.
5. Add null-count pruning for Delta stats, Parquet file stats, and row-group stats.
6. Compose multiple compatible integer predicates into one range constraint.

### Correctness Constraints

- Preserve residual filters for any lossy or inexact pruning.
- Fail open when stats are missing, malformed, or type-incompatible.
- Fail open when prebootstrap pruning produces zero candidates until DataFusion can register the
  table from a trusted schema source independent of candidate Parquet footers. Schema-only
  zero-candidate pruning belongs in a later slice gated on Delta metadata or a previously validated
  cached schema.
- Treat mixed typed/string partitions as a correctness problem, not only a performance miss.
- Keep Delta stats pruning and Parquet footer stats pruning separate in metrics.

### Validation Plan

- Add a DataFusion-session regression where Delta stats make a file decisive and the browser does not request that file URL before query execution.
- Add parity tests for partition `IS NOT NULL`, null `IN`, typed integer/string partitions, boolean partitions, casts, and mixed-case partition columns.
- Add fixtures with missing stats, mixed stats completeness, string/bool/nullCount stats, compound ranges, and row-group byte-range proof.
- Split metrics into files pruned before bootstrap, footer reads avoided, files pruned after footer stats, row groups skipped, and data bytes avoided.

### Exit Criteria

- A Delta-stats-decisive predicate avoids footer reads for pruned files in the DataFusion path.
- Legacy and DataFusion partition pruning agree for covered predicates.
- Residual filter tests prove correctness when pruning remains inexact.

---

## Area 3: Delta Snapshot Bootstrap

### Current State

`apps/axon-web/src/services/query.ts` keeps one module-level session per `QueryTableSource`. Same-source queries reuse the session and `ensureTable` opens the Delta table once per session.

The worker keeps a singleton `SandboxQuerySession`; `open_delta_table` and `sql` run as separate commands in `apps/axon-web/src/sandbox-query-worker.ts`.

Rust open-time bootstrap materializes the descriptor, fetches Parquet metadata for all active files, derives a DataFusion schema, registers the table, and stores the bootstrapped snapshot in `crates/wasm-datafusion-session/src/lib.rs`.

The Delta snapshot resolver in `crates/wasm-delta-snapshot/src/lib.rs` lists log paths, selects checkpoints, reads checkpoint/log bytes, replays actions, and derives schema/protocol/capability state per call.

### Main Gaps

- Public object-storage connect resolves the descriptor and preflights the first active file, but catalog storage keeps summary fields. First query rebuilds from the table URI.
- Local Delta reads log facts in TypeScript, then invokes the WASM resolver, which parses log/checkpoint state again.
- `openDeltaTable` caches only table URI and snapshot version at the SDK boundary, not the parsed descriptor or manifest state.
- Manifest snapshot resolution passes `snapshot_version: None`, so it resolves latest for that path.
- Runtime-limit overrides can re-register the DataFusion table and rederive schema even though they reuse the bootstrapped snapshot.

### Implementation Slices

1. Carry the object-store descriptor produced during connect/preflight through E9 `DataAccessResolver` and query-session runtime cache. Do not store openable descriptors, signed URLs, grants, or read resolutions in durable E1 catalog/navigation state.
2. Cache parsed snapshot descriptors in runtime/session scope by source kind, provider, normalized table URI, snapshot version, and log-manifest fingerprint.
3. Split immutable resolved snapshot structure from expiring access envelopes. Refresh access URLs without redoing schema/protocol/capability work when the snapshot identity matches.
4. Collapse Local Delta duplicate log parsing by letting the resolver return the discovery/protocol/schema facts that `readLocalLogFacts` needs.
5. Make pinned snapshot an explicit cache dimension. Do not satisfy pinned version `N` from latest version `N+1`.
6. Cache derived `DeltaTableDescriptor` and schema across budget-driven re-registration, or move budget state out of provider registration.

### Correctness Constraints

- Cache identity needs checkpoint parts, sidecars, replay JSON identities, size, `ETag` or last-modified, and local file metadata where applicable.
- Access grants need their own key: expiry, grant identity, access mode, and refresh policy.
- Durable state may store only non-secret navigation data, locators, and refs. Read resolutions are short-lived and runtime-scoped.
- Same-snapshot refresh must reject drift in table URI, snapshot version, active file identity, schema, and capabilities.
- Explicit latest refresh may reopen the table when version or descriptor material changes.

### Validation Plan

- Add counters around `buildPublicDeltaLogManifest`, `resolve_delta_snapshot_from_manifest`, `bootstrap_snapshot_metadata`, and `bootstrap_file_metadata`.
- Add tests for object-store connect then query reusing descriptor state.
- Add tests for preflight footer reuse and two SQL queries after one open avoiding bootstrap.
- Add tests for runtime-limit query avoiding footer refetch.
- Add tests for pinned version `N` versus latest version `N+1`.
- Add drift tests for same-snapshot refresh.

### Exit Criteria

- Connect plus first query does not repeat Delta log listing and replay for unchanged object-store inputs when the E9/runtime cache still has a valid read resolution.
- Pinned and latest refresh semantics remain explicit.
- Session telemetry distinguishes descriptor cache hits, session reuse, and opened-table reuse.

---

## Area 4: WASM Memory And Arrow IPC Output

### Current State

The shipped app worker uses `BrowserDataFusionSession`. UI queries request 500 visible rows plus a sentinel via `queryResultPageRequest`, and Rust rewrites SQL with `LIMIT/OFFSET`.

The DataFusion scan path streams internally. `AxonParquetScanExec` returns a `SendableRecordBatchStream`, and Parquet uses `ParquetRecordBatchStreamBuilder`.

The JS/WASM boundary still materializes one Arrow IPC buffer. `execute_stream` writes into `BudgetedArrowIpcBuffer { bytes: Vec<u8> }`, and the worker posts one `Uint8Array` after `session.sql()` returns.

The bridge copies IPC bytes with `.to_vec()` and `Uint8Array::from(bytes)`. Preview decoding then walks the full IPC stream to compute row count, even after it captures the preview rows.

### Main Gaps

- Internal batch streaming stops at the WASM boundary.
- The bridge copies the full IPC output before sending it to JS.
- Preview generation decodes completed IPC instead of consuming batches during encoding.
- Direct SDK or worker callers can request unpaged results unless they set `result_page` or runtime budgets.
- Default browser runtime config has no execution budget.
- UI auto-load stops at 10,000 rows, but manual loading can keep appending pages into one array.
- Legacy row-map runtime still has full-result materialization paths.

### Implementation Slices

1. Add chunked Arrow IPC worker delivery with transferable chunks or batch messages.
2. Remove the `.to_vec()` and `Uint8Array::from` double-copy by transferring or consuming an owned output buffer.
3. Build preview rows while encoding DataFusion batches.
4. Set default app query budgets for IPC bytes, result rows, scan bytes, and preview string bytes.
5. Add a hard loaded-row or loaded-byte cap in UI state.
6. Quarantine the legacy row-map runtime from production query paths, or wrap it with strict budgets.

### Correctness Constraints

- Preserve Arrow IPC stream framing and schema delivery across chunks.
- Surface partial-output and budget errors as structured query errors.
- Keep result-page SQL rewriting before expensive execution for UI queries.
- Keep SDK behavior explicit: require `result_page` or apply a default browser-safe budget.

### Validation Plan

- Add browser perf probes for `started`, `executing`, `arrow_ipc_ready`, and `finished`.
- Record IPC byte length, preview rows, bytes fetched, rows emitted, elapsed time, and browser memory where available.
- Add a wide-string fixture that returns 501 rows with large UTF-8 cells.
- Add SDK-level regression for unpaged direct worker queries.
- Run existing budget tests in `wasm-datafusion-poc`.

### Exit Criteria

- Worker can emit output before the full result IPC buffer exists.
- Peak memory drops for wide/string-heavy page queries.
- Default app and SDK paths enforce output limits without relying on UI helpers.

---

## Area 5: DataFusion Custom Scan Integration

### Current State

`AxonDeltaTableProvider::scan` receives projection, filters, and limit from DataFusion and creates `AxonParquetScanExec` in `crates/wasm-datafusion-poc/src/lib.rs`.

Projection pushdown works through projected schema construction, in-memory `RecordBatch::project`, and Parquet `ProjectionMask`.

Filter pushdown is conservative:

- Exact partition predicates report exact pushdown.
- File-stats and row-group pruning report inexact pushdown.
- Unsupported filters stay above the scan.

Limit pushdown happens only when no inexact filters remain. Ordering does not push down; DataFusion keeps sort and top-k operators above the custom scan.

### Main Gaps

- Exact-only partition pushdown needs plan-shape tests proving no residual `FilterExec` remains above `AxonParquetScanExec`.
- Mixed predicates can re-evaluate exact partition conjuncts above the scan. Example: `partition_col = X AND value > 10` can use the partition part for pruning while DataFusion keeps the full filter above scan.
- Row-group pruning uses one integer predicate.
- `AxonParquetScanTrace` stores planned file paths, filter strings, and projected columns. The engine clones that trace for budget checks, metrics, and display formatting.
- The scan path clones descriptor and file metadata into scan targets.

### Implementation Slices

1. Split lightweight metrics from debug traces. Keep counters in a small struct and make planned file paths explain-only or sampled.
2. Store `DeltaTableDescriptor` and active file metadata behind `Arc`. Pass planned indices instead of cloning file metadata where practical.
3. Add plan-shape tests for exact partition predicates.
4. Add a mixed-predicate residual test. If DataFusion cannot express partial exact pushdown through the current API, document the duplicate evaluation.
5. Extend row-group pruning to compose multiple compatible predicates.

### Correctness Constraints

- Do not mark stats or row-group pruning exact unless the scan proves the predicate without residual evaluation.
- Keep DataFusion 53 API compatibility for `ExecutionPlan::properties()`.
- Preserve explain output, but keep full debug traces out of hot metrics paths.

### Validation Plan

- Keep running:

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown --test custom_scan_exec --locked
```

- Add plan-shape tests that inspect `FilterExec` placement.
- Add trace-size and clone-count probes for large file lists.
- Add row-group compound predicate tests.

### Exit Criteria

- Exact partition filters avoid residual filters above the custom scan where DataFusion supports it.
- Mixed predicates keep only required residual work or document the API limit.
- Metrics collection no longer clones large planned path lists on the hot path.

---

## Area 6: Bundle And Runtime Startup

### Current State

`apps/axon-web/index.html` loads one Vite entry. `apps/axon-web/src/editor/main.tsx` statically imports both `App` and `ConnectPage`. The default route renders `App`, which imports catalog, query, local Delta, and connect modal code.

On initial mount, `App` calls `loadCatalog(querySource)`. `loadCatalog` calls `getSession()`. For the sample manifest path, `buildSession()` calls `ensureWasm()`, fetches `/fixtures/prod-like/delta-log-manifest.json`, and invokes `resolve_delta_snapshot_from_manifest`. Under ADR-0010, that initial catalog path should become E1 discovery-only and should not force E9 read resolution or `ExecutionProvider` startup.

`createAxonBrowserClient()` creates the worker during session build. The worker imports the same generated WASM glue, then calls `init()` when the first open/query command needs a `SandboxQuerySession`.

Current local artifacts show one large WASM asset around 86 MiB in the app output.

### Main Gaps

- Initial catalog render can load the WASM binary before the user runs a query.
- The browser can instantiate WASM on the main thread for catalog work, then instantiate again in the worker for query work.
- Worker construction happens during session build, not first open/query send.
- Route splitting is absent; `/connect` pays for the editor app module graph.
- The production default catalog uses sample fixture metadata and a fixture manifest path.
- `public/designs/**` and old fixture paths land in `dist` because Vite copies `public/`.

### Implementation Slices

1. Stop loading WASM for initial catalog render. Render E1 discovery/catalog metadata first, and resolve snapshots only when E9 read resolution or explicit refresh needs an openable descriptor.
2. Lazily create the worker inside the SDK client's `send()` path or `ensureTable()`.
3. Move manifest snapshot resolution into the worker, or split metadata/preflight into a small WASM artifact.
4. Add route and modal splitting with dynamic imports for `App`, `ConnectPage`, `ConnectModal`, query services, and WASM services.
5. Move `public/designs/**` and unused old fixtures out of production `public`, or gate them behind a dev-only artifact path.
6. Add artifact-size and network-waterfall gates after `npm run build`.

### Correctness Constraints

- Startup changes must keep catalog metadata visible before query runtime loads.
- Query execution must still converge on `TableRef -> DataAccessResolver -> descriptor/read_access_plan -> ExecutionProvider -> openDeltaTable() -> sql()`.
- Route splitting must preserve worker URL construction under Vite.
- Dev/demo fixtures should not hide production startup costs.

### Validation Plan

Run after implementation:

```bash
cd apps/axon-web
npm run build
find dist/assets -maxdepth 1 -type f -exec stat -f '%z %N' {} + | sort -nr
gzip -c dist/assets/*.wasm | wc -c
brotli -c dist/assets/*.wasm | wc -c
npm run dev:server -- --port 5173 --strictPort
```

Then profile `/` and `/connect` in browser network/performance tools:

- The default catalog view should not fetch or instantiate WASM before query execution or explicit refresh.
- `/connect` should not load the editor query graph unless the route needs it.
- Worker JS should load only when the app opens or queries a table.

### Exit Criteria

- Initial render avoids the 86 MiB WASM fetch.
- Worker construction waits until first open/query action.
- Build artifacts and network waterfall have size gates in CI or release-gate scripts.

---

## Recommended Implementation Sequence

### Step 0: Add Baseline Instrumentation

Add metrics and tests that expose duplicate descriptor, footer, and data-range reads. Do this before optimizing so every later slice can prove its effect.

Useful first tests:

- Connect plus first query should count descriptor resolution once.
- Metadata read plus scan should not repeat trailer/footer reads after caching lands.
- DataFusion session open plus SQL should separate bootstrap footer reads from scan data reads.

### Step 1: Preserve Object Identity And Share Footer Cache

Preserve `ETag` and size from snapshot/bootstrap into scan targets. Add the shared Parquet metadata cache and route inspect, preflight, bootstrap, and scan through it.

This step supports Areas 1, 2, and 3.

### Step 2: Add DataFusion Prebootstrap Pruning

Use partition values and Delta stats before `bootstrap_snapshot_metadata`. Fetch footers only for candidate files.

This step addresses the main network waste in Area 2.

### Step 3: Carry Descriptors From Connect To Query

Carry the resolved object-store descriptor and its identity envelope through E9 read resolution and query-session runtime cache. Keep access URL refresh separate from immutable descriptor structure. Do not persist read resolutions or openable descriptors in durable E1 catalog/navigation state.

This step addresses Area 3 and strengthens Area 1.

### Step 4: Stream IPC And Set Browser Budgets

Chunk IPC across the worker boundary, remove the full-buffer copy where possible, build preview during encoding, and enforce browser defaults for result output.

This step addresses Area 4.

### Step 5: Lazy-Load Startup Paths

Remove WASM and worker construction from initial catalog render. Split routes and query runtime imports.

This step addresses Area 6.

### Step 6: Reduce Custom Scan Trace Cost

Split metrics from debug traces, reduce descriptor/file clones, and add plan-shape tests for exact and mixed predicates.

This step addresses Area 5.

---

## Suggested Ticket Breakdown

1. `perf(range): instrument duplicate descriptor/footer/range reads`
2. `perf(range): preserve object identity through DataFusion scan targets`
3. `perf(range): share Parquet footer metadata across preflight/bootstrap/scan`
4. `perf(pruning): prebootstrap prune DataFusion active files from Delta stats`
5. `perf(pruning): normalize partition pruning across DataFusion and legacy runtime`
6. `perf(snapshot): carry public object-store descriptors through E9/query-session runtime cache`
7. `perf(ipc): stream Arrow IPC chunks from worker`
8. `perf(ipc): add browser default output budgets`
9. `perf(startup): avoid WASM load during initial catalog render`
10. `perf(startup): lazy-create query worker and split routes`
11. `perf(scan): split custom scan metrics from debug trace`

Each ticket should include one failing-first test or profiling assertion, the code change, and one command that proves the target behavior changed.
