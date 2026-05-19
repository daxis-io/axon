# Browser Lakehouse Engine Strategy

- Status: Draft
- Date: 2026-03-28
- Scope: turn Axon's current browser preflight and narrow execution slice into a production-oriented browser engine for read-heavy Delta and Parquet workloads
- Related:
  - [WASM + Delta Lake on GCS Program Bundle](./wasm-delta-gcs-program.md)
  - [EPIC-04: Browser DataFusion WASM Runtime And HTTP Object-Store Hardening](../epics/EPIC-04-browser-datafusion-wasm-runtime-and-http-object-store-hardening.md)
  - [Browser Lakehouse Engine Implementation Plan](../plans/2026-03-28-browser-lakehouse-engine-implementation-plan.md)
  - [Browser DataFusion Size Audit](./browser-datafusion-size-audit.md)
  - [Browser Delta Compatibility Matrix](./browser-delta-compatibility-matrix.md)
  - [Browser Unity Catalog Brokered Runtime Contract](./browser-uc-brokered-runtime-contract.md)

## Decision Summary

The next browser-engine phase should stay inside Axon as new workspace crates, not as a new repository and not as more logic stuffed into the current crates.

The recommended shape is:

- keep `crates/delta-control-plane` native and trusted
- keep `crates/wasm-http-object-store` as the browser-safe byte-range transport seam
- keep `crates/wasm-parquet-engine` for browser-side Parquet planning and streamed scan primitives
- add `crates/wasm-delta-kernel-engine` as the browser-safe Delta Kernel integration layer
- keep `crates/wasm-delta-snapshot` as the existing compatibility scaffold while Delta snapshot
  reconstruction moves behind Delta Kernel semantics
- promote the DataFusion POC into a first-class `wasm-datafusion-engine` that owns SQL planning,
  optimization, physical planning, execution, and Arrow `RecordBatch` output in a browser Worker
- keep `crates/wasm-query-runtime` as a legacy fallback, correctness scaffold, and migration bridge
- use `crates/wasm-datafusion-session` as the DataFusion-backed browser session shell above opened descriptors and below the UI runtime contract
- keep `crates/wasm-query-session` as the legacy narrow in-memory session shell isolated for removal

The target browser SKU is therefore:

- Delta protocol semantics come from `delta-kernel-rs` core with its default engine disabled
- `crates/wasm-delta-kernel-engine` implements `AxonBrowserKernelEngine` over Axon's browser
  cache, JSON, Parquet, and expression handlers
- Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`
- streamed Parquet scan primitives inside `crates/wasm-parquet-engine`
- DataFusion physical execution in a browser Worker
- `AxonDeltaTableProvider` and `AxonParquetScanExec` connect Axon's Delta/Parquet stack to DataFusion
- Arrow IPC output from the DataFusion engine wrapper
- a DataFusion-owned in-memory session shell in `crates/wasm-datafusion-session`

The DataFusion and legacy narrow session shells remain in-memory only. The object-store seam now has a narrow OPFS extent-cache backend, while OPFS / IndexedDB session-level persistent caches remain deferred.
Signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation stay outside repo-owned V1 success claims.

The broad browser DataFusion product direction is no longer deferred. The browser query engine
target is DataFusion-backed Delta/Parquet execution. The 30-day gate result, recorded canonically in
the browser DataFusion size audit, is to continue toward a DataFusion physical execution engine with
custom Axon table and scan integration. Bundle size remains a release budget; it is not the
architecture reason to replace DataFusion execution with an Axon IR.

`parquet-viewer` is useful here as a reference implementation for the scan path only. It validates
that browser-side storage adapters, byte-range reads, Parquet access, and optional Arrow/DataFusion
execution are viable in WebAssembly. It is not the Delta layer and should not be treated as the
reason to collapse Delta protocol work into the same crate or split Axon into a separate repository.
For Delta protocol semantics, Axon's browser dependency should be Delta Kernel core, not high-level
`deltalake`.

## Why This Fits Axon

Axon already has the seam this work needs.

- `crates/delta-control-plane` resolves trusted Delta snapshots and emits browser HTTP descriptors.
- `crates/wasm-http-object-store` already owns exact HTTP byte-range reads and response validation.
- `crates/wasm-query-runtime` already materializes browser descriptors, bootstraps Parquet metadata, performs pruning, and executes a constrained browser subset over local loopback-served fixtures.
- Cross-crate tests already prove the handoff from native Delta snapshot resolution into browser-owned runtime state.
- `crates/wasm-delta-snapshot` has already proven the browser can replay constrained Delta log
  descriptors, which makes it a useful migration scaffold for a Delta Kernel-backed engine.

That means the most valuable next step is to deepen those seams, not recreate them across repositories.

Keeping this work in Axon preserves:

- one conformance story across native oracle, control-plane descriptor generation, and browser execution
- one CI surface for `wasm32-unknown-unknown` compatibility
- one place to enforce browser security rules and native fallback behavior
- one set of shared contracts for file descriptors, partition types, and metrics

Splitting repositories now would add versioning, release, and CI coordination cost before the browser Delta and Parquet interfaces are stable.

## Why Not Put This Inside The Existing Crates

The current crates are signaling the right boundary already.

- `crates/delta-control-plane` depends on native `deltalake` and is shaped like trusted-side snapshot resolution. It should stay focused on policy, native correctness, and later signed-URL or proxy issuance.
- `crates/wasm-query-runtime` is already the browser orchestration layer. It should not become the home for raw `_delta_log` reconstruction, remote extent caching, and low-level Parquet scan internals all at once.
- `crates/wasm-http-object-store` is intentionally transport-sized. It should stay responsible for range reads, metadata validation, and cache-friendly browser I/O primitives, not Delta or SQL semantics.
- `crates/wasm-delta-snapshot` should not grow into a long-term home-grown Delta protocol
  implementation now that `delta-kernel-rs` exposes the protocol/runtime split Axon needs.

The next phase needs new sibling crates because the work is real, but the current crate boundaries are still the right ones.

## parquet-viewer Positioning

`parquet-viewer` is the right reference implementation for the layer after snapshot resolution:

`storage adapter -> range reads -> Parquet scan -> Arrow/DataFusion execution`

The useful ideas to carry forward are:

- an object-store-like abstraction that turns a remote URL or browser-local file into efficient Parquet reads
- a byte-range cache that sits directly under the Parquet reader
- local browser file access via `Blob` / `File.slice(...).arrayBuffer()`
- size-conscious WASM packaging and browser test hygiene

The important limit is that this does not solve Delta table state reconstruction. A browser Delta reader still has to:

- inspect `_delta_log`
- choose the newest complete checkpoint
- handle classic and V2 checkpoints
- resolve sidecars when present
- replay JSON commits after the chosen checkpoint
- reconstruct the active `add` file set before any Parquet scan begins

That is why the Delta layer should be its own crate, implemented around Delta Kernel's handler split
rather than bolted onto the Parquet scan layer.

## Target Architecture

```text
browser-sdk / embedding host
            |
            v
        worker host
            |
            v
    crates/wasm-datafusion-session
            |
            v
    crates/wasm-datafusion-engine
      |                 |
      |                 +--> Arrow IPC result boundary
      |
      +--> DataFusion SessionContext
      |      - SQL planning
      |      - logical optimization
      |      - physical planning
      |      - physical execution
      |
      +--> AxonDeltaTableProvider
      |      - DataFusion table registration
      |      - projection / filter / limit pushdown contract
      |      - Delta active-file descriptors
      |
      +--> AxonParquetScanExec
      |      - DataFusion ExecutionPlan
      |      - RecordBatch stream
      |      - cancellation / memory / scan metrics
      |
      +--> crates/wasm-delta-kernel-engine
      |      - delta-kernel-rs core with default engine disabled
      |      - AxonBrowserKernelEngine
      |      - async prefetch into sync DeltaLogCache
      |      - StorageHandler / JsonHandler / ParquetHandler / EvaluationHandler
      |      - Delta snapshot and scan descriptor conversion
      |
      +--> crates/wasm-delta-snapshot
      |      - current compatibility scaffold
      |      - descriptor conversion tests and native parity fixtures during migration
      |
      +--> crates/wasm-parquet-engine
      |      - footer reads
      |      - metadata decode
      |      - row-group / page planning
      |      - Arrow batch scan
      |
      +--> crates/wasm-http-object-store
             - HTTP metadata probe
             - range reads
             - extent cache
             - browser-local file adapters

native side:
  crates/delta-control-plane   -> trusted policy, native snapshot resolution, signed URL / proxy issuance later
  crates/native-query-runtime  -> correctness oracle and mandatory fallback path
```

The key handoff is explicit:

- `wasm-delta-kernel-engine` uses Delta Kernel to produce active file and scan descriptors with path,
  size, partition values, optional stats, protocol metadata, and any Kernel-required transforms
- `AxonDeltaTableProvider` exposes those descriptors to DataFusion as a registered table
- `AxonParquetScanExec` consumes DataFusion projection, filter, and limit pushdown inputs
- `wasm-parquet-engine` and `wasm-http-object-store` produce Arrow `RecordBatch` streams from browser-safe range reads
- DataFusion owns SQL planning, optimization, physical planning, physical execution, and result batches

## Proposed Crate Map

| Crate                             | Responsibility                                                                                                 | Allowed Dependencies                                                                                                                                  | Must Not Do                                                                                                               |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `crates/wasm-http-object-store`   | Browser-safe remote and local byte-range I/O                                                                   | `reqwest`, `bytes`, browser bindings, cache helpers                                                                                                   | Parse Delta logs, decode Parquet schema, own SQL semantics                                                                |
| `crates/wasm-parquet-engine`      | Footer reads, metadata decode, Parquet planning, Arrow batch scans                                             | `parquet`, Arrow-facing helpers, `wasm-http-object-store`                                                                                             | Parse `_delta_log`, own query routing, own browser SDK bindings                                                           |
| `crates/wasm-delta-kernel-engine` | Delta Kernel wrapper, `AxonBrowserKernelEngine`, cached snapshot resolution, Kernel scan descriptor conversion | `delta_kernel` core with default engine disabled, `bytes`, `serde`, `serde_json`, `url`, `thiserror`, `wasm-parquet-engine`, `wasm-http-object-store` | Enable Delta Kernel default engine, perform browser fetch from synchronous Kernel callbacks, depend on native `deltalake` |
| `crates/wasm-delta-snapshot`      | Existing browser snapshot scaffold and migration compatibility layer                                           | `wasm-http-object-store`, JSON decode, Parquet checkpoint readers, shared contracts                                                                   | Become the long-term protocol authority, own SQL planning, own UI bindings, depend on native `deltalake`                  |
| `crates/wasm-datafusion-engine`   | DataFusion `SessionContext`, table registration, SQL execution, Arrow IPC streaming                            | DataFusion, Arrow, `wasm-delta-kernel-engine`, `wasm-parquet-engine`, `wasm-http-object-store`                                                        | Own raw Delta protocol parsing or browser HTTP range internals                                                            |
| `crates/wasm-datafusion-session`  | Dedicated browser DataFusion table/session shell for UI/runtime builds                                         | `query-contract`, `wasm-datafusion-engine`, browser runtime descriptor/materialization helpers                                                        | Depend on the legacy narrow session or own raw Delta/Parquet internals                                                    |
| `crates/wasm-query-runtime`       | Legacy narrow runtime, fallback, correctness scaffold during migration                                         | `query-contract`, `wasm-delta-kernel-engine`, `wasm-delta-snapshot`, `wasm-parquet-engine`                                                            | Be the destination SQL execution engine once DataFusion scan integration is viable                                        |
| `crates/wasm-query-session`       | Legacy narrow in-memory table/session shell over opened browser tables                                         | `query-contract`, `wasm-query-runtime`                                                                                                                | Depend on DataFusion, add persistence, or own SQL/Delta/Parquet internals                                                 |
| `crates/delta-control-plane`      | Trusted-side snapshot resolution, policy enforcement, future signed URL / proxy issuance                       | native `deltalake`, `query-contract`                                                                                                                  | Become the browser Delta engine                                                                                           |
| `crates/browser-sdk`              | Worker command and IPC boundary, browser embedding API                                                         | `query-contract`, Arrow IPC surface                                                                                                                   | Own Delta, Parquet, or runtime/session internals                                                                          |

`wasm-delta-kernel-engine` is the recommended working name because the long-term boundary is the
Delta Kernel `Engine` implementation, not another Axon-owned protocol parser. `wasm-delta-snapshot`
can continue to exist while tests, descriptors, and existing runtime callers migrate.

## Design Rules

### 1. Delta Snapshot And Parquet Scan Stay Separate

The Delta layer resolves table state. The Parquet layer scans files. Do not merge them.

This is the main correction to a naive "just add Delta to parquet-viewer" plan. `parquet-viewer` proves the scan path. It does not solve `_delta_log` semantics.

### 1a. Delta Kernel Is The Protocol Authority

Axon should use `delta-kernel-rs` core for Delta protocol semantics and implement its own browser
Engine. Do not enable Delta Kernel's default engine in the browser package, because that path is
designed around Arrow/Tokio/HTTP runtime choices that Axon should keep outside the protocol core.

High-level `deltalake` remains valuable as a native reference implementation, correctness oracle,
and source of DataFusion integration ideas. It should not become the browser runtime dependency
unless a feature audit proves the native object-store, async, TLS, catalog, and cloud surfaces can be
excluded.

### 2. The Delta Layer Hands Known File Size Into The Scan Layer

Delta `add` actions already carry the data file path and size. The browser Delta layer should pass those values directly into the Parquet layer so metadata bootstrap can avoid extra discovery requests.

That handoff should be the default path for remote reads.

### 3. Worker-First Execution

The browser engine should run in a worker and keep the main thread out of the hot path. The worker boundary should move Arrow IPC, not row-oriented JSON, so large scans do not pay unnecessary serialization cost.

### 4. Single-Threaded WASM Is The Baseline

The default architecture should assume `wasm32-unknown-unknown` with single-threaded worker execution. Shared-memory and threaded execution can be added later for cross-origin-isolated deployments, but they should not sit on the critical path.

### 4a. Adaptive Bundle Selection Is Capability-Gated

Browser hosts should choose worker and WASM assets through a manifest plus platform feature probe,
not through user-agent strings. The browser-facing SDK exposes `getPlatformFeatures()` for
`crossOriginIsolated`, WASM SIMD, WASM threads, and `BigInt64Array`, plus `selectBundle()` for
choosing the highest available manifest entry whose declared requirements match those features.

The shipped baseline remains single-threaded and must work without cross-origin isolation, SIMD, or
shared memory. SIMD, threaded, and SIMD-threaded entries are deployment tiers. They should stay marked
as `future` until the corresponding artifacts are built, hosted, size-gated, and covered by browser
smoke tests. Threaded variants require COOP/COEP deployment headers that make
`crossOriginIsolated` true and expose `SharedArrayBuffer`; deployments without those headers must
fall back to the baseline or another single-threaded bundle.

Bundle selection does not change the security boundary. Worker URLs and WASM URLs are static
application assets, and table data still arrives through browser-safe descriptors minted by the
trusted control plane. Browser code must not mint cloud credentials or embed cloud secrets.

### 5. I/O-Free Core, Async Browser Adapters At The Edge

Protocol logic and scan planning should remain trait-based and testable without browser globals. Browser-specific fetch, `Blob`, and cache implementations should sit at the adapter boundary.

Delta Kernel handler calls are synchronous from the Kernel's perspective, while browser fetch is
promise-based. The browser engine should therefore prefetch `_delta_log`, checkpoint, and sidecar
bytes asynchronously into a `DeltaLogCache`, then invoke Delta Kernel over cached bytes. If complete
prefetch is impossible, use an explicit cache-miss retry loop that fetches missing files outside the
Kernel call and retries snapshot resolution. Do not call browser `fetch()` from inside a synchronous
Kernel handler.

### 6. Extent Cache, Not Exact-Range Cache

The browser I/O layer should move from exact requested ranges toward coalesced extents with stable keys such as `(url_or_path, identity, start, end)`, plus eviction, readahead, and validation. This is the right long-term substrate for both Delta logs and Parquet reads.

The first OPFS-backed adapter stores validated extents behind a hashed per-object index with a
bounded per-identity entry cap. It is enough to prove durable browser-local reuse and failure
isolation, while readahead, quota tuning, and query/session-level cache policy remain later
hardening work.

The remote HTTP path should assume single-range `Range` requests, `206 Partial Content` responses for partial fetches, and explicit exposure of the response headers needed for object identity and range validation when browser code must inspect them.

### 7. Browser DataFusion Is The Query Engine Target

The browser SKU should be a DataFusion-powered Delta/Parquet query engine. SQL breadth should still
be gated by browser size, memory, and latency budgets, but DataFusion physical execution is the
destination architecture rather than an optional planner feeding a custom Axon IR.

The browser DataFusion size audit remains canonical for DataFusion details: measured WASM artifact
sizes, retained dependency surface, feature-splitting candidates, startup/runtime budgets, and the
30-day decision. The strategic boundary is explicit: Axon owns table access, Delta snapshot facts,
browser HTTP range reads, Parquet scan integration, query budgets, fallback, and Arrow IPC delivery;
DataFusion owns SQL planning, logical optimization, physical planning, expression evaluation,
physical execution, and Arrow `RecordBatch` output.

The first production-oriented DataFusion integration should be a custom `AxonDeltaTableProvider`
plus custom `AxonParquetScanExec` over Axon's existing browser-safe Parquet/range stack. A
DataFusion-native Parquet reader with a browser `object_store` adapter is a later evaluation path,
not the first production dependency.

### 8. Bounded Streaming Over Unbounded Materialization

Execution should stream results and enforce query budgets. Operators with large memory footprints need explicit guardrails and native fallback rules instead of implicit "collect everything in the browser" behavior.

## Phased Build Path

### Phase 1: Industrialize Browser I/O And Parquet Scan

Deliverables:

- extend `crates/wasm-http-object-store` with metadata probes, coalescing hooks, and extent-cache primitives
- add a narrow OPFS persistent extent adapter whose failures are treated as cache misses
- extract a new `crates/wasm-parquet-engine` from the current Parquet bootstrap logic in `crates/wasm-query-runtime`
- keep local browser file access as a first-class adapter path alongside remote HTTP reads

Exit criteria:

- Parquet metadata and scan paths no longer live directly in `wasm-query-runtime`
- remote and local file reads share one browser-safe scan abstraction
- known-size metadata bootstrap is covered by tests

### Phase 2: Add Delta Kernel Browser Snapshot Reconstruction

Deliverables:

- new `crates/wasm-delta-kernel-engine`
- `delta_kernel` core dependency with default engine disabled
- cargo-tree gates proving `tokio`, `reqwest`, native TLS, cloud SDKs, and native `deltalake` are
  absent from the Delta Kernel browser crate
- async browser prefetch into a `DeltaLogCache`
- cached `StorageHandler` over trusted descriptors, known versions, or manifest-backed listings
- `JsonHandler`, `ParquetHandler`, and `EvaluationHandler` implementations sized for read-only
  snapshot and scan metadata resolution
- conversion from Kernel snapshot and scan metadata into Axon table descriptors
- compatibility tests against the existing `wasm-delta-snapshot` fixtures and native `deltalake`
  oracle

Exit criteria:

- Delta Kernel core compiles for `wasm32-unknown-unknown` without its default engine
- browser snapshot reconstruction matches native control-plane descriptors on local fixtures
- snapshot tests cover checkpoint and replay edge cases
- `wasm-query-runtime` can receive file descriptors from either trusted native control-plane output or browser-local snapshot reconstruction

### Phase 3: Rewire The Runtime Around The New Crates

Deliverables:

- `wasm-query-runtime` becomes an orchestrator over snapshot + scan crates
- pruning uses Delta add-file facts before opening Parquet whenever possible
- browser SDK and worker host use Arrow IPC for result transport
- `wasm-datafusion-session` provides the DataFusion-backed in-memory table/session shell for repeated UI/runtime queries
- `wasm-query-session` remains a removable legacy narrow shell
- `wasm-datafusion-engine` can register Delta-derived tables in a DataFusion `SessionContext`
- DataFusion SQL executes over `AxonParquetScanExec` and returns Arrow IPC from the worker

Exit criteria:

- runtime logic consumes the new crates rather than owning their internals
- browser/native parity still holds for the supported corpus
- fallback remains structured and observable

### Phase 4: Hardening And Launch Readiness

Deliverables:

- wasm-target CI for all browser crates
- browser tests for the new crates
- size budgets and benchmark reporting
- `SECURITY.md`, compatibility matrix, and browser support matrix
- metrics and dashboards for bytes fetched, files touched, skipped files, cache behavior, and fallback reason

Exit criteria:

- browser build size is budgeted and tracked
- perf claims are backed by repeatable benchmarks
- unsafe code is audited and minimized
- launch checklist reflects the new crate topology

## Success Criteria

This strategy is successful when Axon can do all of the following without changing its security boundary:

- reconstruct supported Delta snapshots through Delta Kernel-backed browser-safe code when the
  deployment model needs it
- scan Delta and plain Parquet datasets through the same browser Parquet engine
- keep `delta-control-plane` as the trusted native boundary for policy and future access issuance
- preserve native parity and deterministic fallback over the supported browser envelope
- keep the architecture modular enough that scan or snapshot crates can be published independently later if they mature into stable standalone assets

## Non-Goals

- browser write-path support
- IndexedDB persistent-cache backends
- session-level persistent table caches
- direct browser cloud credentials
- forcing the native `deltalake` crate into the browser
- enabling Delta Kernel's default engine in the browser runtime without a measured dependency audit
- making multithreaded WASM a launch prerequisite
- splitting Axon into a separate repository before the new crate boundaries are proven

## Open Questions To Resolve During Execution

- Whether the first remote-store iteration should stay HTTP-first or immediately adopt an OpenDAL bridge behind a feature flag
- How to package, cache, and budget the DataFusion browser engine while excluding unneeded
  datasource/function/codecs
- Whether browser-local Delta directories should target OPFS first, drag-and-drop file trees first, or both
- Whether `wasm-delta-kernel-engine` should start with trusted descriptors only, known-version open
  only, or manifest-backed listing for hosted deployments that do not allow arbitrary browser prefix
  listing
- Which Delta Kernel Arrow feature, if any, should be enabled so its Arrow version stays aligned with
  DataFusion's Arrow version

## Primary References

- Delta Kernel Rust API docs: <https://docs.rs/delta_kernel/latest/delta_kernel/>
- Delta Kernel Rust README: <https://github.com/delta-io/delta-kernel-rs>
- Delta Kernel scan APIs: <https://docs.rs/delta_kernel/latest/delta_kernel/scan/index.html>
- delta-rs Rust crate docs: <https://docs.rs/deltalake/latest/deltalake/>
