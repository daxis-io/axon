# Browser Lakehouse Engine Strategy

- Status: Draft
- Date: 2026-03-28
- Scope: turn Axon's current browser preflight and narrow execution slice into a production-oriented browser engine for read-heavy Delta and Parquet workloads
- Related:
  - [WASM + Delta Lake on GCS Program Bundle](./wasm-delta-gcs-program.md)
  - [EPIC-04: Browser DataFusion WASM Runtime And HTTP Object-Store Hardening](../epics/EPIC-04-browser-datafusion-wasm-runtime-and-http-object-store-hardening.md)
  - [Browser Lakehouse Engine Implementation Plan](../plans/2026-03-28-browser-lakehouse-engine-implementation-plan.md)

## Decision Summary

The next browser-engine phase should stay inside Axon as new workspace crates, not as a new repository and not as more logic stuffed into the current crates.

The recommended shape is:

- keep `crates/delta-control-plane` native and trusted
- keep `crates/wasm-http-object-store` as the browser-safe byte-range transport seam
- keep `crates/wasm-parquet-engine` for browser-side Parquet planning and streamed scan primitives
- keep `crates/wasm-delta-snapshot` for browser-safe Delta snapshot reconstruction
- keep `crates/wasm-query-runtime` as the orchestration layer for query-shape analysis, pruning, planning, fallback, metrics, and the browser-facing runtime API
- add `crates/wasm-query-session` as the thin in-memory browser session shell above the runtime and below the worker contract

The repo-grounded V1 is therefore:

- Delta snapshot reconstruction in repo-owned browser-safe code
- streamed Parquet scan primitives inside `crates/wasm-parquet-engine`
- runtime-owned Arrow IPC output from `crates/wasm-query-runtime`
- a worker-owned in-memory session shell in `crates/wasm-query-session`

Broad browser DataFusion remains deferred. V1 should not be described as a browser DataFusion launch.

`parquet-viewer` is useful here as a reference implementation for the scan path only. It validates that browser-side storage adapters, byte-range reads, Parquet access, and optional Arrow/DataFusion execution are viable in WebAssembly. It is not the Delta layer and should not be treated as the reason to collapse Delta protocol work into the same crate or split Axon into a separate repository.

## Why This Fits Axon

Axon already has the seam this work needs.

- `crates/delta-control-plane` resolves trusted Delta snapshots and emits browser HTTP descriptors.
- `crates/wasm-http-object-store` already owns exact HTTP byte-range reads and response validation.
- `crates/wasm-query-runtime` already materializes browser descriptors, bootstraps Parquet metadata, performs pruning, and executes a constrained browser subset over local loopback-served fixtures.
- Cross-crate tests already prove the handoff from native Delta snapshot resolution into browser-owned runtime state.

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

That is why the Delta layer should be its own crate, with a handler split modeled after Delta Kernel rather than bolted onto the Parquet scan layer.

## Target Architecture

```text
browser-sdk / embedding host
            |
            v
        worker host
            |
            v
    crates/wasm-query-session
            |
            v
    crates/wasm-query-runtime
      |                 |
      |                 +--> Arrow IPC result boundary
      |
      +--> crates/wasm-delta-snapshot
      |      - _delta_log listing / reads
      |      - checkpoint selection
      |      - sidecars
      |      - active-file reconstruction
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

- `wasm-delta-snapshot` produces active file descriptors with path, size, partition values, and optional stats
- `wasm-parquet-engine` consumes those descriptors without rediscovering file size or table state
- `wasm-query-runtime` owns query planning and decides whether the request stays in browser or falls back to native

## Proposed Crate Map

| Crate | Responsibility | Allowed Dependencies | Must Not Do |
| --- | --- | --- | --- |
| `crates/wasm-http-object-store` | Browser-safe remote and local byte-range I/O | `reqwest`, `bytes`, browser bindings, cache helpers | Parse Delta logs, decode Parquet schema, own SQL semantics |
| `crates/wasm-parquet-engine` | Footer reads, metadata decode, Parquet planning, Arrow batch scans | `parquet`, Arrow-facing helpers, `wasm-http-object-store` | Parse `_delta_log`, own query routing, own browser SDK bindings |
| `crates/wasm-delta-snapshot` | `_delta_log` listing and reads, checkpoint selection, sidecars, commit replay, active-file reconstruction | `wasm-http-object-store`, JSON decode, Parquet checkpoint readers, shared contracts | Own SQL planning, own UI bindings, depend on native `deltalake` |
| `crates/wasm-query-runtime` | Query-shape analysis, pruning, planning, fallback, metrics, browser runtime API | `query-contract`, `wasm-delta-snapshot`, `wasm-parquet-engine` | Reimplement low-level transport or Delta protocol parsing |
| `crates/wasm-query-session` | In-memory table/session shell over runtime-owned snapshots | `query-contract`, `wasm-query-runtime` | Add persistence, cache SDK envelopes, or imply browser DataFusion |
| `crates/delta-control-plane` | Trusted-side snapshot resolution, policy enforcement, future signed URL / proxy issuance | native `deltalake`, `query-contract` | Become the browser Delta engine |
| `crates/browser-sdk` | Worker command and IPC boundary, browser embedding API | `query-contract`, Arrow IPC surface | Own Delta, Parquet, or runtime/session internals |

`wasm-delta-snapshot` is the recommended working name because it describes scope clearly. If the crate later grows into a broader kernel-like reusable package, it can be renamed or published separately then.

## Design Rules

### 1. Delta Snapshot And Parquet Scan Stay Separate

The Delta layer resolves table state. The Parquet layer scans files. Do not merge them.

This is the main correction to a naive "just add Delta to parquet-viewer" plan. `parquet-viewer` proves the scan path. It does not solve `_delta_log` semantics.

### 2. The Delta Layer Hands Known File Size Into The Scan Layer

Delta `add` actions already carry the data file path and size. The browser Delta layer should pass those values directly into the Parquet layer so metadata bootstrap can avoid extra discovery requests.

That handoff should be the default path for remote reads.

### 3. Worker-First Execution

The browser engine should run in a worker and keep the main thread out of the hot path. The worker boundary should move Arrow IPC, not row-oriented JSON, so large scans do not pay unnecessary serialization cost.

### 4. Single-Threaded WASM Is The Baseline

The default architecture should assume `wasm32-unknown-unknown` with single-threaded worker execution. Shared-memory and threaded execution can be added later for cross-origin-isolated deployments, but they should not sit on the critical path.

### 5. I/O-Free Core, Async Browser Adapters At The Edge

Protocol logic and scan planning should remain trait-based and testable without browser globals. Browser-specific fetch, `Blob`, and cache implementations should sit at the adapter boundary.

### 6. Extent Cache, Not Exact-Range Cache

The browser I/O layer should move from exact requested ranges toward coalesced extents with stable keys such as `(url_or_path, identity, start, end)`, plus eviction, readahead, and validation. This is the right long-term substrate for both Delta logs and Parquet reads.

The remote HTTP path should assume single-range `Range` requests, `206 Partial Content` responses for partial fetches, and explicit exposure of the response headers needed for object identity and range validation when browser code must inspect them.

### 7. Browser DataFusion Is Deferred For The Default SKU

The runtime should support a smaller scan / filter / project profile and a larger SQL/DataFusion profile. SQL breadth should not silently become a bundle-size regression for every browser deployment, and the larger browser DataFusion SKU should remain explicitly deferred or experimental until code and release gates exist in repo.

### 8. Bounded Streaming Over Unbounded Materialization

Execution should stream results and enforce query budgets. Operators with large memory footprints need explicit guardrails and native fallback rules instead of implicit "collect everything in the browser" behavior.

## Phased Build Path

### Phase 1: Industrialize Browser I/O And Parquet Scan

Deliverables:

- extend `crates/wasm-http-object-store` with metadata probes, coalescing hooks, and extent-cache primitives
- extract a new `crates/wasm-parquet-engine` from the current Parquet bootstrap logic in `crates/wasm-query-runtime`
- keep local browser file access as a first-class adapter path alongside remote HTTP reads

Exit criteria:

- Parquet metadata and scan paths no longer live directly in `wasm-query-runtime`
- remote and local file reads share one browser-safe scan abstraction
- known-size metadata bootstrap is covered by tests

### Phase 2: Add Browser Delta Snapshot Reconstruction

Deliverables:

- new `crates/wasm-delta-snapshot`
- checkpoint selection logic
- classic checkpoint support
- V2 checkpoint and sidecar support
- JSON replay and active-file reconstruction

Exit criteria:

- browser snapshot reconstruction matches native control-plane descriptors on local fixtures
- snapshot tests cover checkpoint and replay edge cases
- `wasm-query-runtime` can receive file descriptors from either trusted native control-plane output or browser-local snapshot reconstruction

### Phase 3: Rewire The Runtime Around The New Crates

Deliverables:

- `wasm-query-runtime` becomes an orchestrator over snapshot + scan crates
- pruning uses Delta add-file facts before opening Parquet whenever possible
- browser SDK and worker host use Arrow IPC for result transport
- `wasm-query-session` provides an in-memory-only table/session shell for repeated queries
- optional SQL/DataFusion feature split is defined but remains deferred for the default SKU

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

- reconstruct supported Delta snapshots in browser-safe code when the deployment model needs it
- scan Delta and plain Parquet datasets through the same browser Parquet engine
- keep `delta-control-plane` as the trusted native boundary for policy and future access issuance
- preserve native parity and deterministic fallback over the supported browser envelope
- keep the architecture modular enough that scan or snapshot crates can be published independently later if they mature into stable standalone assets

## Non-Goals

- browser write-path support
- OPFS or IndexedDB persistent-cache backends
- direct browser cloud credentials
- forcing the native `deltalake` crate into the browser
- making multithreaded WASM a launch prerequisite
- splitting Axon into a separate repository before the new crate boundaries are proven

## Open Questions To Resolve During Execution

- Whether the first remote-store iteration should stay HTTP-first or immediately adopt an OpenDAL bridge behind a feature flag
- How broad the first optional browser DataFusion SKU should be before bundle size becomes unacceptable
- Whether browser-local Delta directories should target OPFS first, drag-and-drop file trees first, or both
- Whether `wasm-delta-snapshot` should accept a control-plane-produced file listing as an override path for hosted deployments that do not want browser-side log replay
