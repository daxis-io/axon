# Delta-WASM RFC Repo Grounding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn RFC 0001 into a repo-grounded execution sequence that builds on Axon's existing browser crates instead of restarting the architecture.

**Architecture:** Keep the current split between `delta-control-plane`, `wasm-http-object-store`, `wasm-delta-snapshot`, `wasm-parquet-engine`, `wasm-query-runtime`, `browser-sdk`, and `browser-engine-worker`. Do not treat browser DataFusion as the V1 implementation target. V1 should become: Delta snapshot reconstruction + streaming Parquet scan + runtime-owned Arrow IPC output + a thin in-memory browser session shell, with native remaining the correctness oracle and mandatory fallback.

**Tech Stack:** Rust workspace, `reqwest`, Arrow/Parquet, `deltalake` on the native side, wasm-bindgen test tooling, browser workers, Arrow IPC, CI perf/security gates

---

## Repo Grounding

This RFC is directionally correct, but it must be grounded in what Axon already ships today:

- Keep:
  - `crates/wasm-delta-snapshot` already has handler-separated snapshot reconstruction, `_last_checkpoint`, V2 checkpoint + sidecar handling, and compatibility classification.
  - `crates/wasm-http-object-store` already has validated range reads, extent caching, local-object support, persistent-cache hooks, and transport metrics.
  - `crates/wasm-query-runtime` already does early Delta-file pruning, bootstrapped metadata validation, narrow execution, and native fallback.
  - `crates/browser-sdk` and `crates/browser-engine-worker` already freeze the worker-first Arrow IPC contract.
- Change:
  - Do not rewrite the browser runtime around `SessionContext` / `MemTable` now. The repo does not yet have a browser-safe `RecordBatch` streaming path or runtime-owned Arrow IPC execution output, so that would be architecture-first and repo-second.
  - Do not introduce a new repository. The crate boundaries the RFC wants already exist in this workspace.
- Defer explicitly:
  - service-backed signed URL / proxy issuance
  - OPFS / IndexedDB persistent-cache backend implementation
  - broad browser DataFusion SQL as the default SKU
  - repository extraction

Baseline reality on this branch before starting:

- `cargo test -p wasm-delta-snapshot --locked` passes.
- `cargo test -p wasm-query-runtime --locked` passes.

## Clarifications

These clarifications are authoritative for implementation on this branch:

- Streaming `RecordBatch` execution is internal-only in V1. `wasm-parquet-engine` and
  `wasm-query-runtime` may stream batches internally, but the public browser boundary remains the
  existing one-shot Arrow IPC byte-envelope contract.
- Scan metrics must be explicit. The scan API must return a batch stream plus a metrics
  handle/snapshot path so the runtime can report concrete per-target bytes, rows, files opened,
  and metadata probe counts.
- `wasm-query-runtime` must not depend on `browser-sdk`. The runtime returns a runtime-local Arrow
  IPC payload type, and a higher layer adapts that into the public SDK envelope.
- Literal commit steps are conditional while the worktree is dirty. Commit only if the touched
  files are clean or unrelated edits have been isolated safely; otherwise defer commit creation and
  record verification output.

### Task 1: Add streaming `RecordBatch` scan primitives to `wasm-parquet-engine`

**Files:**
- Modify: `crates/wasm-parquet-engine/Cargo.toml`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/tests/scan.rs`
- Modify: `crates/wasm-parquet-engine/tests/metadata.rs`
- Create: `crates/wasm-parquet-engine/tests/streaming.rs`
- Modify: `crates/wasm-query-runtime/src/parquet_support.rs`

**Step 1: Write the failing scan-stream tests**

Add tests shaped like:

```rust
#[tokio::test]
async fn stream_scan_target_batches_yields_incremental_record_batches() {}

#[tokio::test]
async fn known_size_scan_target_avoids_extra_metadata_probe_round_trips() {}

#[tokio::test]
async fn streamed_scan_reports_rows_and_bytes_per_target() {}
```

Lock these behaviors:

- `ScanTarget.size_bytes` stays the primary metadata-bootstrap path
- the scan layer returns Arrow `RecordBatch` values instead of eagerly flattening all rows
- the stream can expose file-level metrics needed by the runtime

**Step 2: Run the Parquet tests to verify they fail**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
```

Expected: FAIL because the crate currently materializes input rows eagerly and does not expose a streaming Arrow batch API.

**Step 3: Implement the minimal streaming API**

Add a repo-grounded API surface like:

```rust
pub struct ScanTargetMetricsSnapshot {
    pub files_opened: u64,
    pub rows_emitted: u64,
    pub bytes_fetched: u64,
    pub metadata_probe_round_trips: u64,
}

pub trait ScanTargetMetricsHandle {
    fn snapshot(&self) -> ScanTargetMetricsSnapshot;
}

pub struct ScanTargetBatchStream<S> {
    pub batches: S,
    pub metrics: Arc<dyn ScanTargetMetricsHandle + Send + Sync>,
}

pub fn stream_scan_target_batches(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    required_columns: &[String],
    partition_column_types: &BTreeMap<String, PartitionColumnType>,
    request_timeout: Option<Duration>,
) -> Result<
    ScanTargetBatchStream<impl Stream<Item = Result<RecordBatch, QueryError>>>,
    QueryError,
>;
```

Rules:

- keep known-size-aware footer bootstrap
- keep local and HTTP object access behind `wasm-http-object-store`
- keep row-oriented helpers only as test adapters; the runtime should move to batches
- keep scan-layer metrics as the source of truth rather than reconstructing them later in the
  runtime

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-parquet-engine --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
# Only if the touched files are clean or unrelated edits have been isolated.
# Otherwise: defer commit creation for this task and record verification output.
```

### Task 2: Rewire `wasm-query-runtime` around streamed batches and runtime-owned Arrow IPC output

**Files:**
- Modify: `crates/wasm-query-runtime/Cargo.toml`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/src/execution.rs`
- Modify: `crates/wasm-query-runtime/src/lowering.rs`
- Modify: `crates/wasm-query-runtime/src/parquet_support.rs`
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `crates/query-router/src/lib.rs`

**Step 1: Write the failing runtime tests**

Add tests shaped like:

```rust
#[tokio::test]
async fn execute_plan_to_arrow_ipc_streams_supported_query_results() {}

#[tokio::test]
async fn runtime_stops_scanning_when_execution_future_is_cancelled() {}

#[tokio::test]
async fn runtime_routes_over_budget_queries_to_native_before_exhausting_memory() {}
```

Lock these behaviors:

- the runtime consumes streaming `RecordBatch` output from `wasm-parquet-engine`
- browser success results are emitted as Arrow IPC bytes from the runtime boundary
- query budgets fail closed to structured native fallback instead of silently collecting everything

**Step 2: Run the runtime tests to verify they fail**

Run:

```bash
cargo test -p wasm-query-runtime -p query-router --locked
```

Expected: FAIL because the runtime still executes against eager row collections and does not yet emit Arrow IPC itself.

**Step 3: Implement the runtime-owned IPC path**

Add a minimal surface like:

```rust
pub struct BrowserExecutionBudget {
    pub max_rows: u64,
    pub max_bytes: u64,
}

pub struct RuntimeArrowIpcResult {
    pub ipc_bytes: bytes::Bytes,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub scan_metrics: Vec<ScanTargetMetricsSnapshot>,
}

pub async fn execute_plan_to_arrow_ipc(
    &self,
    snapshot: &MaterializedBrowserSnapshot,
    plan: &BrowserExecutionPlan,
) -> Result<RuntimeArrowIpcResult, QueryError>;
```

Rules:

- prune from Delta facts before opening Parquet, as the runtime already does
- keep the current narrow SQL envelope; do not expand SQL breadth in this task
- when a budget is exceeded, return `FallbackRequired` with a structured fallback reason
- keep `query-router` as the only place that turns fallback errors into reroutes
- keep streaming internal to the runtime; do not introduce a public result stream at the SDK
  boundary
- keep adaptation into `browser-sdk::ArrowIpcResultEnvelope` above `wasm-query-runtime`

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-query-runtime -p query-router --locked
cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
# Only if the touched files are clean or unrelated edits have been isolated.
# Otherwise: defer commit creation for this task and record verification output.
```

### Task 3: Add a thin in-memory browser session shell instead of jumping straight to browser DataFusion

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wasm-query-session/Cargo.toml`
- Create: `crates/wasm-query-session/src/lib.rs`
- Create: `crates/wasm-query-session/tests/session.rs`
- Modify: `README.md`

**Step 1: Write the failing session tests**

Add tests shaped like:

```rust
#[tokio::test]
async fn session_reuses_bootstrapped_snapshot_across_repeated_queries() {}

#[tokio::test]
async fn session_evicts_cached_tables_when_memory_budget_is_exceeded() {}

#[tokio::test]
async fn disposing_a_table_releases_cached_snapshot_state() {}
```

Lock these behaviors:

- repeated queries do not rebuild snapshot/bootstrap state every time
- the first session layer is in-memory only
- the session owns table handles and eviction, not `browser-sdk`

**Step 2: Run the session tests to verify they fail**

Run:

```bash
cargo test -p wasm-query-session --locked
```

Expected: FAIL because the crate does not exist yet.

**Step 3: Implement the minimal session crate**

Create a narrow shell like:

```rust
pub struct BrowserQuerySession {
    tables: BTreeMap<String, CachedTable>,
    max_cached_bytes: u64,
}

pub struct CachedTable {
    materialized: MaterializedBrowserSnapshot,
    bootstrapped: Option<BootstrappedBrowserSnapshot>,
    last_used_millis: u64,
}
```

Rules:

- no persistence backend in this task
- no browser DataFusion dependency in this crate
- cache repo-owned runtime state only: descriptors, materialized snapshots, bootstrapped snapshots, and derived execution metadata
- do not cache SDK envelope types in this crate

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-query-session --locked
cargo check -p wasm-query-session --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
# Only if the touched files are clean or unrelated edits have been isolated.
# Otherwise: defer commit creation for this task and record verification output.
```

### Task 4: Expose session-backed worker commands through `browser-sdk` and `browser-engine-worker`

**Files:**
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `crates/browser-sdk/tests/wasm_smoke.rs`
- Modify: `crates/browser-engine-worker/Cargo.toml`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/worker_artifact.rs`
- Modify: `crates/browser-engine-worker/tests/wasm_smoke.rs`

**Step 1: Write the failing worker-contract tests**

Add tests shaped like:

```rust
#[test]
fn browser_sdk_round_trips_open_table_query_and_dispose_commands() {}

#[test]
fn worker_artifact_reports_session_capability_without_claiming_browser_datafusion() {}
```

Lock these behaviors:

- the worker API matches the RFC’s `openTable` / `sql` / `dispose` shape
- large query results stay Arrow IPC
- the worker advertises the current runtime SKU honestly: narrow runtime + session shell, not full browser DataFusion

**Step 2: Run the SDK and worker tests to verify they fail**

Run:

```bash
cargo test -p browser-sdk -p browser-engine-worker --locked
```

Expected: FAIL because the current public SDK only exposes request/response envelopes for one-shot query execution.

**Step 3: Implement the worker command surface**

Add a repo-grounded command layer like:

```rust
pub enum BrowserWorkerCommand {
    OpenTable { request_id: String, name: String, snapshot: BrowserHttpSnapshotDescriptor },
    Sql { request_id: String, name: String, request: QueryRequest },
    Dispose { request_id: String, name: String },
}
```

Rules:

- keep `QueryRequest` as the SQL payload shape
- keep Arrow IPC as the only large-result transport
- keep session state inside the worker, not in the main-thread host
- keep streaming out of the public V1 worker contract; the worker adapts runtime-local IPC output
  into the public SDK envelope

**Step 4: Run verification**

Run:

```bash
cargo test -p browser-sdk -p browser-engine-worker --locked
cargo test -p browser-sdk --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
# Only if the touched files are clean or unrelated edits have been isolated.
# Otherwise: defer commit creation for this task and record verification output.
```

### Task 5: Update docs, CI, and release gates to match the repo-grounded V1 and explicit deferrals

**Files:**
- Modify: `README.md`
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Modify: `docs/program/browser-release-integration-runbook.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/perf/README.md`
- Modify: `tests/security/README.md`

**Step 1: Write the failing release checks**

Add checks or assertions that require:

- the docs to name the current V1 honestly: narrow runtime + streaming scan + in-memory session shell
- browser DataFusion to remain explicitly deferred or experimental, not implied
- external blockers to remain outside repo-owned success claims

**Step 2: Run the current verification commands**

Run:

```bash
cargo check --workspace --locked
cargo check -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
```

Expected: FAIL until the new crate, worker API, and docs are aligned.

**Step 3: Update the release narrative**

Make the docs say exactly this:

- Delta snapshot reconstruction is already repo-owned
- browser execution V1 is not broad browser DataFusion
- the session shell is in-memory only
- persistent-cache hooks exist, but OPFS / IndexedDB backends are still deferred
- signed URL issuance, proxy mode, audit logging, and production CORS remain external blockers

**Step 4: Run final verification**

Run:

```bash
cargo check --workspace --locked
cargo test -p wasm-parquet-engine -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --locked
cargo check -p wasm-parquet-engine -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: PASS.

**Step 5: Commit**

```bash
# Only if the touched files are clean or unrelated edits have been isolated.
# Otherwise: defer commit creation for this task and record verification output.
```

## Explicit Non-Goals For This Plan

- Default browser DataFusion execution
- OPFS or IndexedDB persistent-cache backend
- Browser-side writes, commits, or mutation semantics
- Service-backed signed URL or proxy implementation
- Repository extraction

## Exit Criteria

This plan is complete when Axon can say, truthfully and with tests:

1. Browser-safe Delta snapshot reconstruction is repo-owned and verified against the native control-plane.
2. The browser runtime consumes streamed Arrow batches and emits Arrow IPC results without widening the supported SQL envelope.
3. Repeated browser queries can reuse in-memory session state through a worker-owned table/session shell.
4. Native fallback remains structured, explicit, and test-backed.
5. Docs and release gates no longer imply that broad browser DataFusion or external service work already exists in repo.
