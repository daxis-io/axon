# Parquet Viewer Lessons Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn the useful `parquet-viewer` lessons into Axon-owned browser Delta/Parquet diagnostics, explainability, preview, and input ergonomics without weakening Axon's DataFusion and browser runtime boundaries.

**Architecture:** DataFusion continues to own SQL planning, optimization, physical execution, and `RecordBatch` output. Axon owns Delta descriptors, browser range I/O, Parquet scan integration, budgets, metrics, cancellation, Arrow IPC, SDK contracts, and UI diagnostics. Rich Parquet inspection and explain output should flow through structured worker/SDK contracts, not through UI-only code or DataFusion internals.

**Tech Stack:** Rust workspace, Cargo resolver 2, `query-contract`, `wasm-parquet-engine`, `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, `browser-engine-worker`, `wasm-datafusion-poc`, TypeScript browser sandbox, Playwright, Arrow/Parquet `57.3.0`, DataFusion `52.4.0`.

---

## Current Context

`parquet-viewer` proves that a browser app can compile Arrow, Parquet, DataFusion, and object access into WASM and make Parquet files inspectable and queryable. The project has useful product patterns:

- rich file/row-group/column metadata summaries
- page/encoding/statistics diagnostics
- visible physical plan trees with metrics
- first-batch query UX with "load more"
- local file, URL, S3, and VS Code input paths
- range-read behavior as a visible product claim

Axon should not copy its full architecture. Axon should keep:

- `wasm-parquet-engine` as the browser Parquet scan/metadata boundary over upstream Arrow Rust `parquet`
- `wasm-http-object-store` as the browser-safe range/local/cache boundary
- `wasm-datafusion-poc` / future `wasm-datafusion-engine` as the DataFusion implementation seam
- `browser-engine-worker` as a thin router over `BrowserQuerySession`
- `browser-sdk` and `examples/browser-delta-sandbox/src/axon-browser-sdk.ts` as browser-facing contracts

Prior-art snapshot inspected: `XiangpengHao/parquet-viewer@53be9333504f0657ee76894a98ed93f40e8e798c`.

## Non-Goals

- Do not implement a homegrown Parquet decoder.
- Do not make `wasm-parquet-engine` depend on DataFusion.
- Do not route browser credentials or S3 secret entry into Axon's runtime.
- Do not replace `AxonDeltaTableProvider` / `AxonParquetScanExec` with DataFusion `register_parquet`.
- Do not expose DataFusion internal Rust types over the public browser SDK.
- Do not raise worker artifact budgets to hide dependency regressions.
- Do not change unrelated dirty files.

## Task 0: Baseline And Worktree Hygiene

**Files:**

- Read only: current checkout

**Step 1: Inspect dirty state**

Run:

```bash
git status --short --branch
```

Expected: record any pre-existing dirty files. Do not revert them. If the checkout is dirty, keep each task's staged files exact.

**Step 2: Confirm shipped-worker dependency boundary before broad work**

Run:

```bash
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/perf/report_browser_worker_artifact.sh
```

Expected: both pass. If either fails, stop and investigate before adding new worker-facing contracts.

**Step 3: Confirm DataFusion POC path compiles separately**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked
```

Expected: pass. If this is too slow locally, run only the task-specific DataFusion tests named below and record the skipped broad check in the final handoff.

## Task 1: Add Shared Inspection And Explain Contracts

**Files:**

- Modify: `crates/query-contract/src/lib.rs`
- Modify: `crates/query-contract/tests/query_contract.rs`
- Modify: `crates/query-contract/tests/release_handoff_examples.rs`
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`
- Modify: `examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts`
- Modify: `examples/browser-delta-sandbox/tests/axon-browser-sdk-shape.ts`

**Step 1: Write failing Rust contract tests**

Add tests that assert:

- `QueryResponse` can round-trip with `explain`.
- `BrowserParquetInspection` can round-trip with file, row-group, column, statistics, encoding, and feature-flag fields.
- `BrowserWorkerCommand::inspect_table(...)` serializes as `inspect_table`.
- `BrowserWorkerResponseEnvelope::inspection(...)` serializes as `inspection`.
- existing response JSON without `explain` and without `inspection` remains backward compatible.

Suggested contract shape:

```rust
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserParquetFeatureFlags {
    pub row_group_stats: bool,
    pub column_index: bool,
    pub offset_index: bool,
    pub bloom_filter: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserParquetColumnInspection {
    pub id: usize,
    pub name: String,
    pub path: Vec<String>,
    pub physical_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_type: Option<String>,
    pub nullable: bool,
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub null_count: Option<u64>,
    pub encodings: Vec<String>,
    pub compression: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserParquetRowGroupInspection {
    pub id: usize,
    pub row_count: u64,
    pub compressed_size_bytes: u64,
    pub uncompressed_size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserParquetInspection {
    pub path: String,
    pub object_size_bytes: u64,
    pub footer_length_bytes: u32,
    pub metadata_memory_size_bytes: u64,
    pub row_group_count: u64,
    pub row_count: u64,
    pub column_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    pub format_version: i32,
    pub feature_flags: BrowserParquetFeatureFlags,
    pub row_groups: Vec<BrowserParquetRowGroupInspection>,
    pub columns: Vec<BrowserParquetColumnInspection>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserScanExplain {
    pub projected_columns: Vec<String>,
    pub limit: Option<u64>,
    pub filters: Vec<String>,
    pub exact_filters: Vec<String>,
    pub inexact_filters: Vec<String>,
    pub files_total: u64,
    pub files_planned: u64,
    pub planned_scan_bytes: u64,
    pub planned_file_paths: Vec<String>,
    pub files_skipped: u64,
    pub row_groups_touched: u64,
    pub row_groups_skipped: u64,
    pub bytes_fetched: u64,
    pub rows_emitted: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub struct BrowserQueryExplain {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<BrowserScanExplain>,
}
```

Add this optional field to `QueryResponse`:

```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
pub explain: Option<BrowserQueryExplain>,
```

**Step 2: Run contract tests and verify failure**

Run:

```bash
cargo test -p query-contract --locked browser_query_response_round_trips_optional_explain -- --exact
cargo test -p browser-sdk --locked browser_sdk_round_trips_inspection_response -- --exact
```

Expected: fail until the structs and enum variants exist.

**Step 3: Implement shared structs and worker envelope additions**

In `crates/query-contract/src/lib.rs`, add the inspection and explain types. In `crates/browser-sdk/src/lib.rs`, add:

```rust
pub struct BrowserWorkerInspectTableCommand {
    pub request_id: String,
    pub name: String,
}

pub enum BrowserWorkerCommand {
    OpenTable(BrowserWorkerOpenTableCommand),
    OpenDeltaTable(BrowserWorkerOpenDeltaTableCommand),
    InspectTable(BrowserWorkerInspectTableCommand),
    Sql(BrowserWorkerSqlCommand),
    Dispose(BrowserWorkerDisposeCommand),
}

pub struct BrowserWorkerInspectionEnvelope {
    pub request_id: String,
    pub name: String,
    pub inspection: Vec<BrowserParquetInspection>,
}
```

Update `request_id()`, `table_name()`, response helper methods, and fallback handling for the new variant.

**Step 4: Update TypeScript SDK contract normalization**

In `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`, mirror the Rust types and add:

- `BrowserWorkerInspectTableCommand`
- `{ inspect_table: BrowserWorkerInspectTableCommand }`
- `BrowserWorkerInspectionEnvelope`
- `{ inspection: BrowserWorkerInspectionEnvelope }`
- `AxonBrowserClient.inspectTable(...)`
- validation for `inspection.inspection[*]`

Keep all numeric counts as JS `number` only where they fit current SDK conventions. Do not accept unknown enum values silently.

**Step 5: Run contract verification**

Run:

```bash
cargo test -p query-contract --locked
cargo test -p browser-sdk --locked
npm --prefix examples/browser-delta-sandbox test -- axon-browser-sdk.spec.ts
npm --prefix examples/browser-delta-sandbox run typecheck
```

Expected: pass.

**Step 6: Commit**

```bash
git add crates/query-contract/src/lib.rs crates/query-contract/tests/query_contract.rs crates/query-contract/tests/release_handoff_examples.rs crates/browser-sdk/src/lib.rs crates/browser-sdk/tests/ipc.rs examples/browser-delta-sandbox/src/axon-browser-sdk.ts examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts examples/browser-delta-sandbox/tests/axon-browser-sdk-shape.ts
git commit -m "feat: add browser parquet inspection contracts"
```

## Task 2: Implement Rich Parquet Inspection In `wasm-parquet-engine`

**Files:**

- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/tests/metadata.rs`
- Modify: `crates/wasm-parquet-engine/tests/streaming.rs` only if shared test helpers are useful
- Modify: `crates/wasm-parquet-engine/Cargo.toml` only if a direct `query-contract` type use requires a feature adjustment

**Step 1: Write failing engine tests**

Add tests that build Parquet files with:

- two row groups
- at least two columns
- chunk-level statistics
- known compression and encodings

Test names:

```rust
#[tokio::test]
async fn inspect_parquet_target_reports_file_row_group_and_column_metadata() { ... }

#[tokio::test]
async fn inspect_parquet_target_uses_known_size_footer_ranges() { ... }
```

Assertions:

- trailer and footer range requests are used
- no full-object request is required for inspection
- `object_size_bytes`, `footer_length_bytes`, `metadata_memory_size_bytes`, `row_group_count`, `row_count`, `column_count`, `format_version`, and `created_by` are populated
- each row group reports row count and compressed/uncompressed bytes
- each column reports path, physical type, compression, encoded/compressed sizes, null counts when available, and encodings
- feature flags identify row-group stats, column index, offset index, and bloom-filter availability conservatively

**Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p wasm-parquet-engine --locked --test metadata inspect_parquet_target_reports_file_row_group_and_column_metadata -- --exact
```

Expected: fail because the inspection API does not exist yet.

**Step 3: Implement the inspection API**

Add a public function:

```rust
pub async fn inspect_parquet_target(
    reader: &HttpRangeReader,
    target: &ScanTarget,
    request_timeout: Option<Duration>,
) -> Result<BrowserParquetInspection, QueryError>
```

Implementation notes:

- Reuse `read_parquet_footer_for_target(...)` and `parse_parquet_metadata(...)`.
- Do not construct a DataFusion context.
- Do not decode data pages for the first version.
- Use `ParquetMetaData::memory_size()` for `metadata_memory_size_bytes`.
- Derive feature flags from `metadata.column_index()`, `metadata.offset_index()`, first-column stats, and bloom-filter offsets/lengths.
- Aggregate column compressed/uncompressed sizes, null counts, encodings, and compression across row groups.
- Use strings for physical/logical/compression/encoding display to avoid leaking upstream enum shape over the browser contract.

**Step 4: Run engine tests**

Run:

```bash
cargo test -p wasm-parquet-engine --locked --test metadata
cargo test -p wasm-parquet-engine --locked --test streaming
cargo check -p wasm-parquet-engine --target wasm32-unknown-unknown --locked
```

Expected: pass.

**Step 5: Commit**

```bash
git add crates/wasm-parquet-engine/src/lib.rs crates/wasm-parquet-engine/tests/metadata.rs crates/wasm-parquet-engine/tests/streaming.rs crates/wasm-parquet-engine/Cargo.toml
git commit -m "feat: inspect browser parquet metadata"
```

## Task 3: Surface Inspection Through Runtime, Session, Worker, And SDK

**Files:**

- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/src/parquet_support.rs`
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/wasm-query-session/tests/session.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/wasm_smoke.rs`
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `docs/program/browser-lakehouse-release-handoff-examples/*.json` only for examples directly affected by the new command/response

**Step 1: Write failing session and worker tests**

Add tests:

```rust
#[test]
fn inspect_table_returns_cached_bootstrapped_parquet_inspection() { ... }

#[tokio::test]
async fn worker_handles_inspect_table_command() { ... }
```

Expected behavior:

- `open_delta_table` bootstraps Parquet metadata as it does today.
- `inspect_table("events")` returns inspection summaries from cached bootstrapped metadata.
- `inspect_table` on a missing table returns `QueryErrorCode::InvalidRequest`.
- The worker emits cache metrics but does not emit an Arrow IPC result for inspection.

**Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p wasm-query-session --locked inspect_table_returns_cached_bootstrapped_parquet_inspection -- --exact
cargo test -p browser-engine-worker --locked worker_handles_inspect_table_command -- --exact
```

Expected: fail until session and worker methods exist.

**Step 3: Implement runtime/session mapping**

Add methods:

```rust
impl BrowserQuerySession {
    pub fn inspect_table(&mut self, name: &str) -> Result<Vec<BrowserParquetInspection>, QueryError>
}
```

Mapping rules:

- Prefer cached `BootstrappedBrowserSnapshot` metadata.
- Do not fetch new object bytes in this task.
- If the table is open but lacks bootstrapped metadata, return a structured `InvalidRequest` explaining that inspection requires `open_delta_table` metadata bootstrap.
- Preserve URL redaction in errors.

**Step 4: Implement worker command handling**

In `BrowserWorker::handle_command_streaming_events`, add `BrowserWorkerCommand::InspectTable`.

Expected event flow:

- `progress: started`
- `log: info`
- `cache_metrics`
- `progress: finished`
- `inspection` response

Do not emit `range_read_metrics` unless this command performs new reads in a later task.

**Step 5: Update example JSON contracts if needed**

If adding a new handoff example, create:

```text
docs/program/browser-lakehouse-release-handoff-examples/browser-worker-command.inspect-table.json
docs/program/browser-lakehouse-release-handoff-examples/browser-worker-response.inspection.json
```

Keep the examples minimal and stable.

**Step 6: Run focused verification**

Run:

```bash
cargo test -p wasm-query-runtime --locked
cargo test -p wasm-query-session --locked
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
```

Expected: pass.

**Step 7: Commit**

```bash
git add crates/wasm-query-runtime/src/lib.rs crates/wasm-query-runtime/src/parquet_support.rs crates/wasm-query-runtime/tests/browser_runtime.rs crates/wasm-query-session/src/lib.rs crates/wasm-query-session/tests/session.rs crates/browser-engine-worker/src/lib.rs crates/browser-engine-worker/tests/wasm_smoke.rs crates/browser-sdk/src/lib.rs crates/browser-sdk/tests/ipc.rs docs/program/browser-lakehouse-release-handoff-examples
git commit -m "feat: expose browser parquet inspection"
```

## Task 4: Implement Structured DataFusion Explain Output

**Files:**

- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/scan_pushdown.rs`
- Modify: `crates/wasm-datafusion-poc/tests/arrow_ipc_export.rs`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/wasm-query-session/tests/session.rs`
- Modify: `crates/query-contract/src/lib.rs`
- Modify: `crates/query-contract/tests/query_contract.rs`
- Modify: `docs/program/browser-datafusion-size-audit.md`

**Step 1: Write failing DataFusion explain tests**

Add tests:

```rust
#[tokio::test]
async fn include_explain_returns_physical_plan_and_scan_trace() { ... }

#[tokio::test]
async fn include_explain_false_omits_explain_payload() { ... }
```

Assertions:

- `QueryExecutionOptions { include_explain: true, .. }` populates `QueryResponse.explain`.
- `physical_plan` is a display string, not a serialized DataFusion object.
- `scan.projected_columns`, `exact_filters`, `inexact_filters`, `planned_file_paths`, `bytes_fetched`, and row-group metrics match `AxonParquetScanTrace`.
- default queries keep `explain: None`.

**Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked include_explain_returns_physical_plan_and_scan_trace -- --exact
cargo test -p wasm-query-session --features datafusion --locked include_explain_returns_physical_plan_and_scan_trace -- --exact
```

Expected: fail until explain plumbing exists.

**Step 3: Add DataFusion explain mapping**

In `wasm-datafusion-poc`:

- Add `explain: Option<BrowserQueryExplain>` to `DataFusionArrowIpcResult`.
- Thread an `include_explain: bool` parameter through the result-producing path.
- Map `AxonParquetScanTrace` into `BrowserScanExplain`.
- Use `DisplayableExecutionPlan::indent(true).to_string()` or equivalent for the physical plan string.

Avoid exposing:

- `Arc<dyn ExecutionPlan>`
- DataFusion-specific enums
- internal table-provider structs

**Step 4: Wire through session**

In `wasm-query-session`, pass `request.options.include_explain` to the DataFusion path. Preserve narrow-runtime behavior by returning either `None` or a narrow explain object only if the narrow runtime has a stable equivalent.

**Step 5: Document the boundary**

Update `docs/program/browser-datafusion-size-audit.md` to record:

- explain output is a browser contract for diagnostics
- physical-plan text is diagnostic, not stable API
- scan facts are the stable part to assert in tests and UI

**Step 6: Verify**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked
cargo test -p wasm-query-session --features datafusion --locked
cargo test -p query-contract --locked
```

Expected: pass.

**Step 7: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/tests/scan_pushdown.rs crates/wasm-datafusion-poc/tests/arrow_ipc_export.rs crates/wasm-query-session/src/lib.rs crates/wasm-query-session/tests/session.rs crates/query-contract/src/lib.rs crates/query-contract/tests/query_contract.rs docs/program/browser-datafusion-size-audit.md
git commit -m "feat: expose browser DataFusion explain facts"
```

## Task 5: Promote Bounded Result Preview Into Shared Worker Contracts

**Files:**

- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/wasm_smoke.rs`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `examples/browser-delta-sandbox/src/lib.rs`
- Modify: `examples/browser-delta-sandbox/src/sandbox-query-worker.ts`
- Modify: `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`
- Modify: `examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts`

**Step 1: Write failing tests for Rust-side preview**

The TypeScript sandbox already normalizes optional previews. Promote the shape into Rust:

```rust
pub struct BrowserWorkerResultPreview {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<BrowserWorkerResultPreviewCell>>,
    pub row_count: u64,
    pub preview_row_limit: usize,
    pub truncated: bool,
}
```

Add tests that:

- a success envelope can include preview
- malformed preview cannot deserialize
- preview row count and row limits are bounded

**Step 2: Run tests and verify failure**

Run:

```bash
cargo test -p browser-sdk --locked browser_sdk_round_trips_success_preview -- --exact
```

Expected: fail until Rust contracts include preview.

**Step 3: Implement preview generation in the worker path**

Add a bounded preview helper near the worker success response path. Reuse Arrow IPC stream decoding from `examples/browser-delta-sandbox/src/lib.rs` as prior art, but move shared production logic into Rust crates owned by the worker/runtime layer.

Rules:

- Default preview row limit: `100`.
- Preview generation must not change Arrow IPC bytes.
- Preview generation errors should return structured `QueryError`, not panic.
- Int64 values that exceed JavaScript safe integer should serialize as strings in TypeScript-facing preview output.

**Step 4: Simplify sandbox bridge duplication**

After Rust worker contracts support preview, keep the sandbox TypeScript normalization, but remove duplicate preview-only assumptions where possible.

**Step 5: Verify**

Run:

```bash
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
npm --prefix examples/browser-delta-sandbox test -- axon-browser-sdk.spec.ts
npm --prefix examples/browser-delta-sandbox run typecheck
```

Expected: pass.

**Step 6: Commit**

```bash
git add crates/browser-sdk/src/lib.rs crates/browser-sdk/tests/ipc.rs crates/browser-engine-worker/src/lib.rs crates/browser-engine-worker/tests/wasm_smoke.rs crates/wasm-query-runtime/src/lib.rs crates/wasm-query-session/src/lib.rs examples/browser-delta-sandbox/src/lib.rs examples/browser-delta-sandbox/src/sandbox-query-worker.ts examples/browser-delta-sandbox/src/axon-browser-sdk.ts examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts
git commit -m "feat: add bounded browser query previews"
```

## Task 6: Render Inspection, Explain, And Transport Visibility In The Browser Sandbox

**Files:**

- Modify: `examples/browser-delta-sandbox/src/main.ts`
- Modify: `examples/browser-delta-sandbox/src/styles.css`
- Modify: `examples/browser-delta-sandbox/index.html`
- Modify: `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`
- Modify: `examples/browser-delta-sandbox/tests/browser-delta-sandbox.spec.ts`
- Modify: `examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts`
- Modify: `examples/browser-delta-sandbox/README.md`

**Step 1: Write failing Playwright coverage**

Add or update tests that assert the sandbox renders:

- Parquet inspection summary after opening a table
- feature flags for row-group stats, page index, page offsets, and bloom filters
- explain summary when `include_explain` is enabled
- scan planned files and skipped files
- cache metrics, bytes fetched, bytes reused, validation misses, persistent cache errors when present
- bounded result preview before users inspect raw Arrow IPC metadata

Run:

```bash
npm --prefix examples/browser-delta-sandbox test -- browser-delta-sandbox.spec.ts
```

Expected: fail until UI wiring exists.

**Step 2: Add UI state and rendering**

Add compact operational sections, not a landing page:

- `Parquet Inspection`
- `Query Explain`
- `Transport`
- `Result Preview`

Keep these as dense diagnostics in the existing tool surface. Do not add a marketing hero.

**Step 3: Add SDK calls**

After `openDeltaTable`, call:

```ts
const inspection = await client.inspectTable(QUERY_TABLE_NAME, { requestId });
```

When running SQL, set:

```ts
queryOptions: {
  collect_metrics: true,
  include_explain: true,
}
```

Render the stable scan facts from `response.explain?.scan`. Treat `physical_plan` as diagnostic text.

**Step 4: Verify**

Run:

```bash
npm --prefix examples/browser-delta-sandbox run lint
npm --prefix examples/browser-delta-sandbox run typecheck
npm --prefix examples/browser-delta-sandbox test -- browser-delta-sandbox.spec.ts
npm --prefix examples/browser-delta-sandbox test -- axon-browser-sdk.spec.ts
```

Expected: pass.

**Step 5: Commit**

```bash
git add examples/browser-delta-sandbox/src/main.ts examples/browser-delta-sandbox/src/styles.css examples/browser-delta-sandbox/index.html examples/browser-delta-sandbox/src/axon-browser-sdk.ts examples/browser-delta-sandbox/tests/browser-delta-sandbox.spec.ts examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts examples/browser-delta-sandbox/README.md
git commit -m "feat: show parquet diagnostics in browser sandbox"
```

## Task 7: Add URL And Local-File Ergonomics Without Runtime Secret Handling

**Files:**

- Modify: `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`
- Modify: `examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts`
- Modify: `examples/browser-delta-sandbox/src/main.ts`
- Modify: `examples/browser-delta-sandbox/tests/browser-delta-sandbox.spec.ts`
- Create: `tools/browser-local-parquet-server/Cargo.toml`
- Create: `tools/browser-local-parquet-server/src/main.rs`
- Modify: root `Cargo.toml` if this repo tracks `tools/*` workspace members explicitly
- Modify: `examples/browser-delta-sandbox/README.md`

**Step 1: Write SDK tests for URL helper**

Add a helper:

```ts
export async function parquetUrlSnapshotDescriptor(input: {
  tableUri: string;
  tableName?: string;
  url: string;
  sizeBytes?: number;
  partitionValues?: Record<string, string | null>;
}): Promise<BrowserHttpSnapshotDescriptor>
```

Expected behavior:

- If `sizeBytes` is absent, use `HEAD` and require `Content-Length`.
- Reject non-HTTPS URLs except loopback HTTP in dev/test helpers.
- Redact query strings, fragments, usernames, and passwords from errors.
- Do not accept S3 credentials or cloud secret material.

**Step 2: Implement URL helper**

Keep this example-owned first. It should synthesize a single-file `BrowserHttpSnapshotDescriptor` that flows through the existing `openDeltaTable` path.

**Step 3: Add local dev range server**

Create a small CLI modeled after the useful part of `parquet-viewer`'s local CLI:

```bash
cargo run -p browser-local-parquet-server -- path/to/file.parquet
```

Behavior:

- bind to loopback by default
- serve `HEAD`
- serve `GET` with `Range`
- set `Accept-Ranges: bytes`
- expose `Content-Length` and `Content-Range`
- print a sandbox URL with an encoded `?url=...`
- reject paths other than the single configured file

Do not make this the production local-file API. It is a developer tool for range-read testing and demos.

**Step 4: Add direct File/Blob follow-up note**

In `examples/browser-delta-sandbox/README.md`, note that direct `File | Blob` support should use `wasm-http-object-store::BrowserLocalObject` and a typed worker bridge in a later slice. Do not tunnel blobs through JSON.

**Step 5: Verify**

Run:

```bash
cargo test -p browser-local-parquet-server --locked
npm --prefix examples/browser-delta-sandbox test -- axon-browser-sdk.spec.ts
npm --prefix examples/browser-delta-sandbox test -- browser-delta-sandbox.spec.ts
```

Expected: pass.

**Step 6: Commit**

```bash
git add examples/browser-delta-sandbox/src/axon-browser-sdk.ts examples/browser-delta-sandbox/tests/axon-browser-sdk.spec.ts examples/browser-delta-sandbox/src/main.ts examples/browser-delta-sandbox/tests/browser-delta-sandbox.spec.ts tools/browser-local-parquet-server Cargo.toml examples/browser-delta-sandbox/README.md
git commit -m "feat: add browser parquet URL ergonomics"
```

## Task 8: Transport Cache Metrics End-To-End

**Files:**

- Modify: `crates/wasm-http-object-store/src/lib.rs`
- Modify: `crates/wasm-http-object-store/tests/extent_cache.rs`
- Modify: `crates/wasm-http-object-store/tests/persistent_cache.rs`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `examples/browser-delta-sandbox/src/axon-browser-sdk.ts`
- Modify: `examples/browser-delta-sandbox/src/main.ts`

**Step 1: Write failing metrics propagation tests**

Add tests that prove:

- `BrowserObjectRangeReader` metrics include `bytes_fetched`, `bytes_reused`, `validation_misses`, and `persistent_cache_errors`.
- runtime/session exposes transport metrics after snapshot bootstrap and query execution.
- worker `cache_metrics.transport` is populated when transport metrics are available.
- TypeScript SDK rejects malformed transport metrics.

**Step 2: Implement propagation**

Do not invent a second cache model. Reuse `BrowserTransportMetrics` from `wasm-http-object-store` and map it into `BrowserWorkerTransportCacheMetrics`.

Rules:

- Cache remains fail-open for persistence.
- Metrics must not include raw URLs with query strings or fragments.
- Metrics are optional when a path does not use `BrowserObjectRangeReader`.

**Step 3: Verify**

Run:

```bash
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-query-runtime --locked
cargo test -p wasm-query-session --locked
cargo test -p browser-engine-worker --locked
cargo test -p browser-sdk --locked
npm --prefix examples/browser-delta-sandbox test -- axon-browser-sdk.spec.ts
```

Expected: pass.

**Step 4: Commit**

```bash
git add crates/wasm-http-object-store/src/lib.rs crates/wasm-http-object-store/tests/extent_cache.rs crates/wasm-http-object-store/tests/persistent_cache.rs crates/wasm-query-runtime/src/lib.rs crates/wasm-query-session/src/lib.rs crates/browser-engine-worker/src/lib.rs crates/browser-sdk/src/lib.rs examples/browser-delta-sandbox/src/axon-browser-sdk.ts examples/browser-delta-sandbox/src/main.ts
git commit -m "feat: surface browser transport cache metrics"
```

## Task 9: Documentation And Release-Gate Updates

**Files:**

- Modify: `docs/program/browser-datafusion-size-audit.md`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Modify: `docs/program/browser-observability-contract.md`
- Modify: `docs/program/browser-embedding-deployment.md`
- Modify: `tests/perf/README.md`
- Modify: `examples/browser-delta-sandbox/README.md`

**Step 1: Document stable versus diagnostic fields**

Record:

- inspection JSON is stable SDK contract
- scan explain facts are stable SDK contract
- physical plan text is diagnostic and may change with DataFusion
- result preview is bounded convenience output, not a replacement for Arrow IPC
- transport cache metrics are operational telemetry

**Step 2: Document range/CORS requirements**

In embedding docs, call out:

- `HEAD` support or known object size in descriptor
- `Range` support
- exposed `Content-Length`, `Content-Range`, `Accept-Ranges`, and `ETag` where validation is used
- signed URL query string redaction

**Step 3: Update perf/release gates**

Make sure perf docs distinguish:

- shipped narrow worker artifact gate
- optional DataFusion engine size reporting
- inspection/explain features as contract/observability additions

**Step 4: Verify docs and artifact gates**

Run:

```bash
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/perf/report_browser_worker_artifact.sh
git diff --check
```

Expected: pass.

**Step 5: Commit**

```bash
git add docs/program/browser-datafusion-size-audit.md docs/program/browser-lakehouse-release-handoff.md docs/program/browser-observability-contract.md docs/program/browser-embedding-deployment.md tests/perf/README.md examples/browser-delta-sandbox/README.md
git commit -m "docs: record browser parquet diagnostics contracts"
```

## Final Verification

Run the smallest complete gate set that covers all touched surfaces:

```bash
cargo fmt -p query-contract -p wasm-parquet-engine -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker -p wasm-datafusion-poc
cargo test -p query-contract --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-query-runtime --locked
cargo test -p wasm-query-session --locked
cargo test -p wasm-query-session --features datafusion --locked
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
cargo test -p wasm-datafusion-poc --locked
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/perf/report_browser_worker_artifact.sh
npm --prefix examples/browser-delta-sandbox run lint
npm --prefix examples/browser-delta-sandbox run typecheck
npm --prefix examples/browser-delta-sandbox test
git diff --check
```

Expected:

- all Rust tests pass
- browser SDK lint/typecheck/tests pass
- default shipped worker still excludes DataFusion unless explicitly feature-enabled
- worker artifact remains under budget
- no unrelated files are staged

## Acceptance Criteria

- Browser users can inspect Parquet file/row-group/column metadata through a structured SDK call.
- SQL responses can include explain facts when `include_explain` is true.
- Explain payload preserves Axon/DataFusion ownership boundaries.
- Query responses can include bounded row previews while preserving Arrow IPC as the result transport.
- Browser sandbox renders inspection, explain, preview, range-read, and cache telemetry.
- URL and local-dev file workflows are ergonomic without adding cloud secret handling to the runtime.
- Transport cache metrics are propagated without exposing raw secret-bearing URLs.
- Shipped worker dependency and artifact gates still pass.
