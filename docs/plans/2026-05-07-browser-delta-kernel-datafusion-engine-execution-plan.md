# Browser Delta Kernel DataFusion Engine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:test-driven-development for each code task. Use superpowers:verification-before-completion before any completion claim.

**Goal:** Add the Delta Kernel protocol layer on top of the completed browser DataFusion table/scan/session work, so Delta Kernel owns Delta semantics while the existing Axon DataFusion engine owns SQL execution.

**Architecture:** `delta-kernel-rs` core resolves Delta semantics through an Axon-supplied browser Engine that reads only from prefetched cached bytes. The Kernel layer emits snapshot and scan descriptors consumed by a DataFusion `AxonDeltaTableProvider`, which returns an `AxonParquetScanExec` streaming Arrow `RecordBatch` values from `wasm-parquet-engine` over `wasm-http-object-store`. The browser Worker/session boundary opens tables, executes SQL, enforces budgets and cancellation, and returns Arrow IPC streams.

**Tech Stack:** Rust workspace, Cargo locked builds, `wasm32-unknown-unknown`, `wasm-bindgen`, Delta Kernel Rust core with default engine disabled, DataFusion `=52.4.0`, Arrow `=57.3.0`, `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot` compatibility fixtures, `wasm-query-session`, `browser-engine-worker`, `browser-sdk`, Arrow IPC.

---

## Operating Principles

This plan is intentionally conservative. It treats dependency surface, protocol correctness, memory behavior, and browser compatibility as first-class product requirements, not cleanup work.

Non-negotiables:

- Delta protocol semantics come from `delta-kernel-rs` core, not a growing Axon protocol fork.
- Delta Kernel default engine features stay disabled in the browser crate.
- High-level `deltalake` stays native-only as oracle/reference unless a later feature audit changes that decision.
- Browser network I/O happens before Kernel callbacks or through an explicit cache-miss retry loop, never hidden inside synchronous handler methods.
- DataFusion owns SQL planning and physical execution.
- Axon owns table access, browser range reads, Parquet scan integration, budgets, cancellation, metrics, and Arrow IPC delivery.
- Every implementation slice starts with a failing test, then the smallest implementation, then verification.
- Every checkpoint has a rollback decision if dependencies, size, correctness, or browser behavior fail.

## Success Bar

The work is release-candidate quality only when all of these are true:

- `wasm-delta-kernel-engine` compiles for `wasm32-unknown-unknown`.
- Cargo-tree gates prove unwanted browser dependencies are absent from the Kernel browser crate: `tokio`, `reqwest`, `object_store`, high-level `deltalake`, native TLS stacks, and cloud SDKs.
- A read-only Delta snapshot resolves through `AxonBrowserKernelEngine` over prefetched cached log and checkpoint bytes.
- Kernel-derived descriptors preserve table version, schema, protocol metadata, partition columns, active files, file sizes, partition values, stats, and unsupported feature failures.
- `AxonDeltaTableProvider` registers Kernel-derived tables in DataFusion.
- `AxonParquetScanExec` streams Arrow batches from `wasm-parquet-engine` into DataFusion execution.
- Projection, partition pruning, file-stat pruning, row-group pruning, residual filters, and limits have explicit tests.
- Worker/session APIs open Delta tables, execute SQL, stream Arrow IPC, cancel active work, and report structured errors.
- Differential tests compare browser descriptors and SQL results against native `deltalake` / native DataFusion oracles.
- Size, startup, first query, first metadata query, first real query, bytes fetched, rows emitted, and peak memory are measured.

## Current Completed Work To Preserve

The `docs/plans/2026-05-05-browser-datafusion-strategy-execution-plan.md` work has already landed. Treat these as existing integration points, not work to redo:

- `crates/wasm-datafusion-poc` now has `WasmDataFusionEngine`, `DeltaTableDescriptor`, `DeltaActiveFile`, `AxonDeltaTableProvider`, `AxonParquetScanExec`, scan traces, budgets, and cancellation shape.
- `AxonParquetScanExec` streams browser-style Parquet range reads through `wasm-parquet-engine` and `wasm-http-object-store`.
- `wasm-query-session`, `browser-engine-worker`, and `browser-sdk` expose `OpenDeltaTable` plus SQL Arrow IPC flow over trusted `BrowserHttpSnapshotDescriptor` input.
- Existing DataFusion tests cover table registration, in-memory custom scans, Parquet scans, pushdown traces, budget failures, session routing, and worker IPC.

The remaining Delta Kernel work should adapt to these seams. Do not reimplement the DataFusion provider, scan execution plan, worker command flow, or budget model unless a Kernel compatibility test exposes a concrete bug.

The main compatibility adjustment is descriptor ownership: Kernel-specific snapshot/scan facts should be represented in `query-contract` or `wasm-delta-kernel-engine`, then converted into the existing `wasm_datafusion_poc::DeltaTableDescriptor`. Do not make `wasm-delta-kernel-engine` depend on `wasm-datafusion-poc`.

## Quality Gates

Run these gates at every checkpoint:

```bash
cargo test -p wasm-delta-kernel-engine --tests
cargo check -p wasm-delta-kernel-engine --target wasm32-unknown-unknown --locked
! cargo tree --target wasm32-unknown-unknown -i tokio -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i reqwest -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i object_store -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i deltalake -p wasm-delta-kernel-engine
cargo test -p wasm-datafusion-poc --tests
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
cargo test -p wasm-query-session --tests
cargo test -p browser-sdk --tests
cargo test -p browser-engine-worker --tests
git diff --check
```

Expected: all tests/checks pass and each inverse dependency command exits successfully because the crate is absent.

## Worktree And Branch Setup

Create a dedicated worktree before implementation so unrelated current edits do not leak into this work:

```bash
git worktree add ../axon-delta-kernel-datafusion -b feat/browser-delta-kernel-datafusion
cd ../axon-delta-kernel-datafusion
git status --short
```

Expected: clean worktree before the first code change. If the worktree is not clean, stop and resolve ownership before continuing.

---

### Task 1: Create The Browser Delta Kernel Crate Shell

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/wasm-delta-kernel-engine/Cargo.toml`
- Create: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/dependency_surface.rs`

**Step 1: Write the failing ownership and contract test**

```rust
#[test]
fn browser_kernel_engine_contract_is_read_only_and_custom_engine_only() {
    let contract = wasm_delta_kernel_engine::runtime_contract();

    assert_eq!(wasm_delta_kernel_engine::OWNER, "Runtime / engine team");
    assert!(contract.read_only);
    assert!(!contract.default_delta_kernel_engine_enabled);
    assert!(!contract.native_deltalake_enabled);
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test dependency_surface
```

Expected: FAIL because the crate does not exist.

**Step 2: Add the crate**

Add `crates/wasm-delta-kernel-engine` to the workspace. Start with the latest `delta_kernel` version confirmed by `cargo search delta_kernel --limit 1` or docs.rs at implementation time. As of this plan, `cargo search delta_kernel --limit 1` reports `delta_kernel = "0.22.0"`.

```toml
[package]
name = "wasm-delta-kernel-engine"
edition.workspace = true
license.workspace = true
version.workspace = true

[dependencies]
bytes = "=1.11.1"
delta_kernel = { version = "=0.22.0", default-features = false }
query-contract = { path = "../query-contract" }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = "=2.0.18"
url = "=2.5.8"
wasm-http-object-store = { path = "../wasm-http-object-store" }
wasm-parquet-engine = { path = "../wasm-parquet-engine" }
```

**Step 3: Implement the smallest public contract**

```rust
pub const OWNER: &str = "Runtime / engine team";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowserDeltaKernelRuntimeContract {
    pub read_only: bool,
    pub default_delta_kernel_engine_enabled: bool,
    pub native_deltalake_enabled: bool,
}

pub fn runtime_contract() -> BrowserDeltaKernelRuntimeContract {
    BrowserDeltaKernelRuntimeContract {
        read_only: true,
        default_delta_kernel_engine_enabled: false,
        native_deltalake_enabled: false,
    }
}

#[derive(Clone, Debug, Default)]
pub struct AxonBrowserKernelEngine;
```

**Step 4: Verify dependency surface**

```bash
cargo test -p wasm-delta-kernel-engine --test dependency_surface
cargo check -p wasm-delta-kernel-engine --target wasm32-unknown-unknown --locked
cargo tree --target wasm32-unknown-unknown -e features -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i tokio -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i reqwest -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i object_store -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i deltalake -p wasm-delta-kernel-engine
```

Expected: tests and wasm check pass. Inverse tree commands prove forbidden crates are absent.

**Step 5: Commit**

```bash
git add Cargo.toml crates/wasm-delta-kernel-engine
git commit -m "feat: add browser delta kernel engine crate"
```

---

### Task 2: Add Delta Log Cache And Trusted Manifest Model

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/log_cache.rs`

**Step 1: Write cache behavior tests**

```rust
use bytes::Bytes;
use wasm_delta_kernel_engine::{DeltaLogCache, DeltaLogObjectMeta};

#[test]
fn cache_returns_full_bytes_and_metadata_by_relative_path() {
    let mut cache = DeltaLogCache::default();
    cache.insert(
        "_delta_log/00000000000000000000.json",
        DeltaLogObjectMeta {
            size_bytes: 17,
            etag: Some("etag-0".to_string()),
        },
        Bytes::from_static(br#"{"protocol":{}}"#),
    ).unwrap();

    let entry = cache.get("_delta_log/00000000000000000000.json").unwrap();
    assert_eq!(entry.meta.size_bytes, 17);
    assert_eq!(entry.bytes.as_ref(), br#"{"protocol":{}}"#);
}

#[test]
fn cache_rejects_paths_outside_delta_log() {
    let mut cache = DeltaLogCache::default();
    let err = cache
        .insert("../secret", DeltaLogObjectMeta::default(), Bytes::new())
        .unwrap_err();

    assert!(err.to_string().contains("_delta_log"));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test log_cache
```

Expected: FAIL because cache types do not exist.

**Step 2: Implement cache primitives**

Implement:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeltaLogObjectMeta {
    pub size_bytes: u64,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaLogCacheEntry {
    pub meta: DeltaLogObjectMeta,
    pub bytes: bytes::Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct DeltaLogCache {
    files: std::collections::BTreeMap<String, DeltaLogCacheEntry>,
}
```

Rules:

- Accept only `_delta_log/...` relative paths.
- Reject `..`, absolute paths, empty paths, duplicate inserts with different bytes, and non-log paths.
- Use `BTreeMap` for deterministic listings.

**Step 3: Verify**

```bash
cargo test -p wasm-delta-kernel-engine --test log_cache
cargo test -p wasm-delta-kernel-engine --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/wasm-delta-kernel-engine/tests/log_cache.rs
git commit -m "feat: add delta log cache for browser kernel engine"
```

---

### Task 3: Implement Async Prefetch Outside Kernel Callbacks

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/prefetch_manifest.rs`

**Step 1: Write prefetch tests against loopback HTTP**

Use the same loopback style as existing browser snapshot tests. The test should construct a manifest with signed URLs and prove fetched bytes land in `DeltaLogCache` without exposing query strings in errors.

```rust
#[tokio::test]
async fn prefetch_manifest_fetches_log_bytes_before_kernel_resolution() {
    let server = test_server_with_objects([
        ("/_delta_log/00000000000000000000.json", br#"{"protocol":{}}"#),
        ("/_delta_log/_last_checkpoint", br#"{"version":0,"size":1}"#),
    ]).await;

    let manifest = BrowserDeltaKernelManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaKernelObject::new(
                "_delta_log/00000000000000000000.json",
                server.url("/_delta_log/00000000000000000000.json?sig=secret"),
            ),
            BrowserDeltaKernelObject::new(
                "_delta_log/_last_checkpoint",
                server.url("/_delta_log/_last_checkpoint?sig=secret"),
            ),
        ],
    ).unwrap();

    let cache = prefetch_delta_log_manifest(&manifest).await.unwrap();
    assert!(cache.contains("_delta_log/00000000000000000000.json"));
    assert!(cache.contains("_delta_log/_last_checkpoint"));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test prefetch_manifest
```

Expected: FAIL until manifest and prefetch APIs exist.

**Step 2: Implement manifest types**

Implement:

```rust
pub struct BrowserDeltaKernelManifest {
    pub table_uri: String,
    pub objects: Vec<BrowserDeltaKernelObject>,
}

pub struct BrowserDeltaKernelObject {
    pub relative_path: String,
    pub url: String,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
}
```

Rules:

- Validate object URLs with `query_contract::validate_browser_object_url`.
- Redact query strings in all diagnostics.
- Fetch only manifest-declared files.
- Store full bytes in `DeltaLogCache`.
- Leave arbitrary browser prefix listing out of V1.

**Step 3: Verify**

```bash
cargo test -p wasm-delta-kernel-engine --test prefetch_manifest
cargo test -p wasm-delta-kernel-engine --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/wasm-delta-kernel-engine/tests/prefetch_manifest.rs
git commit -m "feat: prefetch delta log manifests into cache"
```

---

### Task 4: Add Cached Storage Handler

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/storage_handler.rs`

**Step 1: Write storage handler tests**

```rust
#[test]
fn storage_handler_lists_cached_log_paths_in_sorted_order() {
    let cache = test_cache([
        "_delta_log/00000000000000000002.json",
        "_delta_log/00000000000000000000.json",
        "_delta_log/00000000000000000001.json",
    ]);
    let storage = CachedDeltaStorageHandler::new(cache);

    let paths = storage.cached_listing("_delta_log").unwrap();

    assert_eq!(paths, vec![
        "_delta_log/00000000000000000000.json",
        "_delta_log/00000000000000000001.json",
        "_delta_log/00000000000000000002.json",
    ]);
}

#[test]
fn storage_handler_reports_cache_miss_without_network_io() {
    let storage = CachedDeltaStorageHandler::new(DeltaLogCache::default());
    let err = storage.cached_read("_delta_log/00000000000000000000.json").unwrap_err();

    assert!(err.to_string().contains("cache miss"));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test storage_handler
```

Expected: FAIL until storage handler exists.

**Step 2: Implement cached helper methods first**

Before implementing Delta Kernel's exact `StorageHandler` trait, implement and test local helpers:

```rust
pub struct CachedDeltaStorageHandler {
    cache: std::sync::Arc<DeltaLogCache>,
}

impl CachedDeltaStorageHandler {
    pub fn cached_listing(&self, prefix: &str) -> Result<Vec<String>, BrowserDeltaKernelError>;
    pub fn cached_read(&self, relative_path: &str) -> Result<bytes::Bytes, BrowserDeltaKernelError>;
    pub fn cached_head(&self, relative_path: &str) -> Result<DeltaLogObjectMeta, BrowserDeltaKernelError>;
}
```

Then implement Delta Kernel's `StorageHandler` trait against the actual versioned API. Trait methods must call only these cached helpers.

**Step 3: Verify no async dependency leaks into handler**

```bash
cargo test -p wasm-delta-kernel-engine --test storage_handler
cargo check -p wasm-delta-kernel-engine --target wasm32-unknown-unknown --locked
! cargo tree --target wasm32-unknown-unknown -i tokio -p wasm-delta-kernel-engine
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/wasm-delta-kernel-engine/tests/storage_handler.rs
git commit -m "feat: add cached delta kernel storage handler"
```

---

### Task 5: Add JSON And Evaluation Handler Minimums

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/kernel_json_eval.rs`

**Step 1: Write JSON parsing and evaluator tests**

```rust
#[test]
fn json_handler_parses_line_delimited_commit_actions() {
    let handler = AxonKernelJsonHandler::default();
    let rows = handler
        .parse_commit_json_lines(br#"{"protocol":{"minReaderVersion":1}}"#)
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn evaluation_handler_supports_partition_equality() {
    let evaluator = AxonKernelEvaluationHandler::default();
    let row = [("country".to_string(), Some("US".to_string()))].into();

    assert!(evaluator.partition_eq(&row, "country", "US").unwrap());
    assert!(!evaluator.partition_eq(&row, "country", "CA").unwrap());
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test kernel_json_eval
```

Expected: FAIL until handlers exist.

**Step 2: Implement minimal local semantics**

Implement `AxonKernelJsonHandler` over `serde_json` and `AxonKernelEvaluationHandler` over simple scalar/partition predicates needed for metadata filtering. Keep DataFusion out of Kernel internals unless measurement later proves reuse is better.

**Step 3: Implement Delta Kernel traits**

Implement the actual `JsonHandler` and `EvaluationHandler` traits for the selected `delta_kernel` version. Keep unsupported expression forms explicit:

```rust
return Err(BrowserDeltaKernelError::UnsupportedExpression {
    expression: expression_name.to_string(),
});
```

**Step 4: Verify**

```bash
cargo test -p wasm-delta-kernel-engine --test kernel_json_eval
cargo test -p wasm-delta-kernel-engine --tests
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/wasm-delta-kernel-engine/tests/kernel_json_eval.rs
git commit -m "feat: add browser delta kernel json and evaluation handlers"
```

---

### Task 6: Add Parquet Handler For Checkpoint Reads

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/kernel_checkpoint_parquet.rs`

**Step 1: Write checkpoint fixture test**

Use an existing checkpoint fixture from `crates/wasm-delta-snapshot/tests/checkpoints.rs` or extract a small shared fixture helper.

```rust
#[test]
fn parquet_handler_reads_checkpoint_actions_from_cached_bytes() {
    let checkpoint = include_bytes!("fixtures/00000000000000000001.checkpoint.parquet");
    let handler = AxonKernelParquetHandler::for_cached_checkpoint_bytes(checkpoint.as_slice());

    let actions = handler.read_checkpoint_actions_for_test().unwrap();

    assert!(actions.iter().any(|action| action.is_add()));
    assert!(actions.iter().any(|action| action.is_metadata()));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test kernel_checkpoint_parquet
```

Expected: FAIL until the handler can decode checkpoint Parquet.

**Step 2: Expose only the needed Parquet engine helper**

If `wasm-parquet-engine` already has the needed metadata and batch scan primitive, reuse it. If not, add the smallest public helper that reads checkpoint rows from bytes without taking a network dependency.

Do not add Delta parsing logic to `wasm-parquet-engine`; it should return Arrow/Parquet data, not Delta actions.

**Step 3: Implement read-only Parquet handler behavior**

Support:

- checkpoint Parquet reads
- Parquet footer reads if required by Kernel snapshot resolution
- read-only errors for writes

Return a structured read-only error for write methods.

**Step 4: Verify**

```bash
cargo test -p wasm-delta-kernel-engine --test kernel_checkpoint_parquet
cargo test -p wasm-parquet-engine --tests
cargo check -p wasm-delta-kernel-engine --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/wasm-parquet-engine/src/lib.rs crates/wasm-delta-kernel-engine/tests/kernel_checkpoint_parquet.rs
git commit -m "feat: add read-only delta kernel parquet handler"
```

**Batch 2 residual risks to carry forward**

- `AxonKernelJsonHandler` and `AxonKernelEvaluationHandler` are intentionally minimal at this point. Unsupported Kernel expression and `EngineData` paths must continue returning structured errors until later snapshot and residual-filter tasks expand coverage.
- `AxonKernelParquetHandler` is intentionally read-only and scoped to checkpoint Parquet reads. Keep write paths as structured read-only errors and keep Delta parsing out of `wasm-parquet-engine`.
- Full snapshot resolution and descriptor conversion are still Task 7 work; native oracle parity remains Task 8 work.
- The existing vendored `delta_kernel` `ColumnTrie::is_terminal` dead-code warning was observed during Batch 2 verification. Track it as a vendored upstream warning unless it becomes a hard compiler or lint failure.

---

### Task 7: Resolve Snapshot And Convert To Axon Descriptor

**Files:**
- Modify: `crates/wasm-delta-kernel-engine/src/lib.rs`
- Modify: `crates/query-contract/src/lib.rs`
- Create: `crates/wasm-delta-kernel-engine/tests/snapshot_descriptor.rs`

**Step 1: Write descriptor conversion test**

```rust
#[tokio::test]
async fn kernel_snapshot_converts_to_axon_descriptor() {
    let manifest = fixture_manifest("simple_partitioned_delta_table");

    let descriptor = resolve_delta_snapshot_from_manifest(manifest)
        .await
        .unwrap();

    assert_eq!(descriptor.table_version, 2);
    assert_eq!(descriptor.partition_columns, vec!["event_date"]);
    assert!(descriptor.active_files.iter().all(|file| file.size_bytes > 0));
    assert!(descriptor.schema.fields().iter().any(|field| field.name() == "event_type"));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test snapshot_descriptor
```

Expected: FAIL until snapshot resolution exists.

**Step 2: Define descriptor types in the right boundary**

Prefer shared descriptor types in `query-contract` if they cross crate or worker boundaries. Keep Kernel-specific types private to `wasm-delta-kernel-engine`.

Required descriptor fields:

```rust
pub struct AxonDeltaSnapshotDescriptor {
    pub table_uri: String,
    pub table_version: i64,
    pub schema_ipc: Vec<u8>,
    pub partition_columns: Vec<String>,
    pub active_files: Vec<AxonDeltaActiveFile>,
    pub protocol: AxonDeltaProtocolDescriptor,
    pub metadata: AxonDeltaMetadataDescriptor,
}

pub struct AxonDeltaActiveFile {
    pub path: String,
    pub url: String,
    pub size_bytes: u64,
    pub partition_values: std::collections::BTreeMap<String, Option<String>>,
    pub stats_json: Option<String>,
    pub deletion_vector: Option<AxonDeletionVectorDescriptor>,
}
```

**Step 3: Implement snapshot open flow**

```text
BrowserDeltaKernelManifest
  -> prefetch_delta_log_manifest
  -> DeltaLogCache
  -> AxonBrowserKernelEngine
  -> delta_kernel snapshot APIs
  -> AxonDeltaSnapshotDescriptor
```

**Step 4: Verify against existing browser snapshot fixtures**

```bash
cargo test -p wasm-delta-kernel-engine --test snapshot_descriptor
cargo test -p wasm-delta-snapshot --tests
cargo test -p delta-control-plane --tests
```

Expected: PASS and descriptor facts match existing fixture expectations.

**Step 5: Commit**

```bash
git add crates/wasm-delta-kernel-engine/src/lib.rs crates/query-contract/src/lib.rs crates/wasm-delta-kernel-engine/tests/snapshot_descriptor.rs
git commit -m "feat: resolve kernel snapshots into browser descriptors"
```

---

### Task 8: Add Native Oracle Differential Tests

**Files:**
- Create: `crates/wasm-delta-kernel-engine/tests/native_oracle.rs`
- Modify: `crates/delta-control-plane/tests/support/mod.rs`

**Step 1: Write oracle comparison test**

```rust
#[tokio::test]
async fn kernel_descriptor_matches_native_deltalake_oracle() {
    let fixture = delta_control_plane::tests::support::local_delta_fixture().await;

    let native = load_native_oracle_descriptor(&fixture.table_uri).await.unwrap();
    let browser = resolve_fixture_with_browser_kernel(&fixture).await.unwrap();

    assert_eq!(browser.table_version, native.table_version);
    assert_eq!(browser.partition_columns, native.partition_columns);
    assert_eq!(browser.active_files.len(), native.active_files.len());
    assert_eq!(sorted_paths(&browser), sorted_paths(&native));
}
```

Run:

```bash
cargo test -p wasm-delta-kernel-engine --test native_oracle
```

Expected: FAIL until oracle helpers exist.

**Step 2: Implement oracle helper**

Use native `deltalake` only in non-wasm dev-dependencies. Do not add it to normal browser dependencies.

**Step 3: Add fixture matrix**

Cover:

- JSON-only table
- checkpoint plus JSON replay
- partitioned table
- stats-bearing add files
- unsupported deletion vectors or column mapping as expected hard failures

**Step 4: Verify**

```bash
cargo test -p wasm-delta-kernel-engine --test native_oracle
cargo tree --target wasm32-unknown-unknown -i deltalake -p wasm-delta-kernel-engine
```

Expected: oracle tests pass on host. The wasm-target inverse tree command proves `deltalake` is absent from browser dependencies.

**Step 5: Commit**

```bash
git add crates/wasm-delta-kernel-engine/tests/native_oracle.rs crates/delta-control-plane/tests/support/mod.rs
git commit -m "test: compare browser kernel snapshots with native oracle"
```

---

### Task 9: Validate Existing DataFusion Engine Facade

**Files:**
- Reference: `crates/wasm-datafusion-poc/src/lib.rs`
- Reference: `crates/wasm-datafusion-poc/tests/memtable_query.rs`
- Reference: `crates/wasm-datafusion-poc/tests/arrow_ipc_export.rs`
- Modify only if needed: `crates/wasm-datafusion-poc/src/lib.rs`

**Purpose:** Confirm the already-landed `WasmDataFusionEngine` facade is sufficient for Kernel-derived descriptors.

**Step 1: Verify existing engine lifecycle tests**

```rust
#[tokio::test]
async fn engine_registers_batches_and_runs_sql_to_ipc() {
    let mut engine = WasmDataFusionEngine::new();
    let batch = synthetic_record_batch().unwrap();

    engine
        .register_record_batches("events", batch.schema(), vec![batch])
        .await
        .unwrap();

    let ipc = engine
        .sql_to_arrow_ipc("SELECT category, COUNT(*) FROM events GROUP BY category")
        .await
        .unwrap();

    assert!(!ipc.is_empty());
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test memtable_query
cargo test -p wasm-datafusion-poc --test arrow_ipc_export
```

Expected: PASS. If these fail, fix the existing facade before adding Kernel adapters.

**Step 2: Check Kernel-facing requirements**

The existing facade must keep:

- `open_delta_table(DeltaTableDescriptor)`
- Arrow schema ownership inside the DataFusion crate
- `sql_to_record_batches_with_metrics`
- `sql_to_arrow_ipc_result`
- query budget and cancellation accessors

If Kernel descriptors need more fields, add a conversion layer rather than moving Kernel types into this crate.

**Step 3: Verify**

```bash
cargo test -p wasm-datafusion-poc --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-datafusion-poc
git commit -m "test: validate datafusion facade for kernel descriptors"
```

---

### Task 10: Add Kernel Descriptor Adapter To Existing DataFusion Tables

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/Cargo.toml`
- Modify: `crates/wasm-datafusion-poc/tests/delta_table_provider.rs`
- Modify: `crates/query-contract/src/lib.rs`

**Step 1: Write adapter test**

```rust
#[tokio::test]
async fn engine_registers_kernel_descriptor_adapter_as_datafusion_table() {
    let mut engine = WasmDataFusionEngine::new();
    let kernel_descriptor = test_kernel_delta_descriptor();
    let descriptor = DeltaTableDescriptor::try_from_kernel_descriptor("events", kernel_descriptor).unwrap();

    engine.open_delta_table(descriptor).await.unwrap();

    assert!(engine.table_names().contains(&"events".to_string()));
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test delta_table_provider
```

Expected: FAIL until the Kernel descriptor adapter exists.

**Step 2: Add shared Kernel descriptor DTOs**

Add Kernel-derived DTOs in `query-contract` if they cross crate or worker boundaries. Keep Delta Kernel API types private to `wasm-delta-kernel-engine`.

Required fields: table URI, version, logical schema, partition columns, partition types, active files, file sizes, partition values, stats, deletion vector descriptor, protocol metadata, and table metadata.

**Step 3: Convert to existing `DeltaTableDescriptor`**

Convert Kernel DTOs into the existing DataFusion descriptor. Preserve object identity, stats, deletion vector facts, and logical schema. Do not force Parquet bootstrap just to derive the schema when Kernel already supplied it.

**Step 4: Verify**

```bash
cargo test -p wasm-datafusion-poc --test delta_table_provider
cargo test -p wasm-datafusion-poc --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/query-contract/src/lib.rs crates/wasm-datafusion-poc/Cargo.toml crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/tests/delta_table_provider.rs
git commit -m "feat: adapt kernel descriptors to datafusion tables"
```

---

### Task 11: Validate Existing Axon Parquet Scan ExecutionPlan

**Files:**
- Reference: `crates/wasm-datafusion-poc/src/lib.rs`
- Reference: `crates/wasm-datafusion-poc/tests/custom_scan_exec.rs`
- Modify only if needed: `crates/wasm-datafusion-poc/src/lib.rs`

**Purpose:** Keep the already-landed `AxonParquetScanExec` and add only Kernel compatibility checks.

**Step 1: Verify existing in-memory custom scan tests**

```rust
#[tokio::test]
async fn datafusion_executes_operators_above_custom_scan() {
    let mut engine = WasmDataFusionEngine::new();
    let descriptor = test_delta_descriptor_with_batches(vec![synthetic_record_batch().unwrap()]);

    engine.open_delta_table("events", descriptor).await.unwrap();

    let (_schema, batches) = engine
        .sql_to_record_batches("SELECT category, COUNT(*) FROM events GROUP BY category")
        .await
        .unwrap();

    assert!(!batches.is_empty());
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test custom_scan_exec
```

Expected: PASS.

**Step 2: Add Kernel descriptor scan fixture only if needed**

If the Kernel adapter changes descriptor semantics, add one fixture that creates a Kernel-derived descriptor and proves the existing scan still lets DataFusion execute filter, sort, limit, and aggregate operators above the scan.

**Step 3: Verify**

```bash
cargo test -p wasm-datafusion-poc --test custom_scan_exec
cargo test -p wasm-datafusion-poc --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/tests/custom_scan_exec.rs
git commit -m "test: validate custom datafusion scan for kernel descriptors"
```

---

### Task 12: Validate Browser Parquet Streaming With Kernel Descriptors

**Files:**
- Reference: `crates/wasm-datafusion-poc/Cargo.toml`
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs`

**Step 1: Verify existing Parquet-backed SQL tests**

```rust
#[tokio::test]
async fn sql_reads_delta_parquet_file_through_custom_scan() {
    let fixture = test_parquet_delta_descriptor().await;
    let mut engine = WasmDataFusionEngine::new();

    engine.open_delta_table("events", fixture.descriptor).await.unwrap();

    let (_schema, batches) = engine
        .sql_to_record_batches("SELECT event_type FROM events WHERE amount > 0 LIMIT 2")
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 2);
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test parquet_scan_exec
```

Expected: PASS.

**Step 2: Verify Kernel active files convert to scan targets**

Map Kernel-derived active file DTOs into the existing `DeltaActiveFile` and then into `wasm-parquet-engine` scan targets with URL, path, size, ETag, partition values, and requested projection.

**Step 3: Implement stream adapter**

Use DataFusion's stream adapter types where available. Do not collect all scan output before returning to DataFusion.

**Step 4: Verify**

```bash
cargo test -p wasm-datafusion-poc --test parquet_scan_exec
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-poc/Cargo.toml crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs
git commit -m "test: validate parquet streaming for kernel descriptors"
```

---

### Task 13: Validate Pushdown And Residual Correctness With Kernel Descriptors

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Create: `crates/wasm-datafusion-poc/tests/scan_pushdown.rs`

**Step 1: Verify existing trace-based pushdown tests**

```rust
#[tokio::test]
async fn provider_pushes_projection_partition_filter_and_limit() {
    let mut engine = WasmDataFusionEngine::with_scan_trace();
    engine.open_delta_table("events", test_partitioned_descriptor()).await.unwrap();

    engine
        .sql_to_record_batches(
            "SELECT event_type FROM events WHERE event_date = DATE '2026-01-01' LIMIT 5",
        )
        .await
        .unwrap();

    let trace = engine.scan_trace();
    assert_eq!(trace.projected_columns, vec!["event_type"]);
    assert_eq!(trace.limit, Some(5));
    assert!(trace.files_skipped_by_partition > 0);
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown
```

Expected: PASS.

**Step 2: Add Kernel-specific pushdown cases**

Classify every DataFusion filter as one of:

- exact partition prune
- inexact Delta stats prune
- inexact Parquet row-group prune
- unsupported residual

Only report exact filter support to DataFusion when the scan fully enforces semantics. Keep residual filters in DataFusion otherwise. Add a regression test for `WHERE non_partition_col = ... LIMIT ...` so scan-level limit pushdown cannot truncate rows before residual filters are applied.

**Step 3: Verify residual correctness**

Add tests where Delta stats skip some files but DataFusion must still evaluate row-level predicates. Include nulls and string/integer comparisons.

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown
cargo test -p wasm-datafusion-poc --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-parquet-engine/src/lib.rs crates/wasm-datafusion-poc/tests/scan_pushdown.rs
git commit -m "test: validate pushdown correctness for kernel descriptors"
```

---

### Task 14: Add Kernel-Aware Session And Worker Open Path

**Files:**
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `crates/browser-engine-worker/tests/wasm_smoke.rs`

**Step 1: Write Kernel descriptor IPC contract test**

```rust
#[test]
fn sdk_encodes_open_delta_table_and_sql_commands() {
    let open = BrowserCommand::OpenDeltaTable {
        table_name: "events".to_string(),
        descriptor: test_kernel_descriptor_for_ipc(),
    };
    let sql = BrowserCommand::Sql {
        query_id: "q1".to_string(),
        query: "SELECT COUNT(*) FROM events".to_string(),
        output: QueryOutputFormat::ArrowIpcStream,
    };

    assert_round_trips(open);
    assert_round_trips(sql);
}
```

Run:

```bash
cargo test -p browser-sdk --test ipc
cargo test -p browser-engine-worker --tests
```

Expected: FAIL until the Kernel-aware open path exists. Existing trusted snapshot `OpenDeltaTable` should keep passing.

**Step 2: Extend session API without breaking trusted snapshot opens**

Add:

```rust
impl QuerySession {
    pub async fn open_delta_table(&mut self, table_name: String, descriptor: AxonDeltaSnapshotDescriptor) -> Result<(), QueryError>;
    pub async fn sql_arrow_ipc_stream(&mut self, query_id: String, sql: String) -> Result<ArrowIpcStreamHandle, QueryError>;
    pub fn cancel_query(&mut self, query_id: &str) -> Result<(), QueryError>;
}
```

**Step 3: Implement Worker routing**

Route:

```text
OpenDeltaTable -> validate descriptor -> session.open_delta_table
Sql -> session.sql_arrow_ipc_stream -> chunked Arrow IPC responses
CancelQuery -> cancellation token -> scan and output stream stop
```

**Step 4: Verify**

```bash
cargo test -p wasm-query-session --tests
cargo test -p browser-sdk --tests
cargo test -p browser-engine-worker --tests
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-query-session/src/lib.rs crates/browser-engine-worker/src/lib.rs crates/browser-sdk/src/lib.rs crates/browser-sdk/tests/ipc.rs crates/browser-engine-worker/tests/wasm_smoke.rs
git commit -m "feat: add kernel descriptor browser open path"
```

---

### Task 15: Validate Budgets, Cancellation, Metrics, And Observability

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/query_budgets.rs`
- Create only if missing: `crates/wasm-datafusion-poc/tests/query_cancellation.rs`

**Step 1: Verify existing budget tests**

```rust
#[tokio::test]
async fn query_budget_rejects_output_over_limit() {
    let mut engine = WasmDataFusionEngine::with_budget(BrowserQueryBudget {
        max_output_ipc_bytes: Some(128),
        ..Default::default()
    });
    engine.register_record_batches("events", synthetic_schema(), vec![synthetic_record_batch().unwrap()]).await.unwrap();

    let err = engine.sql_to_arrow_ipc("SELECT * FROM events").await.unwrap_err();

    assert_eq!(err.code, QueryErrorCode::BudgetExceeded);
}
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test query_budgets
```

Expected: PASS.

**Step 2: Add Kernel metrics fields only if needed**

```rust
#[derive(Clone, Debug, Default)]
pub struct BrowserQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_range_requests: Option<u64>,
    pub max_rows_returned: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
}
```

**Step 3: Close cancellation gaps**

Cancellation must reach:

- DataFusion stream consumer
- `AxonParquetScanExec`
- `wasm-parquet-engine` scan loop
- range-read scheduling where practical
- Arrow IPC output writer

**Step 4: Add metrics**

Track:

- table version
- files considered/skipped/read
- row groups considered/skipped/read
- range requests
- bytes fetched
- rows emitted
- projected columns
- residual filters
- elapsed timing by phase
- cancellation and budget failure reason
- Kernel snapshot resolution duration
- Kernel cache misses / prefetch bytes

**Step 5: Verify**

```bash
cargo test -p wasm-datafusion-poc --test query_budgets
cargo test -p wasm-datafusion-poc --test query_cancellation
cargo test -p wasm-query-session --tests
cargo test -p browser-engine-worker --tests
```

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-query-session/src/lib.rs crates/browser-engine-worker/src/lib.rs crates/wasm-datafusion-poc/tests/query_budgets.rs crates/wasm-datafusion-poc/tests/query_cancellation.rs
git commit -m "feat: connect kernel metrics to browser query observability"
```

---

### Task 16: Add End-To-End Differential SQL Corpus

**Files:**
- Create: `tests/conformance/browser-delta-kernel-datafusion-corpus.json`
- Create: `crates/wasm-datafusion-poc/tests/delta_sql_corpus.rs`
- Modify: `crates/native-query-runtime/tests/native_runtime.rs`

**Step 1: Add corpus**

Include:

```json
[
  { "name": "limit", "sql": "SELECT * FROM events LIMIT 10" },
  { "name": "projection_filter", "sql": "SELECT event_type, amount FROM events WHERE event_date = DATE '2026-01-01' LIMIT 100" },
  { "name": "count_partition", "sql": "SELECT COUNT(*) FROM events WHERE event_date = DATE '2026-01-01'" },
  { "name": "grouped_count", "sql": "SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY COUNT(*) DESC LIMIT 20" },
  { "name": "min_max_count", "sql": "SELECT MIN(ts), MAX(ts), COUNT(*) FROM events" }
]
```

**Step 2: Write differential runner**

For each case:

- resolve descriptor with browser Kernel engine
- register descriptor in DataFusion
- execute SQL in browser engine path
- execute SQL in native oracle path
- compare Arrow batches after deterministic sorting where needed

**Step 3: Verify**

```bash
cargo test -p wasm-datafusion-poc --test delta_sql_corpus
cargo test -p native-query-runtime --tests
```

Expected: PASS.

**Step 4: Commit**

```bash
git add tests/conformance/browser-delta-kernel-datafusion-corpus.json crates/wasm-datafusion-poc/tests/delta_sql_corpus.rs crates/native-query-runtime/tests/native_runtime.rs
git commit -m "test: add delta kernel datafusion differential corpus"
```

---

### Task 17: Add Size, Dependency, And Browser Smoke Gates

**Files:**
- Modify: `tests/perf/report_datafusion_wasm_size.sh`
- Modify: `tests/perf/report_datafusion_wasm_size_test.sh`
- Create: `tests/perf/report_delta_kernel_wasm_size.sh`
- Create: `tests/perf/browser_delta_kernel_datafusion_smoke.sh`
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/program/browser-datafusion-size-audit.md`

**Step 1: Write perf script test**

```bash
bash tests/perf/report_datafusion_wasm_size_test.sh
bash tests/perf/report_delta_kernel_wasm_size.sh --dry-run
```

Expected: FAIL until scripts exist or include the new crate.

**Step 2: Add reports**

Reports must include:

- raw wasm size
- wasm-bindgen size
- wasm-opt size
- gzip size
- Brotli size
- cargo-tree forbidden dependency checks
- startup and first-query timing when browser runner is available

**Step 3: Add CI gates**

CI should run host tests by default and keep heavy size reports manual or scheduled until build cost is acceptable.

**Step 4: Verify**

```bash
bash tests/perf/report_datafusion_wasm_size_test.sh
bash tests/perf/report_delta_kernel_wasm_size.sh --dry-run
ruby -e 'require "yaml"; YAML.load_file(ARGV.fetch(0))' .github/workflows/ci.yml
git diff --check
```

Expected: PASS.

**Step 5: Commit**

```bash
git add tests/perf/report_datafusion_wasm_size.sh tests/perf/report_datafusion_wasm_size_test.sh tests/perf/report_delta_kernel_wasm_size.sh tests/perf/browser_delta_kernel_datafusion_smoke.sh .github/workflows/ci.yml docs/program/browser-datafusion-size-audit.md
git commit -m "chore: add browser delta kernel datafusion gates"
```

---

### Task 18: Final Hardening And Release Candidate Review

**Files:**
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`
- Modify: `docs/program/browser-datafusion-size-audit.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`

**Step 1: Run full verification**

```bash
cargo test -p wasm-delta-kernel-engine --tests
cargo check -p wasm-delta-kernel-engine --target wasm32-unknown-unknown --locked
! cargo tree --target wasm32-unknown-unknown -i tokio -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i reqwest -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i object_store -p wasm-delta-kernel-engine
! cargo tree --target wasm32-unknown-unknown -i deltalake -p wasm-delta-kernel-engine
cargo test -p wasm-datafusion-poc --tests
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
cargo test -p wasm-query-session --tests
cargo test -p browser-sdk --tests
cargo test -p browser-engine-worker --tests
cargo test -p native-query-runtime --tests
bash tests/perf/report_datafusion_wasm_size_test.sh
bash tests/perf/report_delta_kernel_wasm_size.sh --dry-run
ruby -e 'require "yaml"; YAML.load_file(ARGV.fetch(0))' .github/workflows/ci.yml
git diff --check
```

Expected: PASS.

**Step 2: Update release evidence**

Record:

- exact commands run
- dependency gate output
- artifact sizes
- query corpus result
- unsupported Delta features and fallback policy
- known residual risks, including any persistent vendored `delta_kernel` warnings such as `ColumnTrie::is_terminal`

**Step 3: Review against quality checklist**

Checklist:

- no hidden async network I/O in Kernel handler callbacks
- no native-only dependencies in browser Kernel crate
- no high-level `deltalake` in browser dependency graph
- no unbounded result collection in Worker path
- no row JSON transport for large results
- residual predicates remain correct
- cancellation is observable
- budget failures are deterministic
- errors redact signed URLs

**Step 4: Commit**

```bash
git add docs/program/browser-lakehouse-engine-strategy.md docs/program/browser-datafusion-size-audit.md docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
git commit -m "docs: record browser delta kernel datafusion release evidence"
```

---

## Rollback And Stop Conditions

Stop before expanding implementation scope if any of these occur:

- `delta_kernel` core cannot compile for `wasm32-unknown-unknown` without default engine/runtime dependencies.
- The Kernel API requires async handler behavior that cannot be isolated behind prefetch or retry.
- Checkpoint Parquet support cannot be implemented without pulling broad native dependencies.
- DataFusion scan integration cannot preserve residual predicate correctness.
- Artifact size growth exceeds the current browser budget before feature-splitting options are measured.
- Native oracle comparisons disagree on active files, schema, partition values, stats, or protocol feature handling.

Rollback policy:

- Keep existing `wasm-delta-snapshot` runtime path as fallback until Kernel descriptor parity is proven.
- Keep high-level `deltalake` native-only.
- Revert individual task commits rather than broad resetting.
- Preserve tests that exposed blockers; mark them ignored only with an issue link and owner.

## Execution Order

Recommended order:

1. Tasks 1-4: dependency and cache foundation.
2. Tasks 5-8: Kernel handler correctness and native parity.
3. Task 9: verify the already-landed DataFusion facade against Kernel descriptor needs.
4. Tasks 10-13: add Kernel descriptor adapters and compatibility tests for the existing DataFusion table/scan path.
5. Tasks 14-15: extend or verify worker/session API, budgets, cancellation, metrics, and Kernel observability.
6. Tasks 16-18: differential conformance, size gates, release evidence.

Do not reimplement DataFusion scan integration. The completed `2026-05-05` plan already landed that work. Do not make it the default Delta open path until the Kernel-derived descriptor contract passes native parity for the fixture matrix.

## Primary References

- Browser Lakehouse Engine Strategy: `docs/program/browser-lakehouse-engine-strategy.md`
- Browser DataFusion Size Audit: `docs/program/browser-datafusion-size-audit.md`
- Delta Kernel Rust API docs: <https://docs.rs/delta_kernel/latest/delta_kernel/>
- Delta Kernel Rust README: <https://github.com/delta-io/delta-kernel-rs>
- DataFusion `TableProvider` docs: <https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html>
