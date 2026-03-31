# Browser Lakehouse Engine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Industrialize Axon's browser read path into a Delta snapshot plus Parquet scan architecture inside the existing workspace, without splitting the repository or weakening the native security boundary.

**Architecture:** Add two new sibling crates, `crates/wasm-parquet-engine` and `crates/wasm-delta-snapshot`, beneath `crates/wasm-query-runtime`. Keep `crates/delta-control-plane` native and trusted, keep `crates/wasm-http-object-store` transport-focused, and make the runtime an orchestrator over snapshot reconstruction, Parquet scanning, planning, fallback, and metrics.

**Tech Stack:** Rust, Cargo workspace, `wasm-http-object-store`, `wasm-query-runtime`, `delta-control-plane`, `query-contract`, `parquet`, `reqwest`, wasm-bindgen test tooling, browser-worker IPC

---

### Task 1: Add workspace scaffolding for the new browser engine crates

**Files:**
- Modify: `Cargo.toml`
- Modify: `tests/conformance/verify_workspace_layout.sh`
- Create: `crates/wasm-parquet-engine/Cargo.toml`
- Create: `crates/wasm-parquet-engine/src/lib.rs`
- Create: `crates/wasm-delta-snapshot/Cargo.toml`
- Create: `crates/wasm-delta-snapshot/src/lib.rs`
- Modify: `README.md`

**Step 1: Write the failing workspace checks**

Extend `tests/conformance/verify_workspace_layout.sh` so it expects:

```bash
"crates/wasm-parquet-engine"
"crates/wasm-delta-snapshot"
```

**Step 2: Run the layout check to verify it fails**

Run:
```bash
bash tests/conformance/verify_workspace_layout.sh
```

Expected: FAIL with `missing required directory` for the new crate directories.

**Step 3: Add minimal crate scaffolds and workspace membership**

Create minimal crate manifests and `src/lib.rs` files with owner and responsibility constants such as:

```rust
pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-side Parquet planning and scan primitives.";
```

and:

```rust
pub const OWNER: &str = "Runtime / engine team";
pub const RESPONSIBILITY: &str = "Browser-safe Delta snapshot reconstruction.";
```

Then add both crates to `Cargo.toml` workspace members.
Update `README.md` so the workspace section and local validation commands acknowledge the new crates.

**Step 4: Run scaffold verification**

Run:
```bash
bash tests/conformance/verify_workspace_layout.sh
cargo check -p wasm-parquet-engine -p wasm-delta-snapshot --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add Cargo.toml tests/conformance/verify_workspace_layout.sh crates/wasm-parquet-engine crates/wasm-delta-snapshot README.md
git commit -m "feat: scaffold browser parquet and delta crates"
```

### Task 2: Harden `wasm-http-object-store` into the browser I/O substrate

**Files:**
- Modify: `crates/wasm-http-object-store/Cargo.toml`
- Modify: `crates/wasm-http-object-store/src/lib.rs`
- Modify: `crates/wasm-http-object-store/tests/http_range_reader.rs`
- Create: `crates/wasm-http-object-store/tests/extent_cache.rs`

**Step 1: Write failing tests for metadata probes, coalescing, and validation**

Add tests with shapes like:

```rust
#[tokio::test]
async fn coalesces_adjacent_ranges_before_fetch() {}

#[tokio::test]
async fn rejects_identity_drift_when_if_range_validation_fails() {}

#[tokio::test]
async fn surfaces_missing_exposed_headers_as_protocol_errors() {}
```

Cover:

- optional metadata probe behavior when size or identity is unknown
- coalesced extent reads rather than exact-range-only cache keys
- ETag or equivalent identity validation for cached reads
- failure behavior when browser-visible response headers are insufficient for validation

**Step 2: Run the transport tests to verify they fail**

Run:
```bash
cargo test -p wasm-http-object-store --locked
```

Expected: FAIL on the new test cases.

**Step 3: Implement the minimal I/O substrate changes**

Add:

- an extent-oriented cache key and cache entry model
- metadata probe helpers that can learn file size and object identity when needed
- request validation hooks for reusing cached extents safely
- browser-local-file-friendly abstractions where possible without adding Delta or Parquet semantics

Keep the crate transport-scoped. Do not add `_delta_log` parsing or Parquet schema logic here.

**Step 4: Run transport verification**

Run:
```bash
cargo test -p wasm-http-object-store --locked
cargo check -p wasm-http-object-store --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-http-object-store
git commit -m "feat: harden browser range transport and extent cache"
```

### Task 3: Extract `wasm-parquet-engine` from the runtime-owned Parquet path

**Files:**
- Modify: `crates/wasm-parquet-engine/Cargo.toml`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Create: `crates/wasm-parquet-engine/tests/metadata.rs`
- Create: `crates/wasm-parquet-engine/tests/scan.rs`
- Modify: `crates/wasm-query-runtime/Cargo.toml`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/src/parquet_support.rs`

**Step 1: Write failing tests for metadata and scan handoff**

Add tests with shapes like:

```rust
#[tokio::test]
async fn known_file_size_avoids_extra_metadata_round_trip() {}

#[tokio::test]
async fn scan_target_reads_footer_and_row_groups_from_object_source() {}
```

The new crate API should accept a scan target that includes object identity, path, and known size:

```rust
pub struct ScanTarget {
    pub path: String,
    pub size_bytes: u64,
}
```

**Step 2: Run the new Parquet engine tests and verify they fail**

Run:
```bash
cargo test -p wasm-parquet-engine --locked
```

Expected: FAIL because the crate does not yet own the scan path.

**Step 3: Move Parquet logic out of `wasm-query-runtime`**

Extract from `crates/wasm-query-runtime` into `crates/wasm-parquet-engine`:

- footer bootstrap
- metadata decode
- row-group or file-level planning helpers
- Arrow batch scan primitives for the supported runtime envelope

Use the Delta handoff contract as the stable seam:

- object source
- file path
- known file size
- partition values when needed for synthesized columns

**Step 4: Rewire runtime consumers**

Update `crates/wasm-query-runtime` to depend on `crates/wasm-parquet-engine` instead of owning those internals directly.

**Step 5: Run verification**

Run:
```bash
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-query-runtime --locked
cargo check -p wasm-parquet-engine -p wasm-query-runtime --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/wasm-parquet-engine crates/wasm-query-runtime
git commit -m "refactor: extract browser parquet engine crate"
```

### Task 4: Build `wasm-delta-snapshot` for browser-safe Delta state reconstruction

**Files:**
- Modify: `crates/wasm-delta-snapshot/Cargo.toml`
- Modify: `crates/wasm-delta-snapshot/src/lib.rs`
- Create: `crates/wasm-delta-snapshot/tests/checkpoints.rs`
- Create: `crates/wasm-delta-snapshot/tests/replay.rs`
- Create: `crates/wasm-delta-snapshot/tests/fixtures/`
- Modify: `crates/delta-control-plane/tests/support/mod.rs`

**Step 1: Write failing snapshot reconstruction tests**

Add tests with shapes like:

```rust
#[tokio::test]
async fn prefers_latest_complete_checkpoint_before_json_replay() {}

#[tokio::test]
async fn loads_v2_checkpoint_sidecars_when_present() {}

#[tokio::test]
async fn reconstructs_active_files_with_paths_sizes_and_partition_values() {}
```

Cover:

- `_last_checkpoint` handling
- classic checkpoints
- V2 checkpoint discovery
- sidecar resolution
- JSON replay after the selected checkpoint
- deterministic active-file ordering and tombstone handling

**Step 2: Run the new snapshot tests and verify they fail**

Run:
```bash
cargo test -p wasm-delta-snapshot --locked
```

Expected: FAIL because snapshot logic is not implemented yet.

**Step 3: Implement the handler-based snapshot layer**

Model the internal split after Delta Kernel:

```rust
trait StorageHandler {}
trait JsonHandler {}
trait ParquetHandler {}
```

The first implementation does not need the full upstream API surface, but it must keep storage access, JSON parsing, and Parquet checkpoint reading separated.

**Step 4: Add native cross-check coverage**

Add tests that compare browser-side snapshot reconstruction against `crates/delta-control-plane` output on the same local fixture tables, including path, size, partition values, and snapshot version.

**Step 5: Run verification**

Run:
```bash
cargo test -p wasm-delta-snapshot --locked
cargo test -p delta-control-plane --locked
```

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/wasm-delta-snapshot crates/delta-control-plane/tests/support/mod.rs
git commit -m "feat: add browser delta snapshot reconstruction"
```

### Task 5: Rewire `wasm-query-runtime` into an orchestration layer over snapshot and scan crates

**Files:**
- Modify: `crates/wasm-query-runtime/Cargo.toml`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/src/execution.rs`
- Modify: `crates/wasm-query-runtime/src/lowering.rs`
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `crates/query-router/src/lib.rs`

**Step 1: Write failing integration tests for the new handoff**

Add tests with shapes like:

```rust
#[tokio::test]
async fn runtime_prunes_files_from_delta_snapshot_before_opening_parquet() {}

#[tokio::test]
async fn runtime_falls_back_when_snapshot_features_exceed_browser_capability() {}
```

Cover:

- runtime consuming active files from `wasm-delta-snapshot`
- runtime passing known file size into `wasm-parquet-engine`
- pruning before Parquet opens when Delta metadata is sufficient
- fallback when snapshot features or operators exceed the browser envelope

**Step 2: Run the runtime tests to verify they fail**

Run:
```bash
cargo test -p wasm-query-runtime --locked
```

Expected: FAIL on the new integration cases.

**Step 3: Implement the orchestration refactor**

Make `crates/wasm-query-runtime` responsible for:

- query-shape analysis
- pruning policy
- execution planning
- metrics
- fallback reasons

Make it consume, not own:

- Delta snapshot reconstruction
- Parquet metadata and scan internals
- raw transport behavior

**Step 4: Update router integration**

Ensure `crates/query-router` can route browser vs native execution using the same structured fallback reasons the runtime now emits.

**Step 5: Run verification**

Run:
```bash
cargo test -p wasm-query-runtime --locked
cargo test -p query-router --locked
cargo check -p wasm-query-runtime -p query-router --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/wasm-query-runtime crates/query-router
git commit -m "refactor: make browser runtime orchestrate snapshot and scan crates"
```

### Task 6: Add worker and IPC readiness in `browser-sdk`

**Files:**
- Modify: `crates/browser-sdk/Cargo.toml`
- Modify: `crates/browser-sdk/src/lib.rs`
- Create: `crates/browser-sdk/tests/ipc.rs`
- Modify: `README.md`

**Step 1: Write failing IPC-surface tests**

Add tests with shapes like:

```rust
#[test]
fn browser_sdk_exposes_arrow_ipc_result_envelope() {}

#[test]
fn browser_sdk_preserves_structured_fallback_reason() {}
```

**Step 2: Run the SDK tests to verify they fail**

Run:
```bash
cargo test -p browser-sdk --locked
```

Expected: FAIL because the worker and IPC boundary is not implemented yet.

**Step 3: Implement the minimal browser SDK boundary**

Expose:

- a worker-friendly request envelope
- Arrow IPC result transport rather than row JSON
- structured fallback reason propagation

Do not move Delta or Parquet internals into this crate.

**Step 4: Run verification**

Run:
```bash
cargo test -p browser-sdk --locked
cargo check -p browser-sdk --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/browser-sdk README.md
git commit -m "feat: add browser sdk worker and ipc boundary"
```

### Task 7: Add release gates, security docs, and performance budgets

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `tests/perf/README.md`
- Modify: `tests/security/README.md`
- Create: `SECURITY.md`
- Modify: `README.md`

**Step 1: Write failing CI and documentation expectations**

Add checks for:

- `wasm32-unknown-unknown` compile coverage for `wasm-parquet-engine` and `wasm-delta-snapshot`
- browser tests where applicable
- binary-size reporting or budget enforcement
- documentation for browser compatibility, Delta compatibility, and security reporting

**Step 2: Run the affected verification commands and confirm the new expectations are not met yet**

Run:
```bash
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p browser-sdk --target wasm32-unknown-unknown --locked
```

Expected: FAIL until CI and docs are updated.

**Step 3: Implement the hardening work**

Add:

- CI coverage for the new browser crates
- documented size and benchmark gates
- `SECURITY.md`
- compatibility and release-gate updates for the new architecture

**Step 4: Run verification**

Run:
```bash
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p browser-sdk --target wasm32-unknown-unknown --locked
bash tests/conformance/verify_workspace_layout.sh
```

Expected: PASS.

**Step 5: Commit**

```bash
git add .github/workflows/ci.yml docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md tests/perf/README.md tests/security/README.md SECURITY.md README.md
git commit -m "chore: add browser engine release gates and security docs"
```

### Task 8: Final verification

**Files:**
- Verify only

**Step 1: Format**

Run:
```bash
cargo fmt --all
```

**Step 2: Run package tests**

Run:
```bash
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-delta-snapshot --locked
cargo test -p wasm-query-runtime --locked
cargo test -p delta-control-plane --locked
cargo test -p browser-sdk --locked
cargo test -p query-router --locked
```

**Step 3: Run wasm-target checks**

Run:
```bash
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p browser-sdk --target wasm32-unknown-unknown --locked
```

**Step 4: Run workspace-layout verification**

Run:
```bash
bash tests/conformance/verify_workspace_layout.sh
```

**Step 5: Record verification results in the implementation branch summary**

Capture:

- which packages passed
- which browser tests ran
- current binary-size measurements
- any remaining gaps before execution can continue into broader SDK or UI work
