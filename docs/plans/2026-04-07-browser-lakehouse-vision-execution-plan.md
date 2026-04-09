# Browser Lakehouse Vision Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Turn Axon's current browser lakehouse slices into a productized browser engine for read-heavy Delta and Parquet workloads, with worker-first execution, extent-based browser I/O, Arrow IPC transport, and explicit launch gates.

**Architecture:** Keep the current crate split and deepen it instead of inventing a new repository. `crates/wasm-http-object-store` becomes the browser transport and cache substrate, `crates/wasm-delta-snapshot` owns Delta protocol reconstruction and file-level pruning facts, `crates/wasm-parquet-engine` owns metadata and streaming scan primitives, and `crates/wasm-query-runtime` plus `crates/browser-sdk` plus `crates/browser-engine-worker` own orchestration, IPC, fallback, and artifact health.

**Tech Stack:** Rust workspace, `reqwest`, Arrow/Parquet, `deltalake` on the native side, wasm-bindgen test tooling, browser workers, Arrow IPC, CI bundle/perf/security gates

---

### Task 1: Freeze the browser engine contract around worker-first Arrow IPC

**Files:**
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Modify: `docs/program/browser-observability-contract.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `crates/browser-sdk/src/lib.rs`
- Modify: `crates/browser-sdk/tests/ipc.rs`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/worker_artifact.rs`

**Step 1: Write the failing contract tests**

Add tests that lock:

```rust
#[test]
fn browser_sdk_serializes_arrow_ipc_result_envelopes() {}

#[test]
fn worker_artifact_reports_runtime_sku_and_ipc_boundary() {}
```

Cover:

- Arrow IPC as the only large-result transport across the worker boundary
- structured fallback and terminal-error propagation
- explicit reporting of runtime SKU, access mode, and artifact identity

**Step 2: Run the SDK and worker tests to verify they fail**

Run:

```bash
cargo test -p browser-sdk -p browser-engine-worker --locked
```

Expected: FAIL because the current contract does not yet publish the full worker-first IPC and SKU surface.

**Step 3: Update the contract and runtime-owned reporting**

Make the worker boundary explicit in docs and code:

- Arrow IPC bytes are the result payload
- fallback reasons remain structured metadata, never row JSON
- runtime reports whether it is running the narrow scan runtime or the larger SQL runtime
- observability docs and release gates name the same contract

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
git add docs/program/browser-lakehouse-engine-strategy.md docs/program/browser-lakehouse-release-handoff.md docs/program/browser-observability-contract.md docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md crates/browser-sdk crates/browser-engine-worker
git commit -m "feat: freeze browser worker ipc contract"
```

### Task 2: Harden browser I/O into an extent-oriented substrate

**Files:**
- Modify: `crates/wasm-http-object-store/Cargo.toml`
- Modify: `crates/wasm-http-object-store/src/lib.rs`
- Modify: `crates/wasm-http-object-store/tests/http_range_reader.rs`
- Modify: `crates/wasm-http-object-store/tests/extent_cache.rs`
- Create: `crates/wasm-http-object-store/tests/persistent_cache.rs`
- Modify: `tests/security/README.md`
- Modify: `tests/perf/README.md`

**Step 1: Write the failing transport and cache tests**

Add tests with shapes like:

```rust
#[tokio::test]
async fn metadata_probe_learns_size_and_identity_before_suffix_reads() {}

#[tokio::test]
async fn extent_cache_coalesces_adjacent_ranges_and_reuses_subranges() {}

#[tokio::test]
async fn stale_extent_is_rejected_when_etag_changes() {}
```

Cover:

- explicit metadata probe behavior
- stable extent keys `(object, identity, start, end)`
- `ETag` or equivalent validation for cached extents
- browser-visible header requirements for range validation

**Step 2: Run the transport tests to verify they fail**

Run:

```bash
cargo test -p wasm-http-object-store --locked
```

Expected: FAIL on the new cache and validation cases.

**Step 3: Implement the minimal transport changes**

Add:

- metadata probe helpers for size and object identity
- extent-cache reuse instead of exact-range-only reuse
- optional persistent cache hooks for future OPFS or IndexedDB-backed storage
- security- and perf-facing metrics for bytes fetched, bytes reused, and validation misses

Keep this crate transport-scoped. Do not add Delta or SQL semantics here.

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-http-object-store --locked
cargo check -p wasm-http-object-store --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-http-object-store tests/security/README.md tests/perf/README.md
git commit -m "feat: harden browser range transport with extent cache"
```

### Task 3: Add browser-local and persistent cache paths without changing the trust boundary

**Files:**
- Modify: `crates/wasm-http-object-store/src/lib.rs`
- Create: `crates/wasm-http-object-store/tests/local_file.rs`
- Create: `crates/wasm-http-object-store/tests/wasm_smoke.rs`
- Modify: `README.md`
- Modify: `docs/program/browser-release-integration-runbook.md`

**Step 1: Write failing local-access tests**

Add tests with shapes like:

```rust
#[test]
fn local_file_adapter_exposes_range_reads_from_blob_like_sources() {}

#[wasm_bindgen_test]
fn browser_local_cache_smoke_reports_persistent_cache_mode() {}
```

Cover:

- local `Blob` or file-like range reads
- worker-safe cache mode reporting
- deterministic behavior when persistent cache is unavailable

**Step 2: Run the local-access tests to verify they fail**

Run:

```bash
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: FAIL because the local-file and browser-smoke path is not yet explicit.

**Step 3: Implement the minimal local path**

Add:

- a browser-local object adapter abstraction that can back Delta logs and Parquet files
- cache-mode reporting so callers can distinguish memory-only versus persistent cache
- docs that keep browser-local access separate from signed-URL or proxy access

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-http-object-store README.md docs/program/browser-release-integration-runbook.md
git commit -m "feat: add browser local-file and cache-mode path"
```

### Task 4: Deepen `wasm-delta-snapshot` into a Delta-kernel-style browser reader

**Files:**
- Modify: `crates/wasm-delta-snapshot/Cargo.toml`
- Modify: `crates/wasm-delta-snapshot/src/lib.rs`
- Modify: `crates/wasm-delta-snapshot/tests/checkpoints.rs`
- Modify: `crates/wasm-delta-snapshot/tests/replay.rs`
- Create: `crates/wasm-delta-snapshot/tests/compatibility.rs`
- Modify: `crates/wasm-delta-snapshot/tests/fixtures/README.md`
- Modify: `crates/delta-control-plane/tests/resolve_snapshot.rs`

**Step 1: Write the failing snapshot and compatibility tests**

Add tests with shapes like:

```rust
#[tokio::test]
async fn snapshot_prefers_latest_complete_checkpoint_and_sidecars() {}

#[tokio::test]
async fn add_actions_expose_pruning_facts_without_opening_parquet() {}

#[tokio::test]
async fn unsupported_protocol_features_classify_as_native_only_or_terminal() {}
```

Cover:

- `_last_checkpoint`
- classic and V2 checkpoints
- sidecar resolution
- log replay after checkpoint
- file-level pruning facts from `add` actions
- compatibility classification for advanced Delta features

**Step 2: Run the snapshot tests to verify they fail**

Run:

```bash
cargo test -p wasm-delta-snapshot -p delta-control-plane --locked
```

Expected: FAIL on the new compatibility and pruning expectations.

**Step 3: Implement the handler-based snapshot layer**

Keep internal seams separated:

```rust
trait StorageHandler {}
trait JsonHandler {}
trait ParquetHandler {}
```

The first implementation does not need the full upstream Delta Kernel API, but it must keep:

- storage access isolated from protocol logic
- JSON parsing isolated from checkpoint reading
- compatibility classification isolated from runtime routing

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-delta-snapshot -p delta-control-plane --locked
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-delta-snapshot crates/delta-control-plane/tests/resolve_snapshot.rs
git commit -m "feat: deepen browser delta snapshot protocol support"
```

### Task 5: Make `wasm-parquet-engine` the streaming scan and metadata layer

**Files:**
- Modify: `crates/wasm-parquet-engine/Cargo.toml`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/tests/metadata.rs`
- Modify: `crates/wasm-parquet-engine/tests/scan.rs`
- Create: `crates/wasm-parquet-engine/tests/streaming.rs`
- Modify: `crates/wasm-query-runtime/src/parquet_support.rs`

**Step 1: Write the failing Parquet tests**

Add tests with shapes like:

```rust
#[tokio::test]
async fn known_file_size_avoids_extra_metadata_round_trip() {}

#[tokio::test]
async fn stream_scan_yields_incremental_record_batches() {}
```

Cover:

- known-size metadata bootstrap
- suffix metadata fallback when size is unknown
- streaming `RecordBatch` production rather than eager collection

**Step 2: Run the Parquet engine tests to verify they fail**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
```

Expected: FAIL because the current engine is not yet fully streaming and known-size aware across all paths.

**Step 3: Implement the minimal scan-layer changes**

Add:

- stable scan-target input carrying object identity and known file size
- streaming scan primitives that the runtime can cancel by dropping the stream
- metadata and scan metrics needed by the runtime and perf docs

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-parquet-engine --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-parquet-engine crates/wasm-query-runtime/src/parquet_support.rs
git commit -m "feat: add streaming browser parquet scan layer"
```

### Task 6: Rewire `wasm-query-runtime` to prune early, stream results, and enforce budgets

**Files:**
- Modify: `crates/wasm-query-runtime/Cargo.toml`
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/src/execution.rs`
- Modify: `crates/wasm-query-runtime/src/lowering.rs`
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `crates/query-router/src/lib.rs`

**Step 1: Write the failing runtime tests**

Add tests with shapes like:

```rust
#[tokio::test]
async fn runtime_prunes_from_delta_add_facts_before_footer_reads() {}

#[tokio::test]
async fn runtime_streams_arrow_ipc_results_and_cancels_cleanly() {}

#[tokio::test]
async fn memory_budget_exceeded_routes_to_native_fallback() {}
```

Cover:

- early pruning from Delta facts
- incremental result streaming
- cancellation by dropping execution state
- operator or memory budgets that trigger structured native fallback

**Step 2: Run the runtime tests to verify they fail**

Run:

```bash
cargo test -p wasm-query-runtime -p query-router --locked
```

Expected: FAIL because the current runtime still owns narrower execution behavior and looser budgeting.

**Step 3: Implement the orchestration changes**

Make the runtime:

- consume pruning facts from `wasm-delta-snapshot` before opening Parquet
- consume streaming scan primitives from `wasm-parquet-engine`
- keep narrow runtime execution as the default SKU
- emit structured metrics and fallback reasons across the router boundary

**Step 4: Run verification**

Run:

```bash
cargo test -p wasm-query-runtime -p query-router --locked
cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-query-runtime crates/query-router
git commit -m "refactor: make browser runtime stream and prune early"
```

### Task 7: Split the browser runtime into narrow and SQL SKUs with hard size budgets

**Files:**
- Modify: `crates/wasm-query-runtime/Cargo.toml`
- Modify: `crates/browser-engine-worker/Cargo.toml`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/worker_artifact.rs`
- Modify: `tests/perf/report_browser_worker_artifact.sh`
- Modify: `tests/perf/README.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`

**Step 1: Write the failing artifact tests**

Add tests with shapes like:

```rust
#[test]
fn worker_artifact_reports_narrow_and_sql_skus() {}

#[test]
fn sql_sku_exceeding_budget_fails_release_gate() {}
```

**Step 2: Run the artifact verification to verify it fails**

Run:

```bash
cargo test -p browser-engine-worker --locked
bash tests/perf/report_browser_worker_artifact.sh
```

Expected: FAIL because the build does not yet publish separate SKU evidence or enforce both budgets.

**Step 3: Implement the minimal SKU split**

Add:

- a default narrow build for scan, filter, project, and current supported aggregates
- an opt-in larger SQL build flag
- artifact reporting that names which SKU was built and what features were enabled

**Step 4: Run verification**

Run:

```bash
cargo test -p browser-engine-worker --locked
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
bash tests/perf/report_browser_worker_artifact.sh
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-query-runtime/Cargo.toml crates/browser-engine-worker tests/perf/report_browser_worker_artifact.sh tests/perf/README.md docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
git commit -m "feat: add browser runtime sku split and size gates"
```

### Task 8: Close the launch-hardening loop with compatibility, security, and operator evidence

**Files:**
- Modify: `README.md`
- Modify: `docs/program/browser-dependency-compatibility-review-checklist.md`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- Modify: `tests/security/verify_browser_dependency_guardrails.sh`
- Modify: `tests/security/README.md`

**Step 1: Write the failing release-gate checks**

Add checks that require:

- browser compatibility matrix coverage for supported, native-only, and terminal states
- explicit evidence for worker size, startup, cache behavior, and fallback reporting
- explicit separation between repo-owned proof and external service dependencies

**Step 2: Run the release and security checks to verify they fail**

Run:

```bash
bash tests/security/verify_browser_dependency_guardrails.sh
```

Expected: FAIL because the new compatibility and evidence requirements are not yet encoded.

**Step 3: Update docs and guardrails**

Make the published release story consistent:

- README names the browser engine product shape accurately
- release docs point at real commands and artifacts
- external blockers remain explicit and owned
- compatibility review checklist matches the runtime and control-plane behavior

**Step 4: Run verification**

Run:

```bash
bash tests/security/verify_browser_dependency_guardrails.sh
cargo test -p browser-sdk -p browser-engine-worker -p wasm-query-runtime -p wasm-delta-snapshot -p wasm-parquet-engine --locked
```

Expected: PASS.

**Step 5: Commit**

```bash
git add README.md docs/program/browser-dependency-compatibility-review-checklist.md docs/program/browser-lakehouse-release-handoff.md docs/release-gates tests/security
git commit -m "docs: close browser engine launch-hardening gaps"
```

## Parallel External Track

This repository cannot finish launch alone. Run these items in parallel so they do not land on the engine critical path:

- `services/query-api` or equivalent trusted service for signed URL or proxy issuance
- signed URL TTL approval and audit logging review
- XML-endpoint CORS or origin validation against the production path
- dashboards and alert ownership for browser or native regressions
- production runbook and oncall ownership
- env-gated cloud fixture provisioning and CI variables

Keep these tracked in:

- `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- `docs/program/browser-release-integration-runbook.md`

## Milestones

1. Contract freeze: worker-first Arrow IPC boundary and runtime SKU vocabulary are stable.
2. I/O substrate: extent cache, identity validation, and browser-local access are in place.
3. Delta and Parquet core: snapshot reconstruction, pruning facts, and streaming scan primitives are stable.
4. Runtime productization: early pruning, streaming execution, cancellation, and budgets are enforced.
5. Launch hardening: size gates, compatibility docs, security guardrails, and external blockers are explicit.

## Critical Path

`Task 1 -> Task 2 -> Task 4 -> Task 5 -> Task 6 -> Task 7 -> Task 8`

`Task 3` can overlap with `Task 2`.

The external service track must run in parallel but must not block engine hardening until launch integration.
