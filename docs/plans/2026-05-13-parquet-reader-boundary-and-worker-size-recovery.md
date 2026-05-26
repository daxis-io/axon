# Parquet Reader Boundary And Worker Size Recovery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep the browser Parquet decoder upstream-first while restoring the shipped browser worker dependency and size guardrails.

**Architecture:** `wasm-parquet-engine` remains the browser Parquet scan boundary over Apache Arrow Rust's `parquet` crate. DataFusion work stays available as an explicit experimental or future engine path, but the default shipped worker must not pull `wasm-datafusion-poc` or the broad `datafusion` graph until the SKU intentionally flips. Research output should be captured as repo docs with measurable gates for when a custom or forked Parquet reader is justified.

**Tech Stack:** Rust workspace, Cargo resolver 2, Cargo features, `wasm32-unknown-unknown`, Arrow/Parquet `57.3.0`, DataFusion `52.4.0`, bash CI guardrails, GitHub Actions.

---

## Current Evidence

- `wasm-parquet-engine` uses Apache Arrow Rust's `parquet` crate for metadata and decoding, while Axon owns browser range reads, projection masks, row-group pruning, metrics, and Delta/browser integration.
- `cargo test -p wasm-parquet-engine --locked` passed during investigation.
- `cargo check -p wasm-parquet-engine --target wasm32-unknown-unknown --locked` passed during investigation.
- `cargo check -p browser-engine-worker --target wasm32-unknown-unknown --locked` passed, but compile success is not enough.
- `bash tests/perf/report_browser_worker_artifact.sh` failed during investigation: `browser_engine_worker.wasm` was `82,702,233` bytes against the `750,000` byte budget.
- `cargo tree -p browser-engine-worker --target wasm32-unknown-unknown -i datafusion --locked` showed `browser-engine-worker -> wasm-query-session -> wasm-datafusion-poc -> datafusion`.
- `tests/perf/README.md` and release docs say the shipped worker should be narrow and should report `browser_datafusion = false`, while current code reports `true`.

Primary references for the research writeup:

- Arrow Rust Parquet async builder: `https://docs.rs/parquet/57.3.0/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html`
- DuckDB Parquet reader source: `https://github.com/duckdb/duckdb/blob/main/extension/parquet/parquet_reader.cpp`
- DuckDB column reader source: `https://github.com/duckdb/duckdb/blob/main/extension/parquet/column_reader.cpp`
- DuckDB Parquet docs: `https://duckdb.org/docs/1.3/data/parquet/overview.html`
- DuckDB Parquet blog: `https://duckdb.org/2021/06/25/querying-parquet.html`

## Non-Goals

- Do not implement a homegrown Parquet decoder in this work.
- Do not delete `wasm-datafusion-poc`.
- Do not hide the artifact-size failure by raising the worker budget.
- Do not make `wasm-parquet-engine` depend on DataFusion.
- Do not change unrelated dirty files.

## Task 1: Add A Shipped-Worker Dependency Boundary Guardrail

**Files:**

- Create: `tests/conformance/verify_browser_worker_dependency_boundary.sh`
- Modify: `tests/conformance/README.md`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/upgrade-rehearsal.yml`

**Step 1: Write the failing guardrail script**

Create `tests/conformance/verify_browser_worker_dependency_boundary.sh`:

```bash
#!/usr/bin/env bash

set -euo pipefail

tree="$(cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked)"

if rg -n '(^|[[:space:]])wasm-datafusion-poc v|(^|[[:space:]])datafusion v' <<<"$tree"; then
  echo "browser-engine-worker must not pull DataFusion in the default shipped worker graph" >&2
  exit 1
fi

echo "browser worker dependency boundary verified"
```

**Step 2: Run it to verify it fails**

Run:

```bash
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
```

Expected: FAIL, showing `wasm-datafusion-poc` and `datafusion` in the default worker tree.

**Step 3: Wire it into CI after it is red locally**

In `.github/workflows/ci.yml`, add a step near the other conformance scripts:

```yaml
- name: Verify browser worker dependency boundary
  run: bash tests/conformance/verify_browser_worker_dependency_boundary.sh
```

In `.github/workflows/upgrade-rehearsal.yml`, add the same step near the existing conformance checks.

Update `tests/conformance/README.md` to list:

```markdown
- `bash tests/conformance/verify_browser_worker_dependency_boundary.sh`
```

**Step 4: Do not commit yet**

This task intentionally creates a red guardrail. Commit it only after Task 3 makes it pass.

## Task 2: Make DataFusion Opt-In From `wasm-query-session`

**Files:**

- Modify: `crates/wasm-query-session/Cargo.toml`
- Modify: `crates/wasm-query-session/src/lib.rs`
- Modify: `crates/wasm-query-session/tests/session.rs`

**Step 1: Add an optional feature for DataFusion**

Modify `crates/wasm-query-session/Cargo.toml`:

```toml
[features]
default = []
datafusion = ["dep:wasm-datafusion-poc"]

[dependencies]
query-contract = { path = "../query-contract" }
sqlparser = "=0.59.0"
wasm-datafusion-poc = { path = "../wasm-datafusion-poc", optional = true }
wasm-query-runtime = { path = "../wasm-query-runtime" }
```

**Step 2: Move `BrowserQueryBudget` out of the required DataFusion import path**

In `crates/wasm-query-session/src/lib.rs`, replace:

```rust
pub use wasm_datafusion_poc::BrowserQueryBudget;
```

with a local struct:

```rust
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrowserQueryBudget {
    pub max_scan_bytes: Option<u64>,
    pub max_output_ipc_bytes: Option<u64>,
    pub max_batches_in_flight: Option<usize>,
    pub max_rows_returned: Option<u64>,
}
```

Add `From<BrowserQueryBudget> for wasm_datafusion_poc::BrowserQueryBudget` behind `#[cfg(feature = "datafusion")]`.

**Step 3: Gate DataFusion imports and fields**

Gate these imports with `#[cfg(feature = "datafusion")]`:

```rust
use wasm_datafusion_poc::{
    DataFusionArrowIpcResult, DataFusionScanMetricsSummary, DeltaTableDescriptor,
    DeltaTableFieldDataType, DeltaTableSchema, DeltaTableSchemaField, WasmDataFusionEngine,
};
```

Change `BrowserQuerySession` so the DataFusion engine only exists when the feature is enabled:

```rust
#[cfg(feature = "datafusion")]
datafusion: WasmDataFusionEngine,
```

In constructors, initialize that field only behind `#[cfg(feature = "datafusion")]`.

**Step 4: Preserve default narrow behavior**

In `open_delta_table`, use feature gating:

```rust
#[cfg(feature = "datafusion")]
{
    // existing DataFusion registration path
}

#[cfg(not(feature = "datafusion"))]
{
    self.insert_table(name, Some(descriptor), materialized, bootstrapped, None, false)
}
```

The default worker should still be able to open a table descriptor and run through the existing narrow runtime path. It should not register a DataFusion table.

**Step 5: Gate DataFusion-only helper functions**

Add `#[cfg(feature = "datafusion")]` to DataFusion-only helpers in `crates/wasm-query-session/src/lib.rs`, including:

- `sql_datafusion`
- `datafusion_delta_schema`
- `datafusion_partition_column_type`
- `datafusion_schema_field_from_parquet_field`
- `datafusion_data_type_from_parquet_field`
- `unsupported_datafusion_parquet_field`
- `runtime_result_from_datafusion`
- `datafusion_query_metrics`
- `validate_datafusion_request_match`
- `validate_datafusion_sql_scope`
- `validate_datafusion_query_scope`
- `validate_datafusion_select_scope`
- `validate_datafusion_table_with_joins`
- `wrong_datafusion_table_error`
- `invalid_datafusion_sql`
- `validate_datafusion_execution_budget`
- `datafusion_table_estimated_rows`
- `datafusion_table_estimated_bytes`
- related DataFusion budget error helpers

Keep generic cache and narrow runtime helpers available without the feature.

**Step 6: Update tests**

For tests that assert DataFusion SQL registration or DataFusion-specific rejection behavior, add:

```rust
#[cfg(feature = "datafusion")]
```

Add or update a default-feature test proving `open_delta_table` does not register DataFusion:

```rust
#[test]
fn default_open_delta_table_uses_narrow_session_path() {
    let mut session = BrowserQuerySession::new(BrowserRuntimeConfig::default(), u64::MAX)
        .expect("session should construct");

    test_runtime().block_on(async {
        session
            .open_delta_table("events", empty_delta_descriptor())
            .await
            .expect("default open_delta_table should cache the table");
    });

    assert!(!session.table("events").unwrap().datafusion_registered());
}
```

**Step 7: Verify default build no longer pulls DataFusion**

Run:

```bash
cargo test -p wasm-query-session --locked
cargo check -p wasm-query-session --target wasm32-unknown-unknown --locked
cargo tree -p browser-engine-worker --target wasm32-unknown-unknown -i datafusion --locked
```

Expected:

- tests pass
- wasm check passes
- `cargo tree -i datafusion` reports no path from `browser-engine-worker`, or exits with no matching package in that graph

**Step 8: Verify optional DataFusion path still compiles**

Run:

```bash
cargo test -p wasm-query-session --features datafusion --locked
cargo check -p wasm-query-session --features datafusion --target wasm32-unknown-unknown --locked
```

Expected: PASS. If DataFusion tests are too broad for the first commit, keep the feature build green and move failing behavior tests into the DataFusion POC task.

## Task 3: Make The Worker Capability Report Match The Default SKU

**Files:**

- Modify: `crates/browser-engine-worker/Cargo.toml`
- Modify: `crates/browser-engine-worker/src/lib.rs`
- Modify: `crates/browser-engine-worker/tests/worker_artifact.rs`
- Modify: `crates/browser-engine-worker/tests/wasm_smoke.rs`
- Modify: `docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.narrow.json`

**Step 1: Add a worker feature passthrough**

Modify `crates/browser-engine-worker/Cargo.toml`:

```toml
[features]
default = []
datafusion = ["wasm-query-session/datafusion"]
```

**Step 2: Make reported capabilities feature-driven**

In `crates/browser-engine-worker/src/lib.rs`:

```rust
pub fn runtime_sku() -> BrowserRuntimeSku {
    if cfg!(feature = "datafusion") {
        BrowserRuntimeSku::Sql
    } else {
        BrowserRuntimeSku::Narrow
    }
}

pub fn capabilities() -> BrowserWorkerCapabilities {
    BrowserWorkerCapabilities {
        session_shell: true,
        browser_datafusion: cfg!(feature = "datafusion"),
    }
}
```

**Step 3: Update worker tests for default behavior**

Rename `worker_artifact_reports_datafusion_sql_capability` to:

```rust
#[test]
fn worker_artifact_reports_narrow_default_capability() {
    let report = artifact_report().expect("combined artifact report should be produced");

    assert_eq!(report.runtime_sku, BrowserRuntimeSku::Narrow);
    assert_eq!(report.result_transport, BrowserResultTransport::ArrowIpc);
    assert_eq!(report.capabilities, capabilities());
    assert!(report.capabilities.session_shell);
    assert!(!report.capabilities.browser_datafusion);
    assert_eq!(report.identity.package_name, "browser-engine-worker");
    assert_eq!(report.identity.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(report.identity.wasm_artifact, "browser_engine_worker.wasm");
}
```

Update `crates/browser-engine-worker/tests/wasm_smoke.rs` similarly:

```rust
assert_eq!(report.runtime_sku, BrowserRuntimeSku::Narrow);
assert!(!report.capabilities.browser_datafusion);
```

If feature-specific tests are useful, add separate `#[cfg(feature = "datafusion")]` assertions.

**Step 4: Update the JSON handoff example**

Modify `docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.narrow.json` so it reports:

```json
"runtime_sku": "narrow",
"capabilities": {
  "session_shell": true,
  "browser_datafusion": false
}
```

**Step 5: Verify**

Run:

```bash
cargo test -p browser-engine-worker --locked
cargo check -p browser-engine-worker --target wasm32-unknown-unknown --locked
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
```

Expected: PASS. The wasm smoke output should show `browser_datafusion:false`.

## Task 4: Restore The Shipped Worker Size Gate

**Files:**

- Modify only if needed: `tests/perf/report_browser_worker_artifact.sh`
- Modify: `tests/perf/README.md`
- Modify: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify: `docs/program/browser-release-integration-runbook.md`

**Step 1: Run the dependency guardrail**

Run:

```bash
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
```

Expected: PASS, no `datafusion` or `wasm-datafusion-poc` in the default worker graph.

**Step 2: Run the worker artifact budget**

Run:

```bash
bash tests/perf/report_browser_worker_artifact.sh
```

Expected: PASS under the existing `750000` byte budget.

If it still fails, do not raise the budget. Run:

```bash
cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked
ls -lh target/wasm32-unknown-unknown/release/browser_engine_worker.wasm
```

Then identify the next largest accidental dependency. Treat that as a new task before proceeding.

**Step 3: Update docs to match code**

Confirm these docs state the default shipped worker is narrow and `browser_datafusion = false`:

- `tests/perf/README.md`
- `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- `docs/program/browser-release-integration-runbook.md`
- `docs/program/browser-lakehouse-release-handoff.md`

**Step 4: Commit**

```bash
git add \
  tests/conformance/verify_browser_worker_dependency_boundary.sh \
  tests/conformance/README.md \
  .github/workflows/ci.yml \
  .github/workflows/upgrade-rehearsal.yml \
  crates/wasm-query-session \
  crates/browser-engine-worker \
  tests/perf/README.md \
  docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md \
  docs/program/browser-release-integration-runbook.md \
  docs/program/browser-lakehouse-release-handoff.md \
  docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.narrow.json
git commit -m "fix: keep datafusion out of shipped browser worker"
```

## Task 5: Keep DataFusion POC Research And Gates Separate

**Files:**

- Modify: `tests/perf/report_datafusion_wasm_size.sh`
- Modify: `tests/perf/browser_datafusion_engine_smoke.sh`
- Modify: `tests/perf/report_datafusion_wasm_size_test.sh`
- Modify: `docs/program/browser-datafusion-size-audit.md`
- Modify: `docs/program/browser-lakehouse-engine-strategy.md`

**Step 1: Verify DataFusion POC still owns its own graph**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked
cargo check -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 2: Run optional DataFusion size reporting**

Run:

```bash
bash tests/perf/report_datafusion_wasm_size.sh
```

Expected: It may pass or fail depending on local tool availability and budget env vars. Capture the output in `docs/program/browser-datafusion-size-audit.md` only if the command completes.

**Step 3: Ensure docs distinguish default worker vs DataFusion engine**

In `docs/program/browser-datafusion-size-audit.md`, add a short section:

```markdown
## Default Worker Separation

The shipped `browser-engine-worker` default feature set must not depend on `wasm-datafusion-poc` or `datafusion`. DataFusion size reporting is owned by `tests/perf/report_datafusion_wasm_size.sh` and remains separate from the blocking shipped-worker artifact gate.
```

In `docs/program/browser-lakehouse-engine-strategy.md`, clarify that DataFusion is the target engine direction, but the default shipped worker must opt into that SKU explicitly.

**Step 4: Commit**

```bash
git add tests/perf docs/program/browser-datafusion-size-audit.md docs/program/browser-lakehouse-engine-strategy.md
git commit -m "docs: separate datafusion engine gates from shipped worker"
```

## Task 6: Document The Parquet Reader Boundary And Revisit Criteria

**Files:**

- Create: `docs/program/parquet-reader-boundary.md`
- Modify: `README.md`
- Modify: `docs/program/package-owners.md`

**Step 1: Create the boundary document**

Create `docs/program/parquet-reader-boundary.md`:

```markdown
# Parquet Reader Boundary

## Decision

Axon uses Apache Arrow Rust's `parquet` crate for Parquet metadata and decode semantics. Axon owns the browser-specific storage, planning, and execution integration around that crate.

## Axon-Owned Responsibilities

- HTTP byte-range reads and response validation
- known-size footer bootstrap
- projection masks for browser scans
- row-group pruning predicates from Axon/DataFusion planning
- partition column materialization
- scan metrics and fallback/error mapping
- Delta/browser descriptor integration

## Upstream-Owned Responsibilities

- Parquet metadata parsing
- page and column decoding
- Arrow `RecordBatch` construction
- encoding and compression compatibility
- logical type interpretation where exposed by Arrow

## Why Not A Custom Reader Now

DuckDB owns a custom reader because it emits directly into DuckDB vectors and integrates page, dictionary, row-group, bloom, filter, prefetch, and optimizer behavior into one database engine. Axon's current browser layer does not need to own those decode semantics to get the required integration points.

## Revisit Triggers

Reconsider a fork or custom reader only when at least one measured trigger is true:

- Arrow `parquet` cannot compile or run in the required browser target.
- The cleaned default worker still exceeds the artifact budget because of `parquet`/Arrow, not DataFusion or unrelated dependencies.
- Remote range-read behavior performs avoidable full-object or excessive round-trip reads that cannot be fixed through `AsyncFileReader`, projection, row-group selection, or upstream patches.
- Required page-level, dictionary, bloom-filter, or predicate pushdown cannot be expressed through Arrow APIs and materially affects target workloads.
- Required Delta/Parquet files fail correctness coverage because of an upstream gap that cannot be worked around narrowly.

## Preferred Response To A Trigger

1. Add a failing regression or benchmark.
2. Try Arrow API usage changes.
3. Open an upstream issue or PR.
4. Carry a narrow fork only under ADR-0006 patch inventory rules.
5. Consider a custom reader only after measured evidence shows the fork path is worse.
```

**Step 2: Link from README**

In `README.md`, near the browser-tier `wasm-parquet-engine` bullet, add:

```markdown
See `docs/program/parquet-reader-boundary.md` for the upstream-first Parquet reader boundary and revisit criteria.
```

**Step 3: Update package ownership**

In `docs/program/package-owners.md`, keep `wasm-parquet-engine` owned by Runtime / engine team, and clarify that ownership means browser scan integration, not a custom Parquet decoder.

**Step 4: Verify markdown does not contain local paths**

Run:

```bash
bash tests/conformance/verify_workspace_layout.sh
```

Expected: PASS.

**Step 5: Commit**

```bash
git add docs/program/parquet-reader-boundary.md README.md docs/program/package-owners.md
git commit -m "docs: record parquet reader boundary"
```

## Task 7: Strengthen Parquet Scan Regression Coverage

**Files:**

- Modify: `crates/wasm-parquet-engine/tests/streaming.rs`
- Modify only if needed: `crates/wasm-parquet-engine/src/lib.rs`

**Step 1: Add a projection/range regression if missing**

Add a test that creates a two-column Parquet object, scans only one physical data column plus a partition column, and asserts the result schema contains both requested fields while range requests do not need to fetch unrelated row-group column bytes after metadata.

Name:

```rust
#[tokio::test]
async fn projection_pushdown_avoids_unrequested_data_column_ranges()
```

Use the existing `RequestCapturingServer`, `bounded_request_range`, and row-group byte-range helpers in `crates/wasm-parquet-engine/tests/streaming.rs`.

**Step 2: Run the test and confirm failure or existing coverage**

Run:

```bash
cargo test -p wasm-parquet-engine --locked --test streaming projection_pushdown_avoids_unrequested_data_column_ranges -- --exact
```

Expected: If current code already behaves correctly, PASS. If it fails, implement the smallest fix in `projection_mask_for_required_columns`.

**Step 3: Run all Parquet engine tests**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
cargo check -p wasm-parquet-engine --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 4: Commit**

```bash
git add crates/wasm-parquet-engine/tests/streaming.rs crates/wasm-parquet-engine/src/lib.rs
git commit -m "test: cover parquet projection range behavior"
```

## Task 8: Final Verification Matrix

**Files:**

- No source changes unless a previous task missed a doc or CI reference.

**Step 1: Run default shipped-worker checks**

```bash
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/perf/report_browser_worker_artifact.sh
cargo test -p browser-engine-worker --locked
cargo check -p browser-engine-worker --target wasm32-unknown-unknown --locked
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
```

Expected: all PASS, and the artifact report shows `browser_datafusion:false`.

**Step 2: Run Parquet checks**

```bash
cargo test -p wasm-parquet-engine --locked
cargo check -p wasm-parquet-engine --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 3: Run session checks without and with DataFusion**

```bash
cargo test -p wasm-query-session --locked
cargo check -p wasm-query-session --target wasm32-unknown-unknown --locked
cargo test -p wasm-query-session --features datafusion --locked
cargo check -p wasm-query-session --features datafusion --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 4: Run DataFusion POC checks**

```bash
cargo test -p wasm-datafusion-poc --locked
cargo check -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked
```

Expected: PASS.

**Step 5: Run repo conformance checks**

```bash
bash tests/conformance/verify_workspace_layout.sh
bash tests/conformance/verify_patch_inventory_state.sh
bash tests/conformance/verify_patch_inventory_state_test.sh
```

Expected: PASS.

**Step 6: Inspect changed files**

```bash
git status --short
git diff --stat
git diff -- Cargo.toml crates/wasm-query-session crates/browser-engine-worker crates/wasm-parquet-engine tests docs .github/workflows
```

Expected: Only planned files changed. Do not revert unrelated pre-existing dirty files.

## Task 9: Merge Readiness Summary

**Files:**

- Create only if useful: `docs/release-gates/parquet-reader-boundary-worker-size-summary.md`

**Step 1: Write the summary**

Capture:

- The default worker no longer depends on DataFusion.
- The worker artifact budget result and exact byte count.
- The Parquet engine still uses upstream Arrow Rust `parquet`.
- The criteria for revisiting a fork/custom reader.
- Any tests not run and why.

**Step 2: Final commit**

```bash
git add docs/release-gates/parquet-reader-boundary-worker-size-summary.md
git commit -m "docs: summarize parquet boundary recovery evidence"
```

Skip this commit if the PR description already contains the same evidence.

## Acceptance Criteria

- `browser-engine-worker` default dependency tree contains no `wasm-datafusion-poc` or `datafusion`.
- `browser-engine-worker` default artifact report says `browser_datafusion = false`.
- `bash tests/perf/report_browser_worker_artifact.sh` passes without raising the budget.
- `wasm-query-session --features datafusion` still preserves the DataFusion path.
- `wasm-parquet-engine` host tests and wasm checks pass.
- Docs explicitly state that Axon owns browser Parquet integration, not a custom decoder.
- Docs list measurable triggers for revisiting fork/custom-reader work.

## Execution Notes

- Run in a clean worktree. The current workspace had unrelated dirty files during plan creation:
  - `crates/wasm-datafusion-poc/tests/scan_pushdown.rs`
  - `docs/program/browser-lakehouse-engine-strategy.md`
  - `docs/plans/2026-05-07-browser-delta-kernel-datafusion-engine-execution-plan.md`
- Do not overwrite those changes unless they are part of the branch you intend to finish.
- Use small commits at the end of each green task.
- If the artifact-size budget still fails after DataFusion is removed, pause and investigate the next dependency contributor before changing architecture.
