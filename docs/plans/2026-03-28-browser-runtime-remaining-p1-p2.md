# Browser Runtime Remaining P1/P2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish the remaining P1/P2 review follow-ups for browser execution coverage, execution metrics/deadlines, and `lib.rs` decomposition without changing `query-contract`.

**Architecture:** Add missing regression coverage first so the remaining behavior is pinned down. Then extend the browser executor with runtime-owned metrics and an end-to-end execution deadline using existing local result/config surfaces. After behavior is stable, split execution, lowering, and parquet-decode helpers into internal modules so `lib.rs` shrinks while public API stays in place.

**Tech Stack:** Rust, `wasm-query-runtime`, `delta-control-plane` test fixtures, `query-contract`, `tokio`, `parquet`

---

### Task 1: Add the remaining regression coverage

**Files:**
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `crates/delta-control-plane/tests/browser_snapshot_preflight.rs`
- Modify: `crates/delta-control-plane/tests/support/mod.rs`

**Step 1: Write failing tests for missing P1 coverage**

Add coverage for:
- typed partition execution parity using a partitioned integer or mixed-case partition column fixture
- nullable `ORDER BY` parity
- browser execution metrics parity for touched/skipped files where the corpus already asserts scan metrics

**Step 2: Run the new targeted tests and verify they fail for the right reason**

### Task 2: Add browser execution metrics and an end-to-end execution deadline

**Files:**
- Modify: `crates/wasm-query-runtime/src/lib.rs`
- Modify: `crates/wasm-query-runtime/tests/browser_runtime.rs`
- Modify: `README.md`

**Step 1: Extend the browser runtime config with an execution deadline**

Add a config field with a default and validation coverage.

**Step 2: Extend `BrowserExecutionResult` with metrics**

Populate:
- `bytes_fetched`
- `duration_ms`
- `files_touched`
- `files_skipped`

**Step 3: Add targeted tests**

Add tests for:
- config validation rejects zero execution timeout
- `execute_plan` fails on query-level timeout before a larger per-request timeout
- execution metrics are populated and aligned with known scan counts

**Step 4: Run the targeted tests to green**

### Task 3: Split `lib.rs` helper clusters into internal modules

**Files:**
- Create: `crates/wasm-query-runtime/src/execution.rs`
- Create: `crates/wasm-query-runtime/src/lowering.rs`
- Create: `crates/wasm-query-runtime/src/parquet_decode.rs`
- Modify: `crates/wasm-query-runtime/src/lib.rs`

**Step 1: Move parquet decode helpers into `parquet_decode.rs`**

**Step 2: Move execution helpers into `execution.rs`**

**Step 3: Move execution-plan lowering helpers into `lowering.rs`**

**Step 4: Keep public types in `lib.rs` and wire the modules through `mod` declarations**

**Step 5: Run focused compile/test verification after each move**

### Task 4: Final verification

**Files:**
- Verify only

**Step 1: Format**

Run:
```bash
cargo fmt --package wasm-query-runtime
```

**Step 2: Run runtime package tests**

Run:
```bash
cargo test -p wasm-query-runtime --locked
```

**Step 3: Run cross-crate browser parity tests**

Run:
```bash
cargo test -p delta-control-plane --locked supported_browser_non_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact
cargo test -p delta-control-plane --locked supported_browser_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact
```
