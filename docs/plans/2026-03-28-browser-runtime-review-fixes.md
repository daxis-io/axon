# Browser Runtime Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the validated browser-runtime review findings without changing `query-contract`.

**Architecture:** Keep the existing planning/execution split, but tighten execution semantics. Add failing tests first for the reviewed regressions, then implement minimal runtime changes for partition-value normalization/coercion, zero-column `COUNT(*)` decoding, SQL-safe NULL filter lowering, and native-aligned default NULL sort ordering.

**Tech Stack:** Rust, `wasm-query-runtime`, `delta-control-plane` test fixtures, `parquet`, `tokio`

---

### Task 1: Lock in the reviewed failures with tests

**Files:**
- Modify: `crates/wasm-query-runtime/src/lib.rs`

**Step 1: Write failing unit tests**

Add tests for:
- partition values normalize key casing before execution lookup
- integer-like partition values are coerced for execution
- `parquet_row_to_input_row(..., &[])` skips unsupported columns
- `WHERE value = NULL` and `WHERE value IN (NULL)` are rejected
- ascending sort places NULLs last and descending sort places NULLs first

**Step 2: Run the targeted tests to verify they fail**

Run:
```bash
cargo test -p wasm-query-runtime --locked tests:: -- --nocapture
```

### Task 2: Fix partition-value execution semantics

**Files:**
- Modify: `crates/wasm-query-runtime/src/lib.rs`

**Step 1: Add minimal internal typing support for partition values**

Implement:
- normalized partition-column insertion
- internal scalar-type inference/coercion for partition values used by execution

**Step 2: Re-run the relevant runtime tests**

Run:
```bash
cargo test -p wasm-query-runtime --locked tests::merged_input_rows_expose_partition_values_for_execution -- --exact
```

### Task 3: Fix zero-column decoding and NULL semantics

**Files:**
- Modify: `crates/wasm-query-runtime/src/lib.rs`

**Step 1: Make zero required columns skip parquet field conversion**

**Step 2: Reject NULL literals in lowered compare/in-list filters**

**Step 3: Re-run the targeted tests**

Run:
```bash
cargo test -p wasm-query-runtime --locked build_execution_plan_rejects_non_lowerable_filters -- --exact
```

### Task 4: Align default NULL ordering with native behavior

**Files:**
- Modify: `crates/wasm-query-runtime/src/lib.rs`

**Step 1: Update executor-side sort ordering**

Implement default behavior:
- ascending => NULLS LAST
- descending => NULLS FIRST

**Step 2: Re-run the new sort-order tests**

Run:
```bash
cargo test -p wasm-query-runtime --locked tests:: -- --nocapture
```

### Task 5: Verify the full reviewed slice

**Files:**
- Verify only

**Step 1: Run package tests**

Run:
```bash
cargo test -p wasm-query-runtime --locked
```

**Step 2: Run cross-crate browser parity coverage**

Run:
```bash
cargo test -p delta-control-plane --locked supported_browser_non_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact
cargo test -p delta-control-plane --locked supported_browser_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact
```
