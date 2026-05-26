# Browser DataFusion Full Runtime Parity Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:test-driven-development for every code task. Use superpowers:verification-before-completion before any completion claim.

**Goal:** Turn the browser DataFusion path from a narrow runtime into a measured, browser-proven DataFusion runtime that accepts every Arrow/DataFusion feature Axon can execute correctly in WASM and rejects the rest explicitly.

**Architecture:** DataFusion continues to own SQL planning, optimization, physical execution, and Arrow `RecordBatch` output. Axon owns Delta descriptors, browser-safe object access, Parquet range I/O, scan budgets, cancellation, metrics, fallback classification, and Arrow IPC delivery. Each slice removes one browser-runtime bottleneck and adds parity evidence without reimplementing DataFusion or bypassing the existing `WasmDataFusionEngine` / `AxonParquetScanExec` / session boundary.

**Tech Stack:** Rust workspace, Cargo locked builds, DataFusion `=52.4.0`, Arrow/Parquet `=57.3.0`, `wasm-datafusion-poc`, `wasm-datafusion-session`, `wasm-parquet-engine`, `wasm-http-object-store`, `wasm-query-session`, `browser-engine-worker`, `browser-sdk`, Playwright, Arrow IPC.

---

## Context And Source Plan

This plan executes the broader "full featured DataFusion runtime" follow-up discussed after the unannotated Parquet `BYTE_ARRAY` support slice.

Read this original full plan first for architectural context:

```bash
sed -n '1,90p;754,1068p;1150,1225p;1418,1445p' docs/plans/2026-05-07-browser-delta-kernel-datafusion-engine-execution-plan.md
```

Use that plan as context only. Do not execute the entire Delta Kernel plan in this work. The relevant constraints are:

- DataFusion owns SQL planning and physical execution.
- Axon owns table access, browser range reads, Parquet scan integration, budgets, cancellation, metrics, and Arrow IPC delivery.
- Do not reimplement the DataFusion provider, scan execution plan, worker command flow, or budget model unless a compatibility test exposes a concrete bug.
- Tasks 9-13 of the original plan are the closest prior framing for the DataFusion facade, descriptor adapter, Parquet scan, and pushdown correctness work.

Recent completed slice to preserve:

- `DeltaTableFieldDataType::Binary` exists in `crates/wasm-datafusion-poc/src/lib.rs`.
- `crates/wasm-datafusion-session/src/lib.rs` maps unannotated Parquet `BYTE_ARRAY` to Arrow/DataFusion `Binary`.
- `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs` has a descriptor-backed Parquet scan test proving unannotated `BYTE_ARRAY` returns `DataType::Binary`.

## Non-Goals

- Do not make a BFF/server snapshot path required.
- Do not add a native fallback as a substitute for browser proof.
- Do not claim "full DataFusion" for features that are not tested through the browser-owned runtime path.
- Do not pull broad native dependencies into browser crates.
- Do not rewrite the UI, worker protocol, or SDK unless a later browser proof slice identifies a concrete contract gap.
- Do not collapse all unsupported features into generic `unsupported_feature`; classification must remain useful.

## Global Quality Gates

Run the relevant narrow gates after each task. Run the full gate set before final handoff:

```bash
cargo test -p wasm-datafusion-session
cargo test -p wasm-datafusion-poc
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
cargo test -p wasm-query-session --tests
cargo test -p browser-engine-worker --tests
cargo test -p browser-sdk --tests
npm exec -- tsc --noEmit
npm run test:sdk
npm run format:check
npm run lint
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
git diff --check
```

Expected: all commands pass. If a command is unavailable because dependencies are missing, stop and report the exact blocker instead of substituting a weaker gate.

## Worktree And Safety Setup

Run in a clean, dedicated worktree unless the user explicitly says to continue in the current dirty checkout:

```bash
git status --short
git worktree add ../axon-browser-datafusion-runtime-parity -b feat/browser-datafusion-runtime-parity
cd ../axon-browser-datafusion-runtime-parity
git status --short
```

Expected: clean worktree before implementation. If it is not clean, stop and identify the owner of the dirty files.

---

### Task 1: Replace The Narrow Schema Enum With Arrow-Native Schema Fields

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-session/src/lib.rs`
- Test: `crates/wasm-datafusion-session/src/lib.rs`
- Test: `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs`

**Purpose:** Remove the local schema choke point so browser DataFusion can receive Arrow-supported field types instead of only the hand-maintained primitive enum.

**Step 1: Inspect current schema ownership**

Run:

```bash
rg -n "DeltaTableFieldDataType|DeltaTableSchemaField|datafusion_data_type_from_parquet_field|arrow_data_type" crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-session/src/lib.rs
rg -n "fn from_byte_array|from_int32|from_int64|from_fixed_len_byte_array|PhysicalType::BYTE_ARRAY" ~/.cargo/registry/src/*/parquet-57.3.0/src/arrow/schema/primitive.rs
```

Expected: identify every current local enum mapping and compare it against Arrow Parquet schema conversion.

**Step 2: Write failing schema adapter tests**

Add tests in `crates/wasm-datafusion-session/src/lib.rs` for:

- unannotated `BYTE_ARRAY` -> `DataType::Binary`
- `BYTE_ARRAY` with logical `String` or converted `Utf8` -> `DataType::Utf8`
- `BYTE_ARRAY` with logical or converted `Json` -> `DataType::Utf8`
- `BYTE_ARRAY` with logical or converted `Bson` / `Enum` / `Unknown` -> `DataType::Binary`
- unsupported nested/list/map/variant shapes remain structured `UnsupportedFeature`

Run:

```bash
cargo test -p wasm-datafusion-session schema_adapter
```

Expected: FAIL because `DeltaTableSchemaField` does not yet preserve Arrow-native data types for all cases.

**Step 3: Refactor schema fields to carry Arrow data types**

Change `DeltaTableSchemaField` so it stores an Arrow `DataType` directly or through a thin newtype. Keep constructor ergonomics local and explicit.

Preferred shape:

```rust
pub struct DeltaTableSchemaField {
    pub name: String,
    pub data_type: arrow_schema::DataType,
    pub nullable: bool,
}

impl DeltaTableSchemaField {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self { ... }

    fn arrow_field(&self) -> Field {
        Field::new(self.name.clone(), self.data_type.clone(), self.nullable)
    }
}
```

Keep any compatibility constructors only if they meaningfully reduce churn. Do not keep a permanent enum that must mirror Arrow.

**Step 4: Implement Arrow-like Parquet primitive mapping in the session adapter**

Map only field shapes that have clear Arrow/DataFusion semantics and are supported by the current browser scan path. Use Arrow Rust `parquet-57.3.0/src/arrow/schema/primitive.rs` as the reference for primitive and byte-array mappings.

If a Parquet shape maps to an Arrow type but is not proven through browser scan yet, either:

- add a real descriptor-backed scan test in this task, or
- keep it unsupported with a targeted error message.

Do not silently coerce decimals, fixed-size binary, timestamps, or nested fields unless a scan test proves the path.

**Step 5: Verify**

Run:

```bash
cargo test -p wasm-datafusion-session
cargo test -p wasm-datafusion-poc --test parquet_scan_exec
cargo test -p wasm-datafusion-poc
cargo fmt --check
git diff --check
```

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-session/src/lib.rs crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs
git commit -m "feat: preserve arrow schema types in browser datafusion"
```

---

### Task 2: Add A Browser DataFusion SQL Parity Corpus

**Files:**
- Create: `tests/conformance/browser-datafusion-runtime-corpus.json`
- Create: `crates/wasm-datafusion-poc/tests/runtime_sql_corpus.rs`
- Modify only if needed: `crates/wasm-datafusion-poc/src/lib.rs`

**Purpose:** Establish a growing, explicit SQL capability matrix for the browser DataFusion runtime. The corpus should prove SQL features execute through `WasmDataFusionEngine`, not through a mocked planner or native-only shortcut.

**Step 1: Define the first corpus**

Create `tests/conformance/browser-datafusion-runtime-corpus.json`:

```json
[
  {
    "name": "projection_filter_sort_limit",
    "sql": "SELECT id, category FROM t WHERE value >= 10 ORDER BY id LIMIT 2",
    "expected_columns": ["id", "category"],
    "expected_rows": [[2, "B"], [3, "B"]]
  },
  {
    "name": "aggregate_group_by",
    "sql": "SELECT category, COUNT(*) AS n FROM t GROUP BY category ORDER BY category",
    "expected_columns": ["category", "n"],
    "expected_rows": [["A", 1], ["B", 2], ["C", 1]]
  },
  {
    "name": "case_cast_arithmetic",
    "sql": "SELECT id, CAST(value * 2 AS BIGINT) AS doubled, CASE WHEN value > 10 THEN 'high' ELSE 'low' END AS bucket FROM t ORDER BY id",
    "expected_columns": ["id", "doubled", "bucket"],
    "expected_rows": [[1, 10, "low"], [2, 24, "high"], [3, 50, "high"], [4, 40, "high"]]
  }
]
```

**Step 2: Write failing corpus runner**

Create `crates/wasm-datafusion-poc/tests/runtime_sql_corpus.rs`. It should:

- load the JSON corpus
- register a deterministic `RecordBatch` table with int/string/boolean/null coverage
- run each SQL query through `WasmDataFusionEngine`
- normalize Arrow results into comparable scalar rows
- assert expected columns and rows

Run:

```bash
cargo test -p wasm-datafusion-poc --test runtime_sql_corpus
```

Expected: FAIL until the runner and any required normalization helpers exist.

**Step 3: Implement minimal normalization helpers**

Support only the Arrow scalar types present in the initial corpus:

- `Int32`
- `Int64`
- `Utf8`
- `Boolean`
- null values

Keep helpers inside the test unless production code genuinely needs them.

**Step 4: Add explicit unsupported-case coverage**

Add corpus entries that must return structured unsupported or execution errors only if the runtime currently rejects them intentionally. Do not mark unsupported cases as success.

Examples:

- multi-statement SQL
- write/DDL statements
- table access outside the opened table scope

**Step 5: Verify**

Run:

```bash
cargo test -p wasm-datafusion-poc --test runtime_sql_corpus
cargo test -p wasm-datafusion-poc
cargo fmt --check
git diff --check
```

Expected: PASS.

**Step 6: Commit**

```bash
git add tests/conformance/browser-datafusion-runtime-corpus.json crates/wasm-datafusion-poc/tests/runtime_sql_corpus.rs crates/wasm-datafusion-poc/src/lib.rs
git commit -m "test: add browser datafusion sql parity corpus"
```

---

### Task 3: Expand Parquet Scan Parity For Browser-Readable Types

**Files:**
- Modify: `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs`
- Modify only if tests expose a bug: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify only if tests expose a bug: `crates/wasm-parquet-engine/src/lib.rs`

**Purpose:** Prove browser range-read Parquet scans can execute the schema types admitted by Task 1. Schema support without scan evidence is not enough.

**Step 1: Write failing Parquet fixture tests**

Extend `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs` with descriptor-backed Parquet files covering:

- required and optional UTF8
- unannotated binary payloads
- JSON-annotated `BYTE_ARRAY` if supported by Task 1
- booleans
- `INT32`, `INT64`, `FLOAT`, `DOUBLE`
- nulls in optional fields

Each test must go through:

```rust
WasmDataFusionEngine::new()
engine.open_delta_table(delta_descriptor(...)).await
engine.sql_to_record_batches("SELECT ... FROM events ...").await
```

Run:

```bash
cargo test -p wasm-datafusion-poc --test parquet_scan_exec browser_readable
```

Expected: FAIL for at least one unimplemented or mis-normalized type.

**Step 2: Fix only the proven scan gaps**

If failures come from `AxonParquetScanExec`, fix there. If failures come from `wasm-parquet-engine`, fix there. Do not add special-case row materialization above DataFusion.

Rules:

- Use Arrow/Parquet APIs instead of ad hoc byte parsing.
- Keep browser range I/O in `wasm-http-object-store` / `wasm-parquet-engine`.
- Keep SQL semantics in DataFusion.
- Preserve projection and row-order expectations in tests.

**Step 3: Add null and dictionary regression coverage**

If the Parquet writer naturally dictionary-encodes strings/binary, assert output remains logically correct. Add optional null coverage for each supported nullable type.

**Step 4: Verify**

Run:

```bash
cargo test -p wasm-datafusion-poc --test parquet_scan_exec
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-datafusion-poc
cargo fmt --check
git diff --check
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs crates/wasm-datafusion-poc/src/lib.rs crates/wasm-parquet-engine/src/lib.rs
git commit -m "test: expand browser datafusion parquet type parity"
```

---

### Task 4: Add Pushdown And Residual Correctness Corpus

**Files:**
- Modify: `crates/wasm-datafusion-poc/tests/scan_pushdown.rs`
- Modify only if tests expose a bug: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify only if tests expose a bug: `crates/wasm-parquet-engine/src/lib.rs`

**Purpose:** Ensure broader DataFusion support does not break residual predicate correctness, especially around scan-level limits and inexact pruning.

**Step 1: Write failing residual tests**

Add tests for:

- non-partition filter with `LIMIT` where scan-level limit pushdown would be wrong
- exact partition prune plus residual value filter
- file stats prune plus residual row predicate
- row-group prune plus residual row predicate
- null comparisons: `IS NULL`, `IS NOT NULL`, `= NULL`
- `IN (...)` on partition and non-partition columns

Run:

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown
```

Expected: FAIL for any missing trace field, incorrect exactness classification, or row-result mismatch.

**Step 2: Keep exact/inexact classification conservative**

Classify predicates as:

- exact partition prune
- inexact Delta stats prune
- inexact Parquet row-group prune
- unsupported residual

Only report exact support to DataFusion when Axon fully enforces the predicate. Otherwise retain the residual in DataFusion.

**Step 3: Verify metrics and traces**

Assert trace fields show:

- projected columns
- planned files
- skipped files
- touched/skipped row groups
- pushed limit only when safe
- residual filters when present

**Step 4: Verify**

Run:

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown
cargo test -p wasm-datafusion-poc
cargo fmt --check
git diff --check
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-poc/tests/scan_pushdown.rs crates/wasm-datafusion-poc/src/lib.rs crates/wasm-parquet-engine/src/lib.rs
git commit -m "test: harden browser datafusion residual correctness"
```

---

### Task 5: Harden Runtime Controls For Larger DataFusion Coverage

**Files:**
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/query_budgets.rs`
- Modify: `crates/wasm-datafusion-session/src/lib.rs`
- Modify only if needed: `crates/wasm-query-session/src/lib.rs`
- Modify only if needed: `crates/browser-engine-worker/src/lib.rs`

**Purpose:** More DataFusion features mean larger scans, outputs, and intermediate streams. Runtime controls must remain enforced at plan time and stream time.

**Step 1: Write failing budget tests**

Extend `crates/wasm-datafusion-poc/tests/query_budgets.rs` with:

- output IPC budget exceeded by an aggregate result
- output IPC budget exceeded by a wide binary/string projection
- row budget exceeded after a filter
- scan byte budget exceeded before fetch
- cancellation during Arrow IPC output

Run:

```bash
cargo test -p wasm-datafusion-poc --test query_budgets
```

Expected: FAIL for missing enforcement or missing structured error shape.

**Step 2: Ensure budget enforcement is both plan-time and stream-time**

Plan-time checks should reject obvious oversized scans before fetching. Stream-time checks should enforce actual bytes fetched, rows emitted, and Arrow IPC bytes as they are produced.

**Step 3: Preserve structured errors**

Every budget or cancellation failure must include:

- stable `QueryErrorCode`
- browser execution target
- actionable message naming the budget or cancellation point

**Step 4: Verify session propagation**

Run:

```bash
cargo test -p wasm-datafusion-session
cargo test -p wasm-query-session --tests
cargo test -p browser-engine-worker --tests
```

Expected: PASS. If worker/session changes are unnecessary, leave those crates untouched.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/tests/query_budgets.rs crates/wasm-datafusion-session/src/lib.rs crates/wasm-query-session/src/lib.rs crates/browser-engine-worker/src/lib.rs
git commit -m "test: harden browser datafusion budgets and cancellation"
```

---

### Task 6: Add Browser Worker Proof For The DataFusion Runtime

**Files:**
- Modify: `apps/axon-web/tests/browser-worker-matrix.spec.ts`
- Modify only if needed: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify only if needed: `crates/browser-engine-worker/src/lib.rs`
- Modify only if needed: `tests/conformance/verify_axon_web_datafusion_runtime.sh`

**Purpose:** Host-side Rust tests are necessary but not sufficient. This slice proves the runtime works through the actual browser worker and SDK boundary.

**Step 1: Write failing Playwright worker tests**

Add tests that use the real worker path and assert:

- open table succeeds with a browser-safe descriptor
- SQL returns Arrow IPC, not row-shaped JSON
- binary/string/int columns round trip through worker response metadata
- cancellation returns the structured cancellation shape
- unsupported features return structured `unsupported_feature`

Run:

```bash
npm exec playwright test apps/axon-web/tests/browser-worker-matrix.spec.ts
```

Expected: FAIL until the worker/SKD path exposes the needed behavior.

**Step 2: Fix only browser boundary gaps**

Keep the worker a router over the runtime/session. Do not move SQL planning, scan logic, or DataFusion internals into TypeScript.

**Step 3: Verify the conformance script**

Run:

```bash
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
npm exec -- tsc --noEmit
npm run test:sdk
npm run format:check
npm run lint
```

Expected: PASS.

**Step 4: Commit**

```bash
git add apps/axon-web/tests/browser-worker-matrix.spec.ts apps/axon-web/src/axon-browser-sdk.ts crates/browser-engine-worker/src/lib.rs tests/conformance/verify_axon_web_datafusion_runtime.sh
git commit -m "test: prove browser worker datafusion runtime parity"
```

---

### Task 7: Add Unsupported Feature Classification And Release Evidence

**Files:**
- Modify: `crates/wasm-datafusion-session/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Create: `docs/program/browser-datafusion-runtime-parity.md`

**Purpose:** A full-featured runtime still needs honest edges. Unsupported features must be classified and documented with evidence, not discovered as vague runtime failures.

**Step 1: Write classification tests**

Add tests for unsupported:

- nested/list/map fields if not proven in browser scan
- decimals if not proven
- timestamps if not proven
- fixed-size binary if not proven
- variant/geospatial/UUID/interval if not proven
- non-SELECT SQL and cross-table access

Run:

```bash
cargo test -p wasm-datafusion-session unsupported
cargo test -p wasm-datafusion-poc unsupported
```

Expected: FAIL until unsupported paths have stable messages and codes.

**Step 2: Normalize error classification**

Use specific messages:

- `browser DataFusion schema does not yet support ...`
- `browser DataFusion SQL scope does not allow ...`
- `browser DataFusion scan cannot execute ...`

Keep `QueryErrorCode::UnsupportedFeature` only for true capability gaps. Use `InvalidRequest`, `BudgetExceeded`, `FallbackRequired`, or `ExecutionFailed` where more accurate.

**Step 3: Write runtime parity docs**

Create `docs/program/browser-datafusion-runtime-parity.md` with:

- supported SQL classes
- supported Arrow/Parquet field types
- supported scan optimizations
- unsupported features and owner
- verification command matrix
- browser proof status
- artifact/size caveats

**Step 4: Verify full gate set**

Run:

```bash
cargo test -p wasm-datafusion-session
cargo test -p wasm-datafusion-poc
cargo test -p wasm-parquet-engine --tests
cargo test -p wasm-http-object-store --tests
cargo test -p wasm-query-session --tests
cargo test -p browser-engine-worker --tests
cargo test -p browser-sdk --tests
npm exec -- tsc --noEmit
npm run test:sdk
npm run format:check
npm run lint
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
git diff --check
```

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-session/src/lib.rs crates/wasm-datafusion-poc/src/lib.rs docs/program/browser-lakehouse-release-handoff.md docs/program/browser-datafusion-runtime-parity.md
git commit -m "docs: document browser datafusion runtime parity"
```

---

## Execution Order

Execute in this order:

1. Task 1: remove the schema choke point.
2. Task 2: create the SQL parity corpus.
3. Task 3: prove Parquet scan type parity.
4. Task 4: harden pushdown and residual correctness.
5. Task 5: harden budgets, cancellation, and metrics for larger outputs.
6. Task 6: prove the runtime through the real browser worker boundary.
7. Task 7: classify unsupported features and publish release evidence.

Stop after each task if:

- a clean baseline gate fails before edits,
- a feature needs browser-incompatible dependencies,
- DataFusion cannot preserve correctness through Axon's scan path,
- artifact size grows beyond the current budget without an explicit split,
- a test would require moving SQL planning or execution outside DataFusion.

Do not claim the browser runtime is "full featured" until Tasks 1-7 are implemented, verified, and the docs distinguish supported, unsupported, and unproven features.
