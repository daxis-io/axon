# Streaming Owned-Memory Bounds Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove the inherited Arrow page-index deprecation, place deterministic aggregate bounds around coordinator staging and DataFusion-registered operator memory, and prove owned-memory release and plateau behavior in Chromium, Firefox, and WebKit without changing the public atomic `query()` contract.

**Architecture:** Keep the existing public atomic result boundary and the private coordinator/child streaming protocol. Add a weighted `CoordinatorMemoryBudget` beside the existing lifecycle policy: admission reserves each query's declared Arrow IPC maximum against a 32 MiB aggregate limit, staging updates actual ownership, and every terminal/crash path releases the ledger. Give each `WasmDataFusionEngine` a shared 64 MiB tracked greedy memory pool and carry its limit/current/peak snapshot on the existing private terminal. Publish both layers through a browser-worker `owned_memory_metrics` event so deterministic browser tests can observe release without adding browser-only fields to the cross-runtime query contract.

**Tech Stack:** Rust, Apache Arrow/Parquet 58.3, DataFusion 53.1, TypeScript, Vitest, Playwright, Web Workers, wasm-bindgen.

**Base and mutation boundary:** Work only in `.worktrees/streaming-memory-bounds` on `feat/streaming-memory-bounds`, based on `9e45d68736b12b574bc8d5b16a6661a354a003d3`. Keep all commits local; do not push or open a PR.

**Non-goals:**

- Do not make public `query()` incremental or change its atomic success semantics.
- Do not add SDK-side lazy/chunk-native final result materialization unless the new measurements demonstrate that it is necessary.
- Do not claim the DataFusion pool bounds every Rust, Arrow, WebAssembly, JavaScript, cache, or browser allocation; it governs memory reservations registered with DataFusion.
- Do not add disk spilling to the browser runtime.
- Keep `measureUserAgentSpecificMemory()` as a Chromium-only total-heap canary; use owned-memory telemetry for portable three-browser evidence.

---

## Task 1: Preserve independent Arrow page-index policies

**Files:**

- Modify: `crates/wasm-parquet-engine/src/lib.rs`

**Step 1: Write the failing regression test**

Add a unit test that gives `ArrowReaderOptions` different column-index and offset-index policies (`Optional` and `Required`) and asserts the adapter preserves each policy independently. The test must fail because the adapter helper does not exist yet.

Run:

```bash
cargo test -p wasm-parquet-engine arrow_reader_options_preserve_split_page_index_policies --locked
```

Expected: FAIL for the missing split-policy adapter.

**Step 2: Implement the smallest policy adapter**

Add a private helper that reads `column_index_policy()` and `offset_index_policy()` independently. Apply both to `ParquetMetaDataReader` with `with_column_index_policy` and `with_offset_index_policy`, while preserving `metadata_options`.

**Step 3: Verify behavior and eliminate the warning**

Run:

```bash
cargo test -p wasm-parquet-engine --locked
RUSTFLAGS="-D warnings" cargo check -p wasm-parquet-engine --locked
```

Expected: all tests pass and the deprecated `ArrowReaderOptions::page_index()` warning is absent.

**Step 4: Commit**

```bash
git add crates/wasm-parquet-engine/src/lib.rs
git commit -m "fix: preserve split parquet index policies"
```

## Task 2: Bound aggregate coordinator staging

**Files:**

- Modify: `apps/axon-web/src/sandbox-query-coordinator-policy.ts`
- Modify: `apps/axon-web/src/sandbox-query-coordinator-policy.test.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker-test-harness.ts`
- Modify: `apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts`

**Step 1: Write failing ledger tests**

Add Vitest coverage for a `CoordinatorMemoryBudget` that:

- reserves two declared 16 MiB results under a 32 MiB aggregate limit and rejects a third;
- records actual staged bytes and independent reserved/staged peaks;
- returns current reserved and staged ownership to zero after release;
- rejects duplicate reservation, unknown staging, staging beyond the declared reservation, and double release.

Run:

```bash
npx vitest run src/sandbox-query-coordinator-policy.test.ts
```

Expected: FAIL because the ledger and snapshot contract do not exist.

**Step 2: Implement the ledger**

Add `MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES = 32 * 1024 * 1024` and a deep `CoordinatorMemoryBudget` API with `tryReserve`, `recordStaged`, `release`, and `snapshot`. Store per-query declared and actual bytes privately; expose only aggregate limit/current/peak fields.

**Step 3: Write a failing browser admission regression**

Extend the coordinator test harness with a positive `max_staged_bytes` parameter. Add a real-worker test in `internal-arrow-ipc-stream.spec.ts` that holds one query in the injected hanging child and proves a second query is rejected with `fallback_required` and `browser_runtime_constraint` when its declared maximum would exceed aggregate staging, even though request-count capacity remains.

Run:

```bash
npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --project=chromium --grep "aggregate staging"
```

Expected: FAIL because worker admission is still request-count-only.

**Step 4: Wire weighted admission and exhaustive release**

Reserve the validated `max_arrow_ipc_bytes` before starting a SQL command. Record every accepted private chunk. Release on successful transfer, failed/cancelled/deadline terminals, stream-start failure, watchdog cleanup, and child crash. Keep request-count capacity as a separate defense.

**Step 5: Verify**

Run:

```bash
npx vitest run src/sandbox-query-coordinator-policy.test.ts src/sandbox-query-stream-protocol.test.ts
npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --project=chromium
```

Expected: all focused tests pass.

**Step 6: Commit**

```bash
git add apps/axon-web/src/sandbox-query-coordinator-policy.ts apps/axon-web/src/sandbox-query-coordinator-policy.test.ts apps/axon-web/src/sandbox-query-worker.ts apps/axon-web/src/sandbox-query-worker-test-harness.ts apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts
git commit -m "feat: bound aggregate coordinator staging"
```

## Task 3: Bound and measure DataFusion-registered operator memory

**Files:**

- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/src/ipc_cursor.rs`
- Modify: `crates/wasm-datafusion-poc/tests/custom_scan_exec.rs`
- Modify: `crates/wasm-datafusion-poc/tests/ipc_cursor.rs`
- Modify: `crates/wasm-datafusion-session/src/lib.rs`
- Modify: `apps/axon-web/src/lib.rs`
- Modify: `docs/release-gates/daxis-browser-datafusion-budget-profile.json`
- Modify: `crates/wasm-datafusion-poc/tests/daxis_budget_profile.rs`

**Step 1: Write failing pool and error-contract tests**

Add tests that require:

- the default engine pool limit to be exactly 64 MiB;
- current and peak registered reservation metrics to update and current bytes to return to zero after release;
- a deliberately tiny engine pool to reject an actual sort/aggregate allocation as `fallback_required` with `browser_runtime_constraint`;
- the Daxis budget profile to declare the 64 MiB DataFusion operator pool and 32 MiB coordinator staging limits.

Run:

```bash
cargo test -p wasm-datafusion-poc memory_pool --locked
cargo test -p wasm-datafusion-poc datafusion_operator_memory_exhaustion --locked
cargo test -p wasm-datafusion-poc --test daxis_budget_profile --locked
```

Expected: FAIL because no bounded pool or profile fields exist.

**Step 2: Install the tracked greedy pool**

Add `DEFAULT_BROWSER_DATAFUSION_MEMORY_POOL_BYTES = 64 * 1024 * 1024`. Wrap `TrackConsumersPool<GreedyMemoryPool>` in a small shared pool that records aggregate peak reservations while delegating DataFusion's `MemoryPool` contract. Construct the engine `SessionContext` with a `RuntimeEnv` using that pool. Keep an explicit non-zero-limit constructor for deterministic tests.

Map `DataFusionError::ResourcesExhausted` to `QueryErrorCode::FallbackRequired` plus `FallbackReason::BrowserRuntimeConstraint`; keep a stable browser-memory prefix while retaining DataFusion's consumer diagnostics.

**Step 3: Carry terminal memory snapshots**

Add a `BrowserDataFusionMemoryMetrics` value containing `limit_bytes`, `reserved_bytes`, and `peak_bytes`. Attach it to every cursor terminal only after dropping the cursor's execution state. Carry it through `BrowserDataFusionQueryTerminal` and serialize it as decimal strings in the existing private sandbox terminal metadata. Do not add these browser-owned metrics to `query_contract::QueryMetricsSummary`.

**Step 4: Verify**

Run:

```bash
cargo test -p wasm-datafusion-poc --locked
cargo test -p wasm-datafusion-session --locked
cargo test -p axon-web-wasm --locked
```

Expected: all tests pass, resource exhaustion has the structured fallback category, and successful terminal snapshots report zero current DataFusion reservations.

**Step 5: Commit**

```bash
git add crates/wasm-datafusion-poc/src/lib.rs crates/wasm-datafusion-poc/src/ipc_cursor.rs crates/wasm-datafusion-poc/tests/custom_scan_exec.rs crates/wasm-datafusion-poc/tests/ipc_cursor.rs crates/wasm-datafusion-session/src/lib.rs apps/axon-web/src/lib.rs docs/release-gates/daxis-browser-datafusion-budget-profile.json crates/wasm-datafusion-poc/tests/daxis_budget_profile.rs
git commit -m "feat: bound datafusion operator memory"
```

## Task 4: Publish owned-memory telemetry and prove the portable plateau

**Files:**

- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Modify: `apps/axon-web/src/sandbox-query-stream-protocol.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts`
- Modify: `apps/axon-web/tests/browser-heap-evidence.spec.ts`
- Modify: `docs/plans/2026-07-23-streaming-owned-memory-bounds.md`

**Step 1: Write the failing SDK event test**

Define the intended `owned_memory_metrics` worker event in the test first. Require strict normalization for non-negative safe integers in:

- coordinator limit, current reserved/staged, and peak reserved/staged bytes;
- optional DataFusion limit, current reserved, and peak bytes.

Run:

```bash
npx vitest run tests/axon-browser-sdk.spec.ts --testNamePattern "owned memory"
```

Expected: FAIL because the event tag and normalizer do not exist.

**Step 2: Implement the event and coordinator emission**

Extend `PrivateTerminalMetadata` with optional DataFusion decimal-string memory metrics. Add the typed public worker event and strict normalizer. Emit one owned-memory event before the terminal response after the coordinator has released that query's ledger entry. Crash terminals may omit DataFusion metrics but must still report released coordinator ownership.

**Step 3: Write and run the three-browser plateau proof**

Extend `internal-arrow-ipc-stream.spec.ts` with a warm query followed by 20 identical atomic queries. Capture owned-memory events and assert for every completed query:

- coordinator current reserved/staged bytes are zero;
- DataFusion current registered bytes are zero;
- peaks never exceed their configured limits;
- after warm-up, repeated-query high-water marks plateau rather than grow with query count.

Run:

```bash
npm run build:fixture
npm run build:wasm
npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --grep "owned memory plateau"
```

Expected: PASS in Chromium, Firefox, and WebKit.

**Step 4: Retain the total-heap canary**

Update the Chromium heap evidence annotations to include the owned-memory limit/peak values observed during the same repeated-query loop. Keep `measureUserAgentSpecificMemory()` as a separate retained-heap assertion and document why it cannot be made portable.

Run:

```bash
npm run test:browser:heap-evidence
```

Expected: PASS in Chromium with retained heap delta at or below 8 MiB and owned peaks within both configured limits.

**Step 5: Commit**

```bash
git add apps/axon-web/src/axon-browser-sdk.ts apps/axon-web/tests/axon-browser-sdk.spec.ts apps/axon-web/src/sandbox-query-stream-protocol.ts apps/axon-web/src/sandbox-query-worker.ts apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts apps/axon-web/tests/browser-heap-evidence.spec.ts docs/plans/2026-07-23-streaming-owned-memory-bounds.md
git commit -m "test: prove streaming owned-memory plateau"
```

## Task 5: Full verification and local handoff

**Files:**

- Modify: `docs/plans/2026-07-23-streaming-owned-memory-bounds.md`

**Step 1: Run deterministic verification**

From the repository root:

```bash
cargo fmt --all -- --check
RUSTFLAGS="-D warnings" cargo check -p wasm-parquet-engine --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-datafusion-poc --locked
cargo test -p wasm-datafusion-session --locked
cargo test -p axon-web-wasm --locked
```

From `apps/axon-web`:

```bash
npm test
npm run codegen:check
npm run lint
npx tsc --noEmit
npm run format:check
npm run build
npx playwright test tests/internal-arrow-ipc-stream.spec.ts tests/browser-worker-matrix.spec.ts --config=playwright.config.ts
npm run test:browser:heap-evidence
```

**Step 2: Record evidence and residual boundaries**

Append exact counts and browser results. Record that:

- total browser heap remains Chromium-only evidence;
- coordinator staging is bounded independently from request count;
- the DataFusion limit covers registered reservations, not all Wasm/Arrow allocations;
- SDK final result materialization remains atomic and may still temporarily copy chunked output.

**Step 3: Verify branch shape**

Run:

```bash
git status --short --branch
git log --oneline 9e45d68736b12b574bc8d5b16a6661a354a003d3..HEAD
```

Expected: clean branch with the plan and implementation commits only.

**Step 4: Commit the handoff**

```bash
git add docs/plans/2026-07-23-streaming-owned-memory-bounds.md
git commit -m "docs: record streaming memory verification"
```

