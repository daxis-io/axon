# Browser DataFusion Runtime Parity Review Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Resolve the post-audit correctness, worker-contract, evidence, and hygiene findings for the browser DataFusion runtime parity slice.

**Architecture:** Keep the existing ownership split: DataFusion-facing Rust crates own SQL execution, scan classification, budgets, cancellation, and Arrow IPC; the app worker owns browser message contracts and session lifecycle; release docs must only cite evidence that is reproducible from the current checkout. Fixes should be narrow and behavior-backed, not a new runtime redesign.

**Tech Stack:** Rust workspace crates (`wasm-datafusion-poc`, `wasm-datafusion-session`, `delta-control-plane`), wasm-bindgen app bridge (`apps/axon-web/src/lib.rs`), TypeScript SDK/worker, Playwright, cargo tests, npm app checks.

---

## Findings To Resolve

1. `P1` Worker `dispose` acknowledges success without removing the DataFusion table from the wasm session.
2. `P1` Delta deletion-vector scan errors lose `unsupported_feature` taxonomy at the public engine boundary.
3. `P1` Release handoff cites `browser_snapshot_preflight`, but that suite currently fails 3/16.
4. `P2` Cancel command is exported as request-shaped SDK surface without a response/client contract.
5. `P2` Size caveat points at stale/non-app size evidence.
6. `P2` Real-worker cancellation test is timing-sensitive.
7. `P2` Test support contains duplicated loopback range-server helpers.
8. `P2` Runtime SQL corpus proves MemTable SQL parity but not descriptor-backed scan parity.

## Task 1: Worker Dispose Lifecycle

**Files:**

- Modify: `apps/axon-web/src/lib.rs`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/tests/browser-worker-matrix.spec.ts`
- Optional: `apps/axon-web/tests/axon-browser-sdk.spec.ts`

**Steps:**

1. Add a failing real-worker test proving `client.dispose(name)` removes the table from the worker session by attempting a raw `QueryRequest` for the same table after disposal and expecting an `invalid_request` worker error.
2. Add a wasm-bindgen `SandboxQuerySession::dispose_table(name: String) -> Result<String, JsValue>` bridge that calls `BrowserDataFusionSession::dispose_table`.
3. Update the worker `dispose` branch to instantiate the session when needed, call `session.dispose_table(command.dispose.name)`, then post the existing `disposed` envelope.
4. Run the targeted browser worker test, then the full browser-worker matrix.

**Acceptance Criteria:**

- Disposed tables are removed from both the SDK table map and worker/wasm DataFusion registry.
- Existing `disposed` response shape remains compatible.
- Missing-table dispose behavior is explicit and tested, either idempotent success or structured error.

## Task 2: Unsupported Scan Feature Taxonomy

**Files:**

- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/tests/delta_descriptor.rs` or `tests/query_budgets.rs`

**Steps:**

1. Add a failing public-path test that opens a descriptor containing a `DeltaActiveFile.deletion_vector`, runs `WasmDataFusionEngine::sql_to_arrow_ipc_result`, and asserts `QueryErrorCode::UnsupportedFeature` plus the stable `browser DataFusion scan cannot execute ...` wording.
2. Update `map_datafusion_error` to preserve `DataFusionError::NotImplemented(message)` as `QueryErrorCode::UnsupportedFeature`.
3. Keep wrapped `QueryError` handling as the first branch so existing budget/cancellation errors remain unchanged.
4. Run `cargo test -p wasm-datafusion-poc` and `cargo test -p wasm-datafusion-session`.

**Acceptance Criteria:**

- Deletion vectors fail as `unsupported_feature` through the public engine API, not only the private scan helper.
- Existing cancellation and fallback-required budget taxonomy remains stable.

## Task 3: Delta Control-Plane Preflight Evidence

**Files:**

- Modify: `crates/delta-control-plane/tests/browser_snapshot_preflight.rs`
- Modify only if needed: the runtime code that owns preflight/etag/range-read semantics.
- Modify: `docs/program/browser-lakehouse-release-handoff.md`

**Observed Failures:**

- `browser_execution_reports_metrics_for_pruned_and_empty_scans`
- `browser_execution_rejects_bootstrap_to_execution_etag_drift_on_real_fixture_files`
- `browser_execution_reports_nonzero_duration_metrics_for_successful_reads`

**Steps:**

1. Reproduce `cargo test -p delta-control-plane --test browser_snapshot_preflight` and inspect whether the failures are stale test assertions or runtime regressions.
2. For metrics bytes, update the test expectation only if browser range reads intentionally fetch less than full candidate bytes; otherwise fix metrics aggregation.
3. For ETag drift, determine whether browser execution still has access to bootstrap ETags. If yes, restore the drift guard. If no, mark the test and handoff evidence accurately.
4. For duration, replace brittle full-object-delay assumptions with a deterministic lower-level timing assertion or remove the over-specific timing threshold.
5. Rerun the full preflight suite.

**Acceptance Criteria:**

- `cargo test -p delta-control-plane --test browser_snapshot_preflight` passes, or the release handoff no longer cites it as passing evidence and documents the explicit known gap.
- Any test expectation changes match current browser range-I/O behavior.

## Task 4: Cancel Contract And Deterministic Cancellation

**Files:**

- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/tests/axon-browser-sdk.spec.ts`
- Modify: `apps/axon-web/tests/browser-worker-matrix.spec.ts`

**Steps:**

1. Decide and document the intended cancel contract in code: either fire-and-forget internal control or public SDK method with an ack envelope.
2. Prefer the conservative fix: keep the worker command fire-and-forget, remove the exported `cancelCommand` helper if unused, and use a local test helper for raw worker cancellation.
3. Make the real-worker cancellation test deterministic by delaying the routed Parquet response or waiting for a query progress event before posting cancel.
4. Run SDK tests and the browser-worker matrix.

**Acceptance Criteria:**

- Public SDK surface does not imply a request/response cancel operation unless one is actually implemented.
- Browser-worker cancellation test does not rely on a tiny-query race.

## Task 5: Release Evidence And Size Caveat

**Files:**

- Modify: `docs/program/browser-datafusion-runtime-parity.md`
- Modify: `docs/program/browser-lakehouse-release-handoff.md`
- Optional: `docs/program/browser-datafusion-size-audit.md`

**Steps:**

1. Update the parity doc so size evidence does not imply the current `axon-web-wasm` app artifact is measured in the older `wasm-datafusion-poc` audit.
2. Either add current app artifact size evidence or state that size posture remains a separate release gate.
3. Keep the release handoff’s evidence list aligned with Task 3’s outcome.
4. Run `git diff --check`.

**Acceptance Criteria:**

- Docs distinguish correctness parity evidence from app artifact size/posture evidence.
- Docs do not cite failing gates as passing release evidence.

## Task 6: Optional Test Support Cleanup And Scan-Corpus Extension

**Files:**

- Modify only if low risk: `crates/wasm-datafusion-poc/tests/*`

**Steps:**

1. Avoid broad refactors unless Tasks 1-5 are green.
2. If time permits, centralize the duplicated loopback range-server helpers in one integration-test support module.
3. Add one descriptor-backed SQL corpus runner or targeted descriptor-backed SQL test so corpus-style SQL parity also traverses `AxonParquetScanExec`.
4. Run `cargo test -p wasm-datafusion-poc`.

**Acceptance Criteria:**

- No behavior changes beyond test maintainability and coverage.
- This task may be explicitly deferred if it threatens the P1 fixes.

## Final Verification Matrix

Run from repository root unless noted:

```bash
cargo test -p wasm-datafusion-poc
cargo test -p wasm-datafusion-session
cargo test -p delta-control-plane --test browser_snapshot_preflight
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
cargo fmt --check
git diff --check
```

Run from `apps/axon-web`:

```bash
npm exec -- tsc --noEmit
npm run test:sdk
npm exec -- playwright test --config=playwright.config.ts tests/browser-worker-matrix.spec.ts
npm run format:check
npm run lint
```
