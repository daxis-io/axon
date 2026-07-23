# Streaming Audit Remediation Execution Plan

> **Execution boundary:** local-only work on `perf/resolve-performance-audit`; do not push or open a PR.

**Goal:** Close every blocker and verification gap found in the fresh audit of the landed internal Arrow IPC streaming coordinator at `6cca364`.

**Architecture:** Keep `sql()` atomic and the child protocol private. The coordinator becomes the sole lifecycle authority: it admits a bounded number of requests, owns the output budget, publishes at most one terminal result, retains draining request IDs until the child acknowledges termination, and replaces an unresponsive child after a bounded watchdog. Rust cancel-all uses query-generation snapshots so one cursor cannot clear cancellation for another. Test-only worker entrypoints inject deterministic child crash/hang behavior without changing the public SDK.

**Tech stack:** TypeScript, Web Workers, Vitest, Playwright, Rust, DataFusion, Arrow IPC, wasm-bindgen.

## Baseline

- `origin/main`: `6cca364465fc4fa714ff7403b6df7e3f229c6e8f`
- `npm test`: 167/167 passed.
- `npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --reporter=line`: 7/9 passed.
  - Firefox did not intercept the nested child route, so the intended crash was never induced.
  - WebKit did not report the nested child worker URL, despite the rollback behavior passing.
- `cargo test -p axon-web-wasm --locked`: known current-main compile failure because the test `ParquetPreflightOutput` initializer omits `object_etag`.

## Commit 1: Coordinator policy and lifecycle

**Files**

- Add: `apps/axon-web/src/sandbox-query-coordinator-policy.ts`
- Add: `apps/axon-web/src/sandbox-query-coordinator-policy.test.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`

**Red tests**

- A raw command with a valid 16 MiB Arrow IPC limit preserves 16 MiB.
- Browser-safe defaults clamp the same request to 8 MiB.
- Invalid or missing raw limits fail closed.
- Admission fails once the configured active plus pending request bound is reached.
- A lifecycle publishes only its first terminal result.
- Deadline expiry enters draining state and cannot later commit success.
- A failed/draining request ID cannot be reused until a matching terminal drains it.

**Implementation**

- Centralize coordinator budget/admission validation in a pure policy module.
- Add explicit `active` and `draining` query states and a first-terminal-wins flag.
- Publish deadline and explicit cancellation as authoritative terminal errors.
- Keep the query entry as a tombstone while cancellation drains.
- Start a bounded watchdog; if the child does not acknowledge cancellation, terminate and replace it.
- Reject new work beyond a fixed coordinator capacity.

**Verification**

- `npm test -- sandbox-query-coordinator-policy.test.ts sandbox-query-stream-protocol.test.ts`
- `npm run lint`
- `npx tsc --noEmit`

## Commit 2: Generation-safe Rust cancellation

**Files**

- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/src/ipc_cursor.rs`
- Modify: `crates/wasm-datafusion-poc/tests/ipc_cursor.rs`
- Modify dependent cancellation tests if they encode the old resettable-latch behavior.

**Red tests**

- Start two cursors, invoke `cancel_running_queries()`, and prove both terminate as cancelled.
- Prove a cursor started after that cancellation generation is healthy.
- Prove per-cursor cancellation does not alter the engine-wide generation.

**Implementation**

- Replace the shared resettable `AtomicBool` with a monotonically increasing cancellation generation.
- Capture a query-scoped generation before planning and carry it through the DataFusion session extension into scan execution.
- Remove cursor and complete-result reset calls.

**Verification**

- `cargo test -p wasm-datafusion-poc --test ipc_cursor --locked`
- `cargo test -p wasm-datafusion-poc --locked`
- `cargo test -p wasm-datafusion-session --locked`

## Commit 3: Cursor memory bounds and compatibility contract

**Files**

- Modify: `crates/wasm-datafusion-poc/src/ipc_cursor.rs`
- Modify: `crates/wasm-datafusion-poc/tests/ipc_cursor.rs`
- Modify: `docs/plans/2026-07-15-internal-arrow-ipc-cursor-execution-plan.md`

**Red tests**

- A new cursor does not reserve the entire pending-batch limit eagerly.
- A valid workload with a 16 MiB total budget remains bounded by the 8 MiB per-batch pending cap.
- Preview row accounting does not visit rows after the preview limit while total row count remains exact.
- Dictionary schemas are rejected before writer construction for static, replacement, delta, growing, and nested modes.

**Implementation**

- Grow `BytesMut` lazily and release empty capacity without immediately reallocating the full limit.
- Count total rows from `RecordBatch::num_rows()` and materialize only the remaining preview prefix.
- Document dictionary rejection and the distinction between total-output and per-batch pending-memory limits.

**Verification**

- `cargo test -p wasm-datafusion-poc --test ipc_cursor --locked`
- `cargo fmt --all -- --check`

## Commit 4: Deterministic cross-browser recovery and heap evidence

**Files**

- Add: `apps/axon-web/src/sandbox-query-worker-test-harness.ts`
- Add: `apps/axon-web/src/sandbox-query-child-crash-test-worker.ts`
- Add: `apps/axon-web/src/sandbox-query-child-hang-test-worker.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts`

**Red tests**

- A forwarded command fails exactly once when a deterministic child crashes, then a replacement child serves the next command in Chromium, Firefox, and WebKit.
- A hung SQL child produces one authoritative deadline, ignores late success, is replaced after the watchdog, and the replacement is healthy.
- A cancelled/faulted request ID is rejected while draining and reusable only after drain/replacement.
- Active SQL child-crash recovery fails staged output atomically in all three browsers.
- A Chromium browser-heap probe measures post-GC heap across repeated atomic queries and enforces a bounded retained-heap delta.

**Implementation**

- Inject deadline, watchdog, capacity, and first-child mode only through a dedicated test worker entrypoint.
- Remove `page.route` and nested-worker URL-count assumptions.
- Record browser-heap measurements in the Playwright result and enforce a conservative deterministic bound.

**Verification**

- `npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --reporter=line`
- `npx playwright test tests/browser-worker-matrix.spec.ts --config=playwright.config.ts --reporter=line`

## Commit 5: Current-main host-test repair

**Files**

- Modify: `apps/axon-web/src/lib.rs`

**Red test**

- `cargo test -p axon-web-wasm --locked` fails because the test initializer omits `object_etag`.

**Implementation**

- Add the missing explicit `object_etag` value and assert its serialized representation.

**Verification**

- `cargo test -p axon-web-wasm --locked`
- `cargo check -p axon-web-wasm --target wasm32-unknown-unknown --locked`

## Commit 6: Final verification and handoff

**Files**

- Modify: `docs/plans/2026-07-23-streaming-audit-remediation.md`

**Verification**

- `npm test`
- `npm run lint`
- `npm run format:check`
- `npx tsc --noEmit`
- `cargo test -p wasm-datafusion-poc --locked`
- `cargo test -p wasm-datafusion-session --locked`
- `cargo test -p axon-web-wasm --locked`
- `cargo check -p axon-web-wasm --target wasm32-unknown-unknown --locked`
- `cargo fmt --all -- --check`
- `npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --reporter=line`
- `npx playwright test tests/browser-worker-matrix.spec.ts --config=playwright.config.ts --reporter=line`
- `git diff --check origin/main...HEAD`
- `git status --short --branch`

Record exact command results, residual limitations, branch, commit range, and publication status in this plan.
