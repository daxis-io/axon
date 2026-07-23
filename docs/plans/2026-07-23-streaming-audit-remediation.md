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

## Execution Record - 2026-07-23

### Boundary And Refs

- Worktree:
  `/Users/ethanurbanski/axon/.worktrees/perf-audit-resolution`
- Branch: `perf/resolve-performance-audit`
- Remote base:
  `6cca364465fc4fa714ff7403b6df7e3f229c6e8f`
- Verified implementation and reconciliation range:
  `6cca364..d19f316`
- Publication status: local commits only; no push or pull request.
- The dirty root checkout and all pre-existing dirty worktrees were left
  untouched. Their disposition is recorded in
  `docs/research/browser-query-performance-workstream-inventory.md`.

### Resolved Findings

| Finding | Resolution |
| --- | --- |
| Coordinator lifecycle races | `52b740c` makes the coordinator authoritative for admission, terminal publication, draining request IDs, deadline/cancellation tombstones, watchdog replacement, and bounded capacity. |
| Cancel-all interference | `c3aec8e` replaces the shared resettable latch with query-generation snapshots so cancellation reaches every cursor that was live at the snapshot without poisoning later work. |
| Cursor allocation and preview cost | `62d8225` grows pending storage lazily, releases empty capacity, counts rows in O(1) per batch, materializes only the preview prefix, and rejects dictionary modes before writer construction. |
| Coordinator staging and observability | `8d64d44` rejects before retaining an excess chunk and projects coordinator, pending-encoded, and transport-chunk peaks through Rust, protobuf, TypeScript, and the SDK. |
| Total-output versus pending-memory ambiguity | `4b39e4a` proves a valid result above 8 MiB can succeed under the separate 16 MiB total-output limit while each logical batch stays under the 8 MiB pending cap. |
| Cross-browser fault and atomicity gaps | `a02c346` covers coordinator-authored cancellation, late failure, output-budget failure, crash, hang, and recovery without partial public output in Chromium, Firefox, and WebKit. |
| Current-main host fixture | `7d02686` supplies and serializes the required `object_etag`. |
| Broader performance audit | `ddd53f1`, `f20acb7`, `9178eab`, `a021672`, `10f3e61`, `af42ede`, and `4b39e4a` close the worker-size, public-S3 reproducibility, worker-pool decision, page-index compatibility, speculative-overfetch, component-memory, and full browser-memory evidence findings. |
| Canonical records and format gate | `ab7a61a` reconciles the workstream and parity records. `d19f316` applies the six mechanical Prettier rewrites required for the repository-wide format check. |

### Final Verification

| Command or suite | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed. |
| `cargo test -p query-contract --locked` | Passed: 3 unit, 58 contract, and 13 release-handoff tests. |
| `cargo test -p wasm-parquet-engine --locked` | Passed: 53 host tests after rerunning outside the port sandbox. The first sandboxed attempt failed only because 26 tests could not bind ephemeral loopback ports. |
| `cargo test -p wasm-datafusion-poc --locked --tests` | Passed: 88 tests, including 11 cursor, 11 budget, and 21 pushdown tests. |
| `cargo test -p wasm-datafusion-session --locked` | Passed: 42 tests. |
| `cargo test -p axon-web-wasm --locked` | Passed: 3 tests. |
| `cargo check -p axon-web-wasm --target wasm32-unknown-unknown --locked` | Passed. |
| `npm test` | Passed: 173 tests in 32 files. |
| `npm run format:check` | Passed after the six Prettier-only baseline rewrites in `d19f316`. |
| `npm run lint` and `npm exec -- tsc --noEmit` | Passed. |
| `npm run test:s3-perf-fixture` and `npm run verify:s3-perf-fixture` | Passed, including the intentional checksum-mismatch negative case and 21-object pinned metadata validation. |
| `npm run build:wasm` | Passed. |
| `bash tests/perf/report_datafusion_wasm_size_test.sh` | Passed. |
| `AXON_DF_SIZE_OUT_DIR=target/df-size/axon-web-wasm-resolution bash tests/perf/report_datafusion_wasm_size.sh` | Passed. Raw: 45,397,475; bindgen: 40,081,640; optimized: 23,450,156; gzip: 6,232,474; Brotli: 3,932,517 bytes. The Brotli artifact has 2,358,939 bytes of headroom under the 6,291,456-byte gate. |
| `npm run test:sdk -- --reporter=line` | Passed: 151 tests. |
| `AXON_BROWSER_QUERY_PERF_SKIP_BUILD=1 bash tests/perf/browser_query_performance.sh --reporter=line` | Passed: 1 Chromium test using the already verified shipped WASM and checked-in Delta/Parquet fixture. The release wrapper recorded implementation head `d19f316279e634f2888c54b8481271b31545a803`. |
| `npm run test:browser:worker-pool-compatibility -- --reporter=line` | Passed: 1 Chromium two-coordinator compatibility test. This does not change the research-only worker-pool verdict. |
| `npx playwright test tests/internal-arrow-ipc-stream.spec.ts --config=playwright.config.ts --reporter=line` | Passed: 18 tests across Chromium, Firefox, and WebKit. |
| `npx playwright test tests/browser-worker-matrix.spec.ts --config=playwright.config.ts --reporter=line` | Passed: 21 tests across Chromium, Firefox, and WebKit. |

The final ignored browser artifact at
`target/perf/browser-query-performance.json` records:

| Measurement | Value |
| --- | ---: |
| Startup total | 124.72 ms |
| Cold query / Arrow IPC ready | 72.13 / 71.87 ms |
| Warm query / Arrow IPC ready | 10.04 / 9.98 ms |
| Five repeated queries | 98.05 ms total |
| Public result | 968 bytes in 3 chunks |
| Coordinator peak / cap | 968 / 8,388,608 bytes |
| Cursor pending / transport peak | 704 / 704 bytes |
| Post-GC memory before / after 20 queries | 27,320,276 / 27,532,536 bytes |
| Retained post-GC delta / gate | 212,260 / 16,777,216 bytes |
| One-byte output-budget case | 0 chunks, 1 structured error, 0 successes |

### Residual Boundaries, Not Blockers

- `measureUserAgentSpecificMemory()` is Chromium-only and requires a
  cross-origin-isolated page. Lifecycle and atomicity remain covered in all
  three browser engines.
- The 16 MiB memory gate bounds retained post-GC user-agent memory delta, not
  total RSS or every DataFusion operator's transient peak.
- The public SDK remains atomic. The coordinator may retain a successful result
  up to its 8 MiB browser-safe staging cap; progressive public delivery is out
  of scope.
- The pinned public-S3 fixture was anonymously staged and checksum-validated,
  but the live browser S3 suite was not rerun because its environment variables
  were absent and this local-only audit did not authorize cloud mutation. The
  2026-07-16 artifact remains the only live performance verdict.
- `npm run codegen:check` was not run because the checked-in Buf configuration
  uses remote plugins. This execution did not authorize uploading contract
  sources to that service. The changed generated surfaces are covered by the
  Rust contract tests, TypeScript codegen tests, TypeScript compilation, and
  exact local generation used during implementation.
- Explicit Parquet column-index and offset-index policies are now independently
  preserved. No page-level browser byte reduction is claimed without new
  evidence.
- Worker pools remain a bounded research direction. Current-main two-shard
  compatibility passes, but production value and WCRPC remain unjustified.
