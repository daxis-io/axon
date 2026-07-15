# E3A Exec Contract PI Session

## Baseline

- Worktree: `.worktrees/e3a-exec-contract-pi`.
- Branch: `chore/e3a-exec-contract-pi`.
- Remote baseline: `origin/main` at `536d7752b74f870fd045a27141474bfe41494ee4`.
- Final integration baseline: `origin/main` advanced during implementation to `6c3a6c5c4d2db7505c48b1716660a21e6df83fb1`; the clean five-commit slice was rebased onto that non-overlapping range-cache commit before final verification.
- The root checkout is dirty, four commits ahead of and 47 commits behind `origin/main`; it is not part of implementation or verification.
- Preflight fetched `origin/main` on 2026-07-11 and confirmed the prior E3A contract chain is in ancestry:
  - `5e9d85c docs: plan e3a contract surfaces pi`
  - `d084393 chore(web): extend protobuf codegen for contract packages`
  - `c9f80be chore(web): add catalog contract messages`
  - `61e6062 chore(contract): record rust protobuf backend decision`
  - `b89e47e docs(contract): document e3a contract handoff`
  - `0d6be52 chore(web): preserve optional catalog metadata presence`
- Preflight confirmed the landed dataaccess artifacts are present:
  - `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
  - `apps/axon-web/src/generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts`
  - `apps/axon-web/src/generated/contracts/dataaccess-codegen.test.ts`
  - generated `crates/contract-proto/src/generated/axon.dataaccess.v1.*.rs`
  - the `axon::dataaccess::v1` and `axon_dataaccess_v1` exports in `crates/contract-proto/src/lib.rs`
- Preflight confirmed `apps/axon-web/proto/axon/exec/v1/`, this worktree, and this branch did not already exist.
- Fresh worktree setup installed the absent `apps/axon-web/node_modules` with `npm install` without changing tracked files.
- Baseline checks passed before source edits:
  - `cd apps/axon-web && npm test -- src/generated/contracts` (2 files, 8 tests)
  - `cargo test -p axon-contract-proto --locked`

## Scope

- Add the message and service-descriptor contract for `axon.exec.v1` query execution, worker IPC, previews, errors, metrics, fallback reasons, cancellation, Arrow IPC metadata, and Parquet inspection.
- Extend `axon.dataaccess.v1` with the reusable `BrowserHttpParquetDatasetDescriptor` required by the existing worker and align reusable descriptor protobuf JSON names with the legacy snake_case worker fields.
- Generate and check in deterministic TypeScript for both affected packages.
- Add representative, test-local normalization coverage against the current Rust and TypeScript worker contracts and checked-in handoff fixtures.
- Generate and check in Buffa Rust message types only after locked host and wasm checks prove them.
- Define the `QueryEngine` service descriptor with server-streaming `Execute` and unary `Preview` and `Cancel`, without adding any transport implementation or generated service client/server.

## Public Contract Decisions

- `ExecuteRequest` contains `request_id`, `axon.common.v1.ObjectRef table_ref`, a directly openable descriptor, and the existing `QueryRequest` shape. It never accepts `TableReadResolution`, fallback, or blocked arms.
- `ExecuteResponse` contains either an existing worker event envelope or one terminal worker response envelope.
- `PreviewRequest` contains `request_id`, `table_ref`, a directly openable descriptor, preferred target, and an optional limit.
- `PreviewResponse` contains either success with query metadata and a result preview or a structured query error.
- `CancelRequest` contains its control `request_id` and target `query_id`; `CancelResponse` acknowledges both and whether cancellation was accepted.
- `ExecutionTarget` includes `browser_wasm`, `native`, and reserved `remote_service` targets.
- Commands, events, worker responses, fallback reasons, service stream items, preview outcomes, and preview cells use protobuf `oneof`.
- Arrow IPC remains opaque `bytes`; preview cells are the only row-like UI summary.
- Canonical worker SQL protobuf JSON uses `query`. A test-local adapter accepts the older handoff fixture's `request` alias.
- The TypeScript-only fire-and-forget cancel command and optional success preview are represented. No worker cancel response is invented.
- The generated TypeScript service descriptor is the only service artifact in this PI. Connect dependencies, clients, servers, and transports remain out of scope.

## Normalization Inventory

Representative parity requires explicit boundary normalization rather than byte-for-byte legacy JSON equality:

- Legacy Rust/TypeScript enum strings are lowercase snake_case; protobuf JSON emits enum symbols.
- Legacy serde/TypeScript tagged unions use discriminant properties; protobuf uses `oneof` objects.
- Reusable worker descriptor fields use snake_case JSON names even though protobuf defaults to lowerCamelCase.
- Rust `u64`, JavaScript numbers, and wasm decimal strings normalize to protobuf JSON decimal strings for 64-bit fields.
- Legacy arrays and typed arrays normalize to base64 strings for protobuf `bytes`.
- Protobuf JSON elides default scalar values unless presence is explicit.
- Explicit zero pagination, runtime, metric, byte offset/length/chunk-count, preview-count, nullable Parquet-count, and execution-option values therefore use optional scalars where absence differs from zero or false.
- Legacy previews carry raw primitives; protobuf preview rows carry preview-cell `oneof` values.
- Legacy Arrow delivery and byte-length defaults may be omitted; protobuf normalization supplies the canonical metadata values.
- The older SQL handoff fixture spells the payload `request`; canonical protobuf JSON spells it `query`.
- Rust lacks the TypeScript worker cancel command and optional success preview, so those arms are covered from the TypeScript contract without claiming Rust parity.

## Planned File Boundaries

- Plan and final handoff:
  - `docs/plans/2026-07-11-e3a-exec-contract-pi-session.md`
- Protobuf sources:
  - `apps/axon-web/proto/axon/exec/v1/exec.proto`
  - `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
- Generated TypeScript:
  - `apps/axon-web/src/generated/contracts/protobuf/axon/exec/v1/exec_pb.ts`
  - regenerated `apps/axon-web/src/generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts`
- TypeScript parity tests:
  - `apps/axon-web/src/generated/contracts/exec-codegen.test.ts`
- Rust contract proof:
  - `crates/contract-proto/src/lib.rs`
  - regenerated `crates/contract-proto/src/generated/axon.dataaccess.v1.*.rs`
  - new `crates/contract-proto/src/generated/axon.exec.v1.*.rs`
  - `crates/contract-proto/tests/exec_smoke.rs`
- Fresh-review WASM size proof:
  - `Cargo.lock`
  - `crates/browser-engine-worker/Cargo.toml`
  - `crates/browser-engine-worker/src/lib.rs`
  - `crates/browser-engine-worker/tests/exec_contract_size_probe.rs`
  - `tests/perf/report_browser_worker_artifact.sh`
  - `tests/perf/report_exec_contract_worker_artifact.sh`
  - `tests/perf/README.md`

Existing legacy sources and fixtures are test inputs only and are not edited.

## Commit Slices

1. `docs: plan e3a exec contract pi`
   - Commit this document before source edits.

2. `chore(web): add execution contract messages`
   - Add `exec.proto`, extend `dataaccess.proto`, and check in deterministic TypeScript for both packages.
   - Do not change codegen scripts, package metadata, or dependencies.

3. `test(web): cover execution contract parity`
   - Add test-local normalization adapters and representative coverage for every worker command, response, and event arm; service cardinality; explicit presence; opaque Arrow bytes; preview-cell variants; and the reserved remote target.

4. `chore(contract): generate execution rust contracts`
   - Run the pinned `buf.build/anthropics/buffa:v0.8.0` plugin with `clean: true` and explicit common, dataaccess, and exec proto paths.
   - Keep `buffa = "=0.8.1"` and `buffa-types = "=0.8.1"`.
   - Export both `axon::exec::v1` and compatibility module `axon_exec_v1`.
   - Add Rust smoke coverage for names, exports, oneofs, opaque bytes, and explicit zero presence.
   - Keep the parent E3A M3 size gate reproducible with an off-by-default worker feature that links and encodes the generated contracts without changing production worker behavior.
   - Buffa proves messages only; service stubs are intentionally absent.
   - If generation or compilation fails, discard unproven generated Rust, omit this commit, record the exact blocker and prost fallback note, and leave the PI incomplete.

5. `docs(contract): document execution contract handoff`
   - Record final commits, verification evidence, normalization boundaries, Buffa host/wasm status, and any exact environment blockers.
   - Restate the provider ownership boundary and whether `axon/fs/v1` is unblocked.

## Non-Goals

- Do not add Connect, RPC, HTTP, worker, or provider implementations.
- Do not add generated clients, servers, transports, production adapters, or migration code.
- Do not change existing codegen scripts, production dependency graphs, query runtime behavior, browser SDK behavior, or handoff fixtures. The proof-only `exec-contract-size-probe` feature may link the existing contract/Buffa crates solely for the parent E3A M3 artifact check.
- Do not model Arrow rows or record batches in protobuf.
- Do not accept unresolved, fallback, or blocked read plans in execution or preview requests.
- Do not implement E9 adoption or `axon/fs/v1`.
- Do not push, open a pull request, or modify the root checkout.

## Verification Plan

Run in this order from the isolated worktree:

```bash
cd apps/axon-web && npm install                         # already completed because node_modules was absent
cd apps/axon-web && npm run build:wasm
cd apps/axon-web && npm run codegen:config:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cargo test -p browser-sdk --test ipc --locked
cargo test -p browser-sdk --test release_handoff_examples --locked
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo fmt --check -p axon-contract-proto
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
cargo test -p browser-engine-worker --features exec-contract-size-probe --test exec_contract_size_probe --locked
bash tests/perf/report_exec_contract_worker_artifact.sh
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

## Exit Gates

- Exactly five correctly ordered local commits exist on top of `origin/main`, and the worktree is clean.
- Generated TypeScript is deterministic and all config/contract drift gates pass.
- Representative normalized parity covers every existing worker command, response, and event arm without claiming byte-for-byte JSON parity.
- Execution and preview accept only directly openable descriptors.
- Arrow IPC exists only as opaque bytes plus metadata; preview cells are the sole row-like summary.
- `QueryEngine.Execute` is server-streaming; `Preview` and `Cancel` are unary.
- Buffa-generated common/dataaccess/exec messages pass locked host and wasm checks, with no service-stub claim.
- The feature-gated browser-worker probe links and retains the generated Buffa contract path while remaining within the 750,000-byte artifact budget; a subsequent default build excludes the probe export.
- Every public `axon.exec.v1` declaration has concise source documentation, and every reusable descriptor JSON-name override has direct reflection coverage.
- No required check is skipped without its exact reason being recorded.
- `CatalogProvider` discovers, `DataAccessResolver` resolves, and `ExecutionProvider` runs.
- E9 adoption and Connect implementation remain later work.
- Recommend `axon/fs/v1` only if every prior slice and exit gate completes; otherwise execution-contract proof remains blocking.

## Completed Slice Commits

- `9f511ea docs: plan e3a exec contract pi`
- `f192047 chore(web): add execution contract messages`
- `a2c4ad2 test(web): cover execution contract parity`
- `74e0b3d chore(contract): generate execution rust contracts`
- Slice 5 is this execution-contract handoff documentation commit.

## Implemented Contract Surface

- `axon.dataaccess.v1`
  - Added `BrowserHttpParquetDatasetDescriptor` with the existing worker's `table_uri`, partition metadata, capability reports, and files.
  - Aligned reusable file, snapshot, and Parquet descriptor protobuf JSON names with legacy snake_case worker fields, including `object_etag`, `snapshot_version`, `required_capabilities`, and `active_files`.
- `axon.exec.v1`
  - Query requests, result pages, execution options, runtime limits, responses, errors, fallback reasons, capability keys, metrics, and browser access modes.
  - Execution targets `browser_wasm`, `native`, and reserved `remote_service`.
  - Opaque Arrow IPC results and chunks, including delivery, length, and chunk metadata.
  - Result previews with string, number, boolean, and null cell oneofs.
  - Parquet compression, column, column-chunk, row-group, and inspection summaries.
  - All seven current worker command arms: open table, open Delta table, open Parquet dataset, inspect Parquet, SQL, TypeScript-only fire-and-forget cancel, and dispose.
  - All eight worker event arms: progress, log, range-read metrics, cache metrics, fallback, cancellation, terminal error, and Arrow IPC chunk.
  - All five terminal worker response arms: opened, success, Parquet inspection, disposed, and error. Success optionally carries the TypeScript preview; there is no invented worker cancel response.
  - Directly openable snapshot/Parquet descriptor oneof for execution and preview. Read-resolution fallback and blocked plans are not accepted.
  - `QueryEngine.Execute` server stream plus unary `Preview` and `Cancel` descriptors.
- Generated TypeScript:
  - `apps/axon-web/src/generated/contracts/protobuf/axon/exec/v1/exec_pb.ts`
  - regenerated dataaccess output with the reusable Parquet descriptor and JSON-name alignment.
- Representative normalized parity:
  - `apps/axon-web/src/generated/contracts/exec-codegen.test.ts`
  - 14 execution-contract tests cover every worker command/event/response arm, checked-in and inline fixtures, descriptor JSON names, the legacy SQL `request` alias, default/presence normalization, opaque bytes, preview cells, service method kinds, and the remote target.

## Final Normalization Boundaries

- Coverage is representative normalized parity, not byte-for-byte parity.
- Test-local adapters map lowercase legacy enum strings to protobuf enum symbols and externally tagged serde/TypeScript unions to protobuf oneofs.
- Existing 64-bit numbers and wasm decimal strings normalize to protobuf JSON decimal strings.
- Existing byte arrays, `ArrayBuffer`, and `Uint8Array` values normalize to protobuf JSON base64; protobuf never models Arrow result rows.
- Optional scalar presence preserves explicit zero/false values for pagination, execution defaults, runtime limits, metrics, Arrow offsets/lengths/counts, preview counts, cancellation acceptance, and nullable Parquet counts.
- The resolved snapshot descriptor uses explicit presence for `snapshot_version`, so valid Delta version `0` remains visible in protobuf JSON.
- Raw legacy preview primitives normalize to `PreviewCell` oneofs.
- Omitted legacy Arrow delivery, byte length, and chunk count normalize to the canonical single-buffer metadata.
- The old checked-in SQL fixture's `request` property normalizes to canonical protobuf JSON `query`.
- Rust lacks the TypeScript-only worker cancel arm and optional success preview; the TypeScript boundary covers those arms without claiming Rust parity.

## Fresh-Review Remediation

- The reusable-descriptor parity test now reflects over the generated descriptors and asserts every required snake_case JSON name, including `object_etag`, `partition_column_types`, and `browser_compatibility`. A controlled `object_etag = "objectEtag"` mutation failed that focused test before the required schema was restored and regenerated.
- All 64 public enum, message, and service declarations in `axon.exec.v1` now carry concise source documentation. The new `BrowserHttpParquetDatasetDescriptor` declaration is documented as well. The schema fields, numbers, cardinalities, options, and service signatures are unchanged.
- `browser-engine-worker` now exposes an off-by-default `exec-contract-size-probe` feature. Its retained export constructs and Buffa-encodes a representative `ExecuteRequest` containing common, data-access, and execution messages.
- `tests/perf/report_exec_contract_worker_artifact.sh` verifies the feature build includes `axon-contract-proto`, `buffa`, and `buffa-types`, verifies the exported probe survives WASM linking, and applies the existing 750,000-byte worker budget. The probe artifact is 670,799 bytes, 48,445 bytes above the unchanged 622,354-byte default artifact and 79,201 bytes below budget.

## Final Verification Evidence

Setup and the complete ordered verification bundle passed from the isolated worktree:

```bash
cd apps/axon-web && npm install
cd apps/axon-web && npm run build:wasm
cd apps/axon-web && npm run codegen:config:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cargo test -p browser-sdk --test ipc --locked
cargo test -p browser-sdk --test release_handoff_examples --locked
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo fmt --check -p axon-contract-proto
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
cargo test -p browser-engine-worker --features exec-contract-size-probe --test exec_contract_size_probe --locked
bash tests/perf/report_exec_contract_worker_artifact.sh
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

Focused results:

- Generated-contract Vitest: 3 files, 22 tests passed; 14 are the new execution-contract tests.
- Browser SDK IPC: 18 tests passed.
- Browser SDK release-handoff fixtures: 9 tests passed.
- Buffa Rust smoke: 3 tests passed, covering message names, both export paths, worker/service oneof construction, opaque-byte binary round trips, and explicit zero presence.
- Both `axon-contract-proto` host and `wasm32-unknown-unknown` locked checks passed.
- The feature-gated worker probe test passed and the linked probe artifact measured 670,799 bytes against the 750,000-byte budget; dependency-tree and retained-export assertions proved that the generated Buffa path was present.
- The default release worker remained 622,354 bytes with no probe export, and the browser dependency/bundle guardrails passed against that restored artifact.
- Direct descriptor reflection covers every required snake_case JSON name, and all 64 public exec declarations are documented.
- No required check was blocked or skipped.

Environment notes:

- Buf TypeScript and Buffa generation use remote plugins. Restricted-sandbox attempts could not reach `buf.build`, and fresh-review reruns initially received the BSR `resource_exhausted` rate limit; the exact pinned generators, focused drift checks, and aggregate `codegen:check` passed after the remote queue cleared. During the throttle, the affected TypeScript file was refreshed with an untracked local `@bufbuild/protoc-gen-es` 2.12.1 install and then validated against the pinned remote plugin; no dependency manifest or lockfile changed.
- The final branch was rebased cleanly onto concurrent `origin/main` commit `6c3a6c5 perf(browser): add shared range cache substrate`; it did not overlap this contract slice.
- A post-implementation `buf breaking` audit against `origin/main` reports eight intentional E3A living-draft changes: seven required descriptor `json_name` alignments plus explicit `snapshot_version` presence. Field numbers and scalar types are unchanged. These pre-adoption JSON/presence changes are required by this PI and must be carried into E9 adoption before the contract is frozen.
- The pinned Buffa command used `clean: true`, `buf.build/anthropics/buffa:v0.8.0`, and explicit common, dataaccess, and exec proto paths.
- `buffa = "=0.8.1"` and `buffa-types = "=0.8.1"` remain unchanged.
- Both final WASM builds emitted the existing `wasm-parquet-engine` deprecation warning for `ArrowReaderOptions::page_index`; it is unrelated to this message-only contract slice.

## Rust Backend Result

- Buffa generated common/dataaccess/exec message, view, and oneof code successfully.
- Generated exec messages are available through both `axon::exec::v1` and compatibility module `axon_exec_v1`.
- Buffa generated no `QueryEngine` client, server, or service stub. This PI proves messages only; the TypeScript-generated service descriptor is the sole service artifact.
- The prost fallback was not needed.

## Ownership and Next Work

- `CatalogProvider` discovers.
- `DataAccessResolver` resolves.
- `ExecutionProvider` runs.
- E9 adopts these messages into runtime/provider seams later.
- Connect implementation, clients, servers, and transports remain later work.
- All execution-contract slices and exit gates completed, so `axon/fs/v1` is unblocked and is the recommended next E3A contract package.
