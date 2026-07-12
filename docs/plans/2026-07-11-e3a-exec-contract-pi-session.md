# E3A Exec Contract PI Session

## Baseline

- Worktree: `.worktrees/e3a-exec-contract-pi`.
- Branch: `chore/e3a-exec-contract-pi`.
- Remote baseline: `origin/main` at `536d7752b74f870fd045a27141474bfe41494ee4`.
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
   - Buffa proves messages only; service stubs are intentionally absent.
   - If generation or compilation fails, discard unproven generated Rust, omit this commit, record the exact blocker and prost fallback note, and leave the PI incomplete.

5. `docs(contract): document execution contract handoff`
   - Record final commits, verification evidence, normalization boundaries, Buffa host/wasm status, and any exact environment blockers.
   - Restate the provider ownership boundary and whether `axon/fs/v1` is unblocked.

## Non-Goals

- Do not add Connect, RPC, HTTP, worker, or provider implementations.
- Do not add generated clients, servers, transports, production adapters, or migration code.
- Do not change existing codegen scripts, dependencies, query runtime behavior, browser SDK behavior, or handoff fixtures.
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
- No required check is skipped without its exact reason being recorded.
- `CatalogProvider` discovers, `DataAccessResolver` resolves, and `ExecutionProvider` runs.
- E9 adoption and Connect implementation remain later work.
- Recommend `axon/fs/v1` only if every prior slice and exit gate completes; otherwise execution-contract proof remains blocking.

## Completed Slice Commits

Pending implementation.

## Final Verification Evidence

Pending implementation.
