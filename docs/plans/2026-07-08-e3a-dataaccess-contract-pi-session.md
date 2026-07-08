# E3A Dataaccess Contract PI Session

## Baseline

- Worktree: `.worktrees/e3a-dataaccess-contract-pi`.
- Branch: `chore/e3a-dataaccess-contract-pi`.
- Remote baseline: `origin/main` at `0d6be527f4b9e4913b4e2f4f3fb77ab87d0a839b`.
- Root checkout status at preflight: dirty and diverged from `origin/main`; it is not part of implementation or verification.
- Preflight fetched `origin/main` on 2026-07-08 and confirmed the landed E3A contract slice through:
  - `5e9d85c docs: plan e3a contract surfaces pi`
  - `d084393 chore(web): extend protobuf codegen for contract packages`
  - `c9f80be chore(web): add catalog contract messages`
  - `61e6062 chore(contract): record rust protobuf backend decision`
  - `b89e47e docs(contract): document e3a contract handoff`
  - `0d6be52 chore(web): preserve optional catalog metadata presence`
- Preflight confirmed `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto` is absent on `origin/main`.
- Fresh worktree setup observed `apps/axon-web/node_modules` was absent and installed with `npm install`.

## Scope

- Add message-only `axon.dataaccess.v1` contract messages for browser-safe read access resolution.
- Generate checked-in TypeScript protobuf contracts under `apps/axon-web/src/generated/contracts/protobuf/axon/dataaccess/v1/`.
- Add representative parity and normalization coverage against the existing serde/OpenAPI contract surfaces.
- Extend the Rust contract proof crate to export generated `axon_dataaccess_v1` buffa types only if locked host and wasm checks prove the output.
- Record handoff evidence and the next contract-package order.

## Non-Goals

- Do not add protobuf services, Connect services, HTTP transport wiring, runtime adapters, provider implementations, broker implementations, worker IPC execution, UI adoption, or live vendor tests.
- Do not change the config codegen surface.
- Do not claim byte-for-byte JSON parity where protobuf `oneof` JSON differs from legacy serde JSON.
- Do not widen `axon.catalog.v1`; catalog discovers, dataaccess resolves.
- Do not push.

## Planned File Boundaries

- Plan and handoff:
  - `docs/plans/2026-07-08-e3a-dataaccess-contract-pi-session.md`
- Web proto and generated TypeScript:
  - `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
  - `apps/axon-web/src/generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts`
  - `apps/axon-web/src/generated/contracts/contracts-codegen.test.ts` or a focused sibling under `apps/axon-web/src/generated/contracts/`
- Existing serde/OpenAPI fixture inputs for tests:
  - `crates/query-contract/schemas/read-access-plan.schema.json`
  - `crates/query-contract/schemas/object-grants.openapi.json`
- Rust contract proof, only if generation and checks pass:
  - `crates/contract-proto/src/lib.rs`
  - generated `crates/contract-proto/src/generated/axon.dataaccess.v1.*.rs`
  - contract proto Buf generation config if the existing crate needs package inclusion

## Commit Slices

1. `docs: plan e3a dataaccess contract pi`
   - Add this PI session document before source edits.

2. `chore(web): add dataaccess contract messages`
   - Add `axon.dataaccess.v1` proto messages.
   - Generate TypeScript with the existing contract codegen path.
   - Keep JSON names compatible where the legacy contract requires them.

3. `test(web): cover dataaccess contract parity`
   - Add representative inline fixtures from `read-access-plan.schema.json` and `object-grants.openapi.json`.
   - Cover explicit normalization for legacy `plan_type` and nullable `partition_values`.

4. `chore(contract): generate dataaccess rust contracts`
   - Export `axon_dataaccess_v1` from `crates/contract-proto/src/lib.rs`.
   - Keep `buffa = "=0.8.1"` and the working Buf plugin `buf.build/anthropics/buffa:v0.8.0`.
   - If buffa generation or locked checks fail, stop this slice and record the exact blocker plus prost fallback note here.

5. `docs(contract): document dataaccess contract handoff`
   - Record verification evidence.
   - Set next PI order to `axon/exec/v1`, then `axon/fs/v1`.
   - Restate ownership: `CatalogProvider` discovers, `DataAccessResolver` resolves, `ExecutionProvider` runs.

## Verification Plan

```bash
cd apps/axon-web && npm install
cd apps/axon-web && npm run codegen:config:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo fmt --check -p axon-contract-proto
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

## Exit Gates

- The branch contains the planned local commits and no push has been performed.
- `axon.dataaccess.v1` stays message-only and covers read plan reasons, capability report, partition values, browser HTTP file/snapshot descriptors, brokered object access, brokered policy authority, brokered Delta, Delta Sharing, SQL fallback, blocked read plans, `ReadAccessPlan` oneof, `TableReadResolution` oneof, and object-grant list/head/batch-sign/range request/response messages.
- Contract JSON names preserve the required legacy spellings, including `tableId`, `fullName`, `grantId`, `expiresAtEpochMs`, `deltaAccessMode`, `size_bytes`, `partition_values`, and legacy object-grant camelCase fields.
- Generated TypeScript is deterministic and checked in.
- Legacy serde/OpenAPI representative fixtures have focused protobuf normalization coverage.
- Rust dataaccess output is either proven by locked host and wasm checks, or the exact generation/check blocker is recorded without shipping unproven output.
- Final response lists passed checks, blockers, commit list, and current git status.

