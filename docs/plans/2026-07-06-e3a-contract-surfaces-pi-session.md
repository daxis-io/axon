# E3A Contract Surfaces PI Session

## Baseline

- Worktree: `.worktrees/e3a-contract-surfaces-pi`.
- Branch: `chore/e3a-contract-surfaces-pi`.
- Remote baseline: `origin/main` at `d3ae40de3caf324f61194ba67a475ae8633ef72f`.
- The root checkout is dirty/diverged and is not part of implementation or verification.
- Preflight confirmed `origin/main` contains:
  - E0 M5 query-cache persistence and cache-policy cleanup through `d3ae40d`.
  - The E0 config Buf module under `apps/axon-web/proto/axon/config/v1/settings.proto`.
  - Checked-in config-generated protobuf and JSON Schema output under `apps/axon-web/src/generated/config/`.
- Fresh worktree setup observed:
  - `apps/axon-web/node_modules` was absent and was installed with `npm install`.
  - `apps/axon-web/src/wasm` was absent; run `npm run build:wasm` before TypeScript checks.

## Scope

- Add the first E3A contract-surface slice for normalized discovery contracts only.
- Extend the existing `apps/axon-web` Buf/codegen seam so config generation remains valid and contract generation is drift-checked.
- Add `axon/common/v1` and `axon/catalog/v1` proto messages and checked-in TypeScript output.
- Add a small TypeScript import/JSON round-trip smoke test for the generated common/catalog messages.
- Probe the Rust protobuf backend for a minimal `axon/common/v1` proof. If buffa cannot be proven in this worktree, record the exact blocker and the prost fallback instead of shipping an unproven crate.
- Document the next slices in order: `axon/dataaccess/v1`, then `axon/exec/v1`, then `axon/fs/v1`.

## Non-Goals

- Do not add provider implementations, catalog runtime adoption, read resolution, execution messages, Connect services, UC auth, Envoy/BFF work, Monaco, Arrow grid work, or volume file browsing.
- Do not model signed URLs, object grants, descriptors, worker IPC, result rows, table reads, or file entries in `axon/catalog/v1`.
- Do not replace the E0 config codegen path or move existing config-generated output.
- Do not push.

## Commit Slices

1. `docs: plan e3a contract surfaces pi`
   - Files:
     - `docs/plans/2026-07-06-e3a-contract-surfaces-pi-session.md`
   - Record verified baseline, scope, non-goals, commit boundaries, tests, and exit gates.

2. `chore(web): extend protobuf codegen for contract packages`
   - Files:
     - `apps/axon-web/package.json`
     - `apps/axon-web/package-lock.json` only if scripts/dependency metadata changes
     - `apps/axon-web/buf.gen.contracts.yaml`
     - `apps/axon-web/scripts/*contract*codegen*.mjs`
     - existing config codegen scripts only as needed to share helpers
   - Keep `npm run codegen:config:check` valid.
   - Make `npm run codegen:check` run config drift plus discovered contract-package drift.

3. `chore(web): add catalog contract messages`
   - Files:
     - `apps/axon-web/proto/axon/common/v1/common.proto`
     - `apps/axon-web/proto/axon/catalog/v1/catalog.proto`
     - `apps/axon-web/src/generated/contracts/protobuf/axon/common/v1/*_pb.ts`
     - `apps/axon-web/src/generated/contracts/protobuf/axon/catalog/v1/*_pb.ts`
     - `apps/axon-web/src/settings/config-codegen.test.ts` or a focused contract-codegen test
   - Add discovery-only common/catalog messages.
   - Add a Vitest import and JSON round-trip smoke test.

4. `chore(contract): record rust protobuf backend decision`
   - Preferred if proven:
     - add `crates/contract-proto` as a workspace member
     - pin `buffa = "=0.8.1"`
     - use `buf.build/anthropics/buffa:v0.8.0` for checked-in generated `axon.common.v1` Rust output
     - add a small `lib.rs` wrapper suppressing generated style warnings
   - If the proof fails in this worktree:
     - document the exact buffa blocker and prost fallback in this plan
     - do not add an unproven crate

5. `docs(contract): document e3a contract handoff`
   - Files:
     - `docs/plans/2026-07-06-e3a-contract-surfaces-pi-session.md`
   - Record final evidence and next slices.

## Verification Plan

Setup prerequisites:

```bash
cd apps/axon-web && npm install
cd apps/axon-web && npm run build:wasm
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
```

Required checks:

```bash
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/settings/config-codegen.test.ts
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
```

Rust checks if `crates/contract-proto` is added:

```bash
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
```

Final checks:

```bash
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

## Exit Gates

- The branch contains the planned local commits and no push has been performed.
- Config codegen remains isolated and `npm run codegen:config:check` still works.
- Contract codegen is deterministic and `npm run codegen:check` covers both config and contracts.
- Generated TypeScript exists at `apps/axon-web/src/generated/contracts/protobuf/axon/{common,catalog}/v1/*_pb.ts`.
- Catalog contracts stay discovery-only and do not include read resolution, signed URLs, object grants, execution messages, services, or volume file entries.
- Buffa Rust proof is either green with checked-in generated output and wasm `cargo check`, or the exact blocker and prost fallback are recorded.
- Final response lists passed checks, checks skipped with reasons, and commit list.

## Completed Slice Commits

- `5e9d85c docs: plan e3a contract surfaces pi`
- `d084393 chore(web): extend protobuf codegen for contract packages`
- `c9f80be chore(web): add catalog contract messages`
- `61e6062 chore(contract): record rust protobuf backend decision`
- Slice 5 is this handoff documentation commit.

## Implemented Contract Surface

- `axon/common/v1`
  - `ObjectRef`
  - `PageRequest`
  - `PageInfo`
  - `ProviderCapabilities`
  - `ProviderAuthority`
  - `ProviderError`
  - `ProviderErrorCode`
- `axon/catalog/v1`
  - Discovery nodes: `CatalogNode`, `SchemaNode`, `TableNode`, `ColumnNode`, `VolumeNode`, `FunctionNode`, `ModelNode`
  - Metadata: `TableMetadata`, `VolumeMetadata`, `DeltaProtocolFeature`
  - Responses: list responses for catalogs, schemas, tables, columns, volumes, functions, and models, plus get-table/get-volume metadata responses
- Generated TypeScript:
  - `apps/axon-web/src/generated/contracts/protobuf/axon/common/v1/common_pb.ts`
  - `apps/axon-web/src/generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts`
- TypeScript smoke:
  - `apps/axon-web/src/generated/contracts/contracts-codegen.test.ts`

## Rust Backend Decision

- Decision: use buffa for the first Rust protobuf proof.
- Generation proof:
  - Buf remote plugin: `buf.build/anthropics/buffa:v0.8.0`
  - Generated source package: `axon.common.v1`
  - Checked-in output: `crates/contract-proto/src/generated/axon.common.v1.*.rs`
- Runtime dependency:
  - `buffa = "=0.8.1"`
- Proof crate:
  - `crates/contract-proto`
  - Package name: `axon-contract-proto`
  - Wrapper: no-std module `axon_common_v1`
- Verified:
  - `cargo check -p axon-contract-proto --locked`
  - `cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked`
- Prost fallback was not needed for this slice.

## Verification Evidence

Passed during implementation:

```bash
cd apps/axon-web && npm install
cd apps/axon-web && npm run codegen:config:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts
cd apps/axon-web && npm test -- src/settings/config-codegen.test.ts src/generated/contracts
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cargo check -p axon-contract-proto
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo fmt --check -p axon-contract-proto
git diff --check
```

Environment notes:

- Direct `buf` codegen commands that use remote plugins need network access outside the restricted sandbox. The same commands passed when rerun with network access.
- The machine hit `ENOSPC` while creating a temp directory for codegen. Root cause was the Data volume being full; removing generated Rust build artifacts at `/Users/ethanurbanski/axon/target` freed enough space to continue.
- Because `target/` was removed, later Rust/browser checks rebuild from scratch.

## Next Slices

1. `axon/dataaccess/v1`
   - Port read-resolution messages, descriptors, object-grant request/response shapes, and blocked/fallback reason taxonomy.
   - Add parity tests against existing `crates/query-contract` JSON Schema/OpenAPI fixtures.
   - Keep descriptor/grant messages free of long-lived secrets; signed URLs and proxy paths remain short-lived data-access outputs.

2. `axon/exec/v1`
   - Port execution request/response and browser-worker IPC envelopes.
   - Define the `QueryEngine` service shape only where bytes cross a process/worker boundary.
   - Keep Arrow IPC result payloads opaque bytes; do not model result rows in proto.

3. `axon/fs/v1`
   - Add filesystem entry/listing/object-read-resolution messages for E8.
   - Keep volume file browsing out of `axon/catalog/v1`; catalog volumes remain discovery objects only.

## Handoff Notes

- `npm run codegen:config:check` remains the config-only drift gate and now constrains generation to `proto/axon/config/v1/settings.proto`.
- `npm run codegen:contracts` and `npm run codegen:contracts:check` discover contract protos under `proto/axon` and exclude `proto/axon/config`.
- `npm run codegen:check` runs both config and contract drift checks.
- The Rust proof intentionally covers only `axon.common.v1`; dataaccess/exec/fs should extend the crate only after their proto surfaces land and pass locked host + wasm checks.
