# Roadmap Next Steps Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:verification-before-completion before any completion claim.

**Goal:** Validate the newly merged public object-storage product path, refresh the roadmap evidence, and separate repo-owned next work from external launch blockers.

**Architecture:** Treat public GCS table-root access as the immediate release-candidate path, using browser-owned snapshot reconstruction and the existing `BrowserHttpSnapshotDescriptor -> openDeltaTable()` handoff. Keep external service work out of repo success claims, and keep broad/default-worker DataFusion as the next repo-owned product decision after the public-object-storage gates are green.

**Tech Stack:** `apps/axon-web`, Playwright, TypeScript, Vite, Rust/Cargo browser-runtime crates, release-gate Markdown docs.

---

## Task 1: Baseline And Scope

**Files:**

- Read: `README.md`
- Read: `docs/program/browser-owned-descriptor-materialization.md`
- Read: `docs/program/browser-datafusion-runtime-parity.md`
- Read: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Read: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`

**Steps:**

1. Confirm the working tree status.
2. Confirm there are no live open Axon GitHub issues or PRs that supersede the local docs.
3. Confirm the current roadmap priority order:
   - public object-storage release-candidate validation;
   - external blocker ownership/proof;
   - default-worker DataFusion decision;
   - issue-132 disposition outside the main Axon browser roadmap.

**Verification:**

```bash
git status --short
gh issue list --limit 20 --state open --json number,title,labels,milestone,updatedAt,url
gh pr list --limit 20 --state open --json number,title,headRefName,updatedAt,url
```

Expected: only intentional local plan/doc changes, and no open GitHub issues or PRs.

## Task 2: Public Object-Storage App Gates

**Files:**

- Test: `apps/axon-web/tests/object-storage.spec.ts`
- Test: `apps/axon-web/tests/query-source.spec.ts`
- Test: `apps/axon-web/tests/editor-smoke.spec.ts`
- Test: `apps/axon-web/tests/public-gcs-live.spec.ts`
- Code under test: `apps/axon-web/src/services/object-storage.ts`
- Code under test: `apps/axon-web/src/services/query-source.ts`
- Code under test: `apps/axon-web/src/services/query.ts`
- Code under test: `apps/axon-web/src/editor/connect/ConnectModal.tsx`

**Steps:**

1. Install app dependencies if the worktree does not have `node_modules`.
2. Build the generated WASM bridge before TypeScript checks in a fresh worktree.
3. Generate the prod-like fixture before editor-smoke tests in a fresh worktree.
4. Run formatting, lint, TypeScript, and SDK tests.
5. Run the focused editor smoke for object storage and connect source flows.
6. Run the public GCS live smoke in skip-safe mode; it should skip unless `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is set.

**Verification:**

```bash
cd apps/axon-web
npm install
npm run build:wasm
npm run build:fixture
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:sdk
npm run test:browser:editor-smoke -- --grep "object storage|connect source flows"
npm run test:browser:public-gcs-live
```

Expected: all local checks pass. The live smoke may report skipped tests when the live table env var is absent.

## Task 3: Browser DataFusion And Release Evidence Gates

**Files:**

- Test: `crates/wasm-datafusion-session`
- Test: `crates/wasm-datafusion-poc`
- Test: `crates/delta-control-plane/tests/browser_snapshot_preflight.rs`
- Test: `tests/conformance/verify_axon_web_datafusion_runtime.sh`
- Read: `docs/program/browser-datafusion-runtime-parity.md`
- Read: `docs/program/browser-lakehouse-release-handoff.md`

**Steps:**

1. Run the focused Rust gates that correspond to the runtime parity review-fix plan.
2. Run the browser DataFusion conformance script.
3. If a gate fails, classify it as:
   - environment/dependency setup;
   - stale test expectation;
   - product/runtime regression;
   - external/live-service blocker.
4. Update docs only if the current docs claim evidence that the checkout does not reproduce.

**Verification:**

```bash
cargo test -p wasm-datafusion-poc
cargo test -p wasm-datafusion-session
cargo test -p delta-control-plane --test browser_snapshot_preflight
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
cargo fmt --check
git diff --check
```

Expected: all focused gates pass, or failing evidence is documented with a concrete next action.

## Task 4: Roadmap Disposition Updates

**Files:**

- Modify if evidence changed: `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Modify if evidence changed: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- Modify if evidence changed: `docs/program/browser-datafusion-runtime-parity.md`
- Modify if evidence changed: `docs/program/browser-lakehouse-release-handoff.md`
- Review root checkout only: `docs/plans/2026-05-26-issue-132-catalog-run-index.md`

**Steps:**

1. Keep public object-storage as the immediate repo-owned release-candidate track if Task 2 passes.
2. Keep signed URL/proxy issuance, TTL signoff, audit/correlation, production CORS, dashboards, oncall, and CI fixture provisioning listed as external blockers unless evidence exists in this repo.
3. Keep default-worker DataFusion listed as a repo-owned next decision until a shipped worker artifact reports browser DataFusion as the default SKU with size evidence.
4. Do not fold issue-132 into the Axon browser-lakehouse roadmap unless the user explicitly wants cross-project Arco planning committed in this repo.

**Verification:**

```bash
git diff -- docs/release-gates docs/program docs/plans
git diff --check
```

Expected: docs either remain unchanged because evidence matches, or changes are narrowly scoped to the evidence found in Tasks 2-3.

## Execution Results

- Worktree: `/Users/ethanurbanski/axon/.worktrees/roadmap-next-steps` on branch `feat/roadmap-next-steps`.
- GitHub triage: `gh issue list --state open` and `gh pr list --state open` both returned `[]`.
- Dependency setup: `npm install` required network approval after sandbox DNS failed, then installed 178 packages with 0 vulnerabilities.
- Generated setup: `npm run build:wasm` completed and produced the ignored `apps/axon-web/src/wasm` bridge; `npm run build:fixture` generated the prod-like fixture needed by editor smoke tests.
- App static gates: `npm run format:check`, `npm run lint`, and `npm exec -- tsc --noEmit` passed after generated WASM bindings existed.
- SDK and public object-storage unit gates: `npm run test:sdk` passed 64 tests.
- Browser editor smoke: the sandboxed run failed before app code because Chromium could not use macOS `MachPortRendezvousServer`; the unsandboxed run then needed an existing Vite server and generated fixture. With `PLAYWRIGHT_BASE_URL=https://127.0.0.1:5174` and Vite running, the focused smoke passed 1 test.
- Live public GCS smoke: `npm run test:browser:public-gcs-live` skipped 2 tests because `AXON_LIVE_PUBLIC_GCS_TABLE_URI` was not set.
- Rust DataFusion gate: `cargo test -p wasm-datafusion-poc` passed.
- Rust session gate: fresh worktree target builds were killed silently during `datafusion-catalog`; rerunning with `CARGO_TARGET_DIR=/Users/ethanurbanski/axon/target` passed 24 tests.
- Delta control-plane preflight: sandboxed run failed because loopback listener bind was denied; unsandboxed run with the shared target cache passed 16 tests.
- Conformance and hygiene: `bash tests/conformance/verify_axon_web_datafusion_runtime.sh`, `cargo fmt --check`, and `git diff --check` passed.

## Roadmap Disposition

- Public object-storage remains the immediate repo-owned release-candidate track. The local, non-live gates reproduce with the setup steps above.
- No release-gate docs needed downgrade: the current documented external blockers remain accurate.
- External launch blockers remain outside repo proof: signed URL/proxy issuance, TTL approval, audit/correlation, production CORS validation, dashboards, oncall, and CI/live fixture provisioning.
- Default-worker DataFusion remains the next repo-owned product decision after public object-storage validation, because the app path is proven but the shipped worker SKU is still separately gated.
- The root checkout has an untracked `docs/plans/2026-05-26-issue-132-catalog-run-index.md`; it is cross-project Arco/Daxis planning and was not folded into this Axon browser-lakehouse roadmap.

## Task 5: Final Handoff

**Steps:**

1. Summarize commands run and their outcomes.
2. List any environment blockers separately from product blockers.
3. List the next executable action after this validation batch.
4. Leave unrelated root-checkout untracked files untouched.

**Verification:**

```bash
git status --short
```

Expected: only intentional changes are present.
