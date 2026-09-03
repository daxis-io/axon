# Local Delta Snapshot History Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Show available Delta commit history and the resolved active-file count for user-selected local Delta tables.

**Architecture:** Extract commit parsing into a pure shared service, retain typed history in the local runtime cache, and publish it through query runtime state without exposing browser file handles. Feed the Snapshot UI from resolved catalog metadata rather than execution-plan details.

**Tech Stack:** TypeScript, React, TanStack Query, Vitest, Playwright, Rust-generated WASM snapshot resolver.

---

### Task 1: Lock the browser-visible regression

**Files:**

- Modify: `apps/axon-web/tests/editor-smoke.spec.ts`

**Step 1: Write the failing test**

Extend the existing local Delta browser smoke after the successful query. Open the Snapshot tab and assert that Active files is `2`, that four `.commit` rows are rendered, and that the current row is `v3` with `+2 / -3`.

**Step 2: Run test to verify it fails**

Run: `npm run test:browser:editor-smoke -- --grep "connects a local Delta folder from the root editor"`

Expected: FAIL because Active files is an em dash and no local commit rows exist.

### Task 2: Share commit parsing and retain local history

**Files:**

- Create: `apps/axon-web/src/services/delta-commit-history.ts`
- Create: `apps/axon-web/src/services/delta-commit-history.test.ts`
- Modify: `apps/axon-web/src/services/snapshot.ts`
- Modify: `apps/axon-web/src/services/local-delta.ts`
- Modify: `apps/axon-web/src/services/local-delta.test.ts`

**Step 1: Write failing unit tests**

Add tests for newest-first rollup, `commitInfo`, add/remove counts, inferred operations, and filtering versions above a requested snapshot. Add a local-runtime assertion that the selected fixture logs produce commit entries.

**Step 2: Run tests to verify they fail**

Run: `npm test -- src/services/delta-commit-history.test.ts src/services/local-delta.test.ts`

Expected: FAIL because the shared parser and `LocalDeltaRuntime.commits` do not exist.

**Step 3: Implement the minimal shared parser**

Move the parser and rollup helpers from `snapshot.ts` into `delta-commit-history.ts`. Change manifest loading to fetch text records and pass them to the pure rollup. During `readLocalLogFacts`, retain the already-read commit text; after WASM resolves the snapshot, build and store typed entries on `LocalDeltaRuntime`. Add an exact registry/snapshot cache accessor.

**Step 4: Run focused tests**

Run: `npm test -- src/services/delta-commit-history.test.ts src/services/local-delta.test.ts`

Expected: PASS.

### Task 3: Publish local commit entries to the query adapter

**Files:**

- Modify: `apps/axon-web/src/services/query-runtime-state.ts`
- Modify: `apps/axon-web/src/services/query.ts`
- Modify: `apps/axon-web/src/services/snapshot.ts`
- Create: `apps/axon-web/src/services/snapshot.test.ts`

**Step 1: Write a failing adapter test**

Publish runtime state for a local source with one typed commit and assert that the real `loadCommits` adapter returns it. Preserve manifest-backed loading through the shared rollup.

**Step 2: Run test to verify it fails**

Run: `npm test -- src/services/snapshot.test.ts`

Expected: FAIL because local sources still return an empty list.

**Step 3: Implement runtime publication**

Add optional `commits` to session/runtime state. For an exact local resolved descriptor, retrieve the cached runtime entries and publish them. Make `loadCommits` return published or cached local entries while leaving object-store history unsupported and the manifest path unchanged.

**Step 4: Run focused tests**

Run: `npm test -- src/services/snapshot.test.ts src/services/local-delta.test.ts`

Expected: PASS.

### Task 4: Correct Snapshot presentation

**Files:**

- Modify: `apps/axon-web/src/editor/App.tsx`
- Modify: `apps/axon-web/src/editor/components/RunResultsPanel.tsx`
- Modify: `apps/axon-web/src/editor/components/Results.tsx`

**Step 1: Implement the tested wiring**

Pass `tableMeta.file_count` as `tableFileCount` through the results panel and render it in the Snapshot KPI. Replace the phase-specific empty-history sentence with neutral availability copy.

**Step 2: Run the original browser regression**

Run: `npm run test:browser:editor-smoke -- --grep "connects a local Delta folder from the root editor"`

Expected: PASS with two active files and four commit rows.

### Task 5: Full verification

**Files:**

- Verify all modified files.

**Step 1: Format and inspect**

Run: `npm run format:check`

Run: `git diff --check`

**Step 2: Run complete unit and static checks**

Run: `npm test`

Run: `npm run lint`

Run: `npm run build`

**Step 3: Re-run the focused browser proof**

Run: `npm run test:browser:editor-smoke -- --grep "connects a local Delta folder from the root editor"`

Expected: all commands exit zero.

**Step 4: Review the final diff**

Confirm no browser handles, blob URLs, credentials, or commit-log bytes enter persistent catalog metadata. Do not commit, push, open a PR, deploy, or clean up the worktree without separate authorization.
