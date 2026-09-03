# Bundled Delta Sample Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ship a small real Delta table inside axon-web so fresh checkouts and Vercel artifacts always have a queryable default sample.

**Architecture:** Commit the existing prod-like Delta table and its browser-qualification page-index companion as static assets, and make fixture generation an explicit maintainer action. Add one Node verifier used by local scripts and the deployment artifact guard to enforce inventory integrity, a 128 KiB Delta-table budget, and a 1 MiB combined package budget.

**Tech Stack:** Node.js ESM, Vitest, Bash, Vite static assets, Delta Lake JSON/checkpoint/Parquet files.

---

### Task 1: Specify the bundled fixture contract

**Files:**

- Create: `apps/axon-web/src/services/bundled-delta-fixture.test.ts`
- Create: `apps/axon-web/scripts/verify-bundled-delta-fixture.mjs`

1. Write a Vitest test that invokes the verifier against `public`, expects a real Delta log/checkpoint/Parquet inventory and the page-index qualification artifact, checks manifest sizes and the page-index checksum, and enforces a 128 KiB Delta-table budget plus a 1 MiB combined budget.
2. Run `npm test -- scripts/verify-bundled-delta-fixture.test.ts` and confirm it fails because `public/fixtures/prod-like` is absent in the clean worktree.
3. Implement the minimal reusable verifier and CLI error reporting.
4. Keep the test red until the table assets are added in Task 2.

### Task 2: Package the real table and stop implicit generation

**Files:**

- Modify: `.gitignore`
- Modify: `apps/axon-web/package.json`
- Add: `apps/axon-web/public/fixtures/prod-like/**`

1. Remove the prod-like ignore rule and copy the generator-produced manifest, Delta log, checkpoint, `_last_checkpoint`, and Parquet files into the worktree.
2. Change `build:fixture` to run the integrity verifier and add `regenerate:fixture` for the existing Rust generator.
3. Run the focused Vitest test and `npm run build:fixture`; both must pass.
4. Confirm `git ls-files --others --exclude-standard apps/axon-web/public/fixtures/prod-like` lists the new assets before staging.

### Task 3: Guard Vercel output and document the contract

**Files:**

- Modify: `apps/axon-web/scripts/verify-build-output.sh`
- Modify: `apps/axon-web/README.md`

1. Add a regression test that constructs an otherwise-valid fake build without the Delta fixture and expects `verify-build-output.sh` to fail for the missing sample.
2. Run it and confirm the current guard incorrectly accepts the fake build.
3. Call the Node fixture verifier from `verify-build-output.sh`, then rerun the shell test to green.
4. Update the README to describe the checked-in table, verification, regeneration, and package budget.

### Task 4: Verify browser and build behavior

1. Run `npm test`.
2. Run `npm run build` and `bash scripts/verify-build-output.sh dist`.
3. Run the focused Chromium sample query smoke test from the built app.
4. Inspect `dist/fixtures/prod-like`, its aggregate size, and Git status; report any environment-only browser or Rust build limitation separately.
