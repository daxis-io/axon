# Browser Delta Prod-Like Fixture Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a generated prod-like Delta fixture and browser UI that shows Delta log/checkpoint/data-file inputs mapped to the resolved snapshot output.

**Architecture:** The browser sandbox will generate a local Delta table fixture before dev/build/test using a Rust helper binary gated behind a Cargo feature. The app will fetch the generated manifest, pass only Delta log objects into the WASM resolver, and render manifest objects, commit actions, data files, and active-file output side by side.

**Tech Stack:** Rust, delta-rs, wasm-bindgen, Vite, TypeScript, Playwright.

---

### Task 1: Browser E2E Spec

**Files:**
- Modify: `examples/browser-delta-sandbox/tests/browser-delta-sandbox.spec.ts`

**Steps:**
1. Add a Playwright test that clicks `Resolve Prod-like Snapshot`.
2. Assert the UI shows snapshot version `3`, a checkpoint parquet file, `_last_checkpoint`, JSON commit actions, partitioned data files, inactive files, and active `category=B` / `category=D` outputs.
3. Run `npm run test:e2e` from `examples/browser-delta-sandbox`.
4. Expected red: the button/selectors do not exist yet.

### Task 2: Fixture Generator

**Files:**
- Modify: `examples/browser-delta-sandbox/Cargo.toml`
- Create: `examples/browser-delta-sandbox/src/bin/generate_prod_fixture.rs`
- Modify: `.gitignore`
- Modify: `examples/browser-delta-sandbox/package.json`

**Steps:**
1. Add a `fixture-generator` Cargo feature with optional host-only dependencies.
2. Generate `public/fixtures/prod-like/table` with delta-rs: create partitioned table, append multiple commits, create checkpoint at version `2`, then overwrite at version `3`.
3. Write `public/fixtures/prod-like/delta-log-manifest.json` with log object URLs/sizes and physical data-file inventory.
4. Add `build:fixture`, wire it into `dev`, `build`, and `test:e2e`.
5. Run the generator and inspect the output.

### Task 3: Detailed UI

**Files:**
- Modify: `examples/browser-delta-sandbox/index.html`
- Modify: `examples/browser-delta-sandbox/src/main.ts`
- Modify: `examples/browser-delta-sandbox/src/styles.css`
- Modify: `examples/browser-delta-sandbox/README.md`

**Steps:**
1. Load the prod-like manifest from `/fixtures/prod-like/delta-log-manifest.json`.
2. Fetch JSON commit files for display and classify commit/checkpoint/last-checkpoint objects.
3. Pass only sanitized log objects to the WASM resolver.
4. Render summary, log object inventory, parsed JSON actions, data-file inventory, active output, and input-to-output mapping.
5. Keep the UI dense and inspection-oriented.

### Task 4: Verification

**Commands:**
- `npm run test:e2e`
- `npm run build`
- `cargo check -p browser-delta-sandbox --features fixture-generator --bin generate-prod-fixture --locked`
- `cargo fmt --all -- --check`
- `cargo check --workspace --locked`
- `npm audit --audit-level=high`
- `git diff --check`

**Expected:** All commands pass and the worktree contains only scoped source/lockfile changes plus ignored generated fixtures.
