# EPIC-01 Workspace Scaffolding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create the initial monorepo scaffold for EPIC-01, including the Rust workspace, skeleton crates, shared query contract types, CI, and governance files required to start EPIC-02 and EPIC-03 without reopening structure debates.

**Architecture:** Use a Rust workspace as the repo backbone because EPIC-01's package list is crate-oriented and later epics center on Rust runtimes. Keep skeleton crates intentionally thin, move the first real semantics into `query-contract`, and use repo-level docs and CI to make the scaffold operational rather than just present on disk.

**Tech Stack:** Rust workspace, Cargo, GitHub Actions, Markdown docs, shell verification scripts

---

### Task 1: Add a failing workspace-layout verification

**Files:**
- Create: `tests/conformance/verify_workspace_layout.sh`
- Create: `tests/conformance/README.md`

**Step 1: Write the failing test**

Create a shell verification script that checks for:

- root `Cargo.toml`
- `.github/workflows/ci.yml`
- `CODEOWNERS`
- `docs/program/package-owners.md`
- `docs/program/upstream-patch-inventory.md`
- crate directories for `query-contract`, `query-router`, `native-query-runtime`, `delta-control-plane`, `wasm-query-runtime`, `wasm-http-object-store`, `browser-sdk`, `udf-abi`, and `udf-host-wasi`

**Step 2: Run test to verify it fails**

Run: `bash tests/conformance/verify_workspace_layout.sh`
Expected: FAIL because the workspace files and crate directories do not exist yet.

**Step 3: Write minimal implementation**

Add the missing files and directories until the script passes.

**Step 4: Run test to verify it passes**

Run: `bash tests/conformance/verify_workspace_layout.sh`
Expected: PASS with a success message.

### Task 2: Add a failing contract test for capability and fallback semantics

**Files:**
- Create: `Cargo.toml`
- Create: `crates/query-contract/Cargo.toml`
- Create: `crates/query-contract/tests/query_contract.rs`

**Step 1: Write the failing test**

Create tests that expect:

- a serializable `CapabilityKey` enum
- a `CapabilityState` enum with support classifications
- a `FallbackReason` enum
- a `QueryError` structure with code, message, target, and optional fallback reason

**Step 2: Run test to verify it fails**

Run: `cargo test -p query-contract`
Expected: FAIL because `crates/query-contract/src/lib.rs` does not exist yet.

**Step 3: Write minimal implementation**

Create `crates/query-contract/src/lib.rs` implementing the shared types with `serde` derives and lightweight helper methods.

**Step 4: Run test to verify it passes**

Run: `cargo test -p query-contract`
Expected: PASS for the contract tests.

### Task 3: Scaffold the remaining crates and repo ownership docs

**Files:**
- Create: `CODEOWNERS`
- Create: `docs/program/package-owners.md`
- Create: `docs/program/upstream-patch-inventory.md`
- Create: `crates/query-router/Cargo.toml`
- Create: `crates/query-router/src/lib.rs`
- Create: `crates/native-query-runtime/Cargo.toml`
- Create: `crates/native-query-runtime/src/lib.rs`
- Create: `crates/delta-control-plane/Cargo.toml`
- Create: `crates/delta-control-plane/src/lib.rs`
- Create: `crates/wasm-query-runtime/Cargo.toml`
- Create: `crates/wasm-query-runtime/src/lib.rs`
- Create: `crates/wasm-http-object-store/Cargo.toml`
- Create: `crates/wasm-http-object-store/src/lib.rs`
- Create: `crates/browser-sdk/Cargo.toml`
- Create: `crates/browser-sdk/src/lib.rs`
- Create: `crates/udf-abi/Cargo.toml`
- Create: `crates/udf-abi/src/lib.rs`
- Create: `crates/udf-host-wasi/Cargo.toml`
- Create: `crates/udf-host-wasi/src/lib.rs`
- Create: `tests/perf/README.md`
- Create: `tests/security/README.md`

**Step 1: Write the minimal implementation**

Create placeholder libraries that:

- compile cleanly
- document owner and responsibility in module docs
- depend on `query-contract` only where appropriate

**Step 2: Run build verification**

Run: `cargo check --workspace`
Expected: PASS.

### Task 4: Add CI and repo hygiene files

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.gitignore`

**Step 1: Write the minimal implementation**

Add CI that:

- checks formatting
- runs `cargo check --workspace`
- runs `cargo test -p query-contract`
- runs wasm checks for `wasm-query-runtime`, `wasm-http-object-store`, and `browser-sdk` on `wasm32-unknown-unknown`
- runs `bash tests/conformance/verify_workspace_layout.sh`

Add `.gitignore` entries for `target/`.

**Step 2: Run verification**

Run: `cargo fmt --all --check`
Expected: PASS.

Run: `cargo check --workspace`
Expected: PASS.

Run: `cargo test -p query-contract`
Expected: PASS.

Run: `cargo check -p wasm-query-runtime -p wasm-http-object-store -p browser-sdk --target wasm32-unknown-unknown`
Expected: PASS.

Run: `bash tests/conformance/verify_workspace_layout.sh`
Expected: PASS.

### Task 5: Reconcile docs and top-level navigation

**Files:**
- Modify: `README.md`
- Modify: `docs/epics/EPIC-01-foundation-repo-scaffolding-and-architectural-freeze.md`

**Step 1: Write the minimal implementation**

Update the docs so the repo root points to the new scaffold and EPIC-01 clearly links to the implemented files.

**Step 2: Run verification**

Run: `git status --short`
Expected: only intended scaffold files are changed or added.
