# Upstream WASM Canonicalization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan
> task-by-task.

**Goal:** Convert the proven Daxis upstream-WASM POC into current-head, dependency-ordered,
reviewable canonical submission branches without changing native defaults, Axon's shipping
dependencies, or any canonical repository.

**Architecture:** Refresh leaf codec and browser-HTTP contracts first, then use those accepted
interfaces to refresh DataFusion. Treat Delta Kernel as an explicit design gate and keep delta-rs
blocked until Kernel has a canonical adapter/capability seam. All source work occurs on additive,
DCO-signed branches in isolated worktrees; composite pins live only in an excluded evidence
workspace.

**Tech Stack:** Rust, Cargo resolver v2/v3, `wasm32-unknown-unknown`, wasm-bindgen/wasm-pack,
Chrome, Firefox, Fetch, HTTP Range and validators, Arrow IPC, Parquet, `object_store`, DataFusion,
Delta Kernel, delta-rs, GitHub Actions, and GitHub Daxis forks.

---

- Status: Leaf execution complete; DataFusion and Kernel stop gates reached
- Date: 2026-07-26
- Research matrix:
  [`docs/research/upstream-wasm-canonicalization/README.md`](../research/upstream-wasm-canonicalization/README.md)
- Publication boundary: Daxis fork branches, the Axon documentation branch, and Axon issue #2 are
  authorized after proof. Canonical branches and canonical PRs are not authorized.

## Fresh Canonical Heads

| Repository | Base |
| --- | --- |
| Arrow | `87cd2e526511ce75726bceb59033dfe4078a095d` |
| `object_store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| DataFusion | `88365ddd62b17c1eabd20ed0b064f626f9e77686` |
| Delta Kernel | `2403501198e9b132b714c9945fb3175c0364b1dd` |
| delta-rs | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

Re-run `git fetch --prune upstream` and record `upstream/main` immediately before creating each
branch. If a head moved, update the research matrix and re-run the applicable comparison before
coding.

## Disposition And Execution Boundary

| Concern | Classification | This phase |
| --- | --- | --- |
| Arrow Parquet | bounded semantic adaptation | Execute reduced transplant. |
| Arrow IPC | clean current-head transplant | Execute. |
| `object_store` manifest/runtime | bounded semantic adaptation | Execute current-head seam and proof. |
| `object_store` retry | blocked by another concern | Execute only in the validator-correct branch. |
| `object_store` range | bounded semantic adaptation | Execute correctness; remove/defer public fallback. |
| DataFusion feature ownership | bounded semantic adaptation | Execute only after all leaf branches pass. |
| DataFusion runtime | bounded semantic adaptation | Execute only after feature ownership passes. |
| Kernel | redesign required | Write design decision; no source implementation. |
| delta-rs | blocked by another concern | Preserve evidence; no source implementation. |

## Branch And Worktree Layout

Use:

```text
.worktrees/upstream-wasm-canonicalization/
  axon/
  arrow-parquet/
  arrow-ipc/
  object-store-http/
  object-store-protocol/
  datafusion-features/
  datafusion-runtime/
  integration/
```

Replacement branches:

```text
upstream/2026-07-26/wasm32-parquet-zstd
upstream/2026-07-26/wasm32-arrow-ipc-zstd
upstream/2026-07-26/wasm32-http-manifest
upstream/2026-07-26/wasm32-browser-range-retry
upstream/2026-07-26/wasm32-feature-ownership
upstream/2026-07-26/wasm32-browser-runtime
```

Never rewrite the historical POC, candidate, stack, forward, or tag refs.

## Common Red-Green And Commit Rules

For each executable branch:

1. Prove the worktree is at the freshly fetched canonical base and clean.
2. Capture the current failure with a focused build, dependency policy, or real
   producer/consumer test.
3. Confirm the failure is caused by the missing concern, not toolchain setup.
4. Apply only the minimum historical commits or write the minimum adaptation.
5. Run the same proof green, then relevant native/default tests.
6. Run `cargo fmt --check`, repository-prescribed Clippy/tests, `git diff --check`, and candidate
   hygiene.
7. Commit each logical slice with `git commit -s`; use additive corrections only.
8. Record commands, exit codes, locks/graphs, and SHAs in the evidence ledger.

Candidate hygiene:

```bash
git diff --check upstream/main...HEAD
git grep -n -E 'daxis-io|d1a31ec|f24c67c|1d6cb49|693aa0b|c9a475f|0611f31' \
  upstream/main...HEAD -- . ':!*.md'
git diff --name-only upstream/main...HEAD | \
  rg '(^|/)Cargo\\.toml$|(^|/)Cargo\\.lock$|\\.github/workflows|src/|tests/|ci/|dev/'
```

The first command must pass. The second must have no candidate-source match. Review every file from
the third command; it is an inventory, not a pass/fail assertion.

## Task 1: Arrow Parquet Reduced Current-Head Adaptation

**Repository:** `daxis-io/arrow-rs`

**Branch/base:** `upstream/2026-07-26/wasm32-parquet-zstd` from
`87cd2e526511ce75726bceb59033dfe4078a095d`

**Files:**

- Modify: `parquet/Cargo.toml`
- Modify: `parquet/src/compression.rs`
- Create: `parquet/tests/wasm_codec_availability.rs`
- Create: `parquet/tests/data/wasm_zstd.parquet`
- Modify: `.github/workflows/parquet.yml`
- Create: `dev/check_wasm_dependency_policy.sh`

**Step 1: Capture red**

```bash
cargo check -p parquet --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 \
  --locked
```

Expected: fail in `zstd-sys` or the graph policy reports `zstd-sys`.

**Step 2: Apply the reduced historical series**

```bash
git cherry-pick f68a32a05d2a57f7c2422d3c25a56d6055ecf8e3
git cherry-pick 5793dfad6795be215b01ab97c3c2046ff1c617b9
git cherry-pick d9d6bb626981299425671c5bd8d0b15ab67c1db6
```

Do not cherry-pick `b23db2e51c8df4257fa1549ae427be3def1e7641`; current upstream supports
the object-store writer. If current context requires edits, amend nothing after publication: make a
new signed correction commit.

**Step 3: Prove real behavior and graph**

```bash
cargo check -p parquet --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 \
  --locked
cargo check -p parquet --tests --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 \
  --locked
bash dev/check_wasm_dependency_policy.sh parquet
cargo test -p parquet --test wasm_codec_availability --features zstd --locked
cargo test -p parquet --locked
```

Add an executable WASM runner for the metadata-then-page-decode test if the historical workflow only
compiles it. Watch the test fail without the production patch, restore the patch, and watch it pass.

**Step 4: Repository gates and commit correction if needed**

```bash
cargo fmt --all -- --check
git diff --check upstream/main...HEAD
```

Run the exact Parquet workflow-prescribed Clippy/tests. Record any toolchain mismatch rather than
weakening the checks.

## Task 2: Arrow IPC Clean Current-Head Transplant

**Repository:** `daxis-io/arrow-rs`

**Branch/base:** `upstream/2026-07-26/wasm32-arrow-ipc-zstd` independently based on canonical
`87cd2e526511ce75726bceb59033dfe4078a095d`. Operational ordering follows concern 1, but the source
diff does not depend on it.

**Files:**

- Modify: `arrow-ipc/Cargo.toml`
- Modify: `arrow-ipc/src/compression.rs`
- Modify: `arrow-ipc/src/writer.rs`
- Create: `arrow-ipc/tests/wasm_codec_availability.rs`
- Create: `arrow-ipc/tests/data/wasm_zstd.arrow`
- Modify: `.github/workflows/arrow.yml`
- Modify: `dev/check_wasm_dependency_policy.sh`

**Step 1: Capture red on canonical main**

```bash
cargo check -p arrow-ipc --target wasm32-unknown-unknown \
  --no-default-features --features lz4,zstd --locked
```

Expected: fail through `zstd-sys`.

**Step 2: Transplant**

```bash
git cherry-pick 7eca801309b97ea379e633fad955b001dd534951
```

**Step 3: Prove graph and behavior**

```bash
cargo check -p arrow-ipc --target wasm32-unknown-unknown \
  --no-default-features --features lz4,zstd --locked
cargo check -p arrow-ipc --tests --target wasm32-unknown-unknown \
  --no-default-features --features lz4,zstd --locked
bash dev/check_wasm_dependency_policy.sh arrow-ipc
cargo test -p arrow-ipc --test wasm_codec_availability --features zstd --locked
cargo test -p arrow-ipc --all-features --locked
cargo fmt --all -- --check
git diff --check upstream/main...HEAD
```

Execute, rather than only compile, the real WASM schema/decode and writer-rollback tests.

## Task 3: `object_store` Explicit HTTP/Runtime Seam

**Repository:** `daxis-io/arrow-rs-object-store`

**Branch/base:** `upstream/2026-07-26/wasm32-http-manifest` from
`84d24eb8efcec9448566de09e94d2d4b74b21ebe`

**Starting evidence:** historical current-head branch
`4a89dd1fc831d19570de7e84b85daefda94017d6`

**Files:**

- Modify: `Cargo.toml`
- Modify: target-selected client/connector/crypto/runtime source
- Create: `tests/wasm-consumer/`
- Modify: `.github/workflows/ci.yml`
- Create/modify: graph-policy scripts and browser tests

**Step 1: Capture red**

Run the exact three consumer profiles against canonical main:

```bash
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http-base --locked
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http-base,reqwest,web --locked
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http --locked
```

When the fixture does not yet exist on main, apply only the test/fixture commit first, run it red,
then apply the implementation commits.

**Step 2: Recreate the current-head branch additively**

Create the replacement at canonical main and cherry-pick the exact 15 commits from
`upstream/wasm32-http-manifest` in order. Do not merge or reset the historical branch. Review public
feature composition separately from compiler-free CI.

**Step 3: Add real browser runtime proof**

The test must construct `HttpStore` through the built-in reqwest connector, receive one transient
response, use a nonzero browser timer, and complete a second Fetch. Run in both browsers:

```bash
cargo generate-lockfile --manifest-path tests/wasm-consumer/Cargo.toml
wasm-pack test --headless --chrome --firefox tests/wasm-consumer \
  --locked --test http --no-default-features --features http-base,reqwest,web
```

**Step 4: Verify**

```bash
tests/wasm-consumer/check-wasm-graph.sh
cargo test --locked
cargo clippy --all-features --all-targets --locked -- -D warnings
cargo doc --all-features --no-deps
cargo fmt --all -- --check
git diff --check upstream/main...HEAD
```

Run Apache RAT/audit and every ordinary fork workflow required by current contribution docs.

## Task 4: `object_store` Validator-First Range Retry

**Repository:** `daxis-io/arrow-rs-object-store`

**Branch/base:** `upstream/2026-07-26/wasm32-browser-range-retry` based on Task 3.

**Historical source:** `9b5ffc710d5c7fb38068e8a16dbe29446593a84b`.

**Files:**

- Modify: `src/client/get.rs`
- Modify: request header/extensions code
- Modify: focused client and native HTTP tests
- Modify: browser protocol tests/workflow

**Step 1: Capture validator red**

Add/run tests proving that missing, weak, malformed, or list-shaped ETags do not admit a second
request after truncation. Prove a strong ETag sends `If-Range`; a changed/missing validator and any
retry `200` are rejected.

```bash
cargo test --locked client::get::tests::truncated_body_is_not_retried_without_a_valid_strong_etag
cargo test --locked client::get::http_tests::test_retry_validates_content_range_and_sends_if_range
```

Expected on canonical/concern-3 base: at least one focused behavior is absent or wrong.

**Step 2: Apply historical correctness additively**

Cherry-pick the concern-4/5 commits from the historical current-head branch in original order.
Then add a signed adaptation commit that removes:

- the public arbitrary-subrange `200` fallback builder option;
- fallback buffering and tests;
- any documentation presenting that policy as settled.

Retain:

- clean-EOF/overrun detection;
- strong entity-tag grammar;
- `If-Range`;
- strict `206`, `Content-Range`, length, and identity-encoding checks;
- exact/enclosing range trimming;
- typed errors and producer/consumer tests.

**Step 3: Focused proof**

```bash
cargo test --locked client::get::tests::strong_etag_validation_follows_entity_tag_grammar
cargo test --locked client::get::tests::retries_a_clean_eof_before_the_declared_body_length
cargo test --locked client::get::tests::truncated_body_is_not_retried_without_a_valid_strong_etag
cargo test --locked client::get::http_tests::test_stream_retry
cargo test --locked client::get::http_tests::test_retry_validates_content_range_and_sends_if_range
cargo test --locked client::get::http_tests::test_retry_rejects_changed_if_range_validator
cargo test --locked client::get::http_tests::test_range_response_requires_identity_encoding
```

Run the same protocol producer in Chrome and Firefox. Prove arbitrary subrange `200` remains strict
and that an `If-Range` retry can never enter fallback.

**Step 4: Full gates**

Run the Task 3 native, graph, format, Clippy, docs, RAT, audit, and workflow set on the stacked head.
Prepare a separate issue-#806 note for exact-full-range `200` and optional bounded fallback; do not
include that policy in this branch.

## Task 5: Refresh DataFusion Feature Ownership

**Dependency:** Tasks 1-4 verified and their final SHAs recorded.

**Repository:** `daxis-io/datafusion`

**Branch/base:** `upstream/2026-07-26/wasm32-feature-ownership` from
`88365ddd62b17c1eabd20ed0b064f626f9e77686`

**Files:**

- Modify: `datafusion/common/Cargo.toml`
- Modify: `datafusion/core/Cargo.toml`
- Modify: `datafusion/datasource/Cargo.toml`
- Modify: `datafusion/datasource/src/file_compression_type.rs`
- Create/modify: `ci/scripts/check_wasm_dependency_policy.sh`
- Modify: WASM tests/workflow as required by current `AGENTS.md`

**Step 1: Capture red**

Use a temporary, excluded integration manifest to pin the final leaf branches. Run:

```bash
cargo tree -p datafusion --target wasm32-unknown-unknown \
  -e normal,build,features --locked
cargo check -p datafusion-wasmtest --target wasm32-unknown-unknown --locked
```

Expected current-base evidence: native codec/filesystem dependencies or a native compiler are
required.

**Step 2: Adapt the historical feature-ownership series**

Apply the logical changes from `8d24ccea5` through `2ae13de39`, reconciling current manifests and
current DataFusion APIs. Do not carry historical Daxis pins or obsolete workflow workarounds.

**Step 3: Prove real behavior**

Run the in-memory Parquet query and the distinct feature-disabled versus target-backend-unavailable
file compression tests. Prove one Arrow, Parquet, and `object_store` universe.

**Step 4: Current repository gates**

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
RUST_BACKTRACE=1 cargo test --profile ci \
  --exclude datafusion-examples --exclude datafusion-benchmarks --exclude datafusion-cli \
  --workspace --lib --tests --bins \
  --features avro,json,backtrace,extended_tests,recursive_protection,parquet_encryption
./ci/scripts/doc_prettier_check.sh --write --allow-dirty
```

Run the dependency policy, exact WASM check, and `git diff --check`. Any inherited failure must be
reproduced on exact canonical base before classification.

## Task 6: Refresh DataFusion Browser Runtime

**Dependency:** Task 5 verified.

**Repository:** `daxis-io/datafusion`

**Branch/base:** `upstream/2026-07-26/wasm32-browser-runtime` based on Task 5.

**Files:**

- Modify: `datafusion/execution/Cargo.toml`
- Modify: `datafusion/execution/src/disk_manager.rs`
- Modify: `datafusion/wasmtest/Cargo.toml`
- Modify: `datafusion/wasmtest/src/lib.rs`
- Modify/create: browser server and tests
- Modify: `.github/workflows/rust.yml`
- Create/modify: WASM user documentation

**Step 1: Capture red**

Run the current workflow command without Clang and without global entropy `RUSTFLAGS`. Add the
browser filesystem-spill rejection and HTTP Parquet query tests first; watch the applicable test or
build fail.

**Step 2: Minimal adaptation**

Target-gate filesystem dependencies and DiskManager implementation while preserving native defaults
and custom factories. Configure one partition and disabled disk only in the documented browser
profile. Select `getrandom` through dependency features, never process-global flags.

**Step 3: Browser proof**

Run Chrome and Firefox against a second-origin HTTP Parquet fixture. Require projection, filtering,
ordering, aggregation, request/byte metrics, and typed spill/codec errors.

**Step 4: Full DataFusion gates**

Repeat Task 5's current `AGENTS.md` format, Clippy, extended test, docs, graph, and hygiene gates.

## Task 7: Kernel Design Decision And delta-rs Block Record

**Files:**

- Create: `docs/research/upstream-wasm-canonicalization/kernel-design-decision.md`
- Update: research matrix, evidence ledger, and patch inventory
- Do not modify Kernel or delta-rs source

Document:

1. Separate Arrow adapter versus target-cfg surgery versus relaxed denied graph.
2. Recommended separate adapter plus explicit runtime capabilities.
3. Synchronous prefetched handler flow and typed cache misses.
4. Native-default and compatibility invariants.
5. Small prerequisite PR sequence if Kernel maintainers accept the direction.
6. Questions for Kernel issue #252 and delta-rs ownership.

Record concern 9 as blocked by concern 8 and the failed exact forward CI
`30182193705`. Preserve `e0fa37143e6888c06623c6a43adf1c801a189ca0` unchanged.

## Task 8: Composite Verification

Create an excluded or temporary integration workspace outside Axon's shipping Rust workspace. Pin
only the verified Daxis branch SHAs.

Prove:

- one Arrow, Parquet, `object_store`, DataFusion, and Kernel source universe;
- no `zstd-sys`, `liblzma-sys`, `aws-lc-sys`, `openssl-sys`, `native-tls`, `ring`, `hyper`,
  `walkdir`, `tempfile`, filesystem implementation, cloud battery, or Tokio multithread feature;
- no global `RUSTFLAGS`;
- compiler-free exact-WASM build;
- Chrome and Firefox proof to the highest viable layer;
- native defaults in every changed repository.

If Kernel remains blocked, the refreshed browser stack stops at DataFusion HTTP Parquet. Do not
claim refreshed Delta snapshot/browser parity from a leaf-only stack.

Store bulky evidence beneath:

```text
target/upstream-wasm-canonicalization-evidence/<evidence-lock-hash>/
```

Hash the integration lock, graph report, browser JSON, toolchain files, and any downloaded CI
artifacts.

## Task 9: Independent Review And Corrections

After implementation, request an independent owner-level code review over:

- each canonical-base-to-branch diff;
- this plan and the research matrix;
- candidate hygiene;
- native-default and exact-WASM evidence;
- object-store validator/fallback correctness;
- Kernel stop decision;
- fork-removal conditions.

Resolve every confirmed Critical or Important finding with additive signed commits. Rerun the exact
focused failure proof and aggregate gates affected by each correction.

## Task 10: Evidence, Daxis Publication, And Handoff

**Files:**

- Create: `docs/release-gates/upstream-wasm-canonicalization-evidence.md`
- Update: `docs/program/upstream-patch-inventory.md`

After local gates and review:

1. Push only verified replacement branches to the corresponding Daxis fork.
2. Verify each remote ref equals the tested local SHA.
3. Monitor every exact-head Daxis CI workflow to a terminal conclusion.
4. Add only additive corrections; never force-push.
5. Commit the Axon research/plan/evidence/inventory with DCO sign-off.
6. Push `chore/upstream-wasm-canonicalization` to `daxis-io/axon`, not `origin/main`.
7. Post one concise progress comment to Axon issue #2 with exact branches, SHAs, CI, evidence, and
   the Kernel/delta-rs decision.
8. Prepare canonical PR titles, bodies, tests, dependency order, and reviewer notes without opening
   canonical PRs.

## Stop Conditions

Stop the affected slice if any required condition from the user prompt is met, especially:

- native defaults must change;
- a denied exact-WASM dependency remains;
- a browser build requires Clang/C/C++/CMake, global entropy flags, filesystem, spill, or threads;
- object-store retry can splice representations or fallback is unbounded;
- clean candidate source needs Daxis pins;
- Kernel needs broad native-core surgery;
- disk pressure prevents isolated proof.

Ordinary build/test/lint/browser failures enter systematic debugging; they are not stop conditions.

## Fork Removal Conditions

| Fork patch | Removal gate |
| --- | --- |
| Arrow Parquet | Released target-selected backend contract plus Axon locked browser rehearsal. |
| Arrow IPC | Released target-selected IPC backend contract plus Axon locked browser rehearsal. |
| `object_store` HTTP/runtime | Released explicit browser HTTP/runtime profile plus graph/browser rehearsal. |
| `object_store` retry/range | Released strong-validator Range/retry contract; fallback tracked separately. |
| DataFusion feature/runtime | Released compiler-free graph and documented browser runtime profile. |
| Delta Kernel | Accepted/released adapter/capability design plus prefetched replay proof. |
| delta-rs | Released Kernel/leaf contracts and rebased browser crate without internal/fork-only seams. |

## Publication Boundary

This plan authorizes Daxis-owned branch pushes, Daxis CI, the Axon documentation branch, and one Axon
issue #2 progress update only after verification and independent review. It does not authorize:

- any push to a canonical organization;
- any canonical PR;
- merge/close of POC drafts;
- fork default-branch changes;
- tag movement;
- direct Axon `origin/main` push;
- Axon production dependency replacement.
