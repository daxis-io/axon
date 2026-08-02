# Public S3 Page-Index Byte-Savings A/B Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prove, in a real browser-WASM A/B over identical deterministic local Parquet bytes, whether loading page indexes and applying a predicate-derived page selection reduces net physical bytes without changing any production default.

**Architecture:** Keep the normal scan path on `PageIndexPolicy::Skip`. A feature-gated experiment build carries a private `Skip` or `Predicate` policy through the per-query DataFusion session extension. The Parquet engine loads indexes only for `Predicate`, converts the existing integer pruning predicate to a fail-open `RowSelection`, and leaves DataFusion's residual filter in place. A focused Playwright test starts a fresh worker for each arm, records actual HTTP byte ranges, classifies them with fixture-recorded footer/index/page extents, enforces parity and runtime gates, and writes a redacted ignored artifact.

**Tech Stack:** Rust 2024, Apache Arrow/Parquet 58.3.0, DataFusion 53.1.0, `wasm-bindgen`, TypeScript 5.9, Playwright 1.57, Chromium, Vite.

---

## Provenance and ancestry gate

- Fetch command: `git fetch origin main`
- Handoff SHA: `59dcc8b07e1ed96d29b160e78c55c4aca4d832bb`
- Fetched base: `d1a31ec22479bb7d2fb380bfd61e00fd2f7881e8`
- Ancestry proof: `git merge-base --is-ancestor 59dcc8b07e1ed96d29b160e78c55c4aca4d832bb origin/main` exited zero.
- Worktree: `.worktrees/public-s3-page-index-ab`
- Branch: `perf/public-s3-page-index-ab`
- Root-checkout changes were observed but not touched.

## Read-only research findings

### Current Axon scan path

- `crates/wasm-parquet-engine/src/lib.rs` imports `ArrowReaderOptions`, `PageIndexPolicy`, and `ParquetRecordBatchStreamBuilder`. `HttpRangeAsyncFileReader::load_metadata` preserves independently supplied column/offset-index policies, but `stream_scan_target_batches_with_row_group_pruning_caches_and_query` constructs `ParquetRecordBatchStreamBuilder::new(async_reader)`. The default options skip both indexes.
- The same stream function calls `plan_parquet_row_groups` and supplies only `.with_row_groups(...)`, `.with_projection(...)`, and `.with_batch_size(...)`. It never supplies `.with_row_selection(...)`.
- `crates/wasm-datafusion-poc/src/lib.rs` extracts at most one supported integer comparison from DataFusion filters and carries it as `ParquetRowGroupPruningPredicate`; it remains an inexact filter, so DataFusion retains residual filtering.
- `crates/wasm-query-runtime/src/lib.rs` has the analogous row-group-only integer predicate path for the legacy runtime.
- `crates/wasm-parquet-engine/tests/metadata.rs` creates its indexed inspection fixture with `EnabledStatistics::Page`; `crates/wasm-parquet-engine/tests/streaming.rs` currently proves only row-group pruning and full streamed decoding.
- Conclusion: current main can preserve requested page-index loading policies, but its active runtime neither requests index loading nor converts predicates into page selections. “Index loaded” is not “page pruned.”

### Exact Parquet 58.3 APIs used locally

The locked crate is `parquet = 58.3.0` in `Cargo.lock`. The primary source inspected under the local Cargo registry is:

- `parquet-58.3.0/src/arrow/arrow_reader/mod.rs`
  - `ArrowReaderOptions::with_column_index_policy` and `with_offset_index_policy`
  - `ArrowReaderOptions::with_page_index_policy`
  - builder `.with_row_selection(RowSelection)`
- `parquet-58.3.0/src/file/metadata/reader.rs`
  - `PageIndexPolicy::{Skip, Optional, Required}`
  - `ParquetMetaDataReader::load_page_index`
- `parquet-58.3.0/src/arrow/arrow_reader/selection.rs`
  - `RowSelection::from_consecutive_ranges`
  - `RowSelection::scan_ranges`
- `parquet-58.3.0/src/arrow/async_reader/mod.rs`
  - `ParquetRecordBatchStreamBuilder::new_with_options`
  - upstream tests explicitly combine required page indexes with `RowSelection` to omit page fetches
- `parquet-58.3.0/src/file/page_index/column_index.rs`
  - `ColumnIndexMetaData`, typed min/max iterators, and page counts
- `parquet-58.3.0/src/file/page_index/offset_index.rs`
  - page offsets, compressed sizes, and first-row indexes

These sources correspond to Apache Arrow Rust tag `58.3.0`; no third-party API description is used.

### Immutable public fixture capability

- Generator: `apps/axon-web/src/bin/generate_s3_perf_fixture.rs` uses `EnabledStatistics::Chunk`, four row groups per file, and page row/size limits.
- All eight active objects listed by `apps/axon-web/public/fixtures/s3-perf/s3-perf-fixture-manifest.json` were anonymously downloaded to ignored `target/page-index-inspection/`.
- Every downloaded SHA-256 matched `apps/axon-web/public/fixtures/s3-perf/s3-perf-object-sha256.txt`.
- Actual metadata inspection found, for each file: 4 row groups, 13 columns, 52 column chunks, 52 offset indexes, and **zero column indexes**.
- Decision: `s3-browser-perf-v1` cannot support predicate-to-page selection. Do not replace it, upload anything, run the live public-S3 performance suite, or claim public-S3 page-index evidence.

### Existing telemetry boundary

- Existing range telemetry distinguishes logical bootstrap-footer, scan-footer, and scan-data reads and reports total physical bytes, cache reuse, coalescing gaps, overfetch, and readahead.
- It does not distinguish footer physical bytes from page-index physical bytes because both metadata fetches use the scan-footer phase, and it does not report selected/skipped pages.
- The experiment will avoid a generated/public contract change. The deterministic fixture manifest will record exact footer, column-index, offset-index, and data-page extents. Playwright will observe actual request/response ranges and classify physical bytes/requests against those extents. Rust engine tests will separately prove that Arm B constructs and applies a real `RowSelection`.

### Fixture, predicate, and browser harness

- Extend the ignored `apps/axon-web/public/fixtures/prod-like/` generation with one standalone A/B Parquet object and JSON manifest; do not commit generated Parquet bytes.
- Seed: fixed and recorded. Layout: one row group, 65,536 monotonic `event_id` rows, page row limit 1,024, page statistics, offset indexes, dictionary disabled, deterministic fixed-width payload.
- SQL:

  ```sql
  SELECT
    COUNT(*) AS row_count,
    SUM(event_id) AS event_id_sum,
    SUM(LENGTH(payload)) AS payload_length_sum
  FROM page_index_ab
  WHERE event_id >= 63488
  ```

- The row group survives both arms; only the final two of 64 predicate pages can match. Exact expected result: 2,048 rows plus deterministic sums.
- The existing query-performance harness already serves generated local fixture bytes through Vite and executes the default browser DataFusion worker. A focused Playwright config can use the same real browser-WASM path without a public fixture.

## Experiment design and decision rules

- Arm A: default `Skip`; no index load and no page selection.
- Arm B: feature-gated `Predicate`; independently request column and offset indexes and construct a selection from compatible integer indexes.
- Default production builds do not expose the experiment method. The focused build enables an `axon-web-wasm/page-index-experiment` feature; the worker accepts the internal mode only when that method exists.
- Each run creates a fresh browser context/worker/session and cold in-memory caches. Order: `A/B/B/A/A/B`.
- Preserve identical fixture URL and checksum, SQL, descriptor, limits, projection, and browser-WASM execution.
- Preserve residual DataFusion filtering. Missing, malformed, type-incompatible, or inconsistent indexes fail open to an unselected scan.
- Artifact: `target/perf/page-index-byte-savings-ab-evidence.json`; Playwright attachment under `apps/axon-web/test-results/`.
- Artifact schema includes schema version, fetched base SHA, fixture generator provenance and SHA-256, exact SQL, arm policies, per-run request ranges and metrics, aggregates, parity, correctness/runtime gates, gross data-page bytes avoided, index overhead, net physical savings, and decision.
- Before acceptance, recursively scan serialized evidence for credentials, AWS access keys, tokens, signed-query fields, and `X-Amz-*`.
- Positive evidence requires every run to match exact results, Arm B to skip nonzero pages and data bytes, and every Arm B total physical byte count to be below every Arm A count after index overhead. Latency is descriptive only.
- No-effect, nonpositive, workload-miss, correctness, fallback, ownership, memory, cache/readahead-confound, public-API, or redesign conditions stop the slice with defaults unchanged.

## Vertical TDD tasks

### Task 1: Deterministic indexed fixture

**Files:**
- Modify: `apps/axon-web/src/bin/generate_prod_fixture.rs`
- Test: `crates/wasm-parquet-engine/tests/metadata.rs`

1. Add a failing metadata test requiring independently present column and offset indexes, multiple pages, monotonic per-page integer bounds, and stable layout.
2. Run `cargo test -p wasm-parquet-engine --test metadata --locked`.
3. Add the smallest shared metadata-summary helper needed by the generator/test, or keep inspection local if sharing would broaden the engine.
4. Generate the standalone ignored fixture with `EnabledStatistics::Page`, fixed page row count, and a JSON manifest containing seed, writer properties, file hash, rows, row groups, per-page bounds/extents, index extents, and selectivity.
5. Run the focused metadata test and `npm run build:fixture`.

### Task 2: Predicate-derived page selection

**Files:**
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Test: `crates/wasm-parquet-engine/tests/streaming.rs`
- Test: `crates/wasm-datafusion-poc/tests/parquet_scan_exec.rs`

1. Add failing tests proving Arm A makes no selection, Arm B skips pages, exact results match, and missing/incompatible indexes fail open.
2. Run the focused Rust tests and confirm failure for missing experiment APIs/metrics.
3. Add the internal `ParquetPageIndexPolicy::{Skip, Predicate}` query extension, defaulting to `Skip`.
4. Use `new_with_options(... Optional ...)` only for Arm B.
5. Build a `RowSelection` for selected row groups from integer page min/max plus offset first-row positions. Validate all lengths and conversions; return no selection on incompatibility.
6. Supply `.with_row_selection(...)` only when a real compatible selection exists. Keep row groups, projection, and DataFusion residual filters unchanged.
7. Record internal selected/skipped/touched page counts for Rust assertions.
8. Run both focused test targets.

### Task 3: Physical byte accounting

**Files:**
- Modify: `apps/axon-web/tests/page-index-byte-savings-ab.spec.ts`
- Test: same file

1. Add pure failing tests for inclusive HTTP range parsing, exact interval union/intersection, footer/index/data-page classification, cache-hit exclusion, safe-integer validation, gross/net calculations, and redaction.
2. Implement only the pure accounting and artifact decision helpers.
3. Run the focused Playwright test in unit-only mode or the smallest supported test selection.

### Task 4: Feature-gated browser A/B

**Files:**
- Modify: `apps/axon-web/Cargo.toml`
- Modify: `crates/wasm-datafusion-session/Cargo.toml`
- Modify: `crates/wasm-datafusion-poc/Cargo.toml`
- Modify: `apps/axon-web/src/lib.rs`
- Modify: `crates/wasm-datafusion-session/src/lib.rs`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/package.json`
- Create: `apps/axon-web/playwright.page-index-ab.config.ts`
- Create: `apps/axon-web/tests/page-index-byte-savings-ab.spec.ts`

1. Add the focused test expecting six fresh successful runs, exact scalar parity, browser-WASM/no fallback, terminal zero ownership, bounded peaks, equivalent cold caches, neutral coalescing/readahead, and a complete redacted artifact.
2. Confirm it fails because the feature/build command/control does not exist.
3. Propagate the Cargo feature and add a feature-gated `SandboxQuerySession` method that selects the internal policy.
4. Let only the focused worker URL request call that optional method; default worker construction remains `Skip`.
5. Add `build:wasm:page-index-ab` and `test:browser:page-index-ab`.
6. Run `npm run build:fixture`, the experimental WASM build, and the focused browser test.
7. Hash the completed artifact with `shasum -a 256`.
8. Rebuild normal WASM before the general browser/performance verification.

### Task 5: Evidence decision and documentation

**Files:**
- Modify: `docs/plans/2026-06-23-browser-query-performance-investigation.md`
- Modify: `docs/plans/2026-07-26-public-s3-page-index-byte-savings-ab.md`

1. Record public-fixture index incapability, local fixture provenance, artifact path/hash, every raw and aggregate arm metric, gross avoided data bytes, index overhead, net physical savings, correctness/runtime/cache/memory results, and the decision.
2. Record a prospective immutable public fixture revision, manifest fields, generation/upload/verification commands, but do not upload.
3. State explicitly that proof is local browser-WASM only and defaults remain unchanged.

## Verification commands

From repository root:

```bash
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-datafusion-poc --locked
cargo test -p wasm-query-runtime --locked
cargo check \
  -p wasm-parquet-engine \
  -p wasm-datafusion-poc \
  -p wasm-query-runtime \
  -p axon-web-wasm \
  --target wasm32-unknown-unknown \
  --locked
bash tests/perf/browser_query_performance.sh --reporter=line
```

From `apps/axon-web`:

```bash
npm run build:fixture
npm run build:wasm
npm run test:browser:query-performance -- --reporter=line
npm run test:browser:page-index-ab -- --reporter=line
npm run lint
npx tsc --noEmit
npm run format:check
```

Do not run the live public-S3 suite because actual fixture inspection proved the required column indexes absent. If 5173/5178 is occupied, use an ignored temporary Playwright config and another port; do not kill the owner.

## Commit boundaries

After all implementation/evidence tests pass:

1. Stage only implementation, fixture-generator, focused-test, and this execution-plan paths. Run `git diff --cached --check`. Commit:

   ```text
   test(perf): add page-index byte-savings A/B evidence
   ```

2. Stage only canonical decision documentation. Run `git diff --cached --check`. Commit:

   ```text
   docs(perf): record page-index A/B decision
   ```

Final checks:

```bash
git diff --check origin/main...HEAD
git log --oneline origin/main..HEAD
git status --short --branch
```

Keep the worktree and commits local. Do not push, merge, open a PR, mutate cloud objects, or replace `s3-browser-perf-v1`.

## Execution result

### Measured decision

Positive local browser-WASM byte evidence. Keep the production default off.

The focused A/B passed in Chromium in `A/B/B/A/A/B` order, with a fresh browser
context, worker, and session for every run. The test blocked service workers,
disabled the browser HTTP cache through CDP, observed zero cache-served fixture
responses, and accepted bytes only from completed `206` responses whose
`Content-Range` and `Content-Length` matched the requested ranges. All six runs
returned:

- `browser_wasm`;
- no fallback event or response fallback reason;
- schema `row_count,event_id_sum,payload_length_sum`;
- 2,048 rows, event-id sum 132,119,552, and payload-length sum 262,144;
- result checksum
  `80fee61a9cf95efb491badb1ae901d73502dd68ae5744896d4636a0532dc37bc`;
- 968 IPC bytes in three chunks;
- zero terminal coordinator reserved/staged bytes;
- zero terminal DataFusion bytes;
- owned-memory and cursor peaks within their recorded limits;
- zero scan overfetch, readahead requests/bytes, coalescing-gap bytes, and
  cache reuse/storage.

Every repetition in a given arm produced the same physical metrics:

| Metric | Arm A: `Skip` | Arm B: `Predicate` |
| --- | ---: | ---: |
| Total physical bytes / requests | 643,403 / 6 | 32,934 / 9 |
| Footer bytes / requests | 1,448 / 4 | 1,448 / 4 |
| Column-index bytes | 0 | 9,758 |
| Offset-index bytes | 0 | 1,654 |
| Page-index bytes / requests | 0 / 0 | 11,412 / 1 |
| Scan data bytes / requests | 641,955 / 2 | 20,074 / 4 |
| Pages selected / skipped / touched | 0 / 0 / 64 | 2 / 62 / 2 |
| Files selected / skipped | 1 / 0 | 1 / 0 |
| Row groups selected / skipped | 1 / 0 | 1 / 0 |
| Engine bytes fetched | 642,679 | 32,210 |
| Engine scan-data range reads | 2 | 4 |
| Range-cache hits / misses | 0 / 0 | 0 / 0 |
| Coordinator peak reserved / staged bytes | 8,388,608 / 968 | 8,388,608 / 968 |
| DataFusion peak / terminal bytes | 0 / 0 | 0 / 0 |

- Conservative gross data-page bytes avoided: **621,881**.
- Maximum observed index overhead: **11,412** bytes.
- Conservative net physical savings after index overhead: **610,469** bytes.
- Net reduction from Arm A: approximately **94.9%**.

Latency remains descriptive only and was not used for this decision.

### Fixture and artifact provenance

Generated local fixture:

- revision: `local-page-index-ab-v1`;
- ignored path:
  `apps/axon-web/public/fixtures/prod-like/page-index-ab/event-id.parquet`;
- SHA-256:
  `27c03fe1530ad0de3da608192d3a7742ad0c07884e9176daec98ec098ec66a2e`;
- size: 654,095 bytes;
- seed: `12109367126683295782`;
- 65,536 rows, one row group, 1,024-row page limit;
- two independently present column indexes and two offset indexes;
- exact footer, index, and data-page byte extents recorded in the generated
  ignored manifest.

Canonical ignored evidence:

```text
target/perf/page-index-byte-savings-ab-evidence.json
```

SHA-256:
`7fbd20225bff579dddfb05f929f2f1cbe81c4059ecb8aa6f105429c11eb12c11`.
The adjacent `.sha256` file records the same digest. The JSON includes the
fetched base, exact SQL and policies, all six raw runs and ranges, aggregates,
fixture provenance independently verified against the generated bytes, the
response/cache audit, explicit correctness/runtime/memory/cache decision gates,
and the decision. A serialized scan rejected URI userinfo, AWS access-key
shapes, credential, signed-query, token, authorization, and `X-Amz-*` patterns
before the decision gate.

### Public-fixture gate and prospective revision

This proof is local only. Actual metadata from all eight pinned
`s3-browser-perf-v1` active files showed 52 offset indexes and zero column
indexes per file. No live public-S3 test ran because that immutable fixture
cannot build a page predicate selection.

A prospective, separately authorized revision should be named
`s3-browser-perf-page-index-v2`, use a new immutable prefix
`fixtures/s3-browser-perf-page-index-v2`, and record at least:

- generator commit and seed;
- writer version, page statistics policy, page size/row limit, compression,
  and dictionary policy;
- object inventory, active-file and byte counts, SHA-256 per object, manifest
  SHA-256, and provenance SHA-256;
- per-file row groups, column/offset-index presence, page counts, page byte
  extents, and predicate selectivity;
- exact SQL and expected scalar/checksum.

After changing the generator revision and statistics policy in a dedicated
authorized slice, the prospective local generation command is:

```bash
cd apps/axon-web
npm run build:s3-perf-fixture
AXON_S3_PERF_METADATA_ROOT=../../target/fixtures/s3-perf-generated \
  bash scripts/verify-s3-perf-fixture.sh --metadata-only
```

The prospective upload command is documented but was **not** run:

```bash
AXON_S3_PERF_FIXTURE_BUCKET=axon-public-s3-fixture-452456948477 \
AXON_S3_PERF_FIXTURE_PREFIX=fixtures/s3-browser-perf-page-index-v2 \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
npm run build:s3-perf-fixture
```

After anonymous object/checksum verification, the prospective browser command
would be:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI=s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2/table \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
CI=1 \
npm run test:browser:public-s3-live -- --reporter=line
```

Publishing that fixture, adapting the live test to the page-index A/B, and any
default enablement/guardrail proposal are separate gates. This slice performed
no upload, cloud mutation, public API change, default change, push, merge, or
pull-request action.
