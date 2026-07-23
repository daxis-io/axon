# Performance Audit Findings Resolution Plan

**Date:** 2026-07-23

**Authoritative base:** `origin/main` at
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f`

**Goal:** Close every finding from the browser query-performance audit with a
shipped fix, reproducible evidence, or an explicit evidence-backed decision not
to ship an experiment.

## Boundaries

- Work only in `.worktrees/perf-audit-resolution` on
  `perf/resolve-performance-audit`.
- Treat the root checkout and every pre-existing dirty worktree as read-only
  evidence.
- Do not delete worktrees or branches.
- Do not push, open a pull request, or change cloud resources.
- Preserve the public SDK's atomic-success contract. Private child-worker
  chunks may remain hidden until a successful terminal message.
- Do not enable a production worker pool or add WCRPC without the missing
  process-cold, memory, and workload-coverage evidence.
- Do not tune range-cache, coalescing, or readahead thresholds without a live
  artifact that demonstrates a better request-to-byte tradeoff.

## Closure Matrix

| Audit finding | Resolution |
| --- | --- |
| Default DataFusion worker exceeds its 6 MiB Brotli budget | Restore the shipped release artifact below 6,291,456 bytes, keep a before/after report, and reject regressions in recurring CI. |
| Size job is manual and report-only | Make the default-SKU budget job run on pull requests and `main`; keep manual inputs only for optional exploratory SKUs. |
| Host `cargo test` wall time is mislabeled as browser performance | Replace the release-gate command with a Playwright browser probe that records startup, query milestones, result bytes, and bounded component-memory counters. Retain the host smoke only as a diagnostic. |
| Rust cursor is bounded but the coordinator stages result-sized output | Keep atomic success, expose exact coordinator peak/limit metrics, fail before accepting a chunk that would exceed the cap, and prove the bound in unit and real-browser tests. |
| The canonical plan still says streaming stops at the WASM boundary | Rewrite the current-state section around the landed cursor/coordinator design and name atomic coordinator staging as the remaining invariant. |
| Public-S3 evidence cannot be reproduced from `main` | Land a deterministic fixture generator, pinned manifest/checksums, staging validator, and documented upload/live-test commands without committing data objects or credentials. |
| Current public-S3 artifact proves no overfetch regression, not a speedup | Preserve that honest verdict and make future artifacts identify fixture provenance. Do not claim a cache/readahead latency win. |
| `max_scan_bytes` already enforces planned and realized bytes, despite stale backlog text | Mark scan-byte enforcement complete and add a separate speculative-overfetch counter and optional budget. |
| Deeper page-index pruning is backlog and uses a deprecated Parquet API | Migrate to explicit column/offset index policies and add policy-selection tests. Do not claim page-level byte savings until browser evidence exercises it. |
| Worker-pool results are promising but stale and incomplete | Port the compact analysis/harness onto current main, exclude large raw samples, run current-main deterministic analysis and a browser compatibility probe, and record `continue research / production inconclusive / WCRPC not justified`. |
| Old performance branches/worktrees are ambiguous | Add a dated inventory with exact refs and disposition. Classify only; do not delete user state. |

## Commit 1: Freeze The Resolution Contract

**Files**

- Add: `docs/plans/2026-07-23-performance-audit-findings-resolution.md`

**Verification**

```bash
git diff --check
git status --short
```

**Commit**

```text
docs(perf): plan performance audit findings resolution
```

## Commit 2: Restore And Automate The WASM Size Gate

**Files**

- Modify: `Cargo.toml`
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/perf/report_datafusion_wasm_size.sh`
- Modify: `tests/perf/report_datafusion_wasm_size_test.sh`
- Modify: `tests/perf/README.md`
- Modify: `docs/program/browser-datafusion-size-audit.md`
- Modify: `docs/release-gates/daxis-browser-datafusion-budget-profile.json`

**Red**

1. Extend the shell contract test to require:
   - the default `axon-web-wasm` package/stem,
   - the checked-in 6,291,456-byte budget when no exploratory override is set,
   - a recurring pull-request/`main` workflow condition,
   - artifact upload on failure.
2. Run `bash tests/perf/report_datafusion_wasm_size_test.sh`; confirm it fails
   against the manual, optional job.
3. Preserve the fresh current-main size failure:
   `6,591,104 > 6,291,456`.

**Green**

1. Measure the size-oriented release profile already identified by the May
   profile matrix.
2. Accept it only if the exact default-SKU command passes and the browser-real
   query probe remains within the checked-in startup budgets.
3. Make the recurring job use the same package, stem, profile, and hard budget
   as release evidence.

**Verification**

```bash
bash tests/perf/report_datafusion_wasm_size_test.sh
AXON_DF_SIZE_OUT_DIR=target/df-size/axon-web-wasm-resolution \
  bash tests/perf/report_datafusion_wasm_size.sh
```

**Commit**

```text
perf(wasm): restore and enforce the default worker size budget
```

## Commit 3: Bound And Measure Coordinator Staging

**Files**

- Modify: `apps/axon-web/src/sandbox-query-stream-protocol.ts`
- Modify: `apps/axon-web/src/sandbox-query-stream-protocol.test.ts`
- Modify: `apps/axon-web/src/sandbox-query-worker.ts`
- Modify: `apps/axon-web/src/axon-browser-sdk.ts`
- Modify: `crates/query-contract/src/lib.rs`
- Modify: `crates/query-contract/tests/query_contract.rs`
- Modify: `apps/axon-web/proto/axon/exec/v1/exec.proto`
- Regenerate: contract Rust/TypeScript outputs
- Modify: `apps/axon-web/tests/internal-arrow-ipc-stream.spec.ts`
- Add: `apps/axon-web/tests/browser-query-performance.spec.ts`
- Add: `apps/axon-web/playwright.browser-query-performance.config.ts`
- Modify: `apps/axon-web/package.json`
- Add: `tests/perf/browser_query_performance.sh`
- Modify: `tests/perf/report_datafusion_wasm_size_test.sh`

**Red**

1. Unit-test that `QueryStage` rejects a chunk before retaining it when the next
   length would exceed the query cap.
2. Contract-test optional metrics:
   `coordinator_peak_staged_bytes`, `coordinator_staging_limit_bytes`,
   `cursor_peak_pending_encoded_bytes`, and
   `cursor_peak_transport_chunk_bytes`.
3. Browser-test that a successful query reports finite values, peak does not
   exceed the limit, the limit does not exceed the browser-safe cap, and
   chunked delivery still publishes nothing before success.
4. Browser-test an over-limit result fails without a partial public result.

**Green**

1. Move the limit check into `QueryStage.stage()` so excess ownership is never
   retained.
2. Project cursor and coordinator peaks into successful query metrics.
3. Add a Playwright artifact containing cold/warm duration, time to
   `arrow_ipc_ready`, result bytes/chunks, and the component-memory bounds.
4. Replace the release profile's host-proxy report command with the browser
   probe. Keep the host smoke labeled diagnostic-only.

**Verification**

```bash
npm test -- src/sandbox-query-stream-protocol.test.ts
npm run codegen:check
npm run build:wasm
npm run test:browser:query-performance -- --reporter=line
bash tests/perf/browser_query_performance.sh
```

**Commit**

```text
perf(ipc): bound and report coordinator staging
```

## Commit 4: Make The Public-S3 Fixture Reproducible

**Files**

- Modify: `apps/axon-web/Cargo.toml`
- Add: `apps/axon-web/src/bin/generate_s3_perf_fixture.rs`
- Add: `apps/axon-web/public/fixtures/s3-perf/s3-perf-fixture-manifest.json`
- Add: `apps/axon-web/public/fixtures/s3-perf/s3-perf-active-sha256.txt`
- Add: `apps/axon-web/public/fixtures/s3-perf/S3_ACCESS.md`
- Add: `apps/axon-web/scripts/verify-s3-perf-fixture.sh`
- Add: `apps/axon-web/scripts/verify-s3-perf-fixture.test.sh`
- Modify: `apps/axon-web/package.json`
- Modify: `apps/axon-web/tests/public-s3-live.spec.ts`

**Red**

1. Make the validator fail when the manifest lacks the public URI, region,
   active-file inventory, or checksums.
2. Make the live spec fail evidence creation when fixture provenance is absent.
3. Test that secrets and signed query strings cannot enter committed metadata.

**Green**

1. Land the deterministic, seeded generator behind the existing
   `fixture-generator` feature.
2. Commit metadata and active-object checksums only; keep generated Parquet,
   Delta log objects, credentials, and raw browser artifacts ignored.
3. Validate local/staged objects against the pinned manifest before upload or
   live execution.
4. Embed manifest/checksum identity in the public-S3 evidence artifact.

**Verification**

```bash
cargo test -p axon-web-wasm --locked --features fixture-generator --no-run
bash apps/axon-web/scripts/verify-s3-perf-fixture.test.sh
npm run test:browser:public-s3-live -- --list
```

**Commit**

```text
test(perf): make the public S3 fixture reproducible
```

## Commit 5: Add Explicit Speculative-Overfetch Accounting

**Files**

- Modify: `crates/query-contract/src/lib.rs`
- Modify: `crates/query-contract/tests/query_contract.rs`
- Modify: `crates/wasm-datafusion-poc/src/lib.rs`
- Modify: `crates/wasm-datafusion-poc/src/ipc_cursor.rs`
- Modify: `crates/wasm-datafusion-poc/tests/query_budgets.rs`
- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `apps/axon-web/proto/axon/exec/v1/exec.proto`
- Modify/regenerate: Rust and TypeScript contract projections
- Modify: `docs/release-gates/daxis-browser-datafusion-budget-profile.json`

**Red**

1. Add `max_scan_overfetch_bytes` to runtime limits and
   `scan_overfetch_bytes` to metrics.
2. Define overfetch as fetched coalescing gap bytes plus speculative readahead
   bytes left unused at query completion. Do not count cache hits or logical
   scan bytes as overfetch.
3. Test exact-bound success, one-byte-over failure, no-overfetch success, and
   saturating/checked arithmetic behavior.

**Green**

1. Compute one query-scoped overfetch total from the already query-scoped
   range-read observations.
2. Enforce the optional budget at terminal accounting, returning the existing
   structured browser-runtime-constraint fallback.
3. Set the Daxis default to a bounded nonzero value that admits the existing
   coalescing policy and 512 KiB readahead cap.

**Verification**

```bash
cargo test -p query-contract --locked
cargo test -p wasm-datafusion-poc --locked --test query_budgets
cargo test -p wasm-parquet-engine --locked
npm run codegen:check
```

**Commit**

```text
perf(scan): account for speculative overfetch explicitly
```

## Commit 6: Modernize Page-Index Policy Without Overclaiming

**Files**

- Modify: `crates/wasm-parquet-engine/src/lib.rs`
- Modify: `crates/wasm-parquet-engine/tests/streaming.rs`

**Red**

1. Add policy-selection tests for:
   - both column and offset indexes present,
   - only one index present,
   - no indexes present.
2. Assert that the runtime never enables an index policy whose metadata is
   absent.

**Green**

1. Replace deprecated `ArrowReaderOptions::page_index()` inspection with
   explicit `column_index_policy` and `offset_index_policy`.
2. Preserve fail-open behavior and current row-group/page filtering.

**Verification**

```bash
cargo test -p wasm-parquet-engine --locked
cargo check -p wasm-parquet-engine --target wasm32-unknown-unknown --locked
```

**Commit**

```text
perf(parquet): make page-index policy explicit
```

## Commit 7: Refresh The Worker-Pool Decision On Current Main

**Files**

- Add: `apps/axon-web/tests/worker-pool-benchmark-analysis.ts`
- Add: `apps/axon-web/tests/worker-pool-benchmark-analysis.spec.ts`
- Add: `apps/axon-web/tests/browser-worker-pool-compatibility.spec.ts`
- Add: `apps/axon-web/playwright.worker-pool-compatibility.config.ts`
- Modify: `apps/axon-web/package.json`
- Add: `docs/research/wcrpc-worker-pool-value/CURRENT_MAIN_DECISION.md`

**Red**

1. Port deterministic analysis tests for pairing, percentiles, seeded
   bootstrap intervals, exact aggregate merging, and the split between
   `researchDecision` and production `decision`.
2. Add a browser compatibility probe that proves two independent current-main
   coordinator workers can execute disjoint shards and merge exact integer
   aggregates.

**Green**

1. Port only the compact analysis and compatibility harness. Do not copy the
   67k-line raw sample artifacts.
2. Record the last controlled timing as historical evidence and current-main
   compatibility as fresh evidence.
3. Keep the decision:
   - continue bounded fanout research,
   - production worker pool inconclusive,
   - WCRPC not justified.
4. Restate the next research gate: at least 20 process-cold pairs with
   process-tree memory, P2/P4 workload coverage, skew, output-heavy queries,
   and unique-identity public-S3 runs.

**Verification**

```bash
npm test -- tests/worker-pool-benchmark-analysis.spec.ts
npm run build:wasm
npm run test:browser:worker-pool-compatibility -- --reporter=line
```

**Commit**

```text
test(perf): refresh the worker pool decision on current main
```

## Commit 8: Reconcile The Canonical Performance Record

**Files**

- Modify: `docs/plans/2026-06-23-browser-query-performance-investigation.md`
- Add: `docs/research/browser-query-performance-workstream-inventory.md`
- Modify: `tests/perf/README.md`

**Content**

1. Replace stale WASM-boundary language with the current cursor/coordinator
   contract.
2. Mark planned and realized scan-byte enforcement complete.
3. Record the new overfetch budget, page-index policy, browser performance
   probe, S3 fixture provenance, size gate, and worker-pool decision.
4. Inventory exact merged, superseded, dormant, dirty, and active worktree/ref
   states. Recommend archive candidates without deleting them.

**Verification**

```bash
git diff --check
rg -n "full IPC buffer|stops at the WASM boundary" \
  docs/plans/2026-06-23-browser-query-performance-investigation.md
```

**Commit**

```text
docs(perf): reconcile the browser performance workstream
```

## Final Verification

```bash
cargo fmt --all --check
cargo test -p query-contract --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-datafusion-poc --locked --tests
bash tests/perf/report_datafusion_wasm_size_test.sh
AXON_DF_SIZE_OUT_DIR=target/df-size/axon-web-wasm-resolution \
  bash tests/perf/report_datafusion_wasm_size.sh
npm install
npm run codegen:check
npm test
npm run build:wasm
npm run test:sdk
npm run test:browser:query-performance -- --reporter=line
npm run test:browser:worker-pool-compatibility -- --reporter=line
git diff --check origin/main...HEAD
git status --short --branch
```

The final report must separate:

- measured improvements,
- newly enforced limits,
- fresh browser evidence,
- historical research evidence,
- explicit no-ship decisions,
- external live-S3 execution not rerun because cloud mutation and credentials
  were out of scope.
