# Browser Query Performance Investigation

**Date:** 2026-06-23

**Goal:** Capture the browser Delta and Parquet performance investigation as a step-by-step reference. Each numbered area can become its own implementation slice, test plan, or design review.

**Scope:** Browser-side Delta snapshot resolution, public object-storage range reads, Parquet scan pruning, DataFusion custom scan integration, Arrow IPC output, and startup/runtime loading in `apps/axon-web` and the Rust WASM crates.

**Related framing:**

- [Axon workbench and query engine architecture](../program/axon-workbench-architecture.md)
- [Provider and host integration model](../program/provider-model.md)
- [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)
- [ADR-0010: Pluggable Catalog Providers And Table Read Resolution Seams](../adr/ADR-0010-pluggable-catalog-providers.md)

**Historical read-only status (2026-06-23):** The original investigation did
not edit runtime code. Its checkout had existing local changes in:

- `crates/wasm-datafusion-session/Cargo.toml`
- `crates/wasm-datafusion-session/tests/parquet_dataset.rs`
- `crates/wasm-parquet-engine/src/lib.rs`
- `crates/wasm-query-runtime/src/lib.rs`

Keep those files as user-owned state unless a later implementation plan claims them.

---

## Architecture Alignment

This plan is part of Axon's engine/runtime backlog. It should stay inside the boundary defined by ADR-0009 and ADR-0010:

- Axon owns browser/native execution performance: descriptor validation, browser-safe byte-range reads, Parquet/Delta scan planning, pruning, cache behavior, DataFusion-backed execution, Arrow IPC result transport, runtime budgets, metrics, and fallback taxonomy.
- E1 `CatalogProvider` remains discovery-only. It returns navigation nodes, metadata, refs, and capability facts. It must not mint browser descriptors, resolve grants, inspect table bytes, or call `openDeltaTable()`.
- E9 `DataAccessResolver` owns table read resolution. It returns `descriptor`, `read_access_plan`, `fallback`, or `blocked`.
- `ExecutionProvider` owns query execution and preview. Only openable E9 outcomes may reach `openDeltaTable()`.
- Durable catalog/navigation state may store non-secret locators, refs, display metadata, and refresh hints. It must not persist read resolutions, signed URLs, grants, or openable descriptors.

Performance work can cache resolved descriptors, access envelopes, and footer metadata in runtime/session scope when identity and expiry rules make that safe. That cache is an execution optimization, not durable catalog state.

---

## Current Status

The remote baseline for the 2026-07-23 audit is `origin/main` at
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f`. It includes the original
instrumentation, identity propagation, footer and shared range caches,
prebootstrap pruning, descriptor carryover, lazy startup, scan-trace split,
small-gap coalescing, bounded readahead, and the private Arrow IPC
cursor/coordinator path.

The local-only `perf/resolve-performance-audit` branch closes the remaining
audit findings as follows:

| Finding | Current resolution |
| --- | --- |
| Default worker size | Cargo release now uses `opt-level = "z"`. The current default worker is 3,932,092 Brotli bytes against the 6,291,456-byte gate, which runs on pull requests and `main`. |
| Browser performance evidence | `bash tests/perf/browser_query_performance.sh` runs the shipped WASM in Chromium against the checked-in prod-like Delta/Parquet fixture. It records startup, cold/warm query milestones, IPC bytes/chunks, exact cursor/coordinator peaks, 20-query post-GC user-agent memory, and atomic over-limit behavior. The host smoke is diagnostic-only. |
| IPC lifecycle and memory | Rust emits exact-sized private schema/data/EOS chunks under 1 MiB credit. Pending encoded storage grows lazily and is capped at 8 MiB per logical batch; total output follows the separate 16 MiB Daxis profile. The coordinator stages at most the browser-safe 8 MiB cap, rejects before retaining an excess chunk, and publishes public chunks only after a successful private terminal. |
| Cancellation and recovery | Coordinator cancellation/deadline state is authoritative, request IDs remain reserved while draining, unresponsive children are replaced, and engine-wide cancellation uses query-generation snapshots. Chromium, Firefox, and WebKit cover late failure, output-budget failure, crash, hang, and recovery without partial public results. |
| Scan budgets | Planned and realized `max_scan_bytes` enforcement is complete. `scan_overfetch_bytes` now saturating-adds fetched coalescing gaps and unused speculative readahead, with optional terminal enforcement; the Daxis profile allows 1 MiB. |
| Page-index API | Metadata loading preserves explicit Parquet column-index and offset-index policies independently. This is correctness/compatibility work; no page-level browser byte saving is claimed without new evidence. |
| Public-S3 reproducibility | `s3-browser-perf-v1` now has a deterministic seeded generator, committed manifest/checksums/provenance, staging validator, and documented upload/live commands. No credentials or generated data objects are committed. |
| Worker pool | Current main can run two disjoint coordinator shards and merge exact aggregates. Historical timings justify bounded research only; production value is inconclusive and WCRPC is not justified. |

The 2026-07-16 public-S3 artifact remains honest historical evidence: adaptive
readahead fetched, used, and wasted zero bytes, while physical bytes returned
to the 22,677,645-byte pre-cache baseline. It proves no overfetch regression,
not a latency win.

---

## Remaining Backlog

Prioritize these only when metrics show they are the next limiting factor:

1. Prove page-index pruning changes browser bytes before claiming or tuning
   page-level savings.
2. Refresh representative public-S3 browser UAT when the pinned fixture,
   query, or metrics contract changes. Validate provenance before upload or
   execution.
3. Add physical-vs-logical range request summary metrics only if the current
   labels become operationally ambiguous.
4. Tune range-coalescing or readahead thresholds only if future live telemetry
   shows a better request-to-byte tradeoff.
5. Revisit worker pools only after the documented process-cold S/P2/P4,
   process-tree-memory, skew, output-heavy, and unique-cache-identity evidence
   gate is satisfied.

Keep future changes small enough that each workstream can prove a specific latency, memory, or byte-read improvement.

### Range Coalescing Threshold Telemetry - 2026-06-27

Live public-GCS browser telemetry was blocked in the implementation shell because
`AXON_LIVE_PUBLIC_GCS_TABLE_URI` was not set. The follow-up evidence is therefore
limited to the deterministic synthetic threshold sweep in
`small_gap_coalescing_threshold_sweep_records_request_and_amplification_tradeoffs`.

The sweep keeps logical range metrics distinct from physical HTTP requests:
`scan_data_range_reads` remains logical range count, `coalesced_range_reads`
remains physical coalesced request count, and `coalesced_gap_bytes_fetched`
remains fetched gap overfetch.

| Case | Logical ranges | Exact requests | Physical requests | Logical bytes | Physical bytes | Candidate gap bytes | Fetched gap bytes | Candidate gap amplification |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Nearby page ranges | 2 | 2 | 1 | 65,536 | 69,632 | 4,096 | 4,096 | 6.25% |
| Cumulative gap at 64 KiB limit | 5 | 5 | 1 | 327,680 | 393,216 | 65,536 | 65,536 | 20.00% |
| Amplification at 25% limit | 2 | 2 | 1 | 2,048 | 2,560 | 512 | 512 | 25.00% |
| Individual gap just over 16 KiB | 2 | 2 | 2 | 131,072 | 131,072 | 16,385 | 0 | 12.50% |
| Cumulative gap just over 64 KiB | 6 | 6 | 2 | 393,216 | 458,752 | 65,537 | 65,536 | 16.66% |
| Amplification just over 25% | 2 | 2 | 2 | 2,048 | 2,048 | 513 | 0 | 25.04% |
| Physical range just over 512 KiB | 2 | 2 | 2 | 524,289 | 524,289 | 0 | 0 | 0.00% |

Threshold decision: keep the current policy unchanged until live browser UAT
shows a narrower improvement. The current policy remains 16 KiB max individual
gap, 64 KiB max cumulative gap, 512 KiB max physical range, and 25% max gap
amplification.

Historical recommendation: capture live browser UAT evidence before starting a
shared Delta/Parquet range cache, bounded readahead, or scan-byte budget
enforcement. The cache and readahead slices landed, and the 2026-07-16 public-S3
evidence below supersedes this gate.

### Live Public-GCS Browser UAT Evidence - 2026-06-28

Live public-GCS browser UAT evidence remained blocked in the implementation
shell because `AXON_LIVE_PUBLIC_GCS_TABLE_URI` was not set. No live metrics or
test artifact were collected; this is an external environment gate, not
product-success evidence.

### Live Public-S3 Cache/Readahead Evidence - 2026-07-16

The current resolution pins the live fixture as `s3-browser-perf-v1` through:

- `apps/axon-web/public/fixtures/s3-perf/S3_ACCESS.md`
- `apps/axon-web/public/fixtures/s3-perf/s3-perf-fixture-manifest.json`
- `apps/axon-web/public/fixtures/s3-perf/s3-perf-object-sha256.txt`
- `apps/axon-web/public/fixtures/s3-perf/s3-perf-provenance.json`

The provenance records the table URI
`s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table`,
region `us-east-2`, manifest SHA-256
`18d1c4c3b5e1ce78ce156ce51247a94a46e44401cad9688ec0d14ceaa01b6ab3`,
checksum-inventory SHA-256
`05f6c5823a88c49559eef70072165b584dfe3c320ae8a435c6f6f82f30d719a9`,
21 required objects, 8 active Parquet files, and 82,057,700 active data
bytes. The deterministic validator successfully staged and checked the
anonymous fixture during the 2026-07-23 resolution. The live browser suite was
not rerun because its environment variables were absent and this local-only
audit did not authorize cloud mutation. The artifact below therefore remains
the only live performance verdict.

The CI-forced Chromium run used the public performance fixture and passed all
four tests. Ports 5173 and 5174 were occupied by other worktrees, so the final
measured run used an equivalent temporary Playwright config on port 5175 with
the same spec, fixture, region, and rebuilt WASM:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI=s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
CI=1 npm run test:browser:public-s3-live -- --reporter=line
```

Artifact:

```text
apps/axon-web/test-results/public-s3-live-public-S3-l-0761d--table-root-in-browser-WASM-chromium/public-s3-live-uat-evidence.json
```

SHA-256: `0dbda0ae8f7018f739fbaf57897aebc1dfa5083927c8bc6691f9a494424a7152`.

`scan_data_range_reads` counts logical scan work. `bytes_fetched` counts
physical network bytes. `coalesced_range_reads` counts physical requests that
served multiple logical ranges; it does not count every network request.

| Metric                          |   Pre-cache |    Current | Delta |
| ------------------------------- | ----------: | ---------: | ----: |
| `bytes_fetched`                 |  22,677,645 | 22,677,645 |     0 |
| `scan_data_range_reads`         |         160 |        160 |     0 |
| `coalesced_range_reads`         |          32 |         32 |     0 |
| `range_cache_bytes_reused`      | not emitted |          0 |   n/a |
| `range_readahead_bytes_fetched` | not emitted |          0 |   n/a |
| `range_readahead_bytes_used`    | not emitted |          0 |   n/a |
| `range_readahead_wasted_bytes`  | not emitted |          0 |   n/a |
| `rows_emitted`                  |   1,048,576 |  1,048,576 |     0 |
| `arrow_ipc_bytes`               |      36,744 |     36,744 |     0 |

Current cache and readahead counters:

| Metric                                |      Value |
| ------------------------------------- | ---------: |
| `range_cache_hits`                    |          0 |
| `range_cache_misses`                  |        128 |
| `range_cache_bytes_stored`            | 22,677,645 |
| `range_cache_validation_misses`       |          0 |
| `range_cache_degraded_identity_reads` |          0 |
| `range_readahead_requests`            |          0 |

The adaptive gate kept batched and coalesced network reads outside admission
history, so this workload issued no speculative expansions. Readahead waste is
therefore no greater than use (`0 <= 0`), and physical bytes are 271,560 below
the previous 22,949,205-byte ceiling. Logical range count, rows emitted, and
Arrow IPC size stayed constant. Coalescing still served 160 logical data ranges
through 32 coalesced requests with zero fetched gap bytes. Cache and readahead
counters remained finite, nonnegative, and the artifact contains no signed URL
query material.

Decision: accept the adaptive admission follow-up without changing the 64 KiB
tail, 512 KiB query budget, or default enablement. Open the Slice 6 scan-byte and
overfetch-budget guardrail gate.

---

## Baseline Evidence

Focused tests passed during the investigation:

```bash
cargo test -p wasm-http-object-store --test extent_cache --locked
cargo test -p wasm-datafusion-poc --test scan_pushdown --test custom_scan_exec --locked
```

Observed local artifact sizes:

```text
apps/axon-web/dist/assets/axon_web_wasm_bg-B7VBuHgi.wasm       86M
apps/axon-web/src/wasm/axon_web_wasm_bg.wasm                   86M
target/wasm32-unknown-unknown/release/axon_web_wasm.wasm       93M
```

These artifact sizes came from existing local build outputs. Rebuild before treating them as release evidence for a changed branch.

---

## Shared Measurement Contract

Add counters before changing behavior. Each workstream should be able to answer these questions from logs, test probes, or query metrics:

- How many Delta log objects did the browser list or read?
- How many Parquet files did the browser inspect before SQL pruning?
- How many footer/trailer ranges did it fetch?
- How many exact duplicate ranges did it request?
- How many bytes came from footer, data pages, checkpoint/log materialization, and Arrow IPC output?
- How many rows and bytes crossed the worker boundary?
- Did the browser instantiate WASM before a user action required query execution?

Preferred metric fields:

- `delta_log_manifest_list_ms`
- `snapshot_resolve_ms`
- `descriptor_cache_hit`
- `footer_cache_hit`
- `range_cache_hit`
- `duplicate_range_reads`
- `coalesced_range_reads`
- `coalesced_gap_bytes_fetched`
- `prebootstrap_files_pruned`
- `footer_reads_avoided`
- `row_groups_skipped`
- `arrow_ipc_byte_length`
- `worker_created_before_query`
- `wasm_loaded_before_query`

---

## Area 1: HTTP Range-Read Planning

### Current State

Public object-storage connect resolves a descriptor and preflights one active Parquet file in the UI flow:

- `apps/axon-web/src/editor/connect/ConnectModal.tsx`
- `apps/axon-web/src/services/object-storage.ts`

The connected catalog stores summary state, not an openable read resolution. First query builds a browser session from the table URI and resolves the descriptor in `apps/axon-web/src/services/query.ts`.

Delta log materialization reads `_delta_log` objects through `HttpRangeReader`. Parquet footer inspection reads a trailer range, then a footer payload range in `crates/wasm-parquet-engine/src/lib.rs`.

The active scan path uses `HttpRangeAsyncFileReader`. The first eligible single `get_bytes` range for a strong object identity remains exact; a later single range can expand only when the complete logical range would have fit in the prior range's 64 KiB tail. Batched `get_byte_ranges` calls can coalesce nearby ranges only when the scan target has strong object identity and the request can use `If-Range`, and all batched reads stay outside readahead admission history. The shared range cache reuses identity-safe extents across Parquet reads, and bounded readahead records fetched, used, and wasted bytes.

### Main Gaps

- Range instrumentation, object identity propagation, shared footer metadata reuse, descriptor carryover, and small-gap Parquet range coalescing have landed or are implemented in the current slice.
- Scan-byte guardrail work should preserve adaptive admission, logical range metrics, and strong `ETag`/`If-Range` validation.
- Public connect preflight still checks only the first active file; broader representative-browser UAT remains backlog.

### Implementation Slices

1. Preserve `object_etag`, object size, and canonical resource identity from snapshot bootstrap into DataFusion scan targets. **Status: landed.**
2. Put Parquet range reads behind the existing object-store extent cache, or extract a shared range-cache interface used by both object-store reads and Parquet scan reads. **Status: landed with an identity-safe shared range cache and adaptive bounded readahead. Live public-S3 evidence passed the prerequisite for scan-byte guardrails.**
3. Add a Parquet metadata cache keyed by canonical object id plus immutable identity such as `ETag` and size. Cache trailer bytes, footer bytes, and decoded row-group metadata. **Status: landed for footer metadata reuse.**
4. Add small-gap coalescing for planned Parquet reads. Track coalesced read amplification so the browser does not fetch large gaps to save a small request count. **Status: implemented in the current slice with strict `206`/`Content-Range`/body-length/object-size/ETag validation.**
5. Carry connect-time descriptor and first-file preflight identity into the E9 read-resolution and query-session runtime cache, with expiration and identity refresh rules. Do not persist read resolutions or openable descriptors in durable catalog state. **Status: landed.**

### Correctness Constraints

- Cache keys need strong object identity: `ETag`, generation or hash, and size where available.
- Signed URLs cannot serve as the only cache key. They rotate and may include sensitive query parameters.
- Range reads need strict validation: `206`, valid `Content-Range`, expected body length, and consistent total size.
- Browser CORS must expose `Content-Length`, `Content-Range`, `Accept-Ranges`, and `ETag`.
- The browser should send `Range` and `If-Range` only when the provider allows them.
- Missing `ETag` should disable persistent identity reuse or emit an explicit degraded-mode metric.

### Validation Plan

- Add a failing-first Parquet test: metadata read followed by scan should not fetch the same trailer/footer ranges after caching lands.
- Add a DataFusion session test: open table then run SQL should not repeat bootstrap footer ranges.
- Add a test proving bootstrapped `ETag` reaches scan targets and scan requests send `If-Range`.
- Extend object-storage mocks to expose `ETag`. Add a negative case for missing or hidden `ETag`.
- Add connect-flow coverage proving connect plus first query does not redo descriptor resolution or preflight unless identity or TTL rules require refresh.

### Exit Criteria

- Metrics show duplicate footer/trailer ranges drop to zero for open-plus-query on the same table.
- Scan requests carry identity headers when the provider exposes identity.
- Tests cover `ETag` present, `ETag` missing, object identity drift, and signed URL refresh.
- Live public-S3 evidence records cache reuse and readahead fetched, used, and wasted bytes. **Satisfied on 2026-07-16; the scan-byte budget gate is open.**

---

## Area 2: Parquet Scan Pruning

### Current State

Delta metadata carries partition values and stats into the browser descriptor through:

- `crates/query-contract/src/lib.rs`
- `crates/wasm-delta-snapshot/src/lib.rs`

The legacy runtime can prune files before opening Parquet metadata. The DataFusion browser session currently bootstraps Parquet metadata for all active files before registering the table in `crates/wasm-datafusion-session/src/lib.rs`.

After registration, `crates/wasm-datafusion-poc/src/lib.rs` prunes planned files and row groups. Row-group pruning happens after the engine has opened Parquet metadata in `crates/wasm-parquet-engine/src/lib.rs`.

Parquet metadata loading now preserves explicit column-index and offset-index
policies independently. This resolves the deprecated combined-policy API
without implying that the browser reads fewer bytes.

### Existing Pruning

- Partition pruning handles equality, non-null `IN`, `IS NULL`, `AND`, and same-column `OR` in the DataFusion path.
- Legacy runtime also supports partition `IS NOT NULL`.
- Delta stats pruning handles integer min/max.
- Row-group pruning handles one integer comparison predicate.
- DataFusion marks stats and row-group pruning as inexact, so residual filters remain above the scan.

### Main Gaps

- DataFusion session opens all active file footers before SQL pruning can use partition values or Delta stats.
- DataFusion partition pruning lacks `IS NOT NULL`.
- Partition pruning stringifies literals and uses limited `PartitionColumnType` normalization. Mixed typed and string partition values can miss pruning.
- File and row-group stats do not use null counts for `IS NULL` and `IS NOT NULL`.
- Stats pruning does not cover string, bool, float, decimal, date, timestamp, `BETWEEN`, `NOT IN`, or compound compatible predicates.
- Row-group pruning uses the first compatible integer predicate rather than composing ranges.
- Page-index policy is explicit, but browser byte savings remain unproven.

### Implementation Slices

1. Move DataFusion session toward prebootstrap pruning. Apply partition and Delta stats before `bootstrap_snapshot_metadata`.
2. Fetch Parquet footers only for candidate files that survive prebootstrap pruning.
3. Feed bootstrapped Parquet file stats into DataFusion provider-level pruning.
4. Normalize partition pruning across legacy and DataFusion paths, including `IS NOT NULL`.
5. Add null-count pruning for Delta stats, Parquet file stats, and row-group stats.
6. Compose multiple compatible integer predicates into one range constraint.
7. Preserve explicit Parquet column-index and offset-index policies
   independently. **Status: complete for compatibility; performance evidence
   remains backlog.**

### Correctness Constraints

- Preserve residual filters for any lossy or inexact pruning.
- Fail open when stats are missing, malformed, or type-incompatible.
- Fail open when prebootstrap pruning produces zero candidates until DataFusion can register the
  table from a trusted schema source independent of candidate Parquet footers. Schema-only
  zero-candidate pruning belongs in a later slice gated on Delta metadata or a previously validated
  cached schema.
- Treat mixed typed/string partitions as a correctness problem, not only a performance miss.
- Keep Delta stats pruning and Parquet footer stats pruning separate in metrics.

### Validation Plan

- Add a DataFusion-session regression where Delta stats make a file decisive and the browser does not request that file URL before query execution.
- Add parity tests for partition `IS NOT NULL`, null `IN`, typed integer/string partitions, boolean partitions, casts, and mixed-case partition columns.
- Add fixtures with missing stats, mixed stats completeness, string/bool/nullCount stats, compound ranges, and row-group byte-range proof.
- Split metrics into files pruned before bootstrap, footer reads avoided, files pruned after footer stats, row groups skipped, and data bytes avoided.

### Exit Criteria

- A Delta-stats-decisive predicate avoids footer reads for pruned files in the DataFusion path.
- Legacy and DataFusion partition pruning agree for covered predicates.
- Residual filter tests prove correctness when pruning remains inexact.

---

## Area 3: Delta Snapshot Bootstrap

### Current State

`apps/axon-web/src/services/query.ts` keeps one module-level session per `QueryTableSource`. Same-source queries reuse the session and `ensureTable` opens the Delta table once per session.

The worker keeps a singleton `SandboxQuerySession`; `open_delta_table` and `sql` run as separate commands in `apps/axon-web/src/sandbox-query-worker.ts`.

Rust open-time bootstrap materializes the descriptor, fetches Parquet metadata for all active files, derives a DataFusion schema, registers the table, and stores the bootstrapped snapshot in `crates/wasm-datafusion-session/src/lib.rs`.

The Delta snapshot resolver in `crates/wasm-delta-snapshot/src/lib.rs` lists log paths, selects checkpoints, reads checkpoint/log bytes, replays actions, and derives schema/protocol/capability state per call.

### Main Gaps

- Public object-storage connect resolves the descriptor and preflights the first active file, but catalog storage keeps summary fields. First query rebuilds from the table URI.
- Local Delta reads log facts in TypeScript, then invokes the WASM resolver, which parses log/checkpoint state again.
- `openDeltaTable` caches only table URI and snapshot version at the SDK boundary, not the parsed descriptor or manifest state.
- Manifest snapshot resolution passes `snapshot_version: None`, so it resolves latest for that path.
- Runtime-limit overrides can re-register the DataFusion table and rederive schema even though they reuse the bootstrapped snapshot.

### Implementation Slices

1. Carry the object-store descriptor produced during connect/preflight through E9 `DataAccessResolver` and query-session runtime cache. Do not store openable descriptors, signed URLs, grants, or read resolutions in durable E1 catalog/navigation state.
2. Cache parsed snapshot descriptors in runtime/session scope by source kind, provider, normalized table URI, snapshot version, and log-manifest fingerprint.
3. Split immutable resolved snapshot structure from expiring access envelopes. Refresh access URLs without redoing schema/protocol/capability work when the snapshot identity matches.
4. Collapse Local Delta duplicate log parsing by letting the resolver return the discovery/protocol/schema facts that `readLocalLogFacts` needs.
5. Make pinned snapshot an explicit cache dimension. Do not satisfy pinned version `N` from latest version `N+1`.
6. Cache derived `DeltaTableDescriptor` and schema across budget-driven re-registration, or move budget state out of provider registration.

### Correctness Constraints

- Cache identity needs checkpoint parts, sidecars, replay JSON identities, size, `ETag` or last-modified, and local file metadata where applicable.
- Access grants need their own key: expiry, grant identity, access mode, and refresh policy.
- Durable state may store only non-secret navigation data, locators, and refs. Read resolutions are short-lived and runtime-scoped.
- Same-snapshot refresh must reject drift in table URI, snapshot version, active file identity, schema, and capabilities.
- Explicit latest refresh may reopen the table when version or descriptor material changes.

### Validation Plan

- Add counters around `buildPublicDeltaLogManifest`, `resolve_delta_snapshot_from_manifest`, `bootstrap_snapshot_metadata`, and `bootstrap_file_metadata`.
- Add tests for object-store connect then query reusing descriptor state.
- Add tests for preflight footer reuse and two SQL queries after one open avoiding bootstrap.
- Add tests for runtime-limit query avoiding footer refetch.
- Add tests for pinned version `N` versus latest version `N+1`.
- Add drift tests for same-snapshot refresh.

### Exit Criteria

- Connect plus first query does not repeat Delta log listing and replay for unchanged object-store inputs when the E9/runtime cache still has a valid read resolution.
- Pinned and latest refresh semantics remain explicit.
- Session telemetry distinguishes descriptor cache hits, session reuse, and opened-table reuse.

---

## Area 4: WASM Memory And Arrow IPC Output

### Current State

The shipped app worker uses `BrowserDataFusionSession`. UI queries request 500 visible rows plus a sentinel via `queryResultPageRequest`, and Rust rewrites SQL with `LIMIT/OFFSET`.

The DataFusion scan path streams internally. `AxonParquetScanExec` returns a `SendableRecordBatchStream`, and Parquet uses `ParquetRecordBatchStreamBuilder`.

The private Rust cursor emits exact-sized Arrow IPC schema, data, and
end-of-stream chunks under 1 MiB transport credit. It grows pending encoded
storage lazily, caps one logical batch at 8 MiB, and keeps the separate Daxis
total-output limit at 16 MiB. Preview rows are built while batches are encoded;
exact row counts use `RecordBatch::num_rows()` without revisiting every row.

The worker coordinator stages chunks atomically under the browser-safe 8 MiB
cap and rejects an excess chunk before retaining it. The public SDK receives
and reassembles chunks only after the private child reports a successful
terminal. Coordinator, pending-encoded, and transport-chunk peaks are projected
as query metrics.

### Main Gaps

- The atomic public contract still entails result-sized coordinator staging up
  to its 8 MiB browser-safe cap.
- The public SDK does not expose a progressive cursor.
- UI auto-load stops at 10,000 rows, but manual loading can keep appending pages into one array.
- Legacy row-map runtime still has full-result materialization paths.
- Cursor and coordinator metrics do not bound every DataFusion operator or
  total browser-process memory.

### Implementation Slices

1. Add chunked Arrow IPC worker delivery with transferable chunks or batch
   messages. **Status: complete through the private cursor/coordinator path.**
2. Remove the `.to_vec()` and `Uint8Array::from` double-copy by transferring or
   consuming exact-sized owned chunks. **Status: complete.**
3. Build preview rows while encoding DataFusion batches. **Status: complete.**
4. Set default app query budgets for IPC bytes, result rows, scan bytes, and
   preview string bytes. **Status: complete, including coordinator and cursor
   memory bounds.**
5. Add a hard loaded-row or loaded-byte cap in UI state.
6. Quarantine the legacy row-map runtime from production query paths, or wrap it with strict budgets.

### Correctness Constraints

- Preserve Arrow IPC stream framing and schema delivery across chunks.
- Do not publish partial output through the atomic public SDK. Surface budget,
  cancellation, deadline, and child-fault failures as structured query errors.
- Keep result-page SQL rewriting before expensive execution for UI queries.
- Keep SDK behavior explicit: require `result_page` or apply a default browser-safe budget.

### Validation Plan

- Add browser perf probes for `started`, `executing`, `arrow_ipc_ready`, and `finished`.
- Record IPC byte length, preview rows, bytes fetched, rows emitted, elapsed time, and browser memory where available.
- Add a wide-string fixture that returns 501 rows with large UTF-8 cells.
- Add SDK-level regression for unpaged direct worker queries.
- Run existing budget tests in `wasm-datafusion-poc`.

### Exit Criteria

- The private worker can emit output before the complete result exists.
  **Satisfied.**
- Cursor and coordinator component peaks remain inside their explicit limits,
  and repeated-query browser memory stays within the regression gate.
  **Satisfied for the current fixture; this is not a full-process/operator
  peak claim.**
- Default app and SDK paths enforce output limits without relying on UI
  helpers. **Satisfied.**
- The public SDK remains atomic and exposes no partial output on late failure,
  cancellation, deadline, child fault, or output-budget failure.
  **Satisfied across Chromium, Firefox, and WebKit.**

---

## Area 5: DataFusion Custom Scan Integration

### Current State

`AxonDeltaTableProvider::scan` receives projection, filters, and limit from DataFusion and creates `AxonParquetScanExec` in `crates/wasm-datafusion-poc/src/lib.rs`.

Projection pushdown works through projected schema construction, in-memory `RecordBatch::project`, and Parquet `ProjectionMask`.

Filter pushdown is conservative:

- Exact partition predicates report exact pushdown.
- File-stats and row-group pruning report inexact pushdown.
- Unsupported filters stay above the scan.

Limit pushdown happens only when no inexact filters remain. Ordering does not push down; DataFusion keeps sort and top-k operators above the custom scan.

### Main Gaps

- Exact-only partition pushdown needs plan-shape tests proving no residual `FilterExec` remains above `AxonParquetScanExec`.
- Mixed predicates can re-evaluate exact partition conjuncts above the scan. Example: `partition_col = X AND value > 10` can use the partition part for pruning while DataFusion keeps the full filter above scan.
- Row-group pruning uses one integer predicate.
- `AxonParquetScanTrace` stores planned file paths, filter strings, and projected columns. The engine clones that trace for budget checks, metrics, and display formatting.
- The scan path clones descriptor and file metadata into scan targets.

### Implementation Slices

1. Split lightweight metrics from debug traces. Keep counters in a small struct and make planned file paths explain-only or sampled.
2. Store `DeltaTableDescriptor` and active file metadata behind `Arc`. Pass planned indices instead of cloning file metadata where practical.
3. Add plan-shape tests for exact partition predicates.
4. Add a mixed-predicate residual test. If DataFusion cannot express partial exact pushdown through the current API, document the duplicate evaluation.
5. Extend row-group pruning to compose multiple compatible predicates.

### Correctness Constraints

- Do not mark stats or row-group pruning exact unless the scan proves the predicate without residual evaluation.
- Keep DataFusion 53 API compatibility for `ExecutionPlan::properties()`.
- Preserve explain output, but keep full debug traces out of hot metrics paths.

### Validation Plan

- Keep running:

```bash
cargo test -p wasm-datafusion-poc --test scan_pushdown --test custom_scan_exec --locked
```

- Add plan-shape tests that inspect `FilterExec` placement.
- Add trace-size and clone-count probes for large file lists.
- Add row-group compound predicate tests.

### Exit Criteria

- Exact partition filters avoid residual filters above the custom scan where DataFusion supports it.
- Mixed predicates keep only required residual work or document the API limit.
- Metrics collection no longer clones large planned path lists on the hot path.

---

## Area 6: Bundle And Runtime Startup

### Current State

`apps/axon-web/index.html` loads one Vite entry. `apps/axon-web/src/editor/main.tsx` statically imports both `App` and `ConnectPage`. The default route renders `App`, which imports catalog, query, local Delta, and connect modal code.

On initial mount, `App` calls `loadCatalog(querySource)`. `loadCatalog` calls `getSession()`. For the sample manifest path, `buildSession()` calls `ensureWasm()`, fetches `/fixtures/prod-like/delta-log-manifest.json`, and invokes `resolve_delta_snapshot_from_manifest`. Under ADR-0010, that initial catalog path should become E1 discovery-only and should not force E9 read resolution or `ExecutionProvider` startup.

`createAxonBrowserClient()` creates the worker during session build. The worker imports the same generated WASM glue, then calls `init()` when the first open/query command needs a `SandboxQuerySession`.

Current local artifacts show one large WASM asset around 86 MiB in the app output.

### Main Gaps

- Initial catalog render can load the WASM binary before the user runs a query.
- The browser can instantiate WASM on the main thread for catalog work, then instantiate again in the worker for query work.
- Worker construction happens during session build, not first open/query send.
- Route splitting is absent; `/connect` pays for the editor app module graph.
- The production default catalog uses sample fixture metadata and a fixture manifest path.
- `public/designs/**` and old fixture paths land in `dist` because Vite copies `public/`.

### Implementation Slices

1. Stop loading WASM for initial catalog render. Render E1 discovery/catalog metadata first, and resolve snapshots only when E9 read resolution or explicit refresh needs an openable descriptor.
2. Lazily create the worker inside the SDK client's `send()` path or `ensureTable()`.
3. Move manifest snapshot resolution into the worker, or split metadata/preflight into a small WASM artifact.
4. Add route and modal splitting with dynamic imports for `App`, `ConnectPage`, `ConnectModal`, query services, and WASM services.
5. Move `public/designs/**` and unused old fixtures out of production `public`, or gate them behind a dev-only artifact path.
6. Add artifact-size and network-waterfall gates after `npm run build`.

### Correctness Constraints

- Startup changes must keep catalog metadata visible before query runtime loads.
- Query execution must still converge on `TableRef -> DataAccessResolver -> descriptor/read_access_plan -> ExecutionProvider -> openDeltaTable() -> sql()`.
- Route splitting must preserve worker URL construction under Vite.
- Dev/demo fixtures should not hide production startup costs.

### Validation Plan

Run after implementation:

```bash
cd apps/axon-web
npm run build
find dist/assets -maxdepth 1 -type f -exec stat -f '%z %N' {} + | sort -nr
gzip -c dist/assets/*.wasm | wc -c
brotli -c dist/assets/*.wasm | wc -c
npm run dev:server -- --port 5173 --strictPort
```

Then profile `/` and `/connect` in browser network/performance tools:

- The default catalog view should not fetch or instantiate WASM before query execution or explicit refresh.
- `/connect` should not load the editor query graph unless the route needs it.
- Worker JS should load only when the app opens or queries a table.

### Exit Criteria

- Initial render avoids the 86 MiB WASM fetch.
- Worker construction waits until first open/query action.
- Build artifacts and network waterfall have size gates in CI or release-gate scripts.

---

## Recommended Implementation Sequence

### Step 0: Add Baseline Instrumentation

Add metrics and tests that expose duplicate descriptor, footer, and data-range reads. Do this before optimizing so every later slice can prove its effect.

**Status: complete.**

Useful first tests:

- Connect plus first query should count descriptor resolution once.
- Metadata read plus scan should not repeat trailer/footer reads after caching lands.
- DataFusion session open plus SQL should separate bootstrap footer reads from scan data reads.

### Step 1: Preserve Object Identity And Share Footer Cache

Preserve `ETag` and size from snapshot/bootstrap into scan targets. Add the shared Parquet metadata cache and route inspect, preflight, bootstrap, and scan through it.

This step supports Areas 1, 2, and 3.

**Status: complete.**

### Step 2: Add DataFusion Prebootstrap Pruning

Use partition values and Delta stats before `bootstrap_snapshot_metadata`. Fetch footers only for candidate files.

This step addresses the main network waste in Area 2.

**Status: complete, including typed partition normalization and explicit zero-candidate fail-open behavior.**

### Step 3: Carry Descriptors From Connect To Query

Carry the resolved object-store descriptor and its identity envelope through E9 read resolution and query-session runtime cache. Keep access URL refresh separate from immutable descriptor structure. Do not persist read resolutions or openable descriptors in durable E1 catalog/navigation state.

This step addresses Area 3 and strengthens Area 1.

**Status: complete.**

### Step 4: Stream IPC And Set Browser Budgets

Use the private cursor/coordinator to emit exact-sized credit-bounded IPC
chunks, build preview during encoding, and enforce separate pending-batch,
coordinator-staging, total-output, row, scan, and speculative-overfetch
budgets. Keep the public SDK atomic by committing staged chunks only after a
successful terminal.

This step addresses Area 4.

**Status: complete for private chunked IPC, exact-sized transferable buffers,
authoritative cancellation/recovery, atomic public delivery, browser-safe
defaults, and projected component peaks. Progressive public delivery remains
out of scope.**

### Step 5: Lazy-Load Startup Paths

Remove WASM and worker construction from initial catalog render. Split routes and query runtime imports.

This step addresses Area 6.

**Status: complete for lazy startup query paths.**

### Step 6: Reduce Custom Scan Trace Cost

Split metrics from debug traces, reduce descriptor/file clones, and add plan-shape tests for exact and mixed predicates.

This step addresses Area 5.

**Status: complete for scan metrics/debug trace split. Further clone reductions are backlog only if profiling justifies them.**

### Step 7: Coalesce Small-Gap Parquet Range Batches

Coalesce planned Parquet `get_byte_ranges` batches only under strong object identity, strict `If-Range` validation, and bounded overfetch policy.

**Status: implemented in the current slice.**

---

## Suggested Ticket Breakdown

1. `perf(range): instrument duplicate descriptor/footer/range reads`
2. `perf(range): preserve object identity through DataFusion scan targets`
3. `perf(range): share Parquet footer metadata across preflight/bootstrap/scan`
4. `perf(pruning): prebootstrap prune DataFusion active files from Delta stats`
5. `perf(pruning): normalize partition pruning across DataFusion and legacy runtime`
6. `perf(snapshot): carry public object-store descriptors through E9/query-session runtime cache`
7. `perf(ipc): stream Arrow IPC chunks from worker`
8. `perf(ipc): add browser default output budgets`
9. `perf(startup): avoid WASM load during initial catalog render`
10. `perf(startup): lazy-create query worker and split routes`
11. `perf(scan): split custom scan metrics from debug trace`
12. `perf(range): coalesce small-gap Parquet range batches`

Each ticket should include one failing-first test or profiling assertion, the code change, and one command that proves the target behavior changed.
