# Conformance Tests

This directory holds checks that keep browser and native behavior aligned, plus native-only corpora for oracle correctness, execution metrics, pruning coverage, and historical snapshot reads.

Current contents:

- `verify_workspace_layout.sh`: verifies the EPIC-01 scaffold exists before feature work begins.
- `native-runtime-sql-corpus.json`: 12-case unpartitioned latest-snapshot SQL corpus with golden result tables and an explicit `assert_scan_metrics` contract so scan metrics are only asserted where they are stable.
- `native-runtime-partitioned-sql-corpus.json`: 10-case partitioned latest-snapshot SQL corpus with golden results and an explicit `assert_scan_metrics` contract for pruning-visible metric assertions.
- `native-runtime-snapshot-version-sql-corpus.json`: 4-case historical snapshot-version SQL corpus for the local multi-version fixture.
- `browser-execution-plan-corpus.json`: browser execution-plan lowering corpus over synthetic bootstrapped snapshots, covering passthrough output columns, aggregate measures, grouped output, and output-aligned `ORDER BY` / `LIMIT`.

Deterministic offline negative-path coverage for invalid table locations, unavailable snapshots, missing local data files, and Unix permission-denied local data files lives in `crates/native-query-runtime/tests/native_runtime.rs`.
Env-gated real-GCS smokes, including the Sprint 4 negative cases for `403`, `404`, stale history, and missing objects, live in the same file.
Sprint 8 adds deterministic local HTTP range-read coverage in `crates/wasm-http-object-store/tests/http_range_reader.rs`, including footer-style, bounded, offset, and suffix reads plus `401`, `403`, `404`, `416`, and malformed partial-response handling.
Sprint 9 adds deterministic local browser-runtime envelope coverage in `crates/wasm-query-runtime/tests/browser_runtime.rs`, including constrained config validation, typed object-source construction, explicit unsupported-mode failures, loopback-only host-side HTTP probe handoff through the injected range-reader path, request-timeout enforcement for runtime-owned readers, supported browser query-shape analysis, snapshot-bound planning, partition-value pruning, integer footer-stat pruning, and browser execution-plan lowering driven by `browser-execution-plan-corpus.json`. `crates/wasm-query-runtime/tests/wasm_smoke.rs` adds a `wasm32-unknown-unknown` execution smoke for session, source, query-shape, planning, and execution-plan construction.
The partitioned native SQL corpus now doubles as the local pruning oracle for cross-crate browser-planning parity checks in `crates/delta-control-plane/tests/browser_snapshot_preflight.rs`, where resolved snapshots are served over loopback HTTP, bootstrapped into browser metadata, and compared against native `files_touched` / `files_skipped` metrics for curated full-scan, partition-pruned, no-match, and integer-stats-pruned cases.
That same cross-crate suite now also carries a supported-browser SQL parity corpus over the shared native/browser envelope, asserts the typed execution-plan shape for those curated cases, and keeps explicit tests for intentional native-only divergences such as wildcard projections and set operations, so semantic drift is visible before browser execution work lands.
Fixture provisioning and IAM setup for those env-gated GCS paths are external to this repository.
