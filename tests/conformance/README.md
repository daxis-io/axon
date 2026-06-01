# Conformance Tests

This directory holds checks that keep browser and native behavior aligned, plus native-only corpora for oracle correctness, execution metrics, pruning coverage, and historical snapshot reads.

Current contents:

- `verify_workspace_layout.sh`: verifies the EPIC-01 scaffold exists before feature work begins.
- `verify_browser_worker_dependency_boundary.sh`: verifies the legacy narrow worker and session shell cannot route through the browser DataFusion POC/session path, even under workspace feature resolution.
- `verify_axon_web_datafusion_runtime.sh`: verifies the Daxis-facing `axon-web-wasm` runtime uses `wasm-datafusion-session` and does not depend on the legacy narrow session.
- `verify_embedding_sketch_contract.sh`: verifies the minimal embedding sketch uses the SDK client contract rather than obsolete raw worker messages.
- `verify_patch_inventory_state.sh`: verifies the patch inventory is real state, not a template, and that vendoring or `[patch]` sections cannot appear without an inventory entry.
- `verify_daxis_operational_readiness.sh`: verifies the M4 Daxis operational-readiness handoff names dashboard, runbook, rollout-control, compatibility-dashboard, and release-evidence requirements without claiming production systems exist in this repo.
- `verify_daxis_operational_readiness_test.sh`: regression coverage for the operational-readiness verifier.
- `verify_daxis_release_evidence.sh`: runs or lists the Daxis release evidence gate set for repeatable go/no-go checks.
- `verify_daxis_release_evidence_test.sh`: regression coverage for the Daxis release evidence runner.
- `verify_daxis_rollout_decisions.sh`: verifies the Daxis production rollout decision register for endpoint names, access-mode policy, TTL/refresh posture, budgets, contract versioning, channels, table eligibility, and dashboard thresholds.
- `verify_daxis_rollout_decisions_test.sh`: regression coverage for the rollout-decision verifier.
- `verify_daxis_strategy_traceability.sh`: verifies that Daxis strategy M0-M4 deliverables and exit criteria map to current repo evidence or explicit external Daxis proof.
- `verify_daxis_external_proof_packet.sh`: verifies the Daxis external proof packet names required production proof items, Daxis owners, acceptance checks, rollback evidence, and Axon references.
- `verify_daxis_external_proof_packet_test.sh`: regression coverage for the external proof packet verifier.
- `verify_daxis_contract_artifacts.sh`: verifies the Daxis SDK examples, schemas, OpenAPI document, and JSON fixtures against the checked-in SHA-256 manifest, and rejects browser-visible raw credential fields in those artifacts.
- `verify_daxis_contract_artifacts_test.sh`: regression coverage for the Daxis contract artifact verifier.
- `verify_daxis_architecture_adr.sh`: verifies the Daxis browser read-compute architectural decision is captured as an indexed ADR and linked to the strategy, rollout, traceability, and external-proof artifacts.
- `verify_daxis_architecture_adr_test.sh`: regression coverage for the Daxis architecture ADR verifier.
- `verify_daxis_release_bundle_manifest.sh`: verifies that the Daxis release bundle covers commit identity, contract hashes, Rust and TypeScript summaries, WASM checks, browser matrix results, artifact size, dependency guardrails, fallback reasons, compatibility notes, migration notes, and external blockers. It also keeps migration notes as release-process evidence and requires the release attachment to use `docs/release-gates/daxis-release-migration-notes-template.md`.
- `verify_daxis_release_bundle_manifest_test.sh`: regression coverage for the Daxis release bundle manifest verifier.
- `verify_daxis_pr_checklist.sh`: verifies the checked-in pull request template carries the Daxis-relevant trust-boundary, contract, fallback, parity, dependency, WASM, size, browser-matrix, docs, and release-evidence checklist.
- `verify_daxis_pr_checklist_test.sh`: regression coverage for the Daxis PR checklist verifier.
- `../docs/program/daxis-first-class-integration-examples/approved-axon-read-descriptor.saved-query.json`: first executable Daxis-approved saved SQL descriptor for the headless gateway-to-Axon handoff, checked by `crates/query-contract/tests/release_handoff_examples.rs`.
- `../docs/program/daxis-first-class-integration-examples/query-result.*.json`: result-envelope fixtures for agent, dashboard tile, builder, saved-query, API, policy-denied, unsupported-SQL, and runtime-budget fallback cases.
- `daxis-browser-datafusion-query-corpus.json`: Daxis-shaped browser DataFusion corpus for order exploration queries, exercised through both in-memory and descriptor-backed `AxonParquetScanExec` paths by `crates/wasm-datafusion-poc/tests/daxis_query_corpus.rs`.
- `../docs/release-gates/daxis-browser-datafusion-budget-profile.json`: M3 Daxis browser DataFusion default-worker budget profile, checked by `crates/wasm-datafusion-poc/tests/daxis_budget_profile.rs`.
- `../docs/release-gates/daxis-browser-runtime-isolation-plan.json`: M3 Daxis browser runtime isolation and removal plan, checked by `crates/wasm-datafusion-poc/tests/daxis_runtime_isolation_plan.rs`.
- `native-runtime-sql-corpus.json`: 12-case unpartitioned latest-snapshot SQL corpus with golden result tables and an explicit `assert_scan_metrics` contract so scan metrics are only asserted where they are stable.
- `native-runtime-partitioned-sql-corpus.json`: 10-case partitioned latest-snapshot SQL corpus with golden results and an explicit `assert_scan_metrics` contract for pruning-visible metric assertions.
- `native-runtime-snapshot-version-sql-corpus.json`: 4-case historical snapshot-version SQL corpus for the local multi-version fixture.
- `browser-execution-plan-corpus.json`: browser execution-plan lowering corpus over synthetic bootstrapped snapshots, covering typed lowered filters, required scan columns, passthrough output columns, grouped output, output-aligned `ORDER BY` / `LIMIT`, and the currently supported aliased browser aggregate measures: `AVG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`, `COUNT`, `SUM`, `MIN`, and `MAX`.

Deterministic offline negative-path coverage for invalid table locations, unavailable snapshots, missing local data files, and Unix permission-denied local data files lives in `crates/native-query-runtime/tests/native_runtime.rs`.
Env-gated real-GCS smokes, including the Sprint 4 negative cases for `403`, `404`, stale history, and missing objects, live in the same file.
Sprint 8 adds deterministic local HTTP range-read coverage in `crates/wasm-http-object-store/tests/http_range_reader.rs`, including footer-style, bounded, offset, and suffix reads plus `401`, `403`, `404`, `416`, and malformed partial-response handling.
Sprint 9 adds deterministic local browser-runtime envelope coverage in `crates/wasm-query-runtime/tests/browser_runtime.rs`, including constrained config validation, typed object-source construction, explicit unsupported-mode failures, loopback-only host-side HTTP probe handoff through the injected range-reader path, request-timeout enforcement for runtime-owned readers, supported browser query-shape analysis, snapshot-bound planning, partition-value pruning, integer footer-stat pruning, browser execution-plan lowering driven by `browser-execution-plan-corpus.json`, fail-fast execution handoff validation, and real host-side browser execution over loopback-served Parquet files. That execution-plan layer currently lowers aliased `AVG`, `ARRAY_AGG`, `BOOL_AND`, `BOOL_OR`, `COUNT`, `SUM`, `MIN`, and `MAX`, executes the current curated non-aggregate plus narrow grouped/ungrouped aggregate subset, and routes the deferred aggregate functions to native fallback instead of broadening the browser claim. `crates/wasm-query-runtime/tests/wasm_smoke.rs` adds a `wasm32-unknown-unknown` execution smoke for session, source, query-shape, planning, execution-plan construction, and execute-plan future construction, while `crates/browser-sdk/tests/wasm_smoke.rs`, `crates/wasm-parquet-engine/tests/wasm_smoke.rs`, and `crates/wasm-delta-snapshot/tests/wasm_smoke.rs` keep the browser worker envelope, Parquet primitives, and Delta snapshot replay surfaces live under real wasm32 execution.
The partitioned native SQL corpus now doubles as the local pruning oracle for cross-crate browser-planning parity checks in `crates/delta-control-plane/tests/browser_snapshot_preflight.rs`, where resolved snapshots are served over loopback HTTP, bootstrapped into browser metadata, and compared against native `files_touched` / `files_skipped` metrics for curated full-scan, partition-pruned, no-match, and integer-stats-pruned cases.
That same cross-crate suite now also carries a supported-browser SQL parity corpus over the shared native/browser envelope, asserts the typed execution-plan shape for those curated cases, executes the curated non-aggregate and aggregate browser cases against the real loopback fixture with normalized native-result parity, and keeps explicit tests for intentional native-only divergences such as wildcard projections and set operations, so semantic drift is visible before broader browser execution claims are made.
Fixture provisioning and IAM setup for those env-gated GCS paths are external to this repository.

Useful local commands:

- `bash tests/conformance/verify_workspace_layout.sh`
- `bash tests/conformance/verify_browser_worker_dependency_boundary.sh`
- `bash tests/conformance/verify_axon_web_datafusion_runtime.sh`
- `bash tests/conformance/verify_embedding_sketch_contract.sh`
- `bash tests/conformance/verify_patch_inventory_state.sh`
- `bash tests/conformance/verify_patch_inventory_state_test.sh`
- `bash tests/conformance/verify_daxis_operational_readiness_test.sh`
- `bash tests/conformance/verify_daxis_operational_readiness.sh`
- `bash tests/conformance/verify_daxis_release_evidence_test.sh`
- `bash tests/conformance/verify_daxis_release_evidence.sh --list`
- `bash tests/conformance/verify_daxis_rollout_decisions_test.sh`
- `bash tests/conformance/verify_daxis_rollout_decisions.sh`
- `bash tests/conformance/verify_daxis_strategy_traceability.sh`
- `bash tests/conformance/verify_daxis_contract_artifacts_test.sh`
- `bash tests/conformance/verify_daxis_contract_artifacts.sh`
- `bash tests/conformance/verify_daxis_external_proof_packet_test.sh`
- `bash tests/conformance/verify_daxis_external_proof_packet.sh`
- `bash tests/conformance/verify_daxis_architecture_adr_test.sh`
- `bash tests/conformance/verify_daxis_architecture_adr.sh`
- `bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh`
- `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`
- `bash tests/conformance/verify_daxis_pr_checklist_test.sh`
- `bash tests/conformance/verify_daxis_pr_checklist.sh`
- `cargo test -p wasm-datafusion-poc --test daxis_query_corpus`
- `cargo test -p wasm-datafusion-poc --test daxis_budget_profile`
- `cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan`
