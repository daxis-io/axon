# Conformance Tests

This directory holds checks that keep browser and native behavior aligned, plus native-only corpora for oracle correctness, execution metrics, pruning coverage, and historical snapshot reads.

Current contents:

- `verify_workspace_layout.sh`: verifies the EPIC-01 scaffold exists before feature work begins.
- `verify_browser_worker_dependency_boundary.sh`: verifies the legacy narrow worker and session shell cannot route through the browser DataFusion POC/session path, even under workspace feature resolution.
- `verify_browser_observability_contract.sh`: verifies the browser observability contract keeps repo-owned metrics, worker event fields, release artifact signals, dashboard inputs, alert candidates, Daxis handoff context, and explicit non-claims aligned with the Daxis release evidence boundary.
- `verify_browser_observability_contract_test.sh`: regression coverage for the browser observability contract verifier.
- `verify_axon_web_datafusion_runtime.sh`: verifies the Daxis-facing `axon-web-wasm` runtime uses `wasm-datafusion-session` and does not depend on the legacy narrow session.
- `verify_daxis_query_corpus_coverage.sh`: verifies the Daxis browser DataFusion query corpus declares covered SQL classes and the runtime parity document carries matching compatibility statements.
- `verify_daxis_query_corpus_coverage_test.sh`: regression coverage for the query-corpus compatibility coverage verifier.
- `verify_embedding_sketch_contract.sh`: verifies the minimal embedding sketch uses the SDK client contract rather than obsolete raw worker messages.
- `verify_patch_inventory_state.sh`: verifies the patch inventory is real state, not a template, and that vendoring or `[patch]` sections cannot appear without an inventory entry.
- `verify_upstream_wasm_fork_stack.sh`: verifies the Daxis upstream-WASM POC repository revisions, rejects mutable branch dependencies, compares fork revisions with the nested browser `Cargo.lock`, and checks the target-filtered normal/build graph for duplicate Arrow, Parquet, `object_store`, DataFusion, or Kernel package sources. `--bootstrap` (also accepted as `--allow-unset`) permits unfinished leaf revisions and a missing browser lock; the default final mode rejects both.
- `verify_upstream_wasm_fork_stack_test.sh`: regression coverage for stack-lock shape, revision reachability, bootstrap/final mode separation, immutable dependency pins, Cargo lock provenance, and duplicate guarded package sources.
- `verify_daxis_operational_readiness.sh`: verifies the M4 Daxis operational-readiness handoff names dashboard, runbook, rollout-control, compatibility-dashboard, and release-evidence requirements without claiming production systems exist in this repo. It also requires unique source docs, dashboard names, runbook names, and release automation commands, rejects release automation commands that are not listed by the release-evidence runner, keeps the operational-readiness verifier in the automation set, and requires the ADR, operational maturity, observability, release runbook, strategy, external handoff, rollout decisions, external proof packet, release bundle, release evidence, and external blocker source docs.
- `verify_daxis_operational_readiness_test.sh`: regression coverage for the operational-readiness verifier.
- `verify_daxis_release_evidence.sh`: runs or lists the Daxis release evidence gate set for repeatable go/no-go checks, including Daxis verifier regression scripts, the Daxis default-worker dependency and bundle secret-marker guardrail after the WASM artifact is built, and the skip-safe public GCS live smoke.
- `verify_daxis_release_evidence_test.sh`: regression coverage for the Daxis release evidence runner.
- `verify_daxis_rollout_decisions.sh`: verifies the Daxis production rollout decision register for endpoint names, explicit fallback/block modes, TTL/refresh posture, budgets, contract versioning, safe release promotion, stable-default alignment with the external proof packet and release bundle manifest, table eligibility, external signoff owners, and dashboard thresholds. It also requires unique source docs and the ADR, strategy, operational-maturity, DataFusion budget profile, contract artifact manifest, external proof packet, operational readiness, and release bundle source docs.
- `verify_daxis_rollout_decisions_test.sh`: regression coverage for the rollout-decision verifier.
- `verify_daxis_strategy_document.sh`: verifies the Daxis first-class integration strategy document keeps its required sections, ownership boundaries, fallback vocabulary, linked release artifacts, and release-evidence command references aligned with the Daxis release gates.
- `verify_daxis_strategy_document_test.sh`: regression coverage for the strategy document verifier.
- `verify_daxis_strategy_traceability.sh`: verifies that Daxis strategy M0-M4 deliverables and exit criteria map to current repo evidence or explicit external Daxis proof, keeps milestone item IDs and release gates unique, rejects unsupported release gates, keeps the M0 default-browser-engine proof item explicit, and keeps the matrix tied to full release evidence, the architecture ADR gate, and the external proof packet.
- `verify_daxis_strategy_traceability_test.sh`: regression coverage for the strategy traceability verifier.
- `verify_daxis_external_state.sh`: optional helper for Daxis owners to run the external proof packet's current-state checks against a Daxis checkout through `AXON_DAXIS_PLATFORM_REPO_ROOT`; it lists and runs Daxis commit SHA, branch/ref, origin remote URL, working-tree status capture, the Daxis query contract test, and the active architecture terminology scan without treating those checks as accepted production proof.
- `verify_daxis_external_state_test.sh`: regression coverage for the Daxis external-state helper.
- `verify_daxis_external_proof_attachment.sh`: verifies completed Daxis external proof attachments before stable-default promotion; in local artifact mode it checks helper JSON bytes, helper-result identity, clean or digest-pinned dirty worktree review evidence, production environment class, stable release channel, and required reviewer metadata.
- `verify_daxis_external_proof_attachment_test.sh`: regression coverage for the Daxis external proof attachment verifier.
- `verify_daxis_external_proof_packet.sh`: verifies the Daxis external proof packet names the required ADR, strategy, rollout, readiness, release-bundle, traceability, blocker, handoff, and attachment-template source docs once; names each required production proof item once; keeps Daxis owners aligned with the traceability matrix; and checks unique verification-plan paths, exact supported verification-plan commands, acceptance checks, rollback evidence, Axon references, the proof attachment template, handoff checklist, and the stable-default promotion gate tying accepted external proof to release-process attachments.
- `verify_daxis_external_proof_packet_test.sh`: regression coverage for the external proof packet verifier.
- `verify_daxis_contract_artifacts.sh`: verifies the Daxis SDK examples, schemas, OpenAPI document, and JSON fixtures against the checked-in SHA-256 manifest, and rejects browser-visible raw credential fields in those artifacts.
- `verify_daxis_contract_artifacts_test.sh`: regression coverage for the Daxis contract artifact verifier.
- `verify_daxis_architecture_adr.sh`: verifies the Daxis browser read-compute architectural decision is captured as an indexed ADR and linked to the strategy, rollout, traceability, and external-proof artifacts.
- `verify_daxis_architecture_adr_test.sh`: regression coverage for the Daxis architecture ADR verifier.
- `verify_daxis_release_attachment.sh`: verifies completed Daxis release-process attachments before stable-default promotion; in local artifact mode it checks evidence artifact bytes, stable release channel, production rollout segment, required reviewer metadata, and attachment item coverage.
- `verify_daxis_release_attachment_test.sh`: regression coverage for the Daxis release attachment verifier.
- `verify_daxis_stable_default_promotion_packet.sh`: verifies a complete stable-default promotion packet by checking release evidence log status and digest, listed release-evidence commands, required release-evidence markers, release and proof attachment artifact validation, and a single shared release commit, release ref, release channel, and rollout segment across the packet.
- `verify_daxis_stable_default_promotion_packet_test.sh`: regression coverage for the stable-default promotion packet verifier.
- `verify_daxis_release_bundle_manifest.sh`: verifies that the Daxis release bundle covers commit identity, contract hashes, Rust and TypeScript summaries, WASM checks, browser matrix results, public GCS live smoke, artifact size, dependency guardrails, fallback reasons, compatibility notes, migration notes, and external blockers. It also enforces unique bundle item IDs, unique top-level source docs and release evidence commands, the required strategy, parity, compatibility, ADR, rollout, readiness, traceability, external-proof, release-evidence, blocker, and attachment-template source docs; keeps release-process attachments tied to `docs/release-gates/daxis-release-attachment-template.md`; requires migration notes to use `docs/release-gates/daxis-release-migration-notes-template.md`; and rejects unsupported top-level or item-level release evidence commands.
- `verify_daxis_release_bundle_manifest_test.sh`: regression coverage for the Daxis release bundle manifest verifier.
- `verify_daxis_pr_checklist.sh`: verifies the checked-in pull request template carries the Daxis-relevant trust-boundary, contract, fallback, parity, dependency, WASM, size, browser-matrix, release-attachment, external-proof, stable-default, rollout-promotion, docs, and release-evidence checklist; keeps referenced release and external-proof attachment templates populated with the checklist-backed item IDs; and requires the external proof packet stable-default gate to preserve accepted review, release-evidence, and server-fallback requirements.
- `verify_daxis_pr_checklist_test.sh`: regression coverage for the Daxis PR checklist verifier.
- `../docs/integrations/daxis/daxis-first-class-integration-examples/approved-axon-read-descriptor.saved-query.json`: first executable Daxis-approved saved SQL descriptor for the headless gateway-to-Axon handoff, checked by `crates/query-contract/tests/release_handoff_examples.rs`.
- `../docs/integrations/daxis/daxis-first-class-integration-examples/query-result.*.json`: result-envelope fixtures for agent, dashboard tile, builder, saved-query, API, policy-denied, unsupported-SQL, and runtime-budget fallback cases.
- `daxis-browser-datafusion-query-corpus.json`: Daxis-shaped browser DataFusion corpus for order exploration queries, exercised through both in-memory and descriptor-backed `AxonParquetScanExec` paths by `crates/wasm-datafusion-poc/tests/daxis_query_corpus.rs` and checked for compatibility-claim coverage by `verify_daxis_query_corpus_coverage.sh`.
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
- `bash tests/conformance/verify_browser_observability_contract_test.sh`
- `bash tests/conformance/verify_browser_observability_contract.sh`
- `bash tests/conformance/verify_axon_web_datafusion_runtime.sh`
- `bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh`
- `bash tests/conformance/verify_daxis_query_corpus_coverage.sh`
- `bash tests/conformance/verify_embedding_sketch_contract.sh`
- `bash tests/conformance/verify_patch_inventory_state.sh`
- `bash tests/conformance/verify_patch_inventory_state_test.sh`
- `bash tests/conformance/verify_upstream_wasm_fork_stack_test.sh`
- `bash tests/conformance/verify_upstream_wasm_fork_stack.sh --bootstrap`
- `bash tests/conformance/verify_upstream_wasm_fork_stack.sh`
- `bash tests/conformance/verify_daxis_operational_readiness_test.sh`
- `bash tests/conformance/verify_daxis_operational_readiness.sh`
- `bash tests/conformance/verify_daxis_release_evidence_test.sh`
- `bash tests/conformance/verify_daxis_release_evidence.sh --list`
- `bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`
- `bash tests/conformance/verify_daxis_rollout_decisions_test.sh`
- `bash tests/conformance/verify_daxis_rollout_decisions.sh`
- `bash tests/conformance/verify_daxis_strategy_document_test.sh`
- `bash tests/conformance/verify_daxis_strategy_document.sh`
- `bash tests/conformance/verify_daxis_strategy_traceability_test.sh`
- `bash tests/conformance/verify_daxis_strategy_traceability.sh`
- `bash tests/conformance/verify_daxis_external_state_test.sh`
- `AXON_DAXIS_PLATFORM_REPO_ROOT=/path/to/daxis-platform bash tests/conformance/verify_daxis_external_state.sh`
- `bash tests/conformance/verify_daxis_external_proof_attachment_test.sh`
- `bash tests/conformance/verify_daxis_contract_artifacts_test.sh`
- `bash tests/conformance/verify_daxis_contract_artifacts.sh`
- `bash tests/conformance/verify_daxis_external_proof_packet_test.sh`
- `bash tests/conformance/verify_daxis_external_proof_packet.sh`
- `bash tests/conformance/verify_daxis_architecture_adr_test.sh`
- `bash tests/conformance/verify_daxis_architecture_adr.sh`
- `bash tests/conformance/verify_daxis_release_attachment_test.sh`
- `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md`
- `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments`
- `bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md`
- `bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments`
- `bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`
- `bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh`
- `bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh`
- `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`
- `bash tests/conformance/verify_daxis_pr_checklist_test.sh`
- `bash tests/conformance/verify_daxis_pr_checklist.sh`
- `cargo test -p wasm-datafusion-poc --test daxis_query_corpus`
- `cargo test -p wasm-datafusion-poc --test daxis_budget_profile`
- `cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan`
