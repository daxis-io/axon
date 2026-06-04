# Browser WASM + Delta on GCS Release Evidence

- Date: 2026-03-31
- Planning baseline commit: `0e19f1d`

This evidence pack maps repository-owned release claims to commands and artifacts that exist in the repo. It separates the original planning baseline from later validation snapshots so external service delivery is not inferred from local runtime evidence.

## Baseline Notes

- Use git commit `0e19f1d` as the evidence baseline for this bundle.
- Do not infer service delivery from this document; `services/query-api` is still absent from the repo.
- The latest local validation snapshot, reproduced on 2026-05-30 and extended for Daxis M3, keeps public object-storage table-root access as the immediate repo-owned release-candidate path, now with live fixture coverage against the repo-documented public GCS table. The Daxis-facing app worker reports `browser_datafusion` as its default runtime SKU; artifact size and production rollout approval remain release-process and Daxis-owned evidence.

## Repo-Owned Evidence Matrix

| Claim                                                                                                             | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Browser and native crates build on host and wasm targets                                                          | `.github/workflows/ci.yml` and `.github/workflows/upgrade-rehearsal.yml`                                                                                                                                                                                                                                                                                                                                                     |
| Upgrade rehearsal preserves dependency drift evidence                                                             | uploaded artifact `upgrade-rehearsal-cargo-lock-diff` from `.github/workflows/upgrade-rehearsal.yml`                                                                                                                                                                                                                                                                                                                         |
| Worker artifact size is budgeted                                                                                  | `bash tests/perf/report_browser_worker_artifact.sh`                                                                                                                                                                                                                                                                                                                                                                          |
| Worker startup and memory baselines are emitted                                                                   | `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture` and `cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture`                                                                                                                                                                                                                |
| Browser dependency and artifact guardrails are enforced                                                           | `bash tests/security/verify_browser_dependency_guardrails.sh`                                                                                                                                                                                                                                                                                                                                                                |
| Patch inventory state is verified                                                                                 | `bash tests/conformance/verify_patch_inventory_state.sh`                                                                                                                                                                                                                                                                                                                                                                     |
| The control-plane and browser seam is documented with canonical examples                                          | `docs/program/browser-lakehouse-release-handoff.md` and `docs/program/browser-lakehouse-release-handoff-examples/`                                                                                                                                                                                                                                                                                                           |
| Supported browser queries keep native parity and structured fallback routing                                      | `cargo test -p delta-control-plane --locked` and `cargo test -p wasm-query-runtime -p query-router --locked`                                                                                                                                                                                                                                                                                                                 |
| The legacy in-memory session shell and compatibility worker contract stay repo-owned and testable                 | `cargo test -p wasm-query-session --locked` and `cargo test -p browser-sdk -p browser-engine-worker --locked`                                                                                                                                                                                                                                                                                                                |
| Public object-storage table-root access remains a browser-owned product path                                      | `npm run test:sdk`, `PLAYWRIGHT_BASE_URL=https://127.0.0.1:5173 npm run test:browser:editor-smoke -- --grep "object storage\|connect source flows"`, and env-gated live fixture smoke with `AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta PLAYWRIGHT_BASE_URL=https://127.0.0.1:5173 npm run test:browser:public-gcs-live`                                                  |
| The app WASM DataFusion runtime is the Daxis-facing default worker SKU                                            | `cargo test -p axon-web-wasm`, `cargo test -p wasm-datafusion-poc`, `cargo test -p wasm-datafusion-session`, `docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json`, and `bash tests/conformance/verify_axon_web_datafusion_runtime.sh`                                                                                                                                    |
| Daxis first-class integration contracts stay fixture-backed and hash-pinned                                       | `docs/program/daxis-first-class-integration-examples/`, `docs/release-gates/daxis-contract-artifacts.sha256`, `cargo test -p query-contract`, and `bash tests/conformance/verify_daxis_contract_artifacts.sh`                                                                                                                                                                                                                |
| Daxis rollout, operations, traceability, external proof, architecture, and release-bundle decisions stay explicit | `docs/release-gates/daxis-production-rollout-decisions.json`, `docs/release-gates/daxis-operational-readiness.json`, `docs/release-gates/daxis-strategy-traceability.json`, `docs/release-gates/daxis-external-proof-packet.json`, `docs/release-gates/daxis-release-bundle-manifest.json`, `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md`, and `bash tests/conformance/verify_daxis_release_evidence.sh --list` |

## Daxis Release Bundle Boundary

The machine-readable Daxis release bundle is
[`docs/release-gates/daxis-release-bundle-manifest.json`](./daxis-release-bundle-manifest.json),
checked by `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`.
Run `bash tests/conformance/verify_daxis_release_evidence.sh` before Daxis
release review and attach the captured output to the release packet.

The manifest keeps five items as `release_process_required` because they must
be captured for the exact release commit and rollout segment:

- `git_sha`
- `worker_artifact_size`
- `public_gcs_live_smoke`
- `release_notes`
- `migration_notes`

Use
[`docs/release-gates/daxis-release-attachment-template.md`](./daxis-release-attachment-template.md)
for those attachments. Use
[`docs/release-gates/daxis-release-notes-template.md`](./daxis-release-notes-template.md)
for Daxis-facing release notes covering query-result semantics, Daxis result
metrics and observability fields, fallback behavior, supported SQL and Delta
feature claims, descriptor validation, public error taxonomy,
external proof packet status, and `currentPromotionState`.
Use
[`docs/release-gates/daxis-release-migration-notes-template.md`](./daxis-release-migration-notes-template.md)
for Daxis-facing breaking-change plans or no-breaking-change statements. The
`public_gcs_live_smoke` item requires live output
from:

```bash
AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://... npm run test:browser:public-gcs-live -- --reporter=line
```

If the live fixture is not configured, attach the skip-safe output plus the
external blocker record when `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is absent.

Stable default routing remains blocked while the external proof packet's
`stableDefaultPromotionGate` reports `currentPromotionState:
blocked_external_proof_required`. That state remains in force until the gate has
accepted release-process attachments, accepted Daxis-owned external proof, full
release-evidence output, and `server_fallback` rollback evidence.

## Verification Commands

```bash
cargo check --workspace --locked
cargo fmt --check
cargo test -p query-contract --locked
cargo test -p wasm-datafusion-poc --test daxis_query_corpus
bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh
bash tests/conformance/verify_daxis_query_corpus_coverage.sh
cargo test -p delta-runtime-support --locked
cargo test -p delta-control-plane --locked
cargo test -p native-query-runtime --locked
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-delta-snapshot --locked
cargo test -p wasm-query-runtime -p query-router --locked
cargo test -p wasm-query-session --locked
cargo test -p wasm-datafusion-session --locked
cargo test -p axon-web-wasm --locked
cargo test -p browser-sdk -p browser-engine-worker --locked
cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-sdk --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-parquet-engine --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
cargo check -p wasm-query-session --target wasm32-unknown-unknown --locked
bash tests/perf/report_browser_worker_artifact.sh
bash tests/perf/report_datafusion_wasm_size_test.sh
bash tests/security/verify_browser_dependency_guardrails_test.sh
bash tests/security/verify_browser_dependency_guardrails.sh
env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
bash tests/conformance/verify_patch_inventory_state_test.sh
bash tests/conformance/verify_patch_inventory_state.sh
bash tests/conformance/verify_daxis_contract_artifacts_test.sh
bash tests/conformance/verify_daxis_contract_artifacts.sh
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/conformance/verify_browser_observability_contract_test.sh
bash tests/conformance/verify_browser_observability_contract.sh
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
bash tests/conformance/verify_daxis_strategy_document_test.sh
bash tests/conformance/verify_daxis_strategy_document.sh
bash tests/conformance/verify_daxis_rollout_decisions_test.sh
bash tests/conformance/verify_daxis_rollout_decisions.sh
bash tests/conformance/verify_daxis_operational_readiness_test.sh
bash tests/conformance/verify_daxis_operational_readiness.sh
bash tests/conformance/verify_daxis_strategy_traceability_test.sh
bash tests/conformance/verify_daxis_strategy_traceability.sh
bash tests/conformance/verify_daxis_external_state_test.sh
bash tests/conformance/verify_daxis_external_proof_packet_test.sh
bash tests/conformance/verify_daxis_external_proof_packet.sh
bash tests/conformance/verify_daxis_architecture_adr_test.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh
bash tests/conformance/verify_daxis_release_bundle_manifest.sh
bash tests/conformance/verify_daxis_pr_checklist_test.sh
bash tests/conformance/verify_daxis_pr_checklist.sh
bash tests/conformance/verify_daxis_release_evidence_test.sh
bash tests/conformance/verify_daxis_release_evidence.sh --list
bash tests/conformance/verify_workspace_layout.sh
npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
```

## 2026-05-30 Validation Snapshot

These commands were reproduced locally for the public object-storage live-fixture follow-up:

```bash
cd apps/axon-web
npm run build:wasm
npm run build:fixture
```

Start Vite in a second terminal:

```bash
cd apps/axon-web
npm run dev
```

Then run the browser smokes from the first terminal. On macOS, the focused editor smoke may need to run outside the execution sandbox because Chromium can fail before app code at `MachPortRendezvousServer`.

```bash
cd apps/axon-web
PLAYWRIGHT_BASE_URL=https://127.0.0.1:5173 npm run test:browser:editor-smoke -- --grep "object storage|connect source flows" --reporter=line
AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta PLAYWRIGHT_BASE_URL=https://127.0.0.1:5173 npm run test:browser:public-gcs-live -- --reporter=line
```

Docs hygiene for the evidence update was checked with:

```bash
cd apps/axon-web
npm exec -- prettier --check ../../docs/plans/2026-05-30-roadmap-next-steps-execution-plan.md ../../docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md ../../docs/release-gates/browser-wasm-delta-gcs-external-blockers.md ../../docs/release-gates/browser-wasm-delta-gcs-release-evidence.md
cd ../..
git diff --check
```

The broader local validation snapshot recorded in `docs/plans/2026-05-30-roadmap-next-steps-execution-plan.md` remains the prior evidence for the static app gates and browser DataFusion gates:

```bash
cd apps/axon-web
npm run format:check
npm run lint
npm exec -- tsc --noEmit
npm run test:sdk

cd ../..
cargo test -p wasm-datafusion-poc
cargo test -p wasm-datafusion-session
cargo test -p axon-web-wasm
cargo test -p delta-control-plane --test browser_snapshot_preflight
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
bash tests/conformance/verify_daxis_release_evidence_test.sh
bash tests/conformance/verify_daxis_release_evidence.sh --list
bash tests/conformance/verify_daxis_architecture_adr_test.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh
bash tests/conformance/verify_daxis_release_bundle_manifest.sh
cargo fmt --check
```

The public GCS live smoke passed 2 tests locally with the repo-documented fixture URI. It proves anonymous listing, Delta log read, Parquet range read, and browser app query execution for that public fixture. The smoke remains skip-safe when `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is absent.

CI automation handoff status on 2026-05-30:

- `gh variable list --repo daxis-io/axon` returned no repository Actions variables.
- `gh secret list --repo daxis-io/axon` returned no repository Actions secrets.
- `gh api repos/daxis-io/axon/environments` returned `total_count: 0`, so no repository environment-scoped Actions variables were available either.
- `gh variable list --org daxis-io` and `gh secret list --org daxis-io` returned HTTP 403 with the current token, so org-level inherited variable or secret names could not be inspected.
- `bash tests/conformance/verify_daxis_release_evidence.sh` now includes the env-gated public GCS Playwright smoke. It skips safely until `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is configured, and `apps/axon-web/playwright.public-gcs-live.config.ts` starts Vite automatically only when the live fixture variable is present.
- External automation still requires configuring a repo-accessible Actions variable named `AXON_LIVE_PUBLIC_GCS_TABLE_URI` for the live public fixture and documenting the fixture owner before CI can produce live public-GCS proof.

## External Dependencies That Stay Open

- signed URL issuance
- signed URL TTL policy approval
- object-scoped URL enforcement
- audit logging and request correlation
- production XML-endpoint CORS/origin validation
- dashboards
- production oncall playbooks
- env-gated GCS fixture provisioning and IAM

See [`browser-wasm-delta-gcs-external-blockers.md`](./browser-wasm-delta-gcs-external-blockers.md) for the owner-by-owner blocker register.
