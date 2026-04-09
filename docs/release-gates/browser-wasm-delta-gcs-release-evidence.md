# Browser WASM + Delta on GCS Release Evidence

- Date: 2026-03-31
- Planning baseline commit: `0e19f1d`

This evidence pack maps the repository-owned release claims to commands and artifacts that already exist in the repo.

## Baseline Notes

- Use git commit `0e19f1d` as the evidence baseline for this bundle.
- Do not infer service delivery from this document; `services/query-api` is still absent from the repo.

## Repo-Owned Evidence Matrix

| Claim | Evidence |
| --- | --- |
| Browser and native crates build on host and wasm targets | `.github/workflows/ci.yml` and `.github/workflows/upgrade-rehearsal.yml` |
| Upgrade rehearsal preserves dependency drift evidence | uploaded artifact `upgrade-rehearsal-cargo-lock-diff` from `.github/workflows/upgrade-rehearsal.yml` |
| Worker artifact size is budgeted | `bash tests/perf/report_browser_worker_artifact.sh` |
| Worker startup and memory baselines are emitted | `cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture` and `cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture` |
| Browser dependency and artifact guardrails are enforced | `bash tests/security/verify_browser_dependency_guardrails.sh` |
| Patch inventory state is verified | `bash tests/conformance/verify_patch_inventory_state.sh` |
| The control-plane and browser seam is documented with canonical examples | `docs/program/browser-lakehouse-release-handoff.md` and `docs/program/browser-lakehouse-release-handoff-examples/` |
| Supported browser queries keep native parity | `cargo test -p delta-control-plane --locked` and `cargo test -p wasm-query-runtime --locked` |

## Verification Commands

```bash
cargo check --workspace --locked
cargo test -p query-contract --locked
cargo test -p delta-runtime-support --locked
cargo test -p delta-control-plane --locked
cargo test -p native-query-runtime --locked
cargo test -p wasm-http-object-store --locked
cargo test -p wasm-parquet-engine --locked
cargo test -p wasm-delta-snapshot --locked
cargo test -p wasm-query-runtime --locked
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-sdk --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-parquet-engine --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke
cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails_test.sh
bash tests/security/verify_browser_dependency_guardrails.sh
bash tests/conformance/verify_patch_inventory_state_test.sh
bash tests/conformance/verify_patch_inventory_state.sh
bash tests/conformance/verify_workspace_layout.sh
```

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
