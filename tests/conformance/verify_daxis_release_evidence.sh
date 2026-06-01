#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_RELEASE_EVIDENCE_REPO_ROOT:-$(pwd)}"

commands=(
  "cargo check --workspace --locked"
  "cargo test -p query-contract"
  "cargo test -p wasm-datafusion-poc"
  "cargo test -p wasm-datafusion-session"
  "cargo test -p axon-web-wasm"
  "bash tests/conformance/verify_daxis_contract_artifacts.sh"
  "bash tests/conformance/verify_browser_worker_dependency_boundary.sh"
  "bash tests/conformance/verify_axon_web_datafusion_runtime.sh"
  "bash tests/conformance/verify_daxis_rollout_decisions.sh"
  "bash tests/conformance/verify_daxis_operational_readiness.sh"
  "bash tests/conformance/verify_daxis_strategy_traceability.sh"
  "bash tests/conformance/verify_daxis_external_proof_packet.sh"
  "bash tests/conformance/verify_daxis_architecture_adr.sh"
  "bash tests/conformance/verify_daxis_release_bundle_manifest.sh"
  "bash tests/conformance/verify_daxis_pr_checklist.sh"
  "bash tests/perf/report_datafusion_wasm_size_test.sh"
  "cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked"
  "npm exec -- tsc --noEmit"
  "npm run test:sdk"
  "npm run build:fixture"
  "npm run build:wasm"
  "npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\""
)

list_commands() {
  printf "%s\n" "${commands[@]}"
}

run_step() {
  printf '+ %s\n' "$*"
  "$@"
}

run_release_evidence() {
  cd "$repo_root"
  run_step cargo check --workspace --locked
  run_step cargo test -p query-contract
  run_step cargo test -p wasm-datafusion-poc
  run_step cargo test -p wasm-datafusion-session
  run_step cargo test -p axon-web-wasm
  run_step bash tests/conformance/verify_daxis_contract_artifacts.sh
  run_step bash tests/conformance/verify_browser_worker_dependency_boundary.sh
  run_step bash tests/conformance/verify_axon_web_datafusion_runtime.sh
  run_step bash tests/conformance/verify_daxis_rollout_decisions.sh
  run_step bash tests/conformance/verify_daxis_operational_readiness.sh
  run_step bash tests/conformance/verify_daxis_strategy_traceability.sh
  run_step bash tests/conformance/verify_daxis_external_proof_packet.sh
  run_step bash tests/conformance/verify_daxis_architecture_adr.sh
  run_step bash tests/conformance/verify_daxis_release_bundle_manifest.sh
  run_step bash tests/conformance/verify_daxis_pr_checklist.sh
  run_step bash tests/perf/report_datafusion_wasm_size_test.sh
  run_step cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
  (
    cd "$repo_root/apps/axon-web"
    run_step npm exec -- tsc --noEmit
    run_step npm run test:sdk
    run_step npm run build:fixture
    run_step npm run build:wasm
    run_step npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
  )
}

case "${1:-}" in
  --list)
    list_commands
    ;;
  "")
    run_release_evidence
    ;;
  *)
    echo "usage: $0 [--list]" >&2
    exit 2
    ;;
esac
