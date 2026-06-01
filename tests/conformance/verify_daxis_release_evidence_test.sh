#!/usr/bin/env bash

set -euo pipefail

output="$(bash tests/conformance/verify_daxis_release_evidence.sh --list)"

daxis_browser_matrix_command='npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"'

for expected in \
  "cargo check --workspace --locked" \
  "cargo test -p query-contract" \
  "cargo test -p wasm-datafusion-poc" \
  "cargo test -p axon-web-wasm" \
  "bash tests/conformance/verify_daxis_contract_artifacts.sh" \
  "bash tests/conformance/verify_browser_worker_dependency_boundary.sh" \
  "bash tests/conformance/verify_axon_web_datafusion_runtime.sh" \
  "bash tests/conformance/verify_daxis_rollout_decisions.sh" \
  "bash tests/conformance/verify_daxis_operational_readiness.sh" \
  "bash tests/conformance/verify_daxis_strategy_traceability.sh" \
  "bash tests/conformance/verify_daxis_external_proof_packet.sh" \
  "bash tests/conformance/verify_daxis_architecture_adr.sh" \
  "bash tests/conformance/verify_daxis_release_bundle_manifest.sh" \
  "bash tests/conformance/verify_daxis_pr_checklist.sh" \
  "bash tests/perf/report_datafusion_wasm_size_test.sh" \
  "cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked" \
  "npm exec -- tsc --noEmit" \
  "npm run test:sdk" \
  "npm run build:fixture" \
  "npm run build:wasm" \
  "$daxis_browser_matrix_command"; do
  if [[ "$output" != *"$expected"* ]]; then
    echo "Daxis release evidence runner is missing command: $expected" >&2
    exit 1
  fi
done

unquoted_daxis_browser_matrix_command='npm exec -- playwright test --config=playwright.config.ts --grep Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors'
if [[ "$output" == *"$unquoted_daxis_browser_matrix_command"* ]]; then
  echo "Daxis release evidence runner lists an unsafe unquoted browser-matrix grep command" >&2
  exit 1
fi

if bash tests/conformance/verify_daxis_release_evidence.sh --unknown >/dev/null 2>&1; then
  echo "expected unknown release evidence option to be rejected" >&2
  exit 1
fi

echo "Daxis release evidence runner regression coverage passed"
