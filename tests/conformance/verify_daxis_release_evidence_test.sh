#!/usr/bin/env bash

set -euo pipefail

output="$(bash tests/conformance/verify_daxis_release_evidence.sh --list)"

daxis_browser_matrix_command='npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"'
daxis_default_worker_guardrail_command='env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm'
daxis_host_runtime_command='cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker'
public_gcs_live_command='npm run test:browser:public-gcs-live -- --reporter=line'

for expected in \
  "cargo check --workspace --locked" \
  "cargo fmt --check" \
  "cargo test -p query-contract" \
  "cargo test -p wasm-datafusion-poc" \
  "cargo test -p wasm-datafusion-poc --test daxis_query_corpus" \
  "bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh" \
  "bash tests/conformance/verify_daxis_query_corpus_coverage.sh" \
  "cargo test -p wasm-datafusion-session" \
  "cargo test -p axon-web-wasm" \
  "$daxis_host_runtime_command" \
  "bash tests/conformance/verify_daxis_contract_artifacts.sh" \
  "bash tests/conformance/verify_browser_worker_dependency_boundary.sh" \
  "bash tests/conformance/verify_browser_observability_contract_test.sh" \
  "bash tests/conformance/verify_browser_observability_contract.sh" \
  "bash tests/conformance/verify_axon_web_datafusion_runtime.sh" \
  "bash tests/conformance/verify_daxis_strategy_document_test.sh" \
  "bash tests/conformance/verify_daxis_strategy_document.sh" \
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
  "$daxis_default_worker_guardrail_command" \
  "bash tests/conformance/verify_daxis_contract_artifacts_test.sh" \
  "bash tests/conformance/verify_browser_observability_contract_test.sh" \
  "bash tests/conformance/verify_daxis_strategy_document_test.sh" \
  "bash tests/conformance/verify_daxis_rollout_decisions_test.sh" \
  "bash tests/conformance/verify_daxis_operational_readiness_test.sh" \
  "bash tests/conformance/verify_daxis_strategy_traceability_test.sh" \
  "bash tests/conformance/verify_daxis_external_state_test.sh" \
  "bash tests/conformance/verify_daxis_external_proof_packet_test.sh" \
  "bash tests/conformance/verify_daxis_architecture_adr_test.sh" \
  "bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh" \
  "bash tests/conformance/verify_daxis_pr_checklist_test.sh" \
  "bash tests/conformance/verify_daxis_release_evidence_test.sh" \
  "$daxis_browser_matrix_command" \
  "$public_gcs_live_command"; do
  if [[ "$output" != *"$expected"* ]]; then
    echo "Daxis release evidence runner is missing command: $expected" >&2
    exit 1
  fi
done

RUNNER_OUTPUT="$output" python3 - <<'PY'
import json
import os
import sys

with open("docs/release-gates/daxis-release-bundle-manifest.json", encoding="utf-8") as handle:
    manifest = json.load(handle)

listed_commands = set(os.environ["RUNNER_OUTPUT"].splitlines())
missing_commands: list[str] = []
for item in manifest.get("bundleItems", []):
    if item.get("status") != "repo_verified":
        continue
    for command in item.get("verificationCommands", []):
        if command not in listed_commands:
            missing_commands.append(f"{item.get('id')}: {command}")

if missing_commands:
    print(
        "Daxis release evidence runner is missing repo-verified manifest commands:\n"
        + "\n".join(missing_commands),
        file=sys.stderr,
    )
    sys.exit(1)
PY

command_line_number() {
  awk -v expected="$1" '$0 == expected { print NR; exit }' <<<"$output"
}

build_wasm_line="$(command_line_number "npm run build:wasm")"
tsc_line="$(command_line_number "npm exec -- tsc --noEmit")"
if [[ -z "$build_wasm_line" || -z "$tsc_line" || "$build_wasm_line" -ge "$tsc_line" ]]; then
  echo "Daxis release evidence runner must build generated WASM bindings before TypeScript checking" >&2
  exit 1
fi

unquoted_daxis_browser_matrix_command='npm exec -- playwright test --config=playwright.config.ts --grep Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors'
if [[ "$output" == *"$unquoted_daxis_browser_matrix_command"* ]]; then
  echo "Daxis release evidence runner lists an unsafe unquoted browser-matrix grep command" >&2
  exit 1
fi

browser_matrix_config="apps/axon-web/playwright.config.ts"
if grep -Fq "command: 'npm run dev'" "$browser_matrix_config"; then
  echo "Daxis browser-matrix Playwright webServer must not rebuild fixture and WASM during release evidence" >&2
  exit 1
fi
if ! grep -Fq '"dev:server": "vite --host 127.0.0.1"' apps/axon-web/package.json; then
  echo "Daxis browser-matrix package scripts must expose a Vite-only dev server command" >&2
  exit 1
fi
if ! grep -Fq "command: 'npm run dev:server'" "$browser_matrix_config"; then
  echo "Daxis browser-matrix Playwright webServer must start the Vite-only dev server after release evidence builds artifacts" >&2
  exit 1
fi
if ! grep -Fq "timeout: 60_000" "$browser_matrix_config"; then
  echo "Daxis browser-matrix Playwright timeout must leave enough headroom for real browser worker startup" >&2
  exit 1
fi

public_gcs_live_config="apps/axon-web/playwright.public-gcs-live.config.ts"
if ! grep -Fq "timeout: 60_000" "$public_gcs_live_config"; then
  echo "Daxis public GCS live Playwright webServer timeout must leave enough headroom for release-candidate startup" >&2
  exit 1
fi

stub_bin="$(mktemp -d)"
trap 'rm -rf "$stub_bin"' EXIT
for executable in cargo npm bash; do
  cat >"$stub_bin/$executable" <<'SH'
#!/bin/bash
exit 0
SH
  chmod +x "$stub_bin/$executable"
done

runner_output="$(PATH="$stub_bin:$PATH" /bin/bash tests/conformance/verify_daxis_release_evidence.sh)"
unsafe_logged_daxis_browser_matrix_command='+ npm exec -- playwright test --config=playwright.config.ts --grep Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors'
safe_logged_daxis_browser_matrix_command='+ npm exec -- playwright test --config=playwright.config.ts --grep Daxis\ descriptor-resolver\|preserves\ cancellation\ errors\|surfaces\ unsupported\ feature\ errors'
if [[ "$runner_output" == *"$unsafe_logged_daxis_browser_matrix_command"* ]]; then
  echo "Daxis release evidence runner logs an unsafe unquoted browser-matrix grep command" >&2
  exit 1
fi
if [[ "$runner_output" != *"$safe_logged_daxis_browser_matrix_command"* ]]; then
  echo "Daxis release evidence runner did not log a shell-safe browser-matrix grep command" >&2
  exit 1
fi

if bash tests/conformance/verify_daxis_release_evidence.sh --unknown >/dev/null 2>&1; then
  echo "expected unknown release evidence option to be rejected" >&2
  exit 1
fi

echo "Daxis release evidence runner regression coverage passed"
