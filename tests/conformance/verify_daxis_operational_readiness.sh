#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_OPERATIONAL_REPO_ROOT:-$(pwd)}"
readiness_file="${AXON_DAXIS_OPERATIONAL_READINESS_FILE:-docs/release-gates/daxis-operational-readiness.json}"

readiness_path() {
  if [[ "$readiness_file" = /* ]]; then
    printf "%s\n" "$readiness_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$readiness_file"
}

path="$(readiness_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis operational readiness contract: $readiness_file" >&2
  exit 1
fi

python3 - "$repo_root" "$path" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
path = Path(sys.argv[2])
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


expect(contract.get("readiness") == "daxis_m4_operational_maturity", "invalid readiness id")
expect(contract.get("repoOwnedScope"), "repoOwnedScope is required")
expect(contract.get("externalProductionScope"), "externalProductionScope is required")

for source_doc in contract.get("sourceDocs", []):
    expect(not source_doc.startswith("/") and ".." not in Path(source_doc).parts, f"unsafe source doc path: {source_doc}")
    expect((repo_root / source_doc).is_file(), f"missing source doc: {source_doc}")

dashboard_names = {entry.get("name") for entry in contract.get("dashboards", [])}
for name in [
    "browser_execution",
    "fallback_and_block_reasons",
    "data_fetch_and_object_access",
    "worker_health",
]:
    expect(name in dashboard_names, f"missing dashboard contract: {name}")

required_runbooks = {
    "resolver_failure",
    "object_grant_failure",
    "cors_failure",
    "worker_startup_failure",
    "elevated_fallback_rates",
}
runbooks = contract.get("runbooks", [])
runbook_names = {entry.get("name") for entry in runbooks}
for name in required_runbooks:
    expect(name in runbook_names, f"missing runbook contract: {name}")

for runbook in runbooks:
    name = runbook.get("name", "<unknown>")
    expect(runbook.get("owner"), f"runbook {name} missing owner")
    expect(runbook.get("rollback"), f"runbook {name} missing rollback action")
    expect(len(runbook.get("firstChecks", [])) >= 3, f"runbook {name} needs at least three first checks")

rollout = contract.get("rolloutControls", {})
for dimension in ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel"]:
    expect(dimension in rollout.get("requiredDimensions", []), f"rollout missing dimension: {dimension}")
expect("server_fallback" in rollout.get("requiredStates", []), "rollout controls need server_fallback state")
expect("disabled" in rollout.get("requiredStates", []), "rollout controls need disabled state")
expect(rollout.get("killSwitch"), "rollout controls need a killSwitch")

compatibility = contract.get("compatibilityDashboard", {})
for field in [
    "tenantId",
    "workspaceId",
    "tableId",
    "tableClass",
    "deltaProtocol",
    "deltaFeatures",
    "policyDecision",
    "accessModeDecision",
    "browserCompatibility",
    "fallbackReason",
    "lastVerifiedAt",
]:
    expect(field in compatibility.get("requiredFields", []), f"compatibility dashboard missing field: {field}")

automation_commands = contract.get("releaseEvidenceAutomation", {}).get("verificationCommands", [])
automation_runner = contract.get("releaseEvidenceAutomation", {}).get("runner")
expect(isinstance(automation_runner, str) and automation_runner, "release automation runner is required")
expect(not automation_runner.startswith("/") and ".." not in Path(automation_runner).parts, f"unsafe release automation runner path: {automation_runner}")
expect((repo_root / automation_runner).is_file(), f"missing release automation runner: {automation_runner}")
for command in [
    "cargo test -p query-contract",
    "cargo test -p wasm-datafusion-poc",
    "cargo test -p axon-web-wasm",
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
    "bash tests/conformance/verify_axon_web_datafusion_runtime.sh",
    "bash tests/conformance/verify_daxis_rollout_decisions.sh",
    "bash tests/conformance/verify_daxis_strategy_traceability.sh",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "bash tests/conformance/verify_daxis_architecture_adr.sh",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
    "bash tests/perf/report_datafusion_wasm_size_test.sh",
    "npm run test:sdk",
    "npm run build:fixture",
    "npm run build:wasm",
]:
    expect(command in automation_commands, f"release automation missing command: {command}")

print("Daxis operational readiness contract verified")
PY
