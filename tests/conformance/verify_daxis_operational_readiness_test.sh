#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
contract="$repo_root/docs/release-gates/daxis-operational-readiness.json"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates"
mkdir -p "$repo_root/tests/conformance"
printf '#!/usr/bin/env bash\n' >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh"
for doc in \
  docs/program/daxis-operational-maturity.md \
  docs/adr/ADR-0008-daxis-browser-read-compute-contract.md \
  docs/program/browser-observability-contract.md \
  docs/program/browser-release-integration-runbook.md \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/program/daxis-external-proof-handoff.md \
  docs/release-gates/daxis-production-rollout-decisions.json \
  docs/release-gates/daxis-external-proof-packet.json \
  docs/release-gates/daxis-release-bundle-manifest.json \
  docs/release-gates/browser-wasm-delta-gcs-release-evidence.md \
  docs/release-gates/browser-wasm-delta-gcs-external-blockers.md; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_contract() {
  cat >"$contract" <<'JSON'
{
  "readiness": "daxis_m4_operational_maturity",
  "repoOwnedScope": "contract fixture",
  "externalProductionScope": "production systems are external",
  "sourceDocs": [
    "docs/program/daxis-operational-maturity.md",
    "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    "docs/program/browser-observability-contract.md",
    "docs/program/browser-release-integration-runbook.md",
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/program/daxis-external-proof-handoff.md",
    "docs/release-gates/daxis-production-rollout-decisions.json",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/release-gates/daxis-release-bundle-manifest.json",
    "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"
  ],
  "dashboards": [
    { "name": "browser_execution" },
    { "name": "fallback_and_block_reasons" },
    { "name": "data_fetch_and_object_access" },
    { "name": "worker_health" }
  ],
  "runbooks": [
    {
      "name": "resolver_failure",
      "owner": "gateway",
      "firstChecks": ["request", "catalog", "schema"],
      "rollback": "server_fallback"
    },
    {
      "name": "object_grant_failure",
      "owner": "storage",
      "firstChecks": ["expiry", "routes", "audit"],
      "rollback": "sql_fallback_required"
    },
    {
      "name": "cors_failure",
      "owner": "security",
      "firstChecks": ["origin", "headers", "range"],
      "rollback": "proxy_or_server"
    },
    {
      "name": "worker_startup_failure",
      "owner": "runtime",
      "firstChecks": ["artifact", "matrix", "baseline"],
      "rollback": "disable_browser"
    },
    {
      "name": "elevated_fallback_rates",
      "owner": "query",
      "firstChecks": ["segment", "reason", "corpus"],
      "rollback": "narrow_rollout"
    }
  ],
  "rolloutControls": {
    "requiredDimensions": ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel"],
    "requiredStates": ["disabled", "server_fallback"],
    "killSwitch": "force server fallback"
  },
  "compatibilityDashboard": {
    "requiredFields": [
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
      "lastVerifiedAt"
    ]
  },
  "releaseEvidenceAutomation": {
    "runner": "tests/conformance/verify_daxis_release_evidence.sh",
    "verificationCommands": [
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
      "npm run build:wasm"
    ]
  }
}
JSON
}

verify_fixture() {
  AXON_DAXIS_OPERATIONAL_REPO_ROOT="$repo_root" \
    AXON_DAXIS_OPERATIONAL_READINESS_FILE="$contract" \
    bash tests/conformance/verify_daxis_operational_readiness.sh >/dev/null 2>&1
}

write_valid_contract
verify_fixture

python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["runbooks"] = [
    runbook for runbook in contract["runbooks"] if runbook["name"] != "cors_failure"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing CORS runbook to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["sourceDocs"].append("docs/program/missing-operational-doc.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing source doc to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["rolloutControls"]["requiredDimensions"].remove("runtimeSku")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing runtimeSku rollout dimension to be rejected" >&2
  exit 1
fi

echo "Daxis operational readiness verifier regression coverage passed"
