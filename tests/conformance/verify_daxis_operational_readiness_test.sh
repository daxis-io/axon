#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
contract="$repo_root/docs/release-gates/daxis-operational-readiness.json"
external_proof_packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates"
mkdir -p "$repo_root/tests/conformance"

write_release_evidence_runner() {
  cat >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

case "${1:-}" in
  --list)
    cat <<'COMMANDS'
cargo check --workspace --locked
cargo fmt --check
cargo test -p query-contract
cargo test -p wasm-datafusion-poc
cargo test -p wasm-datafusion-poc --test daxis_query_corpus
bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh
bash tests/conformance/verify_daxis_query_corpus_coverage.sh
cargo test -p wasm-datafusion-session
cargo test -p axon-web-wasm
cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker
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
bash tests/perf/report_datafusion_wasm_size_test.sh
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
npm run build:fixture
npm run build:wasm
npm exec -- tsc --noEmit
npm run test:sdk
env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
npm run test:browser:public-gcs-live -- --reporter=line
COMMANDS
    ;;
  "")
    ;;
  *)
    echo "usage: $0 [--list]" >&2
    exit 2
    ;;
esac
EOF
}

write_release_evidence_runner
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

write_external_proof_packet() {
  cat >"$external_proof_packet" <<'JSON'
{
  "packet": "daxis_external_proof_packet",
  "externalItems": [
    {
      "id": "production_dashboards",
      "milestone": "M4"
    },
    {
      "id": "production_runbooks",
      "milestone": "M4"
    },
    {
      "id": "rollout_controls",
      "milestone": "M4"
    },
    {
      "id": "production_table_compatibility_dashboard",
      "milestone": "M4"
    }
  ],
  "stableDefaultPromotionGate": {
    "requiredExternalProofItemIds": [
      "production_dashboards",
      "production_runbooks",
      "rollout_controls",
      "production_table_compatibility_dashboard"
    ]
  }
}
JSON
}

write_operational_maturity_doc() {
  cat >"$repo_root/docs/program/daxis-operational-maturity.md" <<'EOF'
# Daxis Operational Maturity Contract

The machine-readable gate is `docs/release-gates/daxis-operational-readiness.json`,
checked by `bash tests/conformance/verify_daxis_operational_readiness.sh`.
The release evidence runner is `tests/conformance/verify_daxis_release_evidence.sh`.

Dashboard contracts:

- `browser_execution`
- `fallback_and_block_reasons`
- `data_fetch_and_object_access`
- `worker_health`

Runbook contracts:

- `resolver_failure`
- `object_grant_failure`
- `cors_failure`
- `worker_startup_failure`
- `elevated_fallback_rates`

Rollout states:

- `disabled`
- `server_fallback`
- `descriptor_only`
- `brokered_grants`
- `browser_datafusion`
- `stable_default`

Compatibility dashboard segments:

- `production_candidate_tables`
- `tables_blocked_by_policy`
- `tables_requiring_server_fallback`
- `tables_requiring_proxy_access`
- `tables_with_unknown_delta_features`

Stable default blockers:

- Production dashboard URLs and alert ownership are absent from this repository.
- Production oncall schedules and service incident playbooks are absent from this repository.
- Tenant and workspace rollout controls are Daxis service work outside this repository.
- Production table compatibility inventory is Daxis catalog and QA work outside this repository.
- External proof artifacts listed in docs/release-gates/daxis-external-proof-packet.json are Daxis-owned and absent from this repository.
EOF
}

write_browser_release_runbook() {
  cat >"$repo_root/docs/program/browser-release-integration-runbook.md" <<'EOF'
# Browser Release Integration Runbook

Daxis-facing app worker is browser DataFusion-backed. The legacy narrow runtime plus streaming scan plus an in-memory session shell remains compatibility-only.

For Daxis default-worker DataFusion size evidence, run `AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh`.
EOF
}

write_operational_maturity_doc
write_browser_release_runbook
write_external_proof_packet

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
    {
      "name": "browser_execution",
      "requiredBreakdowns": ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel", "browserFamily"],
      "signals": ["executed_on", "runtime_sku", "selected_bundle_tier", "query_duration_ms", "snapshot_bootstrap_duration_ms", "arrow_ipc_bytes", "rows_returned"]
    },
    {
      "name": "fallback_and_block_reasons",
      "requiredBreakdowns": ["tenantId", "workspaceId", "catalogTableId", "runtimeSku", "fallbackReason", "queryShape"],
      "signals": ["fallback_reason", "query_error_code", "policy_decision", "access_mode_decision", "server_fallback_target"]
    },
    {
      "name": "data_fetch_and_object_access",
      "requiredBreakdowns": ["tenantId", "workspaceId", "tableId", "grantId", "accessMode", "storageProvider"],
      "signals": ["bytes_fetched", "bytes_reused", "files_touched", "files_skipped", "row_groups_touched", "row_groups_skipped", "footer_reads", "object_grant_action", "object_grant_outcome"]
    },
    {
      "name": "worker_health",
      "requiredBreakdowns": ["runtimeSku", "workerArtifact", "browserFamily", "bundleTier", "releaseChannel"],
      "signals": ["worker_startup_ms", "worker_memory_bytes", "worker_terminal_error", "cancellation_count", "artifact_size_bytes", "artifact_brotli_bytes"]
    }
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
    "requiredDimensions": ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel", "browserFamily"],
    "requiredStates": ["disabled", "server_fallback", "descriptor_only", "brokered_grants", "browser_datafusion", "stable_default"],
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
    ],
    "minimumSegments": [
      "production_candidate_tables",
      "tables_blocked_by_policy",
      "tables_requiring_server_fallback",
      "tables_requiring_proxy_access",
      "tables_with_unknown_delta_features"
    ]
  },
  "releaseEvidenceAutomation": {
    "runner": "tests/conformance/verify_daxis_release_evidence.sh",
    "verificationCommands": [
      "cargo check --workspace --locked",
      "cargo fmt --check",
      "cargo test -p query-contract",
      "cargo test -p wasm-datafusion-poc",
      "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
      "bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh",
      "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
      "cargo test -p wasm-datafusion-session",
      "cargo test -p axon-web-wasm",
      "cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker",
      "bash tests/conformance/verify_daxis_contract_artifacts_test.sh",
      "bash tests/conformance/verify_daxis_contract_artifacts.sh",
      "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
      "bash tests/conformance/verify_browser_observability_contract_test.sh",
      "bash tests/conformance/verify_browser_observability_contract.sh",
      "bash tests/conformance/verify_axon_web_datafusion_runtime.sh",
      "bash tests/conformance/verify_daxis_strategy_document_test.sh",
      "bash tests/conformance/verify_daxis_strategy_document.sh",
      "bash tests/conformance/verify_daxis_rollout_decisions_test.sh",
      "bash tests/conformance/verify_daxis_rollout_decisions.sh",
      "bash tests/conformance/verify_daxis_operational_readiness_test.sh",
      "bash tests/conformance/verify_daxis_operational_readiness.sh",
      "bash tests/conformance/verify_daxis_strategy_traceability_test.sh",
      "bash tests/conformance/verify_daxis_strategy_traceability.sh",
      "bash tests/conformance/verify_daxis_external_state_test.sh",
      "bash tests/conformance/verify_daxis_external_proof_packet_test.sh",
      "bash tests/conformance/verify_daxis_external_proof_packet.sh",
      "bash tests/conformance/verify_daxis_architecture_adr_test.sh",
      "bash tests/conformance/verify_daxis_architecture_adr.sh",
      "bash tests/conformance/verify_daxis_release_bundle_manifest_test.sh",
      "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
      "bash tests/conformance/verify_daxis_pr_checklist_test.sh",
      "bash tests/conformance/verify_daxis_pr_checklist.sh",
      "bash tests/conformance/verify_daxis_release_evidence_test.sh",
      "bash tests/perf/report_datafusion_wasm_size_test.sh",
      "cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked",
      "npm run build:fixture",
      "npm run build:wasm",
      "npm exec -- tsc --noEmit",
      "npm run test:sdk",
      "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm",
      "npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\"",
      "npm run test:browser:public-gcs-live -- --reporter=line"
    ]
  },
  "stableDefaultBlockers": [
    "Production dashboard URLs and alert ownership are absent from this repository.",
    "Production oncall schedules and service incident playbooks are absent from this repository.",
    "Tenant and workspace rollout controls are Daxis service work outside this repository.",
    "Production table compatibility inventory is Daxis catalog and QA work outside this repository.",
    "External proof artifacts listed in docs/release-gates/daxis-external-proof-packet.json are Daxis-owned and absent from this repository."
  ]
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
import copy
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["dashboards"].append(copy.deepcopy(contract["dashboards"][0]))
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected duplicate operational-readiness dashboard names to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import copy
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["runbooks"].append(copy.deepcopy(contract["runbooks"][0]))
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected duplicate operational-readiness runbook names to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["sourceDocs"].append("docs/program/daxis-operational-maturity.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected duplicate operational-readiness source docs to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["sourceDocs"].remove("docs/program/browser-observability-contract.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing browser observability source doc to be rejected" >&2
  exit 1
fi

write_valid_contract
write_operational_maturity_doc
python3 - "$repo_root/docs/program/daxis-operational-maturity.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Daxis Operational Maturity Contract\n\n"
        "Daxis needs dashboards, runbooks, rollout controls, and compatibility evidence before stable rollout.\n"
    )
PY

if verify_fixture; then
  echo "expected stale operational maturity doc to be rejected" >&2
  exit 1
fi
write_operational_maturity_doc

write_valid_contract
write_browser_release_runbook
cat >"$repo_root/docs/program/browser-release-integration-runbook.md" <<'EOF'
# Browser Release Integration Runbook

Browser V1 here is the narrow runtime plus streaming scan plus an in-memory session shell. The target is a DataFusion-powered Delta/Parquet browser engine, once the DataFusion table provider and scan integration gates pass.
EOF

if verify_fixture; then
  echo "expected stale browser release runbook runtime boundary to be rejected" >&2
  exit 1
fi
write_browser_release_runbook

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
for dashboard in contract["dashboards"]:
    if dashboard["name"] == "fallback_and_block_reasons":
        dashboard["signals"].remove("server_fallback_target")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing server fallback dashboard signal to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
for dashboard in contract["dashboards"]:
    if dashboard["name"] == "data_fetch_and_object_access":
        dashboard["requiredBreakdowns"].remove("grantId")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing object-grant dashboard breakdown to be rejected" >&2
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

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["rolloutControls"]["requiredDimensions"].remove("browserFamily")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing browserFamily rollout dimension to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["rolloutControls"]["requiredStates"].remove("browser_datafusion")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing browser_datafusion rollout state to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["compatibilityDashboard"]["minimumSegments"].remove("tables_with_unknown_delta_features")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing unknown Delta feature compatibility segment to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].append("cargo test -p query-contract")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected duplicate release automation commands to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].append(
    "bash tests/conformance/verify_stale_operational_release_gate.sh"
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected unsupported release automation command to be rejected" >&2
  exit 1
fi

write_valid_contract
cat >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

case "${1:-}" in
  --list)
    cat <<'COMMANDS'
cargo check --workspace --locked
cargo fmt --check
cargo test -p query-contract
cargo test -p wasm-datafusion-poc
cargo test -p wasm-datafusion-session
cargo test -p axon-web-wasm
cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker
bash tests/conformance/verify_daxis_contract_artifacts.sh
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/conformance/verify_browser_observability_contract_test.sh
bash tests/conformance/verify_browser_observability_contract.sh
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
bash tests/conformance/verify_daxis_rollout_decisions.sh
bash tests/conformance/verify_daxis_operational_readiness.sh
bash tests/conformance/verify_daxis_strategy_traceability.sh
bash tests/conformance/verify_daxis_external_proof_packet.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_release_bundle_manifest.sh
bash tests/conformance/verify_daxis_pr_checklist.sh
bash tests/perf/report_datafusion_wasm_size_test.sh
cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p wasm-query-session -p browser-sdk -p browser-engine-worker --target wasm32-unknown-unknown --locked
npm run build:fixture
npm run build:wasm
npm exec -- tsc --noEmit
npm run test:sdk
env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
npm run test:browser:public-gcs-live -- --reporter=line
bash tests/conformance/verify_future_operational_release_gate.sh
COMMANDS
    ;;
  "")
    ;;
  *)
    echo "usage: $0 [--list]" >&2
    exit 2
    ;;
esac
EOF

if verify_fixture; then
  echo "expected operational readiness automation to include every listed release evidence command" >&2
  exit 1
fi
write_release_evidence_runner

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].remove(
    "bash tests/conformance/verify_daxis_operational_readiness.sh"
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing operational-readiness automation command to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].remove(
    "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm"
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing Daxis default-worker guardrail command to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].remove(
    "npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\""
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing Daxis browser matrix command to be rejected" >&2
  exit 1
fi

write_valid_contract
python3 - "$contract" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    contract = json.load(handle)
contract["releaseEvidenceAutomation"]["verificationCommands"].remove(
    "npm run test:browser:public-gcs-live -- --reporter=line"
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(contract, handle)
PY

if verify_fixture; then
  echo "expected missing public GCS live smoke command to be rejected" >&2
  exit 1
fi

write_valid_contract
write_external_proof_packet
python3 - "$external_proof_packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["externalItems"] = [
    item for item in packet["externalItems"] if item["id"] != "production_runbooks"
]
packet["stableDefaultPromotionGate"]["requiredExternalProofItemIds"].remove("production_runbooks")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected operational readiness to require production runbooks external proof" >&2
  exit 1
fi

echo "Daxis operational readiness verifier regression coverage passed"
