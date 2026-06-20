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
import subprocess
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


def expect_unique(values: list[str], field: str) -> None:
    seen = set()
    for value in values:
        expect(value not in seen, f"{field} contains duplicate entry: {value}")
        seen.add(value)


expect(contract.get("readiness") == "daxis_m4_operational_maturity", "invalid readiness id")
expect(contract.get("repoOwnedScope"), "repoOwnedScope is required")
expect(contract.get("externalProductionScope"), "externalProductionScope is required")

source_docs = contract.get("sourceDocs", [])
expect_unique(source_docs, "sourceDocs")
for source_doc in source_docs:
    expect(not source_doc.startswith("/") and ".." not in Path(source_doc).parts, f"unsafe source doc path: {source_doc}")
    expect((repo_root / source_doc).is_file(), f"missing source doc: {source_doc}")

operational_maturity_doc = "docs/integrations/daxis/daxis-operational-maturity.md"
release_runbook_doc = "docs/program/browser-release-integration-runbook.md"
for required_source in [
    "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    operational_maturity_doc,
    "docs/program/browser-observability-contract.md",
    release_runbook_doc,
    "docs/integrations/daxis/daxis-first-class-integration-strategy.md",
    "docs/integrations/daxis/daxis-external-proof-handoff.md",
    "docs/release-gates/daxis-production-rollout-decisions.json",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/release-gates/daxis-release-bundle-manifest.json",
    "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
]:
    expect(required_source in source_docs, f"sourceDocs missing {required_source}")
operational_maturity_text = (repo_root / operational_maturity_doc).read_text(encoding="utf-8")
release_runbook_text = (repo_root / release_runbook_doc).read_text(encoding="utf-8")
for required_text in [
    "docs/release-gates/daxis-operational-readiness.json",
    "bash tests/conformance/verify_daxis_operational_readiness.sh",
    "tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
]:
    expect(
        required_text in operational_maturity_text,
        f"operational maturity document missing required text: {required_text}",
    )

for forbidden_text in [
    "Browser V1 here is the narrow runtime",
    "The target is a DataFusion-powered Delta/Parquet browser engine, once",
]:
    expect(
        forbidden_text not in release_runbook_text,
        f"browser release integration runbook must not contain stale runtime text: {forbidden_text}",
    )
for required_text in [
    "Daxis-facing app worker is browser DataFusion-backed",
    "legacy narrow runtime plus streaming scan plus an in-memory session shell remains compatibility-only",
    "AXON_DF_SIZE_PACKAGE=axon-web-wasm",
]:
    expect(
        required_text in release_runbook_text,
        f"browser release integration runbook missing required runtime boundary text: {required_text}",
    )

dashboard_entries = contract.get("dashboards", [])
dashboard_name_list = [entry.get("name") for entry in dashboard_entries]
expect_unique(dashboard_name_list, "dashboard names")
dashboard_names = set(dashboard_name_list)
for name in [
    "browser_execution",
    "fallback_and_block_reasons",
    "data_fetch_and_object_access",
    "worker_health",
]:
    expect(name in dashboard_names, f"missing dashboard contract: {name}")
    expect(name in operational_maturity_text, f"operational maturity document missing dashboard: {name}")

dashboard_by_name = {entry.get("name"): entry for entry in dashboard_entries}
required_dashboard_contracts = {
    "browser_execution": {
        "requiredBreakdowns": ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel", "browserFamily"],
        "signals": [
            "executed_on",
            "runtime_sku",
            "selected_bundle_tier",
            "query_duration_ms",
            "snapshot_bootstrap_duration_ms",
            "arrow_ipc_bytes",
            "rows_returned",
        ],
    },
    "fallback_and_block_reasons": {
        "requiredBreakdowns": ["tenantId", "workspaceId", "catalogTableId", "runtimeSku", "fallbackReason", "queryShape"],
        "signals": ["fallback_reason", "query_error_code", "policy_decision", "access_mode_decision", "server_fallback_target"],
    },
    "data_fetch_and_object_access": {
        "requiredBreakdowns": ["tenantId", "workspaceId", "tableId", "grantId", "accessMode", "storageProvider"],
        "signals": [
            "bytes_fetched",
            "bytes_reused",
            "files_touched",
            "files_skipped",
            "row_groups_touched",
            "row_groups_skipped",
            "footer_reads",
            "object_grant_action",
            "object_grant_outcome",
        ],
    },
    "worker_health": {
        "requiredBreakdowns": ["runtimeSku", "workerArtifact", "browserFamily", "bundleTier", "releaseChannel"],
        "signals": [
            "worker_startup_ms",
            "worker_memory_bytes",
            "worker_terminal_error",
            "cancellation_count",
            "artifact_size_bytes",
            "artifact_brotli_bytes",
        ],
    },
}
for name, required_contract in required_dashboard_contracts.items():
    dashboard = dashboard_by_name[name]
    for field in required_contract["requiredBreakdowns"]:
        expect(field in dashboard.get("requiredBreakdowns", []), f"dashboard {name} missing breakdown: {field}")
    for signal in required_contract["signals"]:
        expect(signal in dashboard.get("signals", []), f"dashboard {name} missing signal: {signal}")

required_runbooks = {
    "resolver_failure",
    "object_grant_failure",
    "cors_failure",
    "worker_startup_failure",
    "elevated_fallback_rates",
}
runbooks = contract.get("runbooks", [])
runbook_name_list = [entry.get("name") for entry in runbooks]
expect_unique(runbook_name_list, "runbook names")
runbook_names = set(runbook_name_list)
for name in required_runbooks:
    expect(name in runbook_names, f"missing runbook contract: {name}")
    expect(name in operational_maturity_text, f"operational maturity document missing runbook: {name}")

for runbook in runbooks:
    name = runbook.get("name", "<unknown>")
    expect(runbook.get("owner"), f"runbook {name} missing owner")
    expect(runbook.get("rollback"), f"runbook {name} missing rollback action")
    expect(len(runbook.get("firstChecks", [])) >= 3, f"runbook {name} needs at least three first checks")

rollout = contract.get("rolloutControls", {})
for dimension in ["tenantId", "workspaceId", "tableClass", "runtimeSku", "releaseChannel", "browserFamily"]:
    expect(dimension in rollout.get("requiredDimensions", []), f"rollout missing dimension: {dimension}")
for state in ["disabled", "server_fallback", "descriptor_only", "brokered_grants", "browser_datafusion", "stable_default"]:
    expect(state in rollout.get("requiredStates", []), f"rollout controls need {state} state")
    expect(state in operational_maturity_text, f"operational maturity document missing rollout state: {state}")
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
for segment in [
    "production_candidate_tables",
    "tables_blocked_by_policy",
    "tables_requiring_server_fallback",
    "tables_requiring_proxy_access",
    "tables_with_unknown_delta_features",
]:
    expect(segment in compatibility.get("minimumSegments", []), f"compatibility dashboard missing segment: {segment}")
    expect(segment in operational_maturity_text, f"operational maturity document missing compatibility segment: {segment}")

automation_commands = contract.get("releaseEvidenceAutomation", {}).get("verificationCommands", [])
expect_unique(automation_commands, "release automation commands")
automation_runner = contract.get("releaseEvidenceAutomation", {}).get("runner")
expect(isinstance(automation_runner, str) and automation_runner, "release automation runner is required")
expect(not automation_runner.startswith("/") and ".." not in Path(automation_runner).parts, f"unsafe release automation runner path: {automation_runner}")
automation_runner_path = repo_root / automation_runner
expect(automation_runner_path.is_file(), f"missing release automation runner: {automation_runner}")
artifact_command = contract.get("releaseEvidenceAutomation", {}).get("artifactCommand")
expect(
    artifact_command
    == "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
    "release evidence automation artifactCommand must name the digest-pinned release evidence log writer",
)
try:
    listed_release_commands = subprocess.check_output(
        ["bash", str(automation_runner_path), "--list"],
        cwd=repo_root,
        stderr=subprocess.PIPE,
        text=True,
    ).splitlines()
except subprocess.CalledProcessError as error:
    fail(
        "Daxis release evidence runner --list failed: "
        + error.stderr.strip()
    )
listed_release_command_set = {
    command.strip()
    for command in listed_release_commands
    if command.strip()
}
automation_command_list = [
    command.strip()
    for command in automation_commands
    if isinstance(command, str) and command.strip()
]
listed_release_command_list = [
    command.strip()
    for command in listed_release_commands
    if command.strip()
]
automation_command_set = set(automation_command_list)
missing_listed_commands = sorted(listed_release_command_set - automation_command_set)
extra_automation_commands = sorted(automation_command_set - listed_release_command_set)
expect(
    not missing_listed_commands,
    "release automation commands missing listed Daxis release evidence runner commands: "
    + ", ".join(missing_listed_commands),
)
expect(
    not extra_automation_commands,
    "release automation commands include commands not listed by the Daxis release evidence runner: "
    + ", ".join(extra_automation_commands),
)
expect(
    automation_command_list == listed_release_command_list,
    "release automation commands must match the Daxis release evidence runner --list order",
)
for command in automation_commands:
    expect(
        isinstance(command, str) and command.strip(),
        "release automation commands must be non-empty strings",
    )
    expect(
        command in listed_release_command_set,
        f"release automation command is not listed by the Daxis release evidence runner: {command}",
    )
for command in [
    "cargo check --workspace --locked",
    "cargo fmt --check",
    "cargo test -p wasm-datafusion-poc -- --test-threads=1",
    "cargo test -p query-contract",
    "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
    "bash tests/conformance/verify_daxis_query_corpus_coverage_test.sh",
    "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
    "cargo test -p wasm-datafusion-session",
    "cargo test -p axon-web-wasm",
    "cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker",
    "bash tests/conformance/verify_daxis_contract_artifacts_test.sh",
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
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
    "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm",
    "npm run test:sdk",
    "npm run build:fixture",
    "npm run build:wasm",
    "npm exec -- tsc --noEmit",
    "npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\"",
    "npm run test:browser:public-gcs-live -- --reporter=line",
]:
    expect(command in automation_commands, f"release automation missing command: {command}")

stable_default_blockers = contract.get("stableDefaultBlockers", [])
expect(
    isinstance(stable_default_blockers, list) and stable_default_blockers,
    "stableDefaultBlockers must not be empty",
)
for blocker in [
    "Production dashboard URLs and alert ownership are absent from this repository.",
    "Production oncall schedules and service incident playbooks are absent from this repository.",
    "Tenant and workspace rollout controls are Daxis service work outside this repository.",
    "Production table compatibility inventory is Daxis catalog and QA work outside this repository.",
    "External proof artifacts listed in docs/release-gates/daxis-external-proof-packet.json are Daxis-owned and absent from this repository.",
]:
    expect(blocker in stable_default_blockers, f"stableDefaultBlockers missing: {blocker}")
    expect(blocker in operational_maturity_text, f"operational maturity document missing stable-default blocker: {blocker}")

stable_default_blocker_proof_items = contract.get("stableDefaultBlockerProofItems", {})
expect(
    isinstance(stable_default_blocker_proof_items, dict) and stable_default_blocker_proof_items,
    "stableDefaultBlockerProofItems must not be empty",
)

external_proof_packet_path = repo_root / "docs/release-gates/daxis-external-proof-packet.json"
with open(external_proof_packet_path, encoding="utf-8") as handle:
    external_proof_packet = json.load(handle)
expect(
    external_proof_packet.get("packet") == "daxis_external_proof_packet",
    "external proof packet id mismatch",
)
external_m4_item_ids = {
    item.get("id")
    for item in external_proof_packet.get("externalItems", [])
    if item.get("milestone") == "M4"
}
stable_default_external_ids = set(
    external_proof_packet.get("stableDefaultPromotionGate", {}).get(
        "requiredExternalProofItemIds", []
    )
)
expected_blocker_proof_items = {
    "Production dashboard URLs and alert ownership are absent from this repository.": ["production_dashboards"],
    "Production oncall schedules and service incident playbooks are absent from this repository.": ["production_runbooks"],
    "Tenant and workspace rollout controls are Daxis service work outside this repository.": ["rollout_controls"],
    "Production table compatibility inventory is Daxis catalog and QA work outside this repository.": ["production_table_compatibility_dashboard"],
    "External proof artifacts listed in docs/release-gates/daxis-external-proof-packet.json are Daxis-owned and absent from this repository.": [
        "production_dashboards",
        "production_runbooks",
        "rollout_controls",
        "production_table_compatibility_dashboard",
    ],
}
expect(
    stable_default_blocker_proof_items == expected_blocker_proof_items,
    "stableDefaultBlockerProofItems must map each blocker to its external proof item ids",
)
for item_id in [
    "production_dashboards",
    "production_runbooks",
    "rollout_controls",
    "production_table_compatibility_dashboard",
]:
    expect(
        item_id in external_m4_item_ids,
        f"external proof packet missing M4 operational proof item: {item_id}",
    )
    expect(
        item_id in stable_default_external_ids,
        f"stableDefaultPromotionGate missing M4 operational proof item: {item_id}",
    )

for blocker, proof_item_ids in stable_default_blocker_proof_items.items():
    expect(
        blocker in stable_default_blockers,
        f"stableDefaultBlockerProofItems key is not a stableDefaultBlocker: {blocker}",
    )
    for proof_item_id in proof_item_ids:
        expect(
            proof_item_id in external_m4_item_ids,
            f"stableDefaultBlockerProofItems references non-M4 proof item: {proof_item_id}",
        )
        expect(
            proof_item_id in stable_default_external_ids,
            f"stableDefaultBlockerProofItems references proof item outside stable default gate: {proof_item_id}",
        )

print("Daxis operational readiness contract verified")
PY
