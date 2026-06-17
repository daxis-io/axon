#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_STRATEGY_REPO_ROOT:-$(pwd)}"
matrix_file="${AXON_DAXIS_STRATEGY_TRACEABILITY_FILE:-docs/release-gates/daxis-strategy-traceability.json}"

matrix_path() {
	if [[ "$matrix_file" = /* ]]; then
		printf "%s\n" "$matrix_file"
		return
	fi

	printf "%s/%s\n" "$repo_root" "$matrix_file"
}

path="$(matrix_path)"
if [[ ! -f "$path" ]]; then
	echo "missing Daxis strategy traceability matrix: $matrix_file" >&2
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
    matrix = json.load(handle)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_unique_text_list(values: object, label: str) -> None:
    expect(isinstance(values, list), f"{label} must be a list")
    normalized = []
    for value in values:
        expect(isinstance(value, str) and value.strip(), f"{label} contains an invalid entry")
        normalized.append(value)
    duplicates = sorted(
        value
        for value in set(normalized)
        if normalized.count(value) > 1
    )
    expect(not duplicates, f"{label} has duplicate entries: {', '.join(duplicates)}")


def check_relative_path(path_text: str, label: str) -> None:
    candidate = Path(path_text)
    expect(not candidate.is_absolute() and ".." not in candidate.parts, f"unsafe {label}: {path_text}")
    expect((repo_root / candidate).exists(), f"missing {label}: {path_text}")


expect(matrix.get("matrix") == "daxis_first_class_strategy_traceability", "invalid traceability matrix id")
expect(matrix.get("strategy") == "docs/program/daxis-first-class-integration-strategy.md", "unexpected strategy path")
check_relative_path(matrix["strategy"], "strategy document")

expected_milestones = ["M0", "M1", "M2", "M3", "M4"]
milestones = matrix.get("milestones", {})
expect(sorted(milestones.keys()) == expected_milestones, "traceability matrix must cover M0 through M4")

for milestone, payload in milestones.items():
    expect(payload.get("goal"), f"{milestone} missing goal")
    deliverables = payload.get("deliverables", [])
    exit_criteria = payload.get("exitCriteria", [])
    expect(deliverables, f"{milestone} missing deliverables")
    expect(exit_criteria, f"{milestone} missing exit criteria")
    entry_ids = [
        entry.get("id")
        for entry in deliverables + exit_criteria
        if entry.get("id")
    ]
    duplicate_entry_ids = sorted(
        item_id
        for item_id in set(entry_ids)
        if entry_ids.count(item_id) > 1
    )
    expect(
        not duplicate_entry_ids,
        f"{milestone} has duplicate traceability item ids: {', '.join(duplicate_entry_ids)}",
    )

    for group_name, entries in [("deliverable", deliverables), ("exit criterion", exit_criteria)]:
        for entry in entries:
            item_id = entry.get("id")
            expect(item_id, f"{milestone} {group_name} missing id")
            status = entry.get("status")
            expect(status in {"repo_verified", "external_required"}, f"{milestone} {item_id} has invalid status")
            evidence = entry.get("evidence", [])
            expect(evidence, f"{milestone} {item_id} missing evidence")
            for evidence_path in evidence:
                check_relative_path(evidence_path, f"evidence for {milestone} {item_id}")
            if status == "external_required":
                expect(entry.get("externalOwner"), f"{milestone} {item_id} missing external owner")
                expect(entry.get("externalProof"), f"{milestone} {item_id} missing external proof")

release_gates = matrix.get("releaseGates", [])
expect_unique_text_list(release_gates, "releaseGates")
expected_release_gates = [
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "bash tests/conformance/verify_daxis_rollout_decisions.sh",
    "bash tests/conformance/verify_daxis_operational_readiness.sh",
    "bash tests/conformance/verify_daxis_strategy_document.sh",
    "bash tests/conformance/verify_daxis_strategy_traceability.sh",
    "bash tests/conformance/verify_daxis_external_state_test.sh",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "bash tests/conformance/verify_daxis_architecture_adr.sh",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
]
for required in expected_release_gates:
    expect(required in release_gates, f"traceability matrix missing release gate: {required}")
unsupported_release_gates = sorted(set(release_gates) - set(expected_release_gates))
expect(
    not unsupported_release_gates,
    f"traceability matrix has unsupported release gates: {', '.join(unsupported_release_gates)}",
)

release_evidence_runner = repo_root / "tests/conformance/verify_daxis_release_evidence.sh"
expect(release_evidence_runner.is_file(), "missing Daxis release evidence runner")
try:
    listed_release_commands = subprocess.check_output(
        ["bash", str(release_evidence_runner), "--list"],
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
runner_entrypoint_commands = {
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
}
missing_runner_gates = sorted(
    gate
    for gate in release_gates
    if gate not in runner_entrypoint_commands and gate not in listed_release_command_set
)
expect(
    not missing_runner_gates,
    "traceability release gates missing from release evidence runner --list: "
    + ", ".join(missing_runner_gates),
)

external_proof_packet_path = matrix.get("externalProofPacket")
expect(
    external_proof_packet_path == "docs/release-gates/daxis-external-proof-packet.json",
    "traceability matrix must link the Daxis external proof packet",
)
check_relative_path(external_proof_packet_path, "external proof packet")

external_ids = {
    f"{milestone}.{entry['id']}"
    for milestone, payload in milestones.items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "external_required"
}
for required_external in [
    "M0.daxis_architecture_docs",
    "M0.daxis_names_axon_default_browser_engine",
    "M1.daxis_descriptor_endpoint",
    "M1.daxis_frontend_flow",
    "M2.daxis_read_access_plan_endpoint",
    "M2.storage_cors_proxy_validation",
    "M4.production_dashboards",
    "M4.production_runbooks",
    "M4.rollout_controls",
    "M4.production_table_compatibility_dashboard",
]:
    expect(required_external in external_ids, f"traceability matrix should keep external item explicit: {required_external}")

with open(repo_root / external_proof_packet_path, encoding="utf-8") as handle:
    external_proof_packet = json.load(handle)
packet_external_ids = {
    f"{item.get('milestone')}.{item.get('id')}"
    for item in external_proof_packet.get("externalItems", [])
}
missing_from_packet = sorted(external_ids - packet_external_ids)
extra_in_packet = sorted(packet_external_ids - external_ids)
expect(
    not missing_from_packet,
    f"external proof packet missing traceability items: {', '.join(missing_from_packet)}",
)
expect(
    not extra_in_packet,
    f"external proof packet has items not in traceability matrix: {', '.join(extra_in_packet)}",
)

stable_gate_external_ids = {
    item_id
    for item_id in external_proof_packet.get("stableDefaultPromotionGate", {}).get(
        "requiredExternalProofItemIds", []
    )
}
traceability_external_item_ids = {
    external_id.split(".", 1)[1] for external_id in external_ids
}
expect(
    stable_gate_external_ids == traceability_external_item_ids,
    "stableDefaultPromotionGate required external proof ids must match traceability external items",
)

repo_verified_ids = {
    f"{milestone}.{entry['id']}"
    for milestone, payload in milestones.items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "repo_verified"
}
repo_verified_entries = {
    f"{milestone}.{entry['id']}": entry
    for milestone, payload in milestones.items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "repo_verified"
}


def require_repo_evidence(item_id: str, required_evidence: list[str]) -> None:
    entry = repo_verified_entries.get(item_id)
    expect(entry is not None, f"traceability matrix should keep {item_id} repo-verified")
    evidence = set(entry.get("evidence", []))
    missing_evidence = [
        evidence_path
        for evidence_path in required_evidence
        if evidence_path not in evidence
    ]
    expect(
        not missing_evidence,
        f"traceability matrix {item_id} missing required evidence: "
        + ", ".join(missing_evidence),
    )


expect(
    "M3.datafusion_default_runtime" in repo_verified_ids,
    "traceability matrix should keep the Axon DataFusion default worker artifact repo-verified",
)
require_repo_evidence(
    "M3.datafusion_default_runtime",
    [
        "tests/conformance/verify_axon_web_datafusion_runtime.sh",
        "docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json",
    ],
)
require_repo_evidence(
    "M3.legacy_runtime_isolation",
    [
        "tests/conformance/verify_browser_worker_dependency_boundary.sh",
        "tests/conformance/verify_axon_web_datafusion_runtime.sh",
    ],
)
for item_id in ["M3.daxis_query_corpus", "M3.daxis_exploratory_queries_datafusion"]:
    entry = repo_verified_entries.get(item_id)
    expect(entry is not None, f"traceability matrix should keep {item_id} repo-verified")
    expect(
        "tests/conformance/verify_daxis_query_corpus_coverage.sh" in entry.get("evidence", []),
        f"traceability matrix {item_id} must include the Daxis query corpus coverage verifier",
    )
require_repo_evidence(
    "M3.budget_evidence",
    ["tests/conformance/verify_daxis_release_evidence.sh"],
)
require_repo_evidence(
    "M4.release_evidence_automation",
    [
        "tests/conformance/verify_daxis_release_evidence.sh",
        "tests/conformance/verify_daxis_release_evidence_test.sh",
        "tests/conformance/verify_daxis_release_bundle_manifest.sh",
        "tests/conformance/verify_daxis_external_state.sh",
        "tests/conformance/verify_daxis_external_state_test.sh",
        "tests/conformance/verify_daxis_external_proof_packet.sh",
        "tests/conformance/verify_daxis_pr_checklist.sh",
    ],
)
require_repo_evidence(
    "M4.repeatable_go_no_go_evidence",
    [
        "tests/conformance/verify_daxis_release_evidence.sh",
        "tests/conformance/verify_daxis_release_bundle_manifest.sh",
        "tests/conformance/verify_daxis_external_state.sh",
        "tests/conformance/verify_daxis_external_state_test.sh",
        "tests/conformance/verify_daxis_external_proof_packet.sh",
        "tests/conformance/verify_daxis_pr_checklist.sh",
        "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
    ],
)

print("Daxis strategy traceability matrix verified")
PY
