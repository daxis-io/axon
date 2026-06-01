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
for required in [
    "bash tests/conformance/verify_daxis_contract_artifacts.sh",
    "bash tests/conformance/verify_daxis_rollout_decisions.sh",
    "bash tests/conformance/verify_daxis_operational_readiness.sh",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
]:
    expect(required in release_gates, f"traceability matrix missing release gate: {required}")

external_ids = {
    f"{milestone}.{entry['id']}"
    for milestone, payload in milestones.items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "external_required"
}
for required_external in [
    "M0.daxis_architecture_docs",
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

repo_verified_ids = {
    f"{milestone}.{entry['id']}"
    for milestone, payload in milestones.items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "repo_verified"
}
expect(
    "M3.datafusion_default_runtime" in repo_verified_ids,
    "traceability matrix should keep the Axon DataFusion default worker artifact repo-verified",
)

print("Daxis strategy traceability matrix verified")
PY
