#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT:-$(pwd)}"
packet_file="${AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE:-docs/release-gates/daxis-external-proof-packet.json}"

packet_path() {
  if [[ "$packet_file" = /* ]]; then
    printf "%s\n" "$packet_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$packet_file"
}

path="$(packet_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis external proof packet: $packet_file" >&2
  exit 1
fi

python3 - "$repo_root" "$path" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
path = Path(sys.argv[2])
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)


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


expect(packet.get("packet") == "daxis_external_proof_packet", "invalid external proof packet id")
expect(packet.get("scope"), "scope is required")

source_docs = packet.get("sourceDocs", [])
expect(source_docs, "sourceDocs must not be empty")
for source_doc in source_docs:
    check_relative_path(source_doc, "source doc")
expect(
    "docs/release-gates/daxis-strategy-traceability.json" in source_docs,
    "sourceDocs must include Daxis strategy traceability matrix",
)

external_items = packet.get("externalItems", [])
expect(external_items, "externalItems must not be empty")
item_ids = {item.get("id") for item in external_items}
for required in [
    "daxis_architecture_docs",
    "daxis_names_axon_default_browser_engine",
    "daxis_descriptor_endpoint",
    "daxis_frontend_flow",
    "daxis_read_access_plan_endpoint",
    "storage_cors_proxy_validation",
    "production_dashboards",
    "production_runbooks",
    "rollout_controls",
    "production_table_compatibility_dashboard",
]:
    expect(required in item_ids, f"missing external proof item: {required}")

traceability_path = repo_root / "docs/release-gates/daxis-strategy-traceability.json"
with open(traceability_path, encoding="utf-8") as handle:
    matrix = json.load(handle)
external_traceability_items = {
    f"{milestone}.{entry['id']}"
    for milestone, payload in matrix.get("milestones", {}).items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "external_required"
}
packet_items = {f"{item.get('milestone')}.{item.get('id')}" for item in external_items}
missing_from_packet = sorted(external_traceability_items - packet_items)
extra_in_packet = sorted(packet_items - external_traceability_items)
expect(
    not missing_from_packet,
    f"external proof packet missing traceability items: {', '.join(missing_from_packet)}",
)
expect(
    not extra_in_packet,
    f"external proof packet has items not in traceability matrix: {', '.join(extra_in_packet)}",
)

for item in external_items:
    item_id = item.get("id")
    expect(item_id, "external item missing id")
    expect(item.get("milestone") in {"M0", "M1", "M2", "M3", "M4"}, f"{item_id} has invalid milestone")
    expect(item.get("owner"), f"{item_id} missing owner")
    expect(item.get("requiredProofArtifacts"), f"{item_id} missing required proof artifacts")
    expect(item.get("acceptanceChecks"), f"{item_id} missing acceptance checks")
    expect(item.get("rollbackEvidence"), f"{item_id} missing rollback evidence")
    axon_references = item.get("axonReferences", [])
    expect(axon_references, f"{item_id} missing Axon references")
    for evidence_path in axon_references:
        check_relative_path(evidence_path, f"Axon reference for {item_id}")

handoff = packet.get("handoffChecklist", [])
for required in [
    "Attach Axon release evidence output.",
    "Attach Daxis service endpoint test results.",
    "Attach Daxis rollout-control proof.",
    "Attach Daxis dashboard and oncall proof.",
    "Confirm server fallback rollback path.",
]:
    expect(required in handoff, f"missing handoff checklist item: {required}")

print("Daxis external proof packet verified")
PY
