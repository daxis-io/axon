#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"
traceability="$repo_root/docs/release-gates/daxis-strategy-traceability.json"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates"

for doc in \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/program/daxis-operational-maturity.md \
  docs/program/daxis-external-proof-handoff.md \
  docs/release-gates/daxis-strategy-traceability.json \
  docs/release-gates/daxis-production-rollout-decisions.json \
  docs/release-gates/daxis-operational-readiness.json \
  docs/release-gates/browser-wasm-delta-gcs-external-blockers.md; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_packet() {
  python3 - "$packet" "$traceability" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
traceability_path = Path(sys.argv[2])
items = [
    ("daxis_architecture_docs", "M0"),
    ("daxis_names_axon_default_browser_engine", "M0"),
    ("daxis_descriptor_endpoint", "M1"),
    ("daxis_frontend_flow", "M1"),
    ("daxis_read_access_plan_endpoint", "M2"),
    ("storage_cors_proxy_validation", "M2"),
    ("production_dashboards", "M4"),
    ("production_runbooks", "M4"),
    ("rollout_controls", "M4"),
    ("production_table_compatibility_dashboard", "M4"),
]
packet = {
    "packet": "daxis_external_proof_packet",
    "scope": "test fixture",
    "sourceDocs": [
        "docs/release-gates/daxis-strategy-traceability.json",
        "docs/release-gates/daxis-production-rollout-decisions.json",
        "docs/release-gates/daxis-operational-readiness.json",
        "docs/program/daxis-external-proof-handoff.md",
        "docs/program/daxis-first-class-integration-strategy.md",
    ],
    "externalItems": [
        {
            "id": item_id,
            "milestone": milestone,
            "owner": "Daxis owner",
            "requiredProofArtifacts": ["proof artifact"],
            "acceptanceChecks": ["acceptance check"],
            "rollbackEvidence": "server_fallback proof",
            "axonReferences": [
                "docs/program/daxis-first-class-integration-strategy.md",
                "docs/release-gates/daxis-strategy-traceability.json",
            ],
        }
        for item_id, milestone in items
    ],
    "handoffChecklist": [
        "Attach Axon release evidence output.",
        "Attach Daxis service endpoint test results.",
        "Attach Daxis rollout-control proof.",
        "Attach Daxis dashboard and oncall proof.",
        "Confirm server fallback rollback path.",
    ],
}
path.parent.mkdir(parents=True, exist_ok=True)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)

milestones = {
    "M0": {"goal": "test M0", "deliverables": [], "exitCriteria": []},
    "M1": {"goal": "test M1", "deliverables": [], "exitCriteria": []},
    "M2": {"goal": "test M2", "deliverables": [], "exitCriteria": []},
    "M3": {"goal": "test M3", "deliverables": [], "exitCriteria": []},
    "M4": {"goal": "test M4", "deliverables": [], "exitCriteria": []},
}
for item_id, milestone in items:
    milestones[milestone]["deliverables"].append(
        {
            "id": item_id,
            "status": "external_required",
            "summary": "external proof required",
            "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
            "externalOwner": "Daxis owner",
            "externalProof": "Daxis proof",
        }
    )

matrix = {
    "matrix": "daxis_first_class_strategy_traceability",
    "strategy": "docs/program/daxis-first-class-integration-strategy.md",
    "milestones": milestones,
}
with open(traceability_path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY
}

verify_fixture() {
  AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT="$repo_root" \
    AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE="$packet" \
    bash tests/conformance/verify_daxis_external_proof_packet.sh >/dev/null 2>&1
}

write_valid_packet
verify_fixture

python3 - "$packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["externalItems"] = [
    item for item in packet["externalItems"] if item["id"] != "production_runbooks"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected missing production runbooks proof item to be rejected" >&2
  exit 1
fi

write_valid_packet
python3 - "$packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["externalItems"][0].pop("rollbackEvidence")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected missing rollback evidence to be rejected" >&2
  exit 1
fi

write_valid_packet
python3 - "$packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["externalItems"][0].pop("axonReferences")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected missing Axon references to be rejected" >&2
  exit 1
fi

write_valid_packet
python3 - "$packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["handoffChecklist"].remove("Attach Daxis rollout-control proof.")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected missing handoff checklist item to be rejected" >&2
  exit 1
fi

write_valid_packet
python3 - "$traceability" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["milestones"]["M0"]["exitCriteria"].append(
    {
        "id": "daxis_architecture_default_named",
        "status": "external_required",
        "summary": "Daxis architecture names Axon as the default browser engine.",
        "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
        "externalOwner": "Daxis owner",
        "externalProof": "Daxis proof",
    }
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
  echo "expected traceability external proof drift to be rejected" >&2
  exit 1
fi

echo "Daxis external proof packet verifier regression coverage passed"
