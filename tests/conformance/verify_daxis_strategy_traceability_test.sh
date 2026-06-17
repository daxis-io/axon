#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
matrix="$repo_root/docs/release-gates/daxis-strategy-traceability.json"
proof_packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"
release_evidence_runner="$repo_root/tests/conformance/verify_daxis_release_evidence.sh"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates" "$repo_root/tests/conformance"
for doc in \
	docs/program/daxis-first-class-integration-strategy.md \
	docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json \
	docs/release-gates/browser-wasm-delta-gcs-release-evidence.md \
	docs/release-gates/daxis-external-proof-packet.json \
	tests/conformance/verify_axon_web_datafusion_runtime.sh \
	tests/conformance/verify_browser_worker_dependency_boundary.sh \
	tests/conformance/verify_daxis_external_proof_packet.sh \
	tests/conformance/verify_daxis_pr_checklist.sh \
	tests/conformance/verify_daxis_query_corpus_coverage.sh \
	tests/conformance/verify_daxis_release_bundle_manifest.sh \
	tests/conformance/verify_daxis_external_state.sh \
	tests/conformance/verify_daxis_external_state_test.sh \
	tests/conformance/verify_daxis_release_evidence_test.sh; do
	mkdir -p "$repo_root/$(dirname "$doc")"
	printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_matrix() {
	python3 - "$matrix" "$proof_packet" <<'PY'
import json
import sys
from pathlib import Path

matrix_path = Path(sys.argv[1])
proof_packet_path = Path(sys.argv[2])
external_items = [
    ("M0", "daxis_architecture_docs"),
    ("M0", "daxis_names_axon_default_browser_engine"),
    ("M1", "daxis_descriptor_endpoint"),
    ("M1", "daxis_frontend_flow"),
    ("M2", "daxis_read_access_plan_endpoint"),
    ("M2", "storage_cors_proxy_validation"),
    ("M4", "production_dashboards"),
    ("M4", "production_runbooks"),
    ("M4", "rollout_controls"),
    ("M4", "production_table_compatibility_dashboard"),
]


def external_entry(item_id):
    return {
        "id": item_id,
        "status": "external_required",
        "summary": f"{item_id} requires Daxis proof",
        "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
        "externalOwner": "Daxis owner",
        "externalProof": "Daxis proof artifact",
    }


def repo_entry(item_id, evidence=None):
    return {
        "id": item_id,
        "status": "repo_verified",
        "summary": f"{item_id} is repo verified",
        "evidence": evidence or ["docs/program/daxis-first-class-integration-strategy.md"],
    }


milestones = {
    "M0": {
        "goal": "test M0",
        "deliverables": [external_entry("daxis_architecture_docs")],
        "exitCriteria": [external_entry("daxis_names_axon_default_browser_engine")],
    },
    "M1": {
        "goal": "test M1",
        "deliverables": [
            external_entry("daxis_descriptor_endpoint"),
            external_entry("daxis_frontend_flow"),
        ],
        "exitCriteria": [repo_entry("m1_repo_gate")],
    },
    "M2": {
        "goal": "test M2",
        "deliverables": [
            external_entry("daxis_read_access_plan_endpoint"),
            external_entry("storage_cors_proxy_validation"),
        ],
        "exitCriteria": [repo_entry("m2_repo_gate")],
    },
    "M3": {
        "goal": "test M3",
        "deliverables": [
            repo_entry(
                "datafusion_default_runtime",
                [
                    "docs/program/daxis-first-class-integration-strategy.md",
                    "tests/conformance/verify_axon_web_datafusion_runtime.sh",
                    "docs/program/browser-lakehouse-release-handoff-examples/browser-worker-artifact-report.datafusion.json",
                ],
            ),
            repo_entry(
                "daxis_query_corpus",
                ["tests/conformance/verify_daxis_query_corpus_coverage.sh"],
            ),
            repo_entry(
                "legacy_runtime_isolation",
                [
                    "tests/conformance/verify_browser_worker_dependency_boundary.sh",
                    "tests/conformance/verify_axon_web_datafusion_runtime.sh",
                ],
            ),
        ],
        "exitCriteria": [
            repo_entry(
                "daxis_exploratory_queries_datafusion",
                ["tests/conformance/verify_daxis_query_corpus_coverage.sh"],
            ),
            repo_entry(
                "budget_evidence",
                ["tests/conformance/verify_daxis_release_evidence.sh"],
            ),
        ],
    },
    "M4": {
        "goal": "test M4",
        "deliverables": [
            external_entry("production_dashboards"),
            external_entry("production_runbooks"),
            repo_entry(
                "release_evidence_automation",
                [
                    "tests/conformance/verify_daxis_release_evidence.sh",
                    "tests/conformance/verify_daxis_release_evidence_test.sh",
                    "tests/conformance/verify_daxis_release_bundle_manifest.sh",
                    "tests/conformance/verify_daxis_external_state.sh",
                    "tests/conformance/verify_daxis_external_state_test.sh",
                    "tests/conformance/verify_daxis_external_proof_packet.sh",
                    "tests/conformance/verify_daxis_pr_checklist.sh",
                ],
            ),
            external_entry("rollout_controls"),
            external_entry("production_table_compatibility_dashboard"),
        ],
        "exitCriteria": [
            repo_entry(
                "repeatable_go_no_go_evidence",
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
        ],
    },
}

matrix = {
    "matrix": "daxis_first_class_strategy_traceability",
    "strategy": "docs/program/daxis-first-class-integration-strategy.md",
    "externalProofPacket": "docs/release-gates/daxis-external-proof-packet.json",
    "milestones": milestones,
    "releaseGates": [
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
    ],
}
matrix_path.parent.mkdir(parents=True, exist_ok=True)
with open(matrix_path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)

packet = {
    "packet": "daxis_external_proof_packet",
    "externalItems": [
        {
            "id": item_id,
            "milestone": milestone,
            "owner": "Daxis owner",
            "requiredProofArtifacts": ["proof"],
            "acceptanceChecks": ["check"],
            "rollbackEvidence": "server_fallback",
            "axonReferences": ["docs/program/daxis-first-class-integration-strategy.md"],
        }
        for milestone, item_id in external_items
    ],
    "stableDefaultPromotionGate": {
        "requiredExternalProofItemIds": [item_id for _milestone, item_id in external_items]
    },
}
with open(proof_packet_path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

	cat >"$release_evidence_runner" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

case "${1:-}" in
  --list)
    cat <<'COMMANDS'
bash tests/conformance/verify_daxis_contract_artifacts.sh
bash tests/conformance/verify_daxis_rollout_decisions.sh
bash tests/conformance/verify_daxis_operational_readiness.sh
bash tests/conformance/verify_daxis_strategy_document.sh
bash tests/conformance/verify_daxis_strategy_traceability.sh
bash tests/conformance/verify_daxis_external_state_test.sh
bash tests/conformance/verify_daxis_external_proof_packet.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_release_bundle_manifest.sh
bash tests/conformance/verify_daxis_pr_checklist.sh
COMMANDS
    ;;
  *)
    exit 0
    ;;
esac
EOF
	chmod +x "$release_evidence_runner"
}

verify_fixture() {
	AXON_DAXIS_STRATEGY_REPO_ROOT="$repo_root" \
		AXON_DAXIS_STRATEGY_TRACEABILITY_FILE="$matrix" \
		bash tests/conformance/verify_daxis_strategy_traceability.sh >/dev/null 2>&1
}

write_valid_matrix
verify_fixture

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["releaseGates"].append("bash tests/conformance/verify_daxis_release_evidence.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected duplicate strategy traceability release gates to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["releaseGates"].append("bash tests/conformance/verify_stale_daxis_strategy_gate.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected unsupported strategy traceability release gate to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import copy
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["milestones"]["M1"]["deliverables"].append(
    copy.deepcopy(matrix["milestones"]["M1"]["deliverables"][0])
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected duplicate strategy traceability milestone item ids to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["releaseGates"].remove("bash tests/conformance/verify_daxis_release_evidence.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing full release evidence gate to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["releaseGates"].remove("bash tests/conformance/verify_daxis_strategy_traceability.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing strategy traceability self-gate to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["releaseGates"].remove("bash tests/conformance/verify_daxis_architecture_adr.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing architecture ADR release gate to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
for group_name in ["deliverables", "exitCriteria"]:
    for entry in matrix["milestones"]["M3"][group_name]:
        if entry["id"] in {"daxis_query_corpus", "daxis_exploratory_queries_datafusion"}:
            entry["evidence"] = [
                evidence
                for evidence in entry["evidence"]
                if evidence != "tests/conformance/verify_daxis_query_corpus_coverage.sh"
            ]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis query corpus coverage verifier evidence to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
for entry in matrix["milestones"]["M3"]["deliverables"]:
    if entry["id"] == "datafusion_default_runtime":
        entry["evidence"] = [
            evidence
            for evidence in entry["evidence"]
            if evidence != "tests/conformance/verify_axon_web_datafusion_runtime.sh"
        ]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing DataFusion default runtime verifier evidence to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["milestones"]["M3"]["deliverables"] = [
    entry
    for entry in matrix["milestones"]["M3"]["deliverables"]
    if entry["id"] != "legacy_runtime_isolation"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing legacy runtime isolation traceability item to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
for entry in matrix["milestones"]["M4"]["deliverables"]:
    if entry["id"] == "release_evidence_automation":
        entry["evidence"] = [
            evidence
            for evidence in entry["evidence"]
            if evidence != "tests/conformance/verify_daxis_release_evidence.sh"
        ]
for entry in matrix["milestones"]["M4"]["exitCriteria"]:
    if entry["id"] == "repeatable_go_no_go_evidence":
        entry["evidence"] = [
            evidence
            for evidence in entry["evidence"]
            if evidence != "tests/conformance/verify_daxis_release_evidence.sh"
        ]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing full release evidence command from M4 traceability to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$release_evidence_runner" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("bash tests/conformance/verify_daxis_release_bundle_manifest.sh\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected traceability release gate missing from release evidence runner to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$proof_packet" <<'PY'
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
	echo "expected traceability external proof packet drift to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" "$proof_packet" <<'PY'
import json
import sys

matrix_path = sys.argv[1]
packet_path = sys.argv[2]
with open(matrix_path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix["milestones"]["M0"]["exitCriteria"] = [
    entry
    for entry in matrix["milestones"]["M0"]["exitCriteria"]
    if entry["id"] != "daxis_names_axon_default_browser_engine"
]
matrix["milestones"]["M0"]["exitCriteria"].append(
    {
        "id": "m0_repo_gate",
        "status": "repo_verified",
        "summary": "M0 still has a repo-verified exit criterion.",
        "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
    }
)
with open(matrix_path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)

with open(packet_path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["externalItems"] = [
    item
    for item in packet["externalItems"]
    if item["id"] != "daxis_names_axon_default_browser_engine"
]
packet["stableDefaultPromotionGate"]["requiredExternalProofItemIds"].remove(
    "daxis_names_axon_default_browser_engine"
)
with open(packet_path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
	echo "expected missing default-browser-engine external proof item to be rejected" >&2
	exit 1
fi

write_valid_matrix
python3 - "$matrix" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    matrix = json.load(handle)
matrix.pop("externalProofPacket")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(matrix, handle)
PY

if verify_fixture; then
	echo "expected missing external proof packet link to be rejected" >&2
	exit 1
fi

echo "Daxis strategy traceability verifier regression coverage passed"
