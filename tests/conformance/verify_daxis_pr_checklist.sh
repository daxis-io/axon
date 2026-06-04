#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_PR_CHECKLIST_REPO_ROOT:-$(pwd)}"
template_file="${AXON_DAXIS_PR_CHECKLIST_TEMPLATE:-.github/pull_request_template.md}"

template_path() {
  if [[ "$template_file" = /* ]]; then
    printf "%s\n" "$template_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$template_file"
}

path="$(template_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis pull request checklist template: $template_file" >&2
  exit 1
fi

python3 - "$repo_root" "$path" "$template_file" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
path = Path(sys.argv[2])
template_file = sys.argv[3]
text = path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


required = [
    "## Daxis Impact Summary",
    "- Contract change:",
    "- Compatibility claim:",
    "- Fallback behavior:",
    "- Evidence:",
    "- WASM size, startup, memory, or browser compatibility:",
    "- Coordinated Daxis update:",
    "## Daxis-Relevant Changes",
    "- [ ] The Daxis/Axon trust boundary is unchanged or explicitly documented.",
    "- [ ] Browser code does not receive raw credentials or signing capability.",
    "- [ ] Public contract changes are additive or have a migration path.",
    "- [ ] TypeScript and Rust contract shapes stay aligned.",
    "- [ ] Unsupported behavior returns a structured error, fallback, or block reason.",
    "- [ ] Native/browser parity tests cover any new supported query behavior.",
    "- [ ] Browser dependency guardrails still pass.",
    "- [ ] WASM target checks still pass.",
    "- [ ] Worker artifact size impact is known.",
    "- [ ] Browser matrix coverage is updated when worker behavior changes.",
    "- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes.",
    "- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.",
    "- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements.",
    "- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md before stable default routing.",
    "- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states.",
    "- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.",
    "- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance.",
    "- [ ] Relevant docs and release evidence are updated.",
]

for line in required:
    if line not in text:
        fail(f"{template_file} missing Daxis PR checklist item: {line}")

release_process_item_ids = [
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
]
external_proof_item_ids = [
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
]


def read_required_text(relative_path: str) -> str:
    artifact_path = repo_root / relative_path
    if not artifact_path.is_file():
        fail(f"{template_file} references missing Daxis release artifact: {relative_path}")
    return artifact_path.read_text(encoding="utf-8")


def expect_markdown_item(doc_text: str, relative_path: str, item_id: str) -> None:
    if f"### `{item_id}`" not in doc_text:
        fail(f"{relative_path} missing checklist-backed item: {item_id}")


release_template_path = "docs/release-gates/daxis-release-attachment-template.md"
release_template = read_required_text(release_template_path)
for item_id in release_process_item_ids:
    expect_markdown_item(release_template, release_template_path, item_id)

external_template_path = "docs/release-gates/daxis-external-proof-attachment-template.md"
external_template = read_required_text(external_template_path)
for item_id in external_proof_item_ids:
    expect_markdown_item(external_template, external_template_path, item_id)

external_proof_packet_path = "docs/release-gates/daxis-external-proof-packet.json"
packet_text = read_required_text(external_proof_packet_path)
try:
    packet = json.loads(packet_text)
except json.JSONDecodeError as error:
    fail(f"{external_proof_packet_path} is not valid JSON: {error}")

if packet.get("packet") != "daxis_external_proof_packet":
    fail("stable default checklist points at a non-Daxis external proof packet")

stable_default_gate = packet.get("stableDefaultPromotionGate")
if not isinstance(stable_default_gate, dict):
    fail(f"{external_proof_packet_path} missing stableDefaultPromotionGate")

if stable_default_gate.get("requiredReleaseProcessAttachments") != release_process_item_ids:
    fail("stableDefaultPromotionGate release-process attachments drifted from PR checklist")

if stable_default_gate.get("requiredExternalProofItemIds") != external_proof_item_ids:
    fail("stableDefaultPromotionGate external proof items drifted from PR checklist")

if stable_default_gate.get("requiredReviewState") != "accepted":
    fail("stableDefaultPromotionGate must require accepted review state")

if (
    stable_default_gate.get("requiredReleaseEvidenceCommand")
    != "bash tests/conformance/verify_daxis_release_evidence.sh"
):
    fail("stableDefaultPromotionGate must require the full Daxis release evidence runner")

if stable_default_gate.get("requiredRollbackState") != "server_fallback":
    fail("stableDefaultPromotionGate must preserve server_fallback rollback")

print("Daxis PR checklist verified")
PY
