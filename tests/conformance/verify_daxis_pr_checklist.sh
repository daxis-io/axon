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
    "- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including release_channel, rollout_segment, and releaseAttachmentSchema.allowedReleaseChannels.",
    "- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.",
    "- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements.",
    "- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md plus docs/release-gates/daxis-dirty-worktree-review-template.json for dirty-checkout reviews, and attaches release_channel, production environment_class, axon_release_commit_sha, axon_release_ref, proofAttachmentSchema.allowedReleaseChannels, proofAttachmentSchema.acceptedDaxisWorktreeReviews, the daxis.external_state.v1 JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.",
    "- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states.",
    "- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.",
    "- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance, requiredReleaseChannel stable, and requiredReleaseEvidenceArtifactCommand (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`).",
    "- [ ] Stable default promotion packets are validated with stableDefaultPromotionPacketValidationCommand (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`) before default routing.",
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
for required_text in [
    "Set `release_channel` to the Daxis release channel this attachment supports: `experimental`, `integration`, `candidate`, or `stable`.",
    "The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.",
    "Set `rollout_segment` to the tenant, workspace, table-class, browser-family, or all-segments scope covered by this evidence.",
    "Attach the SHA-256 digest of the release evidence artifact in `artifact_sha256`.",
    "Record only the 64-character lowercase hexadecimal digest generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.",
]:
    if required_text not in release_template:
        fail(f"{release_template_path} missing checklist-backed release-attachment guidance: {required_text}")

external_template_path = "docs/release-gates/daxis-external-proof-attachment-template.md"
external_template = read_required_text(external_template_path)
for item_id in external_proof_item_ids:
    expect_markdown_item(external_template, external_template_path, item_id)
for required_text in [
    "Attach the helper JSON summary generated by `bash tests/conformance/verify_daxis_external_state.sh --json`.",
    "The helper JSON summary must use `schema_version` `daxis.external_state.v1`.",
    "Attach the SHA-256 digest of that helper JSON summary in `daxis_external_state_json_sha256`.",
    "Record only the 64-character lowercase hexadecimal digest generated from the exact helper JSON bytes, for example with `shasum -a 256 path/to/external-state.json`.",
    "Set `environment_class` to `production`; development or staging proof cannot satisfy stable default routing.",
    "Set `release_channel` to the Daxis rollout channel that the proof supports.",
    "The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.",
    "Set `axon_release_commit_sha` and `axon_release_ref` to the exact Axon release commit and branch or tag under review.",
    "Classify the Daxis working-tree review as `clean`, `dirty_reviewed`, or `dirty_rejected`",
    "Set `daxis_worktree_status` to the helper JSON `daxis_worktree_status` value: `clean` or `dirty`.",
    "Set `daxis_worktree_review` to `clean`, `dirty_reviewed`, or `dirty_rejected`; stable default promotion accepts only `clean` or `dirty_reviewed`.",
    "Use `docs/release-gates/daxis-dirty-worktree-review-template.json` as the starting shape for the dirty review artifact.",
]:
    if required_text not in external_template:
        fail(f"{external_template_path} missing checklist-backed external-state guidance: {required_text}")

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

if (
    stable_default_gate.get("requiredReleaseEvidenceArtifactCommand")
    != "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log"
):
    fail("stableDefaultPromotionGate must require the digest-pinned release evidence artifact writer")

if (
    stable_default_gate.get("stableDefaultPromotionPacketValidationCommand")
    != "bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0"
):
    fail("stableDefaultPromotionGate must require the stable-default promotion packet validator")

if stable_default_gate.get("requiredReleaseChannel") != "stable":
    fail("stableDefaultPromotionGate must require stable release channel")

if stable_default_gate.get("requiredRollbackState") != "server_fallback":
    fail("stableDefaultPromotionGate must preserve server_fallback rollback")

print("Daxis PR checklist verified")
PY
