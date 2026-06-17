#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
template="$repo_root/.github/pull_request_template.md"
release_attachment_template="$repo_root/docs/release-gates/daxis-release-attachment-template.md"
external_proof_attachment_template="$repo_root/docs/release-gates/daxis-external-proof-attachment-template.md"
external_proof_packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"

mkdir -p "$repo_root/.github" "$repo_root/docs/release-gates"

write_valid_template() {
	cat >"$template" <<'TEMPLATE'
# Pull Request

## Summary

## Verification

## Daxis Impact Summary

- Contract change:
- Compatibility claim:
- Fallback behavior:
- Evidence:
- WASM size, startup, memory, or browser compatibility:
- Coordinated Daxis update:

## Daxis-Relevant Changes

- [ ] The Daxis/Axon trust boundary is unchanged or explicitly documented.
- [ ] Browser code does not receive raw credentials or signing capability.
- [ ] Public contract changes are additive or have a migration path.
- [ ] TypeScript and Rust contract shapes stay aligned.
- [ ] Unsupported behavior returns a structured error, fallback, or block reason.
- [ ] Native/browser parity tests cover any new supported query behavior.
- [ ] Browser dependency guardrails still pass.
- [ ] WASM target checks still pass.
- [ ] Worker artifact size impact is known.
- [ ] Browser matrix coverage is updated when worker behavior changes.
- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including release_channel, rollout_segment, and releaseAttachmentSchema.allowedReleaseChannels.
- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.
- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements.
- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md plus docs/release-gates/daxis-dirty-worktree-review-template.json for dirty-checkout reviews, and attaches release_channel, production environment_class, axon_release_commit_sha, axon_release_ref, proofAttachmentSchema.allowedReleaseChannels, proofAttachmentSchema.acceptedDaxisWorktreeReviews, the daxis.external_state.v1 JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.
- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states.
- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.
- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance, requiredReleaseChannel stable, and requiredReleaseEvidenceArtifactCommand (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`).
- [ ] Stable default promotion packets are validated with stableDefaultPromotionPacketValidationCommand (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`) before default routing.
- [ ] Relevant docs and release evidence are updated.
TEMPLATE
}

write_valid_release_attachment_template() {
	cat >"$release_attachment_template" <<'TEMPLATE'
# Daxis Release Attachment Template

## Release-Process Items

Set `release_channel` to the Daxis release channel this attachment supports: `experimental`, `integration`, `candidate`, or `stable`.
The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.
Set `rollout_segment` to the tenant, workspace, table-class, browser-family, or all-segments scope covered by this evidence.
Attach the SHA-256 digest of the release evidence artifact in `artifact_sha256`.
Record only the 64-character lowercase hexadecimal digest generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.

### `git_sha`

- Attach the exact Axon release commit SHA.

### `worker_artifact_size`

- Attach worker artifact size output.

### `public_gcs_live_smoke`

- Attach public GCS live smoke output.

### `release_notes`

- Attach release notes.

### `migration_notes`

- Attach migration notes.
TEMPLATE
}

write_valid_external_proof_attachment_template() {
	cat >"$external_proof_attachment_template" <<'TEMPLATE'
# Daxis External Proof Attachment Template

Attach the helper JSON summary generated by `bash tests/conformance/verify_daxis_external_state.sh --json`.
The helper JSON summary must use `schema_version` `daxis.external_state.v1`.
Attach the SHA-256 digest of that helper JSON summary in `daxis_external_state_json_sha256`.
Record only the 64-character lowercase hexadecimal digest generated from the exact helper JSON bytes, for example with `shasum -a 256 path/to/external-state.json`.
Set `environment_class` to `production`; development or staging proof cannot satisfy stable default routing.
Set `release_channel` to the Daxis rollout channel that the proof supports.
The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.
Set `axon_release_commit_sha` and `axon_release_ref` to the exact Axon release commit and branch or tag under review.
Classify the Daxis working-tree review as `clean`, `dirty_reviewed`, or `dirty_rejected`; do not accept proof from a dirty Daxis checkout without owner review.
Set `daxis_worktree_status` to the helper JSON `daxis_worktree_status` value: `clean` or `dirty`.
Set `daxis_worktree_review` to `clean`, `dirty_reviewed`, or `dirty_rejected`; stable default promotion accepts only `clean` or `dirty_reviewed`.
Use `docs/release-gates/daxis-dirty-worktree-review-template.json` as the starting shape for the dirty review artifact.

## External Proof Items

### `daxis_architecture_docs`

- Attach architecture proof.

### `daxis_names_axon_default_browser_engine`

- Attach default-engine proof.

### `daxis_descriptor_endpoint`

- Attach descriptor endpoint proof.

### `daxis_frontend_flow`

- Attach frontend flow proof.

### `daxis_read_access_plan_endpoint`

- Attach read-access-plan proof.

### `storage_cors_proxy_validation`

- Attach storage CORS and proxy validation proof.

### `production_dashboards`

- Attach dashboard proof.

### `production_runbooks`

- Attach runbook proof.

### `rollout_controls`

- Attach rollout-control proof.

### `production_table_compatibility_dashboard`

- Attach compatibility dashboard proof.
TEMPLATE
}

write_valid_external_proof_packet() {
	cat >"$external_proof_packet" <<'JSON'
{
  "packet": "daxis_external_proof_packet",
  "stableDefaultPromotionGate": {
    "requiredReleaseProcessAttachments": [
      "git_sha",
      "worker_artifact_size",
      "public_gcs_live_smoke",
      "release_notes",
      "migration_notes"
    ],
    "requiredExternalProofItemIds": [
      "daxis_architecture_docs",
      "daxis_names_axon_default_browser_engine",
      "daxis_descriptor_endpoint",
      "daxis_frontend_flow",
      "daxis_read_access_plan_endpoint",
      "storage_cors_proxy_validation",
      "production_dashboards",
      "production_runbooks",
      "rollout_controls",
      "production_table_compatibility_dashboard"
    ],
    "requiredReviewState": "accepted",
    "requiredReleaseEvidenceCommand": "bash tests/conformance/verify_daxis_release_evidence.sh",
    "requiredReleaseEvidenceArtifactCommand": "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
    "stableDefaultPromotionPacketValidationCommand": "bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0",
    "requiredReleaseChannel": "stable",
    "requiredRollbackState": "server_fallback"
  }
}
JSON
}

write_valid_fixture() {
	write_valid_template
	write_valid_release_attachment_template
	write_valid_external_proof_attachment_template
	write_valid_external_proof_packet
}

verify_fixture() {
	AXON_DAXIS_PR_CHECKLIST_REPO_ROOT="$repo_root" \
		bash tests/conformance/verify_daxis_pr_checklist.sh >/dev/null 2>&1
}

write_valid_fixture
verify_fixture

expect_missing_item_rejected() {
	local item="$1"
	local description="$2"

	write_valid_fixture

	python3 - "$template" "$item" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
item = sys.argv[2]
text = path.read_text(encoding="utf-8")
text = text.replace(f"{item}\n", "")
path.write_text(text, encoding="utf-8")
PY

	if verify_fixture; then
		echo "expected missing Daxis $description checklist item to be rejected" >&2
		exit 1
	fi
}

python3 - "$template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- [ ] Worker artifact size impact is known.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing Daxis worker artifact size checklist item to be rejected" >&2
	exit 1
fi

expect_missing_item_rejected \
	"- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including release_channel, rollout_segment, and releaseAttachmentSchema.allowedReleaseChannels." \
	"release attachment template"

write_valid_template
write_valid_release_attachment_template
write_valid_external_proof_attachment_template
write_valid_external_proof_packet
python3 - "$release_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Attach the SHA-256 digest of the release evidence artifact in `artifact_sha256`.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing release attachment artifact checksum guidance to be rejected" >&2
	exit 1
fi

write_valid_template
write_valid_release_attachment_template
write_valid_external_proof_attachment_template
write_valid_external_proof_packet
python3 - "$release_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Record only the 64-character lowercase hexadecimal digest generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing release attachment checksum format guidance to be rejected" >&2
	exit 1
fi

write_valid_template
write_valid_release_attachment_template
write_valid_external_proof_attachment_template
write_valid_external_proof_packet
python3 - "$release_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Set `release_channel` to the Daxis release channel this attachment supports: `experimental`, `integration`, `candidate`, or `stable`.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing release attachment release-channel guidance to be rejected" >&2
	exit 1
fi

write_valid_template
write_valid_release_attachment_template
write_valid_external_proof_attachment_template
write_valid_external_proof_packet
python3 - "$release_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Set `rollout_segment` to the tenant, workspace, table-class, browser-family, or all-segments scope covered by this evidence.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing release attachment rollout-segment guidance to be rejected" >&2
	exit 1
fi

expect_missing_item_rejected \
	"- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes." \
	"release notes template"

expect_missing_item_rejected \
	"- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements." \
	"release migration notes template"

expect_missing_item_rejected \
	"- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md plus docs/release-gates/daxis-dirty-worktree-review-template.json for dirty-checkout reviews, and attaches release_channel, production environment_class, axon_release_commit_sha, axon_release_ref, proofAttachmentSchema.allowedReleaseChannels, proofAttachmentSchema.acceptedDaxisWorktreeReviews, the daxis.external_state.v1 JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing." \
	"external proof attachment template"

write_valid_template
write_valid_release_attachment_template
write_valid_external_proof_attachment_template
write_valid_external_proof_packet
python3 - "$external_proof_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Record only the 64-character lowercase hexadecimal digest generated from the exact helper JSON bytes, for example with `shasum -a 256 path/to/external-state.json`.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing Daxis external-state checksum format guidance to be rejected" >&2
	exit 1
fi

expect_missing_item_rejected \
	"- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states." \
	"fallback mode"

expect_missing_item_rejected \
	"- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls." \
	"promotion guardrail"

write_valid_fixture
python3 - "$template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance, requiredReleaseChannel stable, and requiredReleaseEvidenceArtifactCommand (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`).\n",
    "",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing Daxis stable default promotion gate checklist item to be rejected" >&2
	exit 1
fi

write_valid_fixture
python3 - "$template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "- [ ] Stable default promotion packets are validated with stableDefaultPromotionPacketValidationCommand (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`) before default routing.\n",
    "",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing Daxis stable-default promotion packet validation checklist item to be rejected" >&2
	exit 1
fi

expect_missing_item_rejected \
	"## Daxis Impact Summary" \
	"impact summary section"

expect_missing_item_rejected \
	"- Contract change:" \
	"contract change impact field"

expect_missing_item_rejected \
	"- Compatibility claim:" \
	"compatibility claim impact field"

expect_missing_item_rejected \
	"- Fallback behavior:" \
	"fallback behavior impact field"

expect_missing_item_rejected \
	"- Evidence:" \
	"evidence impact field"

expect_missing_item_rejected \
	"- WASM size, startup, memory, or browser compatibility:" \
	"WASM and browser compatibility impact field"

expect_missing_item_rejected \
	"- Coordinated Daxis update:" \
	"coordinated Daxis update impact field"

write_valid_fixture

python3 - "$release_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "### `public_gcs_live_smoke`\n\n"
    "- Attach public GCS live smoke output.\n\n",
    "",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected release attachment template missing public_gcs_live_smoke to be rejected" >&2
	exit 1
fi

write_valid_fixture

python3 - "$external_proof_attachment_template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "### `production_runbooks`\n\n"
    "- Attach runbook proof.\n\n",
    "",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected external proof attachment template missing production_runbooks to be rejected" >&2
	exit 1
fi

write_valid_fixture

python3 - "$external_proof_packet" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
packet = json.loads(path.read_text(encoding="utf-8"))
packet.pop("stableDefaultPromotionGate")
path.write_text(json.dumps(packet, indent=2), encoding="utf-8")
PY

if verify_fixture; then
	echo "expected external proof packet missing stableDefaultPromotionGate to be rejected" >&2
	exit 1
fi

rm -f "$template"

if verify_fixture; then
	echo "expected missing pull request template to be rejected" >&2
	exit 1
fi

echo "Daxis PR checklist verifier regression coverage passed"
