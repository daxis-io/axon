#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$(pwd)"
manifest="$tmpdir/daxis-release-bundle-manifest.json"
packet="$tmpdir/daxis-external-proof-packet.json"
artifact_root="$tmpdir/artifacts"
release_attachments="$tmpdir/release-attachments"
proof_attachments="$tmpdir/proof-attachments"
release_evidence_log="$tmpdir/release-evidence.log"
verifier="$repo_root/tests/conformance/verify_daxis_stable_default_promotion_packet.sh"

sha256_file() {
	shasum -a 256 "$1" | awk '{print $1}'
}

write_manifest() {
	cat >"$manifest" <<'JSON'
{
  "manifest": "daxis_release_bundle_manifest",
  "releaseAttachmentSchema": {
    "releaseProcessItemIds": [
      "git_sha",
      "worker_artifact_size",
      "public_gcs_live_smoke",
      "release_notes",
      "migration_notes"
    ],
    "requiredMetadata": [
      "item_id",
      "release_commit_sha",
      "release_ref",
      "release_channel",
      "rollout_segment",
      "owner",
      "captured_at",
      "artifact_uri",
      "artifact_sha256",
      "verification_command_or_statement",
      "exit_status_or_review_status",
      "rollback_or_migration_note_uri"
    ],
    "allowedReleaseChannels": ["experimental", "integration", "candidate", "stable"],
    "checksumFormat": {
      "field": "artifact_sha256",
      "algorithm": "sha256",
      "encoding": "lowercase_hex",
      "length": 64,
      "sourceBytes": "exact_release_evidence_artifact_bytes"
    },
    "requiredReviewStates": ["attached", "reviewed", "accepted", "rejected"]
  }
}
JSON
}

write_packet() {
	cat >"$packet" <<'JSON'
{
  "packet": "daxis_external_proof_packet",
  "proofAttachmentSchema": {
    "requiredMetadata": [
      "item_id",
      "milestone",
      "owner",
      "captured_at",
      "environment",
      "environment_class",
      "release_channel",
      "axon_release_commit_sha",
      "axon_release_ref",
      "rollout_segment",
      "artifact_uri",
      "verification_command_or_url",
      "exit_status_or_review_status",
      "rollback_evidence_uri",
      "daxis_commit_sha",
      "daxis_ref",
      "daxis_origin_remote_url",
      "daxis_worktree_status",
      "daxis_worktree_review",
      "daxis_external_state_schema_version",
      "daxis_external_state_json_uri",
      "daxis_external_state_json_sha256"
    ],
    "requiredEnvironmentClass": "production",
    "allowedReleaseChannels": ["experimental", "integration", "candidate", "stable"],
    "allowedDaxisWorktreeStatuses": ["clean", "dirty"],
    "allowedDaxisWorktreeReviews": ["clean", "dirty_reviewed", "dirty_rejected"],
    "acceptedDaxisWorktreeReviews": ["clean", "dirty_reviewed"],
    "requiredReviewerRoles": ["Daxis platform owner"],
    "checksumFormat": {
      "field": "daxis_external_state_json_sha256",
      "algorithm": "sha256",
      "encoding": "lowercase_hex",
      "length": 64,
      "sourceBytes": "exact_helper_json_bytes"
    },
    "requiredReviewStates": ["attached", "reviewed", "accepted", "rejected"]
  },
  "externalItems": [
    {
      "id": "daxis_architecture_docs",
      "milestone": "M0"
    },
    {
      "id": "daxis_names_axon_default_browser_engine",
      "milestone": "M0"
    },
    {
      "id": "daxis_descriptor_endpoint",
      "milestone": "M1"
    },
    {
      "id": "daxis_frontend_flow",
      "milestone": "M1"
    },
    {
      "id": "daxis_read_access_plan_endpoint",
      "milestone": "M2"
    },
    {
      "id": "storage_cors_proxy_validation",
      "milestone": "M2"
    },
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
    "requiredReviewState": "accepted",
    "requiredReleaseChannel": "stable",
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
    ]
  }
}
JSON
}

write_release_attachment() {
	local item_id="$1"
	local artifact_path="$artifact_root/release/$item_id.log"
	local artifact_sha
	mkdir -p "$(dirname "$artifact_path")"
	printf 'accepted release artifact for %s\n' "$item_id" >"$artifact_path"
	artifact_sha="$(sha256_file "$artifact_path")"
	cat >"$release_attachments/$item_id.md" <<EOF
# Completed Daxis Release Attachment

- item_id: $item_id
- release_commit_sha: 1111111111111111111111111111111111111111
- release_ref: refs/tags/axon-v1.0.0
- release_channel: stable
- rollout_segment: all-segments
- owner: Release owner
- captured_at: 2026-06-07T12:00:00Z
- artifact_uri: release/$item_id.log
- artifact_sha256: $artifact_sha
- verification_command_or_statement: bash tests/conformance/verify_daxis_release_evidence.sh
- exit_status_or_review_status: accepted
- rollback_or_migration_note_uri: release/server-fallback.md
EOF
}

proof_owner_review_table() {
	cat <<'EOF'

## Review

| Role                 | Name                    | Review state | Notes                    |
| -------------------- | ----------------------- | ------------ | ------------------------ |
| Daxis platform owner | Daxis platform reviewer | accepted     | Platform proof reviewed. |
EOF
}

write_proof_attachment() {
	local item_id="$1"
	local milestone="$2"
	local artifact_path="$artifact_root/proof/$item_id/external-state.json"
	local review_path="$artifact_root/proof/$item_id/dirty-worktree-review.json"
	local artifact_sha
	local review_sha
	mkdir -p "$(dirname "$artifact_path")"
	cat >"$artifact_path" <<'JSON'
{
  "schema_version": "daxis.external_state.v1",
  "daxis_commit_sha": "2222222222222222222222222222222222222222",
  "daxis_ref": "main",
  "daxis_origin_remote_url": "git@github.com:daxis/daxis-platform.git",
  "daxis_worktree_status": "dirty",
  "daxis_worktree_status_lines": [
    " M docs/architecture/daxis-control-plane-architecture.md"
  ],
  "daxis_worktree_review_required": "dirty_reviewed_or_dirty_rejected",
  "contract_test": {
    "command": "cargo test -p daxis-query --test contracts",
    "exit_status": 0,
    "output": "ok"
  },
  "architecture_scan": {
    "command": "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query",
    "exit_status": 0,
    "output": "README.md:1:Axon browser WASM"
  }
}
JSON
	cat >"$review_path" <<'JSON'
{
  "schema_version": "daxis.dirty_worktree_review.v1",
  "daxis_commit_sha": "2222222222222222222222222222222222222222",
  "daxis_ref": "main",
  "daxis_origin_remote_url": "git@github.com:daxis/daxis-platform.git",
  "daxis_worktree_status": "dirty",
  "daxis_worktree_review": "dirty_reviewed",
  "review_state": "accepted",
  "reviewed_at": "2026-06-07T12:00:00Z",
  "reviewer": "Daxis platform reviewer",
  "reviewer_role": "Daxis platform owner",
  "rollout_segment": "all-segments",
  "daxis_worktree_status_lines": [
    " M docs/architecture/daxis-control-plane-architecture.md"
  ],
  "reviewed_paths": [
    "docs/architecture/daxis-control-plane-architecture.md"
  ],
  "notes": "Reviewed for the stable default rollout segment."
}
JSON
	artifact_sha="$(sha256_file "$artifact_path")"
	review_sha="$(sha256_file "$review_path")"
	cat >"$proof_attachments/$item_id.md" <<EOF
# Completed Daxis External Proof Attachment

- item_id: $item_id
- milestone: $milestone
- owner: Daxis gateway owner
- captured_at: 2026-06-07T12:00:00Z
- environment: production-us
- environment_class: production
- release_channel: stable
- axon_release_commit_sha: 1111111111111111111111111111111111111111
- axon_release_ref: refs/tags/axon-v1.0.0
- rollout_segment: all-segments
- artifact_uri: proof/$item_id/evidence.md
- verification_command_or_url: cargo test -p daxis-query --test contracts
- exit_status_or_review_status: accepted
- rollback_evidence_uri: proof/server-fallback.md
- daxis_commit_sha: 2222222222222222222222222222222222222222
- daxis_ref: main
- daxis_origin_remote_url: git@github.com:daxis/daxis-platform.git
- daxis_worktree_status: dirty
- daxis_worktree_review: dirty_reviewed
- daxis_external_state_schema_version: daxis.external_state.v1
- daxis_external_state_json_uri: proof/$item_id/external-state.json
- daxis_external_state_json_sha256: $artifact_sha
- daxis_worktree_review_json_uri: proof/$item_id/dirty-worktree-review.json
- daxis_worktree_review_json_sha256: $review_sha
EOF
	proof_owner_review_table >>"$proof_attachments/$item_id.md"
}

write_attachment_directories() {
	rm -rf "$release_attachments" "$proof_attachments" "$artifact_root"
	mkdir -p "$release_attachments" "$proof_attachments" "$artifact_root"
	for item_id in git_sha worker_artifact_size public_gcs_live_smoke release_notes migration_notes; do
		write_release_attachment "$item_id"
	done
	write_proof_attachment daxis_architecture_docs M0
	write_proof_attachment daxis_names_axon_default_browser_engine M0
	write_proof_attachment daxis_descriptor_endpoint M1
	write_proof_attachment daxis_frontend_flow M1
	write_proof_attachment daxis_read_access_plan_endpoint M2
	write_proof_attachment storage_cors_proxy_validation M2
	write_proof_attachment production_dashboards M4
	write_proof_attachment production_runbooks M4
	write_proof_attachment rollout_controls M4
	write_proof_attachment production_table_compatibility_dashboard M4
}

write_release_evidence_log() {
	bash "$repo_root/tests/conformance/verify_daxis_release_evidence.sh" --list |
		while IFS= read -r command; do
			printf '+ %s\n' "$command"
		done >"$release_evidence_log"
	cat >>"$release_evidence_log" <<'EOF'
+ bash tests/conformance/verify_daxis_query_corpus_coverage.sh
Daxis query corpus compatibility coverage verified
+ bash tests/conformance/verify_daxis_contract_artifacts.sh
Daxis contract artifact manifest verified (34 artifacts)
+ bash tests/conformance/verify_daxis_strategy_document.sh
Daxis strategy document verified
+ bash tests/conformance/verify_daxis_rollout_decisions.sh
Daxis rollout decision register verified
+ bash tests/conformance/verify_daxis_operational_readiness.sh
Daxis operational readiness contract verified
+ bash tests/conformance/verify_daxis_strategy_traceability.sh
Daxis strategy traceability matrix verified
+ bash tests/conformance/verify_daxis_external_proof_attachment_test.sh
Daxis external proof attachment verifier regression coverage passed
+ bash tests/conformance/verify_daxis_external_proof_packet.sh
Daxis external proof packet verified
+ bash tests/conformance/verify_daxis_architecture_adr.sh
Daxis architecture ADR verified
+ bash tests/conformance/verify_daxis_release_attachment_test.sh
Daxis release attachment verifier regression coverage passed
+ bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh
Daxis stable-default promotion packet verifier regression coverage passed
+ bash tests/conformance/verify_daxis_release_bundle_manifest.sh
Daxis release bundle manifest verified
+ bash tests/conformance/verify_daxis_pr_checklist.sh
Daxis PR checklist verified
+ bash tests/conformance/verify_daxis_release_evidence_test.sh
Daxis release evidence runner regression coverage passed
EOF
}

run_verifier() {
	AXON_DAXIS_STABLE_DEFAULT_REPO_ROOT="$repo_root" \
		AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE="$manifest" \
		AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE="$packet" \
		"$verifier" \
		--artifact-root "$artifact_root" \
		--release-attachments "$release_attachments" \
		--proof-attachments "$proof_attachments" \
		--release-evidence-log "$release_evidence_log" \
		--release-evidence-sha256 "$(sha256_file "$release_evidence_log")" \
		--release-evidence-exit-status 0
}

expect_rejected() {
	local expected_message="$1"
	local stderr_file="$tmpdir/stderr.txt"
	if run_verifier 2>"$stderr_file"; then
		echo "expected stable-default promotion packet to be rejected: $expected_message" >&2
		exit 1
	fi
	if ! rg -q "$expected_message" "$stderr_file"; then
		echo "expected rejection message not found: $expected_message" >&2
		cat "$stderr_file" >&2
		exit 1
	fi
}

write_manifest
write_packet
write_attachment_directories
write_release_evidence_log
run_verifier

write_attachment_directories
write_release_evidence_log
rm "$proof_attachments/daxis_frontend_flow.md"
python3 - "$packet" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
packet = json.loads(path.read_text(encoding="utf-8"))
packet["stableDefaultPromotionGate"]["requiredExternalProofItemIds"].remove(
    "daxis_frontend_flow"
)
path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_rejected "stable-default promotion packet requiredExternalProofItemIds must include every stable-default proof item"

write_packet
write_attachment_directories
write_release_evidence_log
rm "$proof_attachments/daxis_frontend_flow.md"
python3 - "$packet" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
packet = json.loads(path.read_text(encoding="utf-8"))
packet["externalItems"] = [
    item
    for item in packet["externalItems"]
    if item["id"] != "daxis_frontend_flow"
]
packet["stableDefaultPromotionGate"]["requiredExternalProofItemIds"].remove(
    "daxis_frontend_flow"
)
path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
expect_rejected "stable-default promotion packet externalItems must include every stable-default proof item"

write_packet
write_attachment_directories
write_release_evidence_log
for candidate in "$proof_attachments"/*.md; do
	perl -0pi -e 's/axon_release_commit_sha: 1111111111111111111111111111111111111111/axon_release_commit_sha: 3333333333333333333333333333333333333333/' \
		"$candidate"
done
expect_rejected "stable-default promotion packet Axon release commit mismatch"

write_attachment_directories
write_release_evidence_log
perl -0pi -e 's#release_ref: refs/tags/axon-v1\.0\.0#release_ref: refs/tags/axon-v1.0.1#' \
	"$release_attachments/worker_artifact_size.md"
expect_rejected "stable-default release attachments must share one release_ref"

write_attachment_directories
write_release_evidence_log
for candidate in "$proof_attachments"/*.md; do
	item_id="$(basename "$candidate" .md)"
	review_path="$artifact_root/proof/$item_id/dirty-worktree-review.json"
	old_review_sha="$(sha256_file "$review_path")"
	python3 - "$review_path" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
review = json.loads(path.read_text(encoding="utf-8"))
review["rollout_segment"] = "workspace-alpha"
path.write_text(json.dumps(review, sort_keys=True) + "\n", encoding="utf-8")
PY
	new_review_sha="$(sha256_file "$review_path")"
	perl -0pi -e 's/rollout_segment: all-segments/rollout_segment: workspace-alpha/' \
		"$candidate"
	perl -0pi -e "s/$old_review_sha/$new_review_sha/" "$candidate"
done
expect_rejected "stable-default promotion packet rollout segment mismatch"

write_packet
write_attachment_directories
write_release_evidence_log
python3 - "$packet" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
packet = json.loads(path.read_text(encoding="utf-8"))
packet["proofAttachmentSchema"]["acceptedDaxisWorktreeReviews"].append(
    "dirty_rejected"
)
path.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
for candidate in "$proof_attachments"/*.md; do
	item_id="$(basename "$candidate" .md)"
	review_path="$artifact_root/proof/$item_id/dirty-worktree-review.json"
	old_review_sha="$(sha256_file "$review_path")"
	python3 - "$review_path" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
review = json.loads(path.read_text(encoding="utf-8"))
review["daxis_worktree_review"] = "dirty_rejected"
path.write_text(json.dumps(review, sort_keys=True) + "\n", encoding="utf-8")
PY
	new_review_sha="$(sha256_file "$review_path")"
	perl -0pi -e 's/daxis_worktree_review: dirty_reviewed/daxis_worktree_review: dirty_rejected/' \
		"$candidate"
	perl -0pi -e "s/$old_review_sha/$new_review_sha/" "$candidate"
done
expect_rejected "stable-default promotion packet acceptedDaxisWorktreeReviews must accept clean and dirty_reviewed only"

write_packet
write_attachment_directories
write_release_evidence_log
expected_sha="$(sha256_file "$release_evidence_log")"
printf 'tampered release evidence\n' >>"$release_evidence_log"
if AXON_DAXIS_STABLE_DEFAULT_REPO_ROOT="$repo_root" \
	AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE="$manifest" \
	AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE="$packet" \
	"$verifier" \
	--artifact-root "$artifact_root" \
	--release-attachments "$release_attachments" \
	--proof-attachments "$proof_attachments" \
	--release-evidence-log "$release_evidence_log" \
	--release-evidence-sha256 "$expected_sha" \
	--release-evidence-exit-status 0 2>"$tmpdir/stderr.txt"; then
	echo "expected stable-default promotion packet with mismatched release evidence digest to be rejected" >&2
	exit 1
fi
if ! rg -q "release evidence log SHA-256 does not match expected digest" "$tmpdir/stderr.txt"; then
	echo "expected release evidence digest mismatch rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_release_evidence_log
if AXON_DAXIS_STABLE_DEFAULT_REPO_ROOT="$repo_root" \
	AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE="$manifest" \
	AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE="$packet" \
	"$verifier" \
	--artifact-root "$artifact_root" \
	--release-attachments "$release_attachments" \
	--proof-attachments "$proof_attachments" \
	--release-evidence-log "$release_evidence_log" \
	--release-evidence-sha256 "$(sha256_file "$release_evidence_log")" \
	--release-evidence-exit-status 1 2>"$tmpdir/stderr.txt"; then
	echo "expected stable-default promotion packet with nonzero release evidence status to be rejected" >&2
	exit 1
fi
if ! rg -q "release evidence exit status must be 0" "$tmpdir/stderr.txt"; then
	echo "expected release evidence status rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_release_evidence_log
perl -0pi -e 's/Daxis release bundle manifest verified\n//' "$release_evidence_log"
expect_rejected "release evidence log is missing required marker: Daxis release bundle manifest verified"

write_release_evidence_log
perl -0pi -e 's/Daxis external proof packet verified\n//' "$release_evidence_log"
expect_rejected "release evidence log is missing required marker: Daxis external proof packet verified"

write_release_evidence_log
perl -0pi -e 's/^\+ cargo fmt --check\n//m' "$release_evidence_log"
expect_rejected "release evidence log is missing listed command: cargo fmt --check"

write_release_evidence_log
rm "$proof_attachments/daxis_frontend_flow.md"
expect_rejected "missing stable-default proof attachment item: daxis_frontend_flow"

echo "Daxis stable-default promotion packet verifier regression coverage passed"
