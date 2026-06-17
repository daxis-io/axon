#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"
attachment="$tmpdir/daxis-external-proof-attachment.md"
attachment_dir="$tmpdir/daxis-external-proof-attachments"
artifact_root="$tmpdir/artifacts"
verifier="$(pwd)/tests/conformance/verify_daxis_external_proof_attachment.sh"

mkdir -p "$repo_root/docs/release-gates"

sha256_file() {
	shasum -a 256 "$1" | awk '{print $1}'
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
      "daxis_external_state_json_sha256",
      "daxis_worktree_review_json_uri",
      "daxis_worktree_review_json_sha256"
    ],
    "requiredEnvironmentClass": "production",
    "allowedReleaseChannels": ["experimental", "integration", "candidate", "stable"],
    "allowedDaxisWorktreeStatuses": ["clean", "dirty"],
    "allowedDaxisWorktreeReviews": ["clean", "dirty_reviewed", "dirty_rejected"],
    "acceptedDaxisWorktreeReviews": ["clean", "dirty_reviewed"],
    "checksumFormat": {
      "field": "daxis_external_state_json_sha256",
      "algorithm": "sha256",
      "encoding": "lowercase_hex",
      "length": 64,
      "sourceBytes": "exact_helper_json_bytes"
    },
    "requiredReviewStates": ["attached", "reviewed", "accepted", "rejected"],
    "requiredReviewerRoles": [
      "Daxis platform owner",
      "Daxis security owner"
    ]
  },
  "externalItems": [
    {
      "id": "daxis_descriptor_endpoint",
      "milestone": "M1"
    },
    {
      "id": "daxis_frontend_flow",
      "milestone": "M1"
    }
  ],
  "stableDefaultPromotionGate": {
    "requiredReviewState": "accepted",
    "requiredReleaseChannel": "stable"
  }
}
JSON
}

owner_review_table() {
	cat <<'EOF'

## Review

| Role                  | Name                  | Review state | Notes                    |
| --------------------- | --------------------- | ------------ | ------------------------ |
| Daxis platform owner  | Daxis platform reviewer | accepted   | Platform proof reviewed. |
| Daxis security owner  | Daxis security reviewer | accepted   | Security proof reviewed. |
EOF
}

write_valid_attachment() {
	cat >"$attachment" <<'EOF'
# Completed Daxis External Proof Attachment

- item_id: daxis_descriptor_endpoint
- milestone: M1
- owner: Daxis gateway owner
- captured_at: 2026-06-07T12:00:00Z
- environment: production-us
- environment_class: production
- release_channel: stable
- axon_release_commit_sha: 1111111111111111111111111111111111111111
- axon_release_ref: refs/tags/axon-v1.0.0
- rollout_segment: all-segments
- artifact_uri: https://daxis.example.invalid/proof/daxis-descriptor-endpoint
- verification_command_or_url: cargo test -p daxis-query --test contracts
- exit_status_or_review_status: accepted
- rollback_evidence_uri: https://daxis.example.invalid/proof/server-fallback
- daxis_commit_sha: 2222222222222222222222222222222222222222
- daxis_ref: main
- daxis_origin_remote_url: git@github.com:daxis/daxis-platform.git
- daxis_worktree_status: dirty
- daxis_worktree_review: dirty_reviewed
- daxis_external_state_schema_version: daxis.external_state.v1
- daxis_external_state_json_uri: https://daxis.example.invalid/proof/external-state.json
- daxis_external_state_json_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
- daxis_worktree_review_json_uri: https://daxis.example.invalid/proof/dirty-worktree-review.json
- daxis_worktree_review_json_sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
EOF
	owner_review_table >>"$attachment"
}

write_valid_local_attachment() {
	local external_state_file="$artifact_root/proof/external-state.json"
	local worktree_review_file="$artifact_root/proof/dirty-worktree-review.json"
	local external_state_sha
	local worktree_review_sha
	mkdir -p "$(dirname "$external_state_file")"
	cat >"$external_state_file" <<'JSON'
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
	cat >"$worktree_review_file" <<'JSON'
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
	external_state_sha="$(sha256_file "$external_state_file")"
	worktree_review_sha="$(sha256_file "$worktree_review_file")"
	cat >"$attachment" <<EOF
# Completed Daxis External Proof Attachment

- item_id: daxis_descriptor_endpoint
- milestone: M1
- owner: Daxis gateway owner
- captured_at: 2026-06-07T12:00:00Z
- environment: production-us
- environment_class: production
- release_channel: stable
- axon_release_commit_sha: 1111111111111111111111111111111111111111
- axon_release_ref: refs/tags/axon-v1.0.0
- rollout_segment: all-segments
- artifact_uri: proof/daxis-descriptor-endpoint
- verification_command_or_url: cargo test -p daxis-query --test contracts
- exit_status_or_review_status: accepted
- rollback_evidence_uri: proof/server-fallback.md
- daxis_commit_sha: 2222222222222222222222222222222222222222
- daxis_ref: main
- daxis_origin_remote_url: git@github.com:daxis/daxis-platform.git
- daxis_worktree_status: dirty
- daxis_worktree_review: dirty_reviewed
- daxis_external_state_schema_version: daxis.external_state.v1
- daxis_external_state_json_uri: proof/external-state.json
- daxis_external_state_json_sha256: $external_state_sha
- daxis_worktree_review_json_uri: proof/dirty-worktree-review.json
- daxis_worktree_review_json_sha256: $worktree_review_sha
EOF
	owner_review_table >>"$attachment"
}

write_clean_local_attachment() {
	local external_state_file="$artifact_root/proof/external-state.json"
	local external_state_sha
	rm -f "$artifact_root/proof/dirty-worktree-review.json"
	mkdir -p "$(dirname "$external_state_file")"
	cat >"$external_state_file" <<'JSON'
{
  "schema_version": "daxis.external_state.v1",
  "daxis_commit_sha": "2222222222222222222222222222222222222222",
  "daxis_ref": "main",
  "daxis_origin_remote_url": "git@github.com:daxis/daxis-platform.git",
  "daxis_worktree_status": "clean",
  "daxis_worktree_status_lines": [],
  "daxis_worktree_review_required": "clean",
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
	external_state_sha="$(sha256_file "$external_state_file")"
	cat >"$attachment" <<EOF
# Completed Daxis External Proof Attachment

- item_id: daxis_descriptor_endpoint
- milestone: M1
- owner: Daxis gateway owner
- captured_at: 2026-06-07T12:00:00Z
- environment: production-us
- environment_class: production
- release_channel: stable
- axon_release_commit_sha: 1111111111111111111111111111111111111111
- axon_release_ref: refs/tags/axon-v1.0.0
- rollout_segment: all-segments
- artifact_uri: proof/daxis-descriptor-endpoint
- verification_command_or_url: cargo test -p daxis-query --test contracts
- exit_status_or_review_status: accepted
- rollback_evidence_uri: proof/server-fallback.md
- daxis_commit_sha: 2222222222222222222222222222222222222222
- daxis_ref: main
- daxis_origin_remote_url: git@github.com:daxis/daxis-platform.git
- daxis_worktree_status: clean
- daxis_worktree_review: clean
- daxis_external_state_schema_version: daxis.external_state.v1
- daxis_external_state_json_uri: proof/external-state.json
- daxis_external_state_json_sha256: $external_state_sha
EOF
	owner_review_table >>"$attachment"
}

write_external_attachment_file() {
	local path="$1"
	local item_id="$2"
	local milestone="$3"
	cat >"$path" <<EOF
# Completed Daxis External Proof Attachment

| Field | Value |
| ----- | ----- |
| \`item_id\` | $item_id |
| \`milestone\` | $milestone |
| \`owner\` | Daxis gateway owner |
| \`captured_at\` | 2026-06-07T12:00:00Z |
| \`environment\` | production-us |
| \`environment_class\` | production |
| \`release_channel\` | stable |
| \`axon_release_commit_sha\` | 1111111111111111111111111111111111111111 |
| \`axon_release_ref\` | refs/tags/axon-v1.0.0 |
| \`rollout_segment\` | all-segments |
| \`artifact_uri\` | https://daxis.example.invalid/proof/$item_id |
| \`verification_command_or_url\` | cargo test -p daxis-query --test contracts |
| \`exit_status_or_review_status\` | accepted |
| \`rollback_evidence_uri\` | https://daxis.example.invalid/proof/server-fallback |
| \`daxis_commit_sha\` | 2222222222222222222222222222222222222222 |
| \`daxis_ref\` | main |
| \`daxis_origin_remote_url\` | git@github.com:daxis/daxis-platform.git |
| \`daxis_worktree_status\` | dirty |
| \`daxis_worktree_review\` | dirty_reviewed |
| \`daxis_external_state_schema_version\` | daxis.external_state.v1 |
| \`daxis_external_state_json_uri\` | https://daxis.example.invalid/proof/external-state.json |
| \`daxis_external_state_json_sha256\` | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
| \`daxis_worktree_review_json_uri\` | https://daxis.example.invalid/proof/dirty-worktree-review.json |
| \`daxis_worktree_review_json_sha256\` | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
EOF
	owner_review_table >>"$path"
}

write_valid_attachment_directory() {
	rm -rf "$attachment_dir"
	mkdir -p "$attachment_dir"
	write_external_attachment_file "$attachment_dir/daxis_descriptor_endpoint.md" "daxis_descriptor_endpoint" "M1"
	write_external_attachment_file "$attachment_dir/daxis_frontend_flow.md" "daxis_frontend_flow" "M1"
}

write_valid_table_attachment() {
	cat >"$attachment" <<'EOF'
# Completed Daxis External Proof Attachment

| Field | Value |
| ----- | ----- |
| `item_id` | daxis_descriptor_endpoint |
| `milestone` | M1 |
| `owner` | Daxis gateway owner |
| `captured_at` | 2026-06-07T12:00:00Z |
| `environment` | production-us |
| `environment_class` | production |
| `release_channel` | stable |
| `axon_release_commit_sha` | 1111111111111111111111111111111111111111 |
| `axon_release_ref` | refs/tags/axon-v1.0.0 |
| `rollout_segment` | all-segments |
| `artifact_uri` | https://daxis.example.invalid/proof/daxis-descriptor-endpoint |
| `verification_command_or_url` | cargo test -p daxis-query --test contracts |
| `exit_status_or_review_status` | accepted |
| `rollback_evidence_uri` | https://daxis.example.invalid/proof/server-fallback |
| `daxis_commit_sha` | 2222222222222222222222222222222222222222 |
| `daxis_ref` | main |
| `daxis_origin_remote_url` | git@github.com:daxis/daxis-platform.git |
| `daxis_worktree_status` | dirty |
| `daxis_worktree_review` | dirty_reviewed |
| `daxis_external_state_schema_version` | daxis.external_state.v1 |
| `daxis_external_state_json_uri` | https://daxis.example.invalid/proof/external-state.json |
| `daxis_external_state_json_sha256` | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
| `daxis_worktree_review_json_uri` | https://daxis.example.invalid/proof/dirty-worktree-review.json |
| `daxis_worktree_review_json_sha256` | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
EOF
	owner_review_table >>"$attachment"
}

run_verifier() {
	AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT="$repo_root" \
		"$verifier" --stable-default "$attachment"
}

run_verifier_with_artifact_root() {
	AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT="$repo_root" \
		"$verifier" --artifact-root "$artifact_root" --require-local-artifacts --stable-default "$attachment"
}

run_directory_verifier() {
	AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT="$repo_root" \
		"$verifier" --stable-default-dir "$attachment_dir"
}

expect_rejected() {
	local expected_message="$1"
	local stderr_file="$tmpdir/stderr.txt"
	if run_verifier 2>"$stderr_file"; then
		echo "expected attachment to be rejected: $expected_message" >&2
		exit 1
	fi
	if ! rg -q "$expected_message" "$stderr_file"; then
		echo "expected rejection message not found: $expected_message" >&2
		cat "$stderr_file" >&2
		exit 1
	fi
}

replace_attachment_text() {
	python3 - "$attachment" "$1" "$2" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
old = sys.argv[2]
new = sys.argv[3]
text = path.read_text(encoding="utf-8")
if old not in text:
    raise SystemExit(f"missing fixture text: {old}")
path.write_text(text.replace(old, new), encoding="utf-8")
PY
}

write_packet
write_valid_attachment
run_verifier

write_valid_table_attachment
run_verifier

write_valid_local_attachment
run_verifier_with_artifact_root

write_clean_local_attachment
run_verifier_with_artifact_root

write_valid_local_attachment
old_external_state_sha="$(sha256_file "$artifact_root/proof/external-state.json")"
python3 - "$artifact_root/proof/external-state.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
summary = json.loads(path.read_text(encoding="utf-8"))
summary["daxis_commit_sha"] = "3333333333333333333333333333333333333333"
path.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
PY
new_external_state_sha="$(sha256_file "$artifact_root/proof/external-state.json")"
replace_attachment_text "- daxis_external_state_json_sha256: $old_external_state_sha" "- daxis_external_state_json_sha256: $new_external_state_sha"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with helper JSON commit mismatch to be rejected" >&2
	exit 1
fi
if ! rg -q "daxis_commit_sha must match helper JSON daxis_commit_sha" "$tmpdir/stderr.txt"; then
	echo "expected helper JSON commit mismatch rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
old_external_state_sha="$(sha256_file "$artifact_root/proof/external-state.json")"
python3 - "$artifact_root/proof/external-state.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
summary = json.loads(path.read_text(encoding="utf-8"))
summary["contract_test"]["exit_status"] = 1
path.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
PY
new_external_state_sha="$(sha256_file "$artifact_root/proof/external-state.json")"
replace_attachment_text "- daxis_external_state_json_sha256: $old_external_state_sha" "- daxis_external_state_json_sha256: $new_external_state_sha"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with failed helper JSON contract test to be rejected" >&2
	exit 1
fi
if ! rg -q "helper JSON contract_test exit_status must be 0" "$tmpdir/stderr.txt"; then
	echo "expected helper JSON contract-test failure rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
old_worktree_review_sha="$(sha256_file "$artifact_root/proof/dirty-worktree-review.json")"
python3 - "$artifact_root/proof/dirty-worktree-review.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
review = json.loads(path.read_text(encoding="utf-8"))
review["daxis_worktree_status_lines"] = ["?? unrelated-proof.md"]
review["reviewed_paths"] = ["unrelated-proof.md"]
path.write_text(json.dumps(review, sort_keys=True) + "\n", encoding="utf-8")
PY
new_worktree_review_sha="$(sha256_file "$artifact_root/proof/dirty-worktree-review.json")"
replace_attachment_text "- daxis_worktree_review_json_sha256: $old_worktree_review_sha" "- daxis_worktree_review_json_sha256: $new_worktree_review_sha"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with mismatched dirty worktree review JSON to be rejected" >&2
	exit 1
fi
if ! rg -q "dirty worktree review JSON status lines must match helper JSON status lines" "$tmpdir/stderr.txt"; then
	echo "expected dirty worktree review status-line mismatch rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
old_worktree_review_sha="$(sha256_file "$artifact_root/proof/dirty-worktree-review.json")"
python3 - "$artifact_root/proof/dirty-worktree-review.json" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
review = json.loads(path.read_text(encoding="utf-8"))
review.pop("reviewer")
path.write_text(json.dumps(review, sort_keys=True) + "\n", encoding="utf-8")
PY
new_worktree_review_sha="$(sha256_file "$artifact_root/proof/dirty-worktree-review.json")"
replace_attachment_text "- daxis_worktree_review_json_sha256: $old_worktree_review_sha" "- daxis_worktree_review_json_sha256: $new_worktree_review_sha"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with unattributed dirty worktree review JSON to be rejected" >&2
	exit 1
fi
if ! rg -q "dirty worktree review JSON reviewer must be non-empty" "$tmpdir/stderr.txt"; then
	echo "expected dirty worktree review reviewer attribution rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with remote helper JSON URI to be rejected in strict local-artifact mode" >&2
	exit 1
fi
if ! rg -q "daxis_external_state_json_uri must reference a local artifact when --require-local-artifacts is set" "$tmpdir/stderr.txt"; then
	echo "expected strict local proof artifact rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
replace_attachment_text "- daxis_external_state_json_sha256: $(sha256_file "$artifact_root/proof/external-state.json")" "- daxis_external_state_json_sha256: cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with mismatched local artifact checksum to be rejected" >&2
	exit 1
fi
if ! rg -q "daxis_external_state_json_sha256 does not match local artifact bytes" "$tmpdir/stderr.txt"; then
	echo "expected local proof artifact checksum mismatch rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
replace_attachment_text "- daxis_external_state_json_uri: proof/external-state.json" "- daxis_external_state_json_uri: proof/missing-external-state.json"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment with missing local artifact to be rejected" >&2
	exit 1
fi
if ! rg -q "missing local attachment artifact for daxis_external_state_json_uri: proof/missing-external-state.json" "$tmpdir/stderr.txt"; then
	echo "expected missing local proof artifact rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment_directory
run_directory_verifier

write_valid_attachment_directory
rm "$attachment_dir/daxis_frontend_flow.md"
if run_directory_verifier 2>"$tmpdir/stderr.txt"; then
	echo "expected proof attachment directory missing required item to be rejected" >&2
	exit 1
fi
if ! rg -q "missing stable-default proof attachment item: daxis_frontend_flow" "$tmpdir/stderr.txt"; then
	echo "expected missing proof attachment item rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment_directory
cp "$attachment_dir/daxis_descriptor_endpoint.md" "$attachment_dir/duplicate_descriptor.md"
if run_directory_verifier 2>"$tmpdir/stderr.txt"; then
	echo "expected duplicate proof attachment item to be rejected" >&2
	exit 1
fi
if ! rg -q "duplicate stable-default proof attachment item: daxis_descriptor_endpoint" "$tmpdir/stderr.txt"; then
	echo "expected duplicate proof attachment item rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment
replace_attachment_text "| Daxis security owner  | Daxis security reviewer | accepted   | Security proof reviewed. |" ""
expect_rejected "missing stable-default proof owner-review role: Daxis security owner"

write_valid_attachment
replace_attachment_text "- release_channel: stable" "- release_channel: candidate"
expect_rejected "stable-default proof attachment must use release_channel stable"

write_valid_attachment
replace_attachment_text "- environment_class: production" "- environment_class: staging"
expect_rejected "environment_class must be production"

write_valid_attachment
replace_attachment_text "- daxis_worktree_review: dirty_reviewed" "- daxis_worktree_review: dirty_rejected"
expect_rejected "stable-default proof attachment requires accepted Daxis worktree review"

write_valid_attachment
replace_attachment_text "- daxis_external_state_json_sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" "- daxis_external_state_json_sha256: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
expect_rejected "daxis_external_state_json_sha256 must be a 64-character lowercase hexadecimal sha256 digest"

write_valid_attachment
replace_attachment_text "- exit_status_or_review_status: accepted" "- exit_status_or_review_status: reviewed"
expect_rejected "stable-default proof attachment must have exit_status_or_review_status accepted"

write_valid_attachment
replace_attachment_text "- daxis_external_state_schema_version: daxis.external_state.v1" "- daxis_external_state_schema_version: daxis.external_state.v0"
expect_rejected "daxis_external_state_schema_version must be daxis.external_state.v1"

echo "Daxis external proof attachment verifier regression coverage passed"
