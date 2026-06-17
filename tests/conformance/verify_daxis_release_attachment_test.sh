#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
manifest="$repo_root/docs/release-gates/daxis-release-bundle-manifest.json"
attachment="$tmpdir/daxis-release-attachment.md"
attachment_dir="$tmpdir/daxis-release-attachments"
artifact_root="$tmpdir/artifacts"
verifier="$(pwd)/tests/conformance/verify_daxis_release_attachment.sh"

mkdir -p "$repo_root/docs/release-gates"

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
    "requiredReviewStates": ["attached", "reviewed", "accepted", "rejected"],
    "requiredReviewerRoles": [
      "Release owner",
      "Runtime / engine owner",
      "Daxis product owner",
      "Daxis query platform owner",
      "Daxis catalog/storage owner",
      "Daxis security owner",
      "Daxis SRE owner"
    ]
  }
}
JSON
}

owner_review_table() {
	cat <<'EOF'

## Owner Review

| Role                        | Name                 | Review state | Notes                  |
| --------------------------- | -------------------- | ------------ | ---------------------- |
| Release owner               | Release reviewer     | accepted     | Release item reviewed. |
| Runtime / engine owner      | Runtime reviewer     | accepted     | Runtime impact reviewed. |
| Daxis product owner         | Product reviewer     | accepted     | Product impact reviewed. |
| Daxis query platform owner  | Query reviewer       | accepted     | Query path reviewed. |
| Daxis catalog/storage owner | Catalog reviewer     | accepted     | Catalog and storage reviewed. |
| Daxis security owner        | Security reviewer    | accepted     | Security boundary reviewed. |
| Daxis SRE owner             | SRE reviewer         | accepted     | Rollback evidence reviewed. |
EOF
}

write_valid_attachment() {
	cat >"$attachment" <<'EOF'
# Completed Daxis Release Attachment

- item_id: public_gcs_live_smoke
- release_commit_sha: 1111111111111111111111111111111111111111
- release_ref: refs/tags/axon-v1.0.0
- release_channel: stable
- rollout_segment: all-segments
- owner: Release owner
- captured_at: 2026-06-07T12:00:00Z
- artifact_uri: https://axon.example.invalid/release/public-gcs-live-smoke.log
- artifact_sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
- verification_command_or_statement: AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://example npm run test:browser:public-gcs-live -- --reporter=line
- exit_status_or_review_status: accepted
- rollback_or_migration_note_uri: https://axon.example.invalid/release/server-fallback.md
EOF
	owner_review_table >>"$attachment"
}

write_valid_local_attachment() {
	local artifact_file="$artifact_root/release/public-gcs-live-smoke.log"
	local artifact_sha
	mkdir -p "$(dirname "$artifact_file")"
	printf 'public GCS live smoke accepted\n' >"$artifact_file"
	artifact_sha="$(sha256_file "$artifact_file")"
	cat >"$attachment" <<EOF
# Completed Daxis Release Attachment

- item_id: public_gcs_live_smoke
- release_commit_sha: 1111111111111111111111111111111111111111
- release_ref: refs/tags/axon-v1.0.0
- release_channel: stable
- rollout_segment: all-segments
- owner: Release owner
- captured_at: 2026-06-07T12:00:00Z
- artifact_uri: release/public-gcs-live-smoke.log
- artifact_sha256: $artifact_sha
- verification_command_or_statement: AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://example npm run test:browser:public-gcs-live -- --reporter=line
- exit_status_or_review_status: accepted
- rollback_or_migration_note_uri: release/server-fallback.md
EOF
	owner_review_table >>"$attachment"
}

write_release_attachment_file() {
	local path="$1"
	local item_id="$2"
	cat >"$path" <<EOF
# Completed Daxis Release Attachment

| Field | Value |
| ----- | ----- |
| \`item_id\` | $item_id |
| \`release_commit_sha\` | 1111111111111111111111111111111111111111 |
| \`release_ref\` | refs/tags/axon-v1.0.0 |
| \`release_channel\` | stable |
| \`rollout_segment\` | all-segments |
| \`owner\` | Release owner |
| \`captured_at\` | 2026-06-07T12:00:00Z |
| \`artifact_uri\` | https://axon.example.invalid/release/$item_id.log |
| \`artifact_sha256\` | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
| \`verification_command_or_statement\` | bash tests/conformance/verify_daxis_release_evidence.sh |
| \`exit_status_or_review_status\` | accepted |
| \`rollback_or_migration_note_uri\` | https://axon.example.invalid/release/server-fallback.md |
EOF
	owner_review_table >>"$path"
}

write_valid_attachment_directory() {
	rm -rf "$attachment_dir"
	mkdir -p "$attachment_dir"
	write_release_attachment_file "$attachment_dir/git_sha.md" "git_sha"
	write_release_attachment_file "$attachment_dir/worker_artifact_size.md" "worker_artifact_size"
	write_release_attachment_file "$attachment_dir/public_gcs_live_smoke.md" "public_gcs_live_smoke"
	write_release_attachment_file "$attachment_dir/release_notes.md" "release_notes"
	write_release_attachment_file "$attachment_dir/migration_notes.md" "migration_notes"
}

write_valid_table_attachment() {
	cat >"$attachment" <<'EOF'
# Completed Daxis Release Attachment

| Field | Value |
| ----- | ----- |
| `item_id` | public_gcs_live_smoke |
| `release_commit_sha` | 1111111111111111111111111111111111111111 |
| `release_ref` | refs/tags/axon-v1.0.0 |
| `release_channel` | stable |
| `rollout_segment` | all-segments |
| `owner` | Release owner |
| `captured_at` | 2026-06-07T12:00:00Z |
| `artifact_uri` | https://axon.example.invalid/release/public-gcs-live-smoke.log |
| `artifact_sha256` | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb |
| `verification_command_or_statement` | AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://example npm run test:browser:public-gcs-live -- --reporter=line |
| `exit_status_or_review_status` | accepted |
| `rollback_or_migration_note_uri` | https://axon.example.invalid/release/server-fallback.md |
EOF
	owner_review_table >>"$attachment"
}

run_verifier() {
	AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT="$repo_root" \
		"$verifier" --stable-default "$attachment"
}

run_verifier_with_artifact_root() {
	AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT="$repo_root" \
		"$verifier" --artifact-root "$artifact_root" --require-local-artifacts --stable-default "$attachment"
}

run_directory_verifier() {
	AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT="$repo_root" \
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

write_manifest
write_valid_attachment
run_verifier

write_valid_table_attachment
run_verifier

write_valid_local_attachment
run_verifier_with_artifact_root

write_valid_attachment
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected release attachment with remote artifact URI to be rejected in strict local-artifact mode" >&2
	exit 1
fi
if ! rg -q "artifact_uri must reference a local artifact when --require-local-artifacts is set" "$tmpdir/stderr.txt"; then
	echo "expected strict local release artifact rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
replace_attachment_text "- artifact_sha256: $(sha256_file "$artifact_root/release/public-gcs-live-smoke.log")" "- artifact_sha256: dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected release attachment with mismatched local artifact checksum to be rejected" >&2
	exit 1
fi
if ! rg -q "artifact_sha256 does not match local artifact bytes" "$tmpdir/stderr.txt"; then
	echo "expected local release artifact checksum mismatch rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_local_attachment
replace_attachment_text "- artifact_uri: release/public-gcs-live-smoke.log" "- artifact_uri: release/missing-public-gcs-live-smoke.log"
if run_verifier_with_artifact_root 2>"$tmpdir/stderr.txt"; then
	echo "expected release attachment with missing local artifact to be rejected" >&2
	exit 1
fi
if ! rg -q "missing local attachment artifact for artifact_uri: release/missing-public-gcs-live-smoke.log" "$tmpdir/stderr.txt"; then
	echo "expected missing local release artifact rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment_directory
run_directory_verifier

write_valid_attachment_directory
rm "$attachment_dir/migration_notes.md"
if run_directory_verifier 2>"$tmpdir/stderr.txt"; then
	echo "expected release attachment directory missing required item to be rejected" >&2
	exit 1
fi
if ! rg -q "missing stable-default release attachment item: migration_notes" "$tmpdir/stderr.txt"; then
	echo "expected missing release attachment item rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment_directory
cp "$attachment_dir/git_sha.md" "$attachment_dir/duplicate_git_sha.md"
if run_directory_verifier 2>"$tmpdir/stderr.txt"; then
	echo "expected duplicate release attachment item to be rejected" >&2
	exit 1
fi
if ! rg -q "duplicate stable-default release attachment item: git_sha" "$tmpdir/stderr.txt"; then
	echo "expected duplicate release attachment item rejection message" >&2
	cat "$tmpdir/stderr.txt" >&2
	exit 1
fi

write_valid_attachment
replace_attachment_text "| Daxis security owner        | Security reviewer    | accepted     | Security boundary reviewed. |" ""
expect_rejected "missing stable-default release owner-review role: Daxis security owner"

write_valid_attachment
replace_attachment_text "- release_channel: stable" "- release_channel: candidate"
expect_rejected "stable-default release attachment must use release_channel stable"

write_valid_attachment
replace_attachment_text "- artifact_sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" "- artifact_sha256: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
expect_rejected "artifact_sha256 must be a 64-character lowercase hexadecimal sha256 digest"

write_valid_attachment
replace_attachment_text "- exit_status_or_review_status: accepted" "- exit_status_or_review_status: reviewed"
expect_rejected "stable-default release attachment must have exit_status_or_review_status accepted"

write_valid_attachment
replace_attachment_text "- item_id: public_gcs_live_smoke" "- item_id: dependency_guardrail_result"
expect_rejected "item_id must reference a release-process attachment item"

write_valid_attachment
replace_attachment_text "- release_commit_sha: 1111111111111111111111111111111111111111" "- release_commit_sha: 1111"
expect_rejected "release_commit_sha must be a 40-character lowercase hexadecimal git commit SHA"

echo "Daxis release attachment verifier regression coverage passed"
