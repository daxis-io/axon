#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_STABLE_DEFAULT_REPO_ROOT:-$(pwd)}"
manifest_file="${AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE:-docs/release-gates/daxis-release-bundle-manifest.json}"
packet_file="${AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE:-docs/release-gates/daxis-external-proof-packet.json}"
artifact_root=""
release_attachments=""
proof_attachments=""
release_evidence_log=""
release_evidence_sha256=""
release_evidence_exit_status=""

usage() {
	cat >&2 <<'EOF'
usage:
  verify_daxis_stable_default_promotion_packet.sh \
    --artifact-root path/to/artifacts \
    --release-attachments path/to/completed-release-attachments \
    --proof-attachments path/to/completed-proof-attachments \
    --release-evidence-log path/to/release-evidence.log \
    --release-evidence-sha256 <64-char-lowercase-hex-sha256> \
    --release-evidence-exit-status 0
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--artifact-root)
		artifact_root="${2:-}"
		shift 2
		;;
	--release-attachments)
		release_attachments="${2:-}"
		shift 2
		;;
	--proof-attachments)
		proof_attachments="${2:-}"
		shift 2
		;;
	--release-evidence-log)
		release_evidence_log="${2:-}"
		shift 2
		;;
	--release-evidence-sha256)
		release_evidence_sha256="${2:-}"
		shift 2
		;;
	--release-evidence-exit-status)
		release_evidence_exit_status="${2:-}"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	--*)
		echo "unknown option: $1" >&2
		usage
		exit 1
		;;
	*)
		echo "unexpected argument: $1" >&2
		usage
		exit 1
		;;
	esac
done

require_arg() {
	local value="$1"
	local label="$2"
	if [[ -z "$value" ]]; then
		echo "missing required argument: $label" >&2
		usage
		exit 1
	fi
}

resolve_path() {
	local value="$1"
	if [[ "$value" = /* ]]; then
		printf "%s\n" "$value"
		return
	fi

	printf "%s/%s\n" "$(pwd)" "$value"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
release_attachment_validator="$script_dir/verify_daxis_release_attachment.sh"
proof_attachment_validator="$script_dir/verify_daxis_external_proof_attachment.sh"
release_evidence_runner="$script_dir/verify_daxis_release_evidence.sh"

require_arg "$artifact_root" "--artifact-root"
require_arg "$release_attachments" "--release-attachments"
require_arg "$proof_attachments" "--proof-attachments"
require_arg "$release_evidence_log" "--release-evidence-log"
require_arg "$release_evidence_sha256" "--release-evidence-sha256"
require_arg "$release_evidence_exit_status" "--release-evidence-exit-status"

artifact_root_path="$(resolve_path "$artifact_root")"
release_attachments_path="$(resolve_path "$release_attachments")"
proof_attachments_path="$(resolve_path "$proof_attachments")"
release_evidence_log_path="$(resolve_path "$release_evidence_log")"

if [[ ! -d "$artifact_root_path" ]]; then
	echo "missing stable-default artifact root: $artifact_root" >&2
	exit 1
fi
if [[ ! -d "$release_attachments_path" ]]; then
	echo "missing stable-default release attachment directory: $release_attachments" >&2
	exit 1
fi
if [[ ! -d "$proof_attachments_path" ]]; then
	echo "missing stable-default proof attachment directory: $proof_attachments" >&2
	exit 1
fi
if [[ ! -f "$release_evidence_log_path" ]]; then
	echo "missing stable-default release evidence log: $release_evidence_log" >&2
	exit 1
fi
if [[ ! -f "$release_evidence_runner" ]]; then
	echo "missing Daxis release evidence runner: $release_evidence_runner" >&2
	exit 1
fi

python3 - "$repo_root" "$manifest_file" "$packet_file" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1]).resolve()
manifest_file = sys.argv[2]
packet_file = sys.argv[3]


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def resolve_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return repo_root / path


def load_json(path: Path, label: str) -> dict:
    if not path.is_file():
        fail(f"missing {label}: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        fail(f"{label} must be valid JSON: {error}")
    expect(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def expect_text_list(values: object, label: str) -> list[str]:
    expect(isinstance(values, list), f"{label} must be a list")
    normalized: list[str] = []
    for value in values:
        expect(isinstance(value, str) and value.strip(), f"{label} contains an invalid entry")
        normalized.append(value)
    duplicates = sorted(value for value in set(normalized) if normalized.count(value) > 1)
    expect(not duplicates, f"{label} has duplicate entries: {', '.join(duplicates)}")
    return normalized


manifest = load_json(resolve_path(manifest_file), "stable-default release bundle manifest")
packet = load_json(resolve_path(packet_file), "stable-default external proof packet")

expect(
    manifest.get("manifest") == "daxis_release_bundle_manifest",
    "stable-default promotion packet release manifest id mismatch",
)
expect(
    packet.get("packet") == "daxis_external_proof_packet",
    "stable-default promotion packet external proof packet id mismatch",
)

expected_release_process_items = [
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
]
schema = manifest.get("releaseAttachmentSchema", {})
expect(isinstance(schema, dict), "stable-default promotion packet releaseAttachmentSchema is required")
release_process_items = expect_text_list(
    schema.get("releaseProcessItemIds", []),
    "stable-default promotion packet releaseProcessItemIds",
)
expect(
    release_process_items == expected_release_process_items,
    "stable-default promotion packet releaseProcessItemIds must require every release-process item",
)

proof_schema = packet.get("proofAttachmentSchema", {})
expect(isinstance(proof_schema, dict), "stable-default promotion packet proofAttachmentSchema is required")
accepted_worktree_reviews = expect_text_list(
    proof_schema.get("acceptedDaxisWorktreeReviews", []),
    "stable-default promotion packet acceptedDaxisWorktreeReviews",
)
expect(
    accepted_worktree_reviews == ["clean", "dirty_reviewed"],
    "stable-default promotion packet acceptedDaxisWorktreeReviews must accept clean and dirty_reviewed only",
)

stable_gate = packet.get("stableDefaultPromotionGate", {})
expect(isinstance(stable_gate, dict), "stable-default promotion packet stableDefaultPromotionGate is required")
gate_release_process_items = expect_text_list(
    stable_gate.get("requiredReleaseProcessAttachments", []),
    "stable-default promotion packet requiredReleaseProcessAttachments",
)
expect(
    gate_release_process_items == release_process_items,
    "stable-default promotion packet requiredReleaseProcessAttachments must match releaseProcessItemIds",
)

expected_external_proof_items = [
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

external_items = packet.get("externalItems", [])
expect(
    isinstance(external_items, list) and external_items,
    "stable-default promotion packet externalItems must not be empty",
)
external_item_ids: list[str] = []
for item in external_items:
    expect(isinstance(item, dict), "stable-default promotion packet externalItems must contain objects")
    item_id = item.get("id")
    expect(
        isinstance(item_id, str) and item_id.strip(),
        "stable-default promotion packet externalItems contain an invalid id",
    )
    external_item_ids.append(item_id)
expect_text_list(external_item_ids, "stable-default promotion packet externalItems ids")
expect(
    external_item_ids == expected_external_proof_items,
    "stable-default promotion packet externalItems must include every stable-default proof item",
)

required_external_item_ids = expect_text_list(
    stable_gate.get("requiredExternalProofItemIds", []),
    "stable-default promotion packet requiredExternalProofItemIds",
)
expect(
    required_external_item_ids == expected_external_proof_items,
    "stable-default promotion packet requiredExternalProofItemIds must include every stable-default proof item",
)
missing_external_items = sorted(set(external_item_ids) - set(required_external_item_ids))
extra_external_items = sorted(set(required_external_item_ids) - set(external_item_ids))
expect(
    not missing_external_items and not extra_external_items,
    "stable-default promotion packet requiredExternalProofItemIds must include every external proof item",
)
PY

python3 - "$release_evidence_log_path" "$release_evidence_sha256" "$release_evidence_exit_status" "$release_evidence_runner" "$repo_root" <<'PY'
import hashlib
import re
import shlex
import subprocess
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
expected_sha256 = sys.argv[2]
exit_status = sys.argv[3]
runner_path = Path(sys.argv[4])
repo_root = Path(sys.argv[5]).resolve()


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
    fail("release evidence SHA-256 must be a 64-character lowercase hexadecimal digest")
if exit_status != "0":
    fail("release evidence exit status must be 0")

actual_sha256 = hashlib.sha256(log_path.read_bytes()).hexdigest()
if actual_sha256 != expected_sha256:
    fail("release evidence log SHA-256 does not match expected digest")

log_text = log_path.read_text(encoding="utf-8", errors="replace")


def split_command(command: str, label: str) -> tuple[str, ...]:
    try:
        tokens = tuple(shlex.split(command))
    except ValueError as error:
        fail(f"{label} is not shell-parseable: {command}: {error}")
    if not tokens:
        fail(f"{label} is blank")
    return tokens


try:
    listed_output = subprocess.check_output(
        ["bash", str(runner_path), "--list"],
        cwd=repo_root,
        stderr=subprocess.PIPE,
        text=True,
    )
except subprocess.CalledProcessError as error:
    fail("Daxis release evidence runner --list failed: " + error.stderr.strip())

listed_commands = [
    line.strip()
    for line in listed_output.splitlines()
    if line.strip()
]
if not listed_commands:
    fail("Daxis release evidence runner --list did not return commands")

log_commands = [
    split_command(line[2:].strip(), "release evidence log command")
    for line in log_text.splitlines()
    if line.startswith("+ ") and line[2:].strip()
]
position = 0
for command in listed_commands:
    expected_tokens = split_command(command, "listed release evidence command")
    while position < len(log_commands) and log_commands[position] != expected_tokens:
        position += 1
    if position == len(log_commands):
        fail(f"release evidence log is missing listed command: {command}")
    position += 1

required_markers = [
    "Daxis query corpus compatibility coverage verified",
    "Daxis contract artifact manifest verified",
    "Daxis strategy document verified",
    "Daxis rollout decision register verified",
    "Daxis operational readiness contract verified",
    "Daxis strategy traceability matrix verified",
    "Daxis external proof attachment verifier regression coverage passed",
    "Daxis external proof packet verified",
    "Daxis architecture ADR verified",
    "Daxis release attachment verifier regression coverage passed",
    "Daxis stable-default promotion packet verifier regression coverage passed",
    "Daxis release bundle manifest verified",
    "Daxis PR checklist verified",
    "Daxis release evidence runner regression coverage passed",
]
for marker in required_markers:
    if marker not in log_text:
        fail(f"release evidence log is missing required marker: {marker}")
PY

AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT="$repo_root" \
	AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE="$manifest_file" \
	"$release_attachment_validator" \
	--artifact-root "$artifact_root_path" \
	--require-local-artifacts \
	--stable-default-dir "$release_attachments_path"
printf 'daxis_stable_default_release_attachments_verified=%s\n' "$release_attachments_path"

AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT="$repo_root" \
	AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE="$packet_file" \
	"$proof_attachment_validator" \
	--artifact-root "$artifact_root_path" \
	--require-local-artifacts \
	--stable-default-dir "$proof_attachments_path"
printf 'daxis_stable_default_proof_attachments_verified=%s\n' "$proof_attachments_path"

python3 - "$release_attachments_path" "$proof_attachments_path" <<'PY'
import re
import sys
from pathlib import Path

release_attachments = Path(sys.argv[1])
proof_attachments = Path(sys.argv[2])


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def parse_metadata(path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    bullet_pattern = re.compile(r"^\s*[-*]\s+([A-Za-z0-9_]+):\s*(.*?)\s*$")
    table_pattern = re.compile(r"^\s*\|\s*(.*?)\s*\|\s*(.*?)\s*\|\s*$")
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        bullet_match = bullet_pattern.match(line)
        if bullet_match:
            key = bullet_match.group(1)
            value = bullet_match.group(2).strip()
        else:
            table_match = table_pattern.match(line)
            if not table_match:
                continue
            raw_key = table_match.group(1).strip()
            key = raw_key.strip("`").strip()
            if key.lower() in {"field", "metadata"} or re.fullmatch(r":?-+:?", key):
                continue
            if not re.fullmatch(r"[A-Za-z0-9_]+", key):
                continue
            value = table_match.group(2).strip()
        expect(key not in metadata, f"duplicate attachment metadata field: {key} on line {line_number}")
        metadata[key] = value
    expect(metadata, f"attachment metadata missing: {path}")
    return metadata


def collect_metadata(directory: Path, label: str) -> list[dict[str, str]]:
    files = sorted(directory.glob("*.md"))
    expect(files, f"{label} directory has no Markdown attachments: {directory}")
    return [parse_metadata(path) for path in files]


def unique_value(records: list[dict[str, str]], field: str, label: str) -> str:
    values = sorted({record.get(field, "") for record in records})
    expect(all(values), f"{label} attachments missing {field}")
    expect(len(values) == 1, f"{label} attachments must share one {field}")
    return values[0]


release_records = collect_metadata(release_attachments, "stable-default release")
proof_records = collect_metadata(proof_attachments, "stable-default proof")

release_commit = unique_value(release_records, "release_commit_sha", "stable-default release")
release_ref = unique_value(release_records, "release_ref", "stable-default release")
release_channel = unique_value(release_records, "release_channel", "stable-default release")
release_rollout_segment = unique_value(release_records, "rollout_segment", "stable-default release")

proof_commit = unique_value(proof_records, "axon_release_commit_sha", "stable-default proof")
proof_ref = unique_value(proof_records, "axon_release_ref", "stable-default proof")
proof_channel = unique_value(proof_records, "release_channel", "stable-default proof")
proof_rollout_segment = unique_value(proof_records, "rollout_segment", "stable-default proof")

expect(
    release_commit == proof_commit,
    "stable-default promotion packet Axon release commit mismatch",
)
expect(
    release_ref == proof_ref,
    "stable-default promotion packet Axon release ref mismatch",
)
expect(
    release_channel == proof_channel,
    "stable-default promotion packet release channel mismatch",
)
expect(
    release_rollout_segment == proof_rollout_segment,
    "stable-default promotion packet rollout segment mismatch",
)
PY
printf 'daxis_stable_default_release_identity_verified=true\n'

printf 'daxis_stable_default_release_evidence_log_verified=%s\n' "$release_evidence_log_path"
printf 'daxis_stable_default_release_evidence_log_sha256=%s\n' "$release_evidence_sha256"
printf 'daxis_stable_default_promotion_packet_verified=true\n'
