#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT:-$(pwd)}"
packet_file="${AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE:-docs/release-gates/daxis-external-proof-packet.json}"
stable_default=false
directory_mode=false
artifact_root=""
require_local_artifacts=false

usage() {
	cat >&2 <<'EOF'
usage:
  verify_daxis_external_proof_attachment.sh [--artifact-root path/to/artifacts] [--require-local-artifacts] [--stable-default] path/to/completed-proof-attachment.md
  verify_daxis_external_proof_attachment.sh [--artifact-root path/to/artifacts] [--require-local-artifacts] --stable-default-dir path/to/completed-proof-attachments
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--artifact-root)
		if [[ $# -lt 2 || "$2" == --* ]]; then
			echo "missing argument for --artifact-root" >&2
			usage
			exit 1
		fi
		artifact_root="$2"
		shift 2
		;;
	--require-local-artifacts)
		require_local_artifacts=true
		shift
		;;
	--stable-default)
		stable_default=true
		shift
		;;
	--stable-default-dir)
		stable_default=true
		directory_mode=true
		shift
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
		break
		;;
	esac
done

if [[ $# -ne 1 ]]; then
	usage
	exit 1
fi

attachment_file="$1"

packet_path() {
	if [[ "$packet_file" = /* ]]; then
		printf "%s\n" "$packet_file"
		return
	fi

	printf "%s/%s\n" "$repo_root" "$packet_file"
}

attachment_path() {
	if [[ "$attachment_file" = /* ]]; then
		printf "%s\n" "$attachment_file"
		return
	fi

	printf "%s/%s\n" "$(pwd)" "$attachment_file"
}

artifact_root_path() {
	if [[ -z "$artifact_root" ]]; then
		printf "\n"
		return
	fi

	if [[ "$artifact_root" = /* ]]; then
		printf "%s\n" "$artifact_root"
		return
	fi

	printf "%s/%s\n" "$(pwd)" "$artifact_root"
}

path="$(packet_path)"
attachment="$(attachment_path)"
artifact_root_absolute="$(artifact_root_path)"

if [[ ! -f "$path" ]]; then
	echo "missing Daxis external proof packet: $packet_file" >&2
	exit 1
fi

if [[ -n "$artifact_root_absolute" && ! -d "$artifact_root_absolute" ]]; then
	echo "missing Daxis external proof artifact root: $artifact_root" >&2
	exit 1
fi

if [[ "$require_local_artifacts" == true && -z "$artifact_root_absolute" ]]; then
	echo "--require-local-artifacts requires --artifact-root" >&2
	exit 1
fi

if [[ "$directory_mode" == true ]]; then
	if [[ ! -d "$attachment" ]]; then
		echo "missing Daxis external proof attachment directory: $attachment_file" >&2
		exit 1
	fi
elif [[ ! -f "$attachment" ]]; then
	echo "missing Daxis external proof attachment: $attachment_file" >&2
	exit 1
fi

python3 - "$path" "$attachment" "$stable_default" "$directory_mode" "$artifact_root_absolute" "$require_local_artifacts" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

packet_path = Path(sys.argv[1])
attachment_path = Path(sys.argv[2])
stable_default = sys.argv[3] == "true"
directory_mode = sys.argv[4] == "true"
artifact_root = Path(sys.argv[5]).resolve() if sys.argv[5] else None
require_local_artifacts = sys.argv[6] == "true"

with packet_path.open(encoding="utf-8") as handle:
    packet = json.load(handle)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_text_list(values: object, label: str) -> list[str]:
    expect(isinstance(values, list), f"{label} must be a list")
    normalized: list[str] = []
    for value in values:
        expect(isinstance(value, str) and value.strip(), f"{label} contains an invalid entry")
        normalized.append(value)
    return normalized


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
    expect(metadata, "attachment metadata must use '- field: value' lines or '| `field` | value |' table rows")
    return metadata


def split_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def is_table_row(line: str) -> bool:
    return line.lstrip().startswith("|") and line.rstrip().endswith("|")


def is_separator_cells(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells)


def parse_owner_review_table(path: Path) -> dict[str, dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    owner_reviews: dict[str, dict[str, str]] = {}
    index = 0
    while index < len(lines):
        if not is_table_row(lines[index]):
            index += 1
            continue
        table_lines: list[str] = []
        while index < len(lines) and is_table_row(lines[index]):
            table_lines.append(lines[index])
            index += 1
        if len(table_lines) < 3:
            continue
        headers = [header.strip("`").strip().lower() for header in split_table_row(table_lines[0])]
        separator = split_table_row(table_lines[1])
        if not is_separator_cells(separator):
            continue
        required_headers = ["role", "name", "review state", "notes"]
        if not all(header in headers for header in required_headers):
            continue
        role_index = headers.index("role")
        name_index = headers.index("name")
        state_index = headers.index("review state")
        notes_index = headers.index("notes")
        for line in table_lines[2:]:
            cells = split_table_row(line)
            if is_separator_cells(cells):
                continue
            if len(cells) <= max(role_index, name_index, state_index, notes_index):
                continue
            role = cells[role_index].strip()
            if not role:
                continue
            expect(role not in owner_reviews, f"duplicate proof owner-review role: {role}")
            owner_reviews[role] = {
                "name": cells[name_index].strip(),
                "review_state": cells[state_index].strip(),
                "notes": cells[notes_index].strip(),
            }
    return owner_reviews


DIRTY_WORKTREE_REVIEW_METADATA = {
    "daxis_worktree_review_json_uri",
    "daxis_worktree_review_json_sha256",
}


def expect_required_metadata(metadata: dict[str, str], required_metadata: object) -> None:
    required = expect_text_list(required_metadata, "proofAttachmentSchema requiredMetadata")
    clean_worktree = metadata.get("daxis_worktree_status") == "clean"
    for field in required:
        if clean_worktree and field in DIRTY_WORKTREE_REVIEW_METADATA:
            continue
        expect(field in metadata, f"missing attachment metadata field: {field}")
        expect(metadata[field], f"attachment metadata field is blank: {field}")


def expect_lower_hex(value: str, length: int, label: str) -> None:
    expect(
        bool(re.fullmatch(rf"[0-9a-f]{{{length}}}", value)),
        f"{label} must be a {length}-character lowercase hexadecimal sha256 digest",
    )


def expect_git_sha(value: str, label: str) -> None:
    expect(
        bool(re.fullmatch(r"[0-9a-f]{40}", value)),
        f"{label} must be a 40-character lowercase hexadecimal git commit SHA",
    )


def resolve_local_artifact(uri: str, uri_field: str):
    if artifact_root is None:
        return None
    parsed = urlparse(uri)
    if parsed.scheme and parsed.scheme != "file":
        if require_local_artifacts:
            fail(f"{uri_field} must reference a local artifact when --require-local-artifacts is set: {uri}")
        return None
    expect(not parsed.query and not parsed.fragment, f"{uri_field} local artifact URI must not include a query or fragment: {uri}")
    if parsed.scheme == "file":
        expect(not parsed.netloc, f"{uri_field} file URI must not include a host: {uri}")
        raw_path = unquote(parsed.path)
    else:
        raw_path = uri
    expect(raw_path, f"{uri_field} local artifact path is blank")
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = artifact_root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(artifact_root)
    except ValueError:
        fail(f"local attachment artifact for {uri_field} must resolve under artifact root: {uri}")
    return candidate


def verify_local_artifact_checksum(uri: str, expected_sha256: str, uri_field: str, digest_field: str):
    local_path = resolve_local_artifact(uri, uri_field)
    if local_path is None:
        return None
    expect(local_path.is_file(), f"missing local attachment artifact for {uri_field}: {uri}")
    actual_sha256 = hashlib.sha256(local_path.read_bytes()).hexdigest()
    expect(actual_sha256 == expected_sha256, f"{digest_field} does not match local artifact bytes")
    return local_path


def expect_json_object(path: Path, label: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        fail(f"{label} must be valid JSON: {error}")
    expect(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def expect_json_string(summary: dict, field: str, label: str) -> str:
    value = summary.get(field)
    expect(isinstance(value, str) and value, f"{label} must include non-empty {field}")
    return value


def validate_external_state_json(metadata: dict[str, str], path: Path) -> dict:
    summary = expect_json_object(path, "Daxis external-state helper JSON")
    field_pairs = [
        ("schema_version", "daxis_external_state_schema_version"),
        ("daxis_commit_sha", "daxis_commit_sha"),
        ("daxis_ref", "daxis_ref"),
        ("daxis_origin_remote_url", "daxis_origin_remote_url"),
        ("daxis_worktree_status", "daxis_worktree_status"),
    ]
    for json_field, metadata_field in field_pairs:
        expect(
            expect_json_string(summary, json_field, "Daxis external-state helper JSON")
            == metadata[metadata_field],
            f"{metadata_field} must match helper JSON {json_field}",
        )

    worktree_status = summary["daxis_worktree_status"]
    expect(worktree_status in allowed_worktree_statuses, "helper JSON daxis_worktree_status is not allowed by proofAttachmentSchema")
    status_lines = summary.get("daxis_worktree_status_lines")
    expect(isinstance(status_lines, list), "helper JSON daxis_worktree_status_lines must be a list")
    expect(
        all(isinstance(line, str) and line for line in status_lines),
        "helper JSON daxis_worktree_status_lines must contain non-empty strings",
    )
    if worktree_status == "clean":
        expect(status_lines == [], "helper JSON clean worktree status must not include status lines")
        expect(
            summary.get("daxis_worktree_review_required") == "clean",
            "helper JSON clean worktree status must require clean review",
        )
    if worktree_status == "dirty":
        expect(status_lines, "helper JSON dirty worktree status must include status lines")
        expect(
            summary.get("daxis_worktree_review_required") == "dirty_reviewed_or_dirty_rejected",
            "helper JSON dirty worktree status must require dirty review classification",
        )

    for step_name in ("contract_test", "architecture_scan"):
        step = summary.get(step_name)
        expect(isinstance(step, dict), f"helper JSON {step_name} must be an object")
        expect(
            step.get("exit_status") == 0,
            f"helper JSON {step_name} exit_status must be 0",
        )
    return summary


def dirty_path_from_status_line(status_line: str) -> str:
    path_text = status_line[3:].strip() if len(status_line) > 3 else status_line.strip()
    if " -> " in path_text:
        path_text = path_text.split(" -> ", 1)[1]
    return path_text


def validate_dirty_worktree_review_json(
    metadata: dict[str, str],
    path: Path,
    external_state_summary: dict,
) -> None:
    review = expect_json_object(path, "Daxis dirty worktree review JSON")
    expected_string_fields = [
        ("schema_version", "daxis.dirty_worktree_review.v1"),
        ("daxis_commit_sha", metadata["daxis_commit_sha"]),
        ("daxis_ref", metadata["daxis_ref"]),
        ("daxis_origin_remote_url", metadata["daxis_origin_remote_url"]),
        ("daxis_worktree_status", metadata["daxis_worktree_status"]),
        ("daxis_worktree_review", metadata["daxis_worktree_review"]),
        ("review_state", "accepted"),
    ]
    for field, expected_value in expected_string_fields:
        expect(
            expect_json_string(review, field, "Daxis dirty worktree review JSON")
            == expected_value,
            f"dirty worktree review JSON {field} mismatch",
        )
    reviewed_at = review.get("reviewed_at")
    expect(
        isinstance(reviewed_at, str)
        and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", reviewed_at)),
        "dirty worktree review JSON reviewed_at must be an ISO-8601 UTC timestamp",
    )
    reviewer = review.get("reviewer")
    expect(
        isinstance(reviewer, str) and reviewer.strip(),
        "dirty worktree review JSON reviewer must be non-empty",
    )
    reviewer_role = review.get("reviewer_role")
    expect(
        isinstance(reviewer_role, str) and reviewer_role.strip(),
        "dirty worktree review JSON reviewer_role must be non-empty",
    )
    required_reviewer_roles = schema.get("requiredReviewerRoles", [])
    if isinstance(required_reviewer_roles, list) and required_reviewer_roles:
        expect(
            reviewer_role in required_reviewer_roles,
            "dirty worktree review JSON reviewer_role must match proofAttachmentSchema requiredReviewerRoles",
        )
    rollout_segment = review.get("rollout_segment")
    expect(
        isinstance(rollout_segment, str) and rollout_segment.strip(),
        "dirty worktree review JSON rollout_segment must be non-empty",
    )
    expect(
        rollout_segment == metadata["rollout_segment"],
        "dirty worktree review JSON rollout_segment must match attachment rollout_segment",
    )
    notes = review.get("notes")
    expect(
        isinstance(notes, str) and notes.strip(),
        "dirty worktree review JSON notes must be non-empty",
    )

    status_lines = review.get("daxis_worktree_status_lines")
    expect(
        isinstance(status_lines, list)
        and all(isinstance(line, str) and line for line in status_lines),
        "dirty worktree review JSON daxis_worktree_status_lines must contain non-empty strings",
    )
    expect(
        status_lines == external_state_summary.get("daxis_worktree_status_lines"),
        "dirty worktree review JSON status lines must match helper JSON status lines",
    )

    reviewed_paths = review.get("reviewed_paths")
    expect(
        isinstance(reviewed_paths, list)
        and all(isinstance(path_text, str) and path_text for path_text in reviewed_paths),
        "dirty worktree review JSON reviewed_paths must contain non-empty strings",
    )
    missing_paths = sorted(
        path_text
        for path_text in (dirty_path_from_status_line(line) for line in status_lines)
        if path_text not in reviewed_paths
    )
    expect(
        not missing_paths,
        "dirty worktree review JSON reviewed_paths missing dirty paths: "
        + ", ".join(missing_paths),
    )


expect(packet.get("packet") == "daxis_external_proof_packet", "invalid external proof packet id")
schema = packet.get("proofAttachmentSchema", {})
expect(isinstance(schema, dict), "proofAttachmentSchema is required")

external_items = packet.get("externalItems", [])
expect(isinstance(external_items, list) and external_items, "externalItems must not be empty")
item_by_id = {
    item.get("id"): item
    for item in external_items
    if isinstance(item, dict) and isinstance(item.get("id"), str)
}
required_environment_class = schema.get("requiredEnvironmentClass")
expect(isinstance(required_environment_class, str) and required_environment_class, "proofAttachmentSchema requiredEnvironmentClass is required")
allowed_release_channels = expect_text_list(schema.get("allowedReleaseChannels", []), "proofAttachmentSchema allowedReleaseChannels")
allowed_worktree_statuses = expect_text_list(schema.get("allowedDaxisWorktreeStatuses", []), "proofAttachmentSchema allowedDaxisWorktreeStatuses")
allowed_worktree_reviews = expect_text_list(schema.get("allowedDaxisWorktreeReviews", []), "proofAttachmentSchema allowedDaxisWorktreeReviews")
checksum_format = schema.get("checksumFormat", {})
expect(isinstance(checksum_format, dict), "proofAttachmentSchema checksumFormat is required")
checksum_field = checksum_format.get("field")
expect(checksum_field == "daxis_external_state_json_sha256", "proofAttachmentSchema checksumFormat field must be daxis_external_state_json_sha256")
expect(checksum_format.get("algorithm") == "sha256", "proofAttachmentSchema checksumFormat algorithm must be sha256")
expect(checksum_format.get("encoding") == "lowercase_hex", "proofAttachmentSchema checksumFormat encoding must be lowercase_hex")
expect(checksum_format.get("length") == 64, "proofAttachmentSchema checksumFormat length must be 64")
required_review_states = expect_text_list(schema.get("requiredReviewStates", []), "proofAttachmentSchema requiredReviewStates")
required_reviewer_roles = expect_text_list(schema.get("requiredReviewerRoles", []), "proofAttachmentSchema requiredReviewerRoles")
gate = packet.get("stableDefaultPromotionGate", {}) if stable_default else {}
if stable_default:
    expect(isinstance(gate, dict), "stableDefaultPromotionGate is required for stable-default proof attachment validation")


def validate_owner_reviews(path: Path) -> None:
    if not required_reviewer_roles:
        return
    owner_reviews = parse_owner_review_table(path)
    for role in required_reviewer_roles:
        expect(
            role in owner_reviews,
            f"missing stable-default proof owner-review role: {role}",
        )
        review = owner_reviews[role]
        expect(
            bool(review["name"].strip()),
            f"stable-default proof owner-review role {role} must include reviewer name",
        )
        expect(
            review["review_state"] in required_review_states,
            f"stable-default proof owner-review role {role} has invalid review state",
        )
        expect(
            review["review_state"] == "accepted",
            f"stable-default proof owner-review role {role} must have review state accepted",
        )
        expect(
            bool(review["notes"].strip()),
            f"stable-default proof owner-review role {role} must include notes",
        )


def validate_metadata(metadata: dict[str, str], path: Path) -> str:
    expect_required_metadata(metadata, schema.get("requiredMetadata", []))
    item_id = metadata["item_id"]
    expect(item_id in item_by_id, "item_id must reference an external proof item")
    item_milestone = item_by_id[item_id].get("milestone")
    if item_milestone:
        expect(metadata["milestone"] == item_milestone, f"milestone must match external proof item {item_id}")
    expect(
        metadata["environment_class"] == required_environment_class,
        f"environment_class must be {required_environment_class}",
    )
    expect(metadata["release_channel"] in allowed_release_channels, "release_channel is not allowed by proofAttachmentSchema")
    expect(metadata["daxis_worktree_status"] in allowed_worktree_statuses, "daxis_worktree_status is not allowed by proofAttachmentSchema")
    expect(metadata["daxis_worktree_review"] in allowed_worktree_reviews, "daxis_worktree_review is not allowed by proofAttachmentSchema")
    if metadata["daxis_worktree_status"] == "clean":
        expect(metadata["daxis_worktree_review"] == "clean", "clean Daxis worktree evidence must use daxis_worktree_review clean")
        for field in DIRTY_WORKTREE_REVIEW_METADATA:
            expect(
                not metadata.get(field, "").strip(),
                f"clean Daxis worktree evidence must not include {field}",
            )
    if metadata["daxis_worktree_status"] == "dirty":
        expect(metadata["daxis_worktree_review"] != "clean", "dirty Daxis worktree evidence must not use daxis_worktree_review clean")
    expect_lower_hex(
        metadata["daxis_external_state_json_sha256"],
        64,
        "daxis_external_state_json_sha256",
    )
    local_external_state_summary = None
    local_external_state_path = verify_local_artifact_checksum(
        metadata["daxis_external_state_json_uri"],
        metadata["daxis_external_state_json_sha256"],
        "daxis_external_state_json_uri",
        "daxis_external_state_json_sha256",
    )
    if local_external_state_path is not None:
        local_external_state_summary = validate_external_state_json(metadata, local_external_state_path)
    if metadata["daxis_worktree_status"] == "dirty":
        review_uri = metadata.get("daxis_worktree_review_json_uri", "")
        review_sha256 = metadata.get("daxis_worktree_review_json_sha256", "")
        expect(review_uri, "dirty Daxis worktree evidence requires daxis_worktree_review_json_uri")
        expect(review_sha256, "dirty Daxis worktree evidence requires daxis_worktree_review_json_sha256")
        expect_lower_hex(review_sha256, 64, "daxis_worktree_review_json_sha256")
        local_review_path = verify_local_artifact_checksum(
            review_uri,
            review_sha256,
            "daxis_worktree_review_json_uri",
            "daxis_worktree_review_json_sha256",
        )
        if local_review_path is not None and local_external_state_summary is not None:
            validate_dirty_worktree_review_json(metadata, local_review_path, local_external_state_summary)
    expect_git_sha(metadata["axon_release_commit_sha"], "axon_release_commit_sha")
    expect_git_sha(metadata["daxis_commit_sha"], "daxis_commit_sha")
    expect(
        metadata["daxis_external_state_schema_version"] == "daxis.external_state.v1",
        "daxis_external_state_schema_version must be daxis.external_state.v1",
    )
    status = metadata["exit_status_or_review_status"]
    expect(
        status in required_review_states or bool(re.fullmatch(r"[0-9]+", status)),
        "exit_status_or_review_status must be a review state or numeric exit status",
    )
    if stable_default:
        required_release_channel = gate.get("requiredReleaseChannel", "stable")
        expect(
            metadata["release_channel"] == required_release_channel,
            f"stable-default proof attachment must use release_channel {required_release_channel}",
        )
        required_review_state = gate.get("requiredReviewState", "accepted")
        expect(
            status == required_review_state,
            f"stable-default proof attachment must have exit_status_or_review_status {required_review_state}",
        )
        accepted_reviews = expect_text_list(schema.get("acceptedDaxisWorktreeReviews", []), "proofAttachmentSchema acceptedDaxisWorktreeReviews")
        expect(
            metadata["daxis_worktree_review"] in accepted_reviews,
            "stable-default proof attachment requires accepted Daxis worktree review",
        )
        validate_owner_reviews(path)
    return item_id


if directory_mode:
    expect(attachment_path.is_dir(), f"stable-default proof attachment path must be a directory: {attachment_path}")
    attachment_files = sorted(attachment_path.glob("*.md"))
    expect(attachment_files, f"stable-default proof attachment directory has no Markdown attachments: {attachment_path}")
    seen_items: dict[str, Path] = {}
    for candidate in attachment_files:
        item_id = validate_metadata(parse_metadata(candidate), candidate)
        expect(
            item_id not in seen_items,
            f"duplicate stable-default proof attachment item: {item_id}",
        )
        seen_items[item_id] = candidate
    required_items = gate.get("requiredExternalProofItemIds") if stable_default else None
    if not isinstance(required_items, list) or not required_items:
        required_items = list(item_by_id)
    for item_id in required_items:
        expect(isinstance(item_id, str) and item_id, "stableDefaultPromotionGate requiredExternalProofItemIds contains an invalid entry")
        expect(
            item_id in seen_items,
            f"missing stable-default proof attachment item: {item_id}",
        )
    print(f"daxis_external_proof_attachment_directory_verified={attachment_path}")
else:
    validate_metadata(parse_metadata(attachment_path), attachment_path)
    print(f"daxis_external_proof_attachment_verified={attachment_path}")
PY
