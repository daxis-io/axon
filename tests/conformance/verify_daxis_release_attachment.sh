#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT:-$(pwd)}"
manifest_file="${AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE:-docs/release-gates/daxis-release-bundle-manifest.json}"
stable_default=false
directory_mode=false
artifact_root=""
require_local_artifacts=false

usage() {
	cat >&2 <<'EOF'
usage:
  verify_daxis_release_attachment.sh [--artifact-root path/to/artifacts] [--require-local-artifacts] [--stable-default] path/to/completed-release-attachment.md
  verify_daxis_release_attachment.sh [--artifact-root path/to/artifacts] [--require-local-artifacts] --stable-default-dir path/to/completed-release-attachments
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

manifest_path() {
	if [[ "$manifest_file" = /* ]]; then
		printf "%s\n" "$manifest_file"
		return
	fi

	printf "%s/%s\n" "$repo_root" "$manifest_file"
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

path="$(manifest_path)"
attachment="$(attachment_path)"
artifact_root_absolute="$(artifact_root_path)"

if [[ ! -f "$path" ]]; then
	echo "missing Daxis release bundle manifest: $manifest_file" >&2
	exit 1
fi

if [[ -n "$artifact_root_absolute" && ! -d "$artifact_root_absolute" ]]; then
	echo "missing Daxis release artifact root: $artifact_root" >&2
	exit 1
fi

if [[ "$require_local_artifacts" == true && -z "$artifact_root_absolute" ]]; then
	echo "--require-local-artifacts requires --artifact-root" >&2
	exit 1
fi

if [[ "$directory_mode" == true ]]; then
	if [[ ! -d "$attachment" ]]; then
		echo "missing Daxis release attachment directory: $attachment_file" >&2
		exit 1
	fi
elif [[ ! -f "$attachment" ]]; then
	echo "missing Daxis release attachment: $attachment_file" >&2
	exit 1
fi

python3 - "$path" "$attachment" "$stable_default" "$directory_mode" "$artifact_root_absolute" "$require_local_artifacts" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

manifest_path = Path(sys.argv[1])
attachment_path = Path(sys.argv[2])
stable_default = sys.argv[3] == "true"
directory_mode = sys.argv[4] == "true"
artifact_root = Path(sys.argv[5]).resolve() if sys.argv[5] else None
require_local_artifacts = sys.argv[6] == "true"

with manifest_path.open(encoding="utf-8") as handle:
    manifest = json.load(handle)


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
            expect(role not in owner_reviews, f"duplicate release owner-review role: {role}")
            owner_reviews[role] = {
                "name": cells[name_index].strip(),
                "review_state": cells[state_index].strip(),
                "notes": cells[notes_index].strip(),
            }
    return owner_reviews


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


def verify_local_artifact_checksum(uri: str, expected_sha256: str, uri_field: str, digest_field: str) -> None:
    local_path = resolve_local_artifact(uri, uri_field)
    if local_path is None:
        return
    expect(local_path.is_file(), f"missing local attachment artifact for {uri_field}: {uri}")
    actual_sha256 = hashlib.sha256(local_path.read_bytes()).hexdigest()
    expect(actual_sha256 == expected_sha256, f"{digest_field} does not match local artifact bytes")


expect(manifest.get("manifest") == "daxis_release_bundle_manifest", "invalid Daxis release bundle manifest id")
schema = manifest.get("releaseAttachmentSchema", {})
expect(isinstance(schema, dict), "releaseAttachmentSchema is required")

required_metadata = expect_text_list(schema.get("requiredMetadata", []), "releaseAttachmentSchema requiredMetadata")
release_process_item_ids = expect_text_list(schema.get("releaseProcessItemIds", []), "releaseAttachmentSchema releaseProcessItemIds")
allowed_release_channels = expect_text_list(schema.get("allowedReleaseChannels", []), "releaseAttachmentSchema allowedReleaseChannels")
checksum_format = schema.get("checksumFormat", {})
expect(isinstance(checksum_format, dict), "releaseAttachmentSchema checksumFormat is required")
expect(checksum_format.get("field") == "artifact_sha256", "releaseAttachmentSchema checksumFormat field must be artifact_sha256")
expect(checksum_format.get("algorithm") == "sha256", "releaseAttachmentSchema checksumFormat algorithm must be sha256")
expect(checksum_format.get("encoding") == "lowercase_hex", "releaseAttachmentSchema checksumFormat encoding must be lowercase_hex")
expect(checksum_format.get("length") == 64, "releaseAttachmentSchema checksumFormat length must be 64")
required_review_states = expect_text_list(schema.get("requiredReviewStates", []), "releaseAttachmentSchema requiredReviewStates")
required_reviewer_roles = expect_text_list(schema.get("requiredReviewerRoles", []), "releaseAttachmentSchema requiredReviewerRoles")


def validate_owner_reviews(path: Path) -> None:
    if not required_reviewer_roles:
        return
    owner_reviews = parse_owner_review_table(path)
    for role in required_reviewer_roles:
        expect(
            role in owner_reviews,
            f"missing stable-default release owner-review role: {role}",
        )
        review = owner_reviews[role]
        expect(
            bool(review["name"].strip()),
            f"stable-default release owner-review role {role} must include reviewer name",
        )
        expect(
            review["review_state"] in required_review_states,
            f"stable-default release owner-review role {role} has invalid review state",
        )
        expect(
            review["review_state"] == "accepted",
            f"stable-default release owner-review role {role} must have review state accepted",
        )
        expect(
            bool(review["notes"].strip()),
            f"stable-default release owner-review role {role} must include notes",
        )


def validate_metadata(metadata: dict[str, str], path: Path) -> str:
    for field in required_metadata:
        expect(field in metadata, f"missing attachment metadata field: {field}")
        expect(metadata[field], f"attachment metadata field is blank: {field}")
    item_id = metadata["item_id"]
    expect(
        item_id in release_process_item_ids,
        "item_id must reference a release-process attachment item",
    )
    expect(metadata["release_channel"] in allowed_release_channels, "release_channel is not allowed by releaseAttachmentSchema")
    expect_lower_hex(metadata["artifact_sha256"], 64, "artifact_sha256")
    verify_local_artifact_checksum(
        metadata["artifact_uri"],
        metadata["artifact_sha256"],
        "artifact_uri",
        "artifact_sha256",
    )
    expect_git_sha(metadata["release_commit_sha"], "release_commit_sha")
    status = metadata["exit_status_or_review_status"]
    expect(
        status in required_review_states or bool(re.fullmatch(r"[0-9]+", status)),
        "exit_status_or_review_status must be a review state or numeric exit status",
    )
    if stable_default:
        expect(
            metadata["release_channel"] == "stable",
            "stable-default release attachment must use release_channel stable",
        )
        expect(
            status == "accepted",
            "stable-default release attachment must have exit_status_or_review_status accepted",
        )
        validate_owner_reviews(path)
    return item_id


if directory_mode:
    expect(attachment_path.is_dir(), f"stable-default release attachment path must be a directory: {attachment_path}")
    attachment_files = sorted(attachment_path.glob("*.md"))
    expect(attachment_files, f"stable-default release attachment directory has no Markdown attachments: {attachment_path}")
    seen_items: dict[str, Path] = {}
    for candidate in attachment_files:
        item_id = validate_metadata(parse_metadata(candidate), candidate)
        expect(
            item_id not in seen_items,
            f"duplicate stable-default release attachment item: {item_id}",
        )
        seen_items[item_id] = candidate
    for item_id in release_process_item_ids:
        expect(
            item_id in seen_items,
            f"missing stable-default release attachment item: {item_id}",
        )
    print(f"daxis_release_attachment_directory_verified={attachment_path}")
else:
    validate_metadata(parse_metadata(attachment_path), attachment_path)
    print(f"daxis_release_attachment_verified={attachment_path}")
PY
