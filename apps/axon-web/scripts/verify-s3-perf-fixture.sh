#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
web_root=$(cd -- "${script_dir}/.." && pwd)
metadata_root="${AXON_S3_PERF_METADATA_ROOT:-${web_root}/public/fixtures/s3-perf}"
mode="${1:---metadata-only}"
table_root=""

case "$mode" in
  --metadata-only)
    if (( $# != 0 && $# != 1 )); then
      echo "usage: $0 [--metadata-only | --table-root <path> | --stage <path>]" >&2
      exit 2
    fi
    ;;
  --table-root | --stage)
    if (( $# != 2 )); then
      echo "usage: $0 [--metadata-only | --table-root <path> | --stage <path>]" >&2
      exit 2
    fi
    table_root="$2"
    ;;
  *)
    echo "usage: $0 [--metadata-only | --table-root <path> | --stage <path>]" >&2
    exit 2
    ;;
esac

manifest="${metadata_root}/s3-perf-fixture-manifest.json"
checksums="${metadata_root}/s3-perf-object-sha256.txt"
provenance="${metadata_root}/s3-perf-provenance.json"

for dependency in awk diff jq rg sort; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "missing required command: $dependency" >&2
    exit 1
  fi
done

if [[ ! -f "$manifest" || ! -f "$checksums" || ! -f "$provenance" ]]; then
  echo "S3 performance fixture manifest, checksums, or provenance is missing" >&2
  exit 1
fi

if ! command -v shasum >/dev/null 2>&1 && ! command -v sha256sum >/dev/null 2>&1; then
  echo "missing required SHA-256 command: shasum or sha256sum" >&2
  exit 1
fi

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

expected_manifest_sha=$(jq -er '.manifest_sha256 | select(test("^[0-9a-f]{64}$"))' "$provenance")
expected_checksums_sha=$(
  jq -er '.object_checksums_sha256 | select(test("^[0-9a-f]{64}$"))' "$provenance"
)
actual_manifest_sha=$(sha256_file "$manifest")
actual_checksums_sha=$(sha256_file "$checksums")

if [[ "$actual_manifest_sha" != "$expected_manifest_sha" ]]; then
  echo "S3 fixture manifest SHA-256 mismatch" >&2
  exit 1
fi
if [[ "$actual_checksums_sha" != "$expected_checksums_sha" ]]; then
  echo "S3 fixture object-checksum SHA-256 mismatch" >&2
  exit 1
fi

manifest_revision=$(jq -er '.fixture_revision' "$manifest")
provenance_revision=$(jq -er '.fixture_revision' "$provenance")
manifest_table_uri=$(jq -er '.table_uri' "$manifest")
provenance_table_uri=$(jq -er '.table_uri' "$provenance")
manifest_region=$(jq -er '.region' "$manifest")
provenance_region=$(jq -er '.region' "$provenance")

if [[ "$manifest_revision" != "$provenance_revision" ||
  "$manifest_table_uri" != "$provenance_table_uri" ||
  "$manifest_region" != "$provenance_region" ]]; then
  echo "S3 fixture manifest and provenance identity disagree" >&2
  exit 1
fi

if [[ "$manifest_table_uri" != s3://* || "$manifest_table_uri" == *[\?\#]* ]]; then
  echo "S3 fixture table URI must be an unsigned s3:// URI" >&2
  exit 1
fi

if rg -i -q \
  'x-amz-(signature|credential|security-token)|aws_(access_key_id|secret_access_key|session_token)|AKIA[0-9A-Z]{16}' \
  "$manifest" "$provenance"; then
  echo "S3 fixture metadata contains credential material" >&2
  exit 1
fi

required_object_count=$(jq -er '.required_object_count' "$provenance")
active_file_count=$(jq -er '.active_file_count' "$manifest")
expected_active_file_count=$(jq -er '.active_file_count' "$provenance")
active_data_bytes=$(jq -er '.actual_active_data_bytes' "$manifest")
expected_active_data_bytes=$(jq -er '.active_data_bytes' "$provenance")
manifest_required_count=$(jq '[.objects[], .active_data_files[]] | length' "$manifest")
checksum_count=$(awk 'NF { count += 1 } END { print count + 0 }' "$checksums")
manifest_active_sum=$(jq '[.active_data_files[].size_bytes] | add // 0' "$manifest")

if [[ "$active_file_count" != "$expected_active_file_count" ||
  "$active_data_bytes" != "$expected_active_data_bytes" ||
  "$active_data_bytes" != "$manifest_active_sum" ||
  "$manifest_required_count" != "$required_object_count" ||
  "$checksum_count" != "$required_object_count" ]]; then
  echo "S3 fixture inventory totals disagree" >&2
  exit 1
fi

inventory_file=$(mktemp)
checksum_inventory_file=$(mktemp)
cleanup() {
  rm -f "$inventory_file" "$checksum_inventory_file"
}
trap cleanup EXIT

while IFS=$'\t' read -r relative_path size_bytes; do
  case "$relative_path" in
    "" | /* | *".."*)
      echo "unsafe S3 fixture object path: $relative_path" >&2
      exit 1
      ;;
  esac
  printf '%s\t%s\n' "$relative_path" "$size_bytes" >>"$inventory_file"
done < <(
  jq -r \
    '(.objects[], .active_data_files[]) | [.relative_path, (.size_bytes | tostring)] | @tsv' \
    "$manifest"
)

if cut -f1 "$inventory_file" | sort | uniq -d | rg -q '.'; then
  echo "S3 fixture manifest contains duplicate object paths" >&2
  exit 1
fi

while read -r digest checksum_path extra; do
  if [[ -n "${extra:-}" || ! "$digest" =~ ^[0-9a-f]{64}$ || "$checksum_path" != table/* ]]; then
    echo "invalid S3 fixture checksum entry" >&2
    exit 1
  fi
  relative_path="${checksum_path#table/}"
  printf '%s\t%s\n' "$relative_path" "$digest" >>"$checksum_inventory_file"
done <"$checksums"

if cut -f1 "$checksum_inventory_file" | sort | uniq -d | rg -q '.'; then
  echo "S3 fixture checksum list contains duplicate object paths" >&2
  exit 1
fi

if ! diff \
  <(cut -f1 "$inventory_file" | sort) \
  <(cut -f1 "$checksum_inventory_file" | sort) \
  >/dev/null; then
  echo "S3 fixture manifest and checksum object paths disagree" >&2
  exit 1
fi

if [[ "$mode" == "--metadata-only" ]]; then
  echo "verified S3 fixture ${manifest_revision}: ${required_object_count} pinned objects"
  exit 0
fi

case "$table_root" in
  "" | "." | "/" | ".." | "~" | */.. | */../* | ../*)
    echo "unsafe S3 fixture table root: ${table_root:-<empty>}" >&2
    exit 1
    ;;
esac

if [[ "$mode" == "--stage" ]] && ! command -v curl >/dev/null 2>&1; then
  echo "missing required command: curl" >&2
  exit 1
fi

uri_without_scheme="${manifest_table_uri#s3://}"
bucket="${uri_without_scheme%%/*}"
prefix="${uri_without_scheme#*/}"
public_table_base="https://${bucket}.s3.${manifest_region}.amazonaws.com/${prefix}/"
mkdir -p "$table_root"

while IFS=$'\t' read -r relative_path expected_size; do
  expected_digest=$(
    awk -F $'\t' -v path="$relative_path" '$1 == path { print $2 }' "$checksum_inventory_file"
  )
  destination="${table_root}/${relative_path}"
  actual_size=0
  actual_digest=""
  if [[ -f "$destination" ]]; then
    actual_size=$(wc -c <"$destination" | tr -d '[:space:]')
    if [[ "$actual_size" == "$expected_size" ]]; then
      actual_digest=$(sha256_file "$destination")
    fi
  fi

  if [[ "$actual_size" != "$expected_size" || "$actual_digest" != "$expected_digest" ]]; then
    if [[ "$mode" != "--stage" ]]; then
      echo "S3 fixture object mismatch: $relative_path" >&2
      exit 1
    fi
    mkdir -p "$(dirname -- "$destination")"
    temporary="${destination}.part"
    curl --silent --show-error --fail --location --retry 3 \
      --proto '=https' --proto-redir '=https' \
      --connect-timeout 15 --max-time 300 --max-filesize "$expected_size" \
      --output "$temporary" \
      "${public_table_base}${relative_path}"
    actual_size=$(wc -c <"$temporary" | tr -d '[:space:]')
    actual_digest=$(sha256_file "$temporary")
    if [[ "$actual_size" != "$expected_size" || "$actual_digest" != "$expected_digest" ]]; then
      rm -f "$temporary"
      echo "downloaded S3 fixture object failed verification: $relative_path" >&2
      exit 1
    fi
    mv "$temporary" "$destination"
  fi
done <"$inventory_file"

echo "verified S3 fixture ${manifest_revision} at ${table_root}: ${required_object_count} objects"
