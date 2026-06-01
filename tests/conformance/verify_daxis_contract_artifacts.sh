#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_CONTRACT_REPO_ROOT:-$(pwd)}"
manifest_file="${AXON_DAXIS_CONTRACT_MANIFEST_FILE:-docs/release-gates/daxis-contract-artifacts.sha256}"
custom_manifest=0
if [[ -n "${AXON_DAXIS_CONTRACT_MANIFEST_FILE:-}" ]]; then
  custom_manifest=1
fi

default_artifacts=(
  "apps/axon-web/examples/daxis-descriptor-resolver.ts"
  "apps/axon-web/examples/daxis-headless-query.ts"
  "apps/axon-web/examples/daxis-object-grant-adapter.ts"
  "apps/axon-web/examples/daxis-read-access-plan.ts"
  "crates/query-contract/schemas/object-grants.openapi.json"
  "crates/query-contract/schemas/read-access-plan.schema.json"
  "docs/program/daxis-first-class-integration-examples/approved-axon-read-descriptor.saved-query.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.audit-event.range.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.batch-sign-request.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.batch-sign-response.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.head-request.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.head-response.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.list-request.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.list-response.json"
  "docs/program/daxis-first-class-integration-examples/object-grants.range-request.json"
  "docs/program/daxis-first-class-integration-examples/query-result.agent.executed.json"
  "docs/program/daxis-first-class-integration-examples/query-result.api.executed.json"
  "docs/program/daxis-first-class-integration-examples/query-result.builder.executed.json"
  "docs/program/daxis-first-class-integration-examples/query-result.dashboard-tile.executed.json"
  "docs/program/daxis-first-class-integration-examples/query-result.policy-denied.rejected.json"
  "docs/program/daxis-first-class-integration-examples/query-result.runtime-budget-overflow.fallback.json"
  "docs/program/daxis-first-class-integration-examples/query-result.saved-query.executed.json"
  "docs/program/daxis-first-class-integration-examples/query-result.unsupported-sql.fallback.json"
  "docs/program/daxis-first-class-integration-examples/read-access-plan.blocked.json"
  "docs/program/daxis-first-class-integration-examples/read-access-plan.brokered-delta.json"
  "docs/program/daxis-first-class-integration-examples/read-access-plan.delta-sharing.json"
  "docs/program/daxis-first-class-integration-examples/read-access-plan.sql-fallback-required.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-request.latest.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-request.version-42.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-response.expired.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-response.invalid-descriptor.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-response.path-escape.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-response.signed-url.json"
  "docs/program/daxis-first-class-integration-examples/snapshot-descriptor-response.snapshot-version-mismatch.json"
)

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi

  shasum -a 256 "$1" | awk '{print $1}'
}

scan_artifact_security() {
  local artifact="$1"
  local path="$repo_root/$artifact"
  python3 - "$artifact" "$path" <<'PY'
import re
import sys
from pathlib import Path

artifact = sys.argv[1]
path = Path(sys.argv[2])
text = path.read_text(encoding="utf-8")

forbidden = [
    (r'(?i)"aws_secret_access_key"\s*:', "aws_secret_access_key"),
    (r'(?i)"aws_session_token"\s*:', "aws_session_token"),
    (r'(?i)"secret_access_key"\s*:', "secret_access_key"),
    (r'(?i)"secretAccessKey"\s*:', "secretAccessKey"),
    (r'(?i)"sessionToken"\s*:', "sessionToken"),
    (r'(?i)"access_token"\s*:', "access_token"),
    (r'(?i)"accessToken"\s*:', "accessToken"),
    (r'(?i)"refresh_token"\s*:', "refresh_token"),
    (r'(?i)"refreshToken"\s*:', "refreshToken"),
    (r'(?i)"client_secret"\s*:', "client_secret"),
    (r'(?i)"clientSecret"\s*:', "clientSecret"),
    (r'(?i)"private_key"\s*:', "private_key"),
    (r'(?i)"privateKey"\s*:', "privateKey"),
    (r'(?i)"service_account"\s*:', "service_account"),
    (r'(?i)"client_email"\s*:', "client_email"),
    (r'(?i)"databricks_bearer_token"\s*:', "databricks_bearer_token"),
    (r'(?i)"bearerToken"\s*:', "bearerToken"),
    (r'(?i)"bearer_token"\s*:', "bearer_token"),
    (r'(?i)"sasToken"\s*:', "sasToken"),
    (r'(?i)"shared_access_signature"\s*:', "shared_access_signature"),
]

for pattern, marker in forbidden:
    if re.search(pattern, text):
        print(f"Daxis contract artifact contains forbidden browser-visible credential field: {artifact} ({marker})", file=sys.stderr)
        sys.exit(1)

for match in re.finditer(r'(?i)\b(AKIA|ASIA)[0-9A-Z]{16}\b', text):
    print(f"Daxis contract artifact contains forbidden AWS access key id marker: {artifact}", file=sys.stderr)
    sys.exit(1)
PY
}

reject_unsafe_path() {
  local path="$1"
  if [[ "$path" = /* || "$path" == *".."* ]]; then
    echo "unsafe contract artifact path in manifest: $path" >&2
    exit 1
  fi
}

manifest_path() {
  if [[ "$manifest_file" = /* ]]; then
    printf "%s\n" "$manifest_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$manifest_file"
}

write_manifest() {
  local path
  path="$(manifest_path)"
  mkdir -p "$(dirname "$path")"

  {
    echo "# Daxis contract artifact SHA-256 manifest"
    echo "# Generated by: bash tests/conformance/verify_daxis_contract_artifacts.sh --write"
    for artifact in "${default_artifacts[@]}"; do
      reject_unsafe_path "$artifact"
      local path="$repo_root/$artifact"
      if [[ ! -f "$path" ]]; then
        echo "missing contract artifact: $artifact" >&2
        exit 1
      fi
      scan_artifact_security "$artifact"
      printf "%s  %s\n" "$(hash_file "$path")" "$artifact"
    done
  } >"$path"
}

expected_manifest_paths() {
  for artifact in "${default_artifacts[@]}"; do
    printf "%s\n" "$artifact"
  done | sort
}

verify_manifest() {
  local path
  path="$(manifest_path)"
  if [[ ! -f "$path" ]]; then
    echo "missing Daxis contract artifact manifest: $manifest_file" >&2
    exit 1
  fi

  local tmpdir
  tmpdir="$(mktemp -d)"
  trap "rm -rf '$tmpdir'" EXIT
  local observed_paths="$tmpdir/observed-paths.txt"
  local artifact_count=0

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ -z "$line" || "$line" == \#* ]]; then
      continue
    fi

    if [[ ! "$line" =~ ^([0-9a-f]{64})[[:space:]]+(.+)$ ]]; then
      echo "invalid Daxis contract manifest line: $line" >&2
      exit 1
    fi

    local expected_hash="${BASH_REMATCH[1]}"
    local artifact="${BASH_REMATCH[2]}"
    reject_unsafe_path "$artifact"

    local path="$repo_root/$artifact"
    if [[ ! -f "$path" ]]; then
      echo "missing contract artifact listed in manifest: $artifact" >&2
      exit 1
    fi
    scan_artifact_security "$artifact"

    local actual_hash
    actual_hash="$(hash_file "$path")"
    if [[ "$actual_hash" != "$expected_hash" ]]; then
      echo "Daxis contract artifact hash mismatch: $artifact" >&2
      echo "  expected: $expected_hash" >&2
      echo "  actual:   $actual_hash" >&2
      exit 1
    fi

    printf "%s\n" "$artifact" >>"$observed_paths"
    artifact_count=$((artifact_count + 1))
  done <"$path"

  if [[ "$artifact_count" -eq 0 ]]; then
    echo "Daxis contract artifact manifest is empty" >&2
    exit 1
  fi

  if [[ "$custom_manifest" -eq 0 ]]; then
    local expected_paths="$tmpdir/expected-paths.txt"
    expected_manifest_paths >"$expected_paths"
    sort "$observed_paths" >"$observed_paths.sorted"
    if ! diff -u "$expected_paths" "$observed_paths.sorted"; then
      echo "Daxis contract artifact manifest entries differ from the default artifact set" >&2
      exit 1
    fi
  fi

  echo "Daxis contract artifact manifest verified ($artifact_count artifacts)"
}

case "${1:-}" in
  --write)
    write_manifest
    ;;
  "")
    verify_manifest
    ;;
  *)
    echo "usage: $0 [--write]" >&2
    exit 2
    ;;
esac
