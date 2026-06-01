#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
manifest="$tmpdir/daxis-contract-artifacts.sha256"

compute_hash() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi

  shasum -a 256 "$1" | awk '{print $1}'
}

mkdir -p "$repo_root/docs/program/daxis-first-class-integration-examples"

artifact_path="docs/program/daxis-first-class-integration-examples/example.json"
artifact_file="$repo_root/$artifact_path"

printf '{"contract":"stable"}\n' >"$artifact_file"

expected_hash="$(compute_hash "$artifact_file")"
cat >"$manifest" <<EOF_MANIFEST
# Daxis contract artifact SHA-256 manifest
$expected_hash  $artifact_path
EOF_MANIFEST

AXON_DAXIS_CONTRACT_REPO_ROOT="$repo_root" \
  AXON_DAXIS_CONTRACT_MANIFEST_FILE="$manifest" \
  bash tests/conformance/verify_daxis_contract_artifacts.sh >/dev/null

printf '{"contract":"changed"}\n' >"$artifact_file"

if AXON_DAXIS_CONTRACT_REPO_ROOT="$repo_root" \
  AXON_DAXIS_CONTRACT_MANIFEST_FILE="$manifest" \
  bash tests/conformance/verify_daxis_contract_artifacts.sh >/dev/null 2>&1; then
  echo "expected changed contract artifact to be rejected" >&2
  exit 1
fi

cat >"$manifest" <<EOF_MANIFEST
# Daxis contract artifact SHA-256 manifest
$expected_hash  $artifact_path
$expected_hash  missing.json
EOF_MANIFEST

if AXON_DAXIS_CONTRACT_REPO_ROOT="$repo_root" \
  AXON_DAXIS_CONTRACT_MANIFEST_FILE="$manifest" \
  bash tests/conformance/verify_daxis_contract_artifacts.sh >/dev/null 2>&1; then
  echo "expected missing contract artifact to be rejected" >&2
  exit 1
fi

printf '{"aws_secret_access_key":"do-not-ship"}\n' >"$artifact_file"
secret_hash="$(compute_hash "$artifact_file")"
cat >"$manifest" <<EOF_MANIFEST
# Daxis contract artifact SHA-256 manifest
$secret_hash  $artifact_path
EOF_MANIFEST

if AXON_DAXIS_CONTRACT_REPO_ROOT="$repo_root" \
  AXON_DAXIS_CONTRACT_MANIFEST_FILE="$manifest" \
  bash tests/conformance/verify_daxis_contract_artifacts.sh >/dev/null 2>&1; then
  echo "expected raw credential contract artifact to be rejected" >&2
  exit 1
fi

echo "Daxis contract artifact verifier regression coverage passed"
