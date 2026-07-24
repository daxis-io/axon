#!/usr/bin/env bash

set -euo pipefail

script="scripts/verify-s3-perf-fixture.sh"
fixture_root="public/fixtures/s3-perf"

bash -n "$script"
bash "$script" --metadata-only

temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT

cp "$fixture_root/s3-perf-fixture-manifest.json" "$temporary_root/"
cp "$fixture_root/s3-perf-object-sha256.txt" "$temporary_root/"
cp "$fixture_root/s3-perf-provenance.json" "$temporary_root/"

jq '.active_file_count += 1' \
  "$temporary_root/s3-perf-fixture-manifest.json" \
  >"$temporary_root/tampered-manifest.json"
mv "$temporary_root/tampered-manifest.json" "$temporary_root/s3-perf-fixture-manifest.json"

if AXON_S3_PERF_METADATA_ROOT="$temporary_root" bash "$script" --metadata-only; then
  echo "tampered S3 fixture metadata unexpectedly passed validation" >&2
  exit 1
fi

for metadata in \
  "$fixture_root/s3-perf-fixture-manifest.json" \
  "$fixture_root/s3-perf-provenance.json"; do
  if rg -i \
    'x-amz-(signature|credential|security-token)|aws_(access_key_id|secret_access_key|session_token)|AKIA[0-9A-Z]{16}' \
    "$metadata"; then
    echo "S3 fixture metadata contains credential material: $metadata" >&2
    exit 1
  fi
done
