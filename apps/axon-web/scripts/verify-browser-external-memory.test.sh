#!/usr/bin/env bash

set -euo pipefail

script="scripts/verify-browser-external-memory.sh"

bash -n "$script"
grep -F 'AXON_STRESS_DELTA_PATH is required for the complete browser external-memory gate' "$script" >/dev/null
grep -F 'spills the original high-cardinality stress aggregate' "$script" >/dev/null
grep -F 'isolates simultaneous OPFS spill scopes across two same-origin tabs' "$script" >/dev/null

temporary_root=$(mktemp -d)
trap 'rm -rf "$temporary_root"' EXIT

mkdir -p "$temporary_root/table/_delta_log"
printf 'parquet-bytes' >"$temporary_root/table/part-00000.parquet"
printf '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n' \
  >"$temporary_root/table/_delta_log/00000000000000000000.json"

data_sha=$(shasum -a 256 "$temporary_root/table/part-00000.parquet" | awk '{print $1}')
log_sha=$(shasum -a 256 "$temporary_root/table/_delta_log/00000000000000000000.json" | awk '{print $1}')

cat >"$temporary_root/fixture-manifest.json" <<JSON
{
  "schema_version": 1,
  "fixture_revision": "browser-external-memory-v1",
  "row_count": 1600000,
  "group_count": 800000,
  "active_file_count": 16,
  "queries": [
    {"id":"aggregate","expected_operator":"GROUPED_AGGREGATE"},
    {"id":"aggregate_states","expected_operator":"GROUPED_AGGREGATE"},
    {"id":"external_sort","expected_operator":"EXTERNAL_SORT"}
  ],
  "objects": [
    {"relative_path":"part-00000.parquet","sha256":"$data_sha"},
    {"relative_path":"_delta_log/00000000000000000000.json","sha256":"$log_sha"}
  ]
}
JSON

cat >"$temporary_root/native-oracle.json" <<'JSON'
{"schema_version":1,"fixture_revision":"browser-external-memory-v1","queries":[{"id":"aggregate","rows":[]},{"id":"aggregate_states","rows":[]},{"id":"external_sort","rows":[]}]}
JSON

AXON_BROWSER_EXTERNAL_MEMORY_FIXTURE_ROOT="$temporary_root" bash "$script" --metadata-only

jq '.row_count = 1599999' "$temporary_root/fixture-manifest.json" \
  >"$temporary_root/tampered-manifest.json"
mv "$temporary_root/tampered-manifest.json" "$temporary_root/fixture-manifest.json"

if AXON_BROWSER_EXTERNAL_MEMORY_FIXTURE_ROOT="$temporary_root" bash "$script" --metadata-only; then
  echo "tampered external-memory fixture unexpectedly passed validation" >&2
  exit 1
fi
