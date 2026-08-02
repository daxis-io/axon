#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
first_base=$(mktemp -d)
second_base=$(mktemp -d)
unsafe_root=$(mktemp -d)
first_root="$first_base/fixture"
second_root="$second_base/fixture"
trap 'rm -rf "$first_base" "$second_base" "$unsafe_root"' EXIT

generate() {
  local output_root=$1
  AXON_SPILL_FIXTURE_TEST_MODE=1 \
  AXON_SPILL_FIXTURE_ACTIVE_FILES=2 \
  AXON_SPILL_FIXTURE_ROWS_PER_FILE=256 \
  AXON_SPILL_FIXTURE_GROUP_COUNT=256 \
    cargo run --locked -p axon-web-wasm --features fixture-generator \
      --bin generate-spill-conformance-fixture -- "$output_root"
}

cd "$repo_root"

printf 'must survive\n' >"$unsafe_root/user-data.txt"
if generate "$unsafe_root"; then
  echo "fixture generator unexpectedly replaced an unmarked directory" >&2
  exit 1
fi
test "$(cat "$unsafe_root/user-data.txt")" = "must survive"

generate "$first_root"
generate "$second_root"
generate "$first_root"

jq -e '
  .schema_version == 1 and
  .fixture_revision == "browser-external-memory-v1" and
  .row_count == 512 and
  .group_count == 256 and
  .active_file_count == 2 and
  (.objects | length) == 3 and
  ([.queries[].id] | sort) == ["aggregate", "aggregate_states", "external_sort"]
' "$first_root/fixture-manifest.json" >/dev/null

jq -e '
  .schema_version == 1 and
  .fixture_revision == "browser-external-memory-v1" and
  ([.queries[].rows | length] | all(. == 256))
' "$first_root/native-oracle.json" >/dev/null

cmp "$first_root/fixture-manifest.json" "$second_root/fixture-manifest.json"
cmp "$first_root/native-oracle.json" "$second_root/native-oracle.json"
cmp "$first_root/table/_delta_log/00000000000000000000.json" \
  "$second_root/table/_delta_log/00000000000000000000.json"
