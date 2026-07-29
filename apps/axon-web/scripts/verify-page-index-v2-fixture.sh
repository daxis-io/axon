#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
fixture_root="${1:-${repo_root}/target/fixtures/s3-browser-perf-page-index-v2}"
manifest="${fixture_root}/fixture-manifest.json"
checksums="${fixture_root}/object-sha256.txt"
provenance="${fixture_root}/provenance.json"
generator="${repo_root}/apps/axon-web/src/bin/generate_page_index_v2_fixture.rs"
lockfile="${repo_root}/Cargo.lock"

for required in "${manifest}" "${checksums}" "${provenance}" "${generator}" "${lockfile}"; do
  if [[ ! -f "${required}" ]]; then
    echo "missing required page-index v2 fixture input: ${required}" >&2
    exit 1
  fi
done

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

expected_manifest_sha="$(jq -er '.manifest_sha256 | select(test("^[0-9a-f]{64}$"))' "${provenance}")"
expected_checksums_sha="$(jq -er '.object_checksums_sha256 | select(test("^[0-9a-f]{64}$"))' "${provenance}")"
[[ "$(sha256_file "${manifest}")" == "${expected_manifest_sha}" ]]
[[ "$(sha256_file "${checksums}")" == "${expected_checksums_sha}" ]]

jq -e '
  .schema_version == 2 and
  .fixture_revision == "s3-browser-perf-page-index-v2" and
  .immutable_prefix == "fixtures/s3-browser-perf-page-index-v2" and
  (.tables | length) == 6 and
  ([.tables[].layout] | unique | length) == 3 and
  ([.tables[].geometry] | unique | length) == 2 and
  ([.tables[].active_files[].predicate_column_indexes_usable] | all) and
  ([.tables[].active_files[].missing_index_column_has_column_index] | any | not) and
  ([.tables[] | select(.geometry == "few_large") |
    .active_file_count == 4 and .row_groups_per_file == 4] | all) and
  ([.tables[] | select(.geometry == "many_small") |
    .active_file_count == 32 and .row_groups_per_file == 1] | all) and
  ([.tables[].selectivity_cases | length == 7] | all)
' "${manifest}" >/dev/null

[[ "$(jq -er '.generator.source_sha256' "${manifest}")" == "$(sha256_file "${generator}")" ]]
[[ "$(jq -er '.generator.cargo_lock_sha256' "${manifest}")" == "$(sha256_file "${lockfile}")" ]]
jq -e '.cloud_upload_attempted == false' "${provenance}" >/dev/null

(
  cd "${fixture_root}"
  shasum -a 256 -c object-sha256.txt >/dev/null
)

echo "Verified page-index v2 fixture at ${fixture_root}"
