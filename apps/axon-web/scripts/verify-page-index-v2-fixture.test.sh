#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
verifier="${script_dir}/verify-page-index-v2-fixture.sh"

temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/axon-page-index-v2-verifier.XXXXXX")"
trap 'rm -rf "${temporary_root}"' EXIT

fixture_root="${temporary_root}/fixture"
export AXON_PAGE_INDEX_V2_TARGET_ACTIVE_BYTES=1048576
export AXON_PAGE_INDEX_V2_ESTIMATED_COMPRESSED_BYTES_PER_ROW=32
export AXON_PAGE_INDEX_V2_DATA_PAGE_SIZE_BYTES=8192
export AXON_PAGE_INDEX_V2_DATA_PAGE_ROW_COUNT_LIMIT=128

default_fixture_root="${temporary_root}/default-fixture"
(cd "${repo_root}" && env -u AXON_PAGE_INDEX_V2_TABLE_URI_BASE \
  cargo run --quiet --locked -p axon-web-wasm --features fixture-generator \
  --bin generate-page-index-v2-fixture -- "${default_fixture_root}")
jq -e '
  . as $manifest |
  $manifest.generated_table_uri_base == "s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2" and
  ([$manifest.tables[] |
    .table_uri == ($manifest.generated_table_uri_base + "/" + .relative_path)
  ] | all)
' "${default_fixture_root}/fixture-manifest.json" >/dev/null

export AXON_PAGE_INDEX_V2_TABLE_URI_BASE="s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf-page-index-v2"

(cd "${repo_root}" && cargo run --quiet --locked -p axon-web-wasm --features fixture-generator \
  --bin generate-page-index-v2-fixture -- "${fixture_root}")

# A clean committed generator must pass the complete source gate. During local development,
# the same gate must fail specifically on the recorded dirty-worktree provenance.
if jq -e '.generator.git_worktree_clean' "${fixture_root}/fixture-manifest.json" >/dev/null; then
  bash "${verifier}" "${fixture_root}" >"${temporary_root}/clean.out" 2>&1
else
  if bash "${verifier}" "${fixture_root}" >"${temporary_root}/dirty.out" 2>&1; then
    echo "verifier unexpectedly accepted dirty generator provenance" >&2
    exit 1
  fi
  rg -q "generator worktree was not clean" "${temporary_root}/dirty.out"
fi

# The Rust verifier must run before checksum acceptance and reject physical/manifest drift.
cp -R "${fixture_root}" "${temporary_root}/tampered"
jq '.tables[0].active_files[0].row_groups[0].pages[0].offset_bytes += 1' \
  "${temporary_root}/tampered/fixture-manifest.json" >"${temporary_root}/manifest.json"
mv "${temporary_root}/manifest.json" "${temporary_root}/tampered/fixture-manifest.json"
if (cd "${repo_root}" && cargo run --quiet --locked -p axon-web-wasm --features fixture-generator \
  --bin generate-page-index-v2-fixture -- --verify "${temporary_root}/tampered") \
  >"${temporary_root}/tampered.out" 2>&1; then
  echo "verifier unexpectedly accepted a malformed recorded page location" >&2
  exit 1
fi
rg -q "page metadata differed from manifest" "${temporary_root}/tampered.out"

expect_rejected() {
  local name="$1"
  local expected="$2"
  local root="${temporary_root}/${name}"
  shift 2
  cp -R "${fixture_root}" "${root}"
  "$@" "${root}"
  if (cd "${repo_root}" && cargo run --quiet --locked -p axon-web-wasm --features fixture-generator \
    --bin generate-page-index-v2-fixture -- --verify "${root}") \
    >"${temporary_root}/${name}.out" 2>&1; then
    echo "verifier unexpectedly accepted ${name}" >&2
    exit 1
  fi
  rg -q "${expected}" "${temporary_root}/${name}.out"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

repair_identity_chain() {
  local root="$1"
  local manifest_sha
  local checksums_sha
  local generator
  local relative
  local sha
  while read -r _ relative; do
    sha="$(sha256_file "${root}/${relative}")"
    printf '%s  %s\n' "${sha}" "${relative}"
  done <"${root}/object-sha256.txt" >"${root}/object-sha256.new"
  mv "${root}/object-sha256.new" "${root}/object-sha256.txt"
  manifest_sha="$(sha256_file "${root}/fixture-manifest.json")"
  checksums_sha="$(sha256_file "${root}/object-sha256.txt")"
  generator="$(jq -c '.generator' "${root}/fixture-manifest.json")"
  jq --argjson generator "${generator}" \
    --arg manifest_sha "${manifest_sha}" \
    --arg checksums_sha "${checksums_sha}" '
      .generator = $generator |
      .manifest_sha256 = $manifest_sha |
      .object_checksums_sha256 = $checksums_sha
    ' "${root}/provenance.json" >"${root}/provenance.new"
  mv "${root}/provenance.new" "${root}/provenance.json"
}

remove_checksum_entry() {
  local root="$1"
  sed '$d' "${root}/object-sha256.txt" >"${root}/object-sha256.new"
  mv "${root}/object-sha256.new" "${root}/object-sha256.txt"
}

add_checksum_entry() {
  local root="$1"
  printf '%064d  unexpected.txt\n' 0 >>"${root}/object-sha256.txt"
}

remove_delta_add() {
  local root="$1"
  local log="${root}/tables/ordered-few-large/_delta_log/00000000000000000000.json"
  sed '$d' "${log}" >"${log}.new"
  mv "${log}.new" "${log}"
}

append_delta_remove() {
  local root="$1"
  local log="${root}/tables/ordered-few-large/_delta_log/00000000000000000000.json"
  printf '%s\n' \
    '{"remove":{"path":"part-00000.parquet","deletionTimestamp":0,"dataChange":true}}' \
    >>"${log}"
  repair_identity_chain "${root}"
}

append_unknown_delta_action() {
  local root="$1"
  local log="${root}/tables/ordered-few-large/_delta_log/00000000000000000000.json"
  printf '%s\n' '{"txn":{"appId":"unexpected","version":1}}' >>"${log}"
  repair_identity_chain "${root}"
}

alter_delta_protocol() {
  local root="$1"
  local log="${root}/tables/ordered-few-large/_delta_log/00000000000000000000.json"
  jq -c 'if has("protocol") then .protocol.minReaderVersion = 2 else . end' \
    "${log}" >"${log}.new"
  mv "${log}.new" "${log}"
  repair_identity_chain "${root}"
}

alter_delta_metadata_schema() {
  local root="$1"
  local log="${root}/tables/ordered-few-large/_delta_log/00000000000000000000.json"
  jq -c '
    if has("metaData") then
      .metaData.schemaString |= sub("\\\"type\\\":\\\"long\\\""; "\\\"type\\\":\\\"string\\\"")
    else
      .
    end
  ' "${log}" >"${log}.new"
  mv "${log}.new" "${log}"
  repair_identity_chain "${root}"
}

set_unknown_generator_commit() {
  local root="$1"
  jq '.generator.git_commit = "unknown" | .generator.git_worktree_clean = true' \
    "${root}/fixture-manifest.json" >"${root}/fixture-manifest.new"
  mv "${root}/fixture-manifest.new" "${root}/fixture-manifest.json"
  repair_identity_chain "${root}"
}

set_dirty_generator_provenance() {
  local root="$1"
  jq '.generator.git_worktree_clean = false' \
    "${root}/fixture-manifest.json" >"${root}/fixture-manifest.new"
  mv "${root}/fixture-manifest.new" "${root}/fixture-manifest.json"
  repair_identity_chain "${root}"
}

set_non_head_generator_commit() {
  local root="$1"
  jq '.generator.git_commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" |
      .generator.git_worktree_clean = true' \
    "${root}/fixture-manifest.json" >"${root}/fixture-manifest.new"
  mv "${root}/fixture-manifest.new" "${root}/fixture-manifest.json"
  repair_identity_chain "${root}"
}

replace_parquet_with_out_of_root_symlink() {
  local root="$1"
  local file="${root}/tables/ordered-few-large/part-00000.parquet"
  local external="${root}.external.parquet"
  cp "${file}" "${external}"
  rm "${file}"
  ln -s "${external}" "${file}"
}

alter_index_extent() {
  local root="$1"
  jq '.tables[0].active_files[0].row_groups[0].column_index_extent.length_bytes += 1' \
    "${root}/fixture-manifest.json" >"${root}/fixture-manifest.new"
  mv "${root}/fixture-manifest.new" "${root}/fixture-manifest.json"
}

alter_table_uri() {
  local root="$1"
  jq '.tables[0].table_uri = .generated_table_uri_base + "/ordered-few-large"' \
    "${root}/fixture-manifest.json" >"${root}/fixture-manifest.new"
  mv "${root}/fixture-manifest.new" "${root}/fixture-manifest.json"
}

expect_rejected "missing-checksum" "checksum inventory differed" remove_checksum_entry
expect_rejected "extra-checksum" "checksum inventory differed" add_checksum_entry
expect_rejected "delta-mismatch" "Delta add inventory differed" remove_delta_add
expect_rejected "delta-remove" "unexpected Delta action was rejected: remove" append_delta_remove
expect_rejected "delta-unknown" "unexpected Delta action was rejected: txn" append_unknown_delta_action
expect_rejected "delta-protocol" "Delta protocol action differed" alter_delta_protocol
expect_rejected "delta-metadata" "Delta metadata action differed" alter_delta_metadata_schema
expect_rejected "index-extent" "index extent metadata differed from manifest" alter_index_extent
expect_rejected "table-uri" "table URI did not equal" alter_table_uri
expect_rejected "dirty-generator" "generator worktree was not clean" set_dirty_generator_provenance
expect_rejected "unknown-generator" "generator git commit was unknown" set_unknown_generator_commit
expect_rejected "non-head-generator" "generator git commit did not equal HEAD" set_non_head_generator_commit
expect_rejected "parquet-symlink" "symlink/non-regular fixture object was rejected" \
  replace_parquet_with_out_of_root_symlink

echo "page-index v2 verifier negative integration checks passed"
