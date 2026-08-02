#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../.." && pwd)"
fixture_root="${1:-${repo_root}/target/fixtures/s3-browser-perf-page-index-v2}"

# Physical verification is authoritative and runs before checksum acceptance. It reopens
# every active Parquet file, reconciles page/index metadata, Delta actions, selectivity
# results, provenance, exact inventories, and stable regular-file identities. No shell-level
# re-open follows it: the Rust verifier accepts or rejects the single file-descriptor snapshot.
(
  cd "${repo_root}"
  cargo run --quiet --locked -p axon-web-wasm --features fixture-generator \
    --bin generate-page-index-v2-fixture -- --verify "${fixture_root}"
)

echo "Verified page-index v2 fixture at ${fixture_root}"
