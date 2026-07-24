#!/usr/bin/env bash

set -euo pipefail

POC_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST_PATH="${POC_ROOT}/Cargo.toml"
TARGET="wasm32-unknown-unknown"
PACKAGE="axon-upstream-wasm-browser-harness"
PACKAGE_TREE="$(mktemp)"
FEATURE_TREE="$(mktemp)"
trap 'rm -f "${PACKAGE_TREE}" "${FEATURE_TREE}"' EXIT

cargo tree \
  --manifest-path "${MANIFEST_PATH}" \
  --package "${PACKAGE}" \
  --target "${TARGET}" \
  --locked \
  --prefix none \
  --format '{p}' \
  | sed 's/ (\*)$//' \
  | sort -u \
  > "${PACKAGE_TREE}"

cargo tree \
  --manifest-path "${MANIFEST_PATH}" \
  --package "${PACKAGE}" \
  --target "${TARGET}" \
  --locked \
  --edges features \
  > "${FEATURE_TREE}"

denied_packages=(
  aws-lc-sys
  hyper
  liblzma-sys
  native-tls
  openssl-sys
  ring
  tempfile
  walkdir
  zstd-sys
)

for package in "${denied_packages[@]}"; do
  if rg -q "^${package} v" "${PACKAGE_TREE}"; then
    echo "denied browser dependency present: ${package}" >&2
    exit 1
  fi
done

denied_features=(
  'object_store feature "aws"'
  'object_store feature "azure"'
  'object_store feature "fs"'
  'object_store feature "gcp"'
  'tokio feature "rt-multi-thread"'
)

for feature in "${denied_features[@]}"; do
  if rg -Fq "${feature}" "${FEATURE_TREE}"; then
    echo "denied browser feature present: ${feature}" >&2
    exit 1
  fi
done

required_universes=(
  arrow-array
  buoyant_kernel
  datafusion
  deltalake-browser
  object_store
  parquet
)

for package in "${required_universes[@]}"; do
  count="$(
    rg "^${package} v" "${PACKAGE_TREE}" \
      | wc -l \
      | tr -d '[:space:]'
  )"
  if [[ "${count}" != "1" ]]; then
    echo "expected one ${package} source universe, found ${count}" >&2
    rg "^${package} v" "${PACKAGE_TREE}" >&2 || true
    exit 1
  fi
done

if rg -n '^\[patch(\.|])' "${POC_ROOT}" -g 'Cargo.toml'; then
  echo "dependency-level patch tables are forbidden in the browser POC" >&2
  exit 1
fi

echo "Axon upstream-WASM browser graph policy passed"
