#!/usr/bin/env bash

set -euo pipefail

required_files=(
  "Cargo.toml"
  "Cargo.lock"
  ".github/workflows/ci.yml"
  "CODEOWNERS"
  "docs/program/package-owners.md"
  "docs/program/upstream-patch-inventory.md"
)

required_dirs=(
  "crates/query-contract"
  "crates/query-router"
  "crates/native-query-runtime"
  "crates/delta-control-plane"
  "crates/wasm-query-runtime"
  "crates/wasm-http-object-store"
  "crates/browser-sdk"
  "crates/udf-abi"
  "crates/udf-host-wasi"
  "tests/conformance"
  "tests/perf"
  "tests/security"
)

for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    echo "missing required file: $path" >&2
    exit 1
  fi
done

for path in "${required_dirs[@]}"; do
  if [[ ! -d "$path" ]]; then
    echo "missing required directory: $path" >&2
    exit 1
  fi
done

if rg -n '/Users/' README.md docs --glob '*.md'; then
  echo "markdown files must not contain workstation-local absolute paths" >&2
  exit 1
fi

echo "workspace layout verified"
