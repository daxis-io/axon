#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
real_bash="$(command -v bash)"
empty_path="$(mktemp -d)"
trap 'rm -rf "$empty_path"' EXIT

verifiers=()
while IFS= read -r verifier; do
  verifiers+=("$verifier")
done < <(
  find \
    "$repo_root/apps/axon-web/scripts" \
    "$repo_root/poc/upstream-wasm-fork-stack" \
    "$repo_root/tests/conformance" \
    "$repo_root/tests/security" \
    -type f \
    \( -name 'verify_*.sh' -o -name 'verify-*.sh' \) \
    ! -name '*_test.sh' \
    ! -name '*.test.sh' \
    -print | sort
)

if [[ ${#verifiers[@]} -eq 0 ]]; then
  echo "rg prerequisite regression setup failed: no verifiers found" >&2
  exit 1
fi

for verifier in "${verifiers[@]}"; do
  output=""
  if output="$(PATH="$empty_path" "$real_bash" "$verifier" 2>&1)"; then
    echo "expected verifier to fail without rg: ${verifier#"$repo_root/"}" >&2
    exit 1
  fi
  if [[ "$output" != *"missing required tool: rg"* ]]; then
    echo "verifier failed without the required missing-rg diagnostic: ${verifier#"$repo_root/"}" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
done

echo "all production verifiers fail closed without rg (${#verifiers[@]} checked)"
