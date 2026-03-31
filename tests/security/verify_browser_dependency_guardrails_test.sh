#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

tree_file="$tmpdir/tree.txt"
artifact="$tmpdir/browser_engine_worker.wasm"
printf 'browser-safe artifact' > "$artifact"

printf 'browser-engine-worker v0.1.0\nopendal v0.50.0\n' > "$tree_file"

if AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact"
then
  echo "expected denylisted dependency to be rejected" >&2
  exit 1
fi

printf 'browser-engine-worker v0.1.0\nbrowser-sdk v0.1.0\n' > "$tree_file"

AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact" >/dev/null

echo "browser dependency guardrail regression coverage passed"
