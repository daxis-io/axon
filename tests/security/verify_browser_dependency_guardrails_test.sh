#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

tree_file="$tmpdir/tree.txt"
artifact="$tmpdir/browser_engine_worker.wasm"
package_file="$tmpdir/package.json"
printf 'browser-safe artifact' > "$artifact"
printf '{"dependencies":{}}\n' > "$package_file"

printf 'browser-engine-worker v0.1.0\nopendal v0.50.0\n' > "$tree_file"

if AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  AXON_BROWSER_PACKAGE_FILES="$package_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact"
then
  echo "expected denylisted dependency to be rejected" >&2
  exit 1
fi

printf '{"dependencies":{"@aws-sdk/client-s3":"3.0.0"}}\n' > "$package_file"
printf 'browser-engine-worker v0.1.0\nbrowser-sdk v0.1.0\n' > "$tree_file"

if AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  AXON_BROWSER_PACKAGE_FILES="$package_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact"
then
  echo "expected denylisted browser package to be rejected" >&2
  exit 1
fi

printf '{"dependencies":{}}\n' > "$package_file"
printf 'service_account private_key' > "$artifact"

if AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  AXON_BROWSER_PACKAGE_FILES="$package_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact"
then
  echo "expected secret-like artifact marker to be rejected" >&2
  exit 1
fi

printf 'browser-safe artifact' > "$artifact"
printf 'browser-engine-worker v0.1.0\nbrowser-sdk v0.1.0\n' > "$tree_file"

AXON_BROWSER_WORKER_TREE_FILE="$tree_file" \
  AXON_BROWSER_PACKAGE_FILES="$package_file" \
  bash tests/security/verify_browser_dependency_guardrails.sh "$artifact" >/dev/null

echo "browser dependency guardrail regression coverage passed"
