#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "missing required tool: rg" >&2
  exit 127
fi

repo_root="$(git rev-parse --show-toplevel)"
strategy="$repo_root/docs/program/browser-lakehouse-engine-strategy.md"

# Markdown code spans are literal verifier inputs.
# shellcheck disable=SC2016
for phrase in \
  'production root `Cargo.lock`' \
  'schema-v2 stack lock' \
  'annotated RC and final tags' \
  'DCO identity' \
  'downstream fan-in' \
  'release attestation' \
  'Chromium, Firefox, and WebKit' \
  'exact staged artifact'; do
  if ! rg -Fq "$phrase" "$strategy"; then
    echo "browser lakehouse strategy v2 verification failed: missing acceptance phrase: $phrase" >&2
    exit 1
  fi
done

if rg -Fq '| Dependency and artifact       | Independent lock;' "$strategy"; then
  echo "browser lakehouse strategy v2 verification failed: stale independent-lock gate remains" >&2
  exit 1
fi

for relative in \
  docs/program/browser-owned-descriptor-materialization.md \
  docs/program/browser-uc-brokered-runtime-contract.md \
  docs/program/provider-model.md \
  docs/program/browser-embedding-deployment.md; do
  if ! rg -Fq 'Supersession notice' "$repo_root/$relative"; then
    echo "browser lakehouse strategy v2 verification failed: missing supersession notice in $relative" >&2
    exit 1
  fi
  if ! rg -Fq 'BrowserDeltaTableDescriptor' "$repo_root/$relative"; then
    echo "browser lakehouse strategy v2 verification failed: supersession notice omits BrowserDeltaTableDescriptor in $relative" >&2
    exit 1
  fi
done

echo "browser lakehouse strategy v2 acceptance and supersession contract passed"
