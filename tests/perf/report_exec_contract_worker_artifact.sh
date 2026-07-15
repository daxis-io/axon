#!/usr/bin/env bash

set -euo pipefail

feature="exec-contract-size-probe"
artifact="target/wasm32-unknown-unknown/release/browser_engine_worker.wasm"
marker="exec_contract_size_probe_encoded_len"
tree="$(cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --features "$feature" --locked)"

for dependency in axon-contract-proto buffa buffa-types; do
  if ! grep -Eq "(^|[[:space:]])${dependency} v" <<<"$tree"; then
    echo "exec contract size probe is missing dependency ${dependency}" >&2
    exit 1
  fi
done

BROWSER_ENGINE_WORKER_FEATURES="$feature" \
  BROWSER_ENGINE_WORKER_ARTIFACT_LABEL="browser-engine-worker exec-contract probe" \
  bash tests/perf/report_browser_worker_artifact.sh

if ! grep -aF "$marker" "$artifact" >/dev/null; then
  echo "exec contract size probe marker is missing from ${artifact}" >&2
  exit 1
fi

echo "exec contract size probe dependencies and retained export verified"
