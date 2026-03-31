#!/usr/bin/env bash

set -euo pipefail

artifact="target/wasm32-unknown-unknown/release/browser_engine_worker.wasm"
budget="${BROWSER_ENGINE_WORKER_WASM_BUDGET_BYTES:-750000}"

cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked

if [ ! -f "$artifact" ]; then
  echo "missing browser worker artifact: $artifact" >&2
  exit 1
fi

size="$(wc -c < "$artifact" | tr -d '[:space:]')"

echo "browser-engine-worker artifact: ${size} bytes (budget ${budget} bytes)"

if [ "$size" -gt "$budget" ]; then
  echo "browser-engine-worker exceeded the wasm artifact budget" >&2
  exit 1
fi
