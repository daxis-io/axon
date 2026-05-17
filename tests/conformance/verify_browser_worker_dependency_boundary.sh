#!/usr/bin/env bash

set -euo pipefail

tree="$(cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked)"

if rg -n '(^|[[:space:]])wasm-datafusion-poc v|(^|[[:space:]])datafusion v' <<<"$tree"; then
  echo "browser-engine-worker must not pull DataFusion in the default shipped worker graph" >&2
  exit 1
fi

if rg -n 'wasm-query-session/datafusion|wasm-datafusion-poc|sqlparser|^datafusion[[:space:]]*=' crates/wasm-query-session/Cargo.toml crates/browser-engine-worker/Cargo.toml; then
  echo "legacy narrow session and shipped worker must not expose a DataFusion feature path" >&2
  exit 1
fi

workspace_tree="$(
  cargo tree --workspace --target wasm32-unknown-unknown --locked -i wasm-datafusion-poc 2>/dev/null || true
)"

if rg -n '(^|[[:space:]])browser-engine-worker v' <<<"$workspace_tree"; then
  echo "workspace feature resolution must not route browser-engine-worker through wasm-datafusion-poc" >&2
  exit 1
fi

echo "browser worker dependency boundary verified"
