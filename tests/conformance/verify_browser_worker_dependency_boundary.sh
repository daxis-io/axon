#!/usr/bin/env bash

set -euo pipefail

tree="$(cargo tree -p browser-engine-worker --target wasm32-unknown-unknown --locked)"

if rg -n '(^|[[:space:]])wasm-datafusion-poc v|(^|[[:space:]])datafusion v' <<<"$tree"; then
  echo "browser-engine-worker must not pull DataFusion in the default shipped worker graph" >&2
  exit 1
fi

echo "browser worker dependency boundary verified"
