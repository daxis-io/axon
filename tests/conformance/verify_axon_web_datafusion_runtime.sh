#!/usr/bin/env bash

set -euo pipefail

tree="$(cargo tree -p axon-web-wasm --target wasm32-unknown-unknown --locked)"

if ! rg -n '(^|[[:space:]])wasm-datafusion-session v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must use the dedicated DataFusion session crate" >&2
  exit 1
fi

if ! rg -n '(^|[[:space:]])wasm-datafusion-poc v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must include the DataFusion session path" >&2
  exit 1
fi

if ! rg -n '(^|[[:space:]])datafusion v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must include DataFusion" >&2
  exit 1
fi

if rg -n '(^|[[:space:]])wasm-query-session v' <<<"$tree"; then
  echo "axon-web-wasm production wasm runtime must not depend on the legacy narrow query session" >&2
  exit 1
fi

echo "axon-web WASM DataFusion runtime verified"
