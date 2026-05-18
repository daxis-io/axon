#!/usr/bin/env bash

set -euo pipefail

embed="examples/embed/index.html"

if ! rg -q "createAxonBrowserClient" "$embed"; then
  echo "embedding sketch must use the browser SDK client instead of hand-rolled worker messages" >&2
  exit 1
fi

if ! rg -q "openDeltaTable\\(" "$embed"; then
  echo "embedding sketch must open descriptors through openDeltaTable()" >&2
  exit 1
fi

if ! rg -q "\\.query\\(" "$embed"; then
  echo "embedding sketch must run SQL through the SDK query() call" >&2
  exit 1
fi

if rg -n "kind: ['\"](open_delta_table|query|progress|result|error)['\"]|env\\.kind|wasmUrl" "$embed"; then
  echo "embedding sketch must not document the obsolete kind-based worker protocol" >&2
  exit 1
fi

echo "embedding sketch contract verified"
