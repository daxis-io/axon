#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
poc_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
manifest="$poc_dir/Cargo.toml"
wasm="$poc_dir/target/wasm32-unknown-unknown/release/axon_upstream_wasm_browser_harness.wasm"
out_dir="$poc_dir/target/browser-pkg"

cargo build \
  --manifest-path "$manifest" \
  --target wasm32-unknown-unknown \
  --release \
  --locked

wasm-bindgen "$wasm" \
  --target web \
  --out-dir "$out_dir" \
  --out-name axon_upstream_wasm_browser_harness \
  --no-typescript
