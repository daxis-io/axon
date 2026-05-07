#!/usr/bin/env bash

set -euo pipefail

package="${AXON_DF_SIZE_PACKAGE:-wasm-datafusion-poc}"
wasm_stem="${AXON_DF_SIZE_WASM_STEM:-${package//-/_}}"
if [[ "${AXON_DF_SIZE_OUT_DIR+x}" == "x" ]]; then
  out_dir="$AXON_DF_SIZE_OUT_DIR"
else
  out_dir="target/df-size/${package}"
fi
cargo_flags="${AXON_DF_SIZE_CARGO_FLAGS:-}"
raw_wasm="target/wasm32-unknown-unknown/release/${wasm_stem}.wasm"
bindgen_wasm="${out_dir}/${wasm_stem}_bg.wasm"
opt_wasm="${out_dir}/${wasm_stem}_bg.opt.wasm"

reject_unsafe_out_dir() {
  echo "unsafe AXON_DF_SIZE_OUT_DIR: ${out_dir:-<empty>} ($1)" >&2
  exit 1
}

validate_out_dir() {
  case "$out_dir" in
    "")
      reject_unsafe_out_dir "value must not be empty"
      ;;
    "." | "/" | "target/df-size" | "target/df-size/")
      reject_unsafe_out_dir "directory is too broad"
      ;;
    /*)
      reject_unsafe_out_dir "absolute paths are not allowed"
      ;;
    ".." | ../* | */../* | */..)
      reject_unsafe_out_dir "parent-directory traversal is not allowed"
      ;;
    ./* | */./* | */.)
      reject_unsafe_out_dir "current-directory path components are not allowed"
      ;;
    *//*)
      reject_unsafe_out_dir "empty path components are not allowed"
      ;;
  esac

  case "$out_dir" in
    target/df-size/*)
      ;;
    *)
      reject_unsafe_out_dir "must be under target/df-size/<child>"
      ;;
  esac
}

require_tool() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required tool: $1" >&2
    exit 1
  fi
}

validate_out_dir

require_tool cargo
require_tool wasm-bindgen
require_tool wasm-opt
require_tool gzip
require_tool brotli
require_tool twiggy

rm -rf "$out_dir"
mkdir -p "$out_dir"

# shellcheck disable=SC2086
cargo tree -p "$package" --target wasm32-unknown-unknown -e normal,features --locked $cargo_flags \
  > "${out_dir}/cargo-tree.txt"
# shellcheck disable=SC2086
cargo build -p "$package" --target wasm32-unknown-unknown --release --locked $cargo_flags

wasm-bindgen "$raw_wasm" \
  --target web \
  --out-dir "$out_dir" \
  --typescript

wasm-opt -Oz -o "$opt_wasm" "$bindgen_wasm"
gzip -9 -c "$opt_wasm" > "${opt_wasm}.gz"
brotli -f -q 11 "$opt_wasm"

twiggy top -n 50 "$bindgen_wasm" > "${out_dir}/twiggy-top.txt"
twiggy monos -g -m 30 "$bindgen_wasm" > "${out_dir}/twiggy-monos.txt"

raw_bytes="$(wc -c < "$raw_wasm" | tr -d '[:space:]')"
bindgen_bytes="$(wc -c < "$bindgen_wasm" | tr -d '[:space:]')"
opt_bytes="$(wc -c < "$opt_wasm" | tr -d '[:space:]')"
gzip_bytes="$(wc -c < "${opt_wasm}.gz" | tr -d '[:space:]')"
brotli_bytes="$(wc -c < "${opt_wasm}.br" | tr -d '[:space:]')"

cat > "${out_dir}/summary.md" <<SUMMARY
# ${package} WASM Size Report

| Artifact | Bytes |
| --- | ---: |
| Raw Rust release wasm | ${raw_bytes} |
| wasm-bindgen generated wasm | ${bindgen_bytes} |
| wasm-opt -Oz generated wasm | ${opt_bytes} |
| gzip -9 of optimized wasm | ${gzip_bytes} |
| Brotli -q 11 of optimized wasm | ${brotli_bytes} |

Additional outputs:

- \`${out_dir}/cargo-tree.txt\`
- \`${out_dir}/twiggy-top.txt\`
- \`${out_dir}/twiggy-monos.txt\`

Optional companion smoke report:

- \`bash tests/perf/browser_datafusion_engine_smoke.sh\` records streaming init, first tiny query,
  repeated tiny query, first Parquet metadata query, first real Delta/Parquet query, and scan metrics.
SUMMARY

cat "${out_dir}/summary.md"

if [[ -n "${AXON_DF_BROTLI_BUDGET_BYTES:-}" ]]; then
  if [[ ! "$AXON_DF_BROTLI_BUDGET_BYTES" =~ ^[0-9]+$ ]]; then
    echo "AXON_DF_BROTLI_BUDGET_BYTES must be an unsigned integer byte count: ${AXON_DF_BROTLI_BUDGET_BYTES}" >&2
    exit 1
  fi

  if (( brotli_bytes > AXON_DF_BROTLI_BUDGET_BYTES )); then
    echo "DataFusion Brotli budget exceeded: ${brotli_bytes} bytes > ${AXON_DF_BROTLI_BUDGET_BYTES} bytes" >&2
    exit 1
  fi
fi
