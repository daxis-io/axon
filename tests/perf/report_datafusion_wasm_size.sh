#!/usr/bin/env bash

set -euo pipefail

package="${AXON_DF_SIZE_PACKAGE:-axon-web-wasm}"
wasm_stem="${AXON_DF_SIZE_WASM_STEM:-${package//-/_}}"
default_brotli_budget_bytes="6291456"
if [[ "$package" == "axon-web-wasm" && "$wasm_stem" == "axon_web_wasm" ]]; then
  brotli_budget_bytes="${AXON_DF_BROTLI_BUDGET_BYTES:-$default_brotli_budget_bytes}"
else
  brotli_budget_bytes="${AXON_DF_BROTLI_BUDGET_BYTES:-}"
fi
if [[ "${AXON_DF_SIZE_OUT_DIR+x}" == "x" ]]; then
  out_dir="$AXON_DF_SIZE_OUT_DIR"
else
  out_dir="target/df-size/${package}"
fi
cargo_flags="${AXON_DF_SIZE_CARGO_FLAGS:-}"
raw_wasm="${CARGO_TARGET_DIR:-target}/wasm32-unknown-unknown/release/${wasm_stem}.wasm"
# The shipped form: the same wasm-bindgen invocation as `npm run build:wasm`, which strips the
# debug name section. A names-bearing copy is kept only for twiggy, and the wasm-opt -Oz copy is a
# reference point; neither is shipped, so neither is budgeted.
bindgen_wasm="${out_dir}/${wasm_stem}_bg.wasm"
names_dir="${out_dir}/with-names"
names_wasm="${names_dir}/${wasm_stem}_bg.wasm"
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
  --typescript \
  --remove-name-section
wasm-bindgen "$raw_wasm" \
  --target web \
  --out-dir "$names_dir" \
  --typescript

gzip -9 -c "$bindgen_wasm" > "${bindgen_wasm}.gz"
brotli -f -q 11 "$bindgen_wasm"
wasm-opt -Oz -o "$opt_wasm" "$bindgen_wasm"
brotli -f -q 11 "$opt_wasm"

twiggy top -n 50 "$names_wasm" > "${out_dir}/twiggy-top.txt"
twiggy monos -g -m 30 "$names_wasm" > "${out_dir}/twiggy-monos.txt"

raw_bytes="$(wc -c < "$raw_wasm" | tr -d '[:space:]')"
bindgen_bytes="$(wc -c < "$bindgen_wasm" | tr -d '[:space:]')"
gzip_bytes="$(wc -c < "${bindgen_wasm}.gz" | tr -d '[:space:]')"
brotli_bytes="$(wc -c < "${bindgen_wasm}.br" | tr -d '[:space:]')"
opt_bytes="$(wc -c < "$opt_wasm" | tr -d '[:space:]')"
opt_brotli_bytes="$(wc -c < "${opt_wasm}.br" | tr -d '[:space:]')"

cat > "${out_dir}/summary.md" <<SUMMARY
# ${package} WASM Size Report

| Artifact | Bytes |
| --- | ---: |
| Raw Rust release wasm (keeps the name section; not shipped) | ${raw_bytes} |
| Shipped wasm (wasm-bindgen --remove-name-section, as \`npm run build:wasm\`) | ${bindgen_bytes} |
| gzip -9 of shipped wasm | ${gzip_bytes} |
| Brotli -q 11 of shipped wasm | ${brotli_bytes} |
| wasm-opt -Oz of shipped wasm (reference only; not shipped) | ${opt_bytes} |
| Brotli -q 11 of wasm-opt -Oz wasm (reference only; not shipped) | ${opt_brotli_bytes} |

Budget:

- Brotli budget bytes, applied to the shipped wasm: \`${brotli_budget_bytes:-report-only}\`
- The served asset of an app build is gated separately by
  \`bash tests/perf/report_shipped_wasm_size.sh apps/axon-web/dist\`.

Additional outputs:

- \`${out_dir}/cargo-tree.txt\`
- \`${out_dir}/twiggy-top.txt\` and \`${out_dir}/twiggy-monos.txt\` (from the names-bearing copy)

Release companion browser performance report:

- \`bash tests/perf/browser_query_performance.sh\` records browser startup, cold and warm
  Delta/Parquet query milestones, result bytes, atomic over-limit behavior, exact cursor and
  coordinator memory bounds, and the post-GC user-agent memory delta across the page and workers.

Host-only diagnostic:

- \`bash tests/perf/browser_datafusion_engine_smoke.sh\` records Rust-side streaming init, tiny
  query, Parquet metadata, Delta/Parquet query, and scan metrics without claiming browser timing.
SUMMARY

cat "${out_dir}/summary.md"

if [[ -n "$brotli_budget_bytes" ]]; then
  if [[ ! "$brotli_budget_bytes" =~ ^[0-9]+$ ]]; then
    echo "AXON_DF_BROTLI_BUDGET_BYTES must be an unsigned integer byte count: ${brotli_budget_bytes}" >&2
    exit 1
  fi

  if (( brotli_bytes > brotli_budget_bytes )); then
    echo "DataFusion Brotli budget exceeded: ${brotli_bytes} bytes > ${brotli_budget_bytes} bytes" >&2
    exit 1
  fi
fi
