#!/usr/bin/env bash

set -euo pipefail

bash -n tests/perf/report_datafusion_wasm_size.sh
rg -q 'AXON_DF_SIZE_PACKAGE' tests/perf/report_datafusion_wasm_size.sh
rg -q 'twiggy monos' tests/perf/report_datafusion_wasm_size.sh
rg -F -q 'target/df-size/${package}' tests/perf/report_datafusion_wasm_size.sh
rg -q 'wasm-opt -Oz generated wasm' tests/perf/report_datafusion_wasm_size.sh
rg -q 'Brotli -q 11 of optimized wasm' tests/perf/report_datafusion_wasm_size.sh

dangerous_out_dirs=(
  ""
  "."
  "/"
  "$(pwd -P)"
  "target/df-size"
  "/tmp/axon-df-size"
  "../target/df-size/package"
  "target/df-size/.."
  "target/df-size/../package"
  "target/not-df-size/package"
)

for unsafe_out_dir in "${dangerous_out_dirs[@]}"; do
  stderr="$(mktemp)"
  if env PATH="/usr/bin:/bin" AXON_DF_SIZE_OUT_DIR="$unsafe_out_dir" \
    bash tests/perf/report_datafusion_wasm_size.sh 2>"$stderr"; then
    echo "expected unsafe AXON_DF_SIZE_OUT_DIR to fail: ${unsafe_out_dir}" >&2
    rm -f "$stderr"
    exit 1
  fi

  if rg -q 'missing required tool|cargo build|cargo tree' "$stderr"; then
    echo "unsafe AXON_DF_SIZE_OUT_DIR was not rejected before tool/build work: ${unsafe_out_dir}" >&2
    cat "$stderr" >&2
    rm -f "$stderr"
    exit 1
  fi

  rg -q 'unsafe AXON_DF_SIZE_OUT_DIR' "$stderr"
  rm -f "$stderr"
done
