#!/usr/bin/env bash

set -euo pipefail

bash -n tests/perf/report_datafusion_wasm_size.sh
bash -n tests/perf/browser_datafusion_engine_smoke.sh
bash -n tests/perf/browser_query_performance.sh
rg -q '^opt-level = "z"$' Cargo.toml
rg -q 'AXON_DF_SIZE_PACKAGE' tests/perf/report_datafusion_wasm_size.sh
rg -F -q 'package="${AXON_DF_SIZE_PACKAGE:-axon-web-wasm}"' tests/perf/report_datafusion_wasm_size.sh
rg -F -q 'default_brotli_budget_bytes="6291456"' tests/perf/report_datafusion_wasm_size.sh
rg -q 'twiggy monos' tests/perf/report_datafusion_wasm_size.sh
rg -F -q 'target/df-size/${package}' tests/perf/report_datafusion_wasm_size.sh
rg -q 'wasm-opt -Oz generated wasm' tests/perf/report_datafusion_wasm_size.sh
rg -q 'Brotli -q 11 of optimized wasm' tests/perf/report_datafusion_wasm_size.sh
rg -q 'AXON_DF_BROTLI_BUDGET_BYTES' tests/perf/report_datafusion_wasm_size.sh
rg -q 'DataFusion Brotli budget exceeded' tests/perf/report_datafusion_wasm_size.sh
rg -q 'browser_query_performance.sh' tests/perf/report_datafusion_wasm_size.sh
rg -q 'streaming init' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'first tiny query' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'repeated tiny query' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'first Parquet metadata query' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'first real Delta/Parquet query' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'scan metrics' tests/perf/browser_datafusion_engine_smoke.sh
rg -q 'AXON_DF_REPEATED_TINY_QUERY_RUNS' tests/perf/browser_datafusion_engine_smoke.sh
rg -q '"test:browser:query-performance"' apps/axon-web/package.json
rg -q 'playwright.browser-query-performance.config.ts' apps/axon-web/package.json
rg -q '"reportCommand": "bash tests/perf/browser_query_performance.sh"' \
  docs/release-gates/daxis-browser-datafusion-budget-profile.json
rg -q '"hostDiagnosticCommand": "bash tests/perf/browser_datafusion_engine_smoke.sh"' \
  docs/release-gates/daxis-browser-datafusion-budget-profile.json
rg -q '"postGcRetainedHeapDeltaBytes": 16777216' \
  docs/release-gates/daxis-browser-datafusion-budget-profile.json
rg -q 'AXON_BROWSER_QUERY_PERF_ARTIFACT' tests/perf/browser_query_performance.sh
rg -q 'npm run build:wasm' tests/perf/browser_query_performance.sh
rg -q 'test:browser:query-performance' tests/perf/browser_query_performance.sh
rg -q 'AXON_BROWSER_MEMORY_EVIDENCE=1' \
  apps/axon-web/playwright.browser-query-performance.config.ts
rg -q 'measureUserAgentSpecificMemory' apps/axon-web/tests/browser-query-performance.spec.ts

rg -q 'name: Browser DataFusion WASM size budget' .github/workflows/ci.yml
rg -q 'AXON_DF_SIZE_PACKAGE: axon-web-wasm' .github/workflows/ci.yml
rg -q 'AXON_DF_SIZE_WASM_STEM: axon_web_wasm' .github/workflows/ci.yml
rg -q 'AXON_DF_BROTLI_BUDGET_BYTES: 6291456' .github/workflows/ci.yml
if rg -q 'Experimental browser DataFusion WASM size report' .github/workflows/ci.yml; then
  echo "default browser DataFusion size gate must not remain experimental" >&2
  exit 1
fi
if rg -q "github.event_name == 'workflow_dispatch' && inputs.report_datafusion_wasm_size" .github/workflows/ci.yml; then
  echo "default browser DataFusion size gate must run on pull requests and main" >&2
  exit 1
fi

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
