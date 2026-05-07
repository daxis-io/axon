#!/usr/bin/env bash

set -euo pipefail

out_dir="${AXON_DF_ENGINE_SMOKE_OUT_DIR:-target/df-size/browser-datafusion-engine-smoke}"
repeated_tiny_query_runs="${AXON_DF_REPEATED_TINY_QUERY_RUNS:-5}"
summary="${out_dir}/summary.md"

reject_unsafe_out_dir() {
  echo "unsafe AXON_DF_ENGINE_SMOKE_OUT_DIR: ${out_dir:-<empty>} ($1)" >&2
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

now_ms() {
  ruby -e 'puts((Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).round)'
}

duration_ms_for() {
  local start_ms="$1"
  local end_ms="$2"
  echo $((end_ms - start_ms))
}

run_timed_step() {
  local key="$1"
  local label="$2"
  shift 2

  local log_file="${out_dir}/${key}.log"
  local start_ms
  local end_ms
  local duration_ms
  start_ms="$(now_ms)"
  "$@" >"$log_file" 2>&1
  end_ms="$(now_ms)"
  duration_ms="$(duration_ms_for "$start_ms" "$end_ms")"

  printf '| %s | %s | `%s` |\n' "$label" "$duration_ms" "$log_file" >> "$summary"
}

validate_out_dir
require_tool cargo
require_tool ruby

if [[ ! "$repeated_tiny_query_runs" =~ ^[1-9][0-9]*$ ]]; then
  echo "AXON_DF_REPEATED_TINY_QUERY_RUNS must be a positive integer: ${repeated_tiny_query_runs}" >&2
  exit 1
fi

rm -rf "$out_dir"
mkdir -p "$out_dir"

cat > "$summary" <<SUMMARY
# Browser DataFusion Engine Smoke Report

This optional smoke report records host-proxy command timings for browser DataFusion viability
tracking. Brotli size is recorded by \`tests/perf/report_datafusion_wasm_size.sh\`.

| Probe | Duration ms | Log |
| --- | ---: | --- |
SUMMARY

run_timed_step \
  "streaming-init" \
  "streaming init" \
  cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture

run_timed_step \
  "first-tiny-query" \
  "first tiny query" \
  cargo test -p wasm-datafusion-poc --locked --test arrow_ipc_export datafusion_query_encodes_arrow_ipc_stream -- --exact

repeated_log="${out_dir}/repeated-tiny-query.log"
repeated_start_ms="$(now_ms)"
for _ in $(seq 1 "$repeated_tiny_query_runs"); do
  cargo test -p wasm-datafusion-poc --locked --test arrow_ipc_export engine_registers_batches_and_runs_sql_to_arrow_ipc -- --exact >>"$repeated_log" 2>&1
done
repeated_end_ms="$(now_ms)"
repeated_duration_ms="$(duration_ms_for "$repeated_start_ms" "$repeated_end_ms")"
printf '| %s | %s | `%s` |\n' "repeated tiny query (${repeated_tiny_query_runs} runs)" "$repeated_duration_ms" "$repeated_log" >> "$summary"

run_timed_step \
  "first-parquet-metadata-query" \
  "first Parquet metadata query" \
  cargo test -p wasm-datafusion-poc --locked --test parquet_scan_exec delta_descriptor_scan_preserves_rows_for_count_star -- --exact

run_timed_step \
  "first-real-delta-parquet-query" \
  "first real Delta/Parquet query" \
  cargo test -p wasm-datafusion-poc --locked --test parquet_scan_exec delta_descriptor_scan_streams_parquet_batches_through_datafusion -- --exact

run_timed_step \
  "scan-metrics" \
  "scan metrics" \
  cargo test -p wasm-datafusion-poc --locked arrow_ipc_result_reports_axon_scan_metrics -- --exact

cat "$summary"
