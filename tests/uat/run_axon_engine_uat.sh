#!/usr/bin/env bash

set -euo pipefail

out_dir="${AXON_ENGINE_UAT_OUT_DIR:-target/uat/axon-engine-query-uat}"
summary="${out_dir}/summary.md"

reject_unsafe_out_dir() {
  echo "unsafe AXON_ENGINE_UAT_OUT_DIR: ${out_dir:-<empty>} ($1)" >&2
  exit 1
}

validate_out_dir() {
  case "$out_dir" in
    "")
      reject_unsafe_out_dir "value must not be empty"
      ;;
    "." | "/" | "target" | "target/" | "target/uat" | "target/uat/")
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
    target/uat/*)
      ;;
    *)
      reject_unsafe_out_dir "must be under target/uat/<child>"
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

# A cargo test name filter that matches nothing still exits 0:
#
#   test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 38 filtered out
#
# `set -e` cannot catch that, because cargo genuinely succeeded. Every probe
# below is pinned to an exact test name, so a rename or a cfg change would
# silently turn a required probe into a no-op and still report a green run.
# Requiring at least one executed test closes that hole.
assert_tests_executed() {
  local log_file="$1"
  local label="$2"

  if ! grep -q '^test result:' "$log_file"; then
    echo "no libtest summary in output for step: ${label} (see ${log_file})" >&2
    exit 1
  fi

  local passed
  passed="$(sed -n 's/^test result:.*[.] \([0-9][0-9]*\) passed.*/\1/p' "$log_file" \
    | awk '{total += $1} END {print total + 0}')"

  if [ "$passed" -eq 0 ]; then
    echo "step executed zero tests: ${label} (see ${log_file})" >&2
    echo "a test-name filter that matches nothing exits 0; treating it as a failure" >&2
    exit 1
  fi
}

timed_invoke() {
  local key="$1"
  local label="$2"
  local require_tests="$3"
  shift 3

  local log_file="${out_dir}/${key}.log"
  local start_ms
  local end_ms
  local duration_ms
  start_ms="$(now_ms)"
  "$@" >"$log_file" 2>&1
  end_ms="$(now_ms)"
  duration_ms="$(duration_ms_for "$start_ms" "$end_ms")"

  if [ "$require_tests" = "require_tests" ]; then
    assert_tests_executed "$log_file" "$label"
  fi

  printf '| %s | %s | `%s` |\n' "$label" "$duration_ms" "$log_file" >>"$summary"
}

# For probes that are not cargo test invocations and so emit no libtest summary.
run_timed_step() {
  local key="$1"
  local label="$2"
  shift 2

  timed_invoke "$key" "$label" "" "$@"
}

# For cargo test probes: fails the run if the invocation executed no tests.
run_timed_cargo_step() {
  local key="$1"
  local label="$2"
  shift 2

  timed_invoke "$key" "$label" "require_tests" "$@"
}

validate_out_dir
require_tool cargo
require_tool ruby

export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_PROFILE_TEST_DEBUG="${CARGO_PROFILE_TEST_DEBUG:-0}"

rm -rf "$out_dir"
mkdir -p "$out_dir"

cat >"$summary" <<SUMMARY
# Axon Engine UAT Report

| Probe | Duration ms | Log |
| --- | ---: | --- |
SUMMARY

run_timed_cargo_step \
  "uat-query-corpus" \
  "host-engine UAT query corpus" \
  cargo test -p wasm-datafusion-poc --locked --test axon_engine_uat_corpus -- --nocapture

run_timed_cargo_step \
  "actual-wasm-uat-query-corpus" \
  "actual wasm32 UAT query corpus" \
  cargo test -p wasm-datafusion-poc --target wasm32-unknown-unknown --locked --test axon_engine_wasm_uat_corpus -- --nocapture

run_timed_cargo_step \
  "native-latest-snapshot-corpus" \
  "native latest-snapshot corpus" \
  cargo test -p native-query-runtime --locked execute_query_runs_the_native_sql_corpus_with_golden_results -- --exact

run_timed_cargo_step \
  "native-partitioned-pruning-corpus" \
  "native partitioned-pruning corpus" \
  cargo test -p native-query-runtime --locked execute_query_runs_the_partitioned_sql_corpus_with_pruning_metrics -- --exact

run_timed_cargo_step \
  "native-snapshot-version-corpus" \
  "native historical snapshot corpus" \
  cargo test -p native-query-runtime --locked execute_query_runs_the_snapshot_version_sql_corpus_with_golden_results -- --exact

run_timed_cargo_step \
  "browser-native-plan-parity" \
  "browser/native plan parity" \
  cargo test -p delta-control-plane --locked supported_browser_sql_queries_have_native_parity_on_partitioned_fixture -- --exact

run_timed_cargo_step \
  "browser-native-non-aggregate-result-parity" \
  "browser/native non-aggregate result parity" \
  cargo test -p delta-control-plane --locked supported_browser_non_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact

run_timed_cargo_step \
  "browser-native-aggregate-result-parity" \
  "browser/native aggregate result parity" \
  cargo test -p delta-control-plane --locked supported_browser_aggregate_queries_have_native_result_parity_on_partitioned_fixture -- --exact

run_timed_step \
  "browser-datafusion-performance-smoke" \
  "browser DataFusion performance smoke" \
  bash tests/perf/browser_datafusion_engine_smoke.sh

cat "$summary"
