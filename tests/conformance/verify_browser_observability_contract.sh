#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_BROWSER_OBSERVABILITY_CONTRACT_REPO_ROOT:-$(pwd)}"
contract_file="${AXON_BROWSER_OBSERVABILITY_CONTRACT_FILE:-docs/program/browser-observability-contract.md}"

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf "%s\n" "$path"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$path"
}

contract_path="$(resolve_path "$contract_file")"
if [[ ! -f "$contract_path" ]]; then
  echo "missing browser observability contract: $contract_file" >&2
  exit 1
fi

python3 - "$contract_path" "$contract_file" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
contract_file = sys.argv[2]
text = path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_text(needle: str, description: str) -> None:
    expect(needle in text, f"{contract_file} missing {description}: {needle}")


for section in [
    "## Repo-Owned Execution Fields",
    "## Daxis Result Metrics Mapping",
    "## Current Evidence Sources",
    "## Dashboard Inputs For External Teams",
    "## Alert Candidates",
    "## Explicit Non-Claims",
]:
    expect_text(section, "section")

for field in [
    "`bytes_fetched`:",
    "`duration_ms`:",
    "`files_touched`:",
    "`files_skipped`:",
    "`row_groups_touched`:",
    "`row_groups_skipped`:",
    "`footer_reads`:",
    "`rows_emitted`:",
    "`snapshot_bootstrap_duration_ms`:",
    "`access_mode`:",
]:
    expect_text(field, "QueryMetricsSummary field")

for field in [
    "`executed_on`: `browser_wasm` or `native`",
    "`capabilities`:",
    "`fallback_reason`:",
    "`arrow_ipc_bytes`:",
    "large results stay in the SDK worker envelope as Arrow IPC bytes",
]:
    expect_text(field, "QueryResponse field")

for field in [
    "`DaxisResultMetrics`",
    "`rows_returned`: maps from `QueryMetricsSummary.rows_emitted`",
    "`arrow_ipc_bytes`: maps from the Arrow IPC result byte length",
    "`scan_bytes`: maps from `QueryMetricsSummary.bytes_fetched`",
    "`duration_ms`: maps from `QueryMetricsSummary.duration_ms`",
    "`files_touched`: maps from `QueryMetricsSummary.files_touched`",
    "`files_skipped`: maps from `QueryMetricsSummary.files_skipped`",
    "`row_groups_touched`: maps from `QueryMetricsSummary.row_groups_touched` when tracked",
    "`row_groups_skipped`: maps from `QueryMetricsSummary.row_groups_skipped` when tracked",
    "`footer_reads`: maps from `QueryMetricsSummary.footer_reads` when tracked",
    "`snapshot_bootstrap_duration_ms`: maps from `QueryMetricsSummary.snapshot_bootstrap_duration_ms` when tracked",
    "`access_mode`: maps from `QueryMetricsSummary.access_mode` when tracked",
    "Fallback, rejection, failure, and cancellation envelopes keep `metrics` as an object",
    "`daxis.query_result.v1`",
]:
    expect_text(field, "Daxis result metrics mapping")

for field in [
    "`runtime_sku`: `browser_datafusion`",
    "`result_transport`: `arrow_ipc`",
    "`identity.package_name`:",
    "`identity.package_version`:",
    "`identity.wasm_artifact`:",
    "`startup.access_mode`:",
]:
    expect_text(field, "BrowserWorkerArtifactReport field")

for field in [
    "`bundle.id`:",
    "`bundle.tier`:",
    "`features.crossOriginIsolated`:",
    "`features.wasmSIMD`:",
    "`features.wasmThreads`:",
    "`features.bigInt64Array`:",
    "Bundle selection is an SDK-side deployment input, not a query result payload.",
]:
    expect_text(field, "BrowserBundleSelection field")

for field in [
    "`bytes_fetched`: bytes read from the backing object source",
    "`bytes_reused`:",
    "`validation_misses`:",
    "`persistent_cache_errors`:",
]:
    expect_text(field, "BrowserTransportMetrics field")

for field in [
    "`progress`:",
    "`log`:",
    "`range_read_metrics`:",
    "`cache_metrics`:",
    "`fallback`:",
    "`cancellation`:",
    "`terminal_error`:",
    "`phase`",
    "`request_id`",
    "`table_name`",
    "`query_id`",
    "live events don't resolve or reject active requests",
]:
    expect_text(field, "BrowserWorkerEventEnvelope field")

for source in [
    "`crates/wasm-http-object-store` computes transport cache metrics",
    "`crates/wasm-query-runtime` computes browser metrics and fallback outcomes.",
    "`crates/browser-sdk` preserves those fields through the worker envelope.",
    "`crates/browser-engine-worker` reports runtime SKU",
    "`tests/perf/report_browser_worker_artifact.sh` enforces the shipped worker artifact size budget.",
    "`tests/security/verify_browser_dependency_guardrails.sh` proves the worker dependency tree",
]:
    expect_text(source, "current evidence source")

for dashboard_input in [
    "- bytes fetched",
    "- files touched",
    "- files skipped",
    "- row groups touched",
    "- row groups skipped",
    "- footer reads",
    "- rows emitted",
    "- snapshot bootstrap duration",
    "- arrow IPC bytes",
    "- access mode",
    "- fallback reason",
    "- runtime SKU",
    "- result transport",
    "- selected bundle ID and tier",
    "- platform features used for bundle selection",
    "- browser object-store cache bytes reused",
    "- browser object-store persistent cache errors",
    "- worker artifact identity",
    "- worker artifact size",
    "- worker startup baseline",
    "- worker memory baseline",
]:
    expect_text(dashboard_input, "dashboard input")

for alert in [
    "- worker artifact size budget failure",
    "- unexpected worker SKU or artifact identity drift across release evidence",
    "- a rise in browser fallback frequency by capability gate",
    "- a rise in persistent cache errors at the browser object-store seam",
    "- loss of pruning effectiveness shown by `files_skipped`",
    "- loss of row-group pruning effectiveness shown by `row_groups_skipped`",
    "- browser startup or memory baseline drift",
    "- dependency guardrail failures",
]:
    expect_text(alert, "alert candidate")

for boundary_text in [
    "There are no dashboards, alert routing, or service-level telemetry pipelines in this repository.",
    "The Daxis-specific dashboard, runbook, rollout-control, and compatibility-dashboard handoff is recorded",
    "Daxis-owned tenant, workspace, catalog, policy, access-mode, audit, rollout, and oncall context",
    "No dashboard or alerting system is implemented in this repository.",
    "No control-plane or service-level request correlation exists in this repository.",
    "Query-level cache-hit ratio is deferred until object-store cache metrics are plumbed through the worker and telemetry pipeline.",
]:
    expect_text(boundary_text, "repo/Daxis observability boundary")

print("Browser observability contract verified")
PY
