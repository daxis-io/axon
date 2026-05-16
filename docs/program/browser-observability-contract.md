# Browser Observability Contract

- Date: 2026-05-01
- Scope: repo-owned metrics and evidence emitted by the browser lakehouse worker boundary and runtime artifact path

This document defines the metrics and routing signals the repository already emits today. It does not claim that dashboards, alert routing, or service-level telemetry pipelines exist in this repository.

## Repo-Owned Execution Fields

`QueryMetricsSummary`

- `bytes_fetched`: bytes read for the executed query path
- `duration_ms`: wall-clock query duration
- `files_touched`: files opened for execution
- `files_skipped`: files skipped by pruning
- `row_groups_touched`: Parquet row groups decoded by the browser scan layer when tracked; native responses currently report `0`
- `row_groups_skipped`: Parquet row groups skipped by browser row-group pruning when tracked; native responses currently report `0`
- `footer_reads`: browser snapshot-bootstrap footer reads when tracked
- `rows_emitted`: rows emitted by the scan layer before projection, filtering, aggregation, ordering, or limit are applied
- `snapshot_bootstrap_duration_ms`: browser snapshot-bootstrap wall-clock duration when tracked
- `access_mode`: browser object access mode when tracked

`QueryResponse`

- `executed_on`: `browser_wasm` or `native`
- `capabilities`: capability report returned by the runtime chosen for execution
- `fallback_reason`: structured reroute reason when the browser path required native execution
- `QueryResponse` does not carry row-shaped result payloads; large results stay in the SDK worker envelope as Arrow IPC bytes.

`BrowserWorkerArtifactReport`

- `runtime_sku`: `narrow` for the shipped browser runtime, with `sql` reserved as the larger future SKU label
- `result_transport`: `arrow_ipc`, the only large-result transport across the worker boundary
- `identity.package_name`: Cargo package name for the shipped worker artifact
- `identity.package_version`: Cargo package version for the shipped worker artifact
- `identity.wasm_artifact`: canonical wasm artifact filename used by perf and release gates
- `startup.access_mode`: browser object access mode used by the runtime defaults

`BrowserBundleSelection`

- `bundle.id`: browser SDK selected worker/WASM bundle ID
- `bundle.tier`: `baseline`, `simd`, `threaded`, or `simd_threaded`
- `features.crossOriginIsolated`: whether deployment headers enabled cross-origin isolation
- `features.wasmSIMD`: whether the browser accepts the SDK's WASM SIMD probe
- `features.wasmThreads`: whether cross-origin isolation and shared WASM memory are available
- `features.bigInt64Array`: whether `BigInt64Array` exists in the browser runtime
- Bundle selection is an SDK-side deployment input, not a query result payload.

`BrowserTransportMetrics`

- `bytes_fetched`: bytes read from the backing object source by the browser object-store seam
- `bytes_reused`: bytes served from in-memory or persistent extent cache
- `validation_misses`: stale in-memory extents rejected after object identity changed
- `persistent_cache_errors`: persistent-cache load or store failures treated as cache misses

## Current Evidence Sources

- `crates/wasm-http-object-store` computes transport cache metrics for the browser object-store seam.
- `crates/wasm-query-runtime` computes browser metrics and structured fallback outcomes.
- `crates/browser-sdk` preserves those fields through the worker envelope.
- `crates/browser-engine-worker` reports runtime SKU, Arrow IPC result transport, artifact identity, startup, and memory baselines.
- `tests/perf/report_browser_worker_artifact.sh` enforces the shipped worker artifact size budget.
- `tests/security/verify_browser_dependency_guardrails.sh` proves the worker dependency tree and artifact remain inside the current trust boundary.

## Dashboard Inputs For External Teams

The following inputs are ready for an external dashboard pipeline once the trusted service and telemetry plumbing exist:

- bytes fetched
- files touched
- files skipped
- row groups touched
- row groups skipped
- footer reads
- rows emitted
- snapshot bootstrap duration
- access mode
- fallback reason
- runtime SKU
- result transport
- selected bundle ID and tier
- platform features used for bundle selection
- browser object-store cache bytes reused
- browser object-store persistent cache errors
- worker artifact identity
- worker artifact size
- worker startup baseline
- worker memory baseline

## Alert Candidates

These are the repo-owned thresholds or trend candidates that external dashboards may choose to monitor:

- worker artifact size budget failure
- unexpected worker SKU or artifact identity drift across release evidence
- unexpected rise in browser fallback frequency by capability gate
- unexpected rise in persistent cache errors at the browser object-store seam
- loss of pruning effectiveness shown by `files_skipped`
- loss of row-group pruning effectiveness shown by `row_groups_skipped`
- browser startup or memory baseline drift
- dependency guardrail failures

## Explicit Non-Claims

- No dashboard or alerting system is implemented in this repository.
- No control-plane or service-level request correlation exists in this repository.
- Query-level cache-hit ratio remains deferred until object-store cache metrics are plumbed through the worker and telemetry pipeline.
