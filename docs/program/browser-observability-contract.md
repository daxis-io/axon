# Browser Observability Contract

- Date: 2026-05-01
- Scope: repo-owned metrics and evidence emitted by the browser lakehouse worker boundary and runtime artifact path

This is the list of metrics and routing signals the repo emits today. There are no dashboards, alert routing, or service-level telemetry pipelines in this repository.

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
- `fallback_reason`: why the browser path rerouted to native, when it did
- `arrow_ipc_bytes`: Arrow IPC result byte length reported in Daxis result envelopes when present
- `QueryResponse` does not carry row-shaped result payloads; large results stay in the SDK worker envelope as Arrow IPC bytes.

## Daxis Result Metrics Mapping

`DaxisResultMetrics`

Executed `daxis.query_result.v1` envelopes map Axon runtime facts into Daxis-facing names:

- `rows_returned`: maps from `QueryMetricsSummary.rows_emitted`
- `arrow_ipc_bytes`: maps from the Arrow IPC result byte length
- `scan_bytes`: maps from `QueryMetricsSummary.bytes_fetched`
- `duration_ms`: maps from `QueryMetricsSummary.duration_ms`
- `files_touched`: maps from `QueryMetricsSummary.files_touched`
- `files_skipped`: maps from `QueryMetricsSummary.files_skipped`
- `row_groups_touched`: maps from `QueryMetricsSummary.row_groups_touched` when tracked
- `row_groups_skipped`: maps from `QueryMetricsSummary.row_groups_skipped` when tracked
- `footer_reads`: maps from `QueryMetricsSummary.footer_reads` when tracked
- `snapshot_bootstrap_duration_ms`: maps from `QueryMetricsSummary.snapshot_bootstrap_duration_ms` when tracked
- `access_mode`: maps from `QueryMetricsSummary.access_mode` when tracked

Fallback, rejection, failure, and cancellation envelopes keep `metrics` as an object and may omit runtime-only fields that were not observed before handoff.

`BrowserWorkerArtifactReport`

- `runtime_sku`: `browser_datafusion` for the Daxis-facing default `axon-web-wasm` worker; `narrow` remains the legacy `browser-engine-worker` compatibility SKU
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

`BrowserWorkerEventEnvelope`

Live worker events are typed observability messages emitted before the final worker response. They don't replace `QueryResponse.metrics`, and large results still move through Arrow IPC in `BrowserWorkerResponseEnvelope::Success`.

- `progress`: lifecycle stage for `instantiate`, `open`, or `query`; stages are `started`, `planning`, `executing`, `arrow_ipc_ready`, and `finished`
- `log`: structured worker log with context, message, and `debug`, `info`, `warn`, or `error` level
- `range_read_metrics`: live range-read and bootstrap fields: bytes fetched, files touched/skipped, row groups touched/skipped, footer reads, rows emitted, snapshot bootstrap duration, and access mode when tracked
- `cache_metrics`: session cache counters plus optional browser object-store transport cache counters
- `fallback`: the `FallbackReason` observed before a fallback response or fallback-required error
- `cancellation`: query cancellation surfaced as a typed event before the terminal response
- `terminal_error`: final `QueryError` emitted before `BrowserWorkerResponseEnvelope::Error`

Every command-scoped event carries a context with `phase`, `request_id`, `table_name`, and, for query commands, `query_id`. Until the public query request grows its own query identifier, the worker sets `query_id` to the SQL command `request_id`.

The browser TypeScript SDK routes `BrowserWorkerEventEnvelope` messages to the optional `onEvent` handler and keeps them separate from terminal worker response envelopes, so live events don't resolve or reject active requests.

## Current Evidence Sources

- `crates/wasm-http-object-store` computes transport cache metrics for the browser object-store seam.
- `crates/wasm-query-runtime` computes browser metrics and fallback outcomes.
- `crates/browser-sdk` preserves those fields through the worker envelope.
- `crates/browser-engine-worker` reports runtime SKU, Arrow IPC result transport, artifact identity, startup, memory baselines, and live worker events for worker instantiate/open/query handling.
- `tests/perf/report_browser_worker_artifact.sh` enforces the shipped worker artifact size budget.
- `tests/security/verify_browser_dependency_guardrails.sh` proves the worker dependency tree and artifact stay inside the current trust boundary.

## Dashboard Inputs For External Teams

These inputs are ready for an external dashboard pipeline once the trusted service and telemetry plumbing exist:

- bytes fetched
- files touched
- files skipped
- row groups touched
- row groups skipped
- footer reads
- rows emitted
- snapshot bootstrap duration
- arrow IPC bytes
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

The Daxis-specific dashboard, runbook, rollout-control, and compatibility-dashboard handoff is recorded in [`daxis-operational-maturity.md`](../integrations/daxis/daxis-operational-maturity.md) and checked by [`../release-gates/daxis-operational-readiness.json`](../release-gates/daxis-operational-readiness.json). That handoff uses these repo-owned fields as inputs, then adds Daxis-owned tenant, workspace, catalog, policy, access-mode, audit, rollout, and oncall context.

## Alert Candidates

Repo-owned thresholds or trends an external dashboard might watch:

- worker artifact size budget failure
- unexpected worker SKU or artifact identity drift across release evidence
- a rise in browser fallback frequency by capability gate
- a rise in persistent cache errors at the browser object-store seam
- loss of pruning effectiveness shown by `files_skipped`
- loss of row-group pruning effectiveness shown by `row_groups_skipped`
- browser startup or memory baseline drift
- dependency guardrail failures

## Explicit Non-Claims

- No dashboard or alerting system is implemented in this repository.
- No control-plane or service-level request correlation exists in this repository.
- Query-level cache-hit ratio is deferred until object-store cache metrics are plumbed through the worker and telemetry pipeline.
