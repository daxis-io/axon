# Browser Lakehouse Release Handoff

- Date: 2026-04-07
- Planning baseline commit: `0e19f1d`

This document is the repo-owned handoff bundle for the browser lakehouse release seam. It describes the contracts that exist in this repository today and points at canonical JSON examples that external service, security, and SRE work can consume without assuming hidden in-repo infrastructure.

## Baseline

- The March 31 browser-engine release baseline referenced by this handoff bundle is git commit `0e19f1d`.
- There is still no `services/query-api` directory or equivalent trusted-service implementation in this repository.
- Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`.
- Browser execution V1 is narrow runtime + streaming scan + in-memory session shell, not broad browser DataFusion.
- The shipped session shell is in-memory only.
- Persistent-cache hooks exist in repo, with a narrow OPFS extent-cache backend below the in-memory session shell; OPFS / IndexedDB session-level persistent caches are still deferred.
- Signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation remain external blockers.

Those external blockers stay outside repo-owned success claims in this handoff bundle.

## Canonical Contract Examples

All examples below are checked into [`docs/program/browser-lakehouse-release-handoff-examples/`](./browser-lakehouse-release-handoff-examples).

- `snapshot-resolution-request.latest.json`: latest-snapshot lookup request sent to the trusted control-plane seam.
- `resolved-snapshot-descriptor.supported.json`: supported metadata-only snapshot descriptor returned by `crates/delta-control-plane`.
- `browser-http-snapshot-descriptor.native-only.json`: browser-facing descriptor shape after trusted URL attachment for a native-only table capability. The descriptor is still valid, but browser execution must reroute before I/O.
- `query-error.unknown-protocol-feature.json`: terminal hard-fail example for unknown Delta protocol features. This is emitted instead of any descriptor because trusted snapshot resolution must fail closed.
- `browser-worker-command.open-table.json`: session-opening worker command carrying a browser HTTP snapshot descriptor.
- `browser-worker-request.supported.json`: session-backed browser worker SQL command carrying a `QueryRequest`.
- `browser-worker-command.dispose.json`: worker command that releases an in-memory table handle.
- `browser-worker-response.opened.json`: session-open acknowledgement from the worker.
- `browser-worker-response.supported.json`: supported browser execution result envelope with browser telemetry.
- `browser-worker-response.native-fallback.json`: native reroute result with a structured fallback reason.
- `browser-worker-response.disposed.json`: session-dispose acknowledgement from the worker.
- `browser-worker-response.hard-fail.json`: terminal error envelope with no fallback reason.
- `browser-worker-artifact-report.narrow.json`: shipped worker artifact report showing the default runtime SKU, session-shell capability, Arrow IPC worker boundary, and artifact identity.

## Field Semantics

`browser_compatibility`

- Snapshot-level report describing what the browser is allowed to claim for the table or descriptor that was resolved.
- When empty, the descriptor is inside the baseline supported browser envelope documented in `README.md`.
- When populated, the browser runtime must treat `native_only`, `unsupported`, and `experimental` states as explicit policy, not hints.

`required_capabilities`

- The runtime gating contract used at materialization, bootstrap, and execution boundaries.
- `native_only` means reroute to native before browser I/O.
- `unsupported` means hard fail rather than reroute.

`fallback_reason`

- The structured reason that must survive runtime, router, and SDK boundaries.
- Presence means the error is reroutable.
- Absence means the error is terminal at the boundary that raised it.
- It remains metadata on `QueryResponse` or `QueryError`; the worker boundary never switches large results back to row-oriented JSON.

`result`

- `BrowserWorkerSuccessEnvelope.result` carries Arrow IPC bytes plus the declared IPC format and content type.
- Legacy row-shaped payloads are not part of the frozen worker contract and are rejected at deserialize time.

`opened` / `disposed`

- `BrowserWorkerResponseEnvelope::Opened` and `::Disposed` acknowledge session lifecycle changes without pretending those operations produced query results.
- The public worker contract remains one-shot Arrow IPC only for large query results; there is still no public result stream.

`runtime_sku`

- Worker artifact reports use `narrow` for the current shipped browser runtime plus in-memory session shell.
- `sql` is reserved for the larger browser SQL SKU introduced in later launch-hardening work; Sprint 1 only freezes the vocabulary.

`capabilities`

- `session_shell = true` means the worker owns reusable in-memory table/session state.
- `browser_datafusion = false` means the shipped worker is not claiming broad browser DataFusion as the V1 execution target.
- The OPFS extent-cache backend sits below the session shell; IndexedDB and session-level persistence remain deferred even when `session_shell = true`.

`result_transport`

- Worker artifact reports use `arrow_ipc` to name the only large-result transport across the worker boundary.

`identity`

- Worker artifact reports publish `package_name`, `package_version`, and `wasm_artifact` so release evidence, perf gates, and smoke tests refer to the same artifact identity.

`snapshot_version`

- Omitted in `SnapshotResolutionRequest` means latest snapshot.
- Present means explicit time-travel to a concrete version.
- The resolved descriptor always returns the concrete snapshot version that was opened.

Browser HTTP URL attachment

- `attach_browser_http_urls` only validates and carries caller-supplied HTTPS object URLs.
- URL issuance, TTL, object scoping, audit fields, request correlation, and production CORS behavior remain external work outside this repo.

## Evidence In Repo

- `docs/program/browser-datafusion-runtime-parity.md` records the newer `apps/axon-web` browser DataFusion runtime parity envelope, including supported SQL classes, field types, scan optimizations, unsupported feature categories, and browser-worker proof.
- `crates/query-contract/tests/release_handoff_examples.rs` validates the request, descriptor, and hard-fail examples.
- `crates/browser-sdk/tests/release_handoff_examples.rs` validates the session-backed worker command and response examples.
- `crates/browser-engine-worker/tests/worker_artifact.rs` validates the worker artifact report example and session-capability / Arrow IPC reporting contract.
- `crates/delta-control-plane/tests/browser_http_descriptor.rs` covers deterministic browser URL attachment and URL redaction behavior.
- `crates/delta-control-plane/tests/browser_snapshot_preflight.rs` covers descriptor-to-runtime supported-path execution over loopback-served fixture data.
- `crates/wasm-query-runtime/tests/browser_runtime.rs` covers native-only reroutes and terminal unsupported behavior before browser I/O.
- `crates/wasm-datafusion-session` and `crates/wasm-datafusion-poc` cover browser DataFusion schema classification, SQL scope enforcement, scan pruning/residual correctness, budgets, cancellation, and Arrow IPC output.

## Explicit External Boundary

These items are still external and must stay external in release docs until code exists:

- signed URL issuance
- proxy-mode request issuance
- signed URL TTL policy approval
- object-scoped URL enforcement
- audit logging and request correlation
- production XML-endpoint CORS/origin validation
- live dashboards
- production oncall / control-plane outage handling

Repo-deferred but not external-service proof:

- IndexedDB persistent-cache backends and session-level persistent table caches
- browser DataFusion as the default SKU

See [`docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`](../release-gates/browser-wasm-delta-gcs-external-blockers.md) for the owner-by-owner blocker register.
