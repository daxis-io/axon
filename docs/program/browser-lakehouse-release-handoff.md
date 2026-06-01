# Browser Lakehouse Release Handoff

- Date: 2026-04-07
- Planning baseline commit: `0e19f1d`

This is the repo-owned handoff bundle for the browser lakehouse release seam. It describes the contracts that exist in the repo today and points at canonical JSON examples that external service, security, and SRE work can build against without assuming hidden in-repo infrastructure.

## Baseline

- The March 31 browser-engine release baseline for this bundle is git commit `0e19f1d`.
- There's still no `services/query-api` directory or equivalent trusted-service implementation in this repository.
- Delta snapshot reconstruction already lives in `crates/wasm-delta-snapshot`.
- Browser execution V1 is the narrow runtime plus streaming scan plus an in-memory session shell. The Daxis-facing app worker now has a separate browser DataFusion artifact report and runtime isolation gate.
- The shipped session shell is in-memory only.
- Persistent-cache hooks exist in repo, with a narrow OPFS extent-cache backend below the in-memory session shell. OPFS / IndexedDB session-level persistent caches are still deferred.
- Signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation are external blockers.

Those external blockers stay outside the repo-owned success claims in this bundle.

## Canonical Contract Examples

All examples below are checked into [`docs/program/browser-lakehouse-release-handoff-examples/`](./browser-lakehouse-release-handoff-examples).

- `snapshot-resolution-request.latest.json`: latest-snapshot lookup request sent to the trusted control-plane seam.
- `resolved-snapshot-descriptor.supported.json`: supported metadata-only snapshot descriptor returned by `crates/delta-control-plane`.
- `browser-http-snapshot-descriptor.native-only.json`: browser-facing descriptor shape after trusted URL attachment for a native-only table capability. The descriptor is still valid, but browser execution has to reroute before I/O.
- `query-error.unknown-protocol-feature.json`: terminal hard-fail example for unknown Delta protocol features. It's emitted instead of any descriptor because trusted snapshot resolution fails closed.
- `browser-worker-command.open-table.json`: session-opening worker command carrying a browser HTTP snapshot descriptor.
- `browser-worker-request.supported.json`: session-backed browser worker SQL command carrying a `QueryRequest`.
- `browser-worker-command.dispose.json`: worker command that releases an in-memory table handle.
- `browser-worker-response.opened.json`: session-open acknowledgement from the worker.
- `browser-worker-response.supported.json`: supported browser execution result envelope with browser telemetry.
- `browser-worker-response.native-fallback.json`: native reroute result carrying its fallback reason.
- `browser-worker-response.disposed.json`: session-dispose acknowledgement from the worker.
- `browser-worker-response.hard-fail.json`: terminal error envelope with no fallback reason.
- `browser-worker-artifact-report.narrow.json`: legacy narrow worker artifact report showing the compatibility runtime SKU, session-shell capability, Arrow IPC worker boundary, and artifact identity.
- `browser-worker-artifact-report.datafusion.json`: Daxis-facing app worker artifact report showing `browser_datafusion` as the default runtime SKU for descriptor-backed browser reads.

Daxis-specific handoff fixtures live in [`docs/program/daxis-first-class-integration-examples/`](./daxis-first-class-integration-examples). They cover `POST /v1/query/delta/snapshot-descriptor`, descriptor fail-closed cases, `POST /v1/catalog/read-access-plan`, grant-scoped object route payloads, the object-grant OpenAPI schema, audit event evidence shape, and the browser-safe `brokered_delta`, `delta_sharing`, `sql_fallback_required`, and `blocked` plan outcomes. The release-facing hash manifest lives at [`docs/release-gates/daxis-contract-artifacts.sha256`](../release-gates/daxis-contract-artifacts.sha256).

## Field Semantics

`browser_compatibility`

- Snapshot-level report of what the browser is allowed to claim for the resolved table or descriptor.
- When empty, the descriptor is inside the baseline supported browser envelope documented in `README.md`.
- When populated, the browser runtime treats `native_only`, `unsupported`, and `experimental` states as policy, not hints.

`required_capabilities`

- The runtime gating contract used at materialization, bootstrap, and execution boundaries.
- `native_only` means reroute to native before any browser I/O.
- `unsupported` means hard fail rather than reroute.

`fallback_reason`

- The reroute reason that has to survive the runtime, router, and SDK boundaries.
- Presence means the error is reroutable.
- Absence means the error is terminal at the boundary that raised it.
- It stays as metadata on `QueryResponse` or `QueryError`; the worker boundary never switches large results back to row-oriented JSON.

`result`

- `BrowserWorkerSuccessEnvelope.result` carries Arrow IPC bytes plus the declared IPC format and content type.
- Legacy row-shaped payloads aren't part of the frozen worker contract and are rejected at deserialize time.

`opened` / `disposed`

- `BrowserWorkerResponseEnvelope::Opened` and `::Disposed` acknowledge session lifecycle changes without pretending those operations produced query results.
- The public worker contract is one-shot Arrow IPC only for large query results. There's still no public result stream.

`runtime_sku`

- Worker artifact reports use `narrow` for the legacy browser runtime plus in-memory session shell.
- Worker artifact reports use `browser_datafusion` for the Daxis-facing app worker that routes descriptor-backed reads through `axon-web-wasm`.
- `sql` is reserved for a future non-DataFusion browser SQL SKU.

`capabilities`

- `session_shell = true` means the worker owns reusable in-memory table/session state.
- `browser_datafusion = false` means the narrow compatibility worker is not claiming the DataFusion execution target.
- `browser_datafusion = true` means the Daxis-facing app worker routes browser SQL execution through the DataFusion session path.
- The OPFS extent-cache backend sits below the session shell; IndexedDB and session-level persistence stay deferred even when `session_shell = true`.

`result_transport`

- Worker artifact reports use `arrow_ipc` to name the only large-result transport across the worker boundary.

`identity`

- Worker artifact reports publish `package_name`, `package_version`, and `wasm_artifact` so release evidence, perf gates, and smoke tests all refer to the same artifact.

`snapshot_version`

- Omitted in `SnapshotResolutionRequest` means latest snapshot.
- Present means explicit time-travel to a concrete version.
- The resolved descriptor always returns the concrete snapshot version that was opened.

Browser HTTP URL attachment

- `attach_browser_http_urls` only validates and carries caller-supplied HTTPS object URLs.
- URL issuance, TTL, object scoping, audit fields, request correlation, and production CORS behavior are external work outside this repo.

## Evidence In Repo

- `docs/program/browser-datafusion-runtime-parity.md` records the newer `apps/axon-web` browser DataFusion runtime parity envelope: supported SQL classes, field types, scan optimizations, unsupported feature categories, and browser-worker proof.
- `crates/query-contract/tests/release_handoff_examples.rs` validates the request, descriptor, and hard-fail examples.
- `crates/query-contract/tests/release_handoff_examples.rs` also validates the Daxis first-class integration fixtures for descriptor resolution, read-access-plan outcomes, object-grant routes, and object-grant audit event evidence.
- `crates/query-contract/tests/query_contract.rs` validates the checked-in read-access-plan JSON Schema and object-grant OpenAPI fixture, including the closed `ObjectGrantAuditEvent` component.
- `tests/conformance/verify_daxis_contract_artifacts.sh` verifies the Daxis SDK examples, schemas, OpenAPI document, and JSON fixtures against the checked-in SHA-256 manifest.
- `crates/browser-sdk/tests/release_handoff_examples.rs` validates the session-backed worker command and response examples.
- `crates/browser-engine-worker/tests/worker_artifact.rs` validates the worker artifact report example and the session-capability / Arrow IPC reporting contract.
- `cargo test -p axon-web-wasm` validates the DataFusion app worker artifact report example and the browser DataFusion bridge contract.
- `crates/delta-control-plane/tests/browser_http_descriptor.rs` covers deterministic browser URL attachment and URL redaction.
- `crates/delta-control-plane/tests/browser_snapshot_preflight.rs` covers descriptor-to-runtime supported-path execution over loopback-served fixture data.
- `crates/wasm-query-runtime/tests/browser_runtime.rs` covers native-only reroutes and terminal unsupported behavior before browser I/O.
- `crates/wasm-datafusion-session` and `crates/wasm-datafusion-poc` cover browser DataFusion schema classification, SQL scope enforcement, scan pruning/residual correctness, budgets, cancellation, and Arrow IPC output.

## Explicit External Boundary

These items are external and stay external in release docs until code exists:

- signed URL issuance
- proxy-mode request issuance
- signed URL TTL policy approval
- object-scoped URL enforcement
- audit logging and request correlation
- production XML-endpoint CORS/origin validation
- live dashboards
- production oncall / control-plane outage handling
- Daxis rollout controls and production table compatibility dashboard

Repo-deferred, but not external-service proof:

- IndexedDB persistent-cache backends and session-level persistent table caches
- release-process full app-size evidence for `axon-web-wasm`; the exact Daxis default-worker command is recorded in [`docs/release-gates/daxis-browser-datafusion-budget-profile.json`](../release-gates/daxis-browser-datafusion-budget-profile.json), and the Daxis M3 isolation plan is recorded in [`docs/release-gates/daxis-browser-runtime-isolation-plan.json`](../release-gates/daxis-browser-runtime-isolation-plan.json)

See [`docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`](../release-gates/browser-wasm-delta-gcs-external-blockers.md) for the owner-by-owner blocker register.
