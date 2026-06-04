# Daxis First-Class Integration Strategy

- Date: 2026-05-30
- Status: Working strategy
- Audience: Axon runtime, SDK, web platform, storage platform, and Daxis control-plane developers
- Scope: how Axon should evolve into the default browser read-compute engine for Daxis
- Related:
  - [Browser lakehouse engine strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Delta compatibility matrix](./browser-delta-compatibility-matrix.md)
  - [Browser embedding and deployment](./browser-embedding-deployment.md)
  - [Browser Unity Catalog brokered runtime contract](./browser-uc-brokered-runtime-contract.md)
  - [Browser observability contract](./browser-observability-contract.md)
  - [Browser DataFusion runtime parity](./browser-datafusion-runtime-parity.md)
  - [ADR-0008: Daxis browser read compute contract](../adr/ADR-0008-daxis-browser-read-compute-contract.md)
  - [Daxis operational maturity contract](./daxis-operational-maturity.md)
  - [Daxis external proof handoff](./daxis-external-proof-handoff.md)
  - [Daxis production rollout decisions](../release-gates/daxis-production-rollout-decisions.json)
  - [Daxis strategy traceability matrix](../release-gates/daxis-strategy-traceability.json)
  - [Daxis external proof packet](../release-gates/daxis-external-proof-packet.json)
  - [Daxis external proof attachment template](../release-gates/daxis-external-proof-attachment-template.md)
  - [Daxis release bundle manifest](../release-gates/daxis-release-bundle-manifest.json)
  - [Daxis release attachment template](../release-gates/daxis-release-attachment-template.md)
  - [Daxis release notes template](../release-gates/daxis-release-notes-template.md)
  - [Daxis release migration notes template](../release-gates/daxis-release-migration-notes-template.md)
  - [Browser WASM + Delta on GCS launch checklist](../release-gates/browser-wasm-delta-gcs-launch-checklist.md)
  - [External blockers](../release-gates/browser-wasm-delta-gcs-external-blockers.md)

## Executive Summary

Axon should be developed as the default browser read-compute engine for Daxis. It should be treated as a production engine and SDK boundary, not as a demo application, UI helper, or isolated proof of concept.

The target integration is browser-first and control-plane-governed:

- Daxis owns the headless query gateway: intent normalization, semantic and text-to-SQL compilation, SQL validation, catalog policy, access grants, descriptor resolution, signed or proxied object access, audit, request correlation, budget decisions, routing, normalized result envelopes, and server fallback.
- Axon owns browser SQL execution, Delta/Parquet read behavior, worker and SDK contracts, Arrow IPC result transport, structured runtime metrics, and deterministic fallback vocabulary.

This boundary is the central design rule. The browser runtime must never become authoritative for Daxis policy, cloud credentials, catalog identity, signing, or audit. The browser receives only browser-safe descriptors, opaque grant identifiers, short-lived object-scoped URLs, proxy URLs, non-secret capability facts, and structured fallback or block decisions.

The long-term product behavior should be "browser first, deterministic fallback". For supported interactive reads, Daxis should route execution to Axon in the browser by default. For unsupported query shapes, unsupported Delta features, governed table constraints, storage-access failures, or browser budget violations, the system should return explicit fallback reasons and route to server execution only through Daxis-owned policy.

## Product Vision

Axon should make Daxis feel like a lakehouse that can query data directly from the browser without requiring always-on analytical compute for ordinary exploration. The user-facing outcome is lower latency, lower infrastructure cost, and a product model where governed Delta/Parquet data can be inspected and analyzed through an in-process browser engine whenever policy and compatibility allow.

Axon should be first-class infrastructure for Daxis in the same sense that a query worker, catalog service, or API gateway is first-class infrastructure. It needs stable contracts, compatibility discipline, release evidence, security boundaries, and operational visibility. Daxis developers should be able to integrate Axon without reading internal runtime implementation details or reverse-engineering fixture behavior.

The engineering standard is:

- contract-first APIs
- native/browser correctness parity
- explicit compatibility claims
- deterministic fallback behavior
- browser security guardrails
- reproducible release evidence
- Daxis integration tests that cover real handoff paths
- controlled deprecation and migration policy

Axon should remain a separate engine/runtime repository while Daxis remains the product and control-plane repository. The repos should move together through versioned contracts and integration gates, not through shared hidden assumptions.

## Non-Goals

Axon should not own these responsibilities:

- Daxis user authentication or tenant membership.
- Daxis catalog object lifecycle or governance policy.
- Unity Catalog production BFF behavior.
- Raw cloud credential handling.
- Object signing keys, refresh tokens, service-account JSON, OAuth tokens, HMAC keys, or broad SAS tokens.
- Production audit pipelines, SIEM routing, alert ownership, or workspace allowlists.
- Authoritative server query fallback decisions.
- Delta writes, maintenance commands, optimization jobs, or table mutation.
- Daxis workflow orchestration, run state, ingestion, upload materialization, or billing.

Axon can define contracts that Daxis services implement. It should not implement production Daxis services inside browser Rust, browser TypeScript, tests, or fixtures.

## Integration Boundary

The integration boundary should stay simple and enforceable.

```text
Daxis browser app
        |
        | Axon SDK, worker bundle, Arrow IPC result path
        v
Axon browser runtime
        |
        | browser-safe descriptors, signed URLs, proxy URLs, object grants
        v
Daxis trusted control plane
        |
        | auth, tenancy, catalog, policy, grant issuance, audit, fallback
        v
Delta tables and Parquet objects on object storage
```

Daxis should answer the governance question: "May this browser session read this table, and through which access mode?"

Axon should answer the execution question: "Given a browser-safe table descriptor and SQL request, can the browser execute this read correctly within the supported runtime envelope?"

These two questions should stay separate. If policy is unknown, Daxis must return `blocked` or `sql_fallback_required`. If execution support is unknown, Axon must return a structured unsupported or fallback reason. Neither side should infer permission or compatibility from missing data.

## Default Read-Compute Model

Daxis should make Axon the default read compute path for interactive browser analytics when all of the following are true:

- The table is represented as Delta/Parquet data that Axon supports.
- Daxis policy confirms direct browser execution is allowed.
- The user, tenant, workspace, and catalog identity are authorized by Daxis.
- The table can be exposed through browser-safe descriptors, object grants, signed URLs, or proxy range URLs.
- The table does not require server-only governance semantics such as row filters, column masks, governed views, catalog-managed access rules that cannot be proven in-browser, or unknown policy state.
- Delta protocol features are classified as supported for browser read execution.
- The SQL shape is inside the browser-supported envelope.
- Browser scan, output, and memory budgets are not exceeded.

When any condition fails, Daxis should either block or route to server execution through an explicit fallback contract. The system should not silently execute in a different engine without preserving the reason.

## Headless Query Gateway

Daxis query surfaces all enter one gateway path. Agents, dashboard tiles, builders, saved queries, and API callers differ in product affordance, not in execution authority. Each request carries Daxis-derived metadata such as `surface_kind`, `intent_kind`, `input_artifact_kind`, and `compiled_artifact_kind`, but the client does not get to authorize itself by choosing a surface.

The gateway state machine is:

```text
received_intent
  -> compiled
  -> validated
  -> policy_checked
  -> descriptor_resolved
  -> routed
  -> executing
  -> executed | rejected | fallback | failed | cancelled
```

Daxis owns every state through `routed` and owns the final result envelope for every terminal status. Axon owns only browser execution after it receives a Daxis-approved Axon read descriptor. That descriptor contains validated read-only SQL, table descriptors, access proof, runtime limits, and fallback preference. Axon must never receive raw prompts, builder plans, semantic plans, or unvalidated generated SQL.

The first executable slice is a saved SQL query that compiles to validated Daxis SQL, resolves one browser-safe Delta descriptor, routes to `axon_browser`, executes with Arrow IPC transport, and returns the same envelope shape used by the synthetic agent, dashboard tile, builder, saved query, and API fixtures.

## Target Architecture

The Daxis integration should converge on three layers.

```text
Layer 1: Daxis product and trusted services
  - API gateway
  - auth and tenancy
  - catalog and table policy
  - snapshot descriptor resolver
  - object grant broker
  - signed URL or proxy range service
  - remote query fallback
  - audit and correlation

Layer 2: Axon public integration surface
  - TypeScript browser SDK
  - worker command and response envelopes
  - query-contract Rust types and schemas
  - BrowserHttpSnapshotDescriptor
  - DeltaLocationResolveResponse
  - ReadAccessPlan and object grant contracts
  - fallback and error taxonomy
  - observability event envelope

Layer 3: Axon browser runtime
  - browser DataFusion session
  - Delta descriptor materialization and snapshot reconstruction paths
  - browser-safe HTTP object store
  - Parquet scan and pruning
  - Arrow IPC result transport
  - runtime budgets, cancellation, and metrics
```

The first production integration should use server-side descriptor resolution because it is the smallest trustworthy path:

```text
Daxis query surfaces
  -> Daxis descriptor resolver
  -> BrowserHttpSnapshotDescriptor
  -> Axon openDeltaTable()
  -> Axon query()
  -> Arrow IPC result and QueryResponse metrics
```

The second production integration should add brokered object grants:

```text
Daxis query surfaces
  -> Daxis read-access plan
  -> BrokeredDeltaReadPlan
  -> object grant list/head/batch-sign/range
  -> Axon descriptor materialization or snapshot reconstruction
  -> Axon query()
```

## Contract Strategy

Axon should treat its integration contracts as product APIs.

Primary contracts:

- `QueryRequest`
- `QueryResponse`
- `QueryError`
- `FallbackReason`
- `DaxisResultEnvelope`
- `DaxisApprovedAxonReadDescriptor`
- `BrowserHttpSnapshotDescriptor`
- `DeltaLocationResolveRequest`
- `DeltaLocationResolveResponse`
- `ReadAccessPlan`
- `BrokeredDeltaReadPlan`
- object grant request and response types
- `BrowserWorkerCommand`
- `BrowserWorkerResponseEnvelope`
- `BrowserWorkerEventEnvelope`
- Arrow IPC result envelope
- browser bundle manifest

Contract changes should follow these rules:

- Additive changes are preferred.
- Breaking changes require a migration note and Daxis compatibility plan.
- Runtime behavior changes need tests that prove old and new shapes fail or adapt predictably.
- Generated schemas and TypeScript types should stay aligned with Rust source contracts.
- Every public error or fallback value needs a clear owner and expected caller behavior.
- Deprecated fields should remain for at least one planned Daxis integration window unless there is a security reason to remove them immediately.

Daxis should consume Axon through these contracts, not through private crate internals or app-only fixture helpers.

## Daxis-Facing API Shapes

The first Daxis service endpoint should return a server-resolved descriptor:

```text
POST /v1/query/delta/snapshot-descriptor
```

Responsibilities:

- authenticate user and workspace
- resolve the logical table reference
- validate table policy
- resolve a pinned Delta snapshot
- classify browser compatibility
- mint object-scoped signed URLs or proxy URLs
- return `DeltaLocationResolveResponse`
- include expiry, access mode, and correlation metadata

The later Daxis service endpoint should return a read-access plan:

```text
POST /v1/catalog/read-access-plan
```

Possible plan results:

- `brokered_delta`
- `delta_sharing`
- `sql_fallback_required`
- `blocked`

Object grant routes should stay grant-scoped:

```text
POST /object-grants/{grantId}/list
POST /object-grants/{grantId}/head
POST /object-grants/{grantId}/batch-sign
GET  /object-grants/{grantId}/range
```

Axon should provide validation utilities, examples, SDK adapters, and conformance fixtures for these shapes. Daxis should provide the production implementation.

The Axon-side compatibility fixtures live in [`daxis-first-class-integration-examples/`](./daxis-first-class-integration-examples/). They are checked by `crates/query-contract/tests/release_handoff_examples.rs` so Daxis can build against stable result-envelope, approved-descriptor, descriptor-resolution, read-access-plan, and object-grant route envelopes without importing private Axon internals. The descriptor fixtures include both the signed-URL success path and fail-closed examples for expiry, pinned snapshot mismatch, table-root path escape, and malformed descriptors.

The Axon browser SDK example for M1 lives in [`../../apps/axon-web/examples/daxis-descriptor-resolver.ts`](../../apps/axon-web/examples/daxis-descriptor-resolver.ts). It wraps `openDeltaLocation()` for the Daxis descriptor resolver endpoint, attaches the stable request ID as `x-daxis-request-id`, preserves structured resolver errors and correlation IDs, and keeps credential-profile references out of the worker command payload.

The headless gateway example lives in [`../../apps/axon-web/examples/daxis-headless-query.ts`](../../apps/axon-web/examples/daxis-headless-query.ts). It accepts only a Daxis-approved Axon read descriptor, validates the access proof and runtime limits, bounds open/query execution by `timeout_ms`, cancels timed-out worker queries through the SDK, terminates timed-out headless clients so pending worker requests cannot outlive the Daxis envelope, opens the approved descriptor, executes Daxis-validated read-only SQL, consumes Arrow IPC bytes, maps cancellation-before-handoff, and returns the versioned Daxis result envelope. The `daxis.approved_axon_read.v1` slice is intentionally single-table; multi-table query support needs a new compatibility claim instead of silently widening this contract.

The M2 read-access-plan SDK example lives in [`../../apps/axon-web/examples/daxis-read-access-plan.ts`](../../apps/axon-web/examples/daxis-read-access-plan.ts). It wraps `openUnityCatalogTable()` for `POST /v1/catalog/read-access-plan`, attaches the stable request ID as `x-daxis-request-id`, preserves structured read-plan errors and correlation IDs, consumes brokered or Delta Sharing plans through descriptor handoff, and returns fallback or block states without opening the worker.

The M2 object-grant helper example lives in [`../../apps/axon-web/examples/daxis-object-grant-adapter.ts`](../../apps/axon-web/examples/daxis-object-grant-adapter.ts). It exposes a grant-scoped client for Daxis `list`, `head`, `batch-sign`, and `range` routes, then layers the SDK `BrokeredDeltaPlanAdapter` batch-sign step on top. It preserves request IDs and structured grant errors, rejects expired grants, denied route capabilities, malformed route payloads, invalid ranges, and malformed range responses before handoff, returns range bytes through the grant route, and keeps grant identifiers out of worker command payloads.

The M2 audit fixture [`object-grants.audit-event.range.json`](./daxis-first-class-integration-examples/object-grants.audit-event.range.json) defines the Axon-side shape Daxis can use to prove an object grant read is tied to tenant, workspace, user, table, grant, request, correlation, and Axon query IDs. It is a compatibility fixture, not an Axon-owned production audit pipeline.

The checked-in object-grant OpenAPI fixture [`../../crates/query-contract/schemas/object-grants.openapi.json`](../../crates/query-contract/schemas/object-grants.openapi.json) documents the grant route envelopes and the closed `ObjectGrantAuditEvent` schema for Daxis service implementations.

The Daxis contract artifact hash manifest [`../release-gates/daxis-contract-artifacts.sha256`](../release-gates/daxis-contract-artifacts.sha256) records SHA-256 digests for the Axon SDK examples, JSON Schema, OpenAPI, and JSON fixtures that make up the current Daxis handoff surface. Refresh it with `bash tests/conformance/verify_daxis_contract_artifacts.sh --write` after intentional contract changes, and verify it with `bash tests/conformance/verify_daxis_contract_artifacts.sh` before release handoff. The verifier also rejects browser-visible raw credential fields in those artifacts.

The M3 browser DataFusion budget profile [`../release-gates/daxis-browser-datafusion-budget-profile.json`](../release-gates/daxis-browser-datafusion-budget-profile.json) records the current Daxis scan, output, row, batch, Brotli artifact, smoke timing, and memory-profile gates for the Axon Daxis-facing default worker SKU. It is checked by `cargo test -p wasm-datafusion-poc --test daxis_budget_profile`.

The M3 runtime isolation plan [`../release-gates/daxis-browser-runtime-isolation-plan.json`](../release-gates/daxis-browser-runtime-isolation-plan.json) records how the legacy narrow worker remains isolated as compatibility scaffolding while the Daxis-facing browser DataFusion worker is the Axon default path for descriptor-backed reads. It is checked by `cargo test -p wasm-datafusion-poc --test daxis_runtime_isolation_plan` and names the dependency-boundary commands that prove the narrow and DataFusion paths do not silently merge.

## Daxis Integration Responsibility Plan

The first implementation should keep ownership explicit across product and platform boundaries. Daxis product platform, gateway, catalog, storage broker, and query service own policy, auth, audit, routing, and fallback.

| Area                  | Owner                  | Responsibilities                                                                                                                                                                   | Must Not Own                                                                                                                                    |
| --------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Daxis query surfaces  | Daxis product platform | Send agent, dashboard, builder, saved-query, and API intents through the Daxis gateway, render fallback/block states, and preserve request IDs in product telemetry.               | Cloud credentials, signing, policy inference, direct catalog tokens, private Axon runtime imports, direct Axon execution bypassing the gateway. |
| Daxis gateway         | Daxis API platform     | Authenticate the user, bind tenant/workspace context, enforce request limits, route descriptor/read-plan/fallback calls, attach request/correlation IDs, normalize service errors. | Delta log parsing, browser SQL execution, object signing implementation details.                                                                |
| Daxis catalog service | Daxis catalog platform | Resolve logical table identity, evaluate row filters, column masks, views, external-engine eligibility, table class, Delta feature policy, and access-mode decision.               | Browser execution claims beyond the Axon compatibility contract.                                                                                |
| Daxis storage broker  | Daxis storage platform | Issue object-scoped signed URLs or proxy URLs, implement object-grant list/head/batch-sign/range routes, enforce table-root scope, expiry, CORS, and URL redaction.                | User-facing SQL fallback decisions or browser result correctness.                                                                               |
| Daxis query service   | Daxis query platform   | Execute server fallback for `sql_fallback_required`, preserve fallback reason, correlate statement execution with catalog and storage decisions, return product telemetry.         | Silent rerouting that drops fallback or block reasons.                                                                                          |
| Axon SDK and worker   | Axon runtime team      | Validate resolver/read-plan envelopes, open descriptors, execute supported browser SQL, return Arrow IPC results, structured metrics, fallback/error taxonomy, and worker events.  | Daxis authentication, authorization, audit, signing, or policy authority.                                                                       |

M0 is complete only when Daxis architecture docs name Axon as the browser read engine, the descriptor resolver is the first integration contract, Daxis endpoint envelopes are fixture-backed, and no browser-visible artifact contains raw credentials or signing capability.

## Runtime Strategy

The browser DataFusion path should become the primary Axon runtime for Daxis. Legacy narrow runtime paths can remain as compatibility scaffolding while they prove useful, but new Daxis-facing capability work should target browser DataFusion execution unless there is a clear reason not to.

The runtime should preserve these principles:

- single-table read-only SQL first
- Arrow IPC over row JSON for result transport
- worker-first execution
- browser-safe HTTP range reads
- bounded memory and output budgets
- cancellation support
- structured runtime metrics
- native fallback reasons
- no browser credentials
- no hidden server calls from the default browser runtime

The browser runtime should fail closed on unsupported table features, unsupported schemas, unsupported SQL, invalid descriptors, expired access, and object-store protocol violations. It should never return best-effort results when correctness is uncertain.

## Query Flow

The ideal Daxis browser query flow is:

1. A Daxis query surface selects a logical table from Daxis catalog state.
2. The surface asks the Daxis query gateway for a browser read plan or descriptor.
3. Daxis authenticates and authorizes the request.
4. Daxis evaluates policy and table compatibility.
5. Daxis returns a browser-safe descriptor, read-access plan, fallback requirement, or block decision.
6. The Axon SDK opens the table in a worker.
7. The Axon worker performs snapshot or descriptor materialization as needed.
8. Axon registers the table with the browser DataFusion session.
9. The Daxis-approved request sends validated SQL through the Axon SDK.
10. Axon validates query scope and runtime budgets.
11. Axon executes the query in the worker.
12. Axon returns Arrow IPC bytes, preview metadata, runtime metrics, events, and fallback data.
13. Daxis records query-level product telemetry and correlates it with service-side read-access evidence.

The query path should include stable request IDs from Daxis and worker-side query IDs from Axon. Those identifiers should be present in logs, metrics, worker events, fallback results, and service audit records.

## Fallback And Error Semantics

Fallback is not an exceptional afterthought. It is part of the product contract.

Axon should return structured runtime failures for:

- unsupported SQL
- unsupported schemas
- unsupported Delta protocol features
- unknown protocol features
- range read failures
- invalid or expired descriptors
- signed URL expired
- browser budget exceeded
- cancellation
- network and object-store protocol failures
- worker initialization failures

Daxis should return structured access decisions for:

- policy denied
- policy unknown
- row filters
- column masks
- governed views
- missing external-engine support
- CORS blocked
- broker unavailable
- direct browser access unavailable
- signed URL unavailable
- proxy required
- warehouse/server fallback required

The Daxis gateway and product surface should preserve the reason internally even when the user-facing message is simpler. Product analytics should measure fallback frequency by reason, table type, tenant, browser, selected bundle, and query shape.

## Security And Governance

The most important security requirement is that browser code never receives production secrets. Browser code must never receive raw credentials.

Forbidden browser-visible values include:

- raw AWS credentials
- Azure account keys or broad SAS tokens
- GCP OAuth tokens, HMAC keys, or service-account JSON
- Databricks tokens
- personal access tokens
- refresh tokens
- object signing secrets
- client secrets

Allowed browser-visible values include:

- opaque grant IDs
- short-lived object-scoped signed URLs
- proxy URLs scoped to the grant and path
- expiry timestamps
- non-secret capability flags
- correlation IDs
- redacted identifiers

Every security-sensitive change should answer:

- Can browser code mint, extend, or broaden access?
- Can a descriptor escape its table-root scope?
- Can a signed URL or proxy URL be reused outside the intended object, tenant, or time window?
- Are URL secrets redacted in logs and UI events?
- Are browser package dependencies free of signing, credential, and cloud-admin SDKs?
- Does Daxis retain the audit trail needed to explain every object read?

## Observability

Axon should expose runtime facts. Daxis should own service and business telemetry.

Axon should emit:

- bytes fetched
- bytes reused from browser transport cache
- files touched
- files skipped
- row groups touched
- row groups skipped
- footer reads
- rows emitted
- query duration
- snapshot bootstrap duration
- selected bundle tier
- browser runtime SKU
- fallback reason
- worker progress events
- worker terminal errors
- Arrow IPC byte length
- session cache bytes
- table count

Daxis should add:

- tenant ID
- workspace ID
- authenticated user or service identity
- catalog table identity
- policy decision
- access mode decision
- signed URL or proxy issuance metadata
- grant ID
- request ID
- server-side correlation ID
- audit evidence for read access
- fallback routing target

The cross-system observability goal is to answer these questions without manual reconstruction:

- Why did this query run in the browser or on the server?
- Which table snapshot was read?
- Which objects were authorized for read access?
- Which browser/runtime bundle executed the query?
- How much data did the browser fetch?
- Why did a query fall back or fail?
- Did browser results match the supported native oracle for this query class?

## Testing And Quality Gates

Axon should continue to use a layered test strategy.

Required gates for Axon engine changes:

- Rust formatting with `cargo fmt --check`.
- Workspace compile checks.
- Host tests for query contracts, router, native runtime, control-plane descriptor support, browser runtime, browser DataFusion session, object store, Parquet engine, Delta snapshot, SDK, and worker.
- `wasm32-unknown-unknown` compile checks for browser crates.
- WASM smoke tests for browser crates.
- Worker artifact size budget.
- Browser dependency guardrails.
- Bundle secret-marker guardrails.
- Browser SDK tests.
- Browser matrix where the change touches real worker behavior.
- Native/browser conformance corpus updates for every newly supported SQL class.
- Daxis contract fixture tests for descriptor resolver and read-access plan shapes.

Daxis integration gates should include:

- descriptor resolver contract test
- signed URL expiry and object-scope test
- policy-denied and policy-unknown tests
- fallback-required tests
- CORS and range-read smoke for at least one production-shaped fixture
- query-surface open/query/cancel smoke through the Axon SDK
- request and correlation ID propagation test

No browser feature should be called supported until it has both runtime coverage and a compatibility statement in the docs.
For the Daxis browser query corpus, `bash tests/conformance/verify_daxis_query_corpus_coverage.sh` enforces that runtime cases, declared SQL classes, and browser DataFusion parity statements stay aligned.

The strategy document itself is checked by `bash tests/conformance/verify_daxis_strategy_document.sh` so the Daxis default-browser-engine boundary, linked release artifacts, and release-gate commands do not drift silently.

## Release And Compatibility Policy

Axon releases intended for Daxis should include a release evidence bundle:

- git SHA
- contract versions or schema hashes
- Rust test summary
- TypeScript test summary
- WASM target checks
- browser matrix result
- public GCS live-smoke output or skip-safe output plus blocker record
- worker artifact size
- dependency guardrail result
- known fallback reasons
- supported SQL and Delta feature matrix
- Daxis compatibility notes
- migration notes
- external blockers

The machine-readable bundle checklist is [`../release-gates/daxis-release-bundle-manifest.json`](../release-gates/daxis-release-bundle-manifest.json), checked by `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`. It distinguishes repo-verified artifacts from release-process attachments such as the exact commit SHA, worker artifact size output, public GCS live-smoke output or skip-safe blocker record, Daxis-facing release notes, and migration notes or a no-breaking-change statement. Its `releaseAttachmentSchema` names [`../release-gates/daxis-release-attachment-template.md`](../release-gates/daxis-release-attachment-template.md) and the metadata each release-process attachment must carry, including item ID, release commit, release ref, owner, capture time, artifact URI, verification command or statement, exit or review status, and rollback or migration-note URI. Release owners should complete [`../release-gates/daxis-release-notes-template.md`](../release-gates/daxis-release-notes-template.md) for release notes that document query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, runtime budgets, worker artifact changes, security-boundary impact, external proof packet status, and `stableDefaultPromotionGate` `currentPromotionState`. Release owners should complete [`../release-gates/daxis-release-migration-notes-template.md`](../release-gates/daxis-release-migration-notes-template.md) for the migration-note attachment so Daxis-facing breaking-change decisions, no-breaking statements, evidence, owner signoff, rollback impact, and stable-default promotion state are recorded consistently.

Recommended release channels:

- `experimental`: active development, no Daxis default routing
- `integration`: Daxis can run controlled environments and fixtures
- `candidate`: Daxis can enable tenant or workspace-gated rollout
- `stable`: Daxis can make this the default browser read path

Axon should avoid changing runtime semantics silently between channels. Changes to query results, fallback behavior, supported SQL, table feature handling, descriptor validation, or error taxonomy need explicit release notes.

## Development Operating Model

Axon development should follow a small set of professional practices:

- Use ADRs for architectural decisions.
- Use program docs for durable strategy and compatibility records.
- Use `docs/plans/` for implementation plans before large changes.
- Keep PRs scoped and reviewable.
- Require tests that match the risk and contract surface.
- Keep browser security checks in CI.
- Keep generated artifacts out of source unless the repo already treats them as checked-in outputs.
- Do not add broad dependencies to browser crates without a dependency review.
- Treat docs, tests, and release evidence as part of the change, not cleanup after the fact.

Every non-trivial PR should include:

- what contract changed, if any
- what compatibility claim changed, if any
- what fallback behavior changed, if any
- what tests prove it
- whether WASM size, startup, memory, or browser compatibility changed
- whether Daxis integration needs a coordinated update

## Roadmap

The milestone deliverables and exit criteria below are mapped to current evidence in [`../release-gates/daxis-strategy-traceability.json`](../release-gates/daxis-strategy-traceability.json), checked by `bash tests/conformance/verify_daxis_strategy_traceability.sh`. The matrix distinguishes Axon repo-verified work from Daxis production work that still requires external proof.

The external proof handoff is recorded in [`daxis-external-proof-handoff.md`](./daxis-external-proof-handoff.md) and [`../release-gates/daxis-external-proof-packet.json`](../release-gates/daxis-external-proof-packet.json), checked by `bash tests/conformance/verify_daxis_external_proof_packet.sh`. It names the Daxis-owned production artifacts required before stable default routing and carries the `stableDefaultPromotionGate` and current `currentPromotionState` value, `blocked_external_proof_required`, that tie accepted external proof to accepted release-process attachments, full release-evidence output, the external blocker register, and `server_fallback` rollback evidence.

### M0: Daxis Alignment

Goal: make the product architecture explicit.

Deliverables:

- Update Daxis docs and `daxis-query` terminology from DuckDB-WASM or browser-local execution to Axon browser WASM.
- Decide the first Daxis integration contract: server-resolved descriptor first.
- Define Daxis endpoint names and request/response envelopes.
- Add a Daxis/Axon compatibility fixture.
- Add an integration plan that identifies the query-surface, gateway, query, catalog, and storage responsibilities.

Exit criteria:

- Daxis architecture names Axon as the default browser read engine.
- Axon docs point to the Daxis handoff path.
- No implementation relies on browser cloud credentials.

### M1: Server-Resolved Descriptor Integration

Goal: allow Daxis to open governed Delta tables in Axon through trusted descriptors.

Deliverables:

- Daxis descriptor resolver endpoint.
- Signed URL or proxy URL attachment for active files.
- Axon SDK example for Daxis descriptor resolution.
- Frontend open/query flow in Daxis.
- Contract tests for expiry, snapshot version, path coverage, and invalid descriptor handling.

Exit criteria:

- Daxis can query a controlled Delta table through Axon in the browser.
- Server fallback remains available and reason-preserving.
- Query metrics and correlation IDs survive the UI, SDK, worker, and service boundary.

### M2: Brokered Object Grants

Goal: support richer governed reads without forcing every table through fully materialized server descriptors.

Deliverables:

- Daxis read-access plan endpoint.
- Object grant routes for list, head, batch-sign, and range.
- Axon adapter and grant-route client helpers for brokered Delta read plans.
- Grant expiry and fallback behavior.
- Storage CORS and proxy-mode validation.

Exit criteria:

- Daxis can choose descriptor, grant, proxy, Delta Sharing, server fallback, or blocked states per table.
- Axon consumes the plan without owning Daxis policy.
- Audit evidence ties object access back to user, workspace, table, grant, and query.

### M3: Browser DataFusion As Primary Runtime

Goal: make browser DataFusion the production runtime target for Daxis.

Deliverables:

- DataFusion-backed SQL execution as the default app/runtime path.
- Broader conformance corpus for Daxis query shapes, starting with [`../../tests/conformance/daxis-browser-datafusion-query-corpus.json`](../../tests/conformance/daxis-browser-datafusion-query-corpus.json).
- Runtime budgets tuned for product use and recorded in [`../release-gates/daxis-browser-datafusion-budget-profile.json`](../release-gates/daxis-browser-datafusion-budget-profile.json).
- Clear removal or isolation plan for legacy narrow runtime paths, recorded in [`../release-gates/daxis-browser-runtime-isolation-plan.json`](../release-gates/daxis-browser-runtime-isolation-plan.json).
- Browser matrix coverage for Daxis-critical flows, including the descriptor resolver open/query path in [`../../apps/axon-web/tests/browser-worker-matrix.spec.ts`](../../apps/axon-web/tests/browser-worker-matrix.spec.ts).

Exit criteria:

- Supported Daxis exploratory queries run through browser DataFusion by default.
- Unsupported queries fall back deterministically.
- Runtime size, cold start, and memory are within documented budgets.

### M4: Operational Maturity

Goal: make Axon safe to operate as default read compute.

Deliverables:

- Daxis dashboards for browser execution, fallback, failures, and data fetched.
- Oncall runbooks for resolver failure, object grant failure, CORS failure, worker startup failure, and elevated fallback rates.
- Release evidence automation.
- Tenant/workspace rollout controls.
- Compatibility dashboard for current Daxis production tables.

The repo-owned M4 contract is recorded in [`daxis-operational-maturity.md`](./daxis-operational-maturity.md) and [`../release-gates/daxis-operational-readiness.json`](../release-gates/daxis-operational-readiness.json). It defines the dashboard, runbook, rollout, compatibility-dashboard, external-proof, and release-automation requirements Daxis must implement before stable default routing. It is checked by `bash tests/conformance/verify_daxis_operational_readiness.sh`, and the release evidence runner is `bash tests/conformance/verify_daxis_release_evidence.sh`.

Exit criteria:

- Daxis can roll out Axon by workspace, tenant, or table class.
- Operators can explain and debug read behavior from logs and dashboards.
- Axon releases have repeatable go/no-go evidence.

## Success Metrics

Engineering success:

- Daxis integrates through versioned contracts.
- Browser runtime changes do not require Daxis developers to inspect private Axon internals.
- Every supported query class has native/browser parity evidence.
- CI catches browser dependency, WASM, and contract drift before release.
- Fallback and block reasons are structured and actionable.

Product success:

- Interactive read queries use browser execution by default where policy allows.
- Server compute is avoided for common exploratory workloads.
- Users see stable query behavior and bounded errors.
- Daxis can explain why each query used browser execution, server fallback, or policy block.

Operational success:

- Axon release evidence is sufficient for rollout decisions.
- Daxis telemetry correlates query execution with table access and object reads.
- Elevated fallback rates, CORS failures, expired descriptors, and worker failures are observable.
- Rollbacks can return Daxis to server execution without corrupting user state.

## Rollout Decision Register

The first Daxis production rollout decisions are recorded in [`../release-gates/daxis-production-rollout-decisions.json`](../release-gates/daxis-production-rollout-decisions.json) and checked by `bash tests/conformance/verify_daxis_rollout_decisions.sh`.

Resolved Axon-side decisions:

- Descriptor resolver endpoint: `POST /v1/query/delta/snapshot-descriptor`.
- Read-access-plan endpoint: `POST /v1/catalog/read-access-plan`.
- Object grant routes: grant-scoped `list`, `head`, `batch-sign`, and `range`.
- Initial access mode: signed URL descriptors for controlled tables, proxy when Daxis policy or CORS requires it, and explicit fallback/block states when browser access is not allowed.
- Descriptor refresh: same snapshot required for the existing refresh path.
- Initial browser budgets: Daxis M3 browser DataFusion budget profile.
- Contract versioning: Rust types plus TypeScript examples, JSON Schema, OpenAPI, and SHA-256 artifact manifest.
- Release channels: `experimental`, `integration`, `candidate`, and `stable`.
- Initial table eligibility: read-only Delta/Parquet tables without row filters, column masks, governed views, unknown policy state, unsupported Delta features, or unsupported SQL shapes.

External signoffs still required before production default routing:

- Daxis security approval for signed URL TTL policy.
- Daxis SRE approval for production dashboard thresholds, alert routing, oncall ownership, rollout controls, and current production-table compatibility inventory.
- Daxis owner attachment of the proof artifacts listed in the external proof packet.

## PR Checklist For Axon Developers

Before merging a Daxis-relevant Axon change, verify the checked-in pull request template at [`../../.github/pull_request_template.md`](../../.github/pull_request_template.md). The Daxis checklist is enforced by `bash tests/conformance/verify_daxis_pr_checklist.sh`.

- The Daxis/Axon trust boundary is unchanged or explicitly documented.
- Browser code does not receive raw credentials or signing capability.
- Public contract changes are additive or have a migration path.
- TypeScript and Rust contract shapes stay aligned.
- Unsupported behavior returns a structured error, fallback, or block reason.
- Native/browser parity tests cover any new supported query behavior.
- Browser dependency guardrails still pass.
- WASM target checks still pass.
- Worker artifact size impact is known.
- Browser matrix coverage is updated when worker behavior changes.
- Release-process evidence uses `docs/release-gates/daxis-release-attachment-template.md` for git SHA, worker size, public GCS live smoke, release notes, and migration notes.
- Daxis-facing release notes use `docs/release-gates/daxis-release-notes-template.md` for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.
- Daxis-facing migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for breaking changes or explicit no-breaking-change statements.
- Daxis-owned production proof uses `docs/release-gates/daxis-external-proof-attachment-template.md` before stable default routing.
- Rollout and fallback changes preserve `sql_fallback_required`, `server_fallback`, and `blocked` states.
- Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.
- Stable default routing is gated on `docs/release-gates/daxis-external-proof-packet.json` `stableDefaultPromotionGate` acceptance.
- Relevant docs and release evidence are updated.

## Handoff Summary

Axon developers should optimize for a production-grade engine boundary:

- Keep Daxis policy and access control in Daxis.
- Keep browser execution and runtime correctness in Axon.
- Make contracts stable and explicit.
- Make fallback deterministic.
- Make security boundaries testable.
- Make release evidence repeatable.
- Make Daxis integration a first-class acceptance criterion, not a downstream afterthought.

This is the path that lets Daxis use Axon as default read compute without turning the browser runtime into a trusted control plane.
