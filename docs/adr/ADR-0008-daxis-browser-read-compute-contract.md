# ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane

- Status: Proposed
- Date: 2026-05-30
- Revised: 2026-07-15
- Decision owner: Runtime / engine team and Daxis platform owners
- Delivery DRI: Runtime / engine team
- Approver: Query platform lead

## Decision

Axon is the browser read-compute engine for Daxis when a table, query, browser, and access mode fit the documented compatibility envelope. Daxis remains the trusted control plane and headless query gateway for identity, tenancy, intent normalization, semantic or text-to-SQL compilation, SQL validation, catalog policy, access grants, descriptor resolution, signed or proxied object access, audit, request correlation, rollout control, budget decisions, routing, normalized result envelopes, and server fallback.

The production integration starts with server-resolved browser reads. Daxis resolves a canonical logical table reference, evaluates policy, validates read-only SQL, pins a snapshot, attaches browser-safe signed or proxy object URLs, and returns one Daxis-approved resolved binding. The binding carries the resource reference, one openable descriptor, its access class and expiry, and audit correlation. The next integration layer adds brokered object grants for Daxis-owned list, head, batch-sign, and range routes, but the resolver must materialize those grants into the same binding before browser execution. Axon consumes the binding and executes supported read-only browser SQL; it does not infer Daxis policy, compile Daxis semantics, validate generated SQL, mint access, or select another source after failure.

The browser must never receive raw cloud credentials, signing secrets, direct catalog tokens, broad SAS tokens, OAuth refresh tokens, HMAC keys, service-account JSON, or signing capability. Unsupported policy state, unsupported SQL, unsupported Delta features, expired access, object-store protocol failures, and budget violations must preserve a structured reason and keep `server_fallback` available through Daxis-owned routing. Fallback changes the execution location for the same resource and query. It never substitutes sample data, a previous table, or another source.

Production proof remains external for Daxis service endpoints, signed URL policy approval, object scoping, production CORS, audit pipelines, dashboards, oncall runbooks, rollout controls, and production table compatibility inventory.

## Context

> **Forward note (ADR-0010, revised 2026-07-15).** [ADR-0010 "Pluggable Catalog
> Providers"](ADR-0010-pluggable-catalog-providers.md) separates discovery,
> access resolution, and execution. This ADR is the Daxis governed-host source
> profile. Daxis supplies discovery, owns access resolution and remote
> enforcement, and chooses browser or server execution. `CatalogProvider`
> remains discovery-only. Brokered grants are resolver inputs rather than
> execution-ready catalog outcomes. The control-plane authority, fail-closed
> reasons, and no-secrets boundary below remain binding.

The Daxis integration strategy needs a durable architectural decision, not only strategy text. The key risk is turning browser execution into an implicit control plane by letting runtime code accept secrets, infer authorization, silently reroute, or broaden table access. The safe architecture keeps Daxis authoritative for trust and production operations while making Axon authoritative for browser read execution within an explicit compatibility envelope.

The first integration contract is intentionally narrow: server-resolved descriptors are easier to audit than browser-side discovery because Daxis can make policy, snapshot, access-mode, and object-scope decisions before the browser worker opens the table. Brokered object grants widen the read path only after Daxis can prove grant-scoped routes, expiry, audit correlation, and fallback behavior.

## Gateway State Machine

Every Daxis query surface enters the same headless gateway state machine:

```text
received_intent
  -> compiled
  -> validated
  -> policy_checked
  -> descriptor_resolved
  -> routed
  -> rejected | browser_accepted | server_accepted

browser_accepted | server_accepted
  -> executing
  -> completed | failed | cancelled
```

Daxis owns every state through `routed` and owns the final normalized result
envelope. Rejection and the browser-versus-server route are pre-acceptance
decisions, not events in an accepted execution. Axon owns browser execution after
it receives a Daxis-approved descriptor, plus runtime-limit enforcement,
cancellation handling, Arrow IPC production, worker artifact identity, and
runtime error mapping.

Daxis creates one execution ID before admission and shares it across the route
decision, immutable request, event stream, cancellation, result envelope, and
audit record. Identical admission retries are idempotent; reusing the ID with
different input is invalid. Cancellation is idempotent, including before
admission. The accepted executor atomically records one completed, failed, or
cancelled state, and a live stream carries at most one corresponding terminal
frame. Daxis does not retry accepted work as a new run; it may route the same user
intent to server execution only as a new, correlated decision and execution ID.

`surface_kind` is trusted Daxis-derived metadata for metrics, rollout, rate limits, and budget tiers. It is not client authority and must not authorize table access unless Daxis policy explicitly derives that rule server-side.

## Policy

- Daxis service contracts must be consumed through public Axon SDK, query-contract, schema, OpenAPI, and fixture surfaces.
- Axon may receive only Daxis-validated read-only SQL plus Daxis-approved descriptors or grants.
- Axon must never receive raw prompts, builder plans, semantic plans, or unvalidated generated SQL.
- Browser-visible inputs may contain only browser-safe descriptors, opaque grant identifiers, short-lived object-scoped URLs, proxy URLs, expiry timestamps, non-secret capability facts, and correlation IDs.
- Daxis owns authoritative authorization and audit for Daxis resources. Browser
  policy supplies early rejection and UI behavior but cannot grant remote access.
- Axon must fail closed on invalid descriptors, path escape, snapshot mismatch, unsupported runtime features, expired access, and unsupported SQL.
- Signed URLs, grants, and resolved browser reads remain memory-only. Governed
  bytes remain memory-only unless Daxis supplies a principal/session cache
  namespace and logout and revocation invalidation.
- Daxis must preserve the fallback or block reason when routing to server execution.
- Release promotion requires repeatable Axon evidence plus Daxis-owned proof artifacts for production systems.

## Consequences

- Daxis teams get stable contracts without depending on private Axon runtime internals.
- Axon can evolve browser DataFusion and SDK behavior behind release gates while preserving the trust boundary.
- Stable default routing cannot be claimed from this repository alone because several production proofs are Daxis-owned.
- Breaking contract changes require migration notes, Daxis compatibility planning, and updated release evidence.

## Acceptance Standard

This ADR is implemented only when:

- the Daxis strategy traceability matrix maps repo-verified and external-required work explicitly
- the Daxis rollout decision register records endpoint names, access modes, release channels, budgets, contract versioning, and table eligibility
- the Daxis external proof packet covers every external-required traceability item
- the Daxis release evidence runner includes the contract, runtime, dependency, rollout, operational, traceability, external-proof, architecture ADR, and browser matrix gates
- browser dependency guardrails prove Axon browser packages do not depend on signing or service-account code
- lifecycle tests prove one execution ID, idempotent cancellation, and one
  terminal outcome
- persistence tests prove that signed URLs, grants, and resolved browser reads do
  not enter durable storage, URLs, logs, or telemetry

## References

- `docs/integrations/daxis/daxis-first-class-integration-strategy.md`
- `docs/release-gates/daxis-production-rollout-decisions.json`
- `docs/release-gates/daxis-strategy-traceability.json`
- `docs/release-gates/daxis-external-proof-packet.json`
- `docs/integrations/daxis/daxis-external-proof-handoff.md`
- `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- `docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md`
