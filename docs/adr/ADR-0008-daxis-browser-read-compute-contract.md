# ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane

- Status: Proposed
- Date: 2026-05-30
- Decision owner: Runtime / engine team and Daxis platform owners
- Delivery DRI: Runtime / engine team
- Approver: Query platform lead

## Decision

Axon is the browser read-compute engine for Daxis when a table, query, browser, and access mode fit the documented compatibility envelope. Daxis remains the trusted control plane and headless query gateway for identity, tenancy, intent normalization, semantic or text-to-SQL compilation, SQL validation, catalog policy, access grants, descriptor resolution, signed or proxied object access, audit, request correlation, rollout control, budget decisions, routing, normalized result envelopes, and server fallback.

The production integration starts with server-resolved descriptors. Daxis resolves a logical table, evaluates policy, validates read-only SQL, pins a snapshot, attaches browser-safe signed or proxy object URLs, and returns the Daxis-approved Axon read descriptor contract. The next integration layer adds brokered object grants for Daxis-owned list, head, batch-sign, and range routes. In both cases, Axon consumes public contracts and executes supported read-only browser SQL; it does not infer Daxis policy, compile Daxis semantics, validate generated SQL, or mint access.

The browser must never receive raw cloud credentials, signing secrets, direct catalog tokens, broad SAS tokens, OAuth refresh tokens, HMAC keys, service-account JSON, or signing capability. Unsupported policy state, unsupported SQL, unsupported Delta features, expired access, object-store protocol failures, and budget violations must preserve a structured reason and keep `server_fallback` available through Daxis-owned routing.

Production proof remains external for Daxis service endpoints, signed URL policy approval, object scoping, production CORS, audit pipelines, dashboards, oncall runbooks, rollout controls, and production table compatibility inventory.

## Context

> **Forward note (ADR-0010, 2026-06-21).** [ADR-0010 "Pluggable Catalog
> Providers"](ADR-0010-pluggable-catalog-providers.md) generalizes the read seam
> described here into a provider-agnostic `CatalogProvider` interface whose
> `resolveTableRead` returns a `TableReadResolution` union (`descriptor |
> read_access_plan | fallback | blocked`). This ADR is now the **Daxis / brokered
> profile** of that seam: the `read_access_plan` arm is the reserved Daxis
> outcome, and everything below (control-plane authority, fail-closed reasons,
> no-secrets boundary) is unchanged and remains binding for the Daxis provider.
> No rewrite is implied.

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
  -> executing
  -> executed | rejected | fallback | failed | cancelled
```

Daxis owns every state through `routed` and owns the final normalized result envelope. Axon owns browser execution after it receives a Daxis-approved descriptor, plus runtime-limit enforcement, cancellation handling, Arrow IPC production, worker artifact identity, and runtime error mapping.

`surface_kind` is trusted Daxis-derived metadata for metrics, rollout, rate limits, and budget tiers. It is not client authority and must not authorize table access unless Daxis policy explicitly derives that rule server-side.

## Policy

- Daxis service contracts must be consumed through public Axon SDK, query-contract, schema, OpenAPI, and fixture surfaces.
- Axon may receive only Daxis-validated read-only SQL plus Daxis-approved descriptors or grants.
- Axon must never receive raw prompts, builder plans, semantic plans, or unvalidated generated SQL.
- Browser-visible inputs may contain only browser-safe descriptors, opaque grant identifiers, short-lived object-scoped URLs, proxy URLs, expiry timestamps, non-secret capability facts, and correlation IDs.
- Axon must fail closed on invalid descriptors, path escape, snapshot mismatch, unsupported runtime features, expired access, and unsupported SQL.
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

## References

- `docs/integrations/daxis/daxis-first-class-integration-strategy.md`
- `docs/release-gates/daxis-production-rollout-decisions.json`
- `docs/release-gates/daxis-strategy-traceability.json`
- `docs/release-gates/daxis-external-proof-packet.json`
- `docs/integrations/daxis/daxis-external-proof-handoff.md`
- `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- `docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md`
