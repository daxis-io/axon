# Daxis Operational Maturity Contract

- Date: 2026-05-30
- Scope: operational readiness contract for using Axon as Daxis default browser read compute
- Owner: Runtime / engine team with Daxis SRE, storage, catalog, gateway, query, and web platform owners
- Related:
  - [Daxis first-class integration strategy](./daxis-first-class-integration-strategy.md)
  - [Daxis external proof handoff](./daxis-external-proof-handoff.md)
  - [Browser observability contract](./browser-observability-contract.md)
  - [Browser release integration runbook](./browser-release-integration-runbook.md)
  - [Daxis operational readiness gate](../release-gates/daxis-operational-readiness.json)
  - [Daxis external proof packet](../release-gates/daxis-external-proof-packet.json)

This document defines the operational evidence Daxis needs before Axon can be treated as stable default browser read compute. It is a contract and template for Daxis production systems. It does not claim that this repository contains live dashboards, alert routing, rollout configuration services, oncall schedules, or production table inventories.

## Repo-Owned Inputs

Axon owns the browser-runtime facts Daxis needs to operate the integration:

- `executed_on`, runtime SKU, worker artifact identity, selected bundle tier, and platform feature probes
- query duration, snapshot bootstrap duration, Arrow IPC byte length, row count, and cancellation events
- bytes fetched, bytes reused, files touched/skipped, row groups touched/skipped, footer reads, and rows emitted
- fallback reason, terminal error code, unsupported feature category, and worker terminal error events
- descriptor, read-access-plan, object-grant, audit-event, and OpenAPI fixtures
- release evidence commands for query contracts, Browser SDK coverage, DataFusion runtime coverage, browser matrix coverage, Daxis default-worker dependency guardrails, skip-safe public GCS live smoke, dependency boundaries, and contract artifact hashes

Daxis owns the production context around those facts:

- tenant, workspace, user, catalog, table, and policy identities
- access-mode decision, grant issuance, signed URL or proxy issuance, and audit pipeline state
- rollout segment, release channel, runtime SKU assignment, and kill-switch state
- dashboard, alert, oncall, and incident-response ownership

## Dashboard Contracts

Daxis should publish four dashboards before widening beyond controlled integration environments.

`browser_execution` shows whether Daxis routed a query to Axon browser execution or server fallback. Break it down by tenant, workspace, table class, runtime SKU, release channel, and browser family. It should include query duration, bootstrap duration, Arrow IPC size, rows returned, worker artifact identity, and selected bundle tier.

`fallback_and_block_reasons` preserves the reason Daxis did not run in the browser. It should separate policy block, policy unknown, unsupported Delta feature, unsupported SQL, object access failure, CORS failure, worker startup failure, and budget violation. Silent server rerouting is not acceptable evidence.

`data_fetch_and_object_access` correlates browser scan metrics with Daxis storage broker evidence. It should show bytes fetched, bytes reused, files touched/skipped, row groups touched/skipped, object-grant action, object-grant outcome, access mode, grant ID, request ID, correlation ID, and Axon query ID.

`worker_health` tracks browser runtime startup and terminal behavior. It should include worker startup latency, memory baseline, artifact size, Brotli size for the DataFusion default worker, terminal errors, cancellation counts, browser family, bundle tier, and release channel.

## Oncall Runbooks

Daxis production runbooks should cover these incident classes:

- `resolver_failure`: descriptor endpoint unavailable, malformed, missing correlation, or resolving the wrong snapshot
- `object_grant_failure`: grant expiry, denied route capability, malformed route payload, object-scope failure, or missing audit identity
- `cors_failure`: production XML endpoint headers, exposed range headers, preflight drift, proxy-mode requirement, or redaction failure
- `worker_startup_failure`: artifact drift, unsupported browser feature probe, bundle selection failure, WASM startup regression, or browser matrix regression
- `elevated_fallback_rates`: fallback spikes by tenant, workspace, table class, query shape, runtime SKU, or Delta feature

Each runbook needs an owner, symptom list, first checks, rollback action, and escalation path. The Daxis-owned rollback path must be able to force `server_fallback` without shipping a new Axon release.

## Rollout Controls

Daxis should control rollout by at least these dimensions:

- tenant ID
- workspace ID
- table class
- runtime SKU
- release channel
- browser family

The required states are `disabled`, `server_fallback`, `descriptor_only`, `brokered_grants`, `browser_datafusion`, and `stable_default`. The kill switch must be segment-aware and reason-preserving: changing a tenant or workspace back to server fallback should preserve why the browser path was disabled.

## Compatibility Dashboard

The compatibility dashboard should inventory current production Daxis tables against Axon browser support. Minimum fields:

- tenant, workspace, catalog, schema, table, and table class
- Delta protocol and table feature set
- policy decision and access-mode decision
- browser compatibility status
- fallback reason or block reason
- last Axon release tested
- last Daxis query corpus result
- last verification time

The dashboard should distinguish production-candidate tables from tables blocked by policy, server-only governance, proxy-only access, unknown Delta features, or unsupported SQL/table features.

Minimum machine-readable segments:

- `production_candidate_tables`
- `tables_blocked_by_policy`
- `tables_requiring_server_fallback`
- `tables_requiring_proxy_access`
- `tables_with_unknown_delta_features`

## Stable Default Gate

Daxis should not treat Axon as stable default read compute until these are true:

- Daxis dashboards exist with alert ownership.
- Daxis oncall runbooks exist for the five incident classes above.
- Tenant, workspace, table-class, runtime-SKU, and release-channel rollout controls exist.
- The production compatibility dashboard is populated for the intended rollout segment.
- Axon release evidence is repeatable and includes query-contract, Browser SDK, DataFusion runtime, browser matrix, Daxis default-worker dependency-boundary, skip-safe public GCS live-smoke, contract-artifact, and operational-readiness gates.

The machine-readable gate is
[`docs/release-gates/daxis-operational-readiness.json`](../release-gates/daxis-operational-readiness.json),
checked by `bash tests/conformance/verify_daxis_operational_readiness.sh`.
The repo-owned release evidence runner is
`bash tests/conformance/verify_daxis_release_evidence.sh`; use `--list` to
print the gate set without executing it.

Stable default blockers that remain outside this repository:

- Production dashboard URLs and alert ownership are absent from this repository.
- Production oncall schedules and service incident playbooks are absent from this repository.
- Tenant and workspace rollout controls are Daxis service work outside this repository.
- Production table compatibility inventory is Daxis catalog and QA work outside this repository.
- External proof artifacts listed in docs/release-gates/daxis-external-proof-packet.json are Daxis-owned and absent from this repository.
