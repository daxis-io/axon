# Daxis First-Class Integration Examples

These fixtures are the Axon-side compatibility examples for the Daxis handoff described in `../daxis-first-class-integration-strategy.md`.

They are not a Daxis service implementation. They document the browser-safe envelopes that Daxis services can return after Daxis has authenticated the user, evaluated tenant/workspace policy, resolved catalog identity, and chosen browser execution, server fallback, or block behavior.

## Approved Axon Read Descriptor

- `approved-axon-read-descriptor.saved-query.json`: the first executable saved SQL slice. Daxis has validated single-table read-only SQL, resolved one browser-safe descriptor, attached access proof and runtime limits, and selected `axon_browser` before Axon receives the request.

## Result Envelopes

- `query-result.agent.executed.json`
- `query-result.dashboard-tile.executed.json`
- `query-result.builder.executed.json`
- `query-result.saved-query.executed.json`
- `query-result.api.executed.json`

These five fixtures prove that agent, dashboard tile, builder, saved-query, and API inputs return the same versioned result-envelope shape after they pass through the Daxis headless query gateway.

The browser-side example in `../../../apps/axon-web/examples/daxis-headless-query.ts` consumes the approved descriptor, executes it through the Axon SDK, and maps the worker result back into these envelopes.

- `query-result.policy-denied.rejected.json`: policy denial returns `status: rejected` and a structured `block_reason`.
- `query-result.unsupported-sql.fallback.json`: unsupported browser SQL returns `status: fallback` and a structured `fallback_reason`.
- `query-result.runtime-budget-overflow.fallback.json`: browser runtime budget overflow returns the same fallback envelope family.

## Descriptor Resolver

- `snapshot-descriptor-request.latest.json`: request body for `POST /v1/query/delta/snapshot-descriptor`.
- `snapshot-descriptor-request.version-42.json`: pinned snapshot request body used by version-consistency checks.
- `snapshot-descriptor-response.signed-url.json`: successful `DeltaLocationResolveResponse` with a pinned snapshot, object-scoped signed URLs, expiry, and correlation metadata.
- `snapshot-descriptor-response.expired.json`: fail-closed response fixture for descriptor expiry.
- `snapshot-descriptor-response.snapshot-version-mismatch.json`: fail-closed response fixture for a resolver answer that does not satisfy a pinned snapshot request.
- `snapshot-descriptor-response.path-escape.json`: fail-closed response fixture for object URLs or active file paths that escape the table root.
- `snapshot-descriptor-response.invalid-descriptor.json`: malformed descriptor fixture that must fail before worker handoff.

## Read Access Plan

- `read-access-plan.brokered-delta.json`: `brokered_delta` plan for browser snapshot reconstruction through object grants.
- `read-access-plan.delta-sharing.json`: `delta_sharing` plan that materializes provider file actions into the descriptor path.
- `read-access-plan.sql-fallback-required.json`: governed table result that must route to Daxis server SQL before worker handoff.
- `read-access-plan.blocked.json`: fail-closed result when Daxis cannot prove policy.

## Object Grants

- `object-grants.list-request.json` and `object-grants.list-response.json`: `POST /object-grants/{grantId}/list` payloads for paths visible inside the grant.
- `object-grants.head-request.json` and `object-grants.head-response.json`: `POST /object-grants/{grantId}/head` payloads for object metadata.
- `object-grants.batch-sign-request.json` and `object-grants.batch-sign-response.json`: `POST /object-grants/{grantId}/batch-sign` payloads for object-scoped signed URLs.
- `object-grants.range-request.json`: query payload shape represented by `GET /object-grants/{grantId}/range?path=...&start=...&end=...`.
- `object-grants.audit-event.range.json`: audit evidence shape tying one granted object range read to tenant, workspace, user, table, grant, request, correlation, and Axon query IDs.

The route envelopes and `ObjectGrantAuditEvent` schema are also captured in `../../../crates/query-contract/schemas/object-grants.openapi.json` for service-generation and contract checks.

The fixtures deliberately use opaque grant IDs, redacted URL signatures, expiry timestamps, and correlation IDs. They must not contain raw cloud credentials, UC tokens, Databricks tokens, refresh tokens, service-account JSON, signing keys, or broad SAS material. `bash tests/conformance/verify_daxis_contract_artifacts.sh` enforces this for the checked-in handoff artifact set.
