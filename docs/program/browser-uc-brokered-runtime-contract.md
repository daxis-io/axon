# Browser Unity Catalog Brokered Runtime Contract

- Status: Draft
- Date: 2026-05-17
- Scope: document Axon's browser-owned Unity Catalog access-plan contract and the first shipped brokered Delta read path
- Related:
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)

## Contract Position

Axon is Contract First for this work. This repository owns the browser/runtime contracts that let a browser query engine consume governed table access, but it does not own the production Unity Catalog BFF or object-access broker.

The production broker is service-owned. It authenticates the user, talks to Unity Catalog, evaluates policy, vends object grants, signs or proxies object access, records audit evidence, and decides when browser execution is allowed. Axon owns the browser-safe shape of that decision: the `ReadAccessPlan` family, object-grant request/response contracts, descriptor/session handoff, query execution, and fallback/error vocabulary.

Catalog providers produce `ReadAccessPlan` values. They do not open Delta snapshots, register DataFusion tables, or execute user SQL. A provider answers "how may this browser access this governed table?" and then returns one of the contract variants:

- `brokered_delta` with object grants when the browser can reconstruct a snapshot through list/head/range access.
- `brokered_delta` or `delta_sharing` with browser-safe descriptor material, manifests, provider file lists, or URL-mode file actions when the browser should materialize the descriptor directly.
- explicit server snapshot resolution when an enterprise deployment chooses server-side descriptor production.
- `sql_fallback_required` when browser execution is not permitted or not supported for this table.
- `blocked` when policy denies the request.

Delta Sharing and UC OSS are compatible access-plan producers. They are not first-release competing gates and should not block shipment of the brokered Delta path when the brokered plan can satisfy the runtime contract.

## First Shipped Path

The first shipped repo path is:

```text
BrokeredDeltaReadPlan
  -> browser snapshot resolver or descriptor materializer
  -> BrowserHttpSnapshotDescriptor
  -> openDeltaTable()
  -> browser DataFusion query
```

The service-owned broker emits a `ReadAccessPlan::BrokeredDelta` / `BrokeredDeltaReadPlan` with:

- table identity fields: `tableId`, `fullName`, and `tableRoot`
- `grantId` and `expiresAtEpochMs`
- `deltaAccessMode` describing whether the browser may resolve Delta log state or should consume presigned files
- `policyAuthority`, including whether external-engine direct reads were confirmed
- `objectAccess` capabilities for `list`, `head`, `get`, `rangeGet`, `batchSign`, and `proxyRange`

The browser runtime consumes this plan by using the grant to obtain the existing Axon descriptors/session inputs, then registers the resulting table through the existing browser DataFusion execution path. Snapshot opening and table registration are runtime/session responsibilities after the provider has returned an access plan, not catalog-provider responsibilities.

If the plan contains object-grant reconstruction capability, the browser snapshot resolver reads Delta log state and active data through the grant. If the plan contains descriptor material, the descriptor materializer converts the supplied material directly to `BrowserHttpSnapshotDescriptor`; it must not list `_delta_log` just because the table is Delta. If the plan is `sql_fallback_required` or `blocked`, the browser must not call `openDeltaTable()`.

## Production Service Responsibilities

The production Unity Catalog BFF/object broker is outside this repo. It owns:

- OAuth and user/session authentication.
- Unity Catalog REST calls, policy lookup, and external-engine support checks.
- Credential vending decisions, with raw credentials kept server-side.
- Signing strategy, proxy strategy, or a mix of both per grant.
- SQL fallback routing when browser direct reads are unavailable or disallowed.
- Explicit server snapshot resolver operation for deployments that choose server-side descriptor production.
- Audit logging, SIEM emission, rate limits, SSRF controls, and workspace allowlists.
- Server-side encrypted token storage keyed only by an opaque `HttpOnly`, `Secure`, `SameSite` session cookie.
- Cloud-specific credential exchange and refresh.

Axon contracts should leave room for those service behaviors, but must not implement the production BFF in browser Rust, browser TypeScript, tests, or fixtures.

The production broker does not reconstruct snapshots or execute SQL by default.
Those are explicit server snapshot resolver and server fallback modes,
respectively.

## Governance Rules

Unknown policy state must fall back or block. The browser must not infer that direct reads are allowed from missing policy data, missing UC capabilities, missing object-grant capabilities, or a provider that cannot prove external-engine support.

Direct object reads are allowed only when both conditions are true:

- direct external-engine support for the table/provider has been confirmed
- a valid object grant exists for the specific table root, path scope, capabilities, and expiry

Catalog-managed Delta tables must fall back or block unless a later contract explicitly supports them. Support cannot be inferred from ordinary Delta protocol compatibility alone because catalog-managed tables may require service-side governance semantics that the browser runtime cannot prove locally.

Fallback and block reasons use the read-access taxonomy: `row_filter`, `column_mask`, `view`, `unknown_policy_state`, `no_direct_external_engine_read_support`, `unsupported_table_type`, `grant_expired`, `storage_cors_blocked`, and `broker_unavailable`.

## Browser Secret Boundary

Browser Rust and TypeScript must not receive, store, log, serialize, or test against raw production credentials. Forbidden browser-visible secrets include:

- raw AWS STS credentials
- broad Azure SAS tokens
- GCP OAuth tokens or HMAC keys
- Databricks tokens
- personal access tokens
- refresh tokens
- object-signing secrets

The browser may receive opaque grant IDs, short-lived object-scoped signed URLs, broker proxy responses, expiry timestamps, non-secret capability flags, and redacted identifiers. Any mock credential-like value used for test shape must stay out of production paths and must not normalize a raw-credential browser contract.

## Object Grant Routes

The object-grant API is the browser-facing broker contract for a `BrokeredDeltaReadPlan`. It exposes only grant-scoped operations:

| Route | Purpose |
| --- | --- |
| `POST /object-grants/{grantId}/list` | List object paths visible through the grant for a requested prefix. |
| `POST /object-grants/{grantId}/head` | Return metadata for one visible object path. |
| `POST /object-grants/{grantId}/batch-sign` | Return short-lived object-scoped bearer URLs for specific paths. |
| `GET /object-grants/{grantId}/range` | Proxy a byte range for one object path when proxy range access is required or signing is unavailable. |

The route set intentionally matches the runtime operations the browser object store needs: list, head, batch-sign, and range. Capability flags on `BrokeredObjectAccess` determine which route classes the browser may call for a grant.

Error and fallback mapping:

- Missing advertised list/head capability maps to a fallback/security-policy failure before making the broker call.
- Missing `rangeGet` maps to `range_read_unavailable` at runtime or `sql_fallback_required` with a read-access reason before runtime handoff.
- Missing `batchSign` may fall back to `proxyRange` when advertised.
- Missing `proxyRange` after signing is unavailable maps to `range_read_unavailable` at runtime or `sql_fallback_required` before runtime handoff.
- `401` means the grant is expired or unauthenticated.
- `403` means the grant does not authorize the requested path or byte range.
- `404` means the granted path does not exist or is not visible and maps to `object_not_found` for optional Delta log reads.
- `416` means the requested range is invalid for that object.

Signed URLs returned by `batch-sign` must be short-lived, object-scoped, and validated by the browser object-store URL policy before use.

## Mock Broker Boundary

The mock broker is a non-production fixture only. It may exercise route shape, capability fallback, redaction, URL validation, and session wiring, but it must not be presented as the production UC broker implementation.

Production deployments must replace the mock with a service-owned broker that enforces authentication, UC policy, grant issuance, signing/proxy decisions, audit, SIEM, rate limits, SSRF controls, workspace allowlists, and encrypted server-side token storage.
