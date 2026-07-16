# Browser Unity Catalog Brokered Runtime Contract

- Status: Draft
- Date: 2026-05-17
- Audit revision: 2026-07-15
- Scope: document Axon's browser-owned Unity Catalog access-plan contract and the first shipped brokered Delta read path
- Related:
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)

## Contract Position

Axon is Contract First for this work. This repository owns the browser/runtime contracts that let a browser query engine consume governed table access, but it does not own the production Unity Catalog BFF or object-access broker.

The production broker is service-owned. It authenticates the user, talks to Unity Catalog, evaluates policy, vends object grants, signs or proxies object access, records audit evidence, and decides when browser execution is allowed. Axon owns the browser-safe shape of that decision. The current compatibility surface includes the `ReadAccessPlan` family, object-grant request/response contracts, descriptor/session handoff, query execution, and fallback/error vocabulary.

`CatalogProvider` discovers the table and returns its logical reference. It never
produces a read plan. A governed `DataAccessResolver` sends that reference to the
broker and adapts the response. The current broker may return one of these
compatibility variants:

- `brokered_delta` with object grants when the browser can reconstruct a snapshot through list/head/range access.
- `brokered_delta` or `delta_sharing` with browser-safe descriptor material, manifests, provider file lists, or URL-mode file actions when the browser should materialize the descriptor directly.
- explicit server snapshot resolution when an enterprise deployment chooses server-side descriptor production.
- `sql_fallback_required` when browser execution is not permitted or not supported for this table.
- `blocked` when policy denies the request.

Delta Sharing and UC OSS may back resolver adapters. They are not catalog
providers with byte-access authority. Neither should block shipment of the
brokered Delta path when that path satisfies the runtime contract.

The target resolver does not expose nested plan states to execution. It
materializes a complete `ResolvedBrowserRead`, returns `remote_required`, returns
`denied`, or raises a typed operational error. `sql_fallback_required` and
`blocked` remain compatibility inputs that map to `remote_required` and `denied`
before execution acceptance.

## Current Repo State And Target Contract

The current SDK parses `ReadAccessPlan`, validates brokered and signed access
before opening, materializes `BrowserHttpSnapshotDescriptor`, and passes that
descriptor to `openDeltaTable()`. Expiry, correlation, selected-table identity,
and resolution provenance travel beside the descriptor in different response
shapes. The current worker handoff therefore cannot enforce those facts from one
input.

The target contract makes every successful browser read resolution produce one
`ResolvedBrowserRead` binding. It groups existing contract facts; it does not
add another provider seam. The binding contains:

- the `BrowserHttpSnapshotDescriptor`
- an access class: `local`, `public`, `signed`, or `proxy`
- mandatory `not_after` for signed, proxy, grant-backed, or otherwise expiring
  access, set to the earliest session, grant, descriptor, or object-URL expiry
- the exact selected source and table identity, plus the resolved snapshot
- resolution provenance, including provider/mode and policy authority when applicable
- an optional correlation ID for service logs and user-visible diagnostics

Immediately before browser execution, the execution path validates the binding
against the selected source and current time. An expired binding is discarded
and resolved again for the same selected source and snapshot intent. A missing
or mismatched source fails closed. It must not select the first connected table
or the sample fixture as a fallback.

The binding is passed to one admission attempt and disposed after rejection or
terminal execution. It is never reused by a later execution. If the resolver
cannot determine a finite bound for capability-bearing access, resolution fails
closed instead of returning `browser_read`.

The current SDK does not expose this unified binding yet. Until it does, callers
must treat the descriptor and its adjacent expiry/provenance metadata as one
execution-local value and apply the same checks before `openDeltaTable()`.

## First Shipped Path

The first shipped repo path is:

```text
BrokeredDeltaReadPlan
  -> browser snapshot resolver or descriptor materializer
  -> BrowserHttpSnapshotDescriptor
  -> openDeltaTable()
  -> browser DataFusion query
```

The target keeps `openDeltaTable()` as the worker handoff but tightens the call
site:

```text
ReadAccessPlan
  -> browser snapshot resolver or descriptor materializer
  -> ResolvedBrowserRead binding
  -> source/expiry validation
  -> openDeltaTable(binding.descriptor)
  -> browser DataFusion query
```

The service-owned broker emits a `ReadAccessPlan::BrokeredDelta` / `BrokeredDeltaReadPlan` with:

- table identity fields: `tableId`, `fullName`, and `tableRoot`
- `grantId` and `expiresAtEpochMs`
- `deltaAccessMode` describing whether the browser may resolve Delta log state or should consume presigned files
- `policyAuthority`, including whether external-engine direct reads were confirmed
- `objectAccess` capabilities for `list`, `head`, `get`, `rangeGet`, `batchSign`, and `proxyRange`

The browser runtime consumes this plan by using the grant to obtain the existing Axon descriptors/session inputs, then registers the resulting table through the existing browser DataFusion execution path. Snapshot opening and table registration are runtime/session responsibilities after the resolver has received and materialized the plan, not catalog-provider responsibilities.

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

Unity Catalog metadata is session-proxied metadata. The browser calls a
same-origin or allowlisted BFF with the opaque session cookie; it does not hold a
Unity Catalog token or call Unity Catalog with browser-managed provider
credentials. A deployment may perform browser execution after this metadata and
access-plan exchange, but that does not make UC authentication "direct" in the
browser.

Axon contracts should leave room for those service behaviors, but must not implement the production BFF in browser Rust, browser TypeScript, tests, or fixtures.

The production broker does not reconstruct snapshots or execute SQL by default.
Those are explicit server snapshot resolver and server fallback modes,
respectively.

## Governance Rules

Unknown policy state must return `remote_required`, `denied`, or a typed error.
The compatibility adapter may represent those decisions as fallback or blocked.
The browser must not infer that direct reads are allowed from missing policy
data, missing UC capabilities, missing object-grant capabilities, or a provider
that cannot prove external-engine support.

Browser-local policy checks support UX and early rejection only. They may hide
an unavailable execution target or reject an obviously expired plan, but they
do not authorize a remote resource. The broker or server evaluates remote
resource policy, issues the resulting capability, and owns the authoritative
audit record. Browser telemetry may carry the binding correlation ID; it is not
a substitute for the broker audit.

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

The browser must keep grants, signed URLs, resolved descriptors, and resolved
read bindings in memory. It must not write them to localStorage, IndexedDB,
OPFS, Cache Storage, persisted TanStack Query state, saved queries, crash
reports, or logs.

## Object Grant Routes

The object-grant API is the browser-facing broker contract for a `BrokeredDeltaReadPlan`. It exposes only grant-scoped operations:

| Route                                      | Purpose                                                                                               |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `POST /object-grants/{grantId}/list`       | List object paths visible through the grant for a requested prefix.                                   |
| `POST /object-grants/{grantId}/head`       | Return metadata for one visible object path.                                                          |
| `POST /object-grants/{grantId}/batch-sign` | Return short-lived object-scoped bearer URLs for specific paths.                                      |
| `GET /object-grants/{grantId}/range`       | Proxy a byte range for one object path when proxy range access is required or signing is unavailable. |

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

`401`, an expired grant, or an expired signed URL invalidates the entire resolved
binding. If execution has not started, a new attempt resolves access again for
the exact selected table and snapshot intent. If an accepted execution encounters
expiry, it terminates with one typed error; the runtime does not replay it. The
browser must not reuse cached bearer URLs or silently move to another source.

## Data Cache Policy

Access material and data bytes have separate retention rules:

- Local-file and anonymous public-object bytes may use durable caches when the
  cache key includes stable source identity and a strong object validator.
- Governed, signed, or grant-backed bytes are memory-only by default. Persisting
  those bytes can extend effective access beyond logout, expiry, or revocation.
- A deployment may opt governed bytes into persistent caching only when it
  namespaces entries by principal and session and guarantees logout and
  revocation invalidation. The deployment also owns the retention and audit
  policy for that cache.
- Signed query parameters and grant IDs are never cache identity. The browser
  still never persists the descriptor or capability that produced the bytes.

## Mock Broker Boundary

The mock broker is a non-production fixture only. It may exercise route shape, capability fallback, redaction, URL validation, and session wiring, but it must not be presented as the production UC broker implementation.

Production deployments must replace the mock with a service-owned broker that enforces authentication, UC policy, grant issuance, signing/proxy decisions, audit, SIEM, rate limits, SSRF controls, workspace allowlists, and encrypted server-side token storage.
