# ADR-0010: Pluggable Catalog Providers And Separate Access And Execution Seams

- Status: Accepted
- Date: 2026-06-21
- Revised: 2026-07-15
- Decision owner: Frontend / workbench team
- Delivery DRI: Frontend / workbench team
- Approver: Query platform lead

## Decision

Axon uses three adjacent seams. Each seam has one responsibility and one owner
for a given source profile.

- `CatalogProvider` discovers catalogs, schemas, tables, and the volume roots
  needed by E8. It returns metadata and canonical resource references. It does
  not resolve byte access, mint capabilities, or execute queries.
- `DataAccessResolver` decides how the current principal and session may use one
  resource now. It returns a resolved browser read, requires remote execution,
  denies the request, or returns a typed operational error.
- `ExecutionProvider` runs a query in one browser, native, or remote runtime. It
  does not rediscover the selected resource or choose a fallback source.

A `SourceProfile` composes these core seams for one connection and may also bind
identity/session, early authorization decisions, and an optional filesystem
seam. It is a configuration record rather than another provider interface. The
profile names the adapter for each seam and its enforcement owner. Different
adapters may share an implementation, but shared code does not merge their
authority.

## Canonical resource binding

Discovery produces one `CanonicalResourceRef` with:

- `connection_id`, the stable identity of the selected connection;
- `provider_namespace`, a stable, versioned name for the identity authority and
  canonicalizer;
- `kind`, a closed resource-kind enum; and
- exactly one non-empty identity arm: `provider_object_id` or
  `canonical_locator`.

The pre-adoption protobuf correction uses this shape; field names may change only
with the same semantics:

```proto
message CanonicalResourceRef {
  string connection_id = 1;
  string provider_namespace = 2;
  ResourceKind kind = 3;
  oneof identity {
    string provider_object_id = 4;
    string canonical_locator = 5;
  }
}
```

`provider_namespace` is a registered implementation-owned identifier with an
identity-version suffix, not an endpoint or display label. Axon-owned adapters
use repo-owned constants; host adapters use a collision-resistant vendor
namespace. Boundary validation rejects an empty namespace or an unspecified
kind.

A provider object ID is opaque and is meaningful only inside the
`(connection_id, provider_namespace, kind)` scope. A canonical locator is allowed
only when the provider has no stable object ID. It must be capability-free and be
the output of the deterministic, versioned canonicalizer named by
`provider_namespace`; signed URLs, grants, credentials, display paths, and raw
user input are never identity. Each locator-based provider publishes conformance
fixtures for normalization and rejection. A canonicalization change creates a
new namespace version rather than silently changing equality.

Resource equality, cache keys, binding checks, and audit joins compare the exact
canonical tuple after adapter validation. Both identity arms, neither arm, an
empty component, a kind mismatch, or a connection/namespace mismatch is invalid.
Display names and other presentation metadata are outside the identity.

Access resolution binds its result to that reference. Browser execution accepts
one `ResolvedBrowserRead` plus the query specification. The resolved binding
contains one openable descriptor, the access class, the conditionally required
`not_after` limit, and correlation and provenance fields. Remote execution accepts the logical resource
reference plus the query specification so the remote enforcement point resolves
access. An execution request never carries both forms, and SQL does not carry an
independent table URI or snapshot that can override the binding.

The target access result has one authoritative algebra:

- `browser_read`: a complete, openable `ResolvedBrowserRead`;
- `remote_required`: the selected resource must run at the named enforcement
  owner, with a structured reason;
- `denied`: policy refuses the action, with a structured reason; or
- a typed operational error such as session expiry, unavailability, malformed
  provider data, or capability expiry.

There are no nested fallback or blocked variants. A brokered plan is an internal
resolver input until materialization finishes; it is not an execution-ready
outcome.

## Authority and lifecycle

The component that controls the resource enforces authorization. Browser policy
may reject known-invalid actions and drive UI state. A remote catalog, broker, or
execution service repeats and owns the authoritative decision for remote
resources. The same owner defines audit records and durable governed-cache
policy.

Catalog metadata may be cached by connection and provider revision. A resolved
browser read is an execution-local value: resolution passes it directly to one
admission attempt, and it is discarded after rejection or the accepted
execution's terminal state. It is never reused for a new execution or stored in
a general memory cache. A signed, proxy, grant-backed, or otherwise expiring
access class requires `not_after`, set to the earliest session, grant, descriptor,
or object-URL expiry. If the resolver cannot establish that bound, it must not
return `browser_read`. Signed URLs, grants, proxy capabilities, and resolved
bindings are never persisted. Local and public bytes may use a durable content
cache keyed by canonical resource identity, strong validator, and size. Governed
bytes remain memory-only until the enforcement owner supplies a principal/session
namespace and logout and revocation invalidation.

## Execution lifecycle

The initiator creates one opaque `execution_id` before admission and sends it with
the immutable canonical resource, query, deadline, and budgets. The executor
atomically admits or rejects that ID. Repeating an identical admission returns
the recorded admission state and never starts duplicate work; reusing the ID with
different immutable input is invalid.

Resolution, admission, and accepted execution use disjoint results:

- resolution returns `browser_read`, `remote_required`, `denied`, or
  `resolution_error`;
- admission returns `accepted(execution_id)` or
  `rejected(execution_id, reason)` and creates no event stream when rejected;
- an accepted execution records one authoritative terminal state: `completed`,
  `failed`, or `cancelled`.

Admission rejection reasons include invalid or mismatched input, access denied,
expired access, unsupported work, unavailable dependencies, an elapsed deadline,
and cancellation before acceptance. Deadline exhaustion after acceptance is a
typed `failed` code. The first successful atomic transition from running to a
terminal state wins completion, cancellation, and deadline races; later signals
are ignored and recorded as invariant violations.

Cancellation is idempotent and keyed by `execution_id`. Cancellation that wins
before admission leaves a bounded tombstone through the request deadline so a
late or repeated admission cannot start work. Cancellation after acceptance
requests termination; cancellation after a terminal state returns the recorded
state.

The authoritative executor records exactly one terminal state. A live stream
delivers at most one corresponding terminal frame and none after it. A local
in-process caller is expected to observe that frame; a disconnected remote caller
may observe none and must report an unknown observation rather than infer failure
or replay the query. Status lookup or resumable delivery requires a later,
explicit contract; this ADR makes no exactly-once delivery claim.

The provider receives a deadline and any enforceable budget before acceptance.
Admission may be retried only with the same ID and identical immutable input. An
executor does not replay accepted work unless a later contract defines an
idempotent resume protocol. The initial browser path returns one byte-budgeted
Arrow IPC buffer; collection and encoding stop at that budget plus documented
fixed overhead. Chunked browser delivery is deferred until the protocol has
explicit credit or acknowledgement and a bounded-buffer test. A streaming remote
provider must use transport flow control and bound buffered bytes.

## Current implementation at the revision date

The `axon/common/v1`, `axon/catalog/v1`, `axon/dataaccess/v1`, `axon/exec/v1`,
and `axon/fs/v1` protobuf packages are landed. The filesystem package is
messages-only contract substrate for roots, entries, directory listing, stat,
and read resolution. It does not mean that an E8 provider, adapter, UI, or
runtime consumer has landed. Production TypeScript does not yet consume these
packages through the three core seams or the optional filesystem seam.

The current data-access messages preserve nested fallback and blocked states,
and the current execution package combines a `QueryEngine` service descriptor
with browser-worker commands, cache diagnostics, and legacy JSON compatibility.
`axon.common.v1.ProviderCapabilities` also carries a generic `authority` field,
although discovery capability cannot identify the access-decision or enforcement
owner. The correction removes that unadopted field rather than expanding its
enum.
Axon treats that execution surface as an internal `BrowserWorker` compatibility
protocol. It is not the portable remote execution service. E9 may make one
pre-adoption correction to canonical resource identity, resource binding,
result states, capability lifetime, and execution lifecycle before the packages
become compatibility commitments.
After adoption, Buf `FILE` compatibility is the default.

Planned implementation paths such as
`apps/axon-web/src/query/providers/` remain planned until their E1 or E9 slice
lands. This ADR does not claim those adapters exist.

## Policy

- Remote metadata I/O uses one session-aware HTTP adapter. It sends same-origin
  session credentials, attaches a correlation ID, maps status codes to typed
  errors, and never builds an `Authorization` header from browser state.
- E1 remains table-first. Functions and models wait for a consumer. E1 may expose
  volume roots only to hand them to E8; E8 owns filesystem traversal and preview.
- Only a valid, unexpired `browser_read` reaches the browser runtime. Every other
  result fails closed before an engine session opens.
- Authority-crossing adapters validate required presence, resource identity,
  kind, variant choice, range semantics, descriptor version, expiry, and
  authority coherence. Wire permissiveness is not domain permissiveness.
- Provider dispatch lives in the seam registry. Explorer, editor, and results UI
  do not branch on source kind.
- Source selection never falls back to sample data, a prior source, or a default
  table after a resource-specific failure.

## Local development and tests

Envoy is a deployment adapter, not a requirement for local tests. A local Unity
Catalog instance may run behind the Vite same-origin proxy. Vendor wire types
remain generated from the vendored vendor specification and are adapted into
Axon's normalized discovery model at the boundary.

Tests exercise the seams rather than provider internals. A reference adapter and
one remote-shaped adapter establish that each seam is real. Negative matrices
cover mismatched resource identities, missing variants, expired access, range
errors, wrong-source fallback, cancellation races, repeated cancel, and events
after termination.

## Consequences

- Catalog UI stays independent of access grants and execution placement.
- Authorization, cache, and audit decisions use the same resource identity.
- Browser and remote executors implement their real authority model rather than
  accepting a union of every possible input.
- Axon pays for adapters only when a second implementation or a test substitute
  proves the seam.
- The landed draft contracts require a bounded correction before E9 adoption,
  but Axon avoids freezing browser-worker mechanics as a remote API.

## Acceptance Standard

This ADR is implemented when:

- every catalog source reaches explorer UI through a discovery-only
  `CatalogProvider`;
- discovery, access resolution, and execution share one canonical resource
  identity and reject mismatches before dispatch;
- browser execution accepts only one valid `ResolvedBrowserRead`, while remote
  execution accepts only one logical resource reference;
- capability-bearing values cannot enter durable browser storage or telemetry;
- execution and cancellation tests prove caller-created identity, idempotent
  admission, cancel-before-admit, first-terminal-transition wins, and at most one
  terminal stream frame;
- remote metadata uses the session-aware HTTP adapter with no browser-managed
  bearer token; and
- the E9 initial slices demonstrate local/public and session-proxied Unity
  Catalog paths; governed remote and volume-preview extensions apply the same
  contracts only when their independently gated consumers exist.

## References

- [ADR-0002: Browser access uses signed HTTPS or proxy, never cloud secrets](ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
- [E1: Pluggable catalog providers execution plan](../plans/2026-06-20-e1-catalog-providers-execution-plan.md)
- [E3A: Provider contract surfaces execution plan](../plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md)
- [E9: Execution provider vertical-slice plan](../plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md)
