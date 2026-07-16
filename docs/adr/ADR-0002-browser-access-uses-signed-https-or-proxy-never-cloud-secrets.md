# ADR-0002: Browser Access Uses Signed HTTPS Or A Narrow Proxy, Never Cloud Secrets

- Status: Accepted
- Date: 2026-03-20
- Revised: 2026-07-15
- Decision owner: Security engineering
- Delivery DRI: Storage platform team
- Approver: Query platform lead

## Decision

Browser and WebAssembly code may read lakehouse bytes through four access classes:

1. local user-granted file access;
2. public HTTPS object access;
3. short-lived, object-scoped signed HTTPS access; or
4. a narrow server range proxy.

The browser must never receive service-account JSON, long-lived refresh tokens,
broad cloud tokens, catalog bearer tokens, signing keys, or configuration that
permits arbitrary bucket traversal.

A data-access resolver binds browser execution to one `ResolvedBrowserRead`. The
binding contains the canonical resource reference, one openable descriptor, the
access class, correlation and provenance fields, and `not_after` whenever access
is signed, proxied, grant-backed, or otherwise expiring. That value is the
earliest session, grant, descriptor, or object-URL expiry. A resolver that cannot
establish a finite bound for capability-bearing access must not return a browser
read. The binding
contains no alternate logical or physical source that the executor may choose.
Browser execution rejects an absent, expired, mismatched, or non-openable binding
before it opens a runtime session. It re-resolves an expiring binding rather than
using stale capability material.

Each resolved browser read is passed directly to one admission attempt and is
discarded after rejection or terminal execution. A new execution always resolves
a new binding. Signed URLs, opaque grants, proxy capabilities, and resolved
browser reads remain execution-local. Axon never stores them in IndexedDB, local storage, persisted query
caches, logs, URLs, or telemetry. A content cache may persist local or public
bytes under a canonical resource identity and strong validator. Governed or
signed bytes remain memory-only unless the enforcing host supplies a
principal-and-session namespace plus revocation and logout invalidation.

## Context

Object-store browser endpoints differ in CORS, range, authentication, and signed
URL behavior. For example, GCS signed URLs use XML API endpoints, while the
authenticated `storage.cloud.google.com` download endpoint does not support
CORS. A trusted signing or proxy layer therefore owns production access for
private GCS data. Equivalent provider constraints apply to other private object
stores.

An expiry check performed only during descriptor construction is insufficient.
Queries issue later metadata and range reads, so the execution boundary needs the
access class and lifetime. The same facts determine whether bytes may outlive a
session in a durable cache.

## Policy

- The enforcing control plane chooses object scope, origin policy, and signed URL
  lifetime and records the authoritative audit event.
- Browser policy checks may hide or disable actions and reject known-invalid
  requests. They do not authorize remote resources on behalf of the control
  plane.
- Signed access uses short lifetimes, exact object scope, and clock-skew-aware
  expiry checks.
- Proxy routes accept only the path and range operations required by the resolved
  resource.
- CORS proof covers the exact endpoint, origin, methods, range behavior, and
  exposed headers used in production.
- Errors fail closed and preserve a structured reason and correlation ID. The
  browser never converts an access failure into a public or sample-data read.

## Acceptance Standard

This ADR is implemented only when:

- bundle inspection proves no cloud or catalog secret reaches browser code;
- authority-crossing adapters reject missing, expired, mismatched, and malformed
  bindings before execution;
- capability-bearing bindings are rejected when `not_after` is absent or does not
  equal the earliest enforced expiry, while non-expiring local/public bindings
  may omit it;
- tests prove bindings are disposed after rejection or terminal execution and
  are not reused for a later execution;
- persistence tests prove that capability-bearing values cannot enter durable
  browser storage or telemetry;
- cache tests distinguish local/public content from governed/signed content and
  exercise logout and revocation invalidation where persistent governed caching
  is enabled;
- CORS is validated for the exact production endpoint shape; and
- security review signs off on lifetime, auditability, object scope, and origin
  policy.

## References

- [GCS signed URLs documentation](https://docs.cloud.google.com/storage/docs/access-control/signed-urls)
- [ADR-0010: Pluggable catalog providers and table read resolution seams](ADR-0010-pluggable-catalog-providers.md)
