# EPIC-03: Trusted Control Plane And Browser-Safe GCS Access

- Type: Epic
- Accountable: Security engineering
- Delivery DRI: Storage platform team
- Depends on: EPIC-01, EPIC-02
- Milestone: `M2`

## Goal

Provide a trusted service that resolves Delta snapshots and returns browser-safe object access descriptors.

## Packages In Scope

- `crates/delta-control-plane`
- `services/query-api`
- `tests/security`
- `tests/conformance`

## Deliverables

- authenticated table-resolution API
- active-file descriptor generation
- signed HTTPS URL mode
- read-proxy mode
- audit logging
- signed URL policy configuration
- origin and CORS validation tests

## Current In-Repo Status

The repository currently implements only the thin in-repo library-owned contract slice across `crates/delta-control-plane` and `crates/query-contract`:

- deterministic snapshot resolution for latest and explicit historical versions
- metadata-only active-file descriptors
- browser HTTP snapshot/file descriptor types in `query-contract`
- deterministic attachment of caller-supplied per-file browser-safe HTTP URLs to resolved snapshot files
- exact-match per-table allow/deny hooks enforced before snapshot I/O

This in-repo slice deliberately stops at descriptor shaping and validation. It does not mint signed URLs, implement proxy reads, or expose an authenticated endpoint.

The following EPIC-03 work is still blocked on out-of-repo service delivery:

- `services/query-api`
- authenticated API surface
- signed URL issuance
- read-proxy mode
- audit logging and request correlation
- production-shape CORS/origin validation

## Child Issues

1. Define the table descriptor schema in `query-contract`.
2. Implement the snapshot resolution endpoint.
3. Implement signed URL generation policy.
4. Implement a range-aware read proxy.
5. Implement audit logging and request correlation.
6. Add signed URL expiry and renewal behavior.
7. Add CORS validation against the real endpoint shape.
8. Add negative tests proving the browser never receives service-account material.
9. Add per-table allow and deny hooks.

## Acceptance Criteria

- the control plane resolves a Delta snapshot and produces a deterministic descriptor
- signed URL mode supports browser range reads needed for Parquet metadata and footer access
- proxy mode supports the same descriptor contract
- security tests prove no long-lived secret reaches the browser
- audit logs correlate browser reads to user, session, and request id
- CORS is validated for the production endpoint shape, not just localhost

## Definition Of Done

Browser-safe object access is production-credible and security-reviewed.
