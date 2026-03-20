# ADR-0002: Browser Access Uses Signed HTTPS Or A Narrow Proxy, Never Cloud Secrets

- Status: Proposed
- Date: 2026-03-20
- Decision owner: Security engineering
- Delivery DRI: Storage platform team
- Approver: Query platform lead

## Decision

The browser may access GCS-backed Delta data in only two sanctioned modes:

1. Signed HTTPS URL mode: the control plane resolves the snapshot and returns short-lived object-scoped signed URLs.
2. Narrow read-proxy mode: the browser hits a server endpoint that proxies range requests.

The browser must never receive:

- service-account JSON
- long-lived refresh tokens
- broad cloud tokens
- configuration that enables arbitrary bucket traversal

## Context

GCS signed URLs only work through XML API endpoints. XML and JSON endpoints behave differently for CORS. The authenticated browser download endpoint `storage.cloud.google.com` does not allow CORS. That makes a trusted signing or proxy layer the correct production pattern for browser reads.

## Policy

- short signed URL TTL
- object-scoped URLs
- auditable request correlation
- CORS validated against the exact XML endpoint style used in production
- per-table allow or deny hooks at the control plane

## Acceptance Standard

This ADR is implemented only when:

- bundle inspection proves no cloud secret reaches the browser
- CORS is validated for the exact endpoint shape used in production
- security review signs off on TTL, auditability, and origin policy

## References

- [GCS signed URLs documentation](https://docs.cloud.google.com/storage/docs/access-control/signed-urls)
