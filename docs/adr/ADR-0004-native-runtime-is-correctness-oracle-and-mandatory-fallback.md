# ADR-0004: Native Runtime Is The Correctness Oracle And Mandatory Fallback

- Status: Proposed
- Date: 2026-03-20
- Revised: 2026-07-15
- Decision owner: Query platform lead
- Delivery DRI: Runtime / engine team
- Approver: Executive sponsor

## Decision

A native or server runtime is mandatory before browser production launch.

It serves as:

1. correctness oracle
2. execution fallback
3. incident-response safety net
4. upgrade canary

Fallback changes the execution location for the same canonical resource and
query. It does not grant access, select a different source, or reuse a browser
descriptor in a remote trust boundary. The remote enforcement point receives the
logical resource reference and resolves access again under its own authority.

## Context

`deltalake_core` already supports GCS and DataFusion integration, and `DeltaTable::update_datafusion_session` explicitly prepares a DataFusion session by registering the table root object store in the runtime environment. That makes the native runtime the strongest candidate for the trusted-side reference path.

## Operational Policy

- every supported browser query must also run in native
- unsupported or untrusted browser cases must return a structured fallback reason
- a fallback decision ends the browser attempt before runtime acceptance and
  starts a new, correlated native/server execution; an accepted execution is not
  retried automatically
- missing or invalid source selection fails and never falls back to sample data,
  a prior table, or a default connection
- native failures block browser releases when they invalidate parity

## Consequences

- the system keeps a correctness anchor while browser support matures upstream
- incident response has a deterministic recovery path
- performance claims remain grounded in a semantic baseline

## References

- [`deltalake_core` docs](https://docs.rs/deltalake-core)
