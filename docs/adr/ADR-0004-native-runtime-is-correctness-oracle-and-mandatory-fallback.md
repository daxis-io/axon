# ADR-0004: Native Runtime Is The Correctness Oracle And Mandatory Fallback

- Status: Proposed
- Date: 2026-03-20
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

## Context

`deltalake_core` already supports GCS and DataFusion integration, and `DeltaTable::update_datafusion_session` explicitly prepares a DataFusion session by registering the table root object store in the runtime environment. That makes the native runtime the strongest candidate for the trusted-side reference path.

## Operational Policy

- every supported browser query must also run in native
- unsupported or untrusted browser cases must return a structured fallback reason
- native failures block browser releases when they invalidate parity

## Consequences

- the system keeps a correctness anchor while browser support matures upstream
- incident response has a deterministic recovery path
- performance claims remain grounded in a semantic baseline

## References

- [`deltalake_core` docs](https://docs.rs/deltalake-core)
