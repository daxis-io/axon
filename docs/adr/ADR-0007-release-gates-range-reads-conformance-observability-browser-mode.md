# ADR-0007: Release Gates Prioritize Range Reads, Conformance, Observability, And Constrained Browser Mode

- Status: Proposed
- Date: 2026-03-20
- Decision owner: QA / performance engineering
- Delivery DRI: Runtime / engine team
- Approver: Query platform lead

## Decision

The hard release gates are:

1. Range-read correctness
2. Conformance
3. Observability
4. Constrained browser mode

## Gate Details

Range-read correctness must prove:

- footer reads
- bounded reads
- offset reads
- suffix reads
- `401`, `403`, `404`, and `416` handling

Conformance must prove:

- every supported browser query has a native comparison
- unsupported cases deterministically fall back or fail

Observability must emit:

- bytes fetched
- files touched
- files skipped
- footer fetch count
- cache hit ratio
- fallback reason
- signed URL vs proxy mode

Constrained browser mode must enforce:

- single-partition launch by default
- multi-partition treated as future work until browser runtime support is solved upstream

## Context

The visible browser object-store code uses `unsafe` Send/Sync erasure and the visible range branches stream `0..meta.size` even for bounded, offset, and suffix cases. Separately, the open multi-partition browser issue shows current browser execution failing because there is no Tokio reactor in the browser runtime. Parquet performance features depend on correct object-store behavior, so range-read proof becomes a release gate rather than a nice-to-have.

## Consequences

- browser launch remains intentionally constrained
- performance claims require evidence
- unsupported runtime conditions fail loudly instead of degrading silently

## References

- [`unsafe_opendal_store.rs` in `datafusion-wasm-bindings`](https://raw.githubusercontent.com/datafusion-contrib/datafusion-wasm-bindings/main/src/unsafe_opendal_store.rs)
- [Apache DataFusion browser multi-partition issue](https://github.com/apache/datafusion/issues/15599)
