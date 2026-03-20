# ADR-0001: Adopt a Hybrid Query Architecture

- Status: Proposed
- Date: 2026-03-20
- Decision owner: Query platform lead
- Delivery DRI: Runtime / engine team
- Approver: Executive sponsor

## Decision

Adopt a three-tier architecture:

- trusted control plane
- browser WASM query plane
- native reference runtime

## Context

The current WASM ecosystem is promising but not yet mature enough to make browser execution the only truth-bearing runtime. DataFusion's upstream WASM work remains tracked in an open epic. Browser multi-partition execution remains open because current execution still hits browser runtime limitations. The community playground remains explicitly experimental. The current browser bindings are pinned to DataFusion 45 and expose HTTP/S3 OpenDAL features rather than a mature GCS-first browser path. Meanwhile, `delta-rs` already provides a stronger trusted-side path for GCS + DataFusion.

## Consequences

Positive:

- highest chance of shipping something production-grade
- secrets stay out of the browser
- native oracle exists from day one

Negative:

- more moving parts
- fallback routing must be explicit
- a service plane becomes mandatory

## Follow-Up

Create and enforce the boundaries for:

- `delta-control-plane`
- `native-query-runtime`
- `wasm-query-runtime`
- `wasm-http-object-store`
- `query-router`

## References

- [Apache DataFusion WASM epic](https://github.com/apache/datafusion/issues/13815)
- [`datafusion-wasm-bindings` Cargo.toml](https://raw.githubusercontent.com/datafusion-contrib/datafusion-wasm-bindings/main/Cargo.toml)
