# ADR-0003: Separate Browser WASM Runtime From Hosted WASI UDF Runtime

- Status: Proposed
- Date: 2026-03-20
- Decision owner: Query platform lead
- Delivery DRI: Runtime / engine team
- Approver: Executive sponsor

## Decision

Maintain two distinct WASM tracks.

Track A: Browser query runtime

- target: `wasm32-unknown-unknown`
- purpose: browser-embedded DataFusion execution
- public interface: SQL-first JS/TS API
- IO model: browser-safe HTTP only
- no UDF ABI guarantee

Track B: Hosted UDF runtime

- target: WASI Preview 2 / Component Model
- purpose: isolated server-side UDF execution
- public interface: versioned WIT ABI
- IO model: host-mediated capabilities only

## Context

The current `datafusion-udf-wasm` guidance recommends WASI Preview 2 because it aligns with the future-proof Component Model and WIT. That is the right shape for hosted plugins, but it is not the same execution target or ABI surface as browser-embedded DataFusion.

## Naming Rule

- browser packages use `wasm-query-*`
- hosted UDF packages use `udf-*`

## Consequences

- browser runtime decisions remain optimized for browser safety, startup, and bundle size
- hosted UDF decisions remain optimized for versioned extension contracts and host capability boundaries
- accidental coupling between browser execution and server-side plugin evolution is prevented

## References

- [`datafusion-udf-wasm` WASM guidance](https://github.com/influxdata/datafusion-udf-wasm/blob/main/WASM.md)
