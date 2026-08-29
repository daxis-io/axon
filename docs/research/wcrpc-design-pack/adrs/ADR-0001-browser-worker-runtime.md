# ADR-0001: Use Browser Web Workers as the Parallel Runtime

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Native DataFusion parallel execution can rely on native threading/Tokio-style scheduling. In the browser Wasm target, pushing DataFusion's native multi-partition execution path into one Wasm instance can run into browser and Tokio reactor constraints. The existing worker-pool design already moves parallelism above a single Wasm DataFusion instance.

## Decision

Use browser Web Workers as the first-class parallel runtime. Each child worker owns an independent Wasm DataFusion session and executes shard-local work.

## Consequences

Positive:

- browser-native parallelism;
- no required `SharedArrayBuffer` in v0;
- easier isolation and cancellation;
- public SDK remains stable.

Negative:

- worker startup overhead;
- Wasm module duplication;
- message/transfer overhead;
- explicit merge logic required.
