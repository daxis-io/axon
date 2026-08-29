# ADR-0008: Run Hot Merge Logic in Wasm

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Coordinator-side TypeScript row loops would erase much of the benefit of columnar execution and risk semantic differences from DataFusion.

## Decision

Use TypeScript for orchestration, but run aggregate/grouped/top-k hot merge logic in Wasm over Arrow/DataFusion batches.

## Consequences

Positive:

- closer to native analytical execution;
- avoids JS row materialization;
- better type/null semantics alignment.

Negative:

- requires Wasm merge APIs;
- merge kernels need direct tests and oracle parity.
