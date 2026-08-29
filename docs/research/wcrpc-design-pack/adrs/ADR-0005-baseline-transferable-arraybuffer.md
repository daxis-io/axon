# ADR-0005: Use Transferable ArrayBuffer as the Baseline Data Transport

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Portable browser deployments cannot assume shared memory. `postMessage` transfer lists can transfer `ArrayBuffer` ownership between workers.

## Decision

Use `MessagePort`/`postMessage` with transferable `ArrayBuffer`s for WCRPC v0.

## Consequences

Positive:

- broad browser compatibility;
- avoids row structured-clone overhead;
- no cross-origin isolation requirement.

Negative:

- not end-to-end zero-copy from Wasm linear memory;
- ownership/detach semantics require care;
- still has worker message scheduling overhead.
