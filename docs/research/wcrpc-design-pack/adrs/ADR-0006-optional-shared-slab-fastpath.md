# ADR-0006: Add Shared Slab Transport Only as an Optional Fast Path

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Shared memory can reduce data movement overhead, but browser `SharedArrayBuffer` requires deployment constraints such as cross-origin isolation.

## Decision

Do not require shared memory for v0. Design WCRPC buffer references so `shared_slab` can be added later.

## Consequences

Positive:

- deployable v0;
- clear path to near-native exchange;
- no architecture rewrite needed later.

Negative:

- v0 may have an extra copy;
- shared-slab allocator remains future work.
