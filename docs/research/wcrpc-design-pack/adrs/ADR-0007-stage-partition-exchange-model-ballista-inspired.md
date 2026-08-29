# ADR-0007: Model WCRPC as Stage/Partition Exchange

**Status:** Proposed  
**Date:** 2026-07-06

## Context

DataFusion Ballista treats distributed execution as stages separated by exchange/shuffle boundaries. Browser worker-pool v0 only needs gather, but future top-k, distinct, hash group-by, and joins require explicit stage and partition identity.

## Decision

Include `stageId`, `sourceTaskId`, `inputPartitionId`, and `outputPartitionId` in WCRPC planning and data frames. Implement only gather in v0.

## Consequences

Positive:

- future-proof exchange model;
- cleaner metrics;
- easier direct worker-to-worker exchange later.

Negative:

- slightly more metadata in v0;
- stage model must not overcomplicate first implementation.
