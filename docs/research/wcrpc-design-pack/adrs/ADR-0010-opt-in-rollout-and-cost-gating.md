# ADR-0010: Roll Out WCRPC Behind Opt-In and Cost Gates

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Worker-pool execution can be slower for small queries or large materialized outputs because startup, transfer, and merge overhead may dominate.

## Decision

Ship WCRPC worker-pool execution behind an opt-in flag first. Later enable automatically only when query shape, file count, estimated bytes, output size, and browser capability suggest speedup.

## Consequences

Positive:

- safer rollout;
- avoids regressions on small workloads;
- metrics can tune the heuristic.

Negative:

- initial feature discoverability limited;
- requires benchmark-based threshold tuning.
