# ADR-0009: Preserve the Public Browser Worker Protocol

**Status:** Proposed  
**Date:** 2026-07-06

## Context

SDK callers currently interact with one worker-backed session. Exposing WCRPC would leak internal distributed execution details.

## Decision

Keep the existing public command and response protocol stable in v0. WCRPC remains internal. Only expose worker-pool details via metrics extensions and structured fallback reasons.

## Consequences

Positive:

- no public API churn;
- future WCRPC internals can evolve;
- product surface remains simple.

Negative:

- debugging requires metrics/trace tooling;
- public callers cannot directly control exchange internals.
