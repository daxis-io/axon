# ADR-0002: Introduce WCRPC as an Internal Protocol

**Status:** Proposed  
**Date:** 2026-07-06

## Context

The public browser worker protocol is a command/response interface. Multi-worker analytical execution needs stream lifecycle, schema validation, budgets, credits, cancellation, status, and metrics.

## Decision

Introduce WCRPC as an internal protocol between coordinator, child, and merge workers. Do not expose WCRPC as a public SDK protocol in v0.

## Consequences

Positive:

- clear internal contract;
- streamable columnar data exchange;
- future exchange topologies possible;
- public protocol stability.

Negative:

- additional protocol implementation;
- versioning required;
- tests needed for frame lifecycle and failure modes.
