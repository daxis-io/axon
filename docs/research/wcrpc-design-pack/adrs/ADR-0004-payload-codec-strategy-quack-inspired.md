# ADR-0004: Use a Quack-Inspired Payload Codec Strategy

**Status:** Proposed  
**Date:** 2026-07-06

## Context

DuckDB Quack demonstrates that engine-native serialization can avoid overhead from transcoding through generic interchange formats. WCRPC should not assume Arrow IPC chunks are always the fastest internal worker exchange representation.

## Decision

Separate WCRPC stream semantics from payload codecs. Support Arrow IPC chunks in v0 and reserve codecs for Arrow record-batch buffers, DataFusion aggregate-state native payloads, and shared-slab Arrow buffers.

## Consequences

Positive:

- compatible v0 path;
- performance experimentation path;
- stable protocol semantics while payloads evolve.

Negative:

- codec negotiation and versioning;
- more benchmark work;
- native codec may be less interoperable.
