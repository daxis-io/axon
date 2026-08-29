# ADR-0003: Split Control Plane and Data Plane

**Status:** Proposed  
**Date:** 2026-07-06

## Context

Analytical data payloads can be large. Encoding rows or batches directly into protobuf/JSON control messages would create avoidable overhead.

## Decision

Use small control frames for stream metadata and Arrow/DataFusion-native data frames for payloads. Prohibit row-wise JSON/protobuf payloads on the hot path.

## Consequences

Positive:

- lower serialization overhead;
- cleaner schema and codec negotiation;
- payload representations can evolve independently.

Negative:

- two-plane protocol complexity;
- buffer lifetime rules required.
