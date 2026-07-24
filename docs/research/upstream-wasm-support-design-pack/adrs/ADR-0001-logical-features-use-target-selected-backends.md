# ADR-0001: Logical Features Use Target-Selected Backends

- Status: Proposed
- Date: 2026-07-23
- Scope: Arrow IPC, Parquet, `object_store`, and DataFusion implementation dependencies

## Decision

Public Cargo features describe logical capabilities. Each crate selects an implementation with
target-specific dependencies and matching source `cfg` expressions.

For this design, the unavailable target predicate is:

```rust
all(target_arch = "wasm32", target_os = "unknown")
```

Native zstd and crypto dependencies live under the inverse predicate. Browser-only dependencies live
under the exact target and an explicit host feature where host choice is required.

## Context

Cargo unions features enabled for one package. A downstream `default-features = false` declaration
cannot suppress a feature enabled by another edge. Ordinary optional dependencies remain active
after that union.

Resolver v2 excludes target-specific dependency declarations that are inactive for the selected
target. It supplies the separation needed without removing native defaults.

## Consequences

- Native public feature sets remain stable.
- Feature-unified WASM graphs omit unavailable native implementations.
- Crates need source gates that match their manifest gates.
- Tests must cover logical enablement and target availability as separate states.
- A later pure-Rust backend can implement the same logical feature.

## Rejected Options

### Remove zstd from native defaults

This changes native behavior and pushes target constraints onto all users.

### Require every downstream to disable defaults

Another graph edge can restore the feature, and each consumer must rediscover the same workaround.

### Add competing public native and Rust backend features

Mutually competing backend features create a second feature-unification problem before users have a
demonstrated need to choose an implementation.

## References

- [Cargo feature unification](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification)
- [Cargo resolver v2](https://doc.rust-lang.org/cargo/reference/features.html#feature-resolver-version-2)
- [Architecture and ownership](../docs/01-architecture-and-ownership.md)
