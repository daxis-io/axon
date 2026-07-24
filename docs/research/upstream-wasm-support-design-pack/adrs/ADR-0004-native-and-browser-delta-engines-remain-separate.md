# ADR-0004: Native And Browser Delta Engines Remain Separate

- Status: Proposed
- Date: 2026-07-23
- Scope: delta-kernel and delta-rs

## Decision

`delta_kernel_default_engine` remains the native Tokio, reqwest, filesystem, and UUID engine.
Browser execution uses a separate engine that implements kernel engine interfaces with browser-safe
object access and host services.

delta-rs keeps native defaults and selects the browser engine through a separate dependency profile.

## Context

The default engine's dependencies express native runtime choices. Adding target exceptions throughout
that crate would mix two host models and retain native dependencies in the browser graph.

The kernel core should compile without the default engine. Its mandatory entropy use and Arrow
integration still need focused target work.

delta-rs also consumes a Buoyant kernel fork from a moving branch at the pinned source revision.
Browser results need exact fork provenance plus a separate upstream adoption test.

## Consequences

- Native engine behavior remains stable.
- Browser services stay in one engine boundary.
- Kernel core APIs remain host-neutral.
- delta-rs can publish a narrow read-only browser profile.
- CI pins the consumed fork and tests the intended upstream kernel revision in another job.

## Rejected Options

### Add WASM exceptions throughout the default engine

This retains native runtime assumptions and expands the test matrix inside one implementation.

### Put Axon descriptors into delta-kernel or delta-rs

Descriptors, broker policy, cache policy, and fallback belong to Axon. The engine boundary accepts
generic object and runtime services.

### Add a public random capability before auditing uses

Write-only IDs, retry jitter, temporary paths, and protocol identifiers have different owners and
security requirements.

## References

- [DataFusion and Delta integration](../docs/05-datafusion-delta-integration.md)
- [delta-kernel default engine manifest](https://github.com/delta-io/delta-kernel-rs/blob/03e2537d5a6203872d9785e31cc00f503ddeb8ec/default-engine/Cargo.toml)
