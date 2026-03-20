# ADR-0006: Upstream-First Fork Policy With Explicit Version Cadence

- Status: Proposed
- Date: 2026-03-20
- Decision owner: DX / OSS maintainer
- Delivery DRI: Runtime / engine team
- Approver: Executive sponsor

## Decision

Forks are allowed only under a strict upstream-first policy.

Every downstream patch must have:

- tracking issue
- owner
- upstream disposition: `proposed`, `opened`, `merged`, `wontfix`, or `temporary`
- removal condition

Quarterly:

- rehearse upgrades in CI
- run conformance and perf
- refresh private patch inventory

## Context

The current browser bindings are pinned to DataFusion 45 while upstream DataFusion continues to evolve and still tracks WASM work as an active area. Long-lived divergence will become expensive quickly unless it is tracked aggressively.

## Preferred Upstream Targets

- DataFusion WASM support improvements
- browser-safe runtime and task abstractions
- range-read correctness in object-store adapters
- release and upgrade SOP for wasm bindings

## Consequences

- downstream divergence becomes visible and reviewable
- upgrade work becomes routine instead of reactive
- private patches gain an explicit exit strategy

## References

- [Apache DataFusion WASM epic](https://github.com/apache/datafusion/issues/13815)
- [`datafusion-wasm-bindings` Cargo.toml](https://raw.githubusercontent.com/datafusion-contrib/datafusion-wasm-bindings/main/Cargo.toml)
