# Upstream WebAssembly Support Strategy

- Status: Draft for upstream review
- Date: 2026-07-23
- Owner: Runtime / engine team
- Scope: Arrow Rust, Parquet, `object_store`, DataFusion, delta-kernel, and delta-rs on `wasm32-unknown-unknown`
- Related:
  - [EPIC-07: Upstream Contributions And Version Lifecycle](../epics/EPIC-07-upstream-contributions-and-version-lifecycle.md)
  - [ADR-0006: Upstream-First Fork Policy](../adr/ADR-0006-upstream-first-fork-policy-and-version-cadence.md)
  - [Browser Dependency Compatibility Review Checklist](./browser-dependency-compatibility-review-checklist.md)
  - [Upstream WASM Support Design Pack](../research/upstream-wasm-support-design-pack/README.md)
  - [Upstream WASM Support Implementation Plan](../plans/2026-07-23-upstream-wasm-support-implementation-plan.md)
  - [Daxis Upstream WASM Fork POC Plan](../plans/2026-07-23-daxis-upstream-wasm-fork-poc.md)

## Decision

Axon will pursue target-safe support in each upstream owning crate. Downstream feature suppression may
remain useful for bundle-size control, but it cannot serve as the correctness condition for a WASM
build.

The upstream work follows these rules:

- Preserve public codec feature names and native default feature sets.
- Select native implementations with target-specific dependencies.
- Allow Cargo to unify logical features into a WASM graph without pulling unavailable backends.
- Return a target-specific error when code requests a codec whose logical feature is enabled but
  whose backend is unavailable.
- Keep `object_store/http-base` free of JavaScript, filesystem, OS entropy, and Tokio runtime
  requirements.
- Expose browser services through an explicit `web` host feature.
- Keep DataFusion's initial browser support claim to the tested in-memory, no-disk,
  single-partition profile.
- Keep delta-kernel's default engine native and build browser execution as a separate engine.

## Axon Boundary

This strategy does not replace Axon's browser access authority. Axon continues to own
`BrowserHttpSnapshotDescriptor`, signed or brokered HTTPS access, identity-aware range caching, query
budgets, and native fallback.

The upstream changes make the shared dependency graph safe. They do not add Axon descriptors,
credential rules, cache policy, or product-specific execution assumptions to Arrow, Parquet,
`object_store`, DataFusion, delta-kernel, or delta-rs.

## Deliverables

The [design pack](../research/upstream-wasm-support-design-pack/README.md) contains:

- the cross-project architecture and ownership model;
- the blocker graph with failure phase and evidence status;
- Arrow and Parquet codec policy;
- the `object_store` browser runtime and HTTP contract;
- DataFusion and Delta integration boundaries;
- CI, browser conformance, and downstream smoke gates;
- a mergeable upstream PR sequence;
- the risk register and reproducible research snapshot;
- package-local architecture decisions and an implementation plan.

## Exit Condition

Axon can remove a downstream workaround only after the owning upstream release passes its focused
WASM gate and Axon's locked browser dependency rehearsal. Any temporary patch remains listed in
[Upstream Patch Inventory](./upstream-patch-inventory.md) until that release is adopted.
