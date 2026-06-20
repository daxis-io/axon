# Axon Workbench Architecture

- Status: Working architecture
- Date: 2026-06-20
- Scope: Axon's product and runtime architecture as a standalone lakehouse workbench
- Related:
  - [Provider model](./provider-model.md)
  - [Browser lakehouse engine strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Delta compatibility matrix](./browser-delta-compatibility-matrix.md)
  - [ADR-0009: Axon Is The Lakehouse Workbench](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)
  - [Daxis integration strategy](../integrations/daxis/daxis-first-class-integration-strategy.md)

## Summary

Axon is a standalone lakehouse workbench and browser/native runtime. It gives users a way to open Delta and Parquet data, inspect table state, run supported SQL in the browser, and fall back to the native runtime when browser execution cannot make a safe correctness claim. The core model keeps lakehouse providers, execution targets, and control-plane integrations separate.

The architecture separates three concerns:

- **Lakehouse providers** resolve table identity, active files, object descriptors, and provider-specific read capabilities.
- **Execution targets** run the query through the browser DataFusion WebAssembly path or the native DataFusion / delta-rs path.
- **Control-plane and deployment integrations** provide governed descriptors, approved read plans, rollout policy, release evidence, or deployment packaging without becoming providers.

That split keeps Axon reusable. A local folder picker, object-storage descriptor, Unity Catalog broker, Delta Sharing endpoint, hosted product integration, and release pipeline can all feed Axon without changing what a lakehouse provider is.

## Runtime Layers

```text
Workbench surfaces and SDK
        |
        v
Provider model
  LocalDelta | ObjectStorage | UnityCatalog | DeltaSharing
        |
        v
Execution target
  browser DataFusion WASM | native DataFusion / delta-rs
        |
        v
Result, metrics, fallback, and release evidence contracts
```

The browser target owns browser-safe range reads, descriptor materialization, DataFusion-backed SQL execution, Arrow IPC result transport, structured metrics, and deterministic unsupported/fallback reasons. The native target remains the correctness oracle and mandatory fallback path for query shapes, table features, or runtime budgets outside the browser contract.

## Integration Boundary

Integrations can provide `ControlPlaneIntegration`, `ApprovedReadPlanSource`, or `GovernedDescriptorSource` implementations. They may approve a descriptor, issue object-scoped URLs, choose a rollout segment, or attach release evidence, but they do not become lakehouse providers unless they expose a provider contract that Axon can use independently of that control-plane policy.

Daxis is one such governed integration. Its documentation lives under `docs/integrations/daxis/` because it describes Daxis-owned gateway, policy, rollout, and production-proof responsibilities around Axon. The core Axon architecture remains provider-driven and integration-neutral.

## Compatibility Rules

- Browser execution must fail closed when table support, policy facts, descriptor safety, SQL shape, or runtime budgets are unknown.
- Native execution remains the reference path for correctness and unsupported browser features.
- Provider contracts should be explicit about table identity, snapshot or file identity, object access mode, capabilities, and fallback semantics.
- Integration-specific release gates can require external proof, but core Axon success claims must stay tied to repo-owned runtime evidence.
