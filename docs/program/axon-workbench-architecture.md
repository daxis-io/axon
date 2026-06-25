# Axon Workbench And Query Engine Architecture

- Status: Working architecture
- Date: 2026-06-20
- Scope: Axon's query-engine, runtime, and workbench architecture
- Related:
  - [Provider model](./provider-model.md)
  - [Browser lakehouse engine strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Delta compatibility matrix](./browser-delta-compatibility-matrix.md)
  - [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)

## Summary

Axon is an embeddable lakehouse query engine and workbench runtime. It gives users and host products a way to open Delta and Parquet data, inspect table state, run supported SQL in the browser, and fall back to the native runtime when browser execution cannot make a safe correctness claim. The core model keeps lakehouse providers, execution targets, and host/control-plane integrations separate.

The architecture separates three concerns:

- **Lakehouse providers** resolve table identity, active files, object descriptors, and provider-specific read capabilities.
- **Execution targets** run the query through the browser DataFusion WebAssembly path or the native DataFusion / delta-rs path.
- **Host/control-plane integrations** provide identity/session context, governed descriptors, approved read plans, rollout policy, release evidence, or deployment packaging without becoming providers or execution engines.

That split keeps Axon reusable. A local folder picker, object-storage descriptor, Unity Catalog broker, Delta Sharing endpoint, hosted analytics product, and release pipeline can all feed Axon without changing what a lakehouse provider is or what the query engine owns.

Axon owns performance-critical read execution: descriptor validation, browser-safe byte-range reads, Parquet/Delta scan planning, pruning, cache behavior, DataFusion-backed execution, Arrow IPC result transport, runtime budgets, metrics, and fallback/error taxonomy. Host platforms own product authority: users, tenants, catalog governance, billing, audit, workflows, dashboards, agents, rollout, and server fallback policy.

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

## Host Integration Boundary

Integrations can provide `ControlPlaneIntegration`, `ApprovedReadPlanSource`, `GovernedDescriptorSource`, or session-aware access implementations. They may authenticate a browser session, approve a descriptor, issue object-scoped URLs, choose a rollout segment, or attach release evidence, but they do not become lakehouse providers unless they expose a provider contract that Axon can use independently of that control-plane policy.

Consumer-specific documentation lives under `docs/integrations/` because it describes host-owned gateway, policy, rollout, and production-proof responsibilities around Axon. The core Axon architecture remains engine-first, provider-driven, and integration-neutral.

## Compatibility Rules

- Browser execution must fail closed when table support, policy facts, descriptor safety, SQL shape, or runtime budgets are unknown.
- Native execution remains the reference path for correctness and unsupported browser features.
- Provider contracts should be explicit about table identity, snapshot or file identity, object access mode, capabilities, and fallback semantics.
- Integration-specific release gates can require external proof, but core Axon success claims must stay tied to repo-owned runtime evidence.
