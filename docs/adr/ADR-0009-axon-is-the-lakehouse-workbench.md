# ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime

- Status: Accepted
- Date: 2026-06-20
- Decision owner: Runtime / engine team

## Decision

Axon is the standalone lakehouse query engine and browser/native workbench runtime for inspecting and querying open lakehouse data. The core product model is provider-driven and engine-first: Axon opens lakehouse data through explicit provider contracts, executes supported reads in browser WebAssembly when possible, and preserves native execution as the correctness oracle and fallback target.

Axon's architecture uses three separate taxonomy buckets so lakehouse providers, execution targets, and control-plane integrations do not collapse into one product-specific model:

- **Lakehouse providers** describe where table metadata, object descriptors, and read eligibility come from. Current providers are `LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing`. Iceberg is reserved for a future provider after a compatibility plan exists.
- **Execution targets** describe where query work runs. Current targets are the browser DataFusion WebAssembly path and the native DataFusion / delta-rs runtime.
- **Host/control-plane integrations** describe external systems that provide identity/session context, governed descriptors, approved read plans, rollout policy, release evidence, or hosted deployment context.

Host products may embed Axon as their analytical read engine while owning product authority such as users, tenants, billing, catalog governance, audit, workflows, dashboards, agents, rollout, and server fallback policy. Those hosts are excluded from Axon's provider taxonomy and are not the organizing model for Axon.

## Context

Axon started with a narrow browser query runtime and later grew downstream integration contracts for governed browser reads. Those contracts remain useful, but placing consumer-specific narrative documents in the core program spine made Axon look like a subsystem of a host product instead of a reusable lakehouse query engine/runtime.

The project now needs docs, navigation, and conformance checks that keep the product model clear without renaming stable consumer wire contracts.

## Policy

- Core docs should introduce Axon first as a lakehouse query engine and workbench runtime.
- Provider docs should name only real lakehouse providers: `LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing`; Iceberg remains future.
- Integration docs may describe consumer-specific gateway, rollout, release-gate, and external-proof behavior under `docs/integrations/`.
- Public consumer contract names remain stable until a separate compatibility plan changes them.
- Core docs should use generic integration names such as `ControlPlaneIntegration`, `ApprovedReadPlanSource`, and `GovernedDescriptorSource`.

## Consequences

The README, program architecture docs, ADR index, conformance tests, and CI checks now enforce the Axon-first taxonomy. Downstream consumer docs and examples live under an integration namespace rather than the core program spine.

## References

- [Axon workbench architecture](../program/axon-workbench-architecture.md)
- [Provider model](../program/provider-model.md)
