# ADR-0009: Axon Is The Lakehouse Workbench

- Status: Accepted
- Date: 2026-06-20
- Decision owner: Runtime / engine team

## Decision

Axon is the standalone lakehouse workbench and browser/native runtime for inspecting and querying open lakehouse data. The core product model is provider-driven: Axon opens lakehouse data through explicit provider contracts, executes supported reads in browser WebAssembly when possible, and preserves native execution as the correctness oracle and fallback target.

Axon's architecture uses three separate taxonomy buckets so lakehouse providers, execution targets, and control-plane integrations do not collapse into one product-specific model:

- **Lakehouse providers** describe where table metadata, object descriptors, and read eligibility come from. Current providers are `LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing`. Iceberg is reserved for a future provider after a compatibility plan exists.
- **Execution targets** describe where query work runs. Current targets are the browser DataFusion WebAssembly path and the native DataFusion / delta-rs runtime.
- **Control-plane and deployment integrations** describe external systems that provide governed descriptors, approved read plans, rollout policy, release evidence, or hosted deployment context.

Daxis belongs only to the control-plane integration and governed deployment context. It is excluded from Axon's provider taxonomy and is not the organizing model for Axon.

## Context

Axon started with a narrow browser query runtime and later grew Daxis integration contracts for governed browser reads. Those Daxis contracts remain useful, but placing Daxis narrative documents in the core program spine made Axon look like a Daxis subsystem instead of a reusable lakehouse workbench/runtime.

The project now needs docs, navigation, and conformance checks that keep the product model clear without renaming stable Daxis wire contracts.

## Policy

- Core docs should introduce Axon first as a lakehouse workbench/runtime.
- Provider docs should name only real lakehouse providers: `LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing`; Iceberg remains future.
- Integration docs may describe Daxis-specific gateway, rollout, release-gate, and external-proof behavior under `docs/integrations/daxis/`.
- Public Daxis contract names remain stable until a separate compatibility plan changes them.
- Core docs should use generic integration names such as `ControlPlaneIntegration`, `ApprovedReadPlanSource`, and `GovernedDescriptorSource`.

## Consequences

The README, program architecture docs, ADR index, conformance tests, and CI checks now enforce the Axon-first taxonomy. Daxis remains a supported integration boundary, but Daxis docs and examples live under an integration namespace rather than the core program spine.

## References

- [Axon workbench architecture](../program/axon-workbench-architecture.md)
- [Provider model](../program/provider-model.md)
- [Daxis integration strategy](../integrations/daxis/daxis-first-class-integration-strategy.md)
