# Provider And Host Integration Model

- Status: Working architecture
- Date: 2026-06-20
- Scope: Axon's lakehouse provider taxonomy, query-engine boundary, and host integration boundary
- Related:
  - [Axon workbench architecture](./axon-workbench-architecture.md)
  - [Browser Unity Catalog brokered runtime contract](./browser-uc-brokered-runtime-contract.md)
  - [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)

## Provider Taxonomy

Axon treats a lakehouse provider as the source of table metadata, active file descriptors, object access facts, and read capability classification. A provider can be local, object-storage-backed, catalog-backed, or protocol-backed, but it must describe lakehouse data rather than product rollout policy.

The current provider set is:

| Provider        | Role                                                                                                               |
| --------------- | ------------------------------------------------------------------------------------------------------------------ |
| `LocalDelta`    | Opens browser-local or native-local Delta tables from user-selected files and directories.                         |
| `ObjectStorage` | Opens browser-safe object-storage descriptors, public objects, signed URLs, proxy URLs, or brokered object routes. |
| `UnityCatalog`  | Opens governed tables through catalog-backed metadata and approved descriptor/read-plan handoff.                   |
| `DeltaSharing`  | Opens tables exposed through the Delta Sharing protocol and its read descriptors.                                  |

Iceberg is reserved as a future provider. It should not be documented as current support until Axon has an implementation plan, compatibility matrix, and release evidence for Iceberg metadata and scan semantics.

## Execution Targets

Providers do not choose the execution engine by themselves. After Axon has table descriptors and capability facts, the query routes to an execution target:

- Browser DataFusion WebAssembly for supported interactive read queries.
- Native DataFusion / delta-rs for correctness-oracle execution and mandatory fallback.

This keeps provider eligibility separate from runtime capability. A provider can expose a table that still requires native fallback because of SQL shape, Delta protocol features, browser storage constraints, object access failure, or runtime budget limits.

## Engine And Host Integration Boundary

Axon is the query engine/runtime layer. Host products and control planes are not lakehouse providers and are not execution engines. Use these generic terms for integration seams:

- `HostProductIntegration`: an external analytics product or service that embeds Axon as its query engine/runtime layer.
- `ControlPlaneIntegration`: an external product or service that owns identity, session, policy, rollout, audit, or deployment decisions around Axon.
- `ApprovedReadPlanSource`: a trusted source of read plans that Axon can validate and consume without becoming the policy authority.
- `GovernedDescriptorSource`: a trusted source of browser-safe table or object descriptors with capability metadata.
- `SessionAccessSource`: a trusted source of session-aware fetch behavior, signed URLs, or proxy routes that keeps cloud/catalog secrets outside browser packages.

An integration can supply `ApprovedReadPlanSource` or `GovernedDescriptorSource` data for a provider such as `ObjectStorage`, `UnityCatalog`, or `DeltaSharing`. That does not make the integration itself part of the lakehouse provider taxonomy.

## Downstream Consumer Scope

Downstream consumers of Axon can define governed deployment boundaries. Their examples, release gates, external proof packets, and rollout contracts live under `docs/integrations/` and `docs/release-gates/` because they are consumer-specific proof and rollout artifacts.

Consumer integrations are excluded from Axon's lakehouse provider taxonomy. They are not provider profiles, execution engines, or organizing models for the open source project. Public consumer-specific contract names remain stable until a separate compatibility plan decides otherwise.
