# Provider Model

- Status: Working architecture
- Date: 2026-06-20
- Scope: Axon's lakehouse provider taxonomy and integration boundary
- Related:
  - [Axon workbench architecture](./axon-workbench-architecture.md)
  - [Browser Unity Catalog brokered runtime contract](./browser-uc-brokered-runtime-contract.md)
  - [ADR-0009: Axon Is The Lakehouse Workbench](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)

## Provider Taxonomy

Axon treats a lakehouse provider as the source of table metadata, active file descriptors, object access facts, and read capability classification. A provider can be local, object-storage-backed, catalog-backed, or protocol-backed, but it must describe lakehouse data rather than product rollout policy.

The current provider set is:

| Provider | Role |
| --- | --- |
| `LocalDelta` | Opens browser-local or native-local Delta tables from user-selected files and directories. |
| `ObjectStorage` | Opens browser-safe object-storage descriptors, public objects, signed URLs, proxy URLs, or brokered object routes. |
| `UnityCatalog` | Opens governed tables through catalog-backed metadata and approved descriptor/read-plan handoff. |
| `DeltaSharing` | Opens tables exposed through the Delta Sharing protocol and its read descriptors. |

Iceberg is reserved as a future provider. It should not be documented as current support until Axon has an implementation plan, compatibility matrix, and release evidence for Iceberg metadata and scan semantics.

## Execution Targets

Providers do not choose the execution engine by themselves. After Axon has table descriptors and capability facts, the query routes to an execution target:

- Browser DataFusion WebAssembly for supported interactive read queries.
- Native DataFusion / delta-rs for correctness-oracle execution and mandatory fallback.

This keeps provider eligibility separate from runtime capability. A provider can expose a table that still requires native fallback because of SQL shape, Delta protocol features, browser storage constraints, object access failure, or runtime budget limits.

## Integration Boundary

Control-plane and deployment integrations are not lakehouse providers. Use these generic terms for integration seams:

- `ControlPlaneIntegration`: an external product or service that owns policy, rollout, audit, or deployment decisions around Axon.
- `ApprovedReadPlanSource`: a trusted source of read plans that Axon can validate and consume without becoming the policy authority.
- `GovernedDescriptorSource`: a trusted source of browser-safe table or object descriptors with capability metadata.

An integration can supply `ApprovedReadPlanSource` or `GovernedDescriptorSource` data for a provider such as `ObjectStorage`, `UnityCatalog`, or `DeltaSharing`. That does not make the integration itself part of the lakehouse provider taxonomy.

## Daxis Scope

Daxis remains a supported control-plane integration and governed deployment boundary. Daxis examples, release gates, external proof packets, and rollout contracts live under `docs/integrations/daxis/` and `docs/release-gates/`.

Daxis is excluded from Axon's provider taxonomy. It is not a provider profile or catalog provider type. Public `DaxisApproved...` contract names remain stable for the Daxis integration until a separate compatibility plan decides otherwise.
