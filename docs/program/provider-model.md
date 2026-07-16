# Provider And Host Integration Model

- Status: Working architecture
- Date: 2026-06-20
- Revised: 2026-07-15
- Scope: Axon's source-profile composition, provider boundaries, authority, and host integration boundary
- Related:
  - [Axon workbench architecture](./axon-workbench-architecture.md)
  - [Browser Unity Catalog brokered runtime contract](./browser-uc-brokered-runtime-contract.md)
  - [ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)
  - [ADR-0010: Pluggable Catalog Providers](../adr/ADR-0010-pluggable-catalog-providers.md)

## Current Implementation

Axon currently executes browser-local Delta and public object-storage sources.
Generated `axon/common/v1`, `axon/catalog/v1`, `axon/dataaccess/v1`,
`axon/exec/v1`, and `axon/fs/v1` messages are landed. The filesystem package is
messages-only contract substrate; no E8 provider, adapter, UI, or runtime
consumer has landed. App-layer `CatalogProvider`, `DataAccessResolver`, and
`ExecutionProvider` interfaces have not been adopted end to end. Unity Catalog
discovery, governed read resolution, remote execution, and filesystem adoption
remain proposed work.

The landed `axon.exec.v1` commands and events describe the browser worker compatibility boundary. They are not a portable remote execution service contract. A remote service contract is deferred until a concrete host implementation can prove its identity, admission, streaming, cancellation, retry, and audit requirements.

## Source Profiles Compose Narrow Seams

A `SourceProfile` is deployment composition, not another provider interface. It binds the narrow seams needed for one usable workflow:

- `IdentitySessionProvider`: establishes the principal and session lifetime.
- `CatalogProvider`: discovers catalog objects and metadata. It never resolves byte access or executes queries.
- `AuthorizationDecisionProvider`: produces an early allow, deny, or unknown
  decision for workbench behavior. Its decision is enforcement only for
  resources Axon itself controls.
- `DataAccessResolver`: converts a logical object reference into one
  pre-acceptance outcome: `browser_read` with a short-lived openable envelope,
  `remote_required`, `denied`, or a typed operational error.
- `ExecutionProvider`: admits a caller-created `execution_id` with exactly one
  resource-binding form, then owns cancellation and authoritative terminal state.
- `FileSystemProvider`: lists and stats file-like objects and returns logical
  references. It is optional and separate from catalog discovery; access and
  preview stay in the resolver and executor seams.

Local Delta and object-storage adapters may implement several seams behind one profile. That does not merge their contracts. Distinct ownership, lifetime, caching, and trust rules remain visible at each boundary.

## Authority And Resource Binding

Authorization decision and enforcement authority are different roles. Browser Cedar may hide or disable actions early and can enforce access to browser-local resources. It cannot enforce access to remote catalog metadata, object bytes, or remote compute. The server or storage boundary serving those resources remains the enforcement point and audit owner.

Every seam uses the same `CanonicalResourceRef`: connection ID, versioned
provider namespace, resource kind, and exactly one provider-object-ID or
canonical-locator identity arm. A provider ID is scoped to the other tuple
fields. A locator is capability-free and is emitted only by that namespace's
versioned, fixture-tested canonicalizer. Presentation names, signed URLs, and
grants never participate in identity.

An execution request carries one resource binding:

- `BrowserWasm` receives an execution-local resolved access envelope containing
  one openable descriptor, access class, correlation/provenance identifiers, and
  mandatory earliest expiry for capability-bearing access. Rejection or terminal
  execution disposes it; a new execution resolves a new envelope.
- Native or remote execution receives a logical object reference and resolves access inside its own trust boundary.

An executor must not accept both a logical reference and a resolved browser descriptor. This prevents accidental re-resolution under a different principal, session, snapshot, or policy decision.

## Supported And Planned Profiles

| Profile                                              | Identity                                            | Discovery                            | Decision / enforcement                                      | Access and execution                                                      | Cache, persistence, and audit owner                                                                                                |
| ---------------------------------------------------- | --------------------------------------------------- | ------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Browser-local Delta                                  | Browser user gesture and retained handle permission | Local adapter                        | Axon decision and browser permission enforcement            | Browser opens local bytes and executes in WASM                            | Metadata may persist; handles require re-grant; Axon owns local diagnostics                                                        |
| Public object storage                                | Anonymous browser session                           | Object-prefix adapter                | Axon safety checks; origin and storage enforce reads        | Browser resolves public HTTPS objects and executes in WASM                | Public immutable ranges may persist by stable object identity; Axon owns local diagnostics                                         |
| Session-proxied Unity Catalog with browser execution | Same-origin authenticated session                   | UC adapter through the session proxy | Browser decision is early UX; proxy/catalog/storage enforce | Resolver returns one execution-local browser-safe envelope; WASM executes | Discovery cache is session-scoped; grants and signed URLs are never persisted or reused across executions; proxy/catalog own audit |
| Governed host with remote execution                  | Host session                                        | Host or catalog adapter              | Host decides and enforces                                   | Browser sends a logical reference; host resolves and executes remotely    | Host owns resource caches, persistence, cancellation records, and audit; browser caches only safe presentation state               |

Volume selection composes catalog discovery with a `FileSystemProvider`: the catalog returns a volume reference, the filesystem provider lists entries, the enforcement owner resolves the selected file, and preview runs through the same one-binding execution rule.

## Host Integration Boundary

Host products and control planes are not lakehouse providers or execution engines. Use these terms for integration seams:

- `HostProductIntegration`: embeds Axon's workbench or query runtime.
- `ControlPlaneIntegration`: owns identity, policy, rollout, audit, or deployment decisions around Axon.
- `ApprovedReadPlanSource`: supplies a read decision that Axon validates before converting it into a browser access envelope.
- `GovernedDescriptorSource`: supplies browser-safe descriptors with explicit expiry and provenance.
- `SessionAccessSource`: supplies session-aware fetch behavior, signed URLs, or proxy routes without exposing cloud or catalog secrets to browser packages.

These are integration roles, not additions to the provider taxonomy. Consumer-specific proof, rollout, and compatibility artifacts remain under `docs/integrations/` and `docs/release-gates/`.

## Deferred Taxonomy

Delta Sharing and Iceberg remain future source profiles. They should not be documented as current support until each has an implementation plan, compatibility matrix, access-resolution contract, and release evidence. Functions, models, writes, and a generic remote connector SPI are also deferred until a concrete vertical slice needs them.
