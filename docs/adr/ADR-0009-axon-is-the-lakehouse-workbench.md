# ADR-0009: Axon Is The Lakehouse Query Engine And Workbench Runtime

- Status: Accepted
- Date: 2026-06-20
- Revised: 2026-07-15
- Decision owner: Runtime / engine team

## Decision

Axon is a standalone lakehouse query engine and browser/native workbench runtime
for inspecting and querying open lakehouse data. It can also run inside a host
that supplies identity, governance, or remote execution. Axon does not become the
host's control plane when embedded.

A source profile composes three core seams:

- `CatalogProvider` discovers resources and returns canonical references.
- `DataAccessResolver` turns a reference into an execution decision for the
  current principal and session.
- `ExecutionProvider` executes in a selected browser, native, or remote runtime.

The profile may also bind identity/session, early authorization decisions, and
an optional filesystem seam. It is configuration, not another provider
interface. Local Delta, public object storage, session-proxied Unity Catalog,
Delta Sharing, and governed hosts compose the seams differently. UI code
consumes the narrow contracts and does not branch on those source labels.

Authority follows the selected profile. Axon owns local file consent and local
browser execution. A catalog or governed host owns authorization for its remote
resources, capability issuance, authoritative audit, and any remote execution.
Browser policy may make early UI and safety decisions, but it does not replace
server enforcement. Native execution remains the correctness oracle and the
explicit fallback target for unsupported browser work.

## Context

Axon started with a narrow browser query runtime and later added integration
contracts for governed reads. Consumer-specific language in the core program
spine made the product appear to be a subsystem of one host. A single generic
"provider" concept created a different problem: catalog discovery, capability
issuance, and execution have different owners, lifetimes, and trust boundaries.

ADR-0010 supersedes the earlier provider taxonomy in this ADR. The terms
`LocalDelta`, `ObjectStorage`, `UnityCatalog`, and `DeltaSharing` now name source
profiles or adapters, not interfaces that may combine discovery, access, and
execution authority.

## Policy

- Core docs introduce Axon as a lakehouse query engine and workbench runtime.
- Core interfaces use host-agnostic names. Consumer-specific gateways, rollout
  rules, and external proof remain under `docs/integrations/`.
- Catalog discovery never grants byte access or chooses an execution target.
- An execution request carries one authoritative resource binding: a resolved
  browser binding for browser execution or a logical resource reference for
  remote execution.
- No profile silently substitutes a sample, prior, or default source after the
  user selects a resource.
- Public consumer wire names remain stable until a compatibility plan changes
  them. Internal worker compatibility messages do not become portable service
  contracts through naming alone.

## Consequences

The same workbench can support local, public, catalog-backed, and governed
deployment profiles without granting every adapter the same authority. Host
products can embed Axon while retaining identity, tenancy, governance, audit,
and remote routing. Axon can change browser worker mechanics without requiring a
remote executor to emulate them.

## References

- [Axon workbench architecture](../program/axon-workbench-architecture.md)
- [Provider model](../program/provider-model.md)
- [ADR-0010: Pluggable catalog providers and table read resolution seams](ADR-0010-pluggable-catalog-providers.md)
