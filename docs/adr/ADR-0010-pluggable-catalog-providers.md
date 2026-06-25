# ADR-0010: Pluggable Catalog Providers And Table Read Resolution Seams

- Status: Proposed
- Date: 2026-06-21
- Decision owner: Frontend / workbench team
- Delivery DRI: Frontend / workbench team
- Approver: Query platform lead

## Decision

Axon's web client uses three adjacent, provider-agnostic seams for lakehouse
objects:

- **`CatalogProvider` discovers** catalog structure: catalogs, schemas, tables,
  views, volumes, functions, models, and metadata. It answers "what exists" and
  produces refs such as `TableRef` and `VolumeRef`.
- **`DataAccessResolver` resolves** whether this client may read a specific table
  now, and from where. It answers "how, if at all, may these bytes be read" and
  returns a `TableReadResolution`.
- **`ExecutionProvider` runs or previews** data through a backend such as
  `BrowserWasm`, future Tauri/native IPC, or a future remote service.

`CatalogProvider` is a discovery-only E1 seam. It must not expose
`resolveTableRead`, mint browser descriptors, inspect access grants, or call
`openDeltaTable()`. Explorer actions such as "Sample data" and "Open in SQL
editor" hand a table ref to E9, where `DataAccessResolver` and
`ExecutionProvider` own resolution and execution.

E9's `DataAccessResolver` returns a `TableReadResolution` union with four arms:

- `descriptor` - a ready-to-open `BrowserHttpSnapshotDescriptor`; the only arm
  that may directly reach `openDeltaTable()`.
- `read_access_plan` - a brokered `ReadAccessPlan` that must be processed into an
  openable descriptor before execution. This is the reserved Daxis/brokered
  outcome; see ADR-0008.
- `fallback` - not directly readable; carries a structured
  `ReadAccessPlanReason` and a human message, for example views, managed tables,
  or non-reachable storage.
- `blocked` - refused; carries reason and message.

The planned proto `axon/dataaccess/v1.TableReadResolution` `oneof` is the wire
contract. The in-process E9 seam may keep a `{kind}` discriminated union and
carry the live SDK descriptor on the `descriptor` arm to avoid a lossy
bigint/null round-trip at the execution boundary. `isDescriptorResolution` and
`isOpenableResolution` type guards gate the handoff.

The implementation paths referenced by this ADR, such as
`apps/axon-web/src/query/providers/` and
`apps/axon-web/src/editor/explorer/open-table.ts`, are planned E1/E9 paths until
those milestones land. This ADR defines the ownership boundary before the code is
created.

## Context

The original E0/E1 planning language conflated navigation and read resolution in
a single `['catalog', connectionId, 'table-derived']` shape and, at times, placed
`resolveTableRead` on `CatalogProvider`. That would make a catalog browser partly
responsible for access grants and execution, which is the wrong trust boundary.

E1 needs DirectUnityCatalog, LocalDelta, ObjectStore, and the deferred Daxis
catalog-listing profile to coexist without UI branching on source kind. E9 needs
the read-resolution and execution path to fail closed and preserve the single
descriptor-to-engine handoff. Keeping discovery, read resolution, and execution
separate lets both happen without redesign.

## Policy

- All remote metadata I/O goes through one session-aware fetch wrapper,
  `SessionHttp` (planned at `apps/axon-web/src/query/providers/http.ts`): always
  `credentials:'include'`, attaches a correlation id, maps `401 → session_expired`
  / `403 → blocked` / `404 → not_found` / `5xx → unavailable`, and **never**
  attaches an `Authorization` header from browser state. Remote providers never
  call `fetch` directly. Reference providers use a no-HTTP dependency.
- **Catalog discovery only**: E1 `CatalogProvider` implementations return
  navigation nodes, metadata, refs, and capability facts. They do not resolve
  table bytes, cache grants, or open execution sessions.
- **Single handoff**: only the `descriptor` (and, when implemented, processed
  `read_access_plan`) outcome may reach `openDeltaTable()`. `fallback` and
  `blocked` must never open an engine session. E9 enforces this at the
  resolver/executor boundary and any explorer open/sample adapter.
- **Fail closed**: any uncertainty (unknown policy state, resolver error,
  non-reachable storage, unsupported shape) resolves to `fallback`/`blocked` with
  a structured reason, never to a speculative open.
- **No secrets in the browser (ADR-0002 restated)**: connection locators carry
  only non-secret routing facts (`bffBaseUrl`, host, path, bucket). Persisted
  registry/navigation nodes contain no credentials, tokens, or signing material.
- Read resolutions are **never persisted** because grants expire. Only
  non-secret navigation nodes and connection locators are durable.
- Provider `kind` dispatch for discovery lives solely in the E1 provider
  registry. Adding a catalog provider is additive there and should not require UI
  branch logic.

## Local development and tests

Envoy (E6) is a **deployment** concern only. Dev and integration tests should run
a real OSS Unity Catalog server reached **same-origin** via the Vite dev proxy
(`/api/uc/* -> local UC`); `SessionHttp` keeps the same contract. UC wire types
are generated from a vendored OpenAPI spec via `openapi-typescript` with a drift
gate, then adapted to the E3A `axon/catalog/v1` normalized messages when those
messages land.

## Consequences

- The explorer is provider-agnostic for discovery; DirectUC, Daxis catalog
  listing, and the reference sources share one navigation path.
- The run lifecycle remains provider-agnostic through E9, not through the E1
  catalog seam.
- The `read_access_plan` arm reserves the Daxis/brokered seam, so Daxis can be
  **deferred without redesign** (ADR-0008 is its profile).
- Navigation node shapes are generated, so contract drift is caught by codegen
  checks rather than at runtime.
- The descriptor-on-the-E9-seam choice trades a small wire/in-process type
  divergence for a lossless, allocation-light execution handoff.

## Acceptance Standard

This ADR is implemented when:

- every catalog source is reached only through the E1 `CatalogProvider` registry,
  with no provider-kind branching in explorer UI;
- E1 provider methods are limited to discovery/navigation and return table or
  volume refs for downstream seams;
- E9's `DataAccessResolver` returns the four-arm union and `fallback`/`blocked`
  provably never reach `openDeltaTable()` (Vitest resolution matrix plus
  open/sample adapter tests);
- all provider HTTP flows through `SessionHttp` with the documented status mapping
  and no `Authorization` header from browser state;
- persisted registry/navigation nodes pass a secret-marker scan;
- UC wire codegen drift check is green.

## References

- [ADR-0002: Browser access uses signed HTTPS or proxy, never cloud secrets](ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
- [ADR-0008: Daxis browser read compute contract](ADR-0008-daxis-browser-read-compute-contract.md)
- [E1 — Pluggable Catalog Providers execution plan](../plans/2026-06-20-e1-catalog-providers-execution-plan.md)
- [E3A provider contract mappings](../program/e3a-provider-contract-mappings.md)
- planned `apps/axon-web/src/query/providers/` (discovery seam, registry,
  providers, queries)
