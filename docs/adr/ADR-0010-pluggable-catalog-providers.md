# ADR-0010: Pluggable Catalog Providers And A Unified Table Read Resolution Seam

- Status: Proposed
- Date: 2026-06-21
- Decision owner: Frontend / workbench team
- Delivery DRI: Frontend / workbench team
- Approver: Query platform lead

## Decision

Axon's web client discovers and reads catalog objects through a single
provider-agnostic seam, `CatalogProvider`
([`apps/axon-web/src/query/providers/types.ts`](../../apps/axon-web/src/query/providers/types.ts)).
Every data source — DirectUnityCatalog, the deferred Daxis provider, and the
reference providers (LocalDelta, ObjectStore, manifest/fixture) — implements the
same interface. UI and the run lifecycle never branch on provider kind; they call
interface methods resolved through a pure factory, `providerForConnection(conn,
deps)` ([registry.ts](../../apps/axon-web/src/query/providers/registry.ts)).

The interface separates two concerns with different lifecycles and trust
properties:

- **Navigation** — listing catalogs/schemas/tables/volumes/functions/models and
  fetching object metadata. Cheap, paginated (`Page<T>` with an opaque
  `nextCursor`), cacheable for minutes. Node shapes are the **generated**
  `axon/catalog/v1` protobuf types so they stay in lockstep with the contract and
  feed E2/E4.
- **Read resolution** — `resolveTableRead(ref)` returns how (if at all) this
  browser may read a specific table *right now*. Policy-gated, expiring,
  **never persisted**, and **fail-closed**.

`resolveTableRead` returns a `TableReadResolution` union with four arms:

- `descriptor` — a ready-to-open `BrowserHttpSnapshotDescriptor`; the only arm
  that may reach `openDeltaTable()`.
- `read_access_plan` — a brokered `ReadAccessPlan` (the reserved Daxis/brokered
  outcome; see ADR-0008). Not executable in the bounded E1 slice.
- `fallback` — not directly readable; carries a structured `ReadAccessPlanReason`
  and a human message (e.g. views, managed tables, non-reachable storage).
- `blocked` — refused; carries reason + message.

The proto `axon/dataaccess/v1.TableReadResolution` `oneof` is the **wire**
contract; the in-process seam keeps the `{kind}` discriminated union and carries
the live SDK descriptor on the `descriptor` arm to avoid a lossy bigint/null
round-trip at the execution boundary. `isDescriptorResolution` /
`isOpenableResolution` type-guards gate the handoff.

## Context

E0 shipped a single `['catalog', connectionId, 'table-derived']` query key that
conflated navigation and read resolution. E1 needs DirectUnityCatalog (richly
browsable, server-paginated, brokered reads) to coexist with today's flat
reference sources without the explorer or run lifecycle branching on source type,
and without redesigning when the deferred Daxis provider lands. A single seam with
a capability-described navigation surface and a union-typed read resolution makes
new providers additive and lets the explorer degrade gracefully via
`ProviderCapabilities`.

## Policy

- All provider I/O goes through one session-aware fetch wrapper, `SessionHttp`
  ([http.ts](../../apps/axon-web/src/query/providers/http.ts)): always
  `credentials:'include'`, attaches a correlation id, maps `401 → session_expired`
  / `403 → blocked` / `404 → not_found` / `5xx → unavailable`, and **never**
  attaches an `Authorization` header from browser state. Providers never call
  `fetch` directly. Reference providers use `NO_HTTP`.
- **Single handoff**: only the `descriptor` (and, when implemented, processed
  `read_access_plan`) outcome may reach `openDeltaTable()`. `fallback` and
  `blocked` must never open an engine session — enforced at the explorer
  open/sample path ([open-table.ts](../../apps/axon-web/src/editor/explorer/open-table.ts)).
- **Fail closed**: any uncertainty (unknown policy state, resolver error,
  non-reachable storage, unsupported shape) resolves to `fallback`/`blocked` with
  a structured reason, never to a speculative open.
- **No secrets in the browser (ADR-0002 restated)**: connection locators carry
  only non-secret routing facts (`bffBaseUrl`, host, path, bucket). Persisted
  registry/navigation nodes contain no credentials, tokens, or signing material.
- Read resolutions are **never persisted** (grants expire); only navigation nodes
  and connection locators are durable.
- Provider `kind` dispatch lives solely in `providerForConnection`; adding a
  provider is additive there and requires no UI changes.

## Local development and tests

Envoy (E6) is a **deployment** concern only. Dev and integration tests run a real
OSS Unity Catalog server reached **same-origin** via the Vite dev proxy
(`/api/uc/* → local UC`); `SessionHttp` is unchanged. UC *wire* types are
generated from a vendored OpenAPI spec via `openapi-typescript` with a drift gate,
then adapted to `axon/catalog/v1`.

## Consequences

- The explorer and run lifecycle are provider-agnostic; DirectUC, Daxis, and the
  reference sources share one code path.
- The `read_access_plan` arm reserves the Daxis/brokered seam, so Daxis can be
  **deferred without redesign** (ADR-0008 is its profile).
- Navigation node shapes are generated, so contract drift is caught by codegen
  checks rather than at runtime.
- The descriptor-on-the-seam choice trades a small wire/in-process type divergence
  for a lossless, allocation-light execution handoff.

## Acceptance Standard

This ADR is implemented when:

- every data source is reached only through `CatalogProvider` /
  `providerForConnection`, with no kind-branching in UI or run lifecycle;
- `resolveTableRead` returns the four-arm union and `fallback`/`blocked` provably
  never reach `openDeltaTable()` (Vitest resolution matrix + explorer open-path
  tests);
- all provider HTTP flows through `SessionHttp` with the documented status mapping
  and no `Authorization` header from browser state;
- persisted registry/navigation nodes pass a secret-marker scan;
- UC wire codegen drift check is green.

## References

- [ADR-0002: Browser access uses signed HTTPS or proxy, never cloud secrets](ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
- [ADR-0008: Daxis browser read compute contract](ADR-0008-daxis-browser-read-compute-contract.md)
- [E1 — Pluggable Catalog Providers execution plan](../plans/2026-06-20-e1-catalog-providers-execution-plan.md)
- [E3A provider contract mappings](../program/e3a-provider-contract-mappings.md)
- `apps/axon-web/src/query/providers/` (seam, registry, providers, queries)
