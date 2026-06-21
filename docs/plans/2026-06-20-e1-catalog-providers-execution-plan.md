# E1 — Pluggable Catalog Providers and Catalog Explorer — Execution Plan

- Status: Draft (planning deliverable)
- Date: 2026-06-20
- Scope: Define the **discovery-only** `CatalogProvider` seam and ship a `DirectUnityCatalog` provider (UC REST navigation + table metadata) alongside the existing `LocalDelta` and `ObjectStore` paths. The seam stays Daxis-compatible by design, but a Daxis provider is **deferred** for now (see the Daxis deferral note below) and is not an E1 integration target. Each provider exposes its **navigation** reads as TanStack Query options so caching/refresh/dedup/pagination apply uniformly. Deliver a multi-catalog connection registry, catalog/schema/table selectors in sidebar + editor, lazy/paginated tree navigation, **volumes surfaced as catalog objects (list + metadata only)**, and a catalog metadata cache that feeds E2 (IntelliSense) and E4 (Cedar entities). **Read resolution (the `ReadAccessPlan`/descriptor/`fallback`/`blocked` family) and query execution / sample-preview are out of scope for E1 and belong to [E9 — Execution Provider and Data Access Resolution](../program/rich-lakehouse-workbench-strategy.md); the Catalog Explorer hands a table *ref* to E9, it does not resolve or execute reads.** **Browsing the files inside a volume, file preview, and file editing are out of scope for E1 and belong to [E8 — Workspace Files & Volumes](../program/rich-lakehouse-workbench-strategy.md); E1 only references a volume as an object, it never opens it.**
- Depends on: **E0** (state/TanStack Query, routing, persistence) and **E6** (Envoy session + WASM credential propagation) for remote API access.
- Feeds: **E2** (catalog-aware SQL), **E4** (policy entities), **E7** (table insight).
- Related:
  - [E0 — Frontend Foundation Execution Plan](./2026-06-20-e0-frontend-foundation-execution-plan.md)
  - [Rich Lakehouse Workbench — High-Level Strategy](../program/rich-lakehouse-workbench-strategy.md)
  - [Browser Unity Catalog Brokered Runtime Contract](../program/browser-uc-brokered-runtime-contract.md)
  - [Browser-Owned Descriptor Materialization](../program/browser-owned-descriptor-materialization.md)
  - [ADR-0002: No cloud secrets in the browser](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
  - [ADR-0008: Daxis Browser Read Compute Contract](../adr/ADR-0008-daxis-browser-read-compute-contract.md)

> E1 turns today's single posture ("Daxis owns catalog and policy; Axon is contract-first") into one profile of a pluggable `CatalogProvider` seam, and adds a peer "direct UC REST" profile. It builds directly on the E0 TanStack Query layer (`queryOptions` provider boundary, `queryKeys` factory, infinite queries, `connectionId` cache isolation) and depends on the E6 Envoy session for any authenticated remote read. ADR-0002 is a hard constraint at every milestone: **no cloud or catalog secrets ever reach browser code.**

> **Scope realignment (2026-06-21) — E1 is discovery-only; read resolution + execution moved to [E9](../program/rich-lakehouse-workbench-strategy.md).** The original plan put `resolveTableRead()` on the `CatalogProvider`, extended `QueryTableSource`, and wired the run lifecycle through the catalog seam. That conflated three concerns — *discovery* (what exists), *read resolution* (the `ReadAccessPlan`/descriptor/`fallback`/`blocked` family), and *execution* (run/sample). E1 now owns **discovery + the Catalog Explorer only**; the **DataAccessResolver** seam (read resolution) and the **ExecutionProvider** seam (run/preview) are owned by **E9**. Where sections below still describe `resolveTableRead`/`TableReadResolution`/the `resolved_descriptor` source variant/the read-resolution matrix, that material is **relocated to E9** and is retained here only as a cross-reference; the heading is annotated where this applies. The CatalogProvider's node/metadata types are the **[E3A](../program/rich-lakehouse-workbench-strategy.md)-generated proto messages**, not hand-written TS. Milestone **M3** (read resolution → execution) and the deferred Daxis **M4** move to E9.

> **Daxis deferral (2026-06-20):** the Daxis provider is **parked** for E1. The `CatalogProvider` discovery seam, the E9 read-resolution seam (incl. the Daxis `read_access_plan` profile), and the ADR strategy below intentionally keep Daxis as a *first-class future profile* — that design is **not** removed — but E1 does **not** build, ship, or depend on a Daxis integration. The integrated providers for E1 are **DirectUnityCatalog**, **LocalDelta**, and **ObjectStore**. All Daxis-specific work (notably milestone **M4** and its fixtures/tests) is retained below, marked _Deferred_, and can be reactivated without reworking the seam.

> **Buf toolchain note (pulled forward by E0):** the Buf seam that E1/E3 rely on is already standing in E0 via the scoped `axon/config/v1` config module (`buf.yaml`/`buf.gen.yaml` → protobuf-es + `protoschema-jsonschema`, checked into `src/generated/config/` behind the codegen drift check; see [E0 plan §3.6](./2026-06-20-e0-frontend-foundation-execution-plan.md)). E1 should reuse that module/`buf.gen.yaml` rather than re-bootstrapping Buf, and **provider options / connection config should be modeled as `axon/config/v1` messages** so they flow into the same schema-driven, layered Settings surface (per-connection scope is the reserved D7 layer).

---

## 1. Executive summary — recommended decisions

| # | Decision | Recommendation |
|---|----------|----------------|
| 1 | `CatalogProvider` interface shape | **Discovery-only: each provider exposes _navigation_ (browse catalogs/schemas/tables/views/volumes/functions/models/columns, each surfaced as a TanStack `queryOptions`/`infiniteQueryOptions` factory) over the [E3A](../program/rich-lakehouse-workbench-strategy.md)-generated message types.** No `resolveTableRead`/read-resolution on this seam — that is the **DataAccessResolver** seam in [E9](../program/rich-lakehouse-workbench-strategy.md). Daxis and DirectUC implement the same discovery interface and differ only in navigation richness (expressed via `capabilities()`). Selecting an object yields a `TableRef` that the Explorer hands to E9 for "Sample data" / "Open in SQL editor"; the descriptor-materialization invariant and the `descriptor`/`read_access_plan`/`fallback`/`blocked` union live with E9. |
| 2 | Where UC credential exchange / CORS lives | **Never in the browser.** "DirectUnityCatalog" means "UC REST _shape_," reached **same-origin through the E6 Envoy proxy** (e.g. `GET /api/uc/2.1/unity-catalog/catalogs`) with `credentials: 'include'`. Envoy terminates login and injects the UC/cloud credential server-side. This sidesteps CORS (single origin) and satisfies ADR-0002. Object/volume bytes are read via short-lived signed URLs or a narrow range proxy (the existing object-store/object-grant path), never cloud creds. E1 defines the fetcher contract; E6 owns the session/refresh/401 mechanics. |
| 3 | Multi-catalog data model | **A `CatalogConnection` registry (E0 client store, persisted) keyed by a stable `connectionId`.** A connection is provider-root-scoped (a UC workspace, a Daxis tenant, a bucket, a local folder), distinct from the *table-scoped* canonical key in `connect/store.ts`. Hierarchy: `connection → catalog → schema → {table|view|volume}`. Active selection `{connectionId, catalog, schema, table}` is **route-owned** (E0 deep link), mirrored into the store. Selection yields a `TableRef` handed to the [E9](../program/rich-lakehouse-workbench-strategy.md) ExecutionProvider (which calls the DataAccessResolver → descriptor → the existing `getSession`/`openDeltaTable` runtime). |
| 4 | Volumes scope | **Discovery only.** E1 lists volumes and shows volume metadata (type, storage location, comment) as catalog objects in the tree (UC `/volumes`, `/volumes/{name}`), exactly like tables/views. **Browsing the files inside a volume, file preview, and file editing are deferred to [E8 — Workspace Files & Volumes](../program/rich-lakehouse-workbench-strategy.md)** (the `FileSystemProvider`/Workspace seam). E1 exposes a volume only as a reference + a handoff point ("Browse files" routes to E8 when present); it ships no Volumes Files API and no preview parsers. |
| 5 | ADR strategy | **Add a new ADR-0009 "Pluggable Catalog Providers"** that defines the seam and explicitly scopes ADR-0008 + the UC brokered contract as the **Daxis/brokered profile**, with a peer **direct-UC profile**. Add a short forward-reference amendment note to ADR-0008 (do not rewrite it — its content is load-bearing for the Daxis release-gate traceability). ADR-0002 is unchanged and remains a hard constraint cited by ADR-0009. |
| 6 | UC contract + navigation patterns (prior art) | **Reuse the OSS Unity Catalog OpenAPI spec and `openapi-typescript`-generated types as the typed UC contract**, and **port the navigation-plane conventions** proven in the sibling `open-lakehouse/node` console (see §2.5) — *not* its access model or styling. Generate UC `components` types from the vendored spec; keep axon's **hand-written `queryOptions`/provider seam + BFF `SessionHttp`** (do **not** adopt `openapi-fetch`/`openapi-react-query`, since axon's metadata plane is governed and same-origin-brokered, unlike OSS UC). Treat **Tables, Volumes, Functions, and Models** as first-class explorer object kinds. |

Cross-cutting: E1 owns no new client-state library and no routing of its own — it consumes E0's seams. It also **does not implement the Envoy proxy or the UC BFF** (E6 / service-owned); it defines and consumes their contracts and ships a **mock broker / mock UC** for tests and local dev, mirroring the existing `daxis-first-class-integration-examples` and `browser-lakehouse-release-handoff-examples` fixtures.

Non-goals for E1: Monaco/IntelliSense (E2), Cedar authorization (E4 — E1 only *populates* entity-shaped metadata), Arrow-native grid (E5), protobuf contracts (E3), and the Envoy/login service itself (E6).

---

## 2. Current-state assessment (grounding)

### 2.1 What exists today

| Area | File(s) / symbols | Reality |
|---|---|---|
| Source model | `connect/data.ts` `SourceId = 'local' \| 'object_store' \| 'unity_catalog' \| 'delta_share'`; `SOURCES` | Four source cards. UC + Delta Sharing gated behind `connectorFeatures.bffAuthServiceConnectors` (`requiresBffAuthService`). |
| Connect flow | `connect/ConnectModal.tsx` | 3-step modal. **Local** and **object_store (GCS)** execute end-to-end (real `Test connection`). **UC** and **Delta Sharing** config panels are present but the test result is **hard-coded sample text**; `discoveryForSource()` returns `null` for them. |
| Multi-catalog model (cosmetic) | `connect/types.ts` `ConnectedCatalog`/`ConnectedCatalogSchema`/`ConnectedTableSourceBinding`; `connect/store.ts` | A real multi-catalog/multi-schema/multi-table structure **already exists** and is persisted in `localStorage` (`axon.connect.catalogs.v1`), with upsert/merge/dedupe and a table-scoped `canonicalKey` (`tableSourceKeyFromParts`). But discovery is faked for UC/DS, so it's mostly populated for local/object-store. |
| Runtime collapse | `services/query-source.ts` `querySourceFromConnectedCatalogs`, `QueryTableSource` | The rich `ConnectedCatalog` tree is **collapsed to a single `QueryTableSource`** (`manifest \| local_delta \| object_store_table_root`) for execution. This is the "single-table Phase 1" runtime catalog. |
| Runtime catalog | `services/catalog.ts` `loadCatalog`/`subscribeCatalog`/`snapshotCatalog`; `services/query.ts` `getSession`/`deriveCatalogTable` | A `Catalog` is **derived from the resolved Delta snapshot** of the *one* active table (`tables: [table]`). This is session-bound, not a provider listing. |
| Object-store read path (reusable) | `services/object-storage.ts` | Anonymous GCS XML-API `_delta_log/` listing → `resolve_delta_snapshot_from_manifest` (WASM) → `BrowserHttpSnapshotDescriptor` → `preflight_parquet_metadata_for_targets` range-read preflight. **This is the table-bytes read primitive [E9](../program/rich-lakehouse-workbench-strategy.md)'s read resolution reuses (and the same primitive E8 reuses for volume files); E1 does not touch it.** |
| WASM surface | `apps/axon-web/src/lib.rs` | `resolve_delta_snapshot_from_manifest`, `preflight_parquet_metadata_for_targets`, `SandboxQuerySession::inspect_parquet`, `open`/`dispose_table`. Range reads happen inside the worker via `wasm-http-object-store`. |
| Feature flag | `services/connector-features.ts` | `bffAuthServiceConnectors` from `VITE_AXON_BFF_AUTH_SERVICE_CONNECTORS`. Gates UC/DS. |

### 2.2 The contract surface E1 builds on (`crates/query-contract/src/lib.rs`)

- **`ReadAccessPlan`** (tagged `plan_type`): `BrokeredDelta(BrokeredDeltaReadPlan)` · `DeltaSharing(DeltaSharingReadPlan)` · `SqlFallbackRequired(SqlFallbackRequiredPlan)` · `Blocked(BlockedReadPlan)`. This is the catalog→browser read-authorization vocabulary and the natural target of [E9](../program/rich-lakehouse-workbench-strategy.md)'s DataAccessResolver (not an E1 surface).
- **`BrokeredDeltaReadPlan::to_browser_http_snapshot_descriptor(resolved_snapshot, object_urls_by_path)`** already converts a brokered plan + signed URLs into an openable descriptor, failing closed unless `direct_external_engine_read == Confirmed`, `batch_sign`, and `range_get` hold.
- **Descriptors**: `BrowserHttpSnapshotDescriptor` (the single execution handoff), `BrowserHttpParquetDatasetDescriptor`, `ResolvedSnapshotDescriptor`.
- **Resolver contract**: `DeltaLocationResolve{Request,Response}` + `validate_delta_location_resolve_exchange` — the explicit server-snapshot-resolver mode.
- **Object-grant routes**: `ObjectGrant{List,Head,BatchSign,Range}*` — list/head/batch-sign/range for `BrokeredObjectAccess` capabilities. **[E9](../program/rich-lakehouse-workbench-strategy.md) uses these for table-bytes read resolution; they are also the primitives E8 will reuse for volume file access. E1 uses none of them.**
- **Daxis**: `DaxisApprovedAxonReadDescriptor` (carries `tables: Vec<DaxisApprovedTableDescriptor>` each with an embedded `BrowserHttpSnapshotDescriptor`, plus `access_proof`, `limits`, `runtime_preference`) and `validate_daxis_approved_axon_read_descriptor`.
- **Reason taxonomy**: `ReadAccessPlanReason` (`row_filter`, `column_mask`, `view`, `unknown_policy_state`, `no_direct_external_engine_read_support`, `unsupported_table_type`, `grant_expired`, `storage_cors_blocked`, `broker_unavailable`) and `FallbackReason` — the structured outcomes the UI surfaces (and E4 maps onto).

### 2.3 Constraints observed in the repo

- **ADR-0002 / browser secret boundary**: browser code must never receive service-account JSON, long-lived/refresh tokens, broad cloud tokens, signing secrets, or bucket-traversal config. Only opaque grant IDs, short-lived object-scoped signed URLs, proxy URLs, expiry timestamps, non-secret capability flags, correlation IDs. Enforced by `verify_browser_dependency_guardrails.sh` (cloud-SDK denylist + secret-marker scan) and `contains_secret_material`/URL-policy validators in the contract.
- **Descriptor materialization invariants** (`browser-owned-descriptor-materialization.md`): BFF does not query or reconstruct by default; browser owns descriptor production from safe material; `openDeltaTable()` is the single handoff; `sql_fallback_required`/`blocked` must **not** call `openDeltaTable()`; unknown policy state must fall back or block (fail closed).
- **WASM artifact budget** (≤ 750 KB `browser_engine_worker.wasm`) is Rust-only; E1 is frontend-heavy + contract-light and does **not** touch this gate (E1 adds no preview parsers; any file-preview WASM helper is an E8 concern and must respect this budget there).
- **Single canonical execution handoff** stays `BrowserHttpSnapshotDescriptor → openDeltaTable()`. E1 must not introduce a parallel execution path.

### 2.4 The core gap

There is **no provider abstraction today** — local/object-store/manifest logic is inlined across `ConnectModal.tsx`, `object-storage.ts`, `query.ts`, and `query-source.ts`, and UC/DS are decorative stubs. The runtime is single-table. E1's job is to (a) introduce the seam, (b) re-express the working paths as reference providers, (c) ship DirectUC + a real Daxis provider, and (d) generalize the single-table runtime into a navigable multi-catalog tree — **without breaking the local/object-store paths that ship today.**

### 2.5 Prior art: the `open-lakehouse/node` UC console (reuse decisions)

A sibling repo (`open-lakehouse/node`) ships a small, mature Databricks-parallel console — a `ui/` app plus a generated `uc-client/` package — that has already solved the **UC metadata-navigation plane** we need. It overlaps with E1 on browse/paginate/tree/detail and diverges on the two things that define E1: it talks to **OSS UC directly** (no brokering, no governance, no secret boundary, no read resolution), and it uses **Tailwind/shadcn + `openapi-fetch`/`openapi-react-query`**, whereas axon uses design-token CSS + a hand-written TanStack Query layer. So we **reuse its contract and navigation patterns, not its access model or styling.**

**Reuse (port into axon's existing seams):**

- **UC REST contract via codegen.** Vendor the OSS UC OpenAPI spec (`uc-client/openapi/unity-catalog.yaml`) into axon and add an `openapi-typescript` codegen target (idiomatic here — axon already runs `buf`/config codegen via `scripts/`). Consume the generated `components` schemas (`CatalogInfo`/`SchemaInfo`/`TableInfo`/`ColumnInfo`/`VolumeInfo`/`FunctionInfo`/`RegisteredModelInfo`, list responses) as the **typed DirectUC contract**. Keep axon's hand-written `queryOptions`/provider seam and route UC through the BFF `SessionHttp` (§4.2/§4.4) — we **do not** adopt `openapi-fetch`/`openapi-react-query`, whose ungoverned `["get", path, init]` fetch model conflicts with axon's brokered, same-origin metadata plane.
- **Navigation-plane conventions** (from `ui/src/lib/uc/queries.ts` + `mutations.ts`), folded into axon's `query/keys.ts`/`query/tree.ts`/provider factories:
  - **Single source of truth for keys** — shared per-list `init`/param builders; reads, prefetch, seeding, and invalidation all reference one key (axon already does this with its `queryKeys` factory; extend it per provider list).
  - **Cursor infinite queries** — `page_token`/`next_page_token` paging (axon's `catalogTreeInfiniteQueryOptions` is a stub of exactly this).
  - **List → detail cache seeding** — list responses embed full objects; on success, seed each item's detail-cache key so drilling into a node is instant with no refetch.
  - **Prefetch-on-intent** — route-loader prefetch + row-hover prefetch, paired with router `defaultPreload: "intent"` and `createRootRouteWithContext<{ queryClient }>`.
  - **Predicate-based hierarchical invalidation** — match by predicate on the canonical key so one call invalidates all pages/params of a list; connection-scoped prefix invalidation on disconnect/resync (feeds §M6).
- **Explorer object model** — first-class **Tables / Volumes / Functions / Models** groups under each schema (axon's explorer is tables-only today), plus the `ListStates` (loading/empty/error/load-more) and per-kind detail panes (metadata + columns table) as a **design-token restyle/port**, not a Tailwind import.

**Explicitly not reused:** the direct, ungoverned UC fetch path (contradicts §4.1/ADR-0002 — axon's read-resolution/fallback/blocked has no OSS-UC equivalent); a Tailwind/shadcn/`lucide` migration (port visual patterns, keep design tokens + the existing icon set); local-component-state selection and the wide labeled sidebar nav (axon deliberately chose route-owned selection + the icon rail, §5.4). The `ServiceFrame` iframe-embed pattern (MLflow/marimo) is noted as prior art for a **future E7 "services" area**, out of E1 scope.

---

## 3. The `CatalogProvider` interface (Decision 1)

### 3.1 Design principle: navigation (E1) vs. read resolution (E9)

Two concerns with different lifecycles, caches, and trust properties — **split across two seams/efforts:**

- **Navigation (E1, this plan)** — "what catalogs/schemas/tables/volumes/columns exist, and what is their metadata?" Cheap, listable, paginated, cacheable for minutes, refetchable in the background. Surfaced as TanStack `queryOptions`. Feeds E2/E4 and the Catalog Explorer.
- **Read resolution ([E9](../program/rich-lakehouse-workbench-strategy.md), DataAccessResolver seam)** — "how, if at all, may this client read this specific table's bytes _right now_?" Policy-gated, short-lived, expiring, fail-closed. Returns the `ReadAccessPlan`/descriptor family. **Never cached like navigation** (grants expire; persisting them would violate ADR-0002 posture). Feeds the E9 ExecutionProvider's run lifecycle.

Conflating them (as the E0 plan's single `['catalog', connectionId, 'table-derived']` key did, and as the original E1 plan did by hanging `resolveTableRead` off the catalog seam) is the central thing this realignment fixes: E1 ships navigation only; E9 owns read resolution + execution.

### 3.2 Types (sketch)

> **Amendment (E3A contract adoption, 2026-06-21).** The node shapes below are
> now **generated**, not hand-written. `CatalogNode/SchemaNode/TableNode/`
> `ColumnNode/ViewNode/VolumeNode/FunctionNode/ModelNode/TableMetadata/`
> `ProviderCapabilities/TableRef/VolumeRef` and the `ProviderKind/ReadAuthority/`
> `TableType/VolumeType` enums come from `axon/catalog/v1`
> ([catalog_pb.ts](../../apps/axon-web/src/generated/proto/axon/catalog/v1/catalog_pb.ts));
> `ReadAccessPlan`, `BrowserHttpSnapshotDescriptor`, and `ReadAccessPlanReason`
> come from `axon/dataaccess/v1`
> ([dataaccess_pb.ts](../../apps/axon-web/src/generated/proto/axon/dataaccess/v1/dataaccess_pb.ts)).
> Only seam/infra types stay hand-written and live in
> [`query/providers/types.ts`](../../apps/axon-web/src/query/providers/types.ts):
> `CatalogConnection`/`ProviderLocator`/`ConnectionId`, the `Page<T>` cursor
> wrapper, the `ProviderKindToken` ⇄ proto-enum mapping, and the
> `CatalogProvider` interface itself. The `TableReadResolution` `{kind}` sketch
> below is kept as the **in-process seam** type: the `descriptor` arm carries the
> live `BrowserHttpSnapshotDescriptor` directly to avoid a lossy bigint/null
> round-trip at the `openDeltaTable()` boundary, while the proto
> `dataaccess/v1.TableReadResolution` `oneof` (`outcome.case`) remains the wire
> contract. `isDescriptorResolution`/`isOpenableResolution` type-guards provide
> ergonomics over the union.

```ts
// query/providers/types.ts
export type ProviderKind = 'direct_uc' | 'daxis' | 'local_delta' | 'object_store';

export type ConnectionId = string; // stable, provider-root-scoped (see §5.1)

export interface CatalogConnection {
  id: ConnectionId;
  kind: ProviderKind;
  label: string;                 // user alias ("workspace")
  locator: ProviderLocator;      // host / bffBaseUrl / profile / bucket — NEVER secrets
  createdAt: string;
}

export interface ProviderCapabilities {
  browsable: boolean;            // can list catalogs/schemas/tables (false → flat single-table, e.g. a pinned manifest)
  paginated: boolean;            // server cursors available
  supportsViews: boolean;
  supportsVolumes: boolean;
  supportsColumnMetadata: boolean;
  authority: 'browser' | 'brokered' | 'daxis'; // who decides read access
}

// Node shapes are intentionally minimal + serializable (persistable, feed E2/E4).
export interface CatalogNode  { name: string; comment?: string; }
export interface SchemaNode   { catalog: string; name: string; comment?: string; }
export interface TableNode    { catalog: string; schema: string; name: string;
                                type: 'table' | 'view' | 'materialized_view' | 'streaming_table';
                                comment?: string; }
export interface ColumnNode   { name: string; type: string; nullable?: boolean;
                                partition?: boolean; comment?: string; }
export interface TableMetadata {
  ref: TableRef;
  columns: ColumnNode[];
  partitionColumns: string[];
  properties: Record<string, string>;
  rowCount?: number; sizeBytes?: number; fileCount?: number;
  latestSnapshotVersion?: number;
  protocol?: { minReaderVersion: number; minWriterVersion: number; features: string[] };
}
// Volumes are discovery-only objects in E1 (list + metadata, like tables/views).
// Browsing the files inside a volume is E8 (FileSystemProvider/Workspace).
export interface VolumeNode   { catalog: string; schema: string; name: string;
                                volumeType: 'managed' | 'external';
                                storageLocation?: string; comment?: string; }

export type TableRef  = { connectionId: ConnectionId; catalog: string; schema: string; table: string };
export type VolumeRef = { connectionId: ConnectionId; catalog: string; schema: string; volume: string };

// ── Read resolution moved to E9 (DataAccessResolver seam) ──────────────────
// The `TableReadResolution` union (descriptor / read_access_plan / fallback /
// blocked) and `resolveTableRead()` are NOT part of the E1 CatalogProvider.
// They are defined as E3A proto messages and owned by E9. The Explorer hands a
// `TableRef` (above) to E9; it never resolves reads itself. (Volume-file read
// resolution is E8.)
```

### 3.3 The provider interface

> **Amendment (E3A contract adoption, 2026-06-21).** The shipped interface in
> [`types.ts`](../../apps/axon-web/src/query/providers/types.ts) exposes the
> navigation methods as plain async paginators returning `Page<T>`
> (`listCatalogs/listSchemas/listTables/getTableMetadata/listVolumes/`
> `getVolumeMetadata/listFunctions/listModels`) rather than returning
> `queryOptions` directly; the TanStack `queryOptions`/`infiniteQueryOptions`
> that wrap them (cursor paging, list→detail seeding, prefetch-on-intent,
> predicate invalidation) live one layer up in
> [`query/providers/queries.ts`](../../apps/axon-web/src/query/providers/queries.ts).
> This keeps the provider seam free of React-Query types and lets non-UI callers
> (run lifecycle, tests) consume it directly. `resolveTableRead` is unchanged.

```ts
export interface CatalogProvider {
  readonly connection: CatalogConnection;
  capabilities(): ProviderCapabilities;

  // ── Navigation (TanStack queryOptions; keys come from the E0 queryKeys factory) ──
  catalogsQuery(): UseQueryOptions<CatalogNode[]>;
  schemasQuery(catalog: string): UseInfiniteQueryOptions<Page<SchemaNode>>;
  tablesQuery(catalog: string, schema: string): UseInfiniteQueryOptions<Page<TableNode>>;
  tableMetadataQuery(ref: TableRef): UseQueryOptions<TableMetadata>;
  volumesQuery(catalog: string, schema: string): UseQueryOptions<VolumeNode[]>;     // discovery only
  volumeMetadataQuery(ref: VolumeRef): UseQueryOptions<VolumeNode>;                  // discovery only
  functionsQuery(catalog: string, schema: string): UseQueryOptions<FunctionNode[]>; // discovery only
  modelsQuery(catalog: string, schema: string): UseQueryOptions<ModelNode[]>;       // discovery only

  // ── No read resolution here ──
  // resolveTableRead() / TableReadResolution belong to E9's DataAccessResolver
  // seam, not the CatalogProvider. The Explorer hands a TableRef to E9.
  // Volume-file listing/reading is E8.
}

export interface Page<T> { items: T[]; nextCursor?: string; }
```

All fetchers go through one **session-aware fetch wrapper** (`query/providers/http.ts`) that always uses `credentials: 'include'`, attaches a correlation id, maps `401 → SessionExpired` (E6 re-login), `403 → blocked`, `404 → not-found`, and **refuses to attach Authorization headers from browser state** (guardrail: there is no token in browser state to attach). Providers never call `fetch` directly.

### 3.4 How each provider maps onto the interface

| Provider | `browsable` | Navigation source (E1 discovery) | Read resolution (→ E9) |
|---|---|---|---|
| **DirectUnityCatalog** | yes | UC REST via Envoy proxy: `/catalogs`, `/schemas`, `/tables`, `/tables/{full_name}` (columns/props), `/volumes`, `/volumes/{name}` (volume list + metadata only; the Volumes **Files** API is E8), `/functions`, `/models` | brokered UC profile of the E9 DataAccessResolver |
| **Daxis** _(deferred — see deferral note)_ | partial | Daxis catalog/list surfaces (thin; may be navigation-light — list approved/recent tables). Tree nodes can be lazily backed by Daxis list APIs where available. | Daxis profile of the E9 DataAccessResolver |
| **ObjectStore** (GCS) | flat (one table root = one table) | Synthesizes a single-catalog/single-schema/single-table tree from the table URI (today's `objectStorageRuntimeFromDescriptor`). Bucket-prefix browsing is an E8 `ObjectStorePrefix` FileSystemProvider concern, not E1. | ObjectStore profile of the E9 DataAccessResolver |
| **LocalDelta** | flat | Single table from the File System Access handle + reconstructed snapshot. | LocalDelta profile of the E9 DataAccessResolver |

The "Read resolution" column is shown only to locate each provider's E9 counterpart; **none of it ships in E1.** Navigation differences (UC is richly browsable; object-store/local are flatter) are expressed through `capabilities()` and degrade gracefully in the explorer UI. The DataAccessResolver's generality — `descriptor` for local/object-store/Daxis-approved, `read_access_plan` for brokered UC, `fallback`/`blocked` for governed shapes — is why Daxis can be **deferred without redesign**; see [E9](../program/rich-lakehouse-workbench-strategy.md).

### 3.5 Provider registry + dispatch

```ts
// query/providers/registry.ts
export function providerForConnection(conn: CatalogConnection, http: SessionHttp): CatalogProvider;
```

A pure factory keyed by `conn.kind`. The explorer and run lifecycle never branch on `kind`; they call interface methods. New providers (a future `RemoteService`/`Tauri` catalog) are additive.

---

## 4. Credential exchange, CORS, and the E6 seam (Decision 2)

### 4.1 The rule

ADR-0002: the browser may read cloud data only via (1) short-lived object-scoped signed HTTPS URLs or (2) a narrow read-proxy. It must never hold service-account JSON, refresh/broad tokens, or bucket-traversal config. **E1 owns only the metadata plane (UC REST, §4.2).** The data plane (table bytes for query execution) is **[E9](../program/rich-lakehouse-workbench-strategy.md)**'s DataAccessResolver/ExecutionProvider concern, and volume *file* bytes are **E8**; both reuse the same rule and the same object-grant path. §4.3 below is retained as a cross-reference to E9.

### 4.2 Metadata plane — UC REST through Envoy

> **Amendment (local UC via Vite proxy; Envoy is deployment-only, 2026-06-21).**
> Envoy is a **deployment** concern, not a dev/test prerequisite. For development
> and integration tests we run a real OSS Unity Catalog server (docker/binary —
> see [`dev/unity-catalog/`](../../apps/axon-web/dev/unity-catalog/) and the
> `uc-up`/`uc-down` `just` recipes) and reach it **same-origin** through Vite
> `server.proxy` (`/api/uc/* → http://localhost:8080/api/2.1/unity-catalog/*` in
> [vite.config.ts](../../apps/axon-web/vite.config.ts)). `SessionHttp` keeps its
> `credentials:'include'` + correlation-id + 401/403/404 contract unchanged; the
> dev proxy stands in for Envoy. OSS UC injects no secrets, so ADR-0002 holds
> trivially on the metadata plane. The UC *wire* types are generated from a
> vendored [`openapi/unity-catalog.yaml`](../../apps/axon-web/openapi/unity-catalog.yaml)
> via `openapi-typescript` (`npm run codegen:uc` + `codegen:uc:check` drift gate)
> and mapped to `axon/catalog/v1` by
> [`uc-adapters.ts`](../../apps/axon-web/src/query/providers/uc-adapters.ts).
> This supersedes the "Envoy required" framing below for dev/test only.

"DirectUnityCatalog" is **not** "browser → Databricks with a PAT." It is:

```
browser (same-origin, credentials:'include')
  → Envoy proxy (E6: terminates login session; injects UC/cloud creds server-side)
    → Unity Catalog REST (/api/2.1/unity-catalog/...)
```

Consequences:
- **No CORS problem**: all metadata calls are same-origin to the Envoy that also serves the bundle. (Cross-origin UC + `COEP: require-corp` + credentialed fetch is exactly the tension E6 calls out; single-origin Envoy is the sanctioned reconciliation.)
- **No secret in browser**: the session cookie is opaque/`HttpOnly`; the UC token lives behind Envoy.
- E1 fetchers target a configurable `bffBaseUrl` (per connection locator, default `/api/uc`). The `uc_bff_url` field already in `ConnectForm` becomes this base.

### 4.3 Data plane — table bytes (moved to [E9](../program/rich-lakehouse-workbench-strategy.md))

The table-bytes read path (object-grant routes → signed URLs / proxy-range → `BrowserHttpFileDescriptor` → the worker's range-read path → `openDeltaTable()`) is the **DataAccessResolver + ExecutionProvider** concern owned by E9, not E1. It is summarized here only to show the boundary; E1 ships none of it. (Public object-store GCS reads already work today via the existing object-store path; E9 wraps that path behind the resolver/executor seams. Worker-credential mechanics remain owned by E6.)

### 4.4 What E1 owns vs. what E6 owns

| Concern | Owner |
|---|---|
| `SessionHttp` wrapper contract (`credentials:'include'`, correlation id, 401/403/404 mapping) | **E1 defines, E6 implements the session/refresh behind it** |
| Login, cookie issuance, token storage, UC credential injection, Envoy config, COOP/COEP headers | **E6** |
| Worker `fetch` credential propagation for range reads | **E6** |
| Provider **navigation** fetchers + queryOptions | **E1** |
| Read-resolution dispatch + descriptor validation | **E9** |
| Mock UC (navigation) fixtures | **E1**; mock broker (read resolution) is **E9** |

**Sequencing**: E1 builds navigation against a **mock UC** (no real session) while E6 lands in parallel; E9 builds read resolution against a mock broker on the same `SessionHttp` seam. The `SessionHttp` contract is the integration point; E1 ships the interface + navigation mock, E6 fills the real session. The feature flag (`bffAuthServiceConnectors`) keeps DirectUC/Daxis dark in production until E6 is ready.

---

## 5. Multi-catalog data model and selector flow (Decision 3)

### 5.1 `connectionId` — connection-scoped, not table-scoped

Today's `tableSourceKeyFromParts` produces a **table-scoped** canonical key (includes schema + table). E1 needs a **connection-root** id (a UC workspace, a Daxis tenant, a bucket root, a local folder) so the whole `['catalog', connectionId, …]` subtree shares one cache and prefix-invalidates on disconnect.

Recommendation: introduce `connectionId = stableHash(kind, normalizedLocator)` in the `connections` store slice (E0). Keep `tableSourceKeyFromParts` for *table identity within* a connection (the deepest queryKey segment + run dedup). This resolves **E0 open question #1** in favor of a dedicated connection id, with the canonical key retained for table-level identity.

### 5.2 Registry

The existing `ConnectedCatalog`/`connect/store.ts` model is **kept and promoted** into the E0 `connections` client-store slice (persisted), generalized so:
- A connection carries `kind`, `label`, `locator`, `capabilities` (cached from `provider.capabilities()`), and a **scope** (selected catalogs/schemas filters — the existing `SchemaSelection` selection model extends to a catalog level).
- The **persisted** part is non-sensitive structure only (locators, labels, selection). Live navigation nodes come from TanStack Query (persisted per the E0 filter rules); read resolutions are never persisted.

### 5.3 Selection → a table ref handed to E9

E1's responsibility ends at producing a `TableRef`; resolution + execution are [E9](../program/rich-lakehouse-workbench-strategy.md):

```
route /catalog/$connectionId/$catalog/$schema/$table   (E0 deep link; source of truth)
  → connections slice mirrors active TableRef
  → Explorer "Sample data" / "Open in SQL editor"  ─hands TableRef→  E9 ExecutionProvider
                                                                       (E9 calls DataAccessResolver, runs/samples)
```

The `resolveTableRead` dispatch, the `QueryTableSource` `resolved_descriptor` variant, and the `descriptor → openDeltaTable()` handoff are **E9** concerns (E9 extends `getSession`/`QueryTableSource` and keeps the existing `manifest`/`local_delta`/`object_store_table_root` variants byte-identical). E1 does not touch the run lifecycle.

### 5.4 Navigation shell + Catalog Explorer

E1 introduces a **top-level navigation layer** so the app stops being a single query editor and becomes a multi-area workbench, mirroring the Databricks Catalog Explorer. The decisions below are load-bearing for M1 and are consumed by M2/M3/M5.

- **Top-level nav = a global left icon rail (Databricks-style).** A new full-height outermost column (`.app-rail`) switches between top-level *areas*: `Editor` (today's workspace) and `Catalog Explorer`. The rail is extensible (future: Settings, E7 Insights). Chosen over topbar tabs because it matches Databricks and scales to more areas. The brand mark moves into the rail head; the topbar starts at the connection pill.
- **Route-driven, not view-state.** Active area derives from the URL (consistent with E0's "route is source of truth"): `/` = Editor, `/explore` = Catalog Explorer, deep link `/explore/$connectionId/$catalog/$schema/$table` for a selected object. The existing `/catalog/...` deep link (which selects a table *in the editor*) is unchanged; the explorer owns the separate `/explore/...` namespace. `/catalogs` redirects to `/explore`. No new client state for "which area".
- **Shared shell chrome via a pathless layout route.** A `ShellLayout` (rail + topbar + statusbar + global modals/toast) renders an `<Outlet/>`; the Editor workspace and the Explorer are sibling area-routes that each fill only the middle region. Today's monolithic `App.tsx` is split into a chrome-free `EditorArea` and the shared `ShellLayout`. Topbar/statusbar read global state directly (connections slice + `useCatalogQuery`), so no prop drilling; the capability matrix is lifted from `App` local state into the `run` slice so both the topbar popover and the Results pane consume one source.
- **Explorer surface mirrors Databricks Catalog Explorer**: a left **object tree** (connections → catalogs → schemas → grouped **Tables / Volumes / Functions / Models** (per §2.5 prior art); flat providers degrade via `capabilities().browsable`) backed by the provider navigation queries with cursor-paged "load more" and list→detail cache seeding (lazy/infinite in M2; the existing connected-catalog registry in M1), plus a main **detail surface** with a header (breadcrumb, full name, object type, primary actions) and tabs: **Overview/Columns**, **Sample data**, **Details** (properties / storage / snapshot / protocol features), and reserved placeholders for History/Lineage/Permissions (E7/E4). The `ConnectedCatalogsPanel` graduates into this tree (its `onActivate` already produces an `ActiveConnectedTableRef`). Primary action **"Open in SQL editor"** creates a tab, sets the active selection, and navigates to `/`. Both the **Sample data** tab and **"Open in SQL editor"** hand the selected `TableRef` to the **[E9](../program/rich-lakehouse-workbench-strategy.md) ExecutionProvider** (Sample data → `preview(ref)`; the editor run → `execute(sql, ref)`); E1 renders the surface and produces the ref but performs no read resolution or execution. Until E9 lands, these actions degrade to the existing single-table run path. **Volumes (and Functions/Models) are reference objects in E1**: selecting a volume shows its metadata (type, storage location, comment) only — there is no inline file listing or preview. When E8 is present, the volume detail surface offers a **"Browse files"** action that hands off to the E8 Files experience; until then it is metadata-only.
- **Editor**: per-tab connection/catalog context (E2 will consume this for completion scoping; E1 stores it on the tab in the E0 `tabs` slice). The catalog/schema/table selectors are a header control bound to the active route.
- **Catalog metadata cache for E2/E4**: `tableMetadataQuery`/`tablesQuery`/`schemasQuery` results under stable keys ARE the cache E2 reads for completion and E4 reads for entity shapes — no separate store. E1 guarantees the node shapes in §3.2 are stable and serializable.

---

## 6. Volumes — discovery only (Decision 4)

E1 treats volumes exactly like tables/views in the catalog: they are **listed and described**, not opened.

- **In scope (E1):** `volumesQuery` (list volumes in a schema) and `volumeMetadataQuery` (type, storage location, comment) via UC `/volumes` and `/volumes/{name}`. Volumes render as objects in the explorer tree with a metadata detail pane (§5.4). Persisted volume metadata is non-sensitive only and passes the secret-marker scan.
- **Out of scope (→ [E8 — Workspace Files & Volumes](../program/rich-lakehouse-workbench-strategy.md)):** browsing the files *inside* a volume (the Volumes Files API / directory listing), file preview (parquet/csv/json/image), and file write/edit. E8 owns the `FileSystemProvider`/Workspace seam and reuses E1's object-grant/range-read primitives (`ObjectGrant{List,Head,BatchSign,Range}`, `validate_browser_object_url`, `inspect_parquet`/`preflight_parquet_metadata_for_targets`) for that work; none of it ships in E1. The volume detail surface exposes a **"Browse files"** handoff to E8 when present.

---

## 7. ADR strategy (Decision 5)

> **Numbering update (2026-06-21).** Upstream `main` landed
> [ADR-0009 "Axon Is The Lakehouse Workbench"](../adr/ADR-0009-axon-is-the-lakehouse-workbench.md)
> while this work was in flight, so the provider-seam ADR shipped as
> [**ADR-0010 "Pluggable Catalog Providers And A Unified Table Read Resolution
> Seam"**](../adr/ADR-0010-pluggable-catalog-providers.md). Note ADR-0010 as
> landed covers *both* the discovery seam and the read-resolution union (rather
> than the discovery-only ADR + separate execution ADR sketched below); the
> future E9 execution/read-resolution ADR therefore takes the next free number.
> Read "ADR-0009 (discovery)" → ADR-0010 and "ADR-0010 (execution)" → a later E9
> ADR throughout the rest of this section.

**Add ADR-0009 "Pluggable Catalog Providers (discovery)"** (new file) that:
- Establishes the **discovery-only** `CatalogProvider` seam (navigation queryOptions over the E3A node/metadata messages) as the contract.
- Declares the **Daxis** and **direct-UC** catalog-listing profiles as peer implementations of the discovery seam.
- Restates ADR-0002 as a hard constraint binding every provider's metadata plane (no secrets; same-origin metadata plane).
- Defines the **discovery → E9 handoff** (the Explorer produces a `TableRef`; read resolution and execution are the E9 seams).

> Read resolution and execution invariants (the `TableReadResolution` union, the fail-closed rule, the single-handoff `descriptor → openDeltaTable()` rule, and the signed-URL/proxy data plane) move to **ADR-0010** owned by [E9](../program/rich-lakehouse-workbench-strategy.md). ADR-0009 is scoped to discovery only.

**Amend ADR-0008** with a one-paragraph forward note: "Under the pluggable provider model, this ADR describes the Daxis profile specifically (catalog listing under ADR-0009; read resolution under ADR-0010); it remains authoritative for that profile." Do **not** rewrite ADR-0008 — its text is referenced by the Daxis release-gate traceability/proof packets, and broad edits would churn those gates.

Rationale for new-ADR-over-amend: ADR-0008 is narrowly Daxis-scoped and tied to external proof artifacts; new ADRs cleanly own the generalization (ADR-0009 discovery, ADR-0010 execution/read resolution) without destabilizing the Daxis evidence chain, and give E4 (which also reconciles ADR-0008) clear seam docs to reference.

---

## 8. Milestones (strangler — app ships at every step)

Each milestone is independently shippable behind flags and gated on: `tsc --noEmit`, ESLint (`--max-warnings=0`), existing Playwright suites (`editor-smoke`, `local-delta`, `public-gcs-live`) green, plus new Vitest. The local/object-store paths must stay byte-identical until explicitly migrated.

### M0 — Discovery seam + reference providers (no behavior change)
- Add `query/providers/{types,registry,http}.ts` (the discovery interface §3, `SessionHttp` contract §4.4 with a passthrough impl for public/local).
- Re-express **LocalDelta**, **ObjectStore**, and **manifest/fixture** as discovery-only `CatalogProvider`s whose navigation synthesizes the current flat tree over the E3A node messages. The existing run path (`query-source.ts`'s `querySourceFromConnectedCatalogs` → `getSession`) is **left untouched** in E1 — wiring it behind the E9 ExecutionProvider/DataAccessResolver (and the `resolved_descriptor` `QueryTableSource` variant) is an E9 milestone, not E1.
- **Gate:** `public-gcs-live` + `local-delta` + `editor-smoke` green (proving no regression); Vitest for the reference providers' navigation + registry dispatch.

### M1 — Multi-catalog registry + selectors + deep links + navigation shell
- Promote `ConnectedCatalog` into the E0 `connections` slice with connection-scoped `connectionId` (§5.1); generalize selection to catalog level. Migrate `connect/store.ts` localStorage onto the E0 persistence KV behind unchanged signatures (coordinate with E0 M1/M2).
- **Top-level navigation shell (§5.4):** add the global left rail; split `App.tsx` into a chrome-free `EditorArea` + a shared `ShellLayout` (rail + topbar + statusbar + global modals) rendered by a pathless layout route; lift the capability matrix into the `run` slice. Restructure `.shell` CSS into a rail column + content column.
- **Catalog Explorer surface (§5.4):** add `/explore` (+ `/explore/$connectionId/$catalog/$schema/$table`) area routes (and redirect `/catalogs` → `/explore`); build the object tree + the Databricks-style detail surface (Overview/Columns, Details) on the existing connected-catalog registry, with an "Open in SQL editor" round-trip. `ConnectedCatalogsPanel` graduates into the tree.
- Lazy catalog tree in the sidebar (infinite queries) + route-driven selection consuming E0's router.
- Per-tab connection/catalog context on the `tabs` slice.
- **Gate:** Playwright deep-link + selection tests + rail area-switch + `/explore` deep-link + "Open in SQL editor" round-trip; Vitest for registry upsert/remove/dedupe (port `connect/store.ts` assertions) + selection reconciliation; existing suites green.

### M2 — DirectUnityCatalog navigation
- **Vendor the OSS UC OpenAPI spec + codegen (§2.5):** add `apps/axon-web/openapi/unity-catalog.yaml` and an `openapi-typescript` codegen target (`scripts/`), generating UC `components` types consumed by the DirectUC provider. Add a `codegen:check` gate so the generated types stay in sync.
- Implement `catalogsQuery`/`schemasQuery`/`tablesQuery`/`tableMetadataQuery`/`volumesQuery`/`functionsQuery`/`modelsQuery` against UC REST through `bffBaseUrl` (Envoy proxy), as hand-written `queryOptions`/`infiniteQueryOptions` over the BFF `SessionHttp` (not `openapi-fetch`), with cursor pagination (`page_token`/`next_page_token`) and `staleTime`/retry from E0. Adopt the §2.5 conventions: single key source per list, **list→detail seeding**, **prefetch-on-intent** (route loader + hover), **predicate-based hierarchical invalidation**. Build the **mock UC** fixture server for tests/dev.
- Switch the M1 explorer tree + Overview/Columns/Details from the connected-catalog registry to live provider navigation queries (lazy/paginated), with **Tables/Volumes/Functions/Models** groups; degrade gracefully via `capabilities()` for flat providers (local/object-store). **Volumes, Functions, and Models are discovery/reference objects only** — list + metadata detail, no browser byte-read path. (Browsing a volume's files is E8; see §6.)
- Replace the UC config stub in `ConnectModal.tsx` with a real `Test connection` (calls `catalogsQuery`) and real discovery; keep behind `bffAuthServiceConnectors`.
- **Gate:** Vitest against mock UC (listing, pagination cursors, metadata shape, list→detail seeding, predicate invalidation, 401→session-expired, 403→blocked); `openapi-typescript` codegen-check green; Playwright UC-connect-and-browse against mock; secret-marker assertion on persisted nodes.

### M3 — DirectUnityCatalog read resolution → execution *(Moved out of E1 → [E9](../program/rich-lakehouse-workbench-strategy.md))*

> **Removed from E1 scope.** `resolveTableRead` (brokered object-grant → `BrokeredDeltaReadPlan` → `to_browser_http_snapshot_descriptor` → `descriptor` → `openDeltaTable()`; governed shapes → `fallback`/`blocked`; fail-closed), wiring the run lifecycle through it, and the "Sample data"/"Open in SQL editor" execution path are the **DataAccessResolver + ExecutionProvider** seams owned by E9. E1 produces the `TableRef` and the Explorer surface; E9 resolves and executes. Retained here only as a pointer.

### M4 — Daxis provider as a first-class peer *(Deferred → [E9](../program/rich-lakehouse-workbench-strategy.md))*

> **Deferred and relocated.** The Daxis *discovery* listing is a deferred profile of the E1 catalog seam; the Daxis *read resolution* (`DaxisApprovedAxonReadDescriptor.tables[*].descriptor` → `descriptor`, `validate_daxis_approved_axon_read_descriptor`, `limits`/`access_proof`/reason preservation) is a deferred profile of the **E9 DataAccessResolver**. Either way it is **not built, gated, or depended on** in E1. The reactivation spec (incl. the `daxis-first-class-integration-examples` golden fixtures and `runtime_preference.allow_remote_fallback`) lives with E9.

### M5 — Volumes browsing + file preview *(Moved out of E1 → [E8 — Workspace Files & Volumes](../program/rich-lakehouse-workbench-strategy.md))*

> **Removed from E1 scope.** Volume *discovery* (list + metadata) ships in M2 as a catalog object kind; browsing the files inside a volume, file preview (parquet/csv/json/image), and file editing are the `FileSystemProvider`/Workspace surface owned by **E8**. E8 reuses E1's object-grant/range-read primitives (`ObjectGrant{List,Head,BatchSign,Range}`, `validate_browser_object_url`, `inspect_parquet`/`preflight_parquet_metadata_for_targets`). E1 ships no Volumes Files API and no preview parsers.

### M6 — Cache hardening for E2/E4 + ADR + cleanup
- Finalize stable node shapes; prefetch via route loaders; prefix-invalidation on disconnect/resync; persistence filter (persist structural catalog nodes, never grants/signed URLs). Document the E2/E4 read contract over the cache.
- Land **ADR-0009** (the provider seam, valid independent of Daxis); the ADR-0008 Daxis forward note **defers with M4**. Remove the decorative UC/DS stub text; update `browser-uc-brokered-runtime-contract.md` cross-reference.
- **Gate:** full regression; dependency guardrail + bundle delta; "no secrets in browser" assertion across persisted state + bundle.

---

## 9. Risks and mitigations

| Risk | L/I | Mitigation |
|---|---|---|
| **CORS / cross-origin** on UC REST or governed object reads | High / High | Single-origin Envoy proxy for the metadata plane (§4.2); signed-URL/proxy data plane validated against the exact endpoint shape (ADR-0002). Mock UC/broker proves the same-origin shape in CI. |
| **Auth not ready (E6 in flight)** | High / Med | `SessionHttp` contract + mock UC/broker decouple E1 from E6; DirectUC/Daxis stay flag-gated until E6 lands. |
| **Secret leakage into browser** (token/SA JSON in a fetcher, signed URL persisted) | Med / High | Fetchers never attach Authorization from browser state; descriptor/URL validators (`contains_secret_material`, `validate_browser_object_url`); never persist grants/signed URLs; guardrail + secret-marker assertions in M2/M6. |
| **Inferring policy / opening a governed table** _(→ [E9](../program/rich-lakehouse-workbench-strategy.md))_ | Med / High | Owned by E9's DataAccessResolver: fail-closed resolution, `to_browser_http_snapshot_descriptor` guards, no-`openDeltaTable()` test for fallback/blocked. Out of E1. |
| **Large-catalog performance** (thousands of schemas/tables) | High / Med | Infinite/cursor queries; virtualized tree; debounced server-side name filter (reuse `uc_schema_filter`); long `staleTime`, focus-refetch off for big trees; per-node `gcTime` tuning; lazy `tableMetadataQuery` only on expand/hover. |
| **Cache-key explosion** across many connections/catalogs | Med / Med | Connection-scoped `connectionId` prefix-invalidation; bounded `gcTime`; persist only recently-used connections' structure. |
| **Grant/descriptor expiry mid-session** _(→ [E9](../program/rich-lakehouse-workbench-strategy.md))_ | Med / Med | Owned by E9: read resolution never cached; `expiresAtEpochMs` checked before handoff; expired → typed re-resolve; 401 → E6 re-login. Out of E1. |
| **`QueryTableSource` extension regresses local/object-store** _(→ [E9](../program/rich-lakehouse-workbench-strategy.md))_ | Med / High | E1 leaves the run path untouched; E9 adds the additive `resolved_descriptor` variant with the existing three variants byte-identical. |
| **Daxis contract drift vs. generalization** _(deferred with M4)_ | Low / Med | Daxis provider tested against the shipped example fixtures; ADR-0008 left intact (forward note only). While Daxis is parked, drift is bounded by leaving ADR-0008 + fixtures untouched and re-validating at reactivation. |

---

## 10. Test strategy

**Runner:** Vitest (jsdom) + `@testing-library/react renderHook` for queries; `fake-indexeddb` for persisted registry; mock UC server + mock broker (fetch-level) reusing repo fixtures.

- **Provider unit/contract:** queryOptions/keys per provider (stable, hierarchical, prefix-invalidatable); fetchers vs. mock UC (listing, pagination cursors, metadata shape, view vs table, volumes, functions, models); **navigation conventions (§2.5): list→detail seeding makes a drill-in a cache hit (no refetch); prefetch-on-intent warms the exact key the hook reads; predicate invalidation drops all pages/params of a list**; `SessionHttp` mapping (401/403/404 → session-expired/blocked/not-found). The vendored UC OpenAPI spec stays in sync via an `openapi-typescript` codegen-check in CI.
- **Read-resolution matrix → [E9](../program/rich-lakehouse-workbench-strategy.md).** The DirectUC eligible→`descriptor` / reason→`fallback`/`blocked` matrix, the `openDeltaTable()`-never-for-blocked assertion, and the Daxis fixture parity are E9's test surface, not E1's.
- **Volumes (discovery):** `volumesQuery`/`volumeMetadataQuery` listing + metadata shape against mock UC; volumes render as tree objects with a metadata detail pane. (Volume *file* listing/preview tests live in E8.)
- **Security:** "no secrets in browser" — assert no Authorization header sourced from browser state, no signed URL/grant persisted, secret-marker scan over persisted registry; keep `verify_browser_dependency_guardrails.sh` green (no cloud SDK added).
- **E2E (Playwright):** rail switches Editor ↔ Catalog Explorer; `/explore` deep link selects the right object; explorer "Open in SQL editor" round-trips into a new editor tab; connect UC (mock) → browse lazy tree → activate table → run query; select a volume → metadata detail renders (no file listing in E1); deep-link selection + reload; **keep `local-delta` (re-grant) and `public-gcs-live` green** as the no-regression anchor.
- **CI gates:** WASM artifact budget (unaffected); dependency guardrail; contract-parity over fixtures; ESLint boundary rule (providers may not import the run/session singletons except through the defined seam).

---

## 11. Proposed updates to the E0 plan

E1 surfaced gaps in [E0](./2026-06-20-e0-frontend-foundation-execution-plan.md). These are applied to E0 as additive, E1-informed amendments (E0 is still Draft):

1. **Add a `catalog` segment to the queryKey hierarchy.** E0 §3.3 jumps `connection → schemas`, omitting the UC `catalog` namespace (a UC connection contains *many* catalogs). Insert `['catalog', connectionId, 'catalogs']` and `['catalog', connectionId, catalogName, 'schemas', …]`. Without this, DirectUC cannot cache per-catalog.
2. **Distinguish navigation metadata from the runtime-derived single-table catalog.** E0 models the catalog query as `['catalog', connectionId, 'table-derived']` with `queryFn = getSession + deriveCatalogTable`. E1 needs *navigation* (provider REST, main-thread, not session-bound), kept separate from *read resolution* (session/`openDeltaTable`, owned by [E9](../program/rich-lakehouse-workbench-strategy.md)). Recommend E0 reserve `table-derived` for the Phase-1 reference path and document that E1 providers contribute navigation queries that are **not** session-derived.
3. **Connection id ≠ table-source key (resolve E0 open question #1).** Adopt a dedicated connection-scoped `connectionId` in the `connections` slice; keep `tableSourceKeyFromParts` for table identity within a connection. (E1 §5.1.)
4. **Volumes key (discovery scope only).** E1 needs only a volume-*object* key, e.g. `['catalog', connectionId, catalog, schema, 'volumes']` and `['catalog', connectionId, catalog, schema, volume]` for list + metadata. The deeper volume-*file* key (`['volume-files', connectionId, catalog, schema, volume, ...path]`) is an **E8** concern; E0 should reserve the namespace but E1 does not use it.
5. **`SessionHttp` is a shared seam.** E0's "purge cache on 401" belongs to a shared session-aware fetch wrapper whose contract E1 defines and E6 implements; note it in E0's codegen/provider-boundary section so the fetcher shape is reserved.
6. **Router carries the QueryClient + intent preloading (§2.5 prior art).** Adopt `createRootRouteWithContext<{ queryClient }>` and `defaultPreload: "intent"` so route loaders can `prefetch*` navigation queries before the area mounts (pairs with the hover-prefetch convention). Small E0 router refinement; no milestone change.

These do not change E0's milestones; they refine the seams E0 already commits to so E1 plugs in without rework.

---

## 12. Open questions to confirm at kickoff

1. **UC REST surface via Envoy** — confirm the proxy path shape (`/api/uc/2.1/unity-catalog/...`) and whether the BFF passes UC pagination cursors through verbatim.
2. **DirectUC read eligibility → [E9](../program/rich-lakehouse-workbench-strategy.md).** Whether the first DirectUC release ships only credential-vended external-read-eligible tables (governed shapes → fallback) or also a server-snapshot-resolver mode is an E9 (DataAccessResolver) question, since read resolution moved there. E1 only confirms the discovery surface.
3. **Daxis navigation depth** _(deferred with M4)_ — how browsable is Daxis catalog listing in practice (full tree vs. approved/recent tables only)? Determines M4 navigation richness when Daxis is reactivated.
4. **Volume file access scoped to E8 (confirmed).** Volume *discovery* (list + metadata) is E1; the Volumes Files API, file preview, file write/edit, and object-store prefix browsing are all **E8 (Workspace Files & Volumes)**. Confirm the E1→E8 handoff seam (a "Browse files" action on the volume detail surface) is sufficient for E1.
5. **ADR-0009 ownership** — confirm new ADR + ADR-0008 forward note (vs. amending ADR-0008) with the Daxis release-gate owners.




