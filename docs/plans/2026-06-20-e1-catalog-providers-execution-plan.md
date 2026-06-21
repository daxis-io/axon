# E1 — Pluggable Catalog Providers and Catalog Explorer — Execution Plan

- Status: Draft (planning deliverable)
- Date: 2026-06-20
- Scope: Define the `CatalogProvider` seam and ship a `DirectUnityCatalog` provider (UC REST navigation + table metadata) alongside the existing Daxis (`ReadAccessPlan`), `LocalDelta`, and `ObjectStore` paths. Each provider exposes its reads as TanStack Query options so caching/refresh/dedup/pagination apply uniformly. Deliver a multi-catalog connection registry, catalog/schema/table selectors in sidebar + editor, lazy/paginated tree navigation, volumes browsing + file preview, and a catalog metadata cache that feeds E2 (IntelliSense) and E4 (Cedar entities).
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

> **Buf toolchain note (pulled forward by E0):** the Buf seam that E1/E3 rely on is already standing in E0 via the scoped `axon/config/v1` config module (`buf.yaml`/`buf.gen.yaml` → protobuf-es + `protoschema-jsonschema`, checked into `src/generated/config/` behind the codegen drift check; see [E0 plan §3.6](./2026-06-20-e0-frontend-foundation-execution-plan.md)). E1 should reuse that module/`buf.gen.yaml` rather than re-bootstrapping Buf, and **provider options / connection config should be modeled as `axon/config/v1` messages** so they flow into the same schema-driven, layered Settings surface (per-connection scope is the reserved D7 layer).

---

## 1. Executive summary — recommended decisions

| # | Decision | Recommendation |
|---|----------|----------------|
| 1 | `CatalogProvider` interface shape | **Split each provider into two concerns behind one interface: _navigation_ (browse catalogs/schemas/tables/views/volumes/columns, each surfaced as a TanStack `queryOptions`/`infiniteQueryOptions` factory) and _read resolution_ (`resolveTableRead(ref)` → a discriminated `TableReadResolution`).** Daxis and DirectUC implement the same interface; they differ mostly in read resolution. Read resolution returns one of `descriptor` (an openable `BrowserHttpSnapshotDescriptor`), `read_access_plan` (the `ReadAccessPlan` family, processed to a descriptor by the runtime), `fallback`, or `blocked`. This keeps the descriptor-materialization invariant (browser never infers policy) and lets Daxis's `ReadAccessPlan`/`DaxisApprovedAxonReadDescriptor` map onto the same seam as direct UC. |
| 2 | Where UC credential exchange / CORS lives | **Never in the browser.** "DirectUnityCatalog" means "UC REST _shape_," reached **same-origin through the E6 Envoy proxy** (e.g. `GET /api/uc/2.1/unity-catalog/catalogs`) with `credentials: 'include'`. Envoy terminates login and injects the UC/cloud credential server-side. This sidesteps CORS (single origin) and satisfies ADR-0002. Object/volume bytes are read via short-lived signed URLs or a narrow range proxy (the existing object-store/object-grant path), never cloud creds. E1 defines the fetcher contract; E6 owns the session/refresh/401 mechanics. |
| 3 | Multi-catalog data model | **A `CatalogConnection` registry (E0 client store, persisted) keyed by a stable `connectionId`.** A connection is provider-root-scoped (a UC workspace, a Daxis tenant, a bucket, a local folder), distinct from the *table-scoped* canonical key in `connect/store.ts`. Hierarchy: `connection → catalog → schema → {table|view|volume}`. Active selection `{connectionId, catalog, schema, table}` is **route-owned** (E0 deep link), mirrored into the store. Selection resolves to `provider.resolveTableRead` → descriptor → the existing `getSession`/`openDeltaTable` runtime. |
| 4 | Volumes / file-reading scope | **Reuse the object-store range-read path.** Provider returns per-file `BrowserHttpFileDescriptor`-shaped entries (signed/proxied URLs); preview uses the existing `inspect_parquet` / `preflight_parquet_metadata_for_targets` for parquet, range/whole-object `fetch` for csv/json, and an object URL for image. First release: read-only, bounded-size preview for parquet/csv/json/image; everything else is reference-only (open externally). No new cloud SDKs. |
| 5 | ADR strategy | **Add a new ADR-0009 "Pluggable Catalog Providers"** that defines the seam and explicitly scopes ADR-0008 + the UC brokered contract as the **Daxis/brokered profile**, with a peer **direct-UC profile**. Add a short forward-reference amendment note to ADR-0008 (do not rewrite it — its content is load-bearing for the Daxis release-gate traceability). ADR-0002 is unchanged and remains a hard constraint cited by ADR-0009. |

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
| Object-store read path (reusable) | `services/object-storage.ts` | Anonymous GCS XML-API `_delta_log/` listing → `resolve_delta_snapshot_from_manifest` (WASM) → `BrowserHttpSnapshotDescriptor` → `preflight_parquet_metadata_for_targets` range-read preflight. **This is the file-read primitive E1 volumes reuse.** |
| WASM surface | `apps/axon-web/src/lib.rs` | `resolve_delta_snapshot_from_manifest`, `preflight_parquet_metadata_for_targets`, `SandboxQuerySession::inspect_parquet`, `open`/`dispose_table`. Range reads happen inside the worker via `wasm-http-object-store`. |
| Feature flag | `services/connector-features.ts` | `bffAuthServiceConnectors` from `VITE_AXON_BFF_AUTH_SERVICE_CONNECTORS`. Gates UC/DS. |

### 2.2 The contract surface E1 builds on (`crates/query-contract/src/lib.rs`)

- **`ReadAccessPlan`** (tagged `plan_type`): `BrokeredDelta(BrokeredDeltaReadPlan)` · `DeltaSharing(DeltaSharingReadPlan)` · `SqlFallbackRequired(SqlFallbackRequiredPlan)` · `Blocked(BlockedReadPlan)`. This is the catalog→browser read-authorization vocabulary and the natural target of `resolveTableRead`.
- **`BrokeredDeltaReadPlan::to_browser_http_snapshot_descriptor(resolved_snapshot, object_urls_by_path)`** already converts a brokered plan + signed URLs into an openable descriptor, failing closed unless `direct_external_engine_read == Confirmed`, `batch_sign`, and `range_get` hold.
- **Descriptors**: `BrowserHttpSnapshotDescriptor` (the single execution handoff), `BrowserHttpParquetDatasetDescriptor`, `ResolvedSnapshotDescriptor`.
- **Resolver contract**: `DeltaLocationResolve{Request,Response}` + `validate_delta_location_resolve_exchange` — the explicit server-snapshot-resolver mode.
- **Object-grant routes**: `ObjectGrant{List,Head,BatchSign,Range}*` — list/head/batch-sign/range for `BrokeredObjectAccess` capabilities. **These are exactly the volume file-read operations.**
- **Daxis**: `DaxisApprovedAxonReadDescriptor` (carries `tables: Vec<DaxisApprovedTableDescriptor>` each with an embedded `BrowserHttpSnapshotDescriptor`, plus `access_proof`, `limits`, `runtime_preference`) and `validate_daxis_approved_axon_read_descriptor`.
- **Reason taxonomy**: `ReadAccessPlanReason` (`row_filter`, `column_mask`, `view`, `unknown_policy_state`, `no_direct_external_engine_read_support`, `unsupported_table_type`, `grant_expired`, `storage_cors_blocked`, `broker_unavailable`) and `FallbackReason` — the structured outcomes the UI surfaces (and E4 maps onto).

### 2.3 Constraints observed in the repo

- **ADR-0002 / browser secret boundary**: browser code must never receive service-account JSON, long-lived/refresh tokens, broad cloud tokens, signing secrets, or bucket-traversal config. Only opaque grant IDs, short-lived object-scoped signed URLs, proxy URLs, expiry timestamps, non-secret capability flags, correlation IDs. Enforced by `verify_browser_dependency_guardrails.sh` (cloud-SDK denylist + secret-marker scan) and `contains_secret_material`/URL-policy validators in the contract.
- **Descriptor materialization invariants** (`browser-owned-descriptor-materialization.md`): BFF does not query or reconstruct by default; browser owns descriptor production from safe material; `openDeltaTable()` is the single handoff; `sql_fallback_required`/`blocked` must **not** call `openDeltaTable()`; unknown policy state must fall back or block (fail closed).
- **WASM artifact budget** (≤ 750 KB `browser_engine_worker.wasm`) is Rust-only; E1 is frontend-heavy + contract-light and does **not** touch this gate (but any new WASM helper for volume preview must respect it).
- **Single canonical execution handoff** stays `BrowserHttpSnapshotDescriptor → openDeltaTable()`. E1 must not introduce a parallel execution path.

### 2.4 The core gap

There is **no provider abstraction today** — local/object-store/manifest logic is inlined across `ConnectModal.tsx`, `object-storage.ts`, `query.ts`, and `query-source.ts`, and UC/DS are decorative stubs. The runtime is single-table. E1's job is to (a) introduce the seam, (b) re-express the working paths as reference providers, (c) ship DirectUC + a real Daxis provider, and (d) generalize the single-table runtime into a navigable multi-catalog tree — **without breaking the local/object-store paths that ship today.**

---

## 3. The `CatalogProvider` interface (Decision 1)

### 3.1 Design principle: navigation vs. read resolution

Two concerns that have different lifecycles, caches, and trust properties:

- **Navigation** — "what tables/schemas/volumes/columns exist, and what is their metadata?" Cheap, listable, paginated, cacheable for minutes, refetchable in the background. Surfaced as TanStack `queryOptions`.
- **Read resolution** — "how, if at all, may this browser read this specific table _right now_?" Policy-gated, short-lived, expiring, fail-closed. Returns the `ReadAccessPlan`/descriptor family. **Never cached like navigation** (grants expire; persisting them would violate ADR-0002 posture).

Conflating them (as the E0 plan's single `['catalog', connectionId, 'table-derived']` key does) is the central thing E1 fixes: navigation feeds E2/E4 and the explorer; read resolution feeds the run lifecycle.

### 3.2 Types (sketch)

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
export interface VolumeNode   { catalog: string; schema: string; name: string;
                                volumeType: 'managed' | 'external'; }
export interface VolumeEntry  { path: string; kind: 'dir' | 'file'; sizeBytes?: number;
                                modifiedAt?: string; contentType?: string; }

export type TableRef  = { connectionId: ConnectionId; catalog: string; schema: string; table: string };
export type VolumeRef = { connectionId: ConnectionId; catalog: string; schema: string; volume: string };

// ── Read resolution: the policy-gated handoff ──────────────────────────────
export type TableReadResolution =
  | { kind: 'descriptor';        descriptor: BrowserHttpSnapshotDescriptor }     // ready to openDeltaTable()
  | { kind: 'read_access_plan';  plan: ReadAccessPlan }                          // runtime processes → descriptor
  | { kind: 'fallback';          reason: ReadAccessPlanReason; message: string;  // sql_fallback_required
                                 statementEndpoint?: string }
  | { kind: 'blocked';           reason: ReadAccessPlanReason; message: string };

export type ObjectReadResolution =
  | { kind: 'signed_url'; url: string; expiresAtEpochMs: number; sizeBytes?: number }
  | { kind: 'proxy_range'; baseUrl: string; sizeBytes?: number }
  | { kind: 'denied'; reason: ReadAccessPlanReason; message: string };
```

### 3.3 The provider interface

```ts
export interface CatalogProvider {
  readonly connection: CatalogConnection;
  capabilities(): ProviderCapabilities;

  // ── Navigation (TanStack queryOptions; keys come from the E0 queryKeys factory) ──
  catalogsQuery(): UseQueryOptions<CatalogNode[]>;
  schemasQuery(catalog: string): UseInfiniteQueryOptions<Page<SchemaNode>>;
  tablesQuery(catalog: string, schema: string): UseInfiniteQueryOptions<Page<TableNode>>;
  tableMetadataQuery(ref: TableRef): UseQueryOptions<TableMetadata>;
  volumesQuery(catalog: string, schema: string): UseQueryOptions<VolumeNode[]>;
  volumeEntriesQuery(ref: VolumeRef, path: string): UseInfiniteQueryOptions<Page<VolumeEntry>>;

  // ── Read resolution (imperative; expiring; fail-closed; never persisted) ──
  resolveTableRead(ref: TableRef, opts?: { snapshotVersion?: number }): Promise<TableReadResolution>;
  resolveVolumeFileRead(ref: VolumeRef, path: string): Promise<ObjectReadResolution>;
}

export interface Page<T> { items: T[]; nextCursor?: string; }
```

All fetchers go through one **session-aware fetch wrapper** (`query/providers/http.ts`) that always uses `credentials: 'include'`, attaches a correlation id, maps `401 → SessionExpired` (E6 re-login), `403 → blocked`, `404 → not-found`, and **refuses to attach Authorization headers from browser state** (guardrail: there is no token in browser state to attach). Providers never call `fetch` directly.

### 3.4 How each provider maps onto the interface

| Provider | `browsable` | Navigation source | `resolveTableRead` returns |
|---|---|---|---|
| **DirectUnityCatalog** | yes | UC REST via Envoy proxy: `/catalogs`, `/schemas`, `/tables`, `/tables/{full_name}` (columns/props), `/volumes`, `/volumes/{name}` + Files API for entries | For external-read-eligible tables: a `BrokeredDeltaReadPlan` (via the brokered object-grant/credential-vending route) → `read_access_plan`; governed shapes (row filter/column mask/view) → `fallback`/`blocked`. |
| **Daxis** | partial | Daxis catalog/list surfaces (thin; may be navigation-light — list approved/recent tables). Tree nodes can be lazily backed by Daxis list APIs where available. | `DaxisApprovedAxonReadDescriptor.tables[*].descriptor` → `descriptor`; non-approved → `fallback`/`blocked` preserving Daxis reason. |
| **ObjectStore** (GCS) | flat (one table root = one table) | Synthesizes a single-catalog/single-schema/single-table tree from the table URI (today's `objectStorageRuntimeFromDescriptor`). `volumesQuery` lists bucket prefixes (optional later). | Reconstructs via `resolvePublicObjectStorageDescriptor` → `descriptor`. |
| **LocalDelta** | flat | Single table from the File System Access handle + reconstructed snapshot. | Local snapshot → `descriptor` (re-grant flow from E0 surfaces as a typed error). |

This table is the heart of Decision 1: **Daxis's `ReadAccessPlan` model and direct UC REST land on the same interface because read resolution is a separate method whose return type is the union of both worlds' outcomes.** Navigation differences (UC is richly browsable; Daxis/object-store/local are flatter) are expressed through `capabilities()` and degrade gracefully in the explorer UI.

### 3.5 Provider registry + dispatch

```ts
// query/providers/registry.ts
export function providerForConnection(conn: CatalogConnection, http: SessionHttp): CatalogProvider;
```

A pure factory keyed by `conn.kind`. The explorer and run lifecycle never branch on `kind`; they call interface methods. New providers (a future `RemoteService`/`Tauri` catalog) are additive.

---

## 4. Credential exchange, CORS, and the E6 seam (Decision 2)

### 4.1 The rule

ADR-0002: the browser may read cloud data only via (1) short-lived object-scoped signed HTTPS URLs or (2) a narrow read-proxy. It must never hold service-account JSON, refresh/broad tokens, or bucket-traversal config. E1 honors this for **both** the metadata plane (UC REST) and the data plane (object/volume bytes).

### 4.2 Metadata plane — UC REST through Envoy

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

### 4.3 Data plane — object/volume bytes

Identical to today's object-store path and the object-grant routes:
- **Public** (object-store GCS): anonymous signed-less HTTPS + CORS on the bucket (already shipped).
- **Governed** (UC external tables / volumes): provider calls the brokered route (`/object-grants/{grantId}/batch-sign` or `/range`) → short-lived signed URLs or proxy-range base → wrapped as `BrowserHttpFileDescriptor` for the worker's existing range-read path.
- The worker's `wasm-http-object-store` range reads carry the **ambient same-origin session** for proxy-range mode, or hit the signed URL directly for signed mode. **The worker-credential mechanics are owned by E6**; E1 consumes whichever of `signed_url` / `proxy` the resolver returns (`ResolverActualAccessMode`).

### 4.4 What E1 owns vs. what E6 owns

| Concern | Owner |
|---|---|
| `SessionHttp` wrapper contract (`credentials:'include'`, correlation id, 401/403/404 mapping) | **E1 defines, E6 implements the session/refresh behind it** |
| Login, cookie issuance, token storage, UC credential injection, Envoy config, COOP/COEP headers | **E6** |
| Worker `fetch` credential propagation for range reads | **E6** |
| Provider fetchers, queryOptions, read-resolution dispatch, descriptor validation | **E1** |
| Mock UC + mock broker fixtures for tests/local dev | **E1** |

**Sequencing**: E1 can build navigation + read-resolution against a **mock UC/broker** (no real session) while E6 lands in parallel. The `SessionHttp` contract is the integration point; E1 ships the interface + mock, E6 fills the real session. The feature flag (`bffAuthServiceConnectors`) keeps DirectUC/Daxis dark in production until E6 is ready.

---

## 5. Multi-catalog data model and selector flow (Decision 3)

### 5.1 `connectionId` — connection-scoped, not table-scoped

Today's `tableSourceKeyFromParts` produces a **table-scoped** canonical key (includes schema + table). E1 needs a **connection-root** id (a UC workspace, a Daxis tenant, a bucket root, a local folder) so the whole `['catalog', connectionId, …]` subtree shares one cache and prefix-invalidates on disconnect.

Recommendation: introduce `connectionId = stableHash(kind, normalizedLocator)` in the `connections` store slice (E0). Keep `tableSourceKeyFromParts` for *table identity within* a connection (the deepest queryKey segment + run dedup). This resolves **E0 open question #1** in favor of a dedicated connection id, with the canonical key retained for table-level identity.

### 5.2 Registry

The existing `ConnectedCatalog`/`connect/store.ts` model is **kept and promoted** into the E0 `connections` client-store slice (persisted), generalized so:
- A connection carries `kind`, `label`, `locator`, `capabilities` (cached from `provider.capabilities()`), and a **scope** (selected catalogs/schemas filters — the existing `SchemaSelection` selection model extends to a catalog level).
- The **persisted** part is non-sensitive structure only (locators, labels, selection). Live navigation nodes come from TanStack Query (persisted per the E0 filter rules); read resolutions are never persisted.

### 5.3 Selection → query execution

```
route /catalog/$connectionId/$catalog/$schema/$table   (E0 deep link; source of truth)
  → connections slice mirrors active TableRef
  → run lifecycle: provider.resolveTableRead(ref)
      → 'descriptor'        → getSession(descriptor) → openDeltaTable() → run SQL
      → 'read_access_plan'  → runtime materializes descriptor (existing BrokeredDelta path) → openDeltaTable()
      → 'fallback'/'blocked'→ surface structured reason; DO NOT open table
```

`QueryTableSource` (the `getSession` input) is **extended** with a fourth variant that carries a pre-resolved descriptor/plan handoff (`{ kind: 'resolved_descriptor', ref, descriptor }`) so the session layer accepts provider output without re-deriving locators. The existing three variants (`manifest`/`local_delta`/`object_store_table_root`) remain for the reference providers, keeping current behavior byte-identical.

### 5.4 Editor integration

- **Sidebar**: a lazy catalog tree (connections → catalogs → schemas → tables/views/volumes) backed by infinite queries; the existing `ConnectedCatalogsPanel` graduates into this tree (its `onActivate` already produces an `ActiveConnectedTableRef`).
- **Editor**: per-tab connection/catalog context (E2 will consume this for completion scoping; E1 stores it on the tab in the E0 `tabs` slice). The catalog/schema/table selectors are a header control bound to the active route.
- **Catalog metadata cache for E2/E4**: `tableMetadataQuery`/`tablesQuery`/`schemasQuery` results under stable keys ARE the cache E2 reads for completion and E4 reads for entity shapes — no separate store. E1 guarantees the node shapes in §3.2 are stable and serializable.

---

## 6. Volumes browsing and file reading (Decision 4)

### 6.1 Scope (first release)

- **Browse**: `volumesQuery` (list volumes in a schema) + `volumeEntriesQuery` (lazy/infinite directory listing) for UC volumes. Object-store gets an optional prefix browser later; local/Daxis: out of scope first release.
- **Preview** (read-only, bounded): **parquet** (footer/schema + first N rows via `inspect_parquet` + a bounded scan), **csv/json** (range/whole-object `fetch`, size-capped, parsed client-side), **image** (object URL → `<img>`, size-capped). Anything else → metadata + "open externally" (the resolved signed URL), no inline render.

### 6.2 Reuse of the range-read path

The data plane is identical to object-store reads:

```
provider.resolveVolumeFileRead(ref, path)
  → ObjectReadResolution { signed_url | proxy_range }       (object-grant batch-sign / range; ADR-0002-safe)
  → wrap as BrowserHttpFileDescriptor
  → parquet:  preflight_parquet_metadata_for_targets + inspect_parquet (worker range reads)
  → csv/json: bounded fetch + parse (main thread or worker)
  → image:    object URL
```

No new cloud SDK, no new credential path. A small CSV/JSON sniff/preview helper is the only genuinely new code; parquet reuses the WASM surface already shipped. Size caps and a "this preview is truncated" affordance bound large-file risk.

### 6.3 Safety

File URLs pass the existing browser-object-URL policy validation (`validate_browser_object_url`, HTTPS-only) before use; signed URLs must be short-lived + object-scoped; the secret-marker scan applies to any persisted volume metadata (never persist signed URLs).

---

## 7. ADR strategy (Decision 5)

**Add ADR-0009 "Pluggable Catalog Providers"** (new file) that:
- Establishes the `CatalogProvider` seam (navigation + read resolution) and the `TableReadResolution` union as the contract.
- Declares the **Daxis/brokered profile** (ADR-0008 + UC brokered runtime contract) and the **direct-UC profile** as peer implementations of the seam.
- Restates ADR-0002 as a hard constraint binding every provider (no secrets; signed-URL/proxy data plane; same-origin metadata plane).
- States the fail-closed rule (unknown policy → fallback/blocked) and the single-handoff rule (`descriptor → openDeltaTable()`).

**Amend ADR-0008** with a one-paragraph forward note: "Under the pluggable provider model (ADR-0009), this ADR describes the Daxis profile specifically; it remains authoritative for that profile." Do **not** rewrite ADR-0008 — its text is referenced by the Daxis release-gate traceability/proof packets, and broad edits would churn those gates.

Rationale for new-ADR-over-amend: ADR-0008 is narrowly Daxis-scoped and tied to external proof artifacts; a new ADR cleanly owns the generalization without destabilizing the Daxis evidence chain, and gives E4 (which also reconciles ADR-0008) a single seam doc to reference.

---

## 8. Milestones (strangler — app ships at every step)

Each milestone is independently shippable behind flags and gated on: `tsc --noEmit`, ESLint (`--max-warnings=0`), existing Playwright suites (`editor-smoke`, `local-delta`, `public-gcs-live`) green, plus new Vitest. The local/object-store paths must stay byte-identical until explicitly migrated.

### M0 — Provider seam + reference providers (no behavior change)
- Add `query/providers/{types,registry,http}.ts` (interface §3, `SessionHttp` contract §4.4 with a passthrough impl for public/local).
- Re-express **LocalDelta**, **ObjectStore**, and **manifest/fixture** as `CatalogProvider`s whose `resolveTableRead` returns today's descriptors, and whose navigation synthesizes the current flat tree. `query-source.ts`'s `querySourceFromConnectedCatalogs` is reimplemented on top of `resolveTableRead` but produces identical `QueryTableSource` output.
- Add the `resolved_descriptor` `QueryTableSource` variant; `getSession` accepts it.
- **Gate:** `public-gcs-live` + `local-delta` + `editor-smoke` green (proving no regression); Vitest for the reference providers + registry dispatch.

### M1 — Multi-catalog registry + selectors + deep links
- Promote `ConnectedCatalog` into the E0 `connections` slice with connection-scoped `connectionId` (§5.1); generalize selection to catalog level. Migrate `connect/store.ts` localStorage onto the E0 persistence KV behind unchanged signatures (coordinate with E0 M1/M2).
- Lazy catalog tree in the sidebar (infinite queries) + route-driven selection (`/catalog/$connectionId/$catalog/$schema/$table`) consuming E0's router. `ConnectedCatalogsPanel` graduates into the tree.
- Per-tab connection/catalog context on the `tabs` slice.
- **Gate:** Playwright deep-link + selection tests; Vitest for registry upsert/remove/dedupe (port `connect/store.ts` assertions) + selection reconciliation; existing suites green.

### M2 — DirectUnityCatalog navigation
- Implement `catalogsQuery`/`schemasQuery`/`tablesQuery`/`tableMetadataQuery`/`volumesQuery` against UC REST through `bffBaseUrl` (Envoy proxy), with cursor pagination and `staleTime`/retry policy from E0. Build the **mock UC** fixture server for tests/dev.
- Replace the UC config stub in `ConnectModal.tsx` with a real `Test connection` (calls `catalogsQuery`) and real discovery; keep behind `bffAuthServiceConnectors`.
- **Gate:** Vitest against mock UC (listing, pagination cursors, metadata shape, 401→session-expired, 403→blocked); Playwright UC-connect-and-browse against mock; secret-marker assertion on persisted nodes.

### M3 — DirectUnityCatalog read resolution → execution
- Implement `resolveTableRead`: external-read-eligible UC tables → brokered object-grant path → `BrokeredDeltaReadPlan` → reuse `to_browser_http_snapshot_descriptor` → `descriptor` → `openDeltaTable()`; governed shapes (row filter/column mask/view/unknown) → `fallback`/`blocked` with structured reason in the UI. Fail closed on unknown policy.
- Wire the run lifecycle (E0 `run` slice) to dispatch through `resolveTableRead`.
- **Gate:** Vitest for the resolution matrix (eligible→descriptor; each `ReadAccessPlanReason`→fallback/blocked; expired grant; missing capability); Playwright run-against-mock-UC table; verify `openDeltaTable()` is never called for fallback/blocked.

### M4 — Daxis provider as a first-class peer
- Implement the Daxis `CatalogProvider`: navigation thin (list approved/recent tables where Daxis exposes them), read resolution rich (`DaxisApprovedAxonReadDescriptor.tables[*].descriptor` → `descriptor`; validate via `validate_daxis_approved_axon_read_descriptor`; preserve `limits`/`access_proof`/Daxis reasons on fallback/block).
- Reuse the existing `daxis-first-class-integration-examples` fixtures as the provider's golden tests.
- **Gate:** contract-parity Vitest over the Daxis example fixtures (executed/fallback/blocked/expired/mismatch); ensure Daxis profile honors `runtime_preference.allow_remote_fallback`.

### M5 — Volumes browsing + file preview
- `volumesQuery`/`volumeEntriesQuery` for UC volumes; `resolveVolumeFileRead` via object-grant batch-sign/range. Preview surfaces for parquet/csv/json/image (§6) reusing the range-read path. Size caps + truncation affordance.
- **Gate:** Vitest for entry pagination + each preview parser (parquet via WASM, csv/json bounded, image URL); URL-policy validation; Playwright volume-browse-and-preview against mock.

### M6 — Cache hardening for E2/E4 + ADR + cleanup
- Finalize stable node shapes; prefetch via route loaders; prefix-invalidation on disconnect/resync; persistence filter (persist structural catalog nodes, never grants/signed URLs). Document the E2/E4 read contract over the cache.
- Land **ADR-0009** + the ADR-0008 forward note; remove the decorative UC/DS stub text; update `browser-uc-brokered-runtime-contract.md` cross-reference.
- **Gate:** full regression; dependency guardrail + bundle delta; "no secrets in browser" assertion across persisted state + bundle.

---

## 9. Risks and mitigations

| Risk | L/I | Mitigation |
|---|---|---|
| **CORS / cross-origin** on UC REST or governed object reads | High / High | Single-origin Envoy proxy for the metadata plane (§4.2); signed-URL/proxy data plane validated against the exact endpoint shape (ADR-0002). Mock UC/broker proves the same-origin shape in CI. |
| **Auth not ready (E6 in flight)** | High / Med | `SessionHttp` contract + mock UC/broker decouple E1 from E6; DirectUC/Daxis stay flag-gated until E6 lands. |
| **Secret leakage into browser** (token/SA JSON in a fetcher, signed URL persisted) | Med / High | Fetchers never attach Authorization from browser state; descriptor/URL validators (`contains_secret_material`, `validate_browser_object_url`); never persist grants/signed URLs; guardrail + secret-marker assertions in M2/M6. |
| **Inferring policy / opening a governed table** | Med / High | Fail-closed `resolveTableRead`; reuse `to_browser_http_snapshot_descriptor` guards (requires `Confirmed` + `batch_sign` + `range_get`); explicit no-`openDeltaTable()` test for fallback/blocked. |
| **Large-catalog performance** (thousands of schemas/tables, deep volumes) | High / Med | Infinite/cursor queries; virtualized tree; debounced server-side name filter (reuse `uc_schema_filter`); long `staleTime`, focus-refetch off for big trees; per-node `gcTime` tuning; lazy `tableMetadataQuery` only on expand/hover. |
| **Cache-key explosion** across many connections/catalogs | Med / Med | Connection-scoped `connectionId` prefix-invalidation; bounded `gcTime`; persist only recently-used connections' structure. |
| **Grant/descriptor expiry mid-session** | Med / Med | Read resolution is never cached; `expiresAtEpochMs` checked before handoff; expired → typed re-resolve, not a stale descriptor; 401 → E6 re-login. |
| **`QueryTableSource` extension regresses local/object-store** | Med / High | New `resolved_descriptor` variant is additive; existing three variants untouched; M0 gate is existing suites byte-identical. |
| **Daxis contract drift vs. generalization** | Low / Med | Daxis provider tested against the shipped example fixtures; ADR-0008 left intact (forward note only). |
| **Bundle growth** (preview parsers) | Low / Low | csv/json parsers tiny; parquet reuses WASM (size-gated separately); image is native; lazy-load preview chunks. |

---

## 10. Test strategy

**Runner:** Vitest (jsdom) + `@testing-library/react renderHook` for queries; `fake-indexeddb` for persisted registry; mock UC server + mock broker (fetch-level) reusing repo fixtures.

- **Provider unit/contract:** queryOptions/keys per provider (stable, hierarchical, prefix-invalidatable); fetchers vs. mock UC (listing, pagination cursors, metadata shape, view vs table, volumes); `SessionHttp` mapping (401/403/404 → session-expired/blocked/not-found).
- **Read-resolution matrix:** DirectUC eligible→`descriptor`; each `ReadAccessPlanReason`→`fallback`/`blocked`; expired grant; missing `batch_sign`/`range_get`/`Confirmed`; **assert `openDeltaTable()` is never invoked for fallback/blocked**. Daxis: parity over `daxis-first-class-integration-examples/*` (executed/fallback/blocked/expired/mismatch).
- **Volumes/preview:** entry pagination; parquet preview via WASM `inspect_parquet`; csv/json bounded parse + truncation; image URL; URL-policy rejection of non-HTTPS/secret-bearing URLs.
- **Security:** "no secrets in browser" — assert no Authorization header sourced from browser state, no signed URL/grant persisted, secret-marker scan over persisted registry; keep `verify_browser_dependency_guardrails.sh` green (no cloud SDK added).
- **E2E (Playwright):** connect UC (mock) → browse lazy tree → activate table → run query; volume browse + preview; deep-link selection + reload; **keep `local-delta` (re-grant) and `public-gcs-live` green** as the no-regression anchor.
- **CI gates:** WASM artifact budget (unaffected); dependency guardrail; contract-parity over fixtures; ESLint boundary rule (providers may not import the run/session singletons except through the defined seam).

---

## 11. Proposed updates to the E0 plan

E1 surfaced gaps in [E0](./2026-06-20-e0-frontend-foundation-execution-plan.md). These are applied to E0 as additive, E1-informed amendments (E0 is still Draft):

1. **Add a `catalog` segment to the queryKey hierarchy.** E0 §3.3 jumps `connection → schemas`, omitting the UC `catalog` namespace (a UC connection contains *many* catalogs). Insert `['catalog', connectionId, 'catalogs']` and `['catalog', connectionId, catalogName, 'schemas', …]`. Without this, DirectUC cannot cache per-catalog.
2. **Distinguish navigation metadata from the runtime-derived single-table catalog.** E0 models the catalog query as `['catalog', connectionId, 'table-derived']` with `queryFn = getSession + deriveCatalogTable`. E1 needs *navigation* (provider REST, main-thread, not session-bound) separate from *read resolution* (session/`openDeltaTable`). Recommend E0 reserve `table-derived` for the Phase-1 reference path and document that E1 providers contribute navigation queries that are **not** session-derived.
3. **Connection id ≠ table-source key (resolve E0 open question #1).** Adopt a dedicated connection-scoped `connectionId` in the `connections` slice; keep `tableSourceKeyFromParts` for table identity within a connection. (E1 §5.1.)
4. **Volumes key needs volume identity.** E0's `['volumes', connectionId, ...path]` should be `['volumes', connectionId, catalog, schema, volume, ...path]` to address a specific UC volume, not just a connection + path.
5. **`SessionHttp` is a shared seam.** E0's "purge cache on 401" belongs to a shared session-aware fetch wrapper whose contract E1 defines and E6 implements; note it in E0's codegen/provider-boundary section so the fetcher shape is reserved.

These do not change E0's milestones; they refine the seams E0 already commits to so E1 plugs in without rework.

---

## 12. Open questions to confirm at kickoff

1. **UC REST surface via Envoy** — confirm the proxy path shape (`/api/uc/2.1/unity-catalog/...`) and whether the BFF passes UC pagination cursors through verbatim.
2. **DirectUC read eligibility** — for the first DirectUC release, do we ship only credential-vended external-read-eligible tables (governed shapes → fallback), matching the brokered contract, or also a server-snapshot-resolver mode?
3. **Daxis navigation depth** — how browsable is Daxis catalog listing in practice (full tree vs. approved/recent tables only)? Determines M4 navigation richness.
4. **Volumes write/upload** — confirmed out of scope for E1 (read-only preview only)?
5. **ADR-0009 ownership** — confirm new ADR + ADR-0008 forward note (vs. amending ADR-0008) with the Daxis release-gate owners.
6. **Object-store prefix browsing** — defer the generic bucket browser to a later release, or include a minimal version in M5?




