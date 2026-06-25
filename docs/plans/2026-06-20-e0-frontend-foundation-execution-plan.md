# E0 — Frontend Foundation: State, Routing, Persistence — Execution Plan

- Status: Draft (planning deliverable)
- Date: 2026-06-20
- Scope: Detailed execution plan for the foundational frontend refactor of `apps/axon-web`: a scalable, typed state architecture (server state on TanStack Query, client/UI/session state on a lightweight store), a unified persistence abstraction, and real routing. Frontend-only; no Rust contract changes.
- Related:
  - [Rich Lakehouse Workbench — High-Level Strategy](../program/rich-lakehouse-workbench-strategy.md) (E0 framing)
  - [Rich Lakehouse Workbench — Planning Prompts](../program/rich-lakehouse-workbench-planning-prompts.md)
  - [ADR-0002: No cloud secrets in the browser](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)

> E0 is the substrate that E1 (catalog providers), E2 (Monaco IntelliSense), E5 (Arrow viz), and E7 (table insight) build on. The TanStack Query layer is the seam E1 providers plug into and the cache E2/E4 read from. This plan keeps the app shippable at every milestone via a strangler migration off the current module singletons.

---

## 1. Executive summary — recommended decisions

| #   | Decision                  | Recommendation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| --- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Client-store library      | **Zustand + slices**, using the vanilla `createStore` so non-React modules (the WASM session manager) can read/write it. `subscribeWithSelector` + a `persist` middleware bound to our persistence abstraction. Reserve **XState** for the query-run lifecycle slice only if it grows complex. Reject Redux Toolkit (boilerplate + RTK Query would compete with TanStack Query).                                                                                                                                                                                                                                                                                                                                                                              |
| 2   | Server vs client boundary | Server state = anything originating outside the app that can be refetched/invalidated (remote catalog/UC/Daxis metadata, object-store listings, **the WASM-derived catalog + commits**, and local user CRUD data in IndexedDB modeled as a "local server"). Client state = UI intent, selectors, layout, the **connection registry**, the **run lifecycle**, **engine status** (push/event-driven), and the **WASM session handle**. Components read server data via `useQuery`/`queryClient`; the run lifecycle reads table metadata via `queryClient.getQueryData(...)` rather than copying it into the store.                                                                                                                                              |
| 3   | queryKey conventions      | A single typed `queryKeys` factory. Hierarchy `[domain, connectionId, resource, ...ids]`, e.g. `['catalog', connectionId, 'schemas', schemaId, 'tables', tableId, 'metadata']`. `connectionId` encodes provider kind + locator, so Daxis vs DirectUC vs local naturally separate caches and prefix-invalidation drops subtrees. Providers contribute TanStack v5 `queryOptions()`; per-resource `staleTime`/`gcTime`/retry-predicate; infinite queries for large trees.                                                                                                                                                                                                                                                                                       |
| 4   | Persistence abstraction   | One `persistence/` module exposing namespaced `KeyValueStore` (IndexedDB via `idb`, with a synchronous `localStorage` backend for FOUC-critical config), and a `HandleStore` for File System Access directory/file handles that encapsulates the re-grant lifecycle. Migrate `metadata.ts`, `connect/store.ts`, and the `local-delta` registry onto it behind unchanged public APIs. OPFS stays worker-owned (engine cache). `persistQueryClient`: persist only cheap, successful, non-sensitive `['catalog', …]` structure + `['local', …]`; everything else memory-only.                                                                                                                                                                                    |
| 5   | Routing                   | **Adopt TanStack Router.** Route loaders `ensureQueryData` for route-driven prefetch, typed params + typed/validated search params, and natural pairing with TanStack Query. Wire the dead `/catalogs` route; add deep links to catalog/schema/table, saved queries, and the E7 insight surface. Keep the History-API router working during migration behind a `navigate()` shim. React Router is the documented fallback if we want fewer new concepts.                                                                                                                                                                                                                                                                                                      |
| 6   | Configuration             | **Proto-first, schema-driven config.** All preferences/tunables are defined in `axon/config/v1` protos (+ protovalidate), generated to **JSON Schema (json-strict)** + protobuf-es types, stored as **layered sparse JSON** (`defaults < user < workspace`) and rendered into one discoverable **Settings** surface (JSON Forms GUI + raw-JSON editor, same schema). Validation is hybrid: **AJV** against the generated schema (structure + standard constraints) with a **protovalidate-es** seam reserved for custom/cross-field CEL. The **Buf toolchain is pulled forward** as a scoped config module (revises "do not block on E3" for config only). Mirrors how VS Code/Zed/Cursor centralize config (schema is canonical, JSON is the runtime store). |

Cross-cutting: stand up a **codegen seam** (typed fetchers + `queryOptions` as the provider boundary) so OpenAPI-/protobuf-generated types and TanStack integrations can drop in incrementally (E1/E3) without reworking the query layer. Add **Vitest** (+ `fake-indexeddb`) for state/persistence unit tests; keep Playwright for E2E.

Non-goals for E0: implementing UC/Daxis providers (E1), Monaco (E2), Arrow-native grid (E5), Cedar (E4). E0 ships the seams plus a **local/fixture reference provider**.

---

## 2. Current-state assessment (grounding)

### 2.1 State inventory and target ownership

| Today (file / symbol)                                                                                                          | What it is                                                                                                                                                                        | Target owner                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `services/query.ts` — `session`, `sessionInit`, `sessionGeneration`, `subscribeSession`, `subscribeWorkerEvents`, `getSession` | Long-lived `AxonBrowserClient` + Web Worker handle; module singletons                                                                                                             | **Client store** (`session` slice / module-scoped `sessionManager` owned by store). Never a query — it is a resource. **E9-informed:** this WASM session handle is ultimately owned by the **BrowserWasm `ExecutionProvider`** (E9); E0 keeps it as a store-held resource and E9 wraps it behind the executor seam.                                                  |
| `services/query.ts` — `runQuery`, pagination, event stream                                                                     | Imperative, streaming, cancellable query execution                                                                                                                                | **Client store** `run` slice (optionally XState). Not TanStack (imperative + streaming + cancel + "load more"). **E9-informed:** the run lifecycle dispatches through the `ExecutionProvider` seam (E9) — the slice holds run/UI state; the executor owns the descriptor→`openDeltaTable()` handoff. E0 keeps today's direct `runQuery()` call; E9 inserts the seam. |
| `services/catalog.ts` — `loadCatalog`/`subscribeCatalog`/`snapshotCatalog` + `deriveCatalogTable`                              | Metadata derived from the resolved Delta snapshot (single-table Phase 1)                                                                                                          | **TanStack Query** `['catalog', connectionId, 'table-derived']`; queryFn calls `getSession` + `deriveCatalogTable`.                                                                                                                                                                                                                                                  |
| `services/snapshot.ts` — `loadCommits`/`subscribeCommits`                                                                      | Remote fetch + parse of commit JSON                                                                                                                                               | **TanStack Query** `['catalog', connectionId, 'commits']`. Also feeds E7.                                                                                                                                                                                                                                                                                            |
| `services/engine.ts` — `subscribeEngineStatus`                                                                                 | Bundle selection (compile-time) + runtime `cache_metrics` events                                                                                                                  | **Client store** `engine` slice, fed by a worker-event→store bridge. Push, not pull → not TanStack.                                                                                                                                                                                                                                                                  |
| `services/history.ts`, `saved.ts` (+ `metadata.ts`)                                                                            | User CRUD data in IndexedDB                                                                                                                                                       | **TanStack Query over IDB** (`['local','history']`, `['local','saved']`) + mutations that invalidate. IDB is source of truth; cache mirrors.                                                                                                                                                                                                                         |
| `editor/connect/store.ts` (localStorage)                                                                                       | Connection **registry** (which catalogs are connected)                                                                                                                            | **Client store** `connections` slice (persisted). It parameterizes server queries; it is not itself server data.                                                                                                                                                                                                                                                     |
| `App.tsx` ~40 `useState`                                                                                                       | tabs/active, runState/result/metrics/events/plan, capMatrix, commits, engineStatus, catalog/history/saved, connect-workflow, layout (`sidebarW`,`resultsH`), tweaks, toast/modals | Split across **client store** slices (`tabs`, `layout`, `ui`, `connections`, `run`, `engine`, **`settings`**) and **TanStack Query** (`catalog`, `commits`, `history`, `saved`). **Appearance/typography/default-target `tweaks` retarget from `layout` to the schema-driven `settings` slice** (generated types); `layout` keeps panel geometry only.               |
| `editor/router.ts`                                                                                                             | History-API helper; `/catalogs` declared, never rendered                                                                                                                          | **TanStack Router**; `navigate()` shim during migration.                                                                                                                                                                                                                                                                                                             |
| `local-delta.ts` registry + handles                                                                                            | IDB registry + File System Access handles + OPFS object URLs + re-grant                                                                                                           | **Persistence** `HandleStore`; re-grant status surfaced via a `localAccess` client-store slice.                                                                                                                                                                                                                                                                      |

### 2.2 Constraints observed in the repo

- **WASM artifact budget is the hard gate**, and it is Rust-only: `tests/perf/report_browser_worker_artifact.sh` enforces `browser_engine_worker.wasm ≤ 750 KB`. Frontend JS deps (TanStack/Zustand/Router) do **not** touch this gate.
- **Browser dependency guardrail** (`tests/security/verify_browser_dependency_guardrails.sh`) denylists cloud SDKs in `package.json`/`package-lock.json` (AWS/GCP/Azure/opendal). TanStack Query, TanStack Router, Zustand, and `idb` are **not** denylisted — but every new dependency must be re-checked against it, and the artifact is scanned for secret-like markers.
- No JS unit-test runner today (only Playwright configs). We add Vitest.
- `main.tsx` renders under `StrictMode` → all store/query/bridge initialization must be idempotent (double-invoke safe).
- Existing legacy→IDB migrations (`history.ts`/`saved.ts` `LEGACY_KEY`) and the local-delta persistence-mode/re-grant behavior (`localAccessNeedsReselect` in `App.tsx`) must be preserved exactly.

---

## 3. Target architecture

```
                +-----------------------------------------------------------+
                |                        React UI                           |
                |  useQuery / useInfiniteQuery        useStore(selectors)   |
                +------------------|----------------------------|----------+
                                   |                            |
              server state         |                            |  client state
        +----------------------+   |              +-------------------------------+
        |   TanStack Query      |  |              |   Zustand store (vanilla)     |
        |   QueryClient + cache |  |              |   slices:                     |
        |                       |  |              |   - layout (persist)          |
        |                       |  |              |   - settings (layered JSON)   |
        |  queryKeys factory    |  |              |   - tabs (persist)            |
        |  queryOptions/providers|<-+- params --- |   - connections (persist)     |
        |   - catalog (derived) |  selectors      |   - ui (modals/toast)         |
        |   - commits           |                 |   - run (lifecycle/XState?)   |
        |   - local: history,   |   getQueryData  |   - engine (event-fed)        |
        |     saved             |---------------->|   - session (WASM handle)     |
        +-----------+-----------+                 |   - localAccess (re-grant)    |
                    |                             +---------------+---------------+
                    | queryFn / mutationFn                        | resource ops
                    v                                             v
        +-----------------------+                 +-------------------------------+
        |  Providers (E0: local/ |                 |  sessionManager (query.ts)    |
        |  fixture; E1: UC/Daxis)|                 |  AxonBrowserClient + Worker   |
        +-----------+-----------+                 +---------------+---------------+
                    |                                             |
                    v                                             v
        +-----------------------------------------------------------------------+
        |                       persistence/ abstraction                        |
        |   KeyValueStore (IndexedDB / localStorage)   HandleStore (FS Access)  |
        |   persistQueryClient persister (IDB, filtered)     OPFS = worker-owned|
        +-----------------------------------------------------------------------+
```

### 3.1 Client-store library (Decision 1)

**Recommend Zustand + slices**, created with the vanilla `createStore` and consumed via `useStore(store, selector)`.

Rationale:

- **Works outside React.** The WASM session handle and the worker-event bridge live at module scope today (`services/query.ts`, `engine.ts`). A vanilla Zustand store can hold/serve them and be mutated from non-React code — Redux's React bindings and XState's actor model are heavier for this.
- **Low ceremony, tiny footprint (~1 KB gz).** No providers, no action-type boilerplate, no Immer requirement. Slices compose into one store.
- **Selector subscriptions prevent re-render storms.** This is the single biggest practical win: today `setRunState` fires every 80 ms during a run and re-renders the whole 1,150-line `App`. With Zustand, only the component selecting `run.elapsed` re-renders. Use `subscribeWithSelector` for the worker→store bridge.
- **Composes cleanly with TanStack Query.** No server-cache overlap (unlike RTK Query). Zustand owns intent/UI; TanStack owns server data.

Middleware: `persist` (bound to our persistence KV adapter), `subscribeWithSelector`, `devtools` (dev only), `immer` optional for the `connections` reducer-heavy logic.

**XState**: adopt only for the `run` slice if the lifecycle (idle → running → streaming events → done/error/cancelled + "load more" + reselect-needed) becomes hard to reason about as plain transitions. Keep it an internal implementation detail of the slice; do not make XState the global store.

**Redux Toolkit**: rejected. More boilerplate; its RTK Query would duplicate the TanStack Query responsibilities we explicitly want; larger bundle.

### 3.2 Server vs client boundary (Decision 2)

Rule of thumb — **owner = "where does truth live?"**:

- **TanStack Query (server/remote/derived):** data that originates outside the app or is derived from outside-the-app sources and can be refetched/invalidated. Includes the WASM-derived **catalog** and **commits** (their truth is the Delta log/snapshot, not the UI), remote UC/Daxis/object-store listings (E1), and **local IDB CRUD** (`history`, `saved`) modeled as a "local server" (IDB is truth; the cache mirrors it; mutations invalidate).
- **Client store (intent/UI/session/runtime):** ephemeral UI (modals, toast), user intent and **selectors** (active connection/schema/table), **layout/tweaks**, the **connection registry** (config that parameterizes queries), the **run lifecycle**, **engine status** (event-driven push), the **WASM session handle**, and **local-access/re-grant status**.

**Anti-duplication rules (enforced in review + lint):**

1. Server data is never copied into the client store. Components read it via `useQuery`; non-render code reads via `queryClient.getQueryData(key)`.
2. The `run` slice obtains the active table's metadata at run time via `queryClient.getQueryData(catalogKey)` (replacing today's `tableMeta = catalog?.tables[0]` prop-drill), not by subscribing to or storing the catalog.
3. Selectors in the store (`connectionId`, `activeTableRef`) are inputs to queryKeys, not mirrors of query results. Switching a selector changes the active queryKey; the cache does the rest.
4. ESLint `no-restricted-imports` forbids `services/*` data-fetch imports inside `state/` slices (the store may import the `sessionManager` resource, not catalog/commits fetchers).

**Why the run lifecycle is not a TanStack mutation/query:** it is imperative (user presses Run), streams progress/log/metrics/fallback events, supports cancellation (`AbortController`), and paginates via "load more" with batch-identity checks (`sameQueryResultPageRun`). That is a state machine, not cache-keyed refetchable data. Results pagination _could_ use `useInfiniteQuery`, but the streaming + cancel + worker handshake make the run slice cleaner; we keep results out of the query cache.

### 3.3 queryKey design (Decision 3)

A single typed factory module (`query/keys.ts`); **never inline key arrays**.

```text
['catalog', connectionId]                                              // provider/connection root
['catalog', connectionId, 'catalogs']                                  // (E1) UC catalog namespace — lazy/infinite
['catalog', connectionId, catalogName, 'schemas']                      // (E1) lazy/infinite
['catalog', connectionId, catalogName, 'schemas', schemaId, 'tables']  // (E1) lazy/infinite
['catalog', connectionId, catalogName, 'schemas', schemaId, 'tables', tableId, 'metadata']
['catalog', connectionId, catalogName, 'schemas', schemaId, 'tables', tableId, 'commits']
['catalog', connectionId, 'table-derived']                             // E0 single-table Phase 1 (session-derived reference path)
['volumes', connectionId, catalogName, schemaId, volumeId, ...path]    // (E1) file listings — addresses a specific UC volume
['local', 'history']
['local', 'saved']
```

> **E1-informed amendment (2026-06-20):** the key hierarchy now carries a `catalogName` segment because a single UC/Daxis connection contains _many_ catalogs; the original `connection → schemas` shape could not cache per-catalog. The `volumes` key carries full volume identity (`catalog/schema/volume`) rather than just `connectionId + path`. `table-derived` is reserved for the Phase-1 session-derived reference path; E1 providers contribute **navigation** queries (provider REST, main-thread, _not_ session-derived) that are distinct from **read resolution** (session/`openDeltaTable`), which is the **E9** DataAccessResolver seam, not E1. See [E1 plan §3.1, §5.1](./2026-06-20-e1-catalog-providers-execution-plan.md).

Conventions:

- **Element 0 = stable domain literal** (`'catalog' | 'volumes' | 'local'`), typed as a union.
- **`connectionId` encodes provider kind + locator** (derived deterministically from the connection registry). Daxis, DirectUC, local, object-store are distinct `connectionId`s → cache isolation per profile; prefix-invalidating `['catalog', connectionId]` drops the whole tree on disconnect/refresh. **E1-informed amendment:** `connectionId` is **connection-root-scoped** (a UC workspace / Daxis tenant / bucket / local folder), which is _not_ the same as `query-source.ts` / `connect/store.ts` `tableSourceKeyFromParts` — that key is _table-scoped_ (it includes schema + table). Use a dedicated `connectionId = stableHash(kind, normalizedLocator)` for the cache root and keep `tableSourceKeyFromParts` for table identity within a connection (deepest key segment + run dedup). See [E1 plan §5.1](./2026-06-20-e1-catalog-providers-execution-plan.md).
- **Providers contribute `queryOptions()`** (TanStack v5) — a `{ queryKey, queryFn, staleTime, … }` bundle per resource. E1 plugs UC/Daxis providers in by implementing the same `queryOptions` factory shape; E2/E4 read the same cache via the same keys.
- **Per-resource policy:** structural catalog metadata `staleTime` long (~5 min) + `gcTime` longer; commits short; volume listings medium. Background refetch-on-focus off for big trees.
- **Retry predicate:** exponential backoff by default, but **no retry on 401/403/404** (auth/permission/not-found) — surfaced to UI for re-login (E6) or re-grant (local). A shared `shouldRetry` lives next to the factory.
- **Infinite queries** for `schemas`/`tables` with `pageParam` = provider cursor/token (ready for E1 large catalogs).

### 3.4 Persistence abstraction (Decision 4)

`persistence/` module:

- **`KeyValueStore(namespace, version)`** — async, IndexedDB-backed (`idb`), with `get/getAll/put/replaceAll/delete/clear`. A synchronous `localStorage` backend variant for FOUC-critical tiny config (e.g. `theme` read before first paint). Each namespace carries a schema `version` to drive migrations. `metadata.ts`’s three stores (`history`, `saved`, `workspace`) and `connect/store.ts`’s localStorage blob migrate onto this — **behind unchanged exported signatures** so callers don’t change in the same PR.
- **`HandleStore(namespace)`** — persists File System Access directory/file handles (structured-cloneable into IDB) and the metadata records that validate them. It owns the **read-files-from-disk lifecycle and re-grant flow** currently spread across `local-delta.ts`: persistence modes (`session_handles` | `persisted_directory_handle` | `metadata_only_reselect`), `queryPermission`/`requestPermission`, and size-match validation (`validateLocalDeltaTableAgainstRecord`).
- **OPFS stays worker-owned.** The engine’s session cache lives inside the worker; the abstraction documents it but does not manage it. Budget/usage continue to surface via `cache_metrics` events into the `engine` slice.
- **Settings namespace (`axon.settings.v1`).** The `settings` slice stores only **sparse overrides** (merged over schema-derived defaults at read time) via the synchronous `LocalConfigStore` so the appearance subset stays FOUC-safe (larger non-FOUC settings can later move to workspace IDB behind the same API). No migration of pre-existing config is retained. Remaining ad-hoc keys are normalized under the shared `axon.<domain>.<key>.v<n>` convention (`axon.connect.catalogs.v1` already conforms; the local-delta pointer is `axon.local-delta.active-id.v1`). These registries stay **domain-owned** but are **surfaced** (not re-homed) in Settings → Data Sources.

**Re-grant state machine** (lifted out of `App.tsx`’s `localAccessNeedsReselect` into a `localAccess` slice + `HandleStore`):

```
ready  --(reload)-->  check permission
check  --granted-->   re-collect entries -> validate vs record -> ready
check  --prompt-->    needs_regrant  --(user reselect)-->  validate -> ready
check  --metadata_only--> needs_regrant (must reselect to restore file access)
validate --size mismatch / missing file--> error(registry_unavailable)
```

The `local_delta` catalog query treats `needs_regrant` as a **typed error with a `reselect()` recovery action**, never an automatic retry.

**persistQueryClient — what is persisted vs memory-only:**

- **Persisted** (IDB persister, filtered `dehydrate`): only **cheap, successful, non-sensitive** entries — `['catalog', connectionId]` structural metadata for recently used connections, and `['local', …]` (already durable in IDB; the persisted cache is just a warm-start mirror). `maxAge` short for remote metadata; key includes a **build/version + schema-version buster** so deploys invalidate; **purge on 401** (E6).
- **Memory-only:** query **results** (not modeled as queries anyway), volume file previews/contents, anything large or **auth-scoped/PII**, and **`local_delta` catalog data** (handles may not be re-granted on next load — never resurrect a table from a stale persisted cache; reconstruct via the re-grant flow).
- **E8-informed boundary (Workspace Files vs persistence):** E0's persistence abstraction is for **app/session state** (registries, layout, settings, warm-start cache mirrors). E8's Workspace Files API is for **user file content** (volume/object/local files, and documents like saved queries). Keep these distinct: file _bytes/previews_ stay memory-only here; E8 owns file lifecycle/reads. The `HandleStore` (File System Access handles, §below) is the one shared seam E8's `LocalFolder` backend builds on. See [E8 in the strategy doc](../program/rich-lakehouse-workbench-strategy.md).
- **Graceful degradation:** if IDB is unavailable (private browsing), the persister no-ops and the app runs memory-only — matching the existing try/catch behavior in `metadata.ts`/`local-delta.ts`.

### 3.5 Routing (Decision 5)

**Adopt TanStack Router.** Routes:

| Path                                            | Purpose                                                                                                                                                                                      |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/`                                             | Editor (today’s `App`)                                                                                                                                                                       |
| `/connect`                                      | Connect flow (today’s `ConnectPage`)                                                                                                                                                         |
| `/catalogs`                                     | **Wire the dead route** — connection registry + catalog explorer (E0: minimal, renders the registry + Phase-1 derived tree; E1 fills it out)                                                 |
| `/catalog/$connectionId/$schema/$table`         | Deep link that **selects the active table** (route is the source of truth for selection; the `connections` slice mirrors it). Loader `ensureQueryData(catalogQueryOptions(...))` prefetches. |
| `/catalog/$connectionId/$schema/$table/insight` | **E7** table-insight surface (placeholder route in E0)                                                                                                                                       |
| `/saved/$savedId`                               | Open a saved query into a tab                                                                                                                                                                |

- **Typed search params** carry transient view state (active tab id, snapshot pin `?v=`) with validation.
- **Route-driven prefetch:** loaders call `ensureQueryData` against the same `queryOptions` used by components — navigation warms the cache (big win for E1 catalog trees).
- **Migration safety:** keep `editor/router.ts` working; introduce TanStack Router behind the same top-level routes first, expose a `navigate()` shim that maps old `Route` calls onto the new router, then add nested routes and retire the shim.
- **Rationale over History-API:** the current helper can’t express typed params, loaders, or nested selection; we’d hand-roll all of it. **React Router** is the fallback if we want fewer new concepts, but TanStack Router’s first-class TanStack Query integration and end-to-end type-safety fit this app best.
- **Cost:** ~12 KB gz + a learning curve; not size-gated.

### 3.6 Codegen seam (typed types / JSON schema / OpenAPI → TanStack)

E0 establishes the **boundary**, not the full pipeline (remote providers + protobuf are E1/E3):

- The provider boundary is a **`queryOptions` factory + typed fetcher** per resource. Whether those types/fetchers are hand-written (today, from `axon-browser-sdk.ts`) or generated later, they implement the same shape behind our `queryKeys` factory.
- For remote REST (UC, Daxis HTTP) in E1: generate with **`openapi-typescript` + `openapi-fetch`**, or a hooks generator (**Orval/Kubb**) that emits `queryOptions` + keys directly — wrapped so generated keys conform to our hierarchy.
- **E1-informed amendment — `SessionHttp` seam:** all remote fetchers go through one **session-aware fetch wrapper** (`credentials:'include'`, correlation id, `401→session-expired` / `403→blocked` / `404→not-found`, **never attaches Authorization from browser state**). E0's "purge cache on 401" hooks into this wrapper. E1 _defines_ the `SessionHttp` contract and E6 _implements_ the real session behind it; reserve the shape here so generated fetchers and the persistQueryClient 401-purge align. See [E1 plan §4.4](./2026-06-20-e1-catalog-providers-execution-plan.md).
- For protobuf (E3): **protobuf-es + Connect-ES**, with **Connect-Query** emitting TanStack Query integration. Our seam must accept Connect-Query `queryOptions` too.
- E0 deliverable: scaffold the generation config + an `npm run codegen` script and a **drift check** (generated output matches schema) mirroring the repo’s existing `verify_*` conformance scripts; implement the **local/fixture provider by hand** as the reference. Generated output lands in `src/generated/` (checked-in for review + offline builds; CI regenerates and diffs).
- **Do not block E0 on E3 (except config).** E0 uses existing `axon-browser-sdk.ts` types for the query layer. **Revision:** the **`axon/config/v1` proto module is the first consumer of the Buf seam, pulled forward ahead of E3 — and it is the base module that [E3A](./2026-06-20-e3a-provider-contract-surfaces-execution-plan.md) extends with the provider-seam messages (catalog / data-access / exec / fs).** It introduces scoped `buf.yaml`/`buf.gen.yaml` with a pinned plugin set — `protoc-gen-es` (protobuf-es types + JSON types) and `buf.build/bufbuild/protoschema-jsonschema` (`json-strict`, which already maps standard protovalidate rules to JSON Schema keywords) — emitting into `src/generated/config/`. Output is guarded by the E0 `npm run codegen:check` drift gate once that script lands (`codegen:config` is the deterministic offline mirror used where the BSR is unreachable; `codegen:config:buf` is the canonical `buf generate`). The package layout is stable so E3A adopts the same module unchanged and extends it for the provider-seam messages (catalog / data-access / exec / fs) and later `query-contract` / `browser-sdk`.

---

## 4. Migration path (strangler — app works at every step)

Each milestone is independently shippable and gated on: `tsc --noEmit`, ESLint (`--max-warnings=0`), and the existing Playwright suites (`editor-smoke`, `local-delta`, `public-gcs-live`) green, plus new Vitest where noted. `App.tsx` is migrated **slice-by-slice**, never in one cut.

### M0 — Scaffolding (no behavior change)

- Add deps: `@tanstack/react-query` (+ devtools, dev-only), `zustand`, `idb`, `@tanstack/react-query-persist-client` + IDB persister. Run the **dependency guardrail** to confirm none are denylisted; record JS bundle delta.
- Add `QueryClientProvider` at `main.tsx` root with default options (staleTime, `shouldRetry` predicate). Empty `state/`, `query/`, `persistence/` modules.
- Add **Vitest + jsdom + `fake-indexeddb`**; wire `npm test`. Keep Playwright.
- **Gate:** all existing suites green; no singletons removed.

### M1 — Persistence unification (leaf, lowest risk)

- Implement `KeyValueStore`/`HandleStore`. Migrate `metadata.ts` (history/saved/workspace) and `connect/store.ts` localStorage onto KV **behind identical public signatures**. Wrap the `local-delta` IDB registry + handles in `HandleStore`, preserving persistence modes + re-grant + size validation. Surface re-grant via a typed status (still consumed the same way by `App` for now).
- **Gate:** `local-delta` + `editor-smoke` Playwright pass (they exercise re-grant + persistence); new Vitest for KV/Handle (incl. legacy→IDB migration, handle permission stub, size-mismatch invalidation).

### M1.5 — Config codegen (pulls the Buf seam forward)

- Stand up the scoped Buf module: `buf.yaml`/`buf.gen.yaml` + `proto/axon/config/v1/settings.proto` (protovalidate). `buf generate` → protobuf-es types + JSON Schema (`json-strict`) checked into `src/generated/config/`; wire `codegen:config`/`codegen:config:buf` into `npm run codegen` + the drift check. Add the sibling JSON Forms **UI-schema** (keyed by field path).
- **Gate:** `codegen:check` (no drift) + `tsc`; dependency guardrail re-run for `ajv`/`ajv-formats`/`@jsonforms/*`.

### M2 — Client store; migrate UI-only state out of `App.tsx`

- Stand up Zustand slices: `layout` (sidebarW/resultsH — persisted), **`settings`** (layered JSON over `axon.settings.v1`: sparse overrides merged over schema-derived defaults; subsumes the old `layout.tweaks`, no legacy migration), `tabs` (tabs/activeTab/per-tab preferred+pin — persisted), `connections` (registry + `selectedTableRef` — persisted; replaces `loadConnectedCatalogs`/`saveConnectedCatalogs` effect and the upsert/remove logic), `ui` (toast + modals: save/connect/connected-panel/caps), `engine` (event-fed), `run` (status/result/metrics/events/plan/resultPageRun/loadingMore), `session` (WASM handle reference), `localAccess`.
  - **E8-informed amendment — tab `kind`:** the `Tab` type in [tabs.ts](../../apps/axon-web/src/state/slices/tabs.ts) is SQL-shaped today (`Tab.sql`, `updateActiveSql`). Reserve a tagged tab **`kind`** (e.g. `'sql' | 'file'`) so the SQL editor tab and **E8's non-SQL file tabs** (and E2's editor) can coexist in one tab strip without a later migration. E0 only needs to introduce the discriminant + keep today's `'sql'` behavior; E8 adds the `'file'` variant and its viewer. See [E8 in the strategy doc](../program/rich-lakehouse-workbench-strategy.md).
- Migrate `App.tsx` in PR-sized steps: **layout → connections → tabs → run last**. `services/query.ts` session singleton stays as-is; the `run` slice still calls `runQuery()`.
- **Critical perf fix:** route the 80 ms run-timer to a narrowly-selected `run.elapsed`; ensure only `Results` re-renders.
- **Wire orphaned config to the `settings` slice:** the theme/density/accent effect in `App.tsx` reads `settings.appearance`; `addTab` reads `settings.execution.defaultTarget` (replaces the hardcoded `browser_wasm`); `createQueryClient` reads `settings.engine` (staleTime/gcTime/maxRetries). Hybrid validation (AJV + protovalidate-es seam) with field-path error mapping ships with the slice but is lazy-loaded behind the Settings surface.
- **Gate:** Vitest for slice actions/selectors (tab-close active fallback; connected-catalog upsert/remove + selection reconciliation ported from `App`/`connect/store.ts`; run transitions); persist round-trip (rehydrate on reload); `editor-smoke` green.

### M3 — Server state → TanStack Query (catalog, commits, history, saved)

- `useCatalogQuery(querySource)` → `['catalog', connectionId, 'table-derived']`, queryFn = `getSession` + `deriveCatalogTable`. `useCommitsQuery` → `['catalog', connectionId, 'commits']` = `loadCommits`. Replace `subscribeCatalog`/`subscribeCommits` usage.
- `history`/`saved` become queries over IDB + mutations (`appendHistory`, `saveQuery`) that invalidate `['local',…]`; remove `App`’s manual `setHistory`/`setSaved` slicing.
- `engine` slice fed by a single idempotent worker-event→store bridge (ports `subscribeWorkerEvents`/`subscribeEngineStatus`).
- Wire **invalidation on source change / session discard** (`discardQuerySession` → invalidate `['catalog', connectionId]`).
- Write the **state-ownership reference** (the table in §2.1) as a living doc + the no-server-state-in-store review checklist.
- **Gate:** `editor-smoke`, `public-gcs-live`, `local-delta` green; Vitest for queryKeys factory (stable/hierarchical/prefix-invalidation), queryFn adapters with mocked session, retry predicate, invalidation-on-source-change.

### M4 — Routing (TanStack Router) + `/catalogs` + deep links

- Port `main.tsx` Router to TanStack Router (`/`, `/connect`, `/catalogs`) with the `navigate()` shim. Add nested `/catalog/$connectionId/$schema/$table` (selection source of truth) with loader prefetch; `/saved/$savedId`; `/catalog/.../insight` placeholder. Typed search params for active tab + snapshot pin.
- Build a **minimal `/catalogs` explorer** so the dead route renders (registry + Phase-1 derived tree); E1 expands it.
- **Settings surface:** a discoverable Settings dialog auto-rendered via JSON Forms (lazy chunk) into grouped sections (Appearance / Editor / Execution / Data Sources / Advanced), reachable from a top-bar gear + `⌘,` + a deep-linkable `/settings` route, with a raw-JSON editor bound to the same schema (Monaco upgrade deferred to E2). Folds in the old floating `TweaksPanel`; routes/removes the dead `Resync` / `Edit session` / `Run options` stub buttons.
- **Gate:** Playwright deep-link tests (table URL → correct active table; reload preserves; back/forward; `/catalogs` renders; `/connect` still works) + Settings discoverability and appearance-persists-across-reload specs.

### M5 — persistQueryClient + cache tuning + cleanup

- Add `persistQueryClient` (IDB persister) with the filtered `dehydrate` (persist only cheap successful `['catalog',…]` structure + `['local',…]`; exclude results/volumes/auth/`local_delta`). `maxAge` + version buster; purge hook for 401 (E6-ready). No-op when IDB unavailable.
- Finalize per-resource `staleTime`/`gcTime`/retry; scaffold infinite-query options for catalog trees (E1-ready).
- Delete dead singleton subscriber paths; remove all direct service subscriptions from `App`. Final boundary + codegen-seam docs; bundle-size check; full regression.

---

## 5. Risks and mitigations

| Risk                                                                                           | Likelihood/Impact | Mitigation                                                                                                                                                              |
| ---------------------------------------------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Re-render storms** from the 80 ms run timer + event stream re-rendering the monolithic `App` | High / High       | Selector-scoped subscriptions; `run.elapsed` isolated to `Results`; `subscribeWithSelector` for the worker bridge; split slices. Validate with React Profiler in M2.    |
| **StrictMode double-invoke** double-creates worker/bridge/subscriptions                        | Med / Med         | Idempotent init guards (the `sessionGeneration` guard already exists); a single module-scoped bridge setup; tests run under StrictMode.                                 |
| **Stale catalog after session discard / source switch**                                        | Med / Med         | Invalidate `['catalog', connectionId]` on `discardQuerySession`/source change; treat the session as the queryFn data source, not a parallel cache.                      |
| **Persisted cache resurrects an inaccessible `local_delta` table** (handle not re-granted)     | Med / High        | Never persist `local_delta` catalog data; reconstruct via the re-grant flow; `needs_regrant` is a typed error with `reselect()`.                                        |
| **Persisting stale / over-privileged UC metadata** (correctness + ADR-0002 posture)            | Med / High        | Short `maxAge`, background refetch, purge on 401, never persist auth-scoped entities; coordinate with E6.                                                               |
| **Bundle growth** (TanStack Query + Router + Zustand ≈ 25–30 KB gz)                            | Low / Low         | Not size-gated; dev-only devtools; lazy-load route chunks. Re-run dependency guardrail on every dep add. The 750 KB **WASM** gate is unaffected (frontend-only effort). |
| **IndexedDB unavailable** (private mode)                                                       | Low / Med         | Graceful no-op persistence + memory-only cache (matches existing behavior); covered by tests.                                                                           |
| **Regressions migrating the 1,150-line `App.tsx`**                                             | High / High       | Strangler, slice-by-slice PRs; each independently green on Playwright + new Vitest; run lifecycle migrated last.                                                        |
| **Scope creep into E1/E2/E5**                                                                  | Med / Med         | E0 ships seams + local/fixture provider only; provider interface and Monaco explicitly deferred.                                                                        |
| **Codegen/contract churn vs E3**                                                               | Low / Med         | E0 only defines the `queryOptions` seam over existing `axon-browser-sdk.ts` types; generation lands incrementally with a drift check.                                   |

---

## 6. Test strategy

**New runner:** Vitest (jsdom) for unit/integration; `fake-indexeddb` for IDB; `@testing-library/react` `renderHook` for query/store hooks. Test `QueryClient` uses `retry:false`, `gcTime:0`.

- **Persistence:** KV migrations (legacy localStorage→IDB for history/saved), namespace versioning; `HandleStore` re-grant state machine with mocked `FileSystemDirectoryHandle` (`queryPermission`/`requestPermission` stubs) and size-mismatch/missing-file invalidation; IDB-unavailable no-op.
- **Client store:** slice actions/selectors as pure units — tab-close active fallback, connected-catalog upsert/remove + selection reconciliation (port the assertions implicit in `App`/`connect/store.ts`), run-lifecycle transitions (idle→running→done/error/cancel, load-more batch identity via `sameQueryResultPageRun`). Persist round-trip rehydration.
- **TanStack layer:** queryKeys factory (stability, hierarchy, prefix-invalidation); queryFn adapters over a mocked `sessionManager`; `shouldRetry` (no retry on 401/403/404); invalidation on source change/session discard; `persistQueryClient` `dehydrate` filter (results/volumes/auth/`local_delta` excluded), `maxAge`/version buster.
- **Configuration:** Vitest for schema-derived defaults, JSON↔override round-trip (`mergeSettings`/`diffToOverride`), AJV validation against the generated schema (enum/pattern/range/`additionalProperties`, field-path mapping) and the protovalidate-es CEL seam, and the `settings.execution.defaultTarget`→`addTab` wiring. Playwright for Settings discoverability (top-bar gear + `/settings`), appearance persistence across reload, and raw-JSON schema rejection.
- **Routing:** unit tests for typed search-param parsing; Playwright E2E for deep links (table URL → active table; reload persists; back/forward; `/catalogs` renders; `/connect` unaffected) and persistence-across-reload (run a query → reload → history present, layout persisted).
- **E2E (existing, extended):** keep `editor-smoke`, `local-delta` (re-grant), `public-gcs-live`. Add deep-link + reload-persistence + local re-grant-drives-status specs.
- **CI gates:** keep the WASM artifact budget + dependency guardrails. Add an ESLint `no-restricted-imports` rule enforcing the no-server-state-in-store boundary, and a **codegen drift check** (stubbed in E0, enforced when generation lands) mirroring `verify_*` conformance scripts.

---

## 7. Bundle-size note

Frontend-only effort. Net new JS ≈ 25–30 KB gz (`@tanstack/react-query` ~13, `@tanstack/react-router` ~12, `zustand` ~1, `idb` ~2); devtools are dev-only. **No impact on the gated `browser_engine_worker.wasm` (≤ 750 KB) budget.** Every dependency addition is re-checked against `verify_browser_dependency_guardrails.sh` (cloud-SDK denylist + secret-marker scan).

**Configuration libs are lazy-loaded.** `@jsonforms/*` + `ajv`/`ajv-formats` (and any future `protovalidate-es`) ship only in the Settings chunk via `lazy(() => import('./settings/SettingsDialog.tsx'))`, keeping them out of the main/FOUC bundle; protobuf-es types are emitted as `import type` where possible. The config libs pass the dependency guardrail npm-package denylist. (Record the exact settings-chunk byte delta from a full `npm run build`, which requires the wasm toolchain.) Note: `@jsonforms/*` is currently pinned to a prerelease available through the internal registry mirror; bump to the stable 3.x line once mirrored.

---

## 8. Open questions to confirm before/at kickoff

1. **`connectionId` derivation** — reuse `tableSourceKeyFromParts` (canonical key) directly, or introduce a dedicated stable connection id in the `connections` slice? (Affects cache isolation + deep-link URLs.) **E1 recommendation:** introduce a dedicated connection-root id; `tableSourceKeyFromParts` is table-scoped and unsuitable as the cache root (see [E1 plan §5.1](./2026-06-20-e1-catalog-providers-execution-plan.md)). **Config interplay (D7):** the same connection-root id is the natural key for a future per-connection scoped settings layer (`defaults < user < workspace < connection`); the `settings` slice reserves the layered seam now and implements the `user` layer only.
2. **History/Saved as TanStack-over-IDB vs a persisted store slice** — recommended TanStack-over-IDB for mutation/invalidation ergonomics; confirm given they’re local-only.
3. **Run lifecycle: plain slice vs XState** — start as a plain slice; adopt XState only if transitions/cancellation/load-more get unwieldy.
4. **Router: TanStack Router vs React Router** — recommended TanStack Router; confirm appetite for the new concepts vs React Router familiarity.
5. **Generated code checked-in vs CI-only** — recommended checked-in + CI drift check; confirm.
6. **Selection source of truth** — route params (recommended) vs store slice when both `/catalog/...` URL and in-app selectors exist; confirm route-wins + store-mirrors.
