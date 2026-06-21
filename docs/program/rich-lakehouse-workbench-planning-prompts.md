# Rich Lakehouse Workbench — Planning-Session Prompts

- Status: Draft
- Date: 2026-06-20
- Scope: Self-contained prompts to seed a dedicated planning session for each effort (E0–E7) in the [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md). Each prompt can be pasted into a fresh planning chat.
- Related:
  - [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md)

> How to use this file: open a new planning-mode chat for one effort, paste its prompt verbatim, and let the session read the referenced files and produce a detailed execution plan (milestones, risks, test/gate strategy). The prompts are intentionally self-contained — they restate the framing so the session does not need prior context. Each assumes the "pluggable provider model" described in the strategy doc: CatalogProvider, Identity/SessionProvider, AuthorizationProvider (PDP), and ExecutionProvider seams, with Daxis/contract-first as one profile and "direct UC + in-app Cedar + local/remote execution" as another.

---

## E0 — Frontend Foundation: State, Routing, Persistence

```text
You are planning the foundational frontend refactor for the Axon web app (apps/axon-web), which we are evolving into a pluggable, provider-driven rich lakehouse workbench. This effort (E0) is the substrate that E1, E2, E5, and E7 depend on.

Context: state today is hand-rolled module-singleton subscribe/snapshot stores in apps/axon-web/src/services (see catalog.ts, engine.ts, snapshot.ts, history.ts, saved.ts, query.ts) plus ~25 useState hooks in apps/axon-web/src/editor/App.tsx. There is no state library. Routing is a minimal History-API helper in apps/axon-web/src/editor/router.ts, and the /catalogs route is declared but never rendered. Persistence is split across IndexedDB (apps/axon-web/src/services/metadata.ts), localStorage (apps/axon-web/src/editor/connect/store.ts), and OPFS / File System Access handles (apps/axon-web/src/services/local-delta.ts).

Read these first: apps/axon-web/src/services/query.ts, catalog.ts, engine.ts, metadata.ts, local-delta.ts; apps/axon-web/src/editor/App.tsx, router.ts, main.tsx; apps/axon-web/src/editor/connect/store.ts.

Goal: design a scalable, typed state architecture that splits state by ownership:
- Server/remote state -> TanStack Query, with a queryKey hierarchy (e.g. [provider, catalogId, 'schemas', schemaId, 'tables']), caching, background refetch, staleTime control, dedup, retry/backoff, and infinite/paginated queries for large catalog trees. This cache is also consumed by E2 (IntelliSense) and E4 (policy entities).
- Client/UI + session state -> a lightweight store (compare Zustand vs Redux Toolkit vs XState; we lean Zustand + slices). It owns editor tabs/buffers, active connection + selectors, layout/tweaks, query-run lifecycle, and the long-lived WASM session handle in query.ts.

Key decisions to resolve and recommend: (1) client-store library choice with rationale; (2) the precise server-vs-client state boundary and how to avoid duplicating server state into the client store; (3) queryKey design conventions across providers; (4) the unified persistence abstraction over IndexedDB/localStorage/OPFS/File-System-Access, including the "read files from disk" lifecycle and re-grant flows, and which TanStack caches are persisted (persistQueryClient) vs memory-only; (5) routing approach — wire the dead /catalogs route and support deep links to catalog/schema/table, saved queries, and the table-insight surface; evaluate TanStack Router (route-driven prefetch/loaders) vs keeping the History-API router.

Constraint: this is a frontend-only effort; do not change Rust contracts here. Be mindful of WASM bundle/size gates already in the repo.

Deliverable: a detailed execution plan with milestones, a migration path off the current singletons that keeps the app working at each step, risks, and a test strategy (including how state/persistence is tested).
```

---

## E1 — Pluggable Catalog Providers and Catalog Explorer

```text
You are planning the catalog integration effort (E1) for the Axon web app. We are generalizing Axon into a pluggable provider model: a CatalogProvider seam with DirectUnityCatalog (UC REST), Daxis (ReadAccessPlan), LocalDelta, and ObjectStore implementations. This builds on E0 (state/TanStack Query) and depends on E6 (auth/session) for remote API access.

Context: the connect flow today models four source types but only LocalDelta and public ObjectStore (GCS) execute end-to-end; Unity Catalog and Delta Sharing are stubbed behind a feature flag. The runtime "catalog" is currently single-table (Phase 1).

Read these first: apps/axon-web/src/editor/connect/ConnectModal.tsx, data.ts, types.ts, store.ts, ConnectedCatalogs.tsx; apps/axon-web/src/services/query-source.ts, catalog.ts, object-storage.ts, connector-features.ts, types.ts; crates/query-contract/src/lib.rs (ReadAccessPlan family, descriptors); docs/program/browser-uc-brokered-runtime-contract.md; docs/program/browser-owned-descriptor-materialization.md; docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md.

Goal: define the CatalogProvider interface and ship a DirectUnityCatalog provider (list catalogs/schemas/tables/views/volumes + table metadata via UC REST) alongside the existing Daxis/local/object-store paths. Each provider exposes its reads as TanStack Query options (queryKey factory + fetcher) so caching/refresh/dedup/pagination apply uniformly. Deliver: multi-catalog connection registry + catalog/schema/table selectors in sidebar and editor; lazy/paginated catalog tree navigation; volumes browsing and file reading (parquet/csv/json/image preview); a catalog metadata cache feeding E2 and E4.

Key decisions to resolve: (1) the CatalogProvider TypeScript interface and how Daxis's ReadAccessPlan model maps onto the same interface as direct UC REST; (2) where UC credential exchange / CORS lives given ADR-0002 (no cloud secrets in browser) — coordinate with E6's Envoy session seam; (3) the multi-catalog data model and how selectors flow into query execution and the editor; (4) volumes/file-reading scope and how it reuses the existing object-store range-read path; (5) how this generalizes ADR-0008 / the UC brokered contract (Daxis as one profile) — decide whether to amend ADR-0008 or add a new ADR.

Constraint: never place cloud or catalog secrets in browser code (ADR-0002). Preserve the Daxis contract-first profile as a first-class provider.

Deliverable: a detailed execution plan with milestones, the provider interface sketch, risks (esp. CORS/auth and large-catalog performance), and a test strategy.
```

---

## E2 — Editor Modernization: Monaco + Catalog-Aware SQL IntelliSense

```text
You are planning the SQL editor modernization effort (E2) for the Axon web app. Depends on E0 (state) and E1 (catalog metadata cache).

Context: the live editor is a custom <textarea> overlaid on a highlighted <pre>, with a hand-rolled tokenizer and an incomplete autocomplete popup. CodeMirror 6 is listed as a dependency but is not actually used. There is no hover/IntelliSense/diagnostics service. A table-detail flyout exists in the sidebar but not in the editor.

Read these first: apps/axon-web/src/editor/components/Editor.tsx; apps/axon-web/src/editor/lib/highlight.ts, format.ts; apps/axon-web/src/editor/components/Sidebar.tsx (TableDetailFlyout); apps/axon-web/src/services/types.ts (Catalog/CatalogTable); apps/axon-web/package.json (editor deps).

Goal: replace the custom editor with a real code editor and a catalog-aware SQL language service that provides context-aware completion (schemas/tables/columns ranked by FROM), hover cards (table/column type, partitioning, row count, latest commit), signature help, and pre-run diagnostics that surface unsupported-SQL / fallback reasons as squiggles. Support multi-tab editing with per-tab catalog/connection context, and a great text-editing UX.

Key decisions to resolve and recommend: (1) Monaco vs finishing CodeMirror 6 — weigh IntelliSense/hover/diagnostics richness, bundle size, worker integration, and theming, and recommend one; (2) the SQL language-service architecture and how it consumes the E1 TanStack catalog metadata cache (sync vs async completion sources); (3) how diagnostics learn the supported-SQL envelope / fallback reasons (reuse runtime capability info vs a lightweight client-side SQL parse); (4) multi-tab editing and per-tab context model in the E0 client store; (5) bundle-size impact and lazy-loading strategy.

Deliverable: a detailed execution plan with milestones, the language-service design, risks (bundle size, completion latency), and a test strategy (including how completion/hover/diagnostics are tested).
```

---

## E3 — Contract IDL and Multi-Target Runtime (Buf-managed protobuf)

```text
You are planning the contract-IDL and multi-target runtime effort (E3) for Axon. We are moving the control/contract plane to protobuf as the single source of truth, managed with the Buf toolchain, and defining a transport-agnostic query engine surface that supports multiple execution backends.

Context: contracts today are serde JSON types in crates/query-contract/src/lib.rs and worker IPC envelopes in crates/browser-sdk/src/lib.rs, with a large hand-maintained TypeScript mirror in apps/axon-web/src/axon-browser-sdk.ts. There is no protobuf anywhere. Arrow IPC already crosses the worker boundary as bytes. There is a documented no-go on using DataFusion/Substrait proto as the hot-path IR.

Read these first: crates/query-contract/src/lib.rs and crates/query-contract/src/delta_protocol_features.rs; crates/browser-sdk/src/lib.rs (BrowserWorkerCommand / response / event envelopes); apps/axon-web/src/axon-browser-sdk.ts (the hand-written TS mirror); apps/axon-web/src/sandbox-query-worker.ts; apps/axon-web/src/lib.rs (wasm-bindgen surface); crates/query-router/src/lib.rs; crates/native-query-runtime/src/lib.rs; docs/program/browser-datafusion-size-audit.md; crates/query-contract/schemas/ (existing JSON schema / OpenAPI artifacts).

Goal: standardize on the Buf toolchain (buf CLI, buf.yaml/buf.gen.yaml, BSR for modules/deps, buf lint + breaking-change detection in CI) and migrate the contract surface to protobuf, retiring the hand-maintained TS mirror. Codegen: TypeScript via @bufbuild/protobuf (protobuf-es) + Connect-ES; Rust via Buf-managed plugins. Define a transport-agnostic QueryEngine RPC surface (Connect) and implement ExecutionProviders: BrowserWasm (today), Tauri (native via IPC reusing native-query-runtime), and RemoteService. No servers are implemented in this effort — contracts + client/runtime bindings only.

Key decisions to resolve: (1) the Buf-vs-Prost question — Buf is the toolchain, but the Rust runtime is produced by a codegen backend (most likely a prost-based plugin via buf generate); confirm whether to keep prost under Buf or evaluate alternatives, and verify it builds for wasm32-unknown-unknown; (2) proto package/module layout (query-contract core types, browser-sdk worker IPC, control-plane APIs); (3) how to stage the migration with round-trip parity tests against current JSON before retiring the TS mirror; (4) the transport-agnostic QueryEngine surface and how BrowserWasm/Tauri/RemoteService implement it; (5) how the inner wasm-bindgen JSON-string seam (snapshot_json/request_json) is affected.

Constraint: Arrow IPC stays the data plane; protobuf is the control plane only (respect the size-audit no-go on proto IR). Coordinate auth metadata on RPCs with E6.

Deliverable: a detailed execution plan with milestones, the proto module layout, a safe migration/parity strategy, risks, and a CI contract-gate plan.
```

---

## E4 — In-App Authorization: Cedar Policy Engine

```text
You are planning the in-app authorization effort (E4) for Axon: integrating the Cedar policy engine as a pluggable authorization provider (PDP). Depends on E1 (entity model), E3 (decision contract types), and E6 (authenticated principal).

Context: today policy is delegated entirely to the external Daxis control plane (ADR-0008); Axon consumes structured outcomes (sql_fallback_required, blocked) and never makes authorization decisions. Under the pluggable provider model, the standalone profile needs an in-app PDP, while the Daxis profile keeps Daxis authoritative (Cedar advisory).

Read these first: docs/adr/ADR-0008-daxis-browser-read-compute-contract.md; docs/program/browser-uc-brokered-runtime-contract.md (governance/fallback taxonomy); crates/query-contract/src/lib.rs (FallbackReason, BlockedReadPlan, ReadAccessPlanReason); the strategy doc docs/program/rich-lakehouse-workbench-strategy.md (provider seams).

Goal: integrate Cedar (cedar-policy Rust crate compiled to WASM) as a pluggable PDP for client-side authorization in standalone mode (advisory in Daxis mode). Model entities/schema (principal, catalog, schema, table, volume; actions browse/query/export), wire a decision point into the request path, and map allow/deny onto the existing structured taxonomy.

Key decisions to resolve: (1) Cedar-in-WASM feasibility, bundle-size impact, and whether the PDP runs on the main thread or in the worker; (2) the entity/schema model and how it is populated from E1 catalog metadata and the E6 authenticated principal; (3) where the decision point sits in the request/execution path and how denials map to FallbackReason/BlockedReadPlan; (4) the authoritative-vs-advisory contract per provider profile, and reconciliation with ADR-0008 (amend vs new ADR); (5) how policies are authored/loaded/versioned and whether they sync from a provider.

Constraint: in governed (Daxis) deployments, in-app Cedar must never be presented as a substitute for server-side enforcement of governed shapes (row filters, column masks, views).

Deliverable: a detailed execution plan with milestones, the Cedar schema/entity design, risks (size, correctness, advisory-vs-authoritative boundaries), and a test strategy.
```

---

## E5 — Arrow-Native Visualization Layer

```text
You are planning the Arrow-native visualization effort (E5) for the Axon web app. Depends on E0 (state); foundation for E7.

Context: query results already cross the worker boundary as Arrow IPC bytes (BrowserWorkerSuccessEnvelope carries Arrow IPC; large results are not row JSON). However, the current UI converts a bounded preview into JS cells for the results grid — the opposite of visualizing Arrow directly. We want to query as Arrow, keep it as Arrow in the environment, and visualize it without converting to TypeScript-native types.

Read these first: apps/axon-web/src/editor/components/Results.tsx; apps/axon-web/src/sandbox-query-worker.ts and apps/axon-web/src/axon-browser-sdk.ts (Arrow IPC result envelope, preview conversion); apps/axon-web/src/services/query.ts, query-pagination.ts; docs/program/browser-embedding-deployment.md; docs/plans/2026-05-16-parquet-viewer-lessons-implementation-plan.md.

Goal: adopt Apache Arrow JS to consume the Arrow IPC bytes directly, eliminating row-JSON materialization for display. Build a virtualized table grid that reads Arrow vectors directly and a charting layer over Arrow columns, with auto-suggested encodings from schema and a "chart from result" flow. Evaluate canvas/WebGL for large results.

Key decisions to resolve and recommend: (1) Arrow JS runtime/library choice and how it integrates with the worker result envelope (transfer vs copy, BigInt64 handling for Int64); (2) virtualized grid approach reading Arrow vectors directly (and how it handles large results and pagination); (3) charting library/approach that can read Arrow columns without row materialization, and the encoding auto-detection model; (4) when, if ever, conversion to JS types is still required (e.g. tooltips) and how to bound it; (5) bundle-size impact and lazy-loading.

Deliverable: a detailed execution plan with milestones, the Arrow data-flow design (worker -> grid/charts), risks (size, large-result performance, type handling), and a test strategy.
```

---

## E6 — Authentication and Session (Envoy proxy + WASM credential propagation)

```text
You are planning the authentication/session effort (E6) for Axon. This is cross-cutting: it pairs with E1 (remote API auth) and E3 (auth metadata on RPCs) and informs E4 (authenticated principal). The hardest part is propagating credentials into the WASM query runtime.

Context: assume an Envoy proxy fronts the deployment — it serves the static bundle and terminates login, maintaining a browser session (e.g. OIDC via Envoy's oauth2 filter / ext_authz, with a session cookie), so the browser never holds cloud or UC secrets (aligns with ADR-0002). The query runtime makes its own HTTP range reads from inside a Web Worker via crates/wasm-http-object-store. Threaded/SIMD WASM bundles require COOP/COEP cross-origin isolation per the bundle manifest.

Read these first: crates/wasm-http-object-store/src/lib.rs (range-read fetch path); apps/axon-web/src/axon-browser-sdk.ts (bundle manifest, COOP/COEP requirements, descriptor types); crates/query-contract/src/lib.rs (BrowserAccessMode / signed_url vs proxy access modes; resolver request/response types); apps/axon-web/src/services/object-storage.ts, local-delta.ts; docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md; docs/program/browser-embedding-deployment.md (CSP/COOP/COEP/CORS guidance).

Goal: design the session model for the app shell (UC/Daxis API calls via E1 TanStack Query fetchers using same-origin requests through Envoy with credentials: 'include', 401/redirect-to-login handling, session expiry/refresh surfaced to the UI), AND resolve how the WASM worker's range-read fetches carry the session.

Key decisions to resolve: (1) credential propagation into the worker fetch path — ambient cookie/session on same-origin proxied reads vs explicit bearer-token injection threaded into the object-store/descriptor layer; (2) how this maps onto the existing signed_url vs proxy access modes in query-contract; (3) the worker-vs-main-thread credential boundary; (4) the COOP/COEP cross-origin isolation tension with credentialed cross-origin fetch and bundle/asset loading — design the Envoy + CORS/COOP/COEP header story together (single-origin Envoy is the simplest reconciliation); (5) keeping the SessionProvider seam pluggable so Tauri/native (OS credentials) and LocalDelta (no auth) profiles do not assume a browser session.

Constraint: never place cloud or catalog secrets in browser code (ADR-0002). Assume the Envoy proxy exists; we are not implementing the proxy/login service in this effort, but we must define the contract the app and WASM runtime rely on.

Deliverable: a detailed execution plan with milestones, the session/credential-propagation design (incl. a diagram of the worker fetch path), the header/origin layout for COOP/COEP + auth, risks, and a test strategy.
```

---

## E7 — Delta Table Insight and Health Surface

```text
You are planning the Delta table insight/health surface effort (E7) for the Axon web app. Depends on E5 (Arrow-native plotting) and E1 (table selection/metadata); reuses the existing snapshot/log reconstruction.

Context: Axon already reconstructs Delta snapshots in the browser and inspects Parquet footers. We want a dedicated UI surface that helps users understand a Delta table from its Delta log — not query results, but the table's structure and health: enabled features, per-column stats boundaries plotted, and health signals derived from the log.

Read these first: crates/wasm-delta-snapshot/src/lib.rs (log replay -> add actions with stats); crates/query-contract/src/delta_protocol_features.rs (DeltaProtocolFeature, KNOWN_DELTA_PROTOCOL_FEATURES) and crates/query-contract/src/lib.rs (CapabilityReport, ParquetInspectionSummary); apps/axon-web/src/lib.rs (inspect_parquet, preflight_parquet_metadata_for_targets, resolve_delta_snapshot_from_manifest); apps/axon-web/src/services/snapshot.ts (commits feed), metadata.ts; apps/axon-web/src/editor/components/Sidebar.tsx.

Goal: design a table-insight surface that renders: (1) metadata and enabled features — schema, partitioning, table properties, and enabled Delta protocol/reader-writer features as a capability/feature view (supported vs native-only vs unsupported per feature: deletion vectors, column mapping, CDF, timestamp-ntz, etc.); (2) stats-boundary plots — per-column min/max boundaries across add-file stats and Parquet footer stats, per-file value ranges, range overlap (proxy for data-skipping/pruning effectiveness), null-count and row-count distributions, and stats-coverage gaps; (3) health signals — file-size histogram (small-file problem), file count, partition cardinality/skew, commit/log growth over the snapshot-version timeline, checkpoint cadence, tombstone/removed-file counts.

Key decisions to resolve: (1) which stats/features are extractable from the existing snapshot reconstruction and Parquet inspection vs what needs new extraction in the WASM layer; (2) which health signals to plot first and how to compute them efficiently (avoid scanning data; use log + footer metadata); (3) how the surface reuses the E5 Arrow-native plotting layer; (4) the route/surface design via E0 routing and how a table is selected from E1; (5) performance for tables with many files/commits.

Constraint: keep it mostly read-only over metadata Axon already reconstructs; avoid full data scans.

Deliverable: a detailed execution plan with milestones, the data-extraction map (source -> signal -> plot), risks (large logs, missing stats), and a test strategy.
```
