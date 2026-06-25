# Rich Lakehouse Workbench — Planning-Session Prompts

- Status: Draft
- Date: 2026-06-20
- Scope: Self-contained prompts to seed a dedicated planning session for each effort (E0–E9; E3 is phased into E3A/E3B) in the [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md). Each prompt can be pasted into a fresh planning chat.
- Related:
  - [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md)

> How to use this file: open a new planning-mode chat for one effort, paste its prompt verbatim, and let the session read the referenced files and produce a detailed execution plan (milestones, risks, test/gate strategy). The prompts are intentionally self-contained — they restate the framing so the session does not need prior context. Each assumes the "pluggable provider model" described in the strategy doc: six seams — CatalogProvider (discovery only), Identity/SessionProvider, AuthorizationProvider (PDP), DataAccessResolver (read resolution), ExecutionProvider (run/preview), and FileSystemProvider/Workspace — with Daxis/contract-first as one profile and "direct UC + in-app Cedar + local/remote execution" as another.

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
You are planning the catalog integration effort (E1) for the Axon web app. We are generalizing Axon into a pluggable provider model. E1 owns the DISCOVERY-ONLY CatalogProvider seam plus the Catalog Explorer surface: DirectUnityCatalog (UC REST), Daxis (catalog listing), LocalDelta, and ObjectStore implementations that answer "what catalogs/schemas/tables/volumes exist and what is their metadata." This builds on E0 (state/TanStack Query) and E3A (provider contract messages in proto), and depends on E6 (auth/session) for remote API access.

IMPORTANT scope boundary: E1 does NOT resolve how to read a table's bytes or run a query. Read resolution (the ReadAccessPlan / descriptor / fallback / blocked family) is the DataAccessResolver seam and execution (run / sample / preview) is the ExecutionProvider seam — both owned by E9. The Explorer's "Sample data" and "Open in SQL editor" actions hand a table ref to E9; design that handoff, not the resolution/execution itself.

Context: the connect flow today models four source types but only LocalDelta and public ObjectStore (GCS) execute end-to-end; Unity Catalog and Delta Sharing are stubbed behind a feature flag. The runtime "catalog" is currently single-table (Phase 1).

Read these first: apps/axon-web/src/editor/connect/ConnectModal.tsx, data.ts, types.ts, store.ts, ConnectedCatalogs.tsx; apps/axon-web/src/services/query-source.ts, catalog.ts, object-storage.ts, connector-features.ts, types.ts; docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md (the generated discovery messages E1 consumes); docs/plans/2026-06-20-e9-execution-provider-execution-plan.md (the read-resolution/execution handoff); docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md.

Goal: define the discovery-only CatalogProvider interface and ship a DirectUnityCatalog provider (list catalogs/schemas/tables/views/volumes/functions/models + table metadata via UC REST) alongside the existing Daxis/local/object-store paths. Each provider exposes NAVIGATION as plain async methods; a TanStack Query layer wraps those methods with query keys, caching, refresh, dedup, and pagination. Node/metadata shapes are the E3A-generated proto messages once they land (DirectUC maps UC's OpenAPI types into them). Deliver: multi-catalog connection registry + catalog/schema/table selectors in sidebar and editor; lazy/paginated catalog tree navigation; volumes surfaced as catalog objects (list + metadata via UC /volumes and /volumes/{name}); a catalog metadata cache feeding E2 and E4. Note: browsing the files inside a volume, file preview, and file editing are OUT OF SCOPE for E1 — they belong to E8 (Workspace Files & Volumes). E1 only references a volume as an object; it never opens it.

Key decisions to resolve: (1) the discovery-only CatalogProvider TypeScript interface over the E3A messages, and how a richly-browsable provider (UC) vs flat providers (local/object-store) degrade gracefully via a capabilities() shape; (2) where UC credential exchange / CORS lives given ADR-0002 (no cloud secrets in browser) — coordinate with E6's Envoy session seam (metadata plane only); (3) the multi-catalog data model and how route-owned selectors produce a table ref that is handed to E9 (do NOT design read resolution or execution here); (4) the volume-as-object model (list + metadata) and the seam where a volume hands off to E8 for file browsing (do NOT design the Volumes Files API here); (5) the Catalog Explorer surface (object tree + detail panes for Tables/Volumes/Functions/Models) and how it consumes the navigation queries.

Constraint: never place cloud or catalog secrets in browser code (ADR-0002). Discovery only — keep read resolution + execution out of E1 (E9) and file-level volume access out of E1 (E8). Preserve the Daxis profile as a first-class catalog-listing provider.

Deliverable: a detailed execution plan with milestones, the discovery-only provider interface sketch, the E9/E8 handoff seams, risks (esp. CORS/auth and large-catalog performance), and a test strategy.
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

## E3A — Provider Contract Surfaces (proto messages, pulled forward)

```text
You are planning E3A for Axon: defining the proto MESSAGE contracts for every pluggable provider seam, early (right after E0), so the seam efforts (E1 discovery, E9 execution + data-access, E8 files, E4 decisions) build on one reviewable, codegen'd contract surface instead of hand-written TypeScript interfaces that would later be re-done in proto. This is the message layer of E3, pulled forward; E3B is the later migration + runtime track.

Context: E0 plans to stand up a scoped Buf module for app config (apps/axon-web/buf.yaml, buf.gen.yaml, proto/axon/config/v1/settings.proto), codegen'd to protobuf-es TS + JSON Schema and checked in behind a drift check. E3A should extend that module after E0 M1.5 lands; if E0 has not landed yet, treat those paths as planned deliverables and use the E0 plan as the source of truth. Read-resolution and object-grant contracts already exist as serde/JSON-schema artifacts in crates/query-contract/schemas/ (read-access-plan.schema.json, object-grants.openapi.json). UC REST has a vendored OpenAPI spec (per E1) and is NOT re-proto'd here.

Read these first: docs/plans/2026-06-20-e0-frontend-foundation-execution-plan.md (the planned config Buf seam); if E0 M1.5 has landed, also read apps/axon-web/buf.yaml, buf.gen.yaml, proto/axon/config/v1/settings.proto, and scripts/config-codegen*.mjs; crates/query-contract/src/lib.rs (ReadAccessPlan family, descriptors, ObjectGrant routes) and crates/query-contract/schemas/; crates/browser-sdk/src/lib.rs (BrowserWorkerCommand / response / event envelopes); apps/axon-web/src/services/query-source.ts, query.ts; docs/program/rich-lakehouse-workbench-strategy.md (the six seams); the E1 and E9 plans (the consumers).

Goal: define, in one Buf module tree extending the axon/config/v1 layout, the proto MESSAGE contracts for all seams — catalog discovery nodes/metadata (CatalogNode/SchemaNode/TableNode/TableMetadata/VolumeNode/FunctionNode/ModelNode + list/page envelopes); data-access resolution + descriptors (port read-access-plan.schema.json + object-grants.openapi.json: ReadAccessPlan family, BrowserHttpSnapshotDescriptor, fallback/blocked reasons); execution request/response + worker IPC envelopes (from browser-sdk); filesystem entries (E8). Define proto SERVICES only at true transport boundaries (the execution worker IPC + future RemoteService); the local discovery/file seams stay TS interfaces that consume generated messages. Codegen protobuf-es TS through the E0 buf.gen.yaml once it exists, and add a Rust backend — PREFER buffa (Anthropic's pure-Rust, no_std protobuf lib; buf remote plugin buf.build/anthropics/buffa; built-in canonical JSON, so no pbjson) over prost, CONTINGENT on verifying it builds for wasm32-unknown-unknown and fits the worker budget; fall back to protoc-gen-prost if not. Check output in + drift-check it like config codegen.

Key decisions to resolve: (1) the proto package/module layout for the seam messages (e.g. axon/catalog/v1, axon/dataaccess/v1, axon/exec/v1, axon/fs/v1) and how it sits beside axon/config/v1; (2) which surfaces get proto services vs stay message-only TS seams; (3) how the normalized catalog messages relate to the vendored UC OpenAPI types (mapping layer, not duplication); (4) how to port the existing serde/JSON-schema contracts (read-access-plan, object-grants) to proto with round-trip fidelity; (5) the "living draft" policy — keep breaking changes allowed (relaxed breaking gate) until E1/E9/E8 adopt, then tighten; (6) the Rust backend: run an early buffa/wasm spike (buf.build/anthropics/buffa generated Rust compiled for wasm32-unknown-unknown within the worker budget) and adopt buffa if it passes, else fall back to prost — this is the central "plays well with wasm" decision.

Constraint: messages-first — do NOT build the Connect transport, runtime wiring, or retire the hand-written TS mirror here (that is E3B). Arrow IPC stays the data plane; proto is control-plane only. UC stays OpenAPI-vendored. Respect the WASM size gates.

Deliverable: a detailed execution plan with milestones, the proto module/package layout, the message inventory per seam, the codegen + drift-check plan, the living-draft/adoption policy, risks, and a test strategy.
```

---

## E3B — Contract Migration and Multi-Target Runtime (Buf-managed protobuf)

```text
You are planning E3B for Axon: completing the migration of the control/contract plane to protobuf (building on the E3A seam messages) and standing up the transport-agnostic, multi-target runtime. E3A already defined and codegen'd the provider seam messages; E3B migrates the remaining control-plane types, retires the hand-maintained TS mirror, and wires the runtime.

Context: contracts today are serde JSON types in crates/query-contract/src/lib.rs and worker IPC envelopes in crates/browser-sdk/src/lib.rs, with a large hand-maintained TypeScript mirror in apps/axon-web/src/axon-browser-sdk.ts. E3A has landed the seam messages in proto. Arrow IPC already crosses the worker boundary as bytes. There is a documented no-go on using DataFusion/Substrait proto as the hot-path IR.

Read these first: docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md (the message layer this builds on); crates/query-contract/src/lib.rs and crates/query-contract/src/delta_protocol_features.rs; crates/browser-sdk/src/lib.rs; apps/axon-web/src/axon-browser-sdk.ts (the hand-written TS mirror to retire); apps/axon-web/src/sandbox-query-worker.ts; apps/axon-web/src/lib.rs (wasm-bindgen surface); crates/query-router/src/lib.rs; crates/native-query-runtime/src/lib.rs; docs/program/browser-datafusion-size-audit.md; the E9 plan (the execution-provider consumer).

Goal: harden the Buf toolchain (buf lint + breaking-change detection wired into CI as a contract gate now that the contract is stabilizing; BSR for modules/deps), migrate the remaining control-plane types to proto, and retire the hand-maintained TS mirror with round-trip parity tests. Define the transport-agnostic QueryEngine RPC surface (Connect) that the E9 ExecutionProviders implement: BrowserWasm (today), Tauri (native via IPC reusing native-query-runtime), and RemoteService. No servers are implemented in this effort — contracts + client/runtime bindings only.

Key decisions to resolve: (1) confirm the Rust runtime backend chosen in E3A (buffa preferred, prost fallback) builds for wasm32-unknown-unknown at acceptable size, and add Rust service codegen (buffa does not emit services yet — use connect-rust/ConnectRPC for the QueryEngine service); (2) how to stage the migration with round-trip parity tests against current JSON before retiring the TS mirror; (3) the transport-agnostic QueryEngine surface and how the E9 BrowserWasm/Tauri/RemoteService backends implement it; (4) how the inner wasm-bindgen JSON-string seam (snapshot_json/request_json) is affected; (5) when to flip the E3A breaking-change gate from relaxed to strict.

Constraint: Arrow IPC stays the data plane; protobuf is the control plane only (respect the size-audit no-go on proto IR). Coordinate auth metadata on RPCs with E6 and the runtime surface with E9.

Deliverable: a detailed execution plan with milestones, a safe migration/parity strategy, the runtime surface design, risks, and a CI contract-gate plan.
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

---

## E8 — Workspace Files and Volumes

```text
You are planning the workspace/file-handling effort (E8) for the Axon web app. This is a dedicated file experience and the FileSystemProvider/Workspace seam behind it — deliberately separate from the SQL editor. Depends on E0 (state/tabs/persistence) and E6 (auth for remote reads); reuses E1's volume references and the existing object-store range-read path; shares the rich text-editing engine with E2 (Monaco).

Context: today there is no file-browsing experience. The catalog (E1) surfaces volumes only as objects (list + metadata); it does NOT open them. Object-store reads exist in apps/axon-web/src/services/object-storage.ts (anonymous GCS XML-API listing + range-read preflight via the WASM surface); local files use OPFS / File System Access in apps/axon-web/src/services/local-delta.ts. The tab model in apps/axon-web/src/state/slices/tabs.ts is entirely SQL-shaped (Tab.sql, updateActiveSql), so there is no concept of a non-SQL tab yet. Brokered object access (short-lived signed URLs / narrow range proxy) is contracted in crates/query-contract/src/lib.rs via the ObjectGrant List/Head/BatchSign/Range routes.

Read these first: apps/axon-web/src/services/object-storage.ts, local-delta.ts; apps/axon-web/src/state/slices/tabs.ts; apps/axon-web/src/lib.rs (inspect_parquet, preflight_parquet_metadata_for_targets, range reads); crates/query-contract/src/lib.rs (ObjectGrant routes, BrowserAccessMode signed_url vs proxy, validate_browser_object_url); docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md (volume-as-object model + the E8 handoff seam); docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md.

Goal: design a uniform FileSystemProvider seam (list / stat / read, with write/upload as a later phase) over file-like backends — UnityCatalogVolume (UC Volumes Files API), ObjectStorePrefix (bucket prefixes), LocalFolder (OPFS / File System Access), and Document (saved queries/files) — and a Files experience: a file browser (directory tree + lazy/infinite listing) plus non-SQL tab kinds (a file preview/viewer tab distinct from the SQL editor tab). First release is read-only preview (parquet via the existing WASM inspect path, csv/json bounded parse + truncation, image via object URL), with size caps and URL-policy validation. Bytes are fetched only via short-lived signed URLs or a narrow proxy (ADR-0002).

Key decisions to resolve: (1) the FileSystemProvider TypeScript interface and how the four backends implement it uniformly (esp. cursor/lazy listing for large volumes and the volume-file read key shape, e.g. ['volume-files', connectionId, catalog, schema, volume, ...path]); (2) generalizing the tab model to a tagged kind (sql | file | ...) in the E0 client store so SQL and file tabs coexist, and how the Files experience routes/deep-links; (3) the preview parser set, size caps, truncation affordance, and lazy-loading of preview chunks (bundle-size); (4) reconciling the Workspace Files API with E0's persistence abstraction (E0 persistence = app/session state; Workspace = user file content) and modeling editor buffers / saved queries as Document files; (5) the write/edit boundary — what E8 owns (file lifecycle, dirty/save, upload) vs what is shared with E2 (the Monaco editing engine for file contents, including SQL-as-a-document); (6) whether the Workspace/FileSystem seam warrants its own ADR.

Constraint: never place cloud or catalog secrets in browser code (ADR-0002); reads go through signed URLs / narrow proxy only, and never persist signed URLs. Do not reimplement the rich text editor — share it with E2. Respect existing WASM bundle/size gates for any new preview helper.

Deliverable: a detailed execution plan with milestones, the FileSystemProvider interface sketch, the tab-kind/Files-experience design, risks (large listings, preview size/perf, bundle growth, secret boundary), and a test strategy.
```

---

## E9 — Execution Provider and Data Access Resolution

```text
You are planning E9 for Axon: the execution counterpart to E1's catalog discovery. You will split two concerns that are fused today into two pluggable seams — an ExecutionProvider (where queries run / where sample data comes from) and a DataAccessResolver (how, if at all, this client may read a table's bytes right now). Depends on E0 (run lifecycle/state), E3A (execution + data-access proto messages), and E6 (worker credential propagation); consumes E1 table refs and E4 decisions.

Context: apps/axon-web/src/services/query.ts fuses read resolution and execution and hard-wires them to the WASM worker — buildSession() resolves a BrowserHttpSnapshotDescriptor per source kind (manifest / local_delta / object_store), then ensureTable()/openDeltaTable() + client.query() execute it via AxonBrowserClient. QueryTableSource (apps/axon-web/src/services/query-source.ts) is the catalog-derived input. There is no app-layer execution seam; execution target is just a 'browser_wasm' | 'native' enum. The brokered/Daxis read-authorization vocabulary (ReadAccessPlan family, descriptors, ObjectGrant routes) lives in crates/query-contract/src/lib.rs. E1 now produces only discovery + a table ref; it does NOT resolve reads or execute.

Read these first: apps/axon-web/src/services/query.ts, query-source.ts, query-pagination.ts; apps/axon-web/src/state/slices/run.ts; apps/axon-web/src/axon-browser-sdk.ts (AxonBrowserClient, descriptors, ExecutionTarget); crates/query-contract/src/lib.rs (ReadAccessPlan family, BrowserHttpSnapshotDescriptor, to_browser_http_snapshot_descriptor, ObjectGrant routes, DaxisApprovedAxonReadDescriptor); crates/query-contract/schemas/; apps/axon-web/src/services/object-storage.ts, local-delta.ts; docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md (the discovery handoff); docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md (messages); docs/program/browser-owned-descriptor-materialization.md; docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md; docs/adr/ADR-0008-daxis-browser-read-compute-contract.md.

Goal: define and ship (1) an ExecutionProvider seam — execute(sql, ref) (streaming, cancellable) and preview(ref) (sample data) — refactoring query.ts behind it with a BrowserWasm implementation that keeps local/object-store behavior byte-identical, and Tauri/RemoteService reserved; the E0 run slice dispatches through it and the BrowserWasm backend owns the WASM session handle. And (2) a DataAccessResolver seam — resolveRead(ref) returning descriptor | read_access_plan | fallback | blocked — with brokered-UC, LocalDelta, and ObjectStore profiles, plus the first-class-but-deferred Daxis profile (DaxisApprovedAxonReadDescriptor). The BrowserWasm executor consumes the resolver; a RemoteService executor resolves server-side and bypasses it.

Key decisions to resolve: (1) the ExecutionProvider TypeScript interface (run lifecycle + preview) and how the existing imperative/streaming/cancel/load-more run path maps onto it without regressing local/object-store; (2) the DataAccessResolver interface and the TableReadResolution union, reusing to_browser_http_snapshot_descriptor and the ObjectGrant routes; (3) the fail-closed rules (unknown policy -> fallback/blocked; never openDeltaTable() for fallback/blocked) and the single handoff descriptor -> openDeltaTable(); (4) how E4 (Cedar) decisions feed the resolver (authoritative standalone vs advisory under Daxis); (5) how this maps onto E6's signed_url vs proxy access modes for worker reads; (6) the ADR (new ADR owning execution + read-resolution invariants; scope ADR-0008 as the Daxis profile of the resolver).

Constraint: never place cloud or catalog secrets in browser code (ADR-0002); bytes via signed URLs / narrow proxy only; never persist grants/signed URLs; descriptor -> openDeltaTable() stays the single execution handoff; keep the existing local/object-store paths working at every milestone. Respect the WASM size gates.

Deliverable: a detailed execution plan with milestones, the ExecutionProvider + DataAccessResolver interface sketches, the read-resolution matrix (eligible -> descriptor; each reason -> fallback/blocked), risks, and a test strategy.
```
