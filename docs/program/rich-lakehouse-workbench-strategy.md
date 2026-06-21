# Rich Lakehouse Workbench — High-Level Strategy

- Status: Draft
- Date: 2026-06-20
- Scope: Propose the high-level direction for evolving Axon's editor app into a pluggable, provider-driven rich lakehouse workbench, broken into separable efforts for team discussion and dedicated planning sessions.
- Related:
  - [Rich Lakehouse Workbench Planning Prompts](./rich-lakehouse-workbench-planning-prompts.md)
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)
  - [Browser Unity Catalog Brokered Runtime Contract](./browser-uc-brokered-runtime-contract.md)
  - [Browser Embedding and Deployment](./browser-embedding-deployment.md)
  - [ADR-0002: Browser Access Uses Signed HTTPS Or A Narrow Proxy, Never Cloud Secrets](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
  - [ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane](../adr/ADR-0008-daxis-browser-read-compute-contract.md)

> This is a strategy and discussion document. It does not change code or commit the project to any specific implementation. Each effort below seeds a dedicated planning session (see the companion [planning prompts](./rich-lakehouse-workbench-planning-prompts.md)) that will validate feasibility and produce a detailed execution plan.

## Vision

Axon today is a narrow but real browser query engine for Delta Lake, with a native correctness oracle and a contract-first integration posture toward an external control plane (Daxis). The goal of this strategy is to grow that foundation into a **rich environment for working with and exploring lakehouses**: connect to remote catalogs, browse them efficiently, write SQL with first-class editor support, visualize results and table health directly from Arrow, and make policy decisions in-app — across multiple deployment targets (browser, desktop, and a future service).

This document frames that ambition as a **core architectural reframe** plus **nine separable efforts** (E0–E8). The efforts are intentionally decoupled so they can be planned, staffed, and shipped independently, with explicit dependencies called out.

## Core reframe: a pluggable provider model

Today the architecture hard-wires a single posture: "Daxis owns catalog and policy; Axon is contract-first" ([ADR-0008](../adr/ADR-0008-daxis-browser-read-compute-contract.md), [Browser UC Brokered Runtime Contract](./browser-uc-brokered-runtime-contract.md)). That posture is valuable and should be preserved — but it should become **one profile among several** rather than the only shape.

The reframe: Axon becomes a standalone rich lakehouse client where the catalog source, the authentication/session authority, the authorization authority, and the execution backend are all **pluggable providers**. The Daxis/contract-first integration becomes one provider profile. A second profile — "direct Unity Catalog REST + in-app Cedar authorization + local or remote execution" — becomes a first-class peer.

Five provider seams anchor every effort:

- **CatalogProvider** — where table/metadata structure comes from: `DirectUnityCatalog` (UC REST), `Daxis` (`ReadAccessPlan`), `LocalDelta`, `ObjectStore`.
- **Identity/SessionProvider** — who the user is and whether the session is valid: an ambient browser session terminated by an Envoy proxy (login/session, no cloud secrets in the browser), vs `LocalDelta` (no auth) vs Tauri/native (OS/native credentials).
- **AuthorizationProvider (PDP)** — whether an action is allowed: `Cedar` (in-app, authoritative in standalone mode; advisory in Daxis mode) or `Daxis` (authoritative).
- **ExecutionProvider** — where the query runs: `BrowserWasm` (Web Worker, today), `Tauri` (native runtime via IPC), `RemoteService` (Connect/gRPC).
- **FileSystemProvider / Workspace** — where file-like bytes and directory listings come from: `UnityCatalogVolume` (UC Volumes Files API), `ObjectStorePrefix` (bucket prefixes), `LocalFolder` (OPFS / File System Access), and `Document` (saved queries/files). Distinct from CatalogProvider: the catalog answers "what tables/volumes/metadata exist," the FileSystemProvider answers "what files are inside a volume/folder and how do I read or write their bytes." Anchors E8.

Identity/Session and Authorization are deliberately separate seams: Envoy answers "who is this and is the session valid," Cedar answers "may they do this." Catalog and FileSystem are likewise separate: the catalog references a volume as an object; the FileSystemProvider browses and reads the files inside it.

### Relationship to existing decisions (extends vs supersedes)

- **Extends** [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md) (no cloud secrets in the browser). The Envoy session model and the "direct UC" profile must still never place cloud or catalog secrets in browser code; the proxy and signed-URL/narrow-proxy patterns remain the rule. ADR-0002 is a hard constraint on every effort here.
- **Generalizes** [ADR-0008](../adr/ADR-0008-daxis-browser-read-compute-contract.md) and the [Browser UC Brokered Runtime Contract](./browser-uc-brokered-runtime-contract.md). Those documents are currently written as "Daxis owns catalog and policy, Axon consumes outcomes." Under the provider model they describe the **Daxis profile** specifically; a new "standalone/direct" profile sits alongside them. The reframe does not invalidate the Daxis contracts — it scopes them as one provider implementation. Whether ADR-0008 should be amended or a new ADR added is a decision for the E1/E4 planning sessions.
- **Preserves** [ADR-0004](../adr/ADR-0004-native-runtime-is-correctness-oracle-and-mandatory-fallback.md). The native runtime stays the correctness oracle regardless of which ExecutionProvider is active.

## Current state (grounding)

Brief, so the efforts below are concrete rather than abstract:

- **State management** is hand-rolled: module-singleton `subscribe`/`snapshot` stores in [apps/axon-web/src/services/](../../apps/axon-web/src/services) plus ~25 `useState` hooks in [App.tsx](../../apps/axon-web/src/editor/App.tsx). No state library. Persistence is split across IndexedDB ([metadata.ts](../../apps/axon-web/src/services/metadata.ts)), localStorage ([connect/store.ts](../../apps/axon-web/src/editor/connect/store.ts)), and OPFS / File System Access ([local-delta.ts](../../apps/axon-web/src/services/local-delta.ts)).
- **Routing** is a minimal History-API helper ([router.ts](../../apps/axon-web/src/editor/router.ts)); the `/catalogs` route is declared but never rendered.
- **Editor** is a custom `<textarea>` + tokenizer ([Editor.tsx](../../apps/axon-web/src/editor/components/Editor.tsx), [highlight.ts](../../apps/axon-web/src/editor/lib/highlight.ts)). CodeMirror is a dependency but unused; there is no IntelliSense/hover/completion service.
- **Catalog/connect** models four source types but only `LocalDelta` and public `ObjectStore` (GCS) execute end-to-end; Unity Catalog and Delta Sharing are stubbed behind a feature flag ([ConnectModal.tsx](../../apps/axon-web/src/editor/connect/ConnectModal.tsx), [query-source.ts](../../apps/axon-web/src/services/query-source.ts)).
- **Contracts** are serde JSON in [query-contract](../../crates/query-contract/src/lib.rs) with a large hand-maintained TypeScript mirror in [axon-browser-sdk.ts](../../apps/axon-web/src/axon-browser-sdk.ts). No protobuf anywhere.
- **Frontend↔WASM boundary** is `BrowserWorkerCommand`/response envelopes over `postMessage` plus an Arrow IPC byte side-channel ([browser-sdk](../../crates/browser-sdk/src/lib.rs), [sandbox-query-worker.ts](../../apps/axon-web/src/sandbox-query-worker.ts)).
- **Arrow IPC already crosses the worker boundary as bytes**, but the UI converts to JS cells for preview — the opposite of native Arrow visualization.

## The nine separable efforts

### E0 — Frontend Foundation: State, Routing, Persistence

The substrate for everything else. Replace the hand-rolled singletons and `useState` sprawl with a scalable, typed state architecture. The key design decision is to **split state by ownership**:

- **Server/remote state → TanStack Query.** All data fetched from remote APIs (Unity Catalog REST, Daxis `ReadAccessPlan`, object-store/GCS listings, volume file metadata) is modeled as queries/mutations with a `queryKey` hierarchy (e.g. `[provider, catalogId, 'schemas', schemaId, 'tables']`). This gives caching, background refetch, `staleTime`/freshness control, request dedup, retry/backoff, pagination/infinite queries for large catalog trees, optimistic mutations, and per-query loading/error states. This is exactly the caching/activeness behavior we want for remote catalogs, and it doubles as the catalog metadata cache that E2 IntelliSense and E4 policy entities read from.
- **Client/UI + session state → a lightweight store** (evaluate Zustand vs Redux Toolkit vs XState; recommend Zustand + slices for low ceremony). Owns non-server state: editor tabs/buffers, active connection + catalog/schema/table selectors, layout/tweaks, in-flight query-run lifecycle, and the long-lived WASM session handle in [query.ts](../../apps/axon-web/src/services/query.ts) (the worker is a client resource, not a TanStack query). Document the boundary so server state is never duplicated into the client store.
- **Unified persistence abstraction** over IndexedDB ([metadata.ts](../../apps/axon-web/src/services/metadata.ts)), localStorage ([connect/store.ts](../../apps/axon-web/src/editor/connect/store.ts)), File System Access handles, and OPFS ([local-delta.ts](../../apps/axon-web/src/services/local-delta.ts)) — with explicit support for the "read files from disk" lifecycle and re-grant flows. Decide which TanStack caches are persisted (`persistQueryClient`) vs memory-only.
- **A real router.** Wire the dead `/catalogs` route in [router.ts](../../apps/axon-web/src/editor/router.ts); support deep links to catalog/schema/table, saved queries, and the E7 table-insight surface. Consider TanStack Router (pairs naturally with TanStack Query for route-driven prefetch/loaders) as one option.

Dependency: blocks E1, E2, E5, E7. The TanStack Query layer is the seam E1 providers plug into.

### E1 — Pluggable Catalog Providers and Catalog Explorer

Define the `CatalogProvider` interface and ship the `DirectUnityCatalog` provider (UC REST: list catalogs/schemas/tables/views/volumes, table metadata) alongside the existing Daxis/local/object-store paths in [query-source.ts](../../apps/axon-web/src/services/query-source.ts) and [ConnectModal.tsx](../../apps/axon-web/src/editor/connect/ConnectModal.tsx). Providers expose their reads as TanStack Query options (a `queryKey` factory + fetcher per provider) so caching, background refresh, dedup, and pagination from E0 apply uniformly across UC/Daxis/local/object-store.

- **Multi-catalog connection registry + selectors** in both the sidebar and the editor, each backed by provider-scoped query keys so switching catalogs reuses cached metadata and refreshes stale entries in the background.
- **Lazy catalog tree navigation** (infinite/paginated queries for large catalogs).
- **Volumes as catalog objects (discovery only)** — list volumes and show volume metadata (type, storage location, comment) alongside tables/views in the explorer. Browsing the files *inside* a volume, previewing, and editing them is **out of scope for E1** and lives in **E8 (Workspace Files & Volumes)**; the catalog only references a volume, it does not open it.
- **A catalog metadata cache** that feeds editor IntelliSense (E2) and policy entities (E4).
- **Open question:** where UC credential exchange / CORS lives given [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md) — likely the Envoy session seam from E6 (a thin token/proxy boundary), reconciled with the pluggable model.

Dependency: needs E0 and E6 (auth for remote APIs); feeds E2, E4, E7, E8.

### E2 — Editor Modernization: Monaco + Catalog-Aware SQL IntelliSense

Replace the custom textarea editor ([Editor.tsx](../../apps/axon-web/src/editor/components/Editor.tsx)) with Monaco (compare against finishing CodeMirror 6, which is already a dependency). Build a SQL language service that consumes E1 catalog metadata to provide:

- Context-aware completion (databases/schemas/tables/columns ranked by the `FROM` clause).
- Hover cards with table/column type, partitioning, row count, and latest commit.
- Signature help for SQL functions.
- Pre-run diagnostics that surface unsupported-SQL / fallback reasons as squiggles before execution.
- Multi-tab editing with per-tab catalog/connection context.

Dependency: needs E0 and E1 metadata.

### E3 — Contract IDL and Multi-Target Runtime (Buf-managed protobuf)

Move the control/contract plane in [query-contract](../../crates/query-contract/src/lib.rs) and the worker envelopes in [browser-sdk](../../crates/browser-sdk/src/lib.rs) to protobuf as the single source of truth, retiring the hand-maintained TS mirror in [axon-browser-sdk.ts](../../apps/axon-web/src/axon-browser-sdk.ts). Standardize on the **Buf toolchain** (not a hand-rolled `prost-build`/`build.rs`): `buf` CLI, `buf.yaml`/`buf.gen.yaml`, BSR for module + dependency management, and `buf lint` + breaking-change detection wired into CI as a contract gate.

- **Codegen targets:** TypeScript via `@bufbuild/protobuf` (protobuf-es) and Connect-ES; Rust generated through Buf-managed plugins. Decision for the E3 planning session: Buf is the build/lint/dependency toolchain, but the Rust *runtime* is still produced by a codegen backend — most likely a prost-based plugin run via `buf generate`. "Buf vs Prost" is a toolchain-vs-backend distinction, not strictly either/or. The session should confirm whether to keep prost as the generated Rust runtime under Buf or evaluate alternatives, and how that interacts with `wasm32-unknown-unknown` builds.
- **Transport-agnostic QueryEngine RPC surface** (Connect) with ExecutionProviders: `BrowserWasm` (today), `Tauri` (native via IPC, reusing [native-query-runtime](../../crates/native-query-runtime/src/lib.rs)), `RemoteService`. Use Buf on any future server side too (Connect server), but **no servers are implemented in this effort** — contracts plus client/runtime bindings only.
- **Hard constraint:** Arrow IPC stays the data plane; protobuf is the control plane only. Honor the documented no-go on DataFusion/Substrait proto as the hot-path IR ([Browser DataFusion Size Audit](./browser-datafusion-size-audit.md)).

Dependency: largely parallelizable track; coordinates with E4 (decision types) and E6 (auth metadata on RPCs).

### E4 — In-App Authorization: Cedar Policy Engine

Integrate Cedar (the `cedar-policy` Rust crate, compiled to WASM) as a pluggable PDP for client-side authorization in standalone mode (advisory in Daxis mode, where Daxis remains authoritative).

- **Model entities/schema:** principal, catalog, schema, table, volume; actions such as `browse`, `query`, `export`.
- **Decision point** wired into the request path, with allow/deny mapped onto the existing structured taxonomy (`FallbackReason`, `BlockedReadPlan` in [query-contract](../../crates/query-contract/src/lib.rs)).
- **Reconcile** with [ADR-0008](../adr/ADR-0008-daxis-browser-read-compute-contract.md): in the Daxis profile, policy is still owned by Daxis and Cedar is advisory; in the standalone profile, Cedar is authoritative for client-side gating but must never be presented as a substitute for server-side enforcement of governed shapes.

Dependency: needs the E1 entity model, the E3 decision contract types, and the authenticated principal from E6.

### E5 — Arrow-Native Visualization Layer

Adopt Apache Arrow JS to consume the existing Arrow IPC result bytes directly (they already cross the worker boundary per [Browser Embedding and Deployment](./browser-embedding-deployment.md)), eliminating the row-JSON materialization currently done for preview.

- A virtualized table grid that reads Arrow vectors directly.
- A charting layer over Arrow columns; auto-suggest encodings from schema; "chart from result."
- Evaluate canvas/WebGL for large results.

Dependency: needs E0; uses the Arrow transport already present. Foundation for E7.

### E6 — Authentication and Session (Envoy proxy + WASM credential propagation)

Assume an **Envoy proxy** fronts the deployment: it serves the static bundle and terminates login, maintaining a browser session (e.g. OIDC via Envoy's oauth2 filter / ext_authz, with a session cookie). The browser therefore never holds cloud or UC secrets, aligning with [ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md).

- **App-shell session model:** how UC/Daxis API calls (E1 TanStack Query fetchers) carry the session — same-origin requests through Envoy with `credentials: 'include'`, 401/redirect-to-login handling, session expiry/refresh surfaced to the UI.
- **The WASM integration is the hard part.** The query runtime makes its own HTTP range reads from inside the Web Worker via [wasm-http-object-store](../../crates/wasm-http-object-store/src/lib.rs). For authenticated/proxied reads, those `fetch` calls must carry the session. Resolve in the planning session: (a) ambient cookie/session on same-origin proxied reads (worker `fetch` with credentials) vs (b) explicit bearer-token injection threaded into the object-store/descriptor layer; how this maps onto the existing `signed_url` vs `proxy` access modes in [query-contract](../../crates/query-contract/src/lib.rs); and the worker-vs-main-thread credential boundary.
- **Cross-origin isolation tension:** threaded/SIMD WASM bundles need COOP/COEP cross-origin isolation (per the bundle manifest in [axon-browser-sdk.ts](../../apps/axon-web/src/axon-browser-sdk.ts)); `COEP: require-corp` interacts with credentialed cross-origin fetches and bundle/asset loading. The Envoy + CORS/COOP/COEP header story must be designed together (a single-origin Envoy is the simplest reconciliation).
- **Pluggable seam:** Tauri/native and LocalDelta profiles bypass Envoy (native credentials or no auth); the SessionProvider seam must not assume a browser session.

Dependency: cross-cutting — pairs with E1 (API auth) and E3 (auth metadata/credentials on RPCs and the RemoteService transport); informs E4 (authenticated principal feeds Cedar entities); touches the WASM HTTP layer.

### E7 — Delta Table Insight and Health Surface

A dedicated UI surface for understanding a Delta table from its Delta log — not query results, but the table's own structure and health.

- **Metadata and enabled features:** schema, partitioning, table properties, and the enabled Delta protocol / reader-writer features rendered as a capability/feature view, sourced from [delta_protocol_features.rs](../../crates/query-contract/src/delta_protocol_features.rs) (`DeltaProtocolFeature`, `KNOWN_DELTA_PROTOCOL_FEATURES`) and the `CapabilityReport` in [query-contract](../../crates/query-contract/src/lib.rs) — showing supported vs native-only vs unsupported per feature (deletion vectors, column mapping, CDF, timestamp-ntz, etc.).
- **Stats-boundary plots:** visualize per-column min/max boundaries across add-file stats from the reconstructed log ([wasm-delta-snapshot](../../crates/wasm-delta-snapshot/src/lib.rs)) and Parquet footer stats (`ParquetInspectionSummary` via `inspect_parquet` / `preflight_parquet_metadata_for_targets` in [apps/axon-web/src/lib.rs](../../apps/axon-web/src/lib.rs)): per-file value ranges, range overlap (a proxy for data-skipping/pruning effectiveness), null-count and row-count distributions, and stats-coverage gaps.
- **Health signals:** file-size histogram (small-file problem), file count, partition cardinality/skew, commit/log growth over the snapshot-version timeline ([snapshot.ts](../../apps/axon-web/src/services/snapshot.ts) commits feed), checkpoint cadence, and tombstone/removed-file counts.

Mostly read-only over data Axon already reconstructs — high insight value for relatively contained new work.

Dependency: needs E5 (plots) and E1 (table selection/metadata); reuses the existing snapshot/log reconstruction.

### E8 — Workspace Files and Volumes

A dedicated **file-handling experience** and the **FileSystemProvider / Workspace** seam behind it. E1 makes the catalog *reference* a volume as an object; E8 is where users actually **browse the files inside** a volume (or a bucket prefix, or a local folder), **preview** them, and — later — **edit/write** them. This is intentionally separate from the SQL editor: it is a different surface with different (non-SQL) tab kinds.

- **FileSystemProvider seam** — a uniform `list / stat / read / (later) write` contract over file-like backends: `UnityCatalogVolume` (UC Volumes Files API), `ObjectStorePrefix` (bucket prefixes, extending [object-storage.ts](../../apps/axon-web/src/services/object-storage.ts)), `LocalFolder` (OPFS / File System Access, extending [local-delta.ts](../../apps/axon-web/src/services/local-delta.ts)), and `Document` (saved queries/files as a backend). Reads reuse the existing object-store range-read path; bytes are fetched only via short-lived signed URLs or a narrow proxy ([ADR-0002](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)).
- **Files experience** — a file-browser surface (directory tree + listing) plus **non-SQL tab kinds**: a file preview/viewer tab distinct from the SQL editor tab. The current tab model ([tabs.ts](../../apps/axon-web/src/state/slices/tabs.ts)) is SQL-shaped (`Tab.sql`) and must be generalized to a tagged tab `kind` so SQL tabs and file tabs coexist.
- **Preview now, edit later** — first release is read-only preview (parquet/csv/json/image) using primitives Axon already ships. Write/upload and rich in-place editing come later; the *rich text-editing engine* for file contents (including SQL-as-a-document) is **shared with E2 (Monaco)** rather than reimplemented here.
- **SQL-editor-content-as-files** — editor buffers and saved queries are themselves `Document` files in the Workspace model; this is the framing that unifies "open a file" and "open a saved query." Reconcile with E0's persistence abstraction: E0 persistence is *app/session state*; the Workspace API is *user file content*.

Dependency: needs E0 (state/tabs/persistence) and E6 (auth for remote volume/object reads); reuses E1's volume references and the object-store read path; shares the editing engine with E2. Whether the Workspace/FileSystem seam warrants its own ADR is an E8 planning-session decision.

## Dependency and sequencing view

```mermaid
flowchart TD
  E0[E0 Foundation: state, routing, persistence]
  E1[E1 Catalog providers and explorer]
  E2[E2 Monaco SQL IntelliSense]
  E3[E3 Buf protobuf IDL and multi-target runtime]
  E4[E4 Cedar policy engine]
  E5[E5 Arrow-native visualization]
  E6[E6 Auth and session: Envoy plus WASM creds]
  E7[E7 Delta table insight and health surface]
  E8[E8 Workspace files and volumes]

  E0 --> E1
  E0 --> E2
  E0 --> E5
  E0 --> E8
  E1 --> E2
  E1 --> E4
  E3 --> E4
  E3 -.parallel track.-> E0
  E6 --> E1
  E6 --> E3
  E6 --> E4
  E6 --> E8
  E5 --> E7
  E1 --> E7
  E1 -->|"volume refs, object-store read path"| E8
  E2 -.shared editing engine.-> E8
```

Recommended order: E0 first; E6 (auth/session) and E3 (Buf contracts) start early in parallel since E1 and E4 depend on them; then E1; E2 after E1; E4 after E1+E3+E6; E5 after E0 (anytime); E7 after E5+E1; E8 after E1+E6 (and pairs with E2 for the shared editing engine).

## UX ideas to carry into planning

- Command palette (catalog navigation, run, switch provider, open file).
- Hover cards on `catalog.schema.table` with schema, partitioning, latest Delta commit, and capability badges.
- Multi-tab editor; per-tab catalog/connection context; pinned/favorite tables.
- Inline mini-charts in results plus a dedicated viz panel with auto-detected encodings.
- Pre-run diagnostics: show fallback/unsupported reasons as squiggles before execution.
- Local-first: open a folder, everything persists via OPFS / File System Access with a clear re-grant UX.
- Files experience (E8): browse the files inside a volume / bucket prefix / local folder and preview them (parquet/csv/json/image) in non-SQL tabs without leaving the workbench.
- Delta table insight/health surface (E7): a dedicated view that visualizes Delta-log metadata, enabled protocol features, per-column stats-boundary plots, and table-health signals (file-size/skew/commit-growth) so users can explore a table's health from what the log reveals.

## Risks and cross-cutting constraints

- **Secret boundary (ADR-0002).** Every remote profile must keep cloud/catalog secrets out of the browser. The Envoy session and direct-UC profiles depend on this holding.
- **Cross-origin isolation vs credentials.** Threaded WASM requires COOP/COEP; credentialed fetch interacts with `COEP: require-corp`. E6 must design headers and origin layout up front.
- **Contract migration risk.** Moving to Buf/protobuf (E3) touches the most central types in the repo; it should be staged with round-trip parity tests against the current JSON contracts before the hand-written TS mirror is retired.
- **WASM bundle size.** New capabilities (Cedar in WASM, Arrow JS, Monaco) have size implications; respect the existing size gates and the DataFusion size audit guidance.
- **Policy correctness.** In-app Cedar (E4) must never be presented as a substitute for server-side enforcement in governed (Daxis) deployments.

## Out of scope for this strategy document

- Detailed per-effort designs (deferred to the dedicated planning sessions seeded by the [planning prompts](./rich-lakehouse-workbench-planning-prompts.md)).
- Implementing any server (E3 delivers contracts and client/runtime bindings only).
- Committing the project to specific library choices where the efforts explicitly list them as decisions (state library, Monaco vs CodeMirror, Rust codegen backend, charting library).
