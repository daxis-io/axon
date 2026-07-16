# Rich Lakehouse Workbench — Planning-Session Prompts

- Status: Revised draft
- Date: 2026-06-20
- Revised: 2026-07-15
- Scope: Self-contained prompts to seed a dedicated planning session for each effort (E0–E9; E3 is phased into E3A/E3B) in the [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md). Each prompt can be pasted into a fresh planning chat.
- Related:
  - [Rich Lakehouse Workbench Strategy](./rich-lakehouse-workbench-strategy.md)

> How to use this file: open a new planning session for one effort, paste its prompt, and re-ground it on current `origin/main` before making a plan. Each prompt assumes a `SourceProfile` composes five narrow table-workflow seams: discovery-only `CatalogProvider`, `IdentitySessionProvider`, `AuthorizationDecisionProvider`, `DataAccessResolver`, and `ExecutionProvider`; it adds `FileSystemProvider` only for file navigation. Browser authorization is an early decision layer; the remote resource owner remains the enforcement and audit authority. Current worker protobuf is compatibility IPC, not a portable remote service. Plans must prefer a usable vertical slice over unused substrate.

---

## E0 — Frontend Foundation: State, Routing, Persistence

```text
You are planning the remaining E0 frontend-foundation work for the Axon web app. Re-ground on origin/main before planning; the Zustand store slices and generated config contracts have landed, so do not plan them again.

Context: apps/axon-web/src/state now has Zustand slices for tabs, connections, run, engine, settings, layout, and UI. Service singletons and split persistence remain in parts of the app. IndexedDB, local storage, OPFS, and File System Access handles have different authority and re-grant rules. Routing still identifies workbench state and must not carry capabilities.

Read these first: apps/axon-web/src/state/store.ts, types.ts, hooks.ts, and slices/; services/query.ts, catalog.ts, engine.ts, metadata.ts, local-delta.ts; editor/App.tsx and router.ts; editor/connect/store.ts; the E0 execution plan and its landed-code notes.

Goal: finish the ownership migration without replacing the landed store:
- Discovery metadata -> TanStack Query, with connection plus principal/session-scoped keys, bounded freshness, explicit invalidation, and pagination.
- Client/UI and operation state -> the existing Zustand store. The worker handle remains an owned runtime resource, not persisted state.

Authority and persistence rules: discovery query keys include the connection plus principal/session namespace; session change invalidates them. Signed URLs, grants, resolved access envelopes, governed descriptors, and protected object bytes are memory-only by default. Only public immutable bytes may persist under stable object identity. Generic query retry applies to discovery, not authorization, access resolution, or accepted execution. The selected source ID is authoritative and missing selection fails rather than falling back to sample data.

Key decisions to resolve and recommend: (1) the remaining migration off service singletons; (2) the precise server-vs-client state boundary; (3) session-scoped query-key and invalidation conventions; (4) persistence by access class, including the local-file re-grant lifecycle and logout clearing; (5) routing logical references without persisting capabilities; (6) one execution state that cannot produce multiple terminal outcomes.

Constraint: this is a frontend-only effort; do not change Rust contracts here. Be mindful of WASM bundle/size gates already in the repo.

Deliverable: a detailed execution plan with milestones, a migration path off the current singletons that keeps the app working at each step, risks, and a test strategy (including how state/persistence is tested).
```

---

## E1 — Pluggable Catalog Providers and Catalog Explorer

```text
You are planning the remaining catalog integration effort (E1) for the Axon web app. E1 owns the DISCOVERY-ONLY CatalogProvider seam and one table-first Catalog Explorer slice. The first remote profile is session-proxied Unity Catalog, alongside LocalDelta and ObjectStore. Governed-host consumers are deployment compositions, not entries in a broad provider taxonomy.

IMPORTANT scope boundary: E1 does NOT resolve bytes or execute. It hands a logical table reference to E9. E9 resolves that reference to `browser_read`, `remote_required`, `denied`, or a typed operational error before accepting execution.

Context: LocalDelta and public ObjectStore execute end to end. Unity Catalog and Delta Sharing do not. Generated `axon/catalog/v1` messages are landed, but no app-layer CatalogProvider adoption or UC vertical slice is complete. The selected source must become an explicit authoritative input; query dispatch must not substitute a sample or another connection.

Read these first: apps/axon-web/src/editor/connect/ConnectModal.tsx, data.ts, types.ts, store.ts, ConnectedCatalogs.tsx; apps/axon-web/src/services/query-source.ts, catalog.ts, object-storage.ts, connector-features.ts, types.ts; apps/axon-web/proto/axon/catalog/v1/catalog.proto; docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md; docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md; docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md.

Goal: adopt a narrow CatalogProvider interface over the landed generated messages and ship one vertical slice: connect through the same-origin session proxy, list catalogs/schemas/tables with pagination, inspect table/column metadata, select one table, and hand its logical reference to E9. Volume discovery may return a logical volume reference for E8. Functions, models, writes, generic capabilities, and governed-host discovery are deferred. The adapter validates UC OpenAPI responses once before constructing generated domain values.

Key decisions to resolve: (1) the smallest discovery-only TypeScript interface over `axon/catalog/v1`; (2) validated UC-to-domain mapping; (3) principal/session-scoped cache keys and logout invalidation; (4) route-owned connection and table selection handed to E9; (5) the logical volume-reference handoff to E8. Do not introduce a capabilities abstraction until a second implemented provider needs one.

Constraint: never place cloud or catalog secrets in browser code (ADR-0002). Discovery only: keep read resolution and execution in E9 and file-level volume access in E8. Browser policy may gate UI but the proxy/catalog enforces remote metadata access.

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

## E3A — Stabilize And Adopt Landed Contract Surfaces

```text
You are planning the remaining E3A work for Axon. Re-ground on origin/main: generated axon/common/v1, axon/catalog/v1, axon/dataaccess/v1, axon/exec/v1, and axon/fs/v1 TypeScript outputs are landed. Buffa Rust proof exists for common, data-access, execution, and filesystem messages, not catalog. The filesystem package is messages-only substrate; E8 provider, adapter, UI, and runtime adoption have not landed. App-layer E1/E9 adoption is not complete. The current BrowserWorker messages and QueryEngine service descriptor model browser client/worker compatibility IPC; they are not a portable remote service.

Read these first: apps/axon-web/proto/axon/common/v1/common.proto, catalog/v1/catalog.proto, dataaccess/v1/dataaccess.proto, exec/v1/exec.proto, fs/v1/fs.proto; generated TS and crates/contract-proto outputs; docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md; docs/plans/2026-07-11-e3a-exec-contract-pi-session.md; docs/plans/2026-07-15-e3a-fs-contract-pi-session.md; docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md; current query-source.ts and query.ts.

Goal: after E9 Slice 1 establishes selected-source integrity and the execution lifecycle without protobuf changes, make one intentional pre-adoption correction before E9 Slice 2 adopts the seams. Define `CanonicalResourceRef` as connection ID, versioned provider namespace, resource kind, and exactly one non-empty provider-object-ID or canonical-locator arm. Require capability-free, versioned, fixture-tested canonicalization for locator providers. Browser execution receives one execution-local resolved access envelope carrying that reference, descriptor, conditional expiry, access class, and correlation/provenance. Capability-bearing access requires the earliest finite expiry. Remote/native execution receives one logical reference and resolves it inside its trust boundary. No request accepts both. Keep resolution, admission, and accepted terminal results disjoint. Keep CatalogProvider, DataAccessResolver, and ExecutionProvider as in-process interfaces over generated domain messages; do not encode TanStack Query or HTTP mechanics in proto.

Key decisions: (1) exact scoped identity `oneof`, equality, and canonicalization fixtures; (2) execution-local resolved browser envelope and conditionally mandatory shortest expiry; (3) caller-created `execution_id`, idempotent admission, cancel-before-admit, and first-terminal-transition rules; (4) authoritative terminal state versus at-most-one stream frame; (5) one byte-budgeted browser result until credit-based chunking exists; (6) authority-boundary validation for JSON/protobuf adapters; (7) the corrected Buf FILE baseline and migration gate; (8) which Rust outputs have real consumers.

Constraint: do not reopen the landed axon/fs/v1 package or add E8 runtime adoption without its read-only volume consumer. Do not add Cedar decision packages, Connect, Tauri, or a remote service without a consuming vertical slice. Arrow IPC remains the data plane. UC remains OpenAPI-vendored. Preserve explicit worker-scoped names.

Deliverable: a bounded correction/adoption plan with exact contract changes, affected adapters, compatibility policy, conformance tests, and a rollback point before the first adopted consumer.
```

---

## E3B — Migrate Proven Control Boundaries

```text
You are planning E3B for Axon. Migrate proven control boundaries one at a time. Do not design a generic multi-target runtime or portable remote service from the browser worker protocol.

Context: generated control messages coexist with serde JSON, wasm-bindgen JSON strings, browser-sdk worker envelopes, and a hand-maintained TypeScript mirror. BrowserWorker commands/events are compatibility IPC. Arrow IPC already carries results.

Read these first: the landed proto and generated outputs; crates/query-contract/src/lib.rs; crates/browser-sdk/src/lib.rs; apps/axon-web/src/axon-browser-sdk.ts; sandbox-query-worker.ts; apps/axon-web/src/lib.rs; docs/program/browser-datafusion-size-audit.md; the E9 vertical-slice plan.

Goal: adopt protobuf at the browser client/worker boundary with parity and malformed-input tests, then retire only the corresponding duplicate mirror. Keep in-process TypeScript seams in process. Preserve Arrow IPC bytes. Tighten Buf FILE compatibility after adoption.

Key decisions: (1) the smallest next duplicated boundary; (2) adapter validation for required fields, unknown enums, oneof conflicts, expiry, and URL policy; (3) parity and rollback strategy; (4) Rust generation only for active consumers; (5) the exact compatibility gate.

Constraint: defer Connect, Tauri, generic RPC auth, and a portable QueryEngine service until a concrete remote host provides identity, admission, deadlines, streaming/backpressure, cancellation, retry, and audit requirements plus a conformance test.

Deliverable: a boundary-specific migration plan with parity fixtures, malformed-input tests, size proof, compatibility gate, and deletion list for the duplicate code it removes.
```

---

## E4 — Authorization Decision Boundary

```text
You are planning E4 for Axon: define the smallest authorization-decision boundary needed by a concrete standalone profile. Do not assume Cedar is required.

Context: browser-local resources can be gated by browser decisions and browser permission APIs. Protected catalog metadata, object bytes, and remote compute are enforced by the service that serves them. A browser decision can fail early and improve UX but is not a capability and cannot replace remote enforcement or audit.

Read these first: docs/program/provider-model.md; docs/program/browser-uc-brokered-runtime-contract.md; the strategy; the E9 vertical-slice plan; current blocked/fallback outcome types.

Goal: define allow, deny, and unknown decisions, their principal/resource/action inputs, and the enforcement owner for each source profile. Unknown fails closed. Denial remains distinct from remote-required execution fallback. Remote boundaries repeat authorization and own the authoritative audit record.

Key decisions: (1) whether a current standalone consumer needs policy beyond local browser permissions; (2) the minimal decision interface; (3) how decision provenance crosses into access resolution; (4) how denial and unknown terminate before execution acceptance; (5) how browser diagnostics correlate with remote audit.

Constraint: if no current consumer needs an embedded PDP, defer implementation. If Cedar is proposed, prove its schema source, policy lifecycle, bundle impact, and conformance suite. Never present it as enforcement for remote resources.

Deliverable: an authority matrix and the smallest consumer-backed plan, including an explicit defer decision when no implementation is warranted.
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

## E6 — Authentication, Session, And Capability Lifetime

```text
You are planning the authentication/session effort (E6) for Axon. It supplies the session boundary required by E1 and E9. It does not design generic RPC authentication.

Context: assume an Envoy proxy fronts the deployment — it serves the static bundle and terminates login, maintaining a browser session (e.g. OIDC via Envoy's oauth2 filter / ext_authz, with a session cookie), so the browser never holds cloud or UC secrets (aligns with ADR-0002). The query runtime makes its own HTTP range reads from inside a Web Worker via crates/wasm-http-object-store. Threaded/SIMD WASM bundles require COOP/COEP cross-origin isolation per the bundle manifest.

Read these first: crates/wasm-http-object-store/src/lib.rs (range-read fetch path); apps/axon-web/src/axon-browser-sdk.ts (bundle manifest, COOP/COEP requirements, descriptor types); crates/query-contract/src/lib.rs (BrowserAccessMode / signed_url vs proxy access modes; resolver request/response types); apps/axon-web/src/services/object-storage.ts, local-delta.ts; docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md; docs/program/browser-embedding-deployment.md (CSP/COOP/COEP/CORS guidance).

Goal: design the app-shell session model for same-origin UC and governed-host metadata calls, and the worker access model for public HTTPS, object-scoped signed URLs, or same-origin narrow proxy routes. Session expiry clears principal-scoped discovery state and every memory-only access envelope.

Key decisions to resolve: (1) the same-origin cookie/session contract; (2) signed-URL versus narrow-proxy access without injecting catalog or cloud bearer tokens into descriptors; (3) how capability expiry and session expiry bound a resolved browser envelope; (4) the worker/main-thread boundary; (5) COOP/COEP, CORS, and credential headers; (6) logout invalidation for discovery, descriptors, protected bytes, and worker sessions.

Constraint: never place cloud or catalog secrets or general-purpose bearer credentials in browser descriptors (ADR-0002). Native and local profiles keep credentials inside their own trust boundaries.

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

## E8 — Volume Selection And Bounded Preview

```text
You are planning the first E8 vertical slice for Axon: select a volume discovered by E1, list files, select one file, and show one bounded read-only preview. Do not design a general workspace.

Context: axon/fs/v1 messages and generated TypeScript and Buffa Rust outputs are landed as contract substrate, but no E8 provider, adapter, UI, or runtime consumer exists. E1 may return a logical volume reference but never file credentials. Existing object-storage range reads and Parquet inspection are the reuse points. The E9 access envelope and execution lifecycle remain authoritative for protected preview reads.

Read these first: object-storage.ts and local-delta.ts; existing range-read and Parquet-inspection paths; catalog.proto, dataaccess.proto, and fs.proto plus generated filesystem outputs; the E1 plan; the filesystem-contract PI handoff; the E9 vertical-slice plan; ADR-0002.

Goal: adopt the landed filesystem messages behind the smallest FileSystemProvider needed for one volume: paginated list and stat returning logical file references. Selecting a file invokes access resolution and a bounded preview. Start with Parquet because Axon already has the path. Define explicit file-size, row, byte, time, and string limits plus cancellation and one terminal outcome.

Key decisions: (1) how an E1 `VolumeNode.ref` maps to the landed `FsRootRef` and how `FsEntry` paths are validated; (2) list pagination and path normalization; (3) enforcement and audit ownership for list/stat/read; (4) the short-lived envelope used for preview; (5) bounded Parquet preview and cancellation; (6) cache and persistence rules by access class.

Constraint: volume discovery never returns signed URLs. Grants, descriptors, and protected bytes are memory-only. Treat axon/fs/v1 as landed messages-only substrate and do not claim runtime adoption before this read-only consumer passes. Defer writes, upload, Document backends, generic filesystem capabilities, broad tab-model changes, and additional preview formats.

Deliverable: a vertical-slice plan with the list-to-preview trace, owner/authority matrix, limits, error/cancel semantics, and tests against one real volume-compatible service.
```

---

## E9 — Execution Provider and Data Access Resolution

```text
You are executing the remaining E9 plan for Axon. Use docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md as the sequence and re-ground it on current origin/main. E9 separates DataAccessResolver from ExecutionProvider without inventing a generic remote transport.

Context: query-source.ts and query.ts fuse source lookup, descriptor construction, worker session management, and execution. A missing or mismatched selected source can fall through to sample behavior. Generated data-access and exec messages are landed but not adopted end to end. axon/fs/v1 is also landed as messages-only substrate, but E8 adoption is deferred. BrowserWorker protobuf is compatibility IPC, not a remote service contract.

Read these first: the E9 vertical-slice plan; query.ts, query-source.ts, query-pagination.ts, and run.ts; exec.proto and dataaccess.proto plus generated outputs; axon-browser-sdk.ts and sandbox-query-worker.ts; object-storage.ts and local-delta.ts; ADR-0002; provider-model.md.

Goal: ship the gated sequence exactly. E9 Slice 1 makes the selected connection/resource authoritative, removes sample/wrong-source fallback, and normalizes admission and cancellation around one caller-created domain `execution_id` without protobuf changes; map that ID to existing worker correlation fields. One intentional E3A correction PI then updates the unadopted wire contracts. E9 Slice 2 adopts resolver and executor seams for local Delta and public object storage with behavior parity. E9 Slice 3 composes E1 discovery and the E6 session boundary to prove one session-proxied Unity Catalog browser query. After Slice 3, treat governed remote execution and read-only volume preview as independent extensions: the first waits for a concrete host, and E8 runtime adoption waits for its read-only volume consumer. Neither blocks the other.

Canonical resource binding: every reference carries connection ID, versioned provider namespace, resource kind, and exactly one provider-object-ID or capability-free canonical-locator arm. Browser execution receives one execution-local ResolvedBrowserRead containing an openable descriptor, access class, correlation/provenance, and the earliest finite `notAfter` for capability-bearing access. A new execution resolves a new binding. Remote/native execution receives one logical object reference and resolves inside its own trust boundary. An executor never accepts both forms.

Outcome model: resolution returns `browser_read`, `remote_required`, `denied`, or `resolution_error`. The caller creates `execution_id`; admission returns `accepted` or `rejected`, is idempotent for identical input, and rejects mismatched ID reuse. The accepted executor atomically records one completed, failed, or cancelled terminal state; a live stream carries at most one terminal frame and a remote disconnect may leave observation unknown. Cancel is idempotent, including before admission, and the first terminal transition wins races. Initial browser delivery is one byte-budgeted Arrow IPC buffer; chunking waits for explicit credits. Discovery or resolution may retry before acceptance; accepted work is never automatically replayed.

Fail closed on missing selection, unknown authority, expired capability, malformed adapter data, unsafe URL, or an unspecified enum/oneof. Do not open a table before an accepted browser-ready binding exists. Never persist grants, signed URLs, resolved envelopes, or protected bytes. Keep local/public behavior usable at every milestone.

Defer Tauri, Connect, generic RPC auth metadata, and a portable remote QueryEngine. A real remote host must define identity, admission, logical-reference resolution, streaming/backpressure, cancellation, retries, persistence, and audit before Axon defines that service.

Deliverable: an execution-ready slice plan or implementation update with exact seams, the pre-acceptance and terminal state model, error/cancel/backpressure tests, capability-expiry tests, local/public parity proof, and the UC browser-query acceptance test.
```
