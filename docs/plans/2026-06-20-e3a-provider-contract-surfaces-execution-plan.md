# E3A — Provider Contract Surfaces (proto messages) — Execution Plan

- Status: Draft (planning deliverable)
- Date: 2026-06-20
- Scope: Define, in one reviewable Buf module tree, the **proto message contracts** for every pluggable provider seam in the lakehouse workbench — catalog **discovery** (E1), **data-access resolution** + descriptors (E9), **execution** request/response + worker IPC (E9), and **filesystem** entries (E8) — and generate TypeScript (protobuf-es) and Rust (**[buffa](https://github.com/anthropics/buffa)**, preferred over prost contingent on wasm verification) from them. Messages-first: proto **services** only at true transport boundaries; the local provider seams stay TypeScript interfaces that *consume* the generated messages. Pulled forward right after E0 so the seam efforts build on one codegen'd contract instead of hand-written interfaces that would later be re-done in proto.
- Related:
  - [Rich Lakehouse Workbench — High-Level Strategy](../program/rich-lakehouse-workbench-strategy.md) (E3 phased into E3A/E3B; the six seams)
  - [Rich Lakehouse Workbench — Planning Prompts](../program/rich-lakehouse-workbench-planning-prompts.md) (E3A prompt)
  - [E0 — Frontend Foundation](./2026-06-20-e0-frontend-foundation-execution-plan.md) (the `axon/config/v1` Buf module this extends; §3.6, M1.5)
  - [E1 — Catalog Providers and Explorer](./2026-06-20-e1-catalog-providers-execution-plan.md) (consumer: discovery messages)
  - [ADR-0002: No cloud secrets in the browser](../adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md)
  - [Browser DataFusion Size Audit](../program/browser-datafusion-size-audit.md) (Arrow IPC stays the data plane; no proto IR)

> E3A is the **contract substrate**. It does not build transports, runtimes, or retire the hand-written TS mirror — that is **E3B**. It defines the shapes the seam efforts (E1 discovery, E9 execution + data access, E8 files, E4 decisions) import, so the seams are designed once, in one place, and reviewed as a unit. The contract is a **living draft** while clients adopt; breaking changes stay allowed until adoption, then the gate tightens.

---

## 1. Executive summary — recommended decisions

| # | Decision | Recommendation |
|---|----------|----------------|
| 1 | What E3A delivers | **Messages, not transports.** Proto message contracts for all four seam families + codegen (protobuf-es TS, **buffa** Rust) + a checked-in/drift-gated output, in one Buf module tree. No Connect services for the local seams, no runtime wiring, no TS-mirror retirement (all E3B). |
| 2 | Module reuse | **Extend E0's `axon/config/v1` Buf module**, not a new toolchain. E0 already stands up `apps/axon-web/buf.yaml` + `buf.gen.yaml` + `proto/axon/config/v1/settings.proto` with protobuf-es codegen and a drift check (E0 §3.6, M1.5). E3A adds sibling packages under the same module and the same `npm run codegen` + `codegen:check` gates. |
| 3 | Package layout | **One package per seam family**, versioned `v1`: `axon/catalog/v1` (discovery nodes/metadata), `axon/dataaccess/v1` (read resolution + descriptors), `axon/exec/v1` (execution req/resp + worker IPC), `axon/fs/v1` (filesystem entries), plus shared `axon/common/v1` (refs, pagination, errors). `axon/config/v1` stays as-is. |
| 4 | Services vs messages | **Proto `service` only where bytes cross a process/worker boundary**: the execution worker IPC and the future `RemoteService` executor (`axon/exec/v1`). The discovery and filesystem **local** seams stay TanStack `queryOptions` / TS interfaces that consume generated messages — proto would add no value there and would imply a transport that does not exist. |
| 5 | UC REST | **Stays OpenAPI-vendored** (per E1 §2.5). Proto defines the *normalized* catalog nodes; the DirectUC provider maps UC's generated OpenAPI types into them. We do **not** re-proto the UC wire format. |
| 6 | Rust backend | **Prefer [`buffa`](https://github.com/anthropics/buffa)** (Anthropic's pure-Rust, `no_std` + `alloc` protobuf implementation) over prost, run as the Buf remote plugin `buf.build/anthropics/buffa` via `buf generate` (no local plugin install). It needs **no C toolchain** (unlike Google's upb-based protobuf-v4), ships **built-in proto-canonical JSON** via serde (drops the `prost` + `pbjson` combo and simplifies the round-trip parity tests), preserves unknown fields, and offers zero-copy `bytes` views (a bonus for the opaque Arrow payload). Its `no_std` + `alloc` core is the right shape for the WASM worker. **Adoption is contingent on verifying `wasm32-unknown-unknown` builds and fits the worker budget (M0 gate, MSRV 1.87);** if buffa can't build/fit, fall back to `protoc-gen-prost`. buffa is **pre-1.0 (`0.x`)** and **does not yet generate Rust _services_** — fine for E3A (messages only); the `QueryEngine` service Rust impl is **E3B** (via the companion `connect-rust`/ConnectRPC). |
| 7 | Living-draft policy | **`breaking: FILE` relaxed during E3A.** The contract is expected to churn as E1/E9/E8 implement. The breaking-change gate is **deferred to E3B** (flipped to strict at adoption). E3A's gate is *drift* (generated output matches source) + *compiles* (TS `tsc`, Rust `cargo check` incl. wasm target), not *no-breaking*. |
| 8 | Port strategy | **Port the existing serde/JSON-schema contracts with round-trip fidelity.** `read-access-plan.schema.json` and `object-grants.openapi.json` become `axon/dataaccess/v1`; the `browser-sdk` worker envelopes become `axon/exec/v1`. Field names/casing preserved (or mapped explicitly) so E9 can swap the hand-written types for generated ones without behavior change. |

Non-goals for E3A: Connect/gRPC transport, server or runtime implementations, retiring `axon-browser-sdk.ts`, flipping the breaking-change gate to strict, and re-proto'ing UC REST. All of those are E3B (or never, for UC).

---

## 2. Current-state assessment (grounding)

### 2.1 What already exists

| Asset | Where | Role in E3A |
|---|---|---|
| Buf module + codegen seam | `apps/axon-web/buf.yaml`, `buf.gen.yaml`, `proto/axon/config/v1/settings.proto`, `scripts/config-codegen*.mjs` (E0 deliverables) | The module E3A **extends**. Same `buf.yaml` (add packages), same `buf.gen.yaml` (add the buffa Rust backend), same `npm run codegen` + `codegen:check` drift gate. |
| protobuf-es codegen | `buf.gen.yaml` (`protoc-gen-es` + `protoschema-jsonschema`) | Reused for the TS types of the new messages. |
| Read-authorization contract | `crates/query-contract/schemas/read-access-plan.schema.json` | Ported verbatim to `axon/dataaccess/v1` (see §3.2). |
| Object-grant routes contract | `crates/query-contract/schemas/object-grants.openapi.json` | The `ObjectGrant{List,Head,BatchSign,Range}` route shapes feed `axon/dataaccess/v1` request/response messages. |
| Rust contract types | `crates/query-contract/src/lib.rs` — `ExecutionTarget` (l.21), `BrowserHttpSnapshotDescriptor` (l.264), the `ReadAccessPlan` family | The Rust-side source of truth E3A mirrors in proto (E3B later migrates the crate to consume generated types). |
| Worker IPC envelopes | `crates/browser-sdk/src/lib.rs` — `BrowserWorkerCommand` (l.86), `BrowserWorkerEventPhase` (l.240), `BrowserWorkerEventEnvelope` (l.341), `BrowserWorkerResponseEnvelope` (l.529) | The execution/worker-IPC source for `axon/exec/v1`. |
| Hand-written TS mirror | `apps/axon-web/src/axon-browser-sdk.ts` | The mirror E3B eventually retires; E3A leaves it untouched and generates *alongside* it. |

### 2.2 The conflation E3A's structure resolves

Today the contract is spread across three representations — Rust serde types (`query-contract`, `browser-sdk`), JSON Schema / OpenAPI artifacts (`schemas/`), and a hand-maintained TS mirror (`axon-browser-sdk.ts`) — with no single source of truth and no codegen between them. Worse, the *seam boundaries* are implicit: discovery, read resolution, and execution all live tangled in `services/query.ts` and the worker envelopes. E3A makes the seam boundaries **explicit as proto packages** so each effort imports exactly its surface.

### 2.3 Constraints observed in the repo

- **WASM artifact budget** (`browser_engine_worker.wasm ≤ 750 KB`, enforced by `tests/perf/report_browser_worker_artifact.sh`): the buffa-generated Rust must not bloat the worker. buffa is `no_std` + `alloc` and pure-Rust (no C/upb), which is the right shape, but selecting only the needed crate features (e.g. excluding JSON in the worker build if unused) and confirming the budget at M3 is required. This is the central "plays well with wasm" check.
- **Arrow IPC is the data plane** ([size audit](../program/browser-datafusion-size-audit.md)): proto is control-plane only. **No** DataFusion/Substrait proto, **no** result rows in proto — Arrow bytes continue to cross the worker boundary opaquely.
- **Dependency guardrail** (`tests/security/verify_browser_dependency_guardrails.sh`): any new TS/Rust codegen dep is re-checked against the cloud-SDK denylist + secret-marker scan.
- **ADR-0002**: descriptor/grant messages must never model a place to stash a cloud secret; `BrowserHttpFileDescriptor.url` is a short-lived signed URL or proxy path, nothing more.
- **`StrictMode` / determinism**: generated output is checked in and must be byte-deterministic so the drift check is stable.

---

## 3. Message inventory per seam

All packages are `option`-clean, comment-rich, and use `axon/common/v1` shared types. Field casing in JSON output mirrors the existing serde representation (protobuf-es `json_name` where the current key differs from proto's default lowerCamelCase) so ported contracts round-trip.

### 3.0 `axon/common/v1` (shared)

- `ObjectRef` — `{ connection_id, catalog, schema, name }` (the `TableRef`/`VolumeRef` base the Explorer produces and hands to E9).
- `Page` envelope — `{ repeated <T> items; string next_cursor; }` modeled as a convention (each list response carries `items` + `next_cursor`) for cursor pagination.
- `ProviderError` — `{ code (enum: session_expired | blocked | not_found | unavailable | invalid), message, correlation_id }` mapping the `SessionHttp` 401/403/404 taxonomy (E1 §4.4).
- `Capabilities` — booleans/enums describing how browsable/queryable a provider is (consumed by the Explorer to degrade gracefully).

### 3.1 `axon/catalog/v1` — discovery (E1 consumer)

Normalized discovery nodes (UC OpenAPI types map *into* these; not the UC wire format):

- `CatalogNode`, `SchemaNode`, `TableNode`, `ColumnNode`, `ViewNode`, `VolumeNode`, `FunctionNode`, `ModelNode`.
- `TableMetadata` — columns, properties, storage location, table type, snapshot/version, Delta protocol features (mirror `delta_protocol_features.rs` shape).
- `VolumeMetadata` — type, storage location, comment (discovery only; **no** file entries — those are `axon/fs/v1`).
- List responses for each level (`ListCatalogs`, `ListSchemas`, `ListTables`, `ListVolumes`, `ListFunctions`, `ListModels`) using the `Page` convention.

No service — the discovery seam is TanStack `queryOptions` over these messages (E1 §3.3).

### 3.2 `axon/dataaccess/v1` — read resolution + descriptors (E9 consumer)

Direct port of `read-access-plan.schema.json` (§ grounding above) + the object-grant routes:

- `ReadAccessPlanReason` (enum) — `row_filter`, `column_mask`, `view`, `unknown_policy_state`, `no_direct_external_engine_read_support`, `unsupported_table_type`, `grant_expired`, `storage_cors_blocked`, `broker_unavailable`.
- `BrowserHttpFileDescriptor` — `{ path, url, size_bytes, partition_values, stats }`.
- `BrokeredObjectAccess` — `{ list, head, get, range_get, batch_sign, proxy_range }`.
- `BrokeredPolicyAuthority` — `{ authority (unity_catalog | delta_sharing | mock_broker), direct_external_engine_read (confirmed | not_confirmed) }`.
- `ReadAccessPlan` (`oneof plan`): `BrokeredDeltaReadAccessPlan` · `DeltaSharingReadAccessPlan` · `SqlFallbackRequiredReadAccessPlan` · `BlockedReadAccessPlan` (each preserving its required fields incl. `tableId`/`fullName`/`expiresAtEpochMs`).
- `BrowserHttpSnapshotDescriptor` — mirror `query-contract/src/lib.rs:264` (the openable descriptor → `openDeltaTable()`).
- `TableReadResolution` (`oneof`): `descriptor` | `read_access_plan` | `fallback` | `blocked` — the E9 DataAccessResolver return type.
- `ObjectGrant{List,Head,BatchSign,Range}Request/Response` — from `object-grants.openapi.json`. **These are request/response messages, not a proto service** (they hit the BFF over the `SessionHttp` fetch wrapper, not a Connect endpoint, until E3B decides otherwise).

### 3.3 `axon/exec/v1` — execution + worker IPC (E9 consumer; the one place with services)

Port the `browser-sdk` envelopes and the execution target:

- `ExecutionTarget` (enum) — mirror `query-contract/src/lib.rs:21` (`browser_wasm` | `native` | reserved `remote_service`).
- `ExecuteRequest` — `{ sql, ObjectRef ref, descriptor handoff, pagination/limit, request_id }`.
- `PreviewRequest` — `{ ObjectRef ref, limit }` (the Explorer "Sample data" call).
- `BrowserWorkerCommand` (`oneof`) — mirror `browser-sdk/src/lib.rs:86`.
- `BrowserWorkerEventEnvelope` / `BrowserWorkerEventPhase` — progress/log/metrics/fallback stream (mirror l.341 / l.240).
- `BrowserWorkerResponseEnvelope` — mirror l.529.
- **`service QueryEngine`** (defined here, *implemented* in E3B/E9): `Execute (stream events)`, `Preview`, `Cancel`. The Arrow IPC result payload stays an opaque `bytes` field — **not** modeled as proto rows (and buffa's zero-copy `bytes` views suit this opaque payload). Note: buffa does not yet emit Rust *service* stubs, so only the **messages** generate in E3A; the Rust service impl is E3B via `connect-rust` (the TS side uses Connect-ES). The proto `service` declaration is committed now so the surface is reviewable.

> The wasm-bindgen inner JSON-string seam (`snapshot_json`/`request_json`) is **not** changed by E3A; how/whether it adopts proto-JSON is an E3B question.

### 3.4 `axon/fs/v1` — filesystem entries (E8 consumer)

- `FsEntry` — `{ name, path, kind (file | dir), size_bytes, modified_at, content_type }`.
- `ListDirectoryRequest/Response` (Page convention) — directory listing for `UnityCatalogVolume` / `ObjectStorePrefix` / `LocalFolder` backends.
- `ObjectReadResolution` (`oneof`) — `signed_url` | `proxy_range` | `denied` (the file-bytes analogue of `TableReadResolution`, reusing `axon/dataaccess/v1` object-grant primitives).
- No service — the filesystem seam is a TS interface over these messages (E8).

---

## 4. Codegen and repository layout

```
apps/axon-web/
  buf.yaml                      # E0; add the new packages to the module
  buf.gen.yaml                  # E0; add the buffa Rust backend (buf.build/anthropics/buffa)
  proto/axon/
    config/v1/settings.proto    # E0 (unchanged)
    common/v1/*.proto           # E3A
    catalog/v1/*.proto          # E3A
    dataaccess/v1/*.proto       # E3A
    exec/v1/*.proto             # E3A
    fs/v1/*.proto               # E3A
  src/generated/
    config/                     # E0 (unchanged)
    catalog/ dataaccess/ exec/ fs/ common/   # E3A protobuf-es TS (checked in)
crates/
  <contract-proto-crate>/src/generated/       # E3A buffa Rust (checked in), or a buffa-build OUT_DIR build — decide at M1
```

- **TS**: `protoc-gen-es` emits `src/generated/<pkg>/` (checked in, like config). `import type` where possible to keep them out of the FOUC/main bundle.
- **Rust**: add the **buffa** plugin to `buf.gen.yaml` — preferably the Buf remote plugin `buf.build/anthropics/buffa` (with the `json=true` opt for canonical JSON; optionally `protoc-gen-buffa-packaging` to emit the module tree), so `buf generate` needs only the `buf` CLI. Depend on `buffa` + `buffa-types` (`0.x`); use `buffa-build` for a `build.rs` variant if we choose `OUT_DIR` over checked-in. Emit into a contract crate and **verify `cargo check --target wasm32-unknown-unknown`** (the gating wasm check; fall back to `protoc-gen-prost` only if buffa fails it).
- **Gates**: extend `npm run codegen` (canonical `buf generate`) + the deterministic offline mirror used where the BSR is unreachable, and the `codegen:check` drift gate, to cover the new packages. Rust generation is verified by `cargo check` (incl. the wasm target).
- **Checked-in generated output** mirrors the repo's existing convention (review-visible + offline builds; CI regenerates and diffs).

---

## 5. Milestones (each independently reviewable)

Each milestone gates on: `buf lint` (relaxed breaking), `codegen:check` (no drift), `tsc --noEmit`, `cargo check` (incl. `wasm32-unknown-unknown`), and the dependency guardrail for any new codegen dep. No client is required to adopt the generated types within E3A (adoption is E1/E9/E8).

### M0 — Module scaffolding + `axon/common/v1` + buffa/wasm spike (no client change)
- Add the new packages to `buf.yaml`; add the **buffa** backend (`buf.build/anthropics/buffa`, `json=true`) to `buf.gen.yaml`; wire the new dirs into `npm run codegen` + `codegen:check`.
- Define `axon/common/v1` (`ObjectRef`, `Page` convention, `ProviderError`, `Capabilities`) and generate TS + Rust.
- **buffa/wasm spike (the decision gate):** compile the generated `axon/common/v1` buffa Rust for `wasm32-unknown-unknown` with the feature set the worker would use; confirm it builds and the artifact-budget headroom is acceptable. If buffa fails the wasm build/budget, fall back to `protoc-gen-prost` (+ `pbjson` for JSON) and record the decision here.
- **Gate:** codegen drift + `tsc` + `cargo check --target wasm32-unknown-unknown` green; dependency guardrail re-run for the buffa crates/plugin.

### M1 — `axon/dataaccess/v1` (port existing contracts)
- Port `read-access-plan.schema.json` + the object-grant routes to proto; preserve field names/casing (`json_name`) for round-trip.
- Add a **round-trip parity test**: existing JSON fixtures decode into the generated types and re-encode identically (the safety net E9/E3B rely on).
- **Gate:** parity fixtures pass in TS and Rust; drift + compile gates green.

### M2 — `axon/catalog/v1` + `axon/fs/v1` (discovery + filesystem)
- Define the normalized discovery nodes/metadata and the filesystem entries/listing/resolution messages. Document the UC-OpenAPI → `axon/catalog/v1` mapping (no UC re-proto).
- **Gate:** drift + compile; a small example mapping test (UC OpenAPI sample → `TableNode`/`TableMetadata`).

### M3 — `axon/exec/v1` + the `QueryEngine` service shape + size check
- Port the worker IPC envelopes + execution requests; define the `QueryEngine` service signature (messages + service stub only; **no** implementation). Arrow payload stays opaque `bytes`.
- Run the WASM artifact budget check with the buffa types compiled into a throwaway worker build to confirm no budget regression.
- **Gate:** drift + compile; WASM budget unaffected; explicit assertion that no result-row/Arrow data is modeled in proto.

### M4 — Adoption guide + living-draft policy doc + handoff
- Write the per-seam "how to consume the generated types" note for E1/E9/E8 (which package, which messages, how the TS types replace hand-written sketches), and document the **living-draft → strict** transition that E3B owns (when each consumer adopts, the breaking gate flips for that package).
- **Gate:** docs reviewed; the four packages generate clean TS + Rust; one downstream smoke import from E1's navigation types proves consumability.

---

## 6. Risks and mitigations

| Risk | L/I | Mitigation |
|---|---|---|
| **Designing the wrong contract early** (churn forces re-review) | High / Med | This is *expected* — living-draft policy keeps breaking changes allowed; E3A's value is one reviewable surface, not a frozen one. Tighten only at adoption (E3B). |
| **buffa doesn't build for / fit `wasm32-unknown-unknown`** | Med / High | The M0 spike is the decision gate; buffa is `no_std` + `alloc` and pure-Rust (no C/upb) so it should fit, but select minimal crate features and confirm the budget. Documented **prost fallback** if it fails. |
| **buffa is pre-1.0 (`0.x`)** — API churn / MSRV moves | Med / Med | Pin exact versions; checked-in generated output + drift gate localizes any regen change; buffa's MSRV tracks ~12 months behind stable (currently 1.87), comfortably within our toolchain. Re-evaluate at E3B before flipping gates strict. |
| **buffa lacks Rust service codegen** | Low / Low | E3A is messages-only, so unaffected; the `QueryEngine` service Rust impl is E3B via `connect-rust`. |
| **generated Rust bloats the WASM worker** past the 750 KB gate | Med / High | M3 runs the existing artifact budget check with the generated types compiled in; exclude unused features (e.g. JSON) in the worker build; keep messages lean; result payload stays opaque `bytes`. |
| **Round-trip drift** when porting JSON-schema/serde to proto | Med / High | M1/M2 parity fixtures (decode existing JSON → generated type → re-encode identical), in both TS and Rust, before any client adopts. |
| **Re-proto'ing UC REST by accident** | Low / Med | Explicit decision (D5): proto defines normalized nodes only; UC stays OpenAPI-vendored with a documented mapping layer. |
| **Modeling secrets / Arrow rows in proto** (ADR-0002 / size-audit no-go) | Low / High | Descriptors carry only signed-URL/proxy paths; Arrow stays opaque `bytes`; explicit M3 assertion. |
| **Adding a transport prematurely** (scope creep into E3B) | Med / Med | Services only for the execution boundary, as a *signature* with no implementation; everything else is message-only. |
| **BSR/offline codegen divergence** | Low / Med | Keep both the canonical `buf generate` and the deterministic offline mirror (as E0's config codegen already does) under one drift gate. |

---

## 7. Test strategy

- **Codegen drift:** `codegen:check` extended to the new packages (generated TS matches source); Rust generation verified by `cargo check` incl. `wasm32-unknown-unknown`.
- **Round-trip parity (the core test):** existing `read-access-plan` + object-grant JSON fixtures decode into generated types and re-encode byte-identically, in **both** TS (protobuf-es JSON) and Rust (**buffa's built-in proto-canonical JSON via serde** — no `pbjson` needed). The same for `browser-sdk` envelope fixtures.
- **Mapping example:** a UC OpenAPI sample maps into `axon/catalog/v1` `TableNode`/`TableMetadata` without loss for the fields E1 needs.
- **WASM budget:** M3 builds a throwaway worker with the buffa types and runs `report_browser_worker_artifact.sh`; assert no regression. (The M0 spike already proves the `wasm32-unknown-unknown` build.)
- **No-go assertions:** a test/lint that fails if any `axon/exec/v1` message models result rows / Arrow data (must stay opaque `bytes`), and a secret-marker scan over the generated output + descriptors.
- **Consumability smoke:** one import from a downstream package (E1 navigation type) compiles against the generated TS — proves the seam is usable before E1 starts.
- **CI gates:** dependency guardrail (new codegen plugins), WASM budget (unaffected), drift + compile. The **breaking-change gate stays relaxed** in E3A and is flipped to strict per-package in E3B at adoption.

---

## 8. ADR strategy

E3A is a build/codegen contract substrate, not an architectural-invariant change, so it does **not** introduce an ADR of its own. The invariants it serves are owned elsewhere: discovery seam → **ADR-0009** (E1), execution + read-resolution invariants → **ADR-0010** (E9), browser secret boundary → **ADR-0002**. E3A only records its message layout + living-draft policy in this plan and the strategy doc. The Buf-toolchain hardening + breaking-change-gate decision is **E3B**'s.

---

## 9. Open questions to confirm at kickoff

1. **Generated-Rust home** — a dedicated `contract-proto` crate with checked-in `src/generated/`, or an `OUT_DIR` `build.rs` consumed by `query-contract`? (Affects review visibility vs. build ergonomics; recommend checked-in + drift check, matching the repo convention.)
2. **JSON casing strategy** — preserve every existing serde key via `json_name` (maximal round-trip fidelity, some proto-idiom friction) vs. adopt proto-idiomatic lowerCamelCase and map at the edges. Recommend `json_name` for the ported `dataaccess`/`exec` packages, idiomatic for the new `catalog`/`fs` packages.
3. **`oneof` vs. tagged-string** for the `ReadAccessPlan`/`TableReadResolution` unions — proto `oneof` (clean codegen) vs. mirroring the existing `plan_type` discriminant string. Recommend `oneof` with the discriminant preserved in JSON via field naming.
4. **buffa for the Rust backend (preferred)** — confirm the M0 spike: does `buf.build/anthropics/buffa`-generated Rust build for `wasm32-unknown-unknown` and fit the worker budget? If yes (expected, given `no_std` + pure-Rust), adopt buffa and drop the prost+pbjson path entirely. If no, fall back to `protoc-gen-prost` and treat buffa adoption as an E3B revisit. Also decide checked-in generated source vs. `buffa-build` `OUT_DIR`.
5. **How much of `browser-sdk` to port now** — the full envelope set, or only the execution subset E9 needs first? Recommend full port (one reviewable surface) but mark the not-yet-consumed messages.
