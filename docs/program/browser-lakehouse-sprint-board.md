# Browser Lakehouse Sprint Board

- Status: Draft
- Date: 2026-04-07
- Scope: board-ready sprint sequencing for the browser lakehouse vision and launch-hardening work
- Source plans:
  - [Browser Lakehouse Vision Execution Plan](../plans/2026-04-07-browser-lakehouse-vision-execution-plan.md)
  - [Browser Lakehouse Engine Next PI Plan](../plans/2026-03-31-browser-engine-next-pi-plan.md)
  - [Browser Lakehouse Engine Strategy](./browser-lakehouse-engine-strategy.md)
  - [GitHub Project Setup](./github-project-setup.md)

## Board Defaults

- PI length: 5 sprints
- Milestone target: `M4`
- Primary outcome: ship a releaseable browser lakehouse engine without changing the trust boundary
- Issue granularity: one board card should fit inside one sprint and produce testable evidence

## Recommended Project Fields

- `Title`
- `Sprint`
- `Status`
- `Owner`
- `Area`
- `Risk`
- `Milestone`
- `Depends on`
- `Exit evidence`

## Recommended Columns

- `Backlog`
- `Ready`
- `In Progress`
- `Review`
- `Blocked`
- `Done`

## Label Conventions

- Area:
  - `area/runtime-wasm`
  - `area/control-plane`
  - `area/perf`
  - `area/security`
  - `area/conformance`
- Type:
  - `type/epic`
  - `type/release-gate`
- Risk:
  - `risk/high`
  - `risk/medium`
  - `risk/low`

## Sprint 1: Contract Freeze

**Goal:** lock the worker-first browser engine contract so the rest of the work builds on one transport, one observability vocabulary, and one runtime SKU story.

### BL-01 Freeze Arrow IPC worker boundary

- Owner: Web platform team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: none
- Scope:
  - make Arrow IPC the only large-result transport in `crates/browser-sdk`
  - preserve structured fallback and hard-fail metadata
  - expose runtime SKU and artifact identity from `crates/browser-engine-worker`
- Exit evidence:
  - `cargo test -p browser-sdk -p browser-engine-worker --locked`
  - `cargo test -p browser-sdk --target wasm32-unknown-unknown --locked --test wasm_smoke`
  - `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke`

### BL-02 Align release handoff and observability contract

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/medium`
- Milestone: `M4`
- Depends on: `BL-01`
- Scope:
  - align `docs/program/browser-lakehouse-release-handoff.md`
  - align `docs/program/browser-observability-contract.md`
  - align `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`
- Exit evidence:
  - docs merged with no contract drift between SDK, worker, and release docs

### BL-03 Seed GitHub board fields and labels

- Owner: Query platform lead
- Area: `area/runtime-wasm`
- Risk: `risk/low`
- Milestone: `M4`
- Depends on: none
- Scope:
  - create project fields from this board
  - apply area, risk, and milestone labels
  - open Sprint 1 issues first
- Exit evidence:
  - project created or updated using `docs/program/github-project-setup.md`

## Sprint 2: Browser I/O Substrate

**Goal:** harden the transport layer so Parquet and Delta readers can depend on stable extent caching, object identity validation, and browser-local access modes.

### BL-04 Add metadata probe and extent cache

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-01`
- Scope:
  - add metadata probe helpers for size and identity
  - coalesce adjacent range requests into reusable extents
  - replace exact-range-only reuse with stable extent keys
- Exit evidence:
  - `cargo test -p wasm-http-object-store --locked`
  - `cargo check -p wasm-http-object-store --target wasm32-unknown-unknown --locked`

### BL-05 Add cache validation and transport metrics

- Owner: Runtime / engine team
- Area: `area/perf`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-04`
- Scope:
  - validate cached extents with object identity
  - surface protocol errors when validation headers are unavailable
  - emit bytes-fetched, bytes-reused, and validation-miss metrics
- Exit evidence:
  - transport tests cover stale-cache and header-validation cases
  - perf docs name the new transport metrics

### BL-06 Add browser-local object and cache-mode reporting

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/medium`
- Milestone: `M4`
- Depends on: `BL-04`
- Scope:
  - add browser-local file or blob-backed reads
  - expose memory-only versus persistent-cache mode
  - keep browser-local access separate from signed-URL or proxy access
- Exit evidence:
  - `cargo test -p wasm-http-object-store --locked`
  - `cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke`

## Sprint 3: Delta Snapshot Core

**Goal:** make browser-side Delta snapshot reconstruction explicit, testable, and useful for file pruning before any Parquet bootstrap work begins.

### BL-07 Deepen checkpoint and sidecar support

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-04`
- Scope:
  - `_last_checkpoint`
  - classic and V2 checkpoints
  - sidecar resolution
  - deterministic replay after checkpoint
- Exit evidence:
  - `cargo test -p wasm-delta-snapshot -p delta-control-plane --locked`
  - `cargo test -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked --test wasm_smoke`

### BL-08 Expose pruning facts from Delta add actions

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-07`
- Scope:
  - expose file path, size, partition values, and stats for early pruning
  - keep storage, JSON, and Parquet handlers separated
  - cross-check browser snapshot output against `delta-control-plane`
- Exit evidence:
  - snapshot tests prove file-level facts are available before Parquet reads

### BL-09 Classify advanced Delta features for browser routing

- Owner: Runtime / engine team
- Area: `area/conformance`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-07`
- Scope:
  - classify native-only versus terminal unsupported protocol states
  - keep unknown protocol features as hard failures
  - encode compatibility expectations in snapshot tests
- Exit evidence:
  - control-plane and browser snapshot tests agree on compatibility output

## Sprint 4: Streaming Scan And Runtime Orchestration

**Goal:** shift the browser runtime from narrow in-crate execution logic to a productized orchestration layer over snapshot facts and streaming Parquet scans.

### BL-10 Add streaming Parquet scan primitives

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-04`, `BL-08`
- Scope:
  - add known-size-aware metadata bootstrap
  - support streaming `RecordBatch` production
  - publish scan metrics needed by runtime and perf docs
- Exit evidence:
  - `cargo test -p wasm-parquet-engine --locked`
  - `cargo test -p wasm-parquet-engine --target wasm32-unknown-unknown --locked --test wasm_smoke`

### BL-11 Rewire runtime for early pruning and streaming results

- Owner: Runtime / engine team
- Area: `area/runtime-wasm`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-08`, `BL-10`
- Scope:
  - prune from Delta facts before footer reads
  - stream Arrow IPC results through the runtime path
  - preserve structured fallback through `query-router`
- Exit evidence:
  - `cargo test -p wasm-query-runtime -p query-router --locked`
  - `cargo test -p wasm-query-runtime --target wasm32-unknown-unknown --locked --test wasm_smoke`

### BL-12 Add cancellation and query-budget enforcement

- Owner: Runtime / engine team
- Area: `area/perf`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-11`
- Scope:
  - cancellation by dropping execution state
  - memory or operator budgets for unsupported heavy operators
  - structured native fallback when the budget is exceeded
- Exit evidence:
  - runtime tests prove deterministic reroute for over-budget requests

## Sprint 5: Launch Hardening

**Goal:** make the browser engine measurable and shippable with explicit size budgets, compatibility evidence, security guardrails, and clear external blockers.

### BL-13 Split DataFusion and legacy narrow SKUs with artifact reporting

- Owner: Web platform team
- Area: `area/perf`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-11`
- Scope:
  - Daxis-facing DataFusion runtime SKU
  - legacy narrow compatibility SKU
  - worker artifact reports enabled feature set, SKU identity, and browser DataFusion availability
- Exit evidence:
  - `cargo test -p browser-engine-worker --locked`
  - `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture`
  - `bash tests/perf/report_browser_worker_artifact.sh`

### BL-14 Enforce browser size, dependency, and artifact guardrails

- Owner: Security engineering
- Area: `area/security`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `BL-13`
- Scope:
  - enforce size budgets for worker artifacts
  - inspect dependency tree and browser bundle composition
  - keep cloud-secret and signing dependencies out of browser crates
- Exit evidence:
  - `bash tests/security/verify_browser_dependency_guardrails.sh`

### BL-15 Publish release evidence and compatibility matrix

- Owner: QA / performance engineering
- Area: `area/conformance`
- Risk: `risk/medium`
- Milestone: `M4`
- Depends on: `BL-09`, `BL-13`, `BL-14`
- Scope:
  - update release evidence and launch checklist
  - publish supported, native-only, and terminal compatibility states
  - keep external blockers explicit instead of implied
- Exit evidence:
  - release docs link only to real repo-owned artifacts and commands

## Parallel External Lane

These cards must exist on the board, but they should remain in `Blocked` or `External` until the owning teams engage. They are launch dependencies, not engine-implementation tasks.

### EX-01 Ship trusted service for signed URL or proxy issuance

- Owner: Storage platform team
- Area: `area/control-plane`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: none
- Exit evidence:
  - service implementation merged outside this repository

### EX-02 Approve signed URL TTL, audit logging, and request correlation

- Owner: Security engineering
- Area: `area/security`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `EX-01`
- Exit evidence:
  - approved TTL policy
  - audit and correlation pipeline demonstrated

### EX-03 Validate production XML-endpoint CORS or origin behavior

- Owner: Security engineering
- Area: `area/security`
- Risk: `risk/high`
- Milestone: `M4`
- Depends on: `EX-01`
- Exit evidence:
  - production-path CORS validation documented

### EX-04 Provision cloud fixtures, CI variables, and service dashboards

- Owner: Storage platform team and SRE / production engineering
- Area: `area/perf`
- Risk: `risk/medium`
- Milestone: `M4`
- Depends on: `EX-01`
- Exit evidence:
  - env-gated fixture coverage runs in CI
  - live dashboards and alert ownership exist outside the repo

## Definition Of Done For The PI

- Sprint 1 through Sprint 5 cards are in `Done`
- external cards are either complete or explicitly tracked as launch blockers
- the release checklist and release evidence docs remain aligned with the real repository state

## Suggested Issue Creation Order

1. `BL-01`
2. `BL-02`
3. `BL-04`
4. `BL-05`
5. `BL-06`
6. `BL-07`
7. `BL-08`
8. `BL-09`
9. `BL-10`
10. `BL-11`
11. `BL-12`
12. `BL-13`
13. `BL-14`
14. `BL-15`
15. `EX-01` through `EX-04`
