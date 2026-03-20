# WASM + Delta Lake on GCS Program Bundle

- Status: Drafted from in-chat architecture bundle
- Date: 2026-03-20
- Scope: architecture, ownership, ADR split, epic split, release gates, and GitHub setup

This document is the source bundle for the initial program definition. The ADRs under [docs/adr](../adr/README.md) and the epic issue bodies under [docs/epics](../epics/README.md) are derived from this document.

## Ecosystem Facts Anchoring The Design

As of March 20, 2026:

- DataFusion's WASM work is still tracked in an open upstream epic.
- Browser multi-partition execution in wasm is still open because current execution hits Tokio or runtime limits in the browser.
- The community playground still calls itself early experimental and currently advertises HTTP and S3, with GCS and Azure still "on the way".
- Current `datafusion-wasm-bindings` targets `wasm32-unknown-unknown` but is pinned to DataFusion 45 and only enables OpenDAL `services-s3` and `services-http`.
- `delta-rs` / `deltalake_core` already exposes GCS and DataFusion integration on the native side, including `update_datafusion_session`.
- GCS signed URLs only work through XML API endpoints, and browser CORS behavior depends on the XML endpoint path actually used.

One important correction carried into this bundle: deletion-vector support should be treated as capability-gated and proven by tests in the exact runtime. Ecosystem signals are mixed enough that a blanket "supported" or "unsupported" statement is not strong enough for engineering policy.

## North-Star Outcome

Build a production-ready system where:

- DataFusion runs in browser WASM for supported read workloads.
- Delta tables live in GCS.
- The browser never receives long-lived cloud credentials.
- A trusted control plane resolves snapshots and provides browser-safe access.
- A native runtime exists as the correctness oracle and fallback path.
- Hosted UDF/WASM uses a separate ABI and runtime track from browser WASM.

## Core Architectural Choice

Adopt a hybrid architecture with three tiers:

1. Trusted control plane
2. Browser WASM query plane
3. Native reference runtime

The control plane resolves Delta snapshots, applies feature and table policy, produces signed HTTPS URLs or narrow proxy descriptors, and audits access. The browser runtime runs DataFusion in `wasm32-unknown-unknown`, uses browser-safe HTTP only, supports a constrained read-only query envelope, and falls back deterministically when capability is insufficient. The native runtime uses `deltalake_core` + GCS + DataFusion as the semantic oracle, operational fallback, and upgrade canary.

## Best-In-Class Engineering Standards

- Security by design: no cloud secrets in the browser, signed object-scoped access only, auditable access path.
- Correctness before optimization: native reference runtime exists before browser production launch.
- Capability-gated protocol support: advanced Delta features are classified, tested, and routed or failed explicitly.
- Range-read correctness as a hard gate: Parquet and Delta performance depends on real range reads, not interface assumptions.
- Observable fallback: every browser-to-native fallback emits a structured reason.
- Upstream-first fork policy: every private patch is tracked, owned, and assigned an upstream disposition.
- Conformance before performance claims: no "fast" claim without bytes-fetched, files-skipped, and parity data.

## Role-Based Ownership Model

### Core Roles

- Executive sponsor: approves funding, roadmap trade-offs, and production launch criteria; breaks ties on schedule versus scope.
- Query platform lead: owns end-to-end architecture and release readiness; owns ADR approval, epic sequencing, and cross-team dependencies.
- Runtime / engine team: owns DataFusion integration, native runtime, browser runtime, query correctness, object-store adapters, and fallback logic.
- Storage platform team: owns Delta snapshot resolution, GCS integration, signed URL generation, retention alignment, storage access policy, and the trusted control plane.
- Web platform team: owns browser bindings, JS/TS SDK, app shell, browser UX, and bundle health.
- Security engineering: owns threat model, IAM, CORS, signed URL TTL policy, secret handling, and launch review.
- SRE / production engineering: owns deployment, observability, runbooks, scaling, and incident readiness.
- QA / performance engineering: owns conformance harness, performance benchmarks, regression thresholds, and parity dashboards.
- DX / OSS maintainer: owns upstream contribution strategy, fork policy, release cadence, and dependency upgrade playbooks.

### DRI Rule

Every ADR and every epic must have:

- one accountable owner,
- one delivery DRI,
- at least one consulted role,
- one explicit approver.

### Escalation Order

1. Delivery DRI
2. Accountable owner
3. Query platform lead
4. Executive sponsor

## Package Boundaries

This program assumes a monorepo with the following initial package map:

| Package | Owner | Responsibility |
| --- | --- | --- |
| `crates/query-contract` | Runtime / engine | Shared request/response types, error taxonomy, capability flags, fallback reasons |
| `crates/native-query-runtime` | Runtime / engine | Authoritative native DataFusion + Delta execution path |
| `crates/delta-control-plane` | Storage platform | Snapshot resolution, signed URL/proxy descriptors, table policy |
| `crates/wasm-query-runtime` | Runtime / engine | Browser SQL execution, browser-safe runtime controls |
| `crates/wasm-http-object-store` | Runtime / engine | HTTP range adapter for signed URL / proxy object access |
| `crates/browser-sdk` | Web platform | Public JS/TS API for browser query sessions |
| `crates/query-router` | Runtime / engine | Chooses browser vs native execution and records fallback reason |
| `crates/udf-abi` | Runtime / engine | Versioned WIT / WASI Preview 2 ABI for hosted UDFs |
| `crates/udf-host-wasi` | Runtime / engine | Hosted UDF runtime using WASIp2 / wasmtime |
| `apps/web-console` | Web platform | Playground, demo, operator console |
| `services/query-api` | Storage platform | Authenticated API for snapshot resolution and optional server execution |
| `tests/conformance` | QA / performance | Browser vs native semantic parity |
| `tests/perf` | QA / performance | Latency, bytes fetched, skip rates, cache hit rate, bundle size |
| `tests/security` | Security | Secret leakage checks, CORS/origin tests, signed URL policy tests |

### Boundary Rules

- `native-query-runtime` may depend on `deltalake_core` with `gcs` and `datafusion`, may register object stores in `RuntimeEnv`, and must not contain browser fetch logic.
- `delta-control-plane` may inspect Delta snapshots and mint short-lived access, must not embed query planner logic, and must not expose long-lived credentials.
- `wasm-query-runtime` may accept SQL and query options and register tables through descriptors, but must not hold service-account keys or independently discover tables using browser cloud credentials.
- `wasm-http-object-store` may issue HTTP range requests, but must not own SQL semantics or signing.
- `udf-abi` and `udf-host-wasi` must stay separate from browser runtime assumptions and must use a versioned WIT contract.

### Dependency Direction

Low level:

- `query-contract`
- `udf-abi`

Middle layer:

- `native-query-runtime`
- `delta-control-plane`
- `wasm-http-object-store`
- `query-router`
- `udf-host-wasi`

Top layer:

- `browser-sdk`
- `services/query-api`
- `apps/web-console`

Tests may depend on everything.

## Roadmap

### Phase 0: Architecture And Scaffolding

Objective: freeze architecture before implementation drift.

Exit criteria:

- ADRs approved
- package ownership assigned
- no browser package allowed to take a direct cloud credential dependency
- control-plane and native-runtime boundaries reviewed

### Phase 1: Native Reference Runtime

Objective: build the correctness oracle first.

Exit criteria:

- native read-only Delta-on-GCS path working
- SQL corpus running
- explicit handling for unsupported features
- explain + metrics available

### Phase 2: Trusted Control Plane And Browser Read Path

Objective: allow browser reads using signed HTTPS or proxy descriptors.

Exit criteria:

- browser receives no service-account material
- short-lived signed URLs or proxy descriptors working
- CORS validated against actual endpoint shape
- audit logging working

### Phase 3: Browser WASM Query Runtime

Objective: run supported DataFusion workloads in browser wasm.

Exit criteria:

- browser parity for supported SQL
- deterministic fallback for unsupported cases
- size, startup, and memory budgets tracked

### Phase 4: Conformance, Performance, And Security Hardening

Objective: make the system production-worthy.

Exit criteria:

- range reads proven
- parity suite stable
- metrics dashboards live
- release checklist enforced
- security review signed off

### Phase 5: WASI UDF ABI And Hosted Runtime

Objective: create a separate hosted extension model.

Exit criteria:

- versioned WIT ABI
- hosted runtime working
- compatibility tests passing

### Phase 6: Upstreaming And Lifecycle

Objective: reduce fork cost and stay maintainable.

Exit criteria:

- no untracked private patches
- upgrade rehearsal in CI
- regular compatibility reviews

## Milestones

- `M0`: Architecture frozen
- `M1`: Native reference reads Delta on GCS
- `M2`: Control plane signs browser-safe read access
- `M3`: Browser runtime executes supported SQL
- `M4`: Production hardening complete
- `M5`: Hosted WASI UDF path available
- `M6`: Upstreamed / maintainable

## Critical Path

The primary critical path is:

`EPIC-01 -> EPIC-02 -> EPIC-03 -> EPIC-04 -> EPIC-05`

Parallelizable work:

- EPIC-02 and EPIC-03 can overlap after EPIC-01.
- EPIC-06 can run later without blocking the browser MVP.
- EPIC-07 should start as soon as private patches exist.

## Work That Must Not Reach The Critical Path

- browser multi-threading
- direct browser cloud credentials
- write-path support
- unversioned UDF ABI design
- premature optimization before range-read correctness is proven

Browser multi-partition support remains future work while the upstream browser/runtime issue remains open.

## Non-Negotiable Release Gates

1. No cloud secrets in the browser.
2. Range-read correctness is proven, not assumed.
3. Native runtime exists before browser production launch.
4. Browser runtime ships single-partition by default.
5. Every supported browser query has a native parity test.
6. Advanced or unsupported Delta features fail clearly or route to native.
7. Fallback reasons are structured and observable.
8. Every private fork patch is tracked with owner and upstream disposition.

## Program Recommendation

The recommended path remains:

- hybrid architecture
- browser-safe signed HTTPS or proxy access
- native correctness oracle
- separate hosted UDF/WASI track
- upstream-first fork discipline

This recommendation matches the ecosystem state as of March 20, 2026: browser execution is real but still maturing upstream, current community browser tooling is still experimental and not yet first-class for GCS, native `delta-rs` remains the strongest trusted-side path, and GCS signed URL plus CORS behavior strongly favors a control-plane-mediated browser design.

## References

- [Apache DataFusion WASM epic](https://github.com/apache/datafusion/issues/13815)
- [Apache DataFusion browser multi-partition issue](https://github.com/apache/datafusion/issues/15599)
- [delta-rs best practices](https://delta-io.github.io/delta-rs/delta-lake-best-practices/)
- [`unsafe_opendal_store.rs` in `datafusion-wasm-bindings`](https://raw.githubusercontent.com/datafusion-contrib/datafusion-wasm-bindings/main/src/unsafe_opendal_store.rs)
- [GCS signed URLs documentation](https://docs.cloud.google.com/storage/docs/access-control/signed-urls)
- [`datafusion-udf-wasm` WASM guidance](https://github.com/influxdata/datafusion-udf-wasm/blob/main/WASM.md)
- [`deltalake_core` docs](https://docs.rs/deltalake-core)
- [`datafusion-wasm-bindings` Cargo.toml](https://raw.githubusercontent.com/datafusion-contrib/datafusion-wasm-bindings/main/Cargo.toml)
