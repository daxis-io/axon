# Browser Lakehouse Engine Next PI Plan

- Date: 2026-03-30
- PI length: 5 sprints / 10 weeks
- Planning baseline: treat `docs/plans/2026-03-28-browser-lakehouse-engine-implementation-plan.md` as complete

## PI Recommendation

There are three viable next-PI shapes:

1. Compatibility + conformance hardening
2. Browser packaging + performance substrate
3. Service-backed signed URL / proxy integration

Recommended: option 1.

Why this is the best fit:

- the crate split and narrow browser execution path are already in place
- the largest remaining product risk is ambiguous Delta feature support, not missing crate boundaries
- option 2 is useful, but it should follow explicit protocol classification and fallback policy
- option 3 is blocked on `services/query-api`, which is not present in this repository

## 1. PI Summary

### PI objective

Make the current browser lakehouse engine launch-shape correct, explicit, and measurable by hardening Delta protocol compatibility, browser-local snapshot parity, structured fallback behavior, and browser-specific verification gates without broadening the browser SQL envelope.

### Why this PI matters now

The previous implementation plan delivered the intended architecture:

- `delta-control-plane` remains native and trusted
- `wasm-delta-snapshot` exists as the browser-safe Delta reader
- `wasm-parquet-engine` exists as the browser scan layer
- `wasm-query-runtime` orchestrates planning, pruning, execution, and fallback
- `browser-sdk` owns the embedding and IPC boundary

What is still missing is the evidence and policy needed to treat that architecture as a production-bound slice:

- browser-local Delta snapshots still do not emit partition column type metadata
- advanced Delta features are not yet classified and enforced end-to-end
- dedicated wasm execution coverage is incomplete
- current browser size tracking is still a proxy, not a true shipped-engine artifact
- release-gate items around observability and advanced-feature handling remain open

### Expected outcomes by the end of the PI

- Browser and native snapshot resolution agree on snapshot version, active-file set, partition column typing, and advanced-feature classification for the supported fixture corpus.
- Tables that require native-only Delta capabilities are detected before browser scan planning and are routed or failed with structured reasons.
- The browser Delta reader has an explicit compatibility matrix covering time travel, checkpoint variants, sidecars, partition typing, and advanced protocol items.
- CI runs dedicated wasm execution coverage for the browser-engine crates and tracks a real browser-engine wasm artifact budget plus cold-start baseline.
- Release documentation, perf notes, and fallback telemetry reflect the actual supported browser envelope instead of future-roadmap placeholders.

## 2. Starting Point

### What is already complete

- `crates/wasm-http-object-store` provides browser-safe range reads, metadata probes, validation hooks, and extent-cache primitives.
- `crates/wasm-parquet-engine` owns footer bootstrap, metadata decode, and bounded scan primitives.
- `crates/wasm-delta-snapshot` reconstructs active files from `_delta_log`, including classic checkpoints, multipart checkpoints, V2 checkpoints, JSON replay, and sidecars.
- `crates/wasm-query-runtime` consumes snapshot and Parquet crates, performs query-shape analysis, pruning, narrow execution-plan lowering, narrow execution, and structured fallback.
- `crates/browser-sdk` exposes the worker-envelope and Arrow IPC boundary.
- `crates/query-router` preserves structured fallback reasons when rerouting browser failures to native.
- `crates/delta-control-plane` resolves trusted native snapshots, emits browser HTTP descriptors, and preserves partition column typing.
- CI already checks `wasm32-unknown-unknown` compilation for the browser crates and enforces provisional size budgets.

### What remains open

- `wasm-delta-snapshot` still returns empty `partition_column_types`, so browser-local snapshot reconstruction does not yet match trusted native metadata.
- Advanced Delta features are not yet classified into supported, native-only, unsupported, or experimental with automated enforcement in the browser path.
- Dedicated wasm execution suites for `browser-sdk`, `wasm-parquet-engine`, and `wasm-delta-snapshot` are still missing.
- Startup and memory budgets are not automated; current size tracking is still based on `.rlib` proxy artifacts.
- Query metrics do not yet cover the full browser release-gate set such as footer-read counts and stable fallback telemetry across the embedding boundary.
- There is still no in-repo `services/query-api`, so signed URL issuance, proxy mode, audit logging, and production CORS validation remain external.

### Key documented limitations and future-work items driving this PI

- `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md` still lists advanced Delta feature classification as open.
- The same checklist still marks dedicated wasm execution suites, browser startup budgets, browser memory budgets, and several observability items as future work.
- `README.md` explicitly states that the current browser runtime is still a narrow execution-plan interpreter, not broad browser SQL or DataFusion execution.
- `docs/program/wasm-delta-gcs-program.md` still treats browser multi-partition execution as future work.
- This repository has no `apps/` or `services/` tree, so service-backed browser access work is not a realistic critical-path target for this PI.

## 3. Scope Decision

### In scope for this PI

- Define and enforce the browser Delta compatibility policy in code, tests, and release docs.
- Bring `wasm-delta-snapshot` to metadata parity with `delta-control-plane` for partition typing and advanced-feature detection.
- Propagate capability and fallback outcomes through `wasm-query-runtime`, `query-router`, and `browser-sdk`.
- Expand conformance coverage so supported, native-only, and hard-fail paths are all tested intentionally.
- Add dedicated wasm execution coverage and move browser artifact tracking closer to a real shipped-engine wasm target.
- Improve browser-side observability and fallback reporting without changing the trust boundary.

### Out of scope for this PI

- Broad browser SQL or full DataFusion-in-browser execution.
- Browser write-path support of any kind.
- Direct browser cloud credentials.
- Multithreaded or multi-partition browser execution.
- Service-backed signed URL issuance, proxy reads, audit logging, request correlation, or production CORS validation.
- New repository splits, major crate-topology changes, or collapsing snapshot / scan / runtime boundaries.
- UI work, sample consoles, or product surfaces that require an `apps/` tree that does not exist here.

### Work that remains native-only, deferred, or feature-flagged

Native-only:

- `delta-control-plane` policy enforcement
- signed URL or proxy issuance
- audit logging
- direct cloud-provider integration
- deletion-vector execution
- multi-partition execution

Deferred:

- broad DataFusion/browser SQL coverage
- real service integration through `services/query-api`
- production CORS and TTL validation
- cache-hit-ratio gates that depend on a real integrated browser extent cache
- browser memory dashboards beyond a narrow harness baseline

Experimental / feature-flagged:

- any browser-side probe for deletion-vector or other advanced-feature handling beyond detection and fallback must stay behind an explicit feature flag and must not change the default capability classification in this PI

## 4. Sprint Plan

### Sprint 1

**Sprint goal:** freeze the compatibility contract and remove ambiguity about what the browser engine is allowed to claim.

**Major stories / workstreams**

- Define a browser Delta compatibility matrix and publish it in repo documentation.
- Decide whether snapshot capability metadata travels on shared snapshot descriptors or remains runtime-local, then implement the chosen contract in `query-contract`.
- Align native and control-plane capability naming, fallback reasons, and release-gate terminology so later browser work uses one vocabulary.
- Decide the measurement approach for a real browser-engine wasm artifact: minimal harness artifact vs continued proxy metric.

**Expected deliverables**

- approved browser Delta compatibility matrix document
- updated shared contract for capability and fallback reporting
- updated release checklist entries tied to the new compatibility matrix
- implementation note for the wasm artifact / startup measurement approach

**Dependencies**

- `query-contract`
- `delta-control-plane`
- `native-query-runtime`
- `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`

**Risks**

- snapshot-level capability shape may cause cross-crate contract churn
- over-designing the contract could slow the PI before browser gaps are closed

**Verification gates**

- contract serialization tests updated and green
- control-plane descriptor tests green after contract changes
- release-gate doc updated to reference concrete capability states instead of generic future work

### Sprint 2

**Sprint goal:** make browser-local Delta snapshot resolution match the native control-plane metadata envelope.

**Major stories / workstreams**

- Extend `wasm-delta-snapshot` to reconstruct and emit `partition_column_types`.
- Parse enough metadata / protocol state during snapshot reconstruction to classify advanced table features instead of silently ignoring them.
- Add fixture coverage for historical snapshots, partition typing, and advanced-feature detection.
- Cross-check browser snapshot output against `delta-control-plane` on the same fixtures.

**Expected deliverables**

- `wasm-delta-snapshot` emits partition typing parity with `delta-control-plane`
- browser snapshot output includes explicit capability classification or equivalent structured feature findings
- expanded parity tests for latest and historical snapshots

**Dependencies**

- Sprint 1 capability contract
- existing local fixture builders in `crates/delta-control-plane/tests/support`

**Risks**

- checkpoint parsing currently focuses on add/remove/sidecar actions, so protocol / metadata parity work could reveal hidden parser gaps
- generating local fixtures for advanced Delta features may require careful native-side fixture shaping

**Verification gates**

- `cargo test -p wasm-delta-snapshot --locked`
- `cargo test -p delta-control-plane --locked`
- new cross-checks prove equality on snapshot version, active files, partition typing, and feature classification
- `cargo check -p wasm-delta-snapshot --target wasm32-unknown-unknown --locked`

### Sprint 3

**Sprint goal:** enforce the browser capability envelope before execution and preserve structured outcomes end-to-end.

**Major stories / workstreams**

- Update `wasm-query-runtime` to consume the snapshot capability / feature findings and reject or reroute native-only cases before browser scan planning.
- Tighten `query-router` so reroutes preserve capability-gated reasons consistently.
- Update `browser-sdk` so worker envelopes preserve the final capability and fallback state seen by the embedding host.
- Add focused tests for tables and queries that should route to native instead of reaching browser execution.

**Expected deliverables**

- early native-only gating for unsupported browser table features
- consistent structured fallback reasons across runtime, router, and SDK
- no silent downgrade from “unsupported in browser” to “best effort”

**Dependencies**

- Sprint 2 snapshot metadata parity
- existing browser execution / router / SDK test suites

**Risks**

- hard-fail vs native-reroute semantics may be inconsistent unless decided early
- regressions in the narrow supported browser SQL path if gating is wired too late in planning

**Verification gates**

- `cargo test -p wasm-query-runtime --locked`
- `cargo test -p query-router --locked`
- `cargo test -p browser-sdk --locked`
- cross-crate tests prove native-only features do not reach browser execution
- `cargo check -p wasm-query-runtime -p browser-sdk --target wasm32-unknown-unknown --locked`

### Sprint 4

**Sprint goal:** convert the supported browser envelope into a tested conformance target, not just a unit-tested implementation.

**Major stories / workstreams**

- Expand `tests/conformance` to cover supported-browser, native-only, and hard-fail scenarios explicitly.
- Add dedicated wasm execution suites for `wasm-delta-snapshot`, `wasm-parquet-engine`, and `browser-sdk`.
- Add or finalize the minimal browser-engine wasm harness needed for real artifact execution coverage.
- Keep the cross-crate seam tests anchored around `delta-control-plane -> wasm-query-runtime -> browser-sdk`, not just isolated crate tests.

**Expected deliverables**

- conformance fixture matrix for checkpoint variants, time travel, typed partitions, and native-only features
- dedicated wasm execution jobs for the currently missing browser crates
- minimal harness that can produce and execute a real browser-engine wasm artifact in CI

**Dependencies**

- Sprint 3 gating behavior
- chosen wasm harness approach from Sprint 1

**Risks**

- wasm test infrastructure can be flaky if the harness is too browser-specific
- feature fixtures may expose native/browser differences that require contract adjustments rather than simple test updates

**Verification gates**

- CI runs new wasm suites in addition to compile-only checks
- conformance runs show supported cases pass and native-only cases reroute or fail deterministically
- artifact build for the browser engine succeeds in CI and is archived or reported

### Sprint 5

**Sprint goal:** close the PI with measurable release evidence and explicit residual gaps.

**Major stories / workstreams**

- Expand browser metrics and response metadata to cover the release-gate items that are realistically measurable in-repo now.
- Replace or augment the `.rlib` proxy with the harness artifact size budget and add a cold-start baseline.
- Update release docs, perf docs, security notes, and operator-facing fallback documentation.
- Run the full browser-engine verification set and record the remaining deferred work for the next PI.

**Expected deliverables**

- browser-engine size budget tied to the harness artifact
- cold-start baseline captured in CI or reproducible local perf commands
- richer structured metrics and fallback reporting for supported browser runs
- updated release checklist with PI-complete vs still-blocked items called out honestly

**Dependencies**

- Sprint 4 harness and wasm coverage
- stable structured capability / fallback behavior from Sprint 3

**Risks**

- cold-start measurement can become noisy if the harness is not deterministic
- attempting to force memory or cache metrics without a real integrated substrate would create misleading gates

**Verification gates**

- full targeted package suite green
- wasm execution suites green
- artifact size budget enforced in CI
- release checklist updated and reviewed
- perf and security docs match the actual tested state of the repository

## 5. Cross-Cutting Work

### Testing and conformance

- Every browser-supported capability must have a positive test.
- Every native-only capability must have a deterministic reroute test.
- Every unsupported / hard-fail condition must have a terminal failure test.
- Keep `crates/delta-control-plane/tests/browser_snapshot_preflight.rs` as the primary cross-crate seam suite; add focused crate tests only where the seam test would be too indirect.
- Add fixture coverage for latest snapshot, historical snapshot, classic checkpoint, multipart checkpoint, V2 checkpoint, and sidecar resolution paths.

### Performance and bundle-size tracking

- Stop treating the `.rlib` proxy as the only browser artifact metric.
- Produce a minimal real browser-engine wasm artifact for size tracking.
- Add a cold-start baseline for module init plus one representative metadata-bootstrap flow.
- Do not promise cache-hit-ratio gates in this PI unless a real integrated cache path lands; keep that item deferred rather than fabricating a metric.

### Security hardening

- Preserve the current native trust boundary.
- Add explicit tests that browser-facing descriptors and transport errors do not leak query strings, fragments, credentials, or raw signed URL secrets.
- Add dependency and packaging checks so browser crates cannot pick up signing or service-account code accidentally.
- Keep sidecar path hardening and table-root escape rejection as permanent release gates.
- Record external service security dependencies separately instead of pretending they are solved in-repo.

### Documentation and release gates

- Update `README.md` so browser support claims match the tested compatibility matrix exactly.
- Update `docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md` so checked items map to automated evidence.
- Expand `tests/perf/README.md` and `tests/security/README.md` with the new harness commands and in-repo vs external-service boundaries.
- Add a concise “residual gaps after this PI” section so the next PI starts from an honest state.

### Observability and fallback behavior

- Preserve one structured fallback reason end-to-end from runtime to router to SDK response.
- Extend browser metrics only where the repository can measure them reliably now: bytes fetched, files touched, files skipped, fallback reason, bootstrap duration, and footer-read count.
- Distinguish clearly between native reroute, browser hard fail, and invalid request.
- Keep silent downgrade out of scope; unsupported cases must route or fail explicitly.

## 6. Delta Protocol Reader Planning

### Delta reader features in scope for this PI

- latest-snapshot reconstruction
- explicit `snapshot_version` reconstruction when the target version is still available
- `_last_checkpoint` hint handling
- classic single-part checkpoints
- classic multipart checkpoints
- V2 checkpoint discovery
- sidecar resolution for V2 checkpoints
- JSON replay after the selected checkpoint
- active-file reconstruction
- partition value reconstruction
- partition column type reconstruction and propagation
- advanced-feature detection sufficient to classify browser vs native-only handling

### Supported in browser by the end of this PI

- Latest snapshot reads for tables within the supported browser envelope.
- Historical snapshot reads through explicit `snapshot_version` when the requested version exists and the resolved snapshot stays inside the supported browser envelope.
- `_last_checkpoint` handling.
- Classic single-part and multipart checkpoint selection.
- V2 checkpoints with parquet or JSON checkpoint manifests.
- Checkpoint sidecars, including the existing path-hardening rules.
- JSON replay after checkpoint selection.
- Active-file reconstruction with path, size, and partition values.
- Partition column typing for the currently supported type set: `string`, `int64`, `boolean`, plus explicit `unsupported` markers for everything else.

### Native-only by the end of this PI

- Deletion vectors.
- Change data feed.
- Column mapping tables.
- Timestamp NTZ tables until browser execution semantics are proven in the exact runtime.
- Multi-partition execution.

Policy for native-only features:

- the reader must detect them
- the browser runtime must not attempt best-effort execution
- the router and SDK must preserve the capability-gated fallback reason

### Unsupported / hard fail by the end of this PI

- malformed `_last_checkpoint`
- incomplete multipart checkpoints
- missing sidecars or checkpoint parts
- sidecar paths that escape the table root or violate the local-file security rules
- negative snapshot versions
- snapshot versions newer than table head
- checkpoint variants or protocol shapes outside the explicitly tested corpus
- corrupted metadata / protocol actions that cannot be classified safely

### Experimental / feature-flagged in this PI

- No browser Delta reader feature is committed for experimental enablement by default in this PI.
- If a deletion-vector or other advanced-feature probe is explored, it must stay behind an explicit feature flag, default off, and cannot change the default classification above.

### Explicit callouts

Deletion vectors:

- classify as native-only
- detection and structured fallback are in scope
- browser-side application of deletion vectors is out of scope

Time travel:

- support explicit `snapshot_version` in browser only when the requested snapshot exists and remains inside the supported browser envelope
- require parity tests for latest vs historical browser/native resolution

Partition typing:

- `wasm-delta-snapshot` must emit the same partition typing contract as `delta-control-plane`
- browser execution continues to fail closed when a required partition column is marked `unsupported`

Checkpoint variants:

- classic single-part: supported
- classic multipart: supported
- V2 checkpoint manifests: supported
- sidecars: supported
- unknown or untested checkpoint layouts: hard fail

Other advanced protocol items:

- anything not explicitly named and tested in the compatibility matrix is treated as unsupported in browser by default
- do not broaden support through inference or “probably works” assumptions

## 7. Exit Criteria

### What must be true by the end of the PI

- The browser Delta compatibility matrix exists, is reviewed, and matches implementation behavior.
- `wasm-delta-snapshot` and `delta-control-plane` agree on snapshot version, active files, partition column types, and advanced-feature classification for the supported local fixture corpus.
- Browser-native routing is deterministic for native-only features, and those reasons survive through `query-router` and `browser-sdk`.
- Dedicated wasm execution coverage exists for the currently missing browser crates.
- CI enforces a browser-engine artifact size gate and records a cold-start baseline.
- Release docs reflect the actual tested state of the browser engine and clearly separate in-repo completion from external service blockers.

### Evidence that proves completion

- green unit and integration suites for `wasm-delta-snapshot`, `wasm-query-runtime`, `query-router`, `browser-sdk`, and `delta-control-plane`
- green wasm execution suites for the browser-engine crates
- updated conformance fixtures and passing supported/native-only/hard-fail coverage
- automated CI report for the browser-engine wasm artifact size and cold-start baseline
- updated release checklist and supporting perf/security docs

### Required CI, test, and review gates

- `cargo test -p wasm-delta-snapshot --locked`
- `cargo test -p wasm-parquet-engine --locked`
- `cargo test -p wasm-query-runtime --locked`
- `cargo test -p delta-control-plane --locked`
- `cargo test -p query-router --locked`
- `cargo test -p browser-sdk --locked`
- `cargo check -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p wasm-query-runtime -p browser-sdk --target wasm32-unknown-unknown --locked`
- dedicated wasm execution jobs for the browser-engine crates
- review sign-off from runtime / engine and storage platform leads on the compatibility matrix and fallback policy

## 8. Risks and Sequencing

### Critical path

1. Freeze the capability contract and compatibility matrix in Sprint 1.
2. Land browser-local snapshot metadata parity in Sprint 2.
3. Wire deterministic browser gating and routing in Sprint 3.
4. Add wasm and conformance evidence in Sprint 4.
5. Close release gates and artifact tracking in Sprint 5.

### Biggest technical risks

- `wasm-delta-snapshot` may need to parse more Delta metadata than the current add/remove/sidecar-only shape to classify advanced features correctly.
- Local fixture generation for deletion vectors, column mapping, or timestamp-NTZ cases may be harder than expected.
- A real wasm artifact harness can easily drift into app-layer work if not kept minimal.
- Capability reporting can cause contract churn across `query-contract`, `delta-control-plane`, `wasm-query-runtime`, `query-router`, and `browser-sdk`.
- Browser startup measurement may be noisy unless the harness and CI environment are tightly controlled.

### Decisions that must be made early

- whether snapshot capability metadata is part of the shared descriptor contract
- whether native-only browser cases should always reroute when a router exists and hard fail only in pure browser-local mode
- what artifact counts as the enforced browser-engine size budget target
- which advanced-feature fixtures are mandatory for PI exit vs documented follow-on

## 9. Deliverable

This PI plan is written to:

- `docs/plans/2026-03-30-browser-engine-next-pi-plan.md`
