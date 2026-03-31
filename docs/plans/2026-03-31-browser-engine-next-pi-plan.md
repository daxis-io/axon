# Browser Lakehouse Engine Next PI Plan

- Date: 2026-03-31
- PI length: 5 sprints
- Recommendation: compatibility plus launch hardening

## PI Summary

### PI objective

Make the current browser lakehouse engine explicit, measurable, and releaseable without broadening the SQL envelope or changing the trust boundary.

### Why this PI matters now

The prior implementation plan established the browser-safe architectural split and shipped the core crates. The remaining risk is not missing architecture. It is missing execution clarity:

- the browser compatibility contract is still too implicit
- advanced Delta behavior needs stronger conformance and hard-fail policy
- browser release evidence still needs a real worker artifact, startup reporting, and bundle guardrails
- docs and release gates need to match the actual repository evidence instead of provisional language

### End-of-PI outcomes

- one browser compatibility contract is carried through control-plane descriptors, browser snapshot reconstruction, runtime gating, router fallback, and SDK envelopes
- native-only browser limitations reroute deterministically before browser I/O
- unsupported Delta protocol conditions hard fail during snapshot resolution instead of leaking into browser execution
- a real `browser-engine-worker.wasm` artifact is built in CI with a blocking size budget
- browser startup and footprint baselines are published in CI

## Starting Point

### Already complete

- `wasm-delta-snapshot` reconstructs supported latest and historical snapshots, checkpoints, sidecars, replay state, active files, and typed partitions
- `wasm-parquet-engine` owns browser footer bootstrap and metadata decode
- `wasm-query-runtime` owns browser planning, pruning, narrow execution, and structured fallback
- `browser-sdk` owns the worker/host envelope contract
- `delta-control-plane` remains the trusted native snapshot resolver and browser URL attachment seam
- wasm compile checks, host tests, and targeted smoke coverage already exist

### Open items driving this PI

- compatibility semantics must be explicit at the snapshot and runtime boundaries
- advanced Delta failure modes need broader conformance coverage
- the repository still needs a real browser worker artifact instead of an `.rlib` size proxy
- release docs and checklists still need to reflect real CI evidence
- browser guardrails need dependency and artifact inspection, not just contract tests

### Key limitations carried into this PI

- native control-plane remains trusted and browser access issuance stays out of repo
- browser execution remains single-partition and narrow-SQL only
- browser writes, commit logic, direct cloud credentials, and service-backed signed URL logic remain out of scope

## Scope Decision

### In scope

- additive browser compatibility reporting on snapshot descriptors
- deterministic browser gating for `native_only` versus terminal unsupported conditions
- explicit hard-fail handling for unknown Delta protocol features during snapshot resolution
- advanced-feature fixture coverage for change data feed, column mapping, deletion vectors, timestamp NTZ, malformed checkpoint state, and unknown protocol features
- internal `browser-engine-worker` artifact for real wasm size tracking plus startup and footprint reporting
- bundle dependency guardrails and browser artifact inspection in CI
- release checklist, perf notes, security notes, and README updates aligned to actual evidence

### Out of scope

- broad browser SQL expansion, joins, wildcard projections, `DISTINCT`, or `HAVING`
- browser writes or browser-side Delta commit support
- signed URL issuance, TTL enforcement, audit logging, request correlation, or production CORS validation
- browser cache hit-rate budgets
- hosted UDF runtime convergence with the browser runtime

### Deferred or native-only

- control-plane policy and browser-safe URL issuance remain native-only
- active deletion-vector execution remains native-only
- change data feed remains native-only
- column mapping remains native-only
- timestamp NTZ remains native-only

## Sprint Plan

### Sprint 1

- Goal: freeze one browser compatibility vocabulary
- Work:
  - add explicit `browser_compatibility` to snapshot descriptors
  - keep `required_capabilities` additive during migration
  - add browser telemetry fields for footer reads, snapshot bootstrap duration, and access mode
  - define reroute versus hard-fail policy by capability state
- Deliverables:
  - contract updates in `query-contract`
  - browser/runtime/control-plane propagation
  - updated serialization coverage
- Verification:
  - `cargo test -p query-contract --locked`

### Sprint 2

- Goal: make advanced-feature classification and hard-fail behavior explicit
- Work:
  - align `delta-control-plane` and `wasm-delta-snapshot` on browser compatibility output
  - classify change data feed, column mapping, deletion vectors, and timestamp NTZ as native-only
  - hard fail unknown protocol features during snapshot resolution
  - extend shared fixture support to mutate valid tables into advanced or unsafe protocol shapes
- Deliverables:
  - conformance coverage across trusted and browser-safe readers
  - shared unknown-protocol hard-fail policy
- Verification:
  - `cargo test -p delta-control-plane -p wasm-delta-snapshot --locked`

### Sprint 3

- Goal: enforce compatibility decisions before browser bootstrap
- Work:
  - gate browser runtime execution on snapshot compatibility before footer reads
  - keep `native_only` paths as structured fallback
  - keep terminal unsupported states as non-reroutable browser errors
  - preserve fallback reasons across runtime and SDK boundaries
- Deliverables:
  - deterministic pre-I/O browser gating
  - additive browser telemetry in query metrics
- Verification:
  - `cargo test -p wasm-query-runtime -p browser-sdk --locked`

### Sprint 4

- Goal: replace proxy bundle evidence with a real worker artifact
- Work:
  - create `crates/browser-engine-worker`
  - build the worker to a real `browser_engine_worker.wasm`
  - publish cold-start and footprint baselines from the worker baseline tests
  - enforce a blocking wasm size budget on the worker artifact
- Deliverables:
  - internal worker artifact
  - real wasm size gate
  - CI startup and memory reports
- Verification:
  - `cargo test -p browser-engine-worker --locked`
  - `cargo test -p browser-engine-worker --target wasm32-unknown-unknown --locked --test wasm_smoke -- --nocapture`
  - `bash tests/perf/report_browser_worker_artifact.sh`

### Sprint 5

- Goal: close the PI with release evidence and security guardrails
- Work:
  - add dependency-tree and artifact inspection for browser packages
  - update README, perf docs, security docs, and launch checklist
  - record external blockers explicitly instead of implying in-repo completion
- Deliverables:
  - CI browser dependency guardrails
  - updated release checklist and supporting docs
- Verification:
  - `bash tests/security/verify_browser_dependency_guardrails.sh`
  - targeted host and wasm browser suite green

## Cross-Cutting Work

### Testing and conformance

- every supported browser path keeps positive parity coverage against the native runtime
- every native-only feature keeps deterministic fallback coverage
- every malformed or unknown protocol state keeps terminal failure coverage

### Performance and bundle size

- the blocking budget moves to the real `browser-engine-worker.wasm` artifact
- startup and memory reporting are CI-published but memory remains report-only this PI

### Security hardening

- browser crates are checked for denylisted signing, cloud-credential, and service-account dependency classes
- the built worker artifact is inspected for secret-like markers
- service-level controls remain explicitly out of repo

### Documentation and release gates

- release docs must point at actual CI commands and artifacts
- unresolved external blockers remain unchecked in the launch checklist

### Observability and fallback

- browser metrics include bytes fetched, files touched, files skipped, footer reads, snapshot bootstrap duration, and access mode
- fallback reasons remain structured through runtime and SDK boundaries

## Delta Protocol Reader Planning

### Supported in browser

- latest snapshot reads
- explicit `snapshot_version` reads when the snapshot exists and resolves inside the supported browser envelope
- `_last_checkpoint`
- classic and multipart checkpoints
- V2 checkpoints and sidecars allowed by current path hardening
- JSON replay after checkpoint
- active-file reconstruction
- partition typing for `string`, `int64`, and `boolean`

### Native-only

- active deletion-vector execution
- change data feed
- column mapping
- timestamp NTZ
- browser multi-partition execution

### Unsupported or hard fail

- malformed `_last_checkpoint`
- incomplete checkpoint sets
- missing checkpoint parts or sidecars
- sidecar escape attempts and unsafe symlink paths
- negative or unavailable snapshot versions
- unknown protocol features not classified in the compatibility matrix
- corrupted metadata or protocol actions that cannot be classified safely

### Experimental or feature-flagged

- none enabled by default in this PI

### Explicit callouts

- protocol-only deletion-vector declarations are covered separately from active deletion-vector use
- time travel is supported only through explicit `snapshot_version`
- unsupported partition typing still fails closed to native fallback before browser execution

## Exit Criteria

### Must be true

- control-plane and browser-safe snapshot reconstruction agree on supported snapshot descriptors
- unknown protocol features fail during snapshot resolution
- browser runtime rejects native-only features before browser metadata bootstrap
- browser telemetry crosses the SDK boundary
- CI builds and budgets the real worker artifact
- docs and launch checklist reflect actual in-repo evidence

### Evidence

- host suites green for `delta-control-plane`, `wasm-delta-snapshot`, `wasm-query-runtime`, `browser-sdk`, `browser-engine-worker`, and `native-query-runtime`
- wasm checks green for the browser crates plus `browser-engine-worker`
- worker baseline tests print startup and footprint reports
- CI enforces the worker wasm size budget
- CI runs browser dependency and artifact guardrails

### Required gates

- code review on compatibility and routing semantics
- security review of the dependency and artifact guardrails
- release-doc review to confirm external blockers remain explicit

## Risks and Sequencing

### Critical path

- compatibility contract propagation
- advanced Delta hard-fail coverage
- real worker artifact and CI size gate
- release docs aligned to evidence

### Largest technical risks

- semantic drift between trusted and browser-safe snapshot readers
- unsafe protocol states being downgraded into fallback instead of terminal failure
- worker artifact size regressions without a real bundle target
- overclaiming service-level security guarantees that the repository still cannot prove

### Decisions that must be made early

- `native_only` always reroutes when a native path exists
- unknown protocol features hard fail during snapshot resolution
- worker artifact size budget is enforced on `browser-engine-worker.wasm`
- memory stays report-only until a real browser memory policy exists
