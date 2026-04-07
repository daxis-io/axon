# Browser Release Integration Runbook

- Date: 2026-03-31
- Scope: repo-owned troubleshooting for the shipped browser lakehouse seam

This runbook covers only what exists in the repository today. It does not cover production control-plane deployment, signed URL issuance, live dashboard operation, or oncall procedures for an external service.

## 1. Capability-Gated Native Fallback

Symptom:

- browser execution returns `fallback_required`
- `fallback_reason` is `capability_gate`

What it means:

- the browser path correctly detected a native-only or experimental requirement before browser I/O

Local commands:

```bash
cargo test -p wasm-query-runtime --locked
cargo test -p query-router --locked
cargo test -p browser-sdk --locked
```

Focus areas:

- `crates/wasm-query-runtime/tests/browser_runtime.rs`
- `crates/query-router/src/lib.rs`
- `crates/browser-sdk/tests/ipc.rs`

## 2. Terminal Unsupported Failure

Symptom:

- control-plane or browser snapshot reconstruction returns `unsupported_feature`
- no `fallback_reason` is present

What it means:

- the repo hit an unsafe or unknown feature and failed closed

Local commands:

```bash
cargo test -p delta-control-plane --locked
cargo test -p wasm-delta-snapshot --locked
```

Focus areas:

- unknown Delta protocol features must fail during trusted snapshot resolution
- descriptor-less failure is expected for unknown protocol features

## 3. Worker Artifact Regression

Symptom:

- size budget fails
- startup/memory baseline changes unexpectedly

Local commands:

```bash
bash tests/perf/report_browser_worker_artifact.sh
cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture
cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture
```

Focus areas:

- unexpected dependency additions into the worker
- serialization envelope growth
- wasm-target package drift

## 4. Browser Dependency Guardrail Failure

Symptom:

- denylisted package or secret-like marker detected

Local commands:

```bash
bash tests/security/verify_browser_dependency_guardrails_test.sh
bash tests/security/verify_browser_dependency_guardrails.sh
```

Interpretation:

- browser-target dependencies crossed the current trust boundary
- fix the dependency tree before treating the build as releaseable

## 5. Transport Validation Or Cache-Mode Drift

Symptom:

- browser extent reads start failing with `object_store_protocol`
- stale cache bytes appear after an object replacement
- browser-local tests unexpectedly report `memory_only` versus `persistent`, or the inverse

Local commands:

```bash
cargo test -p wasm-http-object-store --locked
cargo check -p wasm-http-object-store --target wasm32-unknown-unknown --locked
cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Interpretation:

- validated HTTP extent reuse depends on browser-visible object identity headers such as `ETag`
- `bytes_fetched`, `bytes_reused`, and `validation_misses` are the transport-local counters to inspect first
- browser-local access should still use the same extent seam, but it must remain distinct from signed-URL or proxy-backed access
- Sprint 2 only ships persistent-cache hooks and reporting; an actual OPFS or IndexedDB backend is still deferred

## 6. Env-Gated Native GCS Smoke Failure

Symptom:

- CI native GCS smoke fails or skips unexpectedly

Local commands:

```bash
cargo test -p native-query-runtime --locked
```

Interpretation:

- local deterministic tests still prove repo behavior
- GCS fixture provisioning, IAM, credentials, and CI variable population are external dependencies

## 7. Explicit External Boundary

Do not troubleshoot these as repo bugs unless new code lands:

- `services/query-api`
- signed URL issuance or TTL policy
- audit logging or request correlation
- production CORS/origin behavior
- live dashboards
- production control-plane outage runbooks or oncall
