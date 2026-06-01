# Browser Release Integration Runbook

- Date: 2026-03-31
- Scope: repo-owned troubleshooting for the shipped browser lakehouse seam

This runbook covers what's in the repository today. It doesn't cover production control-plane deployment, signed URL issuance, live dashboards, or oncall for an external service.

Browser V1 here is the narrow runtime plus streaming scan plus an in-memory session shell. The target is a DataFusion-powered Delta/Parquet browser engine, once the DataFusion table provider and scan integration gates pass. Delta snapshot reconstruction already lives in `crates/wasm-delta-snapshot`. Signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation are external blockers, not repo-owned regressions.

## 1. Session Shell Regression

Symptom:

- `open_table` / `sql` / `dispose` commands stop round-tripping
- repeated browser queries rebuild snapshot state when they shouldn't
- cache eviction or dispose no longer releases in-memory table state

Cause:

- the V1 session shell drifted away from the worker contract

Local commands:

```bash
cargo test -p wasm-query-session --locked
cargo test -p browser-sdk --locked
cargo test -p browser-engine-worker --locked
```

Look at:

- `crates/wasm-query-session/src/lib.rs`
- `crates/browser-sdk/src/lib.rs`
- `crates/browser-engine-worker/src/lib.rs`

The session layer is in-memory only. There's a narrow OPFS extent-cache backend below it; IndexedDB and session-level persistence are still deferred.

## 2. Capability-Gated Native Fallback

Symptom:

- browser execution returns `fallback_required`
- `fallback_reason` is `capability_gate`

Expected meaning:

- the browser path caught a native-only or experimental requirement before doing any browser I/O

Local commands:

```bash
cargo test -p wasm-query-runtime --locked
cargo test -p query-router --locked
cargo test -p browser-sdk --locked
```

Look at:

- `crates/wasm-query-runtime/tests/browser_runtime.rs`
- `crates/query-router/src/lib.rs`
- `crates/browser-sdk/tests/ipc.rs`

## 3. Terminal Unsupported Failure

Symptom:

- control-plane or browser snapshot reconstruction returns `unsupported_feature`
- no `fallback_reason` is present

Cause:

- the repo hit an unsafe or unknown feature and failed closed

Local commands:

```bash
cargo test -p delta-control-plane --locked
cargo test -p wasm-delta-snapshot --locked
```

Look at:

- unknown Delta protocol features have to fail during trusted snapshot resolution
- a descriptor-less failure is expected for unknown protocol features

## 4. Worker Artifact Regression

Symptom:

- size budget fails
- startup/memory baseline changes when it shouldn't

Local commands:

```bash
bash tests/perf/report_browser_worker_artifact.sh
cargo test -p browser-engine-worker --locked report_worker_artifact_baseline -- --exact --nocapture
cargo test -p browser-engine-worker --locked report_worker_memory_baseline -- --exact --nocapture
```

Look at:

- dependencies that snuck into the worker
- command or response envelope growth
- wasm-target package drift
- the worker artifact should claim `session_shell = true` and `browser_datafusion = false`

The CI size budget applies to the shipped browser worker artifact. For Daxis default-worker DataFusion size evidence, run `AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh` when the release environment has `wasm-bindgen`, `wasm-opt`, `brotli`, and `twiggy`.

## 5. Browser Dependency Guardrail Failure

Symptom:

- denylisted package or secret-like marker detected

Local commands:

```bash
bash tests/security/verify_browser_dependency_guardrails_test.sh
bash tests/security/verify_browser_dependency_guardrails.sh
```

Cause:

- a browser-target dependency crossed the trust boundary

Fix the dependency tree before you treat the build as releasable.

## 6. Transport Validation Or Cache-Mode Drift

Symptom:

- browser extent reads start failing with `object_store_protocol`
- stale cache bytes show up after an object replacement
- browser-local tests report `memory_only` vs `persistent`, or the inverse

Local commands:

```bash
cargo test -p wasm-http-object-store --locked
cargo check -p wasm-http-object-store --target wasm32-unknown-unknown --locked
cargo test -p wasm-http-object-store --target wasm32-unknown-unknown --locked --test wasm_smoke
```

Notes:

- validated HTTP extent reuse depends on browser-visible object identity headers like `ETag`
- check `bytes_fetched`, `bytes_reused`, and `validation_misses` first
- browser-local access uses the same extent seam but has to stay distinct from signed-URL or proxy-backed access
- the object-store seam has a narrow OPFS extent-cache backend; IndexedDB and session-level persistence are still deferred

## 7. Env-Gated Native GCS Smoke Failure

Symptom:

- CI native GCS smoke fails or skips when it shouldn't

Local commands:

```bash
cargo test -p native-query-runtime --locked
```

Notes:

- local deterministic tests still prove repo behavior
- GCS fixture provisioning, IAM, credentials, and CI variable population are external dependencies

## 8. Explicit External Boundary

Don't troubleshoot these as repo bugs unless new code lands:

- `services/query-api`
- signed URL issuance or TTL policy
- proxy-mode read issuance
- audit logging or request correlation
- production CORS/origin behavior
- live dashboards
- production control-plane outage runbooks or oncall
