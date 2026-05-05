# Browser WASM + Delta on GCS Launch Checklist

- Date: 2026-04-07
- Applies to milestone: `M4`

## Documentation

- [x] Browser compatibility is documented in `README.md` and backed by CI checks for `wasm32-unknown-unknown` coverage across the browser crates plus the internal worker artifact.
- [x] Delta compatibility is documented in `README.md` and the browser engine strategy docs, including already repo-owned snapshot reconstruction in `wasm-delta-snapshot` plus the `wasm-parquet-engine` / `wasm-query-session` split.
- [x] Security reporting is documented in `SECURITY.md` and `tests/security/README.md`.
- [x] Canonical handoff examples live in `docs/program/browser-lakehouse-release-handoff.md` and `docs/program/browser-lakehouse-release-handoff-examples/`, including the session-backed worker command contract and the worker artifact report contract for runtime SKU, session capability, Arrow IPC transport, and artifact identity.
- [x] Repo-owned release evidence keeps shipped scope separate from external blockers: V1 is narrow runtime + streaming scan + in-memory session shell, while signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation remain outside repo-owned success claims.

## Release Gates

- [x] `wasm32-unknown-unknown` compile coverage includes `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, and `browser-engine-worker`.
- [x] Host tests run for `wasm-query-runtime`, `query-router`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `browser-sdk`, and `browser-engine-worker`.
- [x] Host tests run for `wasm-query-session` to verify repeated-query reuse, in-memory eviction, and dispose semantics.
- [x] Dedicated `wasm32-unknown-unknown` smoke suites run in CI for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- [x] Browser release-artifact size reporting is enforced in CI on the real `browser-engine-worker.wasm` artifact.
- [x] Browser DataFusion engine size reporting is optional and manual in CI while the engine is not yet the shipped browser query path, separate from the current worker budget.
- [x] Host-proxy worker startup baseline is reported in CI from the worker baseline tests, and wasm smoke validates the same artifact-report path on the browser target.
- [x] The worker artifact report names the runtime SKU, session-shell capability, Arrow IPC result transport, browser access mode, and shipped wasm artifact identity on both host and wasm paths.
- [x] Host-proxy worker memory baseline is reported in CI from the worker baseline tests; blocking thresholds remain deferred.

## Security

- [x] No cloud secrets or service-account markers are detected in the in-repo browser bundle artifact.
- [x] Browser dependency and bundle guardrails reject signing and service-account dependency classes in CI, with regression coverage for `cargo tree`-shaped dependency lines.
- [ ] Signed URL TTL policy is approved by security engineering. External blocker: no approval artifact exists in this repository.
- [ ] Signed URLs are object-scoped. External blocker: signed URL issuance is not implemented in this repository.
- [ ] Audit logging correlates read access to user, session, and request id. External blocker: no service or logging pipeline exists in this repository.
- [ ] CORS is validated for the exact production XML endpoint shape. External blocker: no production endpoint exists in this repository.

## Correctness

- [x] Native runtime exists and is production-callable.
- [x] Every supported browser query has a native parity test.
- [x] Advanced Delta features are classified as supported, native-only, unsupported, or hard-fail in the browser engine plan and README, including unknown protocol features as terminal snapshot-resolution failures.
- [x] Unsupported cases fail clearly or route deterministically to native.
- [x] Range-read correctness is proven for footer, bounded, offset, and suffix reads.
- [x] Failure-path coverage exists for `401`, `403`, `404`, and `416`.
- [x] Repeated browser queries can reuse in-memory session state through the worker-owned session shell.

## Runtime Constraints

- [x] Browser runtime ships single-partition by default.
- [x] Current browser V1 is documented as narrow runtime + streaming scan + in-memory session shell, while the target browser query engine is DataFusion-backed Delta/Parquet execution.
- [x] The shipped session shell is documented as in-memory only; persistent-cache hooks may exist below it, but OPFS / IndexedDB backends remain deferred.
- [x] Browser bundle size is tracked in CI on the real worker artifact.
- [x] Browser DataFusion engine budget reporting is tracked separately until DataFusion becomes the shipped browser query path; optional Brotli budget checks are not the current worker size gate.
- [x] Browser packages do not depend on signing or service-account code.
- [x] Hosted UDF runtime remains separate from browser runtime dependencies.

## Observability

- [x] Metrics include bytes fetched, files touched, files skipped, footer fetch count, snapshot bootstrap duration, fallback reason, and access mode.
- [x] Worker artifact reporting exposes runtime SKU, session capability, Arrow IPC result transport, and artifact identity for the shipped browser bundle.
- [ ] Cache-hit ratio remains deferred until a browser cache layer exists in-repo.
- [x] Browser-to-native fallbacks emit structured reasons.
- [ ] Dashboards exist for native and browser regression trends. External blocker: repo exports the metric contract, but no dashboard pipeline exists here.

## Release Hygiene

- [x] All private fork patches are tracked with owner and upstream disposition.
- [x] Upgrade rehearsal CI exists for the candidate dependency set.
- [ ] The oncall playbook covers fallback failure and control-plane outage. External blocker: the repository only contains a local integration runbook, not a service-aware production oncall playbook.

## References

- Repo-owned release evidence: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`
- External blocker register: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
