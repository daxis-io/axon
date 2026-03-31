# Browser WASM + Delta on GCS Launch Checklist

- Date: 2026-03-31
- Applies to milestone: `M4`

## Documentation

- [x] Browser compatibility is documented in `README.md` and backed by CI checks for `wasm32-unknown-unknown` coverage across the browser crates plus the internal worker artifact.
- [x] Delta compatibility is documented in `README.md` and the browser engine strategy docs, including the `wasm-delta-snapshot` and `wasm-parquet-engine` split.
- [x] Security reporting is documented in `SECURITY.md` and `tests/security/README.md`.

## Release Gates

- [x] `wasm32-unknown-unknown` compile coverage includes `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `browser-sdk`, and `browser-engine-worker`.
- [x] Host tests run for `wasm-parquet-engine`, `wasm-delta-snapshot`, `browser-sdk`, and `browser-engine-worker`.
- [x] Dedicated `wasm32-unknown-unknown` smoke suites run in CI for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- [x] Browser release-artifact size reporting is enforced in CI on the real `browser-engine-worker.wasm` artifact.
- [x] Host-proxy worker startup baseline is reported in CI from the worker baseline tests, and wasm smoke validates the same artifact-report path on the browser target.
- [x] Host-proxy worker memory baseline is reported in CI from the worker baseline tests; blocking thresholds remain deferred.

## Security

- [x] No cloud secrets or service-account markers are detected in the in-repo browser bundle artifact.
- [x] Browser dependency and bundle guardrails reject signing and service-account dependency classes in CI, with regression coverage for `cargo tree`-shaped dependency lines.
- [ ] Signed URL TTL policy is approved by security engineering.
- [ ] Signed URLs are object-scoped.
- [ ] Audit logging correlates read access to user, session, and request id.
- [ ] CORS is validated for the exact production XML endpoint shape.

## Correctness

- [x] Native runtime exists and is production-callable.
- [x] Every supported browser query has a native parity test.
- [x] Advanced Delta features are classified as supported, native-only, unsupported, or hard-fail in the browser engine plan and README, including unknown protocol features as terminal snapshot-resolution failures.
- [x] Unsupported cases fail clearly or route deterministically to native.
- [x] Range-read correctness is proven for footer, bounded, offset, and suffix reads.
- [x] Failure-path coverage exists for `401`, `403`, `404`, and `416`.

## Runtime Constraints

- [x] Browser runtime ships single-partition by default.
- [x] Browser bundle size is tracked in CI on the real worker artifact.
- [x] Browser packages do not depend on signing or service-account code.
- [x] Hosted UDF runtime remains separate from browser runtime dependencies.

## Observability

- [x] Metrics include bytes fetched, files touched, files skipped, footer fetch count, snapshot bootstrap duration, fallback reason, and access mode.
- [ ] Cache-hit ratio remains deferred until a browser cache layer exists in-repo.
- [x] Browser-to-native fallbacks emit structured reasons.
- [ ] Dashboards exist for native and browser regression trends.

## Release Hygiene

- [ ] All private fork patches are tracked with owner and upstream disposition.
- [ ] Upgrade rehearsal CI is green on the candidate dependency set.
- [ ] The oncall playbook covers fallback failure and control-plane outage.
