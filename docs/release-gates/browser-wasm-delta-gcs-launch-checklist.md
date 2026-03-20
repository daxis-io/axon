# Browser WASM + Delta on GCS Launch Checklist

- Date: 2026-03-20
- Applies to milestone: `M4`

## Security

- [ ] No cloud secrets reach the browser bundle.
- [ ] No service-account material reaches browser runtime payloads.
- [ ] Signed URL TTL policy is approved by security engineering.
- [ ] Signed URLs are object-scoped.
- [ ] Audit logging correlates read access to user, session, and request id.
- [ ] CORS is validated for the exact production XML endpoint shape.

## Correctness

- [ ] Native runtime exists and is production-callable.
- [ ] Every supported browser query has a native parity test.
- [ ] Advanced Delta features are classified as supported, native-only, unsupported, or experimental.
- [ ] Unsupported cases fail clearly or route deterministically to native.
- [ ] Range-read correctness is proven for footer, bounded, offset, and suffix reads.
- [ ] Failure-path coverage exists for `401`, `403`, `404`, and `416`.

## Runtime Constraints

- [ ] Browser runtime ships single-partition by default.
- [ ] Browser startup, bundle size, and memory budgets are tracked in CI.
- [ ] Browser packages do not depend on signing or service-account code.
- [ ] Hosted UDF runtime remains separate from browser runtime dependencies.

## Observability

- [ ] Metrics include bytes fetched, files touched, files skipped, footer fetch count, cache hit ratio, fallback reason, and access mode.
- [ ] Browser-to-native fallbacks emit structured reasons.
- [ ] Dashboards exist for native and browser regression trends.

## Release Hygiene

- [ ] All private fork patches are tracked with owner and upstream disposition.
- [ ] Upgrade rehearsal CI is green on the candidate dependency set.
- [ ] The oncall playbook covers fallback failure and control-plane outage.
