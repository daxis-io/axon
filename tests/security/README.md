# Security Tests

Report security issues through [`SECURITY.md`](../../SECURITY.md). Do not use public issues for vulnerability reports, browser secret leakage, or access-control bypasses.

Current in-repo security coverage:

- Per-table allow/deny enforcement is covered in `crates/delta-control-plane/tests`.
- Canonical table policy-key coverage lives in `crates/delta-runtime-support` unit tests.
- Browser-facing object URL and fallback policy coverage lives in the browser/runtime tests and query-contract tests.
- `tests/security/verify_browser_dependency_guardrails.sh` denies signing, cloud-credential, and service-account dependency classes from the browser worker dependency tree and inspects the built worker artifact for secret-like markers.
- `tests/security/verify_browser_dependency_guardrails_test.sh` regression-checks the denylist parser against `cargo tree`-shaped dependency lines.

Useful local commands:

- `bash tests/security/verify_browser_dependency_guardrails.sh`
- `bash tests/security/verify_browser_dependency_guardrails_test.sh`
- `cargo test -p delta-control-plane --locked`
- `cargo test -p wasm-query-runtime --locked`

This directory still does not contain service-level EPIC-03 security checks because `services/query-api` is not in this repository. Secret-leakage tests, signed URL behavior, audit logging checks, and production-shape origin/CORS validation remain blocked on that external service layer.
