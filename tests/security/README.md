# Security Tests

Report security issues through [`SECURITY.md`](../../SECURITY.md). Do not use public issues for vulnerability reports, browser secret leakage, or access-control bypasses.

Current in-repo security coverage:

- Per-table allow/deny enforcement is covered in `crates/delta-control-plane/tests`.
- Canonical table policy-key coverage lives in `crates/delta-runtime-support` unit tests.
- Browser-facing object URL and fallback policy coverage lives in the browser/runtime tests and query-contract tests.
- Delta snapshot reconstruction is already repo-owned in `crates/wasm-delta-snapshot`.
- `crates/wasm-http-object-store/tests` verifies that cached extent reuse requires browser-visible object identity, fails closed when validation headers are unavailable, and keeps browser-local object access separate from signed-URL or proxy flows.
- `crates/wasm-query-session/tests` verifies that the worker-owned session shell stays in-memory only and releases cached runtime state on eviction or dispose.
- The current browser V1 remains narrow runtime + streaming scan + in-memory session shell; broad browser DataFusion and OPFS / IndexedDB browser-cache backends are still deferred.
- `tests/security/verify_browser_dependency_guardrails.sh` denies signing, cloud-credential, and service-account dependency classes from the browser worker dependency tree and inspects the built worker artifact for secret-like markers.
- `tests/security/verify_browser_dependency_guardrails_test.sh` regression-checks the denylist parser against `cargo tree`-shaped dependency lines.

Useful local commands:

- `bash tests/security/verify_browser_dependency_guardrails.sh`
- `bash tests/security/verify_browser_dependency_guardrails_test.sh`
- `cargo test -p wasm-http-object-store --locked`
- `cargo test -p delta-control-plane --locked`
- `cargo test -p wasm-query-runtime --locked`

Supporting docs:

- `docs/program/browser-release-integration-runbook.md` covers repo-owned troubleshooting for guardrail failures and fallback behavior.

This directory still does not contain service-level EPIC-03 security checks because `services/query-api` is not in this repository. Signed URL issuance, proxy-mode request issuance, audit logging, and production-shape CORS/origin validation remain external blockers outside repo-owned success claims.
Proxy-mode reads and persistent browser-cache backends are also not implemented in repo and must not be implied by the worker/session shell coverage above.
