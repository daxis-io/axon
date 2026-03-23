# Security Tests

Current Sprint 7 status:

- Per-table allow/deny enforcement is covered in `crates/delta-control-plane/tests`.
- Canonical table policy-key coverage lives in `crates/delta-runtime-support` unit tests.

This directory still does not contain service-level EPIC-03 security checks because `services/query-api` is not in this repository. Secret-leakage tests, signed URL behavior, audit logging checks, and production-shape origin/CORS validation remain blocked on that external service layer.
