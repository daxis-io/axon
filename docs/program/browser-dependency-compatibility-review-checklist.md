# Browser Dependency Compatibility Review Checklist

- Date: 2026-03-31
- Scope: repo-owned review for dependency upgrades touching the browser lakehouse slice

Use this checklist after `.github/workflows/upgrade-rehearsal.yml` runs on a candidate dependency set.

## Inputs

- `Cargo.lock` diff artifact from the rehearsal run: `upgrade-rehearsal-cargo-lock-diff`
- local `git diff -- Cargo.lock` output when reproducing the rehearsal manually
- worker artifact output from `bash tests/perf/report_browser_worker_artifact.sh`
- browser dependency guardrail output from `bash tests/security/verify_browser_dependency_guardrails.sh`
- wasm smoke output for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`

## Review Checklist

- Confirm no new browser-worker dependency matches the denylisted classes enforced by `tests/security/verify_browser_dependency_guardrails.sh`.
- Confirm no new dependency introduces signing, service-account, cloud-credential, or generic object-store client code into browser-target packages.
- Review `deltalake`, Arrow, Parquet, `reqwest`, `rustls`, `wasm-bindgen`, and wasm-test related lockfile deltas for contract or target-support changes.
- Rerun the canonical handoff example tests if any contract type or serialization shape changed.
- Confirm unknown Delta protocol features still hard fail in `delta-control-plane` and `wasm-delta-snapshot`.
- Confirm native-only browser capabilities still reroute before browser I/O in `wasm-query-runtime`.
- Confirm the worker artifact size budget still passes and that startup/memory baselines remain explainable if they move.
- Confirm no README, release-gate, or runbook claim became stale relative to the new dependency behavior.

## Required Follow-Up If The Review Fails

- Update [`docs/program/upstream-patch-inventory.md`](./upstream-patch-inventory.md) if a temporary downstream patch or vendored change becomes necessary.
- Record any new browser/runtime compatibility caveat in [`docs/program/browser-lakehouse-release-handoff.md`](./browser-lakehouse-release-handoff.md).
- Update [`docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`](../release-gates/browser-wasm-delta-gcs-launch-checklist.md) if the dependency change reopens a release gate.
