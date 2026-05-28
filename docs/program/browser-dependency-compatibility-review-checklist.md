# Browser Dependency Compatibility Review Checklist

- Date: 2026-03-31
- Scope: repo-owned review for dependency upgrades that touch the browser lakehouse slice

Run this after `.github/workflows/upgrade-rehearsal.yml` finishes on a candidate dependency set. It's the list a reviewer walks through before signing off on the upgrade.

## Inputs

- `Cargo.lock` diff artifact from the rehearsal run: `upgrade-rehearsal-cargo-lock-diff`
- local `git diff -- Cargo.lock` output if you're reproducing the rehearsal by hand
- worker artifact output from `bash tests/perf/report_browser_worker_artifact.sh`
- browser dependency guardrail output from `bash tests/security/verify_browser_dependency_guardrails.sh`
- wasm smoke output for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`

## Review Checklist

- No new browser-worker dependency hits the denylisted classes enforced by `tests/security/verify_browser_dependency_guardrails.sh`.
- No new dependency drags signing, service-account, cloud-credential, or generic object-store client code into browser-target packages.
- Walk the `deltalake`, Arrow, Parquet, `reqwest`, `rustls`, `wasm-bindgen`, and wasm-test lockfile deltas for contract or target-support changes.
- If any contract type or serialization shape changed, rerun the canonical handoff example tests.
- Unknown Delta protocol features still hard fail in `delta-control-plane` and `wasm-delta-snapshot`.
- Native-only browser capabilities still reroute before any browser I/O in `wasm-query-runtime`.
- The worker artifact size budget still passes. If startup or memory baselines moved, you can explain why.
- No README, release-gate, or runbook claim is now stale against the new dependency behavior.

## Required Follow-Up If The Review Fails

- Update [`docs/program/upstream-patch-inventory.md`](./upstream-patch-inventory.md) if you have to land a temporary downstream patch or vendored change.
- Record any new browser/runtime compatibility caveat in [`docs/program/browser-lakehouse-release-handoff.md`](./browser-lakehouse-release-handoff.md).
- Update [`docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md`](../release-gates/browser-wasm-delta-gcs-launch-checklist.md) if the dependency change reopens a release gate.
