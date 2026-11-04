# Production readiness execution

Goal: close the locally reproducible release blockers on the combined candidate at
`a71d5754fad6a241fb0092fd6785f36ab4d8aef6`, which already includes PRs #139–#142.

Use the existing integration worktree on `fix/production-readiness-20261103`.
Preserve the root checkout, the runner-fix worktree, and retained proof artifacts.
No remote merges, fork publication/tags, provider operations, or deployment are
part of this local execution. Final qualification remains blocked by incomplete
fork release provenance and the separate canonical browser-runtime identity audit.

1. Add failing regressions for blocked deployment access, rejected bypasses,
   deployed manifest identity, the supported browser runner, and the mandatory
   staged query smoke before promotion.
2. In `apps/axon-web/scripts/verify-deployment.sh`, fail when verification cannot
   access the deployment and compare the deployed runtime manifest with the
   qualified local artifact. Keep existing routing and MIME checks.
3. Apply the existing Ubuntu 24.04 runner correction. Reuse the production-bundle
   query smoke for deployed origins, scope bypass headers to that origin, and
   run it on the staged deployment before promotion.
4. Reproduce the two editor lazy-startup failures against the existing generated
   WASM. Repair the owning import boundary if confirmed, preserving error handling.
5. Run focused regressions, TypeScript/lint/format, then the full unit and SDK
   suites and bounded browser checks using installed browsers. Check capacity
   before browser runs. Do not rebuild Rust/WASM or run the ten-spill lane with
   the current 4.5 GiB disk reserve.
6. Review the incremental changes against this contract and update the release
   qualification document with passed, failed, blocked, and untested gates.

Acceptance: blocked or mismatched deployments cannot pass verification; both
staging and promotion require a real query against the staged artifact; browser
installation uses a supported runner; startup remains lazy; targeted validation
passes without modifying unrelated work or claiming remote qualification.
