# Browser WASM + Delta on GCS Launch Checklist

- Date: 2026-04-07
- Applies to milestone: `M4`

## Documentation

- [x] Browser compatibility is documented in `README.md` and backed by CI checks for `wasm32-unknown-unknown` coverage across the browser crates plus the internal worker artifact.
- [x] Delta compatibility is documented in `README.md` and the browser engine strategy docs, including already repo-owned snapshot reconstruction in `wasm-delta-snapshot` plus `wasm-parquet-engine`, `wasm-datafusion-session`, and legacy `wasm-query-session` compatibility isolation.
- [x] Security reporting is documented in `SECURITY.md` and `tests/security/README.md`.
- [x] Canonical handoff examples live in `docs/program/browser-lakehouse-release-handoff.md` and `docs/program/browser-lakehouse-release-handoff-examples/`, including the session-backed worker command contract and the worker artifact report contract for runtime SKU, session capability, Arrow IPC transport, and artifact identity.
- [x] Repo-owned release evidence keeps Axon runtime scope separate from external blockers: the Daxis-facing app worker reports browser DataFusion as its default SKU, the legacy narrow worker remains compatibility-only, and signed URL issuance, proxy-mode request issuance, audit logging, and production CORS/origin validation remain outside repo-owned browser-engine success claims.

## Release Gates

- [x] `wasm32-unknown-unknown` compile coverage includes `wasm-http-object-store`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, `wasm-query-session`, `browser-sdk`, and `browser-engine-worker`.
- [x] Host tests run for `wasm-query-runtime`, `query-router`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `browser-sdk`, and `browser-engine-worker`.
- [x] Host tests run for `wasm-query-session` to verify repeated-query reuse, in-memory eviction, and dispose semantics.
- [x] Dedicated `wasm32-unknown-unknown` smoke suites run in CI for `browser-sdk`, `wasm-parquet-engine`, `wasm-delta-snapshot`, `wasm-query-runtime`, and `browser-engine-worker`.
- [x] Legacy narrow worker artifact size reporting remains enforced in CI on the real `browser-engine-worker.wasm` artifact.
- [x] Daxis DataFusion default worker size reporting remains release-process evidence for `axon-web-wasm` until the full size toolchain is available in the gate, separate from correctness and runtime-SKU evidence.
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
- [x] Public object-storage table-root access is covered by SDK object-storage/query-source tests, a focused browser editor smoke, and a local live public GCS smoke against `gs://axon-public-delta-fixture-20260522-6cf5c6/axon-smoke-delta`. CI/prod fixture provisioning remains an external blocker until a repo-accessible `AXON_LIVE_PUBLIC_GCS_TABLE_URI` Actions variable and fixture owner are configured.

## Runtime Constraints

- [x] Browser runtime ships single-partition by default.
- [x] The legacy narrow runtime + streaming scan + in-memory session shell remains documented as compatibility-only, while the Daxis-facing app worker is DataFusion-backed Delta/Parquet execution.
- [x] The shipped session shell is documented as in-memory only; persistent-cache hooks may exist below it, but OPFS / IndexedDB backends remain deferred.
- [x] Browser bundle size is tracked in CI on the legacy narrow worker artifact and in release-process evidence for the Daxis DataFusion default worker.
- [x] Browser DataFusion budget reporting is tracked for the Daxis-facing default worker; optional Brotli budget checks are not the current always-on worker size gate.
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
- [x] Daxis release evidence gates are scriptable through `bash tests/conformance/verify_daxis_release_evidence.sh`; `--list` prints the exact gate set.
- [x] Daxis production rollout decisions are captured in `docs/release-gates/daxis-production-rollout-decisions.json` and checked by `bash tests/conformance/verify_daxis_rollout_decisions.sh`.
- [x] Daxis strategy deliverables and exit criteria are mapped in `docs/release-gates/daxis-strategy-traceability.json` and checked by `bash tests/conformance/verify_daxis_strategy_traceability.sh`.
- [x] Daxis external proof requirements are captured in `docs/release-gates/daxis-external-proof-packet.json` and checked by `bash tests/conformance/verify_daxis_external_proof_packet.sh`.
- [x] Daxis browser read-compute ownership is captured in `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md` and checked by `bash tests/conformance/verify_daxis_architecture_adr.sh`.
- [x] Daxis release-bundle contents are captured in `docs/release-gates/daxis-release-bundle-manifest.json` and checked by `bash tests/conformance/verify_daxis_release_bundle_manifest.sh`.
- [x] Daxis release attachments require `artifact_sha256`, `release_channel`, `rollout_segment`, `releaseAttachmentSchema.allowedReleaseChannels`, `releaseAttachmentSchema.checksumFormat`, and `releaseAttachmentSchema.requiredReviewerRoles` so launch evidence is digest-pinned, channel-scoped, segment-scoped, and owner-reviewed.
- [x] Daxis external proof attachments require `daxis_external_state_json_sha256`, `daxis_worktree_review_json_sha256`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, `proofAttachmentSchema.checksumFormat`, `proofAttachmentSchema.dirtyWorktreeReviewChecksumFormat`, `proofAttachmentSchema.dirtyWorktreeReviewTemplatePath`, and `proofAttachmentSchema.requiredReviewerRoles` so production proof is channel-scoped, clean-or-digest-reviewed, template-shaped, digest-pinned, and owner-reviewed.
- [x] Stable-default release and external proof attachments have scriptable validators through `releaseAttachmentSchema.stableDefaultValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md`), `releaseAttachmentSchema.stableDefaultDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments`), `proofAttachmentSchema.stableDefaultValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md`), and `proofAttachmentSchema.stableDefaultDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments`).
- [x] Local release and proof packets can additionally verify evidence artifact bytes through `releaseAttachmentSchema.stableDefaultArtifactValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-release-attachment.md`), `releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-release-attachments`), `proofAttachmentSchema.stableDefaultArtifactValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-proof-attachment.md`), and `proofAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-proof-attachments`).
- [x] The complete stable-default promotion packet has a single verifier through `stableDefaultPromotionPacketValidationCommand` (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`), requires `requiredReleaseEvidenceArtifactCommand` (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`), and prints `daxis_stable_default_release_identity_verified=true` only after release attachments and external proof attachments share one Axon release commit, one Axon release ref, one release channel, and one rollout segment.
- [ ] The oncall playbook covers fallback failure and control-plane outage. External blocker: the repository only contains a local integration runbook, not a service-aware production oncall playbook.

## References

- Repo-owned release evidence: `docs/release-gates/browser-wasm-delta-gcs-release-evidence.md`
- External blocker register: `docs/release-gates/browser-wasm-delta-gcs-external-blockers.md`
- Daxis external proof handoff: `docs/integrations/daxis/daxis-external-proof-handoff.md`
- Daxis external proof attachment template: `docs/release-gates/daxis-external-proof-attachment-template.md`
- Daxis browser read-compute ADR: `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md`
- Daxis release bundle manifest: `docs/release-gates/daxis-release-bundle-manifest.json`
- Daxis release attachment template: `docs/release-gates/daxis-release-attachment-template.md`
