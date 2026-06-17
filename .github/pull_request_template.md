# Pull Request

## Summary

## Verification

## Daxis Impact Summary

- Contract change:
- Compatibility claim:
- Fallback behavior:
- Evidence:
- WASM size, startup, memory, or browser compatibility:
- Coordinated Daxis update:

## Daxis-Relevant Changes

- [ ] The Daxis/Axon trust boundary is unchanged or explicitly documented.
- [ ] Browser code does not receive raw credentials or signing capability.
- [ ] Public contract changes are additive or have a migration path.
- [ ] TypeScript and Rust contract shapes stay aligned.
- [ ] Unsupported behavior returns a structured error, fallback, or block reason.
- [ ] Native/browser parity tests cover any new supported query behavior.
- [ ] Browser dependency guardrails still pass.
- [ ] WASM target checks still pass.
- [ ] Worker artifact size impact is known.
- [ ] Browser matrix coverage is updated when worker behavior changes.
- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes, including release_channel, rollout_segment, and releaseAttachmentSchema.allowedReleaseChannels.
- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.
- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements.
- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md plus docs/release-gates/daxis-dirty-worktree-review-template.json for dirty-checkout reviews, and attaches release_channel, production environment_class, axon_release_commit_sha, axon_release_ref, proofAttachmentSchema.allowedReleaseChannels, proofAttachmentSchema.acceptedDaxisWorktreeReviews, the daxis.external_state.v1 JSON summary, its SHA-256 digest, and clean or digest-pinned dirty-reviewed Daxis worktree classification before stable default routing.
- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states.
- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.
- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance, requiredReleaseChannel stable, and requiredReleaseEvidenceArtifactCommand (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`).
- [ ] Stable default promotion packets are validated with stableDefaultPromotionPacketValidationCommand (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`) before default routing.
- [ ] Relevant docs and release evidence are updated.
