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
- [ ] Release-process evidence uses docs/release-gates/daxis-release-attachment-template.md for git SHA, worker size, public GCS live smoke, release notes, and migration notes.
- [ ] Daxis-facing release notes use docs/release-gates/daxis-release-notes-template.md for semantic, Daxis result metrics and observability fields, fallback, compatibility, descriptor, error-taxonomy, runtime-budget, worker-artifact, and trust-boundary changes.
- [ ] Daxis-facing migration notes use docs/release-gates/daxis-release-migration-notes-template.md for breaking changes or explicit no-breaking-change statements.
- [ ] Daxis-owned production proof uses docs/release-gates/daxis-external-proof-attachment-template.md before stable default routing.
- [ ] Rollout and fallback changes preserve sql_fallback_required, server_fallback, and blocked states.
- [ ] Promotion requires passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls.
- [ ] Stable default routing is gated on docs/release-gates/daxis-external-proof-packet.json stableDefaultPromotionGate acceptance.
- [ ] Relevant docs and release evidence are updated.
