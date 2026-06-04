# Daxis External Proof Handoff

- Date: 2026-05-30
- Scope: Daxis-owned evidence required before Axon is treated as the production default browser read-compute path
- Owner: Daxis product, platform, storage, catalog, web, security, and SRE owners
- Related:
  - [Daxis first-class integration strategy](./daxis-first-class-integration-strategy.md)
  - [Daxis operational maturity contract](./daxis-operational-maturity.md)
  - [Daxis external proof packet](../release-gates/daxis-external-proof-packet.json)
  - [Daxis external proof attachment template](../release-gates/daxis-external-proof-attachment-template.md)
  - [Daxis strategy traceability matrix](../release-gates/daxis-strategy-traceability.json)

This handoff turns the Axon-side strategy, traceability, rollout, and operational-readiness contracts into the proof packet Daxis must attach during rollout review. It does not claim the external Daxis production systems exist in this repository.

## Required Packet

The machine-readable packet is
[`docs/release-gates/daxis-external-proof-packet.json`](../release-gates/daxis-external-proof-packet.json),
checked by `bash tests/conformance/verify_daxis_external_proof_packet.sh`.

Each packet item names:

- the milestone it blocks
- the Daxis owner for the external work
- the proof artifacts Daxis must attach
- the acceptance checks reviewers should apply
- the rollback evidence that keeps server fallback available
- the Axon references that define the contract boundary

The packet also includes a Daxis verification plan with current-state commands
and active Daxis paths that reviewers can use before attaching proof artifacts.
These checks are review aids; they do not replace production endpoint,
dashboard, rollout-control, oncall, or table-compatibility evidence. The
current Daxis verification-plan commands can be run from Axon with:

```bash
AXON_DAXIS_PLATFORM_REPO_ROOT=/path/to/daxis-platform bash tests/conformance/verify_daxis_external_state.sh
```

For proof attachments, also capture the machine-readable summary:

```bash
AXON_DAXIS_PLATFORM_REPO_ROOT=/path/to/daxis-platform bash tests/conformance/verify_daxis_external_state.sh --json
```

That summary uses `schema_version` `daxis.external_state.v1` and records the
Daxis commit SHA, ref, origin remote URL, clean or dirty worktree status,
required dirty-worktree review classification, contract-test status, and
architecture-scan status.

The helper lists and runs these Daxis-side checks:

```bash
git rev-parse HEAD
git rev-parse --abbrev-ref HEAD
git remote get-url origin
git status --short
cargo test -p daxis-query --test contracts
rg -n "Axon browser WASM|Browser read compute|Headless query gateway" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query
```

Attach the helper transcript and JSON summary with the Daxis proof artifacts so
every review has the Daxis commit SHA, branch or detached-ref label, origin
remote URL, working-tree status, contract-test output, and architecture
terminology scan captured for the rollout segment.
If `git status --short` is non-empty, classify the Daxis working-tree status as
`dirty_reviewed` or `dirty_rejected` in the attachment and include owner review
that ties every modified or untracked path to the rollout segment. Clean
checkouts should use `clean`.

Every Daxis proof attachment should use
[`docs/release-gates/daxis-external-proof-attachment-template.md`](../release-gates/daxis-external-proof-attachment-template.md)
and carry the metadata named in `proofAttachmentSchema`: item ID, milestone,
owner, capture time, environment, rollout segment, artifact URI, verification
command or dashboard URL, exit or review status, rollback evidence URI, Daxis
commit SHA, Daxis ref, Daxis origin remote URL, Daxis working-tree status, and
Daxis working-tree review classification. Attach `daxis_external_state_json_uri`
and `daxis_external_state_schema_version` for the helper JSON summary so
reviewers can verify the exact Daxis checkout state without parsing logs.
This keeps rollout proof attributable and repeatable instead of relying on
loose links or screenshots.

The packet's `stableDefaultPromotionGate` keeps the final promotion rule
machine-readable. Its current `currentPromotionState` is
`blocked_external_proof_required`; that state must remain in place until Daxis
attaches accepted proof for every external packet item, accepted
release-process attachments for `git_sha`, `worker_artifact_size`,
`public_gcs_live_smoke`, `release_notes`, and `migration_notes`, full
`bash tests/conformance/verify_daxis_release_evidence.sh` output, the external
blocker register, and a verified `server_fallback` rollback state.

The stable external proof item IDs are:

- `daxis_architecture_docs`
- `daxis_names_axon_default_browser_engine`
- `daxis_descriptor_endpoint`
- `daxis_frontend_flow`
- `daxis_read_access_plan_endpoint`
- `storage_cors_proxy_validation`
- `production_dashboards`
- `production_runbooks`
- `rollout_controls`
- `production_table_compatibility_dashboard`

## Review Flow

1. Run the Axon release evidence runner and attach the output.
2. Attach Daxis external-state helper output from the packet's verification-plan commands with the Daxis proof artifacts.
3. Attach Daxis service endpoint tests for descriptor resolution and read-access-plan outcomes.
4. Attach Daxis frontend open/query/cancel evidence for the supported rollout browsers.
5. Attach storage CORS, proxy, object-scope, TTL, and audit evidence for the first rollout segment.
6. Attach Daxis dashboard, oncall, rollout-control, and compatibility-dashboard proof.
7. Confirm the rollback path can force `server_fallback` without shipping a new Axon release.
8. Confirm `stableDefaultPromotionGate` has all required release-process attachments and external proof items in the accepted state.

## Stable Default Boundary

Axon can supply repo-owned contracts, SDK examples, fixtures, browser matrix coverage, runtime-budget evidence, and release-evidence automation. Daxis must supply production proof for architecture docs, service endpoints, UI integration, storage access, rollout controls, dashboards, runbooks, and current table compatibility.

Until every item in the packet has attached Daxis evidence, the release state should remain at `integration` or `candidate`; it should not be treated as `stable_default`.
