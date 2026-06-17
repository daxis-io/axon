#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
manifest="$repo_root/docs/release-gates/daxis-release-bundle-manifest.json"
external_proof_packet="$repo_root/docs/release-gates/daxis-external-proof-packet.json"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates" "$repo_root/tests/conformance"
for doc in \
	docs/adr/ADR-0008-daxis-browser-read-compute-contract.md \
	docs/program/daxis-first-class-integration-strategy.md \
	docs/release-gates/daxis-release-attachment-template.md \
	docs/release-gates/daxis-release-notes-template.md \
	docs/release-gates/daxis-release-migration-notes-template.md \
	docs/release-gates/browser-wasm-delta-gcs-release-evidence.md \
	docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md \
	docs/release-gates/browser-wasm-delta-gcs-external-blockers.md \
	docs/release-gates/daxis-production-rollout-decisions.json \
	docs/release-gates/daxis-operational-readiness.json \
	docs/release-gates/daxis-strategy-traceability.json \
	docs/release-gates/daxis-external-proof-packet.json \
	docs/release-gates/daxis-dirty-worktree-review-template.json \
	docs/program/daxis-external-proof-handoff.md \
	docs/program/browser-observability-contract.md \
	docs/program/browser-datafusion-runtime-parity.md \
	docs/program/browser-delta-compatibility-matrix.md \
	crates/query-contract/tests/query_contract.rs \
	tests/conformance/daxis-browser-datafusion-query-corpus.json \
	tests/conformance/verify_daxis_query_corpus_coverage.sh \
	tests/conformance/verify_daxis_external_state.sh \
	tests/conformance/verify_daxis_external_state_test.sh \
	tests/conformance/verify_daxis_release_evidence.sh \
	tests/conformance/verify_daxis_release_attachment.sh \
	tests/conformance/verify_daxis_stable_default_promotion_packet.sh \
	tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh; do
	mkdir -p "$repo_root/$(dirname "$doc")"
	printf '# test fixture\n' >"$repo_root/$doc"
done

write_release_attachment_template() {
	cat >"$repo_root/docs/release-gates/daxis-release-attachment-template.md" <<'EOF'
# Daxis Release Attachment Template

Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, or private customer identifiers.

## Release Attachment Metadata

- item_id:
- release_commit_sha:
- release_ref:
- release_channel:
- rollout_segment:
- owner:
- captured_at:
- artifact_uri:
- artifact_sha256:
- verification_command_or_statement:
- exit_status_or_review_status:
- rollback_or_migration_note_uri:

## Release-Process Items

Use the matching guidance for `item_id`.
Set `release_channel` to the Daxis release channel this attachment supports: `experimental`, `integration`, `candidate`, or `stable`.
The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.
Set `rollout_segment` to the tenant, workspace, table-class, browser-family, or all-segments scope covered by this evidence.
To write an attachment-ready release evidence log and print its digest, run `bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`.
Attach the SHA-256 digest of the release evidence artifact in `artifact_sha256`.
Record only the 64-character lowercase hexadecimal digest generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.
Before stable default promotion, validate a completed release-process attachment with `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md`.
Before stable default promotion, validate the completed release-process attachment set with `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments`.
When the release packet contains local evidence artifacts, validate a completed release-process attachment with `bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-release-attachment.md`.
When the release packet contains local evidence artifacts, validate the completed release-process attachment set with `bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-release-attachments`.

### `git_sha`

- Attach the exact Axon release commit SHA.
- Attach the branch or tag name in `release_ref`.
- Use the release packet URI, commit permalink, or tag permalink as `artifact_uri`.
- Use the rollback or prior-release reference as `rollback_or_migration_note_uri`.

### `worker_artifact_size`

- Attach output from `AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh`.
- If the optional size toolchain is unavailable, attach the release-owner statement that records why size evidence is deferred and where the blocker is tracked.

### `public_gcs_live_smoke`

- Attach output from `AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://... npm run test:browser:public-gcs-live -- --reporter=line`.
- If `AXON_LIVE_PUBLIC_GCS_TABLE_URI` is not configured, attach the skip-safe output and the external blocker record for fixture ownership, runner identity, and CI variable provisioning.

### `release_notes`

- Attach a completed copy of `docs/release-gates/daxis-release-notes-template.md`.
- Cover query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, runtime budgets, worker artifact changes, security-boundary impact, external proof packet status, and `currentPromotionState`.
- For unchanged releases, include the unchanged statement from that template.

### `migration_notes`

- Attach a completed copy of `docs/release-gates/daxis-release-migration-notes-template.md`.
- Include external proof packet status and stable default promotion state.
- For no-breaking releases, include the no-breaking-change statement from that template.

## Review State

- attached
- reviewed
- accepted
- rejected

## Owner Review

- Release owner
- Runtime / engine owner
- Daxis product owner
- Daxis query platform owner
- Daxis catalog/storage owner
- Daxis security owner
- Daxis SRE owner
EOF
}

write_release_evidence_doc() {
	cat >"$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'EOF'
# Browser WASM + Delta on GCS Release Evidence

The Daxis release evidence bundle is controlled by `docs/release-gates/daxis-release-bundle-manifest.json`
and uses `docs/release-gates/daxis-release-attachment-template.md` for `release_process_required`
attachments, including the `artifact_sha256` digest of each release evidence artifact.
Attachments also include `release_channel` and `rollout_segment`.
`releaseAttachmentSchema.allowedReleaseChannels` records the allowed release channels.
`releaseAttachmentSchema.checksumFormat` records that release attachment digests are 64-character lowercase hexadecimal digests generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.
`releaseAttachmentSchema.requiredReviewerRoles` records the owner roles required for release attachment review.
Validate completed stable-default release-process attachments with `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md`.
The manifest carries the exact command as `releaseAttachmentSchema.stableDefaultValidationCommand`.
Validate the completed stable-default release-process attachment set with `bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments`.
The manifest carries the exact command as `releaseAttachmentSchema.stableDefaultDirectoryValidationCommand`.
When the release packet contains local evidence artifacts, validate completed stable-default release-process attachments with `bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-release-attachment.md`.
The manifest carries the exact command as `releaseAttachmentSchema.stableDefaultArtifactValidationCommand`.
When the release packet contains local evidence artifacts, validate the completed stable-default release-process attachment set with `bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-release-attachments`.
The manifest carries the exact command as `releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand`.
Validate the complete stable-default promotion packet with `bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`.
The manifest carries the exact command as `stableDefaultPromotionPacketValidationCommand`.
The stable-default promotion packet verifier also compares the attached log against `bash tests/conformance/verify_daxis_release_evidence.sh --list`, rejects logs missing listed commands, and requires every release attachment and external proof attachment to share one Axon release commit, one Axon release ref, one release channel, and one rollout segment before it prints `daxis_stable_default_release_identity_verified=true`.

Run `bash tests/conformance/verify_daxis_release_evidence.sh` before release review.

Release-process attachments:

- `git_sha`
- `worker_artifact_size`
- `public_gcs_live_smoke`
- `release_notes`
- `migration_notes`

The `public_gcs_live_smoke` attachment uses `AXON_LIVE_PUBLIC_GCS_TABLE_URI`.
Release notes use `docs/release-gates/daxis-release-notes-template.md` and cover query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, external proof packet status, and `currentPromotionState`.
Migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for Daxis-facing breaking-change plans or no-breaking-change statements.
Stable default routing is gated by `stableDefaultPromotionGate`.

Daxis runner commands:

- `cargo fmt --check`
- `cargo test -p query-contract`
- `cargo test -p wasm-datafusion-poc --test daxis_query_corpus`
- `bash tests/conformance/verify_daxis_query_corpus_coverage.sh`
- `cargo test -p wasm-datafusion-session`
- `cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker`
- `bash tests/conformance/verify_browser_worker_dependency_boundary.sh`
- `bash tests/conformance/verify_browser_observability_contract.sh`
- `bash tests/conformance/verify_axon_web_datafusion_runtime.sh`
- `bash tests/conformance/verify_daxis_rollout_decisions.sh`
- `bash tests/conformance/verify_daxis_strategy_document.sh`
- `bash tests/conformance/verify_daxis_strategy_traceability.sh`
- `bash tests/conformance/verify_daxis_external_state_test.sh`
- `bash tests/conformance/verify_daxis_external_proof_packet.sh`
- `bash tests/conformance/verify_daxis_architecture_adr.sh`
- `bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh`
- `env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm`
- `bash tests/conformance/verify_daxis_pr_checklist.sh`
- `bash tests/perf/report_datafusion_wasm_size_test.sh`
- `npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"`
EOF
}

write_launch_checklist() {
	cat >"$repo_root/docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md" <<'EOF'
# Browser WASM + Delta on GCS Launch Checklist

## Release Hygiene

- [x] Daxis release evidence gates are scriptable through `bash tests/conformance/verify_daxis_release_evidence.sh`; `--list` prints the exact gate set.
- [x] Daxis release attachments require `artifact_sha256`, `release_channel`, `rollout_segment`, `releaseAttachmentSchema.allowedReleaseChannels`, `releaseAttachmentSchema.checksumFormat`, and `releaseAttachmentSchema.requiredReviewerRoles`.
- [x] Daxis external proof attachments require `daxis_external_state_json_sha256`, `daxis_worktree_review_json_sha256`, `proofAttachmentSchema.allowedReleaseChannels`, `proofAttachmentSchema.acceptedDaxisWorktreeReviews`, `proofAttachmentSchema.checksumFormat`, `proofAttachmentSchema.dirtyWorktreeReviewChecksumFormat`, `proofAttachmentSchema.dirtyWorktreeReviewTemplatePath`, and `proofAttachmentSchema.requiredReviewerRoles`.
- [x] Stable-default release and external proof attachments have scriptable validators through `releaseAttachmentSchema.stableDefaultValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md`), `releaseAttachmentSchema.stableDefaultDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments`), `proofAttachmentSchema.stableDefaultValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md`), and `proofAttachmentSchema.stableDefaultDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments`).
- [x] Local release packets can additionally verify evidence artifact bytes through `releaseAttachmentSchema.stableDefaultArtifactValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-release-attachment.md`) and `releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand` (`bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-release-attachments`).
- [x] Stable-default promotion packets can be verified with `stableDefaultPromotionPacketValidationCommand` (`bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0`), require `requiredReleaseEvidenceArtifactCommand` (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`), and print `daxis_stable_default_release_identity_verified=true` after one Axon release commit, one Axon release ref, one release channel, and one rollout segment are shared across release and proof attachments.
EOF
}

write_release_evidence_runner() {
	cat >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

case "${1:-}" in
  --list)
    cat <<'COMMANDS'
cargo fmt --check
cargo test -p query-contract
cargo test -p wasm-datafusion-poc --test daxis_query_corpus
bash tests/conformance/verify_daxis_query_corpus_coverage.sh
cargo test -p wasm-datafusion-session
cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
bash tests/conformance/verify_browser_observability_contract.sh
bash tests/conformance/verify_axon_web_datafusion_runtime.sh
bash tests/conformance/verify_daxis_rollout_decisions.sh
bash tests/conformance/verify_daxis_strategy_document.sh
bash tests/conformance/verify_daxis_strategy_traceability.sh
bash tests/conformance/verify_daxis_external_state_test.sh
bash tests/conformance/verify_daxis_external_proof_packet.sh
bash tests/conformance/verify_daxis_architecture_adr.sh
bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh
env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
bash tests/conformance/verify_daxis_pr_checklist.sh
bash tests/perf/report_datafusion_wasm_size_test.sh
npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
COMMANDS
    ;;
  *)
    exit 0
    ;;
esac
EOF
	chmod +x "$repo_root/tests/conformance/verify_daxis_release_evidence.sh"
}

write_release_notes_template() {
	cat >"$repo_root/docs/release-gates/daxis-release-notes-template.md" <<'EOF'
# Daxis Release Notes Template

Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, private customer identifiers, or unredacted tenant data.

## Release Identity

- Axon commit SHA:
- Branch or tag:
- Release channel:
- Rollout segment:
- Release date:
- Release owner:
- Daxis rollout decision link:
- Migration notes link:

## Compatibility Summary

```text
This Axon release does not change Daxis-facing query results, fallback behavior, supported SQL claims, Delta feature handling, descriptor validation, or public error taxonomy.
```

## Required Release Note Sections

### Query Result Semantics

### Query Metrics And Observability

Daxis result metrics include `rows_returned`, `arrow_ipc_bytes`, `scan_bytes`, `duration_ms`, `files_touched`, `files_skipped`, `row_groups_touched`, `row_groups_skipped`, `footer_reads`, `snapshot_bootstrap_duration_ms`, and `access_mode`.

Daxis dashboard or telemetry action required:

### Fallback Behavior

### Supported SQL And Delta Features

### Descriptor Validation

### Error And Fallback Taxonomy

### Runtime Budgets And Worker Artifact

### Security And Browser Trust Boundary

## Required Evidence

- Daxis release evidence runner:
- Daxis release bundle manifest verifier:
- Daxis compatibility notes:
- External proof packet status:
- Stable default promotion state:
- Migration notes or no-breaking-change statement:
- Browser matrix result when worker behavior changed:
- Contract artifact verifier when public contracts changed:

## Owner Signoff

- Release owner:
- Runtime / engine owner:
- Daxis product owner:
- Daxis query platform owner:
- Daxis catalog/storage owner:
- Daxis security owner:
- Daxis SRE owner:
EOF
}

write_migration_notes_template() {
	cat >"$repo_root/docs/release-gates/daxis-release-migration-notes-template.md" <<'EOF'
# Daxis Release Migration Notes Template

Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, or private customer identifiers.

## Release Identity

- Axon commit SHA:
- Branch or tag:
- Release channel:
- Rollout segment:
- Release date:
- Release owner:
- Daxis platform owners reviewed:
- Daxis rollout decision link:

## Compatibility Classification

- [ ] No Daxis-facing breaking change.
- [ ] Breaking Daxis-facing change with migration required before rollout.

For a no-breaking release, include this statement:

```text
This Axon release does not introduce breaking changes to Daxis-facing contracts, runtime semantics, fallback vocabulary, compatibility claims, browser artifact selection, or rollout requirements.
```

## Change Inventory

| Area                                                    | Changed? | Evidence | Daxis action required | Rollback impact |
| ------------------------------------------------------- | -------- | -------- | --------------------- | --------------- |
| Public Rust contracts and JSON Schema                   |          |          |                       |                 |
| TypeScript SDK examples and worker envelopes            |          |          |                       |                 |
| Descriptor resolver or read-access-plan behavior        |          |          |                       |                 |
| Object-grant route envelopes or audit fixtures          |          |          |                       |                 |
| Fallback, block, or runtime-error vocabulary            |          |          |                       |                 |
| Supported SQL, Delta features, or table eligibility     |          |          |                       |                 |
| Runtime budgets, worker artifact, or default worker SKU |          |          |                       |                 |
| Browser dependency, credential, or signing guardrails   |          |          |                       |                 |
| Release channel, rollout controls, or external blockers |          |          |                       |                 |

## Required Evidence

- Contract artifact verifier:
- Daxis release evidence runner:
- Browser matrix result:
- Worker artifact size output:
- Daxis production rollout decision:
- External proof packet status:
- Stable default promotion state:

## Migration Plan

- Affected Daxis services, surfaces, or owners:
- Old contract or runtime behavior:
- New contract or runtime behavior:
- Required Daxis code or configuration changes:
- Backward compatibility window:
- Staged rollout sequence:
- Required dashboard or alert updates:
- User or operator communication:
- Rollback plan:

## Owner Signoff

- Release owner:
- Runtime / engine owner:
- Daxis product owner:
- Daxis query platform owner:
- Daxis catalog/storage owner:
- Daxis security owner:
- Daxis SRE owner:
EOF
}

write_release_attachment_template
write_release_evidence_doc
write_launch_checklist
write_release_evidence_runner
write_release_notes_template
write_migration_notes_template

write_valid_manifest() {
	python3 - "$manifest" "$external_proof_packet" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
external_proof_packet_path = Path(sys.argv[2])
item_ids = [
    "git_sha",
    "contract_versions_schema_hashes",
    "rust_test_summary",
    "typescript_test_summary",
    "wasm_target_checks",
    "browser_matrix_result",
    "public_gcs_live_smoke",
    "worker_artifact_size",
    "dependency_guardrail_result",
    "known_fallback_reasons",
    "supported_sql_delta_feature_matrix",
    "daxis_compatibility_notes",
    "release_notes",
    "migration_notes",
    "external_blockers",
]
items = []
for item_id in item_ids:
    status = "repo_verified"
    item = {
        "id": item_id,
        "status": status,
        "owner": "Runtime / engine team",
        "description": f"{item_id} release bundle fixture",
        "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
        "verificationCommands": ["cargo test -p wasm-datafusion-session"],
    }
    if item_id == "git_sha":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Attach the release commit SHA and branch or tag name."
    if item_id == "rust_test_summary":
        item["verificationCommands"] = [
            "cargo fmt --check",
            "cargo test -p query-contract",
            "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
            "cargo test -p wasm-datafusion-session",
            "cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker",
        ]
    if item_id == "supported_sql_delta_feature_matrix":
        item["evidence"] = [
            "docs/program/browser-datafusion-runtime-parity.md",
            "docs/program/browser-delta-compatibility-matrix.md",
            "tests/conformance/daxis-browser-datafusion-query-corpus.json",
            "tests/conformance/verify_daxis_query_corpus_coverage.sh",
        ]
        item["verificationCommands"] = [
            "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
            "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
        ]
    if item_id == "worker_artifact_size":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Attach output from AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh."
    if item_id == "public_gcs_live_smoke":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Attach live output from AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://... npm run test:browser:public-gcs-live -- --reporter=line, or attach the skip-safe output plus the external blocker record when the live fixture variable is not configured."
        item["evidence"] = [
            "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
            "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
        ]
    if item_id == "dependency_guardrail_result":
        item["verificationCommands"] = [
            "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
            "bash tests/conformance/verify_axon_web_datafusion_runtime.sh",
            "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm",
        ]
    if item_id == "known_fallback_reasons":
        item["evidence"] = [
            "docs/program/daxis-first-class-integration-strategy.md",
            "docs/program/browser-observability-contract.md",
            "docs/program/browser-datafusion-runtime-parity.md",
            "crates/query-contract/tests/query_contract.rs",
        ]
        item["verificationCommands"] = [
            "bash tests/conformance/verify_browser_observability_contract.sh",
            "cargo test -p query-contract",
            "cargo test -p wasm-datafusion-session",
        ]
    if item_id == "daxis_compatibility_notes":
        item["verificationCommands"] = [
            "bash tests/conformance/verify_daxis_rollout_decisions.sh",
            "bash tests/conformance/verify_daxis_strategy_document.sh",
            "bash tests/conformance/verify_daxis_strategy_traceability.sh",
            "bash tests/conformance/verify_daxis_external_proof_packet.sh",
            "bash tests/conformance/verify_daxis_architecture_adr.sh",
        ]
    if item_id == "migration_notes":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Use docs/release-gates/daxis-release-migration-notes-template.md to attach migration notes or an explicit no-breaking-change statement for this release channel and rollout segment, including the external proof packet status and stableDefaultPromotionGate currentPromotionState."
        item["evidence"] = [
            "docs/program/daxis-first-class-integration-strategy.md",
            "docs/release-gates/daxis-release-migration-notes-template.md",
        ]
    if item_id == "release_notes":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Use docs/release-gates/daxis-release-notes-template.md to attach Daxis-facing release notes or an explicit unchanged statement for this release channel and rollout segment, including query results, Daxis result metrics and observability fields, fallback behavior, SQL and Delta support, descriptor validation, error taxonomy, external proof packet status, and currentPromotionState."
        item["evidence"] = [
            "docs/program/daxis-first-class-integration-strategy.md",
            "docs/release-gates/daxis-release-notes-template.md",
        ]
    if item_id == "external_blockers":
        item["evidence"] = [
            "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
            "docs/release-gates/daxis-external-proof-packet.json",
            "docs/program/daxis-external-proof-handoff.md",
            "tests/conformance/verify_daxis_external_state.sh",
            "tests/conformance/verify_daxis_external_state_test.sh",
        ]
        item["verificationCommands"] = [
            "bash tests/conformance/verify_daxis_external_state_test.sh",
            "bash tests/conformance/verify_daxis_external_proof_packet.sh",
        ]
    items.append(item)

manifest = {
    "manifest": "daxis_release_bundle_manifest",
    "strategy": "docs/program/daxis-first-class-integration-strategy.md",
    "sourceDocs": [
        "docs/program/daxis-first-class-integration-strategy.md",
        "docs/release-gates/daxis-release-attachment-template.md",
        "docs/release-gates/daxis-release-notes-template.md",
        "docs/release-gates/daxis-release-migration-notes-template.md",
        "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
        "docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md",
        "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
        "docs/release-gates/daxis-production-rollout-decisions.json",
        "docs/release-gates/daxis-operational-readiness.json",
        "docs/release-gates/daxis-strategy-traceability.json",
        "docs/release-gates/daxis-external-proof-packet.json",
        "docs/release-gates/daxis-dirty-worktree-review-template.json",
        "docs/program/browser-datafusion-runtime-parity.md",
        "docs/program/browser-delta-compatibility-matrix.md",
        "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    ],
    "releaseAttachmentSchema": {
        "purpose": "Minimum metadata release owners attach for release-process-required bundle items.",
        "templatePath": "docs/release-gates/daxis-release-attachment-template.md",
        "releaseProcessItemIds": [
            "git_sha",
            "worker_artifact_size",
            "public_gcs_live_smoke",
            "release_notes",
            "migration_notes",
        ],
        "requiredMetadata": [
            "item_id",
            "release_commit_sha",
            "release_ref",
            "release_channel",
            "rollout_segment",
            "owner",
            "captured_at",
            "artifact_uri",
            "artifact_sha256",
            "verification_command_or_statement",
            "exit_status_or_review_status",
            "rollback_or_migration_note_uri",
        ],
        "allowedReleaseChannels": ["experimental", "integration", "candidate", "stable"],
        "checksumFormat": {
            "field": "artifact_sha256",
            "algorithm": "sha256",
            "encoding": "lowercase_hex",
            "length": 64,
            "sourceBytes": "exact_release_evidence_artifact_bytes",
        },
        "stableDefaultValidationCommand": "bash tests/conformance/verify_daxis_release_attachment.sh --stable-default path/to/completed-release-attachment.md",
        "stableDefaultDirectoryValidationCommand": "bash tests/conformance/verify_daxis_release_attachment.sh --stable-default-dir path/to/completed-release-attachments",
        "stableDefaultArtifactValidationCommand": "bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-release-attachment.md",
        "stableDefaultArtifactDirectoryValidationCommand": "bash tests/conformance/verify_daxis_release_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-release-attachments",
        "requiredReviewStates": ["attached", "reviewed", "accepted", "rejected"],
        "requiredReviewerRoles": [
            "Release owner",
            "Runtime / engine owner",
            "Daxis product owner",
            "Daxis query platform owner",
            "Daxis catalog/storage owner",
            "Daxis security owner",
            "Daxis SRE owner",
        ],
    },
    "bundleItems": items,
    "releaseEvidenceCommands": [
        "bash tests/conformance/verify_daxis_release_evidence.sh",
        "bash tests/conformance/verify_daxis_release_evidence.sh --list",
        "bash tests/conformance/verify_daxis_external_proof_attachment_test.sh",
        "bash tests/conformance/verify_daxis_release_attachment_test.sh",
        "bash tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh",
        "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    ],
    "releaseEvidenceArtifactCommand": "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
    "stableDefaultPromotionPacketValidationCommand": "bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0",
}
path.parent.mkdir(parents=True, exist_ok=True)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)

external_proof_packet = {
    "packet": "daxis_external_proof_packet",
    "stableDefaultPromotionGate": {
        "requiredReleaseProcessAttachments": [
            "git_sha",
            "worker_artifact_size",
            "public_gcs_live_smoke",
            "release_notes",
            "migration_notes",
        ],
        "requiredReviewState": "accepted",
        "requiredReleaseEvidenceCommand": "bash tests/conformance/verify_daxis_release_evidence.sh",
        "requiredReleaseEvidenceArtifactCommand": "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
        "requiredReleaseChannel": "stable",
        "requiredRollbackState": "server_fallback",
        "blockerRegister": "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
        "requiredReleaseAttachmentSchemaFields": [
            "artifact_sha256",
            "release_channel",
            "rollout_segment",
            "releaseAttachmentSchema.allowedReleaseChannels",
            "releaseAttachmentSchema.checksumFormat",
            "releaseAttachmentSchema.requiredReviewerRoles",
        ],
        "requiredProofAttachmentSchemaFields": [
            "release_channel",
            "environment_class",
            "axon_release_commit_sha",
            "axon_release_ref",
            "daxis_worktree_status",
            "daxis_worktree_review",
            "daxis_external_state_json_sha256",
            "daxis_worktree_review_json_sha256",
            "proofAttachmentSchema.allowedReleaseChannels",
            "proofAttachmentSchema.allowedDaxisWorktreeStatuses",
            "proofAttachmentSchema.allowedDaxisWorktreeReviews",
            "proofAttachmentSchema.acceptedDaxisWorktreeReviews",
            "proofAttachmentSchema.requiredEnvironmentClass",
            "proofAttachmentSchema.checksumFormat",
            "proofAttachmentSchema.dirtyWorktreeReviewChecksumFormat",
            "proofAttachmentSchema.dirtyWorktreeReviewTemplatePath",
            "proofAttachmentSchema.requiredReviewerRoles",
        ],
    },
    "proofAttachmentSchema": {
        "allowedReleaseChannels": ["experimental", "integration", "candidate", "stable"],
        "allowedDaxisWorktreeStatuses": ["clean", "dirty"],
        "allowedDaxisWorktreeReviews": ["clean", "dirty_reviewed", "dirty_rejected"],
        "acceptedDaxisWorktreeReviews": ["clean", "dirty_reviewed"],
        "dirtyWorktreeReviewTemplatePath": "docs/release-gates/daxis-dirty-worktree-review-template.json",
        "dirtyWorktreeReviewChecksumFormat": {
            "field": "daxis_worktree_review_json_sha256",
            "algorithm": "sha256",
            "encoding": "lowercase_hex",
            "length": 64,
            "sourceBytes": "exact_dirty_worktree_review_json_bytes",
        },
    },
}
with open(external_proof_packet_path, "w", encoding="utf-8") as handle:
    json.dump(external_proof_packet, handle)
PY
}

verify_fixture() {
	AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT="$repo_root" \
		AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE="$manifest" \
		bash tests/conformance/verify_daxis_release_bundle_manifest.sh >/dev/null 2>&1
}

write_valid_manifest
verify_fixture

python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["sourceDocs"].remove("docs/adr/ADR-0008-daxis-browser-read-compute-contract.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release bundle missing architecture ADR source doc to be rejected" >&2
	exit 1
fi
write_valid_manifest

python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["sourceDocs"].remove("docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release bundle missing launch checklist source doc to be rejected" >&2
	exit 1
fi
write_valid_manifest

python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["sourceDocs"].append("docs/program/daxis-first-class-integration-strategy.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected duplicate release bundle source docs to be rejected" >&2
	exit 1
fi
write_valid_manifest

write_launch_checklist
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("`proofAttachmentSchema.requiredReviewerRoles`", "`proofAttachmentSchema.reviewers`")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected launch checklist missing external proof reviewer-role schema to be rejected" >&2
	exit 1
fi
write_launch_checklist

python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-launch-checklist.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(
    "require `requiredReleaseEvidenceArtifactCommand` (`bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log`), and ",
    "",
)
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected launch checklist missing stable-default release evidence artifact command to be rejected" >&2
	exit 1
fi
write_launch_checklist

write_release_evidence_doc
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Browser WASM + Delta on GCS Release Evidence\n\n"
        "Run `bash tests/conformance/verify_daxis_release_evidence.sh` before release review.\n\n"
        "Release-process attachments use `docs/release-gates/daxis-release-attachment-template.md`.\n"
    )
PY

if verify_fixture; then
	echo "expected release evidence doc missing stable-default and release-process coverage to be rejected" >&2
	exit 1
fi
write_release_evidence_doc

write_valid_manifest
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    text = handle.read()
text = text.replace(
    "Migration notes use `docs/release-gates/daxis-release-migration-notes-template.md` for Daxis-facing breaking-change plans or no-breaking-change statements.\n",
    "",
)
with open(path, "w", encoding="utf-8") as handle:
    handle.write(text)
PY

if verify_fixture; then
	echo "expected release evidence doc missing Daxis migration notes template guidance to be rejected" >&2
	exit 1
fi
write_release_evidence_doc

write_valid_manifest
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Browser WASM + Delta on GCS Release Evidence\n\n"
        "The Daxis release evidence bundle is controlled by `docs/release-gates/daxis-release-bundle-manifest.json`\n"
        "and uses `docs/release-gates/daxis-release-attachment-template.md` for `release_process_required` attachments.\n\n"
        "Run `bash tests/conformance/verify_daxis_release_evidence.sh` before release review.\n\n"
        "Release-process attachments:\n\n"
        "- `git_sha`\n"
        "- `worker_artifact_size`\n"
        "- `public_gcs_live_smoke`\n"
        "- `release_notes`\n"
        "- `migration_notes`\n\n"
        "The `public_gcs_live_smoke` attachment uses `AXON_LIVE_PUBLIC_GCS_TABLE_URI`.\n"
        "Release notes use `docs/release-gates/daxis-release-notes-template.md` and cover query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, external proof packet status, and `currentPromotionState`.\n"
        "Stable default routing is gated by `stableDefaultPromotionGate`.\n\n"
        "Daxis runner commands:\n\n"
        "- `cargo test -p wasm-datafusion-session`\n"
        "- `npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\"`\n"
    )
PY

if verify_fixture; then
	echo "expected release evidence doc missing Daxis default-worker dependency guardrail to be rejected" >&2
	exit 1
fi
write_release_evidence_doc

write_valid_manifest
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Browser WASM + Delta on GCS Release Evidence\n\n"
        "The Daxis release evidence bundle is controlled by `docs/release-gates/daxis-release-bundle-manifest.json`\n"
        "and uses `docs/release-gates/daxis-release-attachment-template.md` for `release_process_required` attachments.\n\n"
        "Run `bash tests/conformance/verify_daxis_release_evidence.sh` before release review.\n\n"
        "Release-process attachments:\n\n"
        "- `git_sha`\n"
        "- `worker_artifact_size`\n"
        "- `public_gcs_live_smoke`\n"
        "- `release_notes`\n"
        "- `migration_notes`\n\n"
        "The `public_gcs_live_smoke` attachment uses `AXON_LIVE_PUBLIC_GCS_TABLE_URI`.\n"
        "Release notes use `docs/release-gates/daxis-release-notes-template.md` and cover query-result semantics, Daxis result metrics and observability fields, fallback behavior, supported SQL and Delta feature claims, descriptor validation, public error taxonomy, external proof packet status, and `currentPromotionState`.\n"
        "Stable default routing is gated by `stableDefaultPromotionGate`.\n\n"
        "Daxis runner commands:\n\n"
        "- `cargo test -p wasm-datafusion-session`\n"
        "- `env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm`\n"
        "- `npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\"`\n"
    )
PY

if verify_fixture; then
	echo "expected release evidence doc missing Daxis query corpus command to be rejected" >&2
	exit 1
fi
write_release_evidence_doc

write_valid_manifest
python3 - "$repo_root/docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    text = handle.read()
text = text.replace("- `bash tests/conformance/verify_daxis_pr_checklist.sh`\n", "")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(text)
PY

if verify_fixture; then
	echo "expected release evidence doc missing Daxis PR checklist command to be rejected" >&2
	exit 1
fi
write_release_evidence_doc

write_valid_manifest
cat >"$repo_root/tests/conformance/verify_daxis_release_evidence.sh" <<'EOF'
#!/usr/bin/env bash

set -euo pipefail

case "${1:-}" in
  --list)
    cat <<'COMMANDS'
cargo test -p wasm-datafusion-poc --test daxis_query_corpus
bash tests/conformance/verify_daxis_query_corpus_coverage.sh
cargo test -p wasm-datafusion-session
bash tests/conformance/verify_browser_worker_dependency_boundary.sh
env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm
bash tests/conformance/verify_daxis_pr_checklist.sh
bash tests/perf/report_datafusion_wasm_size_test.sh
npm exec -- playwright test --config=playwright.config.ts --grep "Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors"
bash tests/conformance/verify_unlisted_future_daxis_gate.sh
COMMANDS
    ;;
  *)
    exit 0
    ;;
esac
EOF
chmod +x "$repo_root/tests/conformance/verify_daxis_release_evidence.sh"

if verify_fixture; then
	echo "expected release evidence doc missing a listed runner command to be rejected" >&2
	exit 1
fi
write_release_evidence_runner

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest.pop("releaseAttachmentSchema")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release attachment schema to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("templatePath")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release attachment template path to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-attachment-template.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Daxis Release Attachment Template\n\n"
        "## Release Attachment Metadata\n\n"
        "- item_id:\n"
        "- release_commit_sha:\n"
        "- release_ref:\n"
        "- owner:\n"
        "- captured_at:\n"
        "- artifact_uri:\n"
        "- verification_command_or_statement:\n"
        "- exit_status_or_review_status:\n"
    )
PY

if verify_fixture; then
	echo "expected release attachment template missing rollback metadata to be rejected" >&2
	exit 1
fi
write_release_attachment_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-attachment-template.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Daxis Release Attachment Template\n\n"
        "Do not attach browser-visible secrets, signed URLs, raw credentials, service-account material, private customer identifiers.\n\n"
        "## Release Attachment Metadata\n\n"
        "- item_id:\n"
        "- release_commit_sha:\n"
        "- release_ref:\n"
        "- owner:\n"
        "- captured_at:\n"
        "- artifact_uri:\n"
        "- verification_command_or_statement:\n"
        "- exit_status_or_review_status:\n"
        "- rollback_or_migration_note_uri:\n\n"
        "## Release-Process Items\n\n"
        "- git_sha\n"
        "- worker_artifact_size\n"
        "- public_gcs_live_smoke\n"
        "- release_notes\n"
        "- migration_notes\n\n"
        "## Review State\n\n"
        "- attached\n"
        "- reviewed\n"
        "- accepted\n"
        "- rejected\n"
    )
PY

if verify_fixture; then
	echo "expected release attachment template missing per-item evidence guidance to be rejected" >&2
	exit 1
fi
write_release_attachment_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-attachment-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace(", external proof packet status, and `currentPromotionState`", "")
text = text.replace("- Include external proof packet status and stable default promotion state.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected release attachment template missing release-note and migration proof-state guidance to be rejected" >&2
	exit 1
fi
write_release_attachment_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-notes-template.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Daxis Release Notes Template\n\n"
        "## Release Identity\n\n"
        "- Axon commit SHA:\n"
        "- Branch or tag:\n"
    )
PY

if verify_fixture; then
	echo "expected incomplete release notes template to be rejected" >&2
	exit 1
fi
write_release_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-notes-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- External proof packet status:\n", "")
text = text.replace("- Stable default promotion state:\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected release notes template missing external proof and promotion-state evidence to be rejected" >&2
	exit 1
fi
write_release_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-notes-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- Rollout segment:\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected release notes template missing rollout segment to be rejected" >&2
	exit 1
fi
write_release_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-notes-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("### Query Metrics And Observability\n\n", "")
text = text.replace(
    "Daxis result metrics include `rows_returned`, `arrow_ipc_bytes`, `scan_bytes`, `duration_ms`, `files_touched`, `files_skipped`, `row_groups_touched`, `row_groups_skipped`, `footer_reads`, `snapshot_bootstrap_duration_ms`, and `access_mode`.\n\n",
    "",
)
text = text.replace("Daxis dashboard or telemetry action required:\n\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected release notes template missing Daxis result-metrics observability guidance to be rejected" >&2
	exit 1
fi
write_release_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-migration-notes-template.md" <<'PY'
import sys

path = sys.argv[1]
with open(path, "w", encoding="utf-8") as handle:
    handle.write(
        "# Daxis Release Migration Notes Template\n\n"
        "## Release Identity\n\n"
        "- Axon commit SHA:\n"
        "- Branch or tag:\n"
    )
PY

if verify_fixture; then
	echo "expected incomplete migration notes template to be rejected" >&2
	exit 1
fi
write_migration_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-migration-notes-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- Stable default promotion state:\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected migration notes template missing stable default promotion state to be rejected" >&2
	exit 1
fi
write_migration_notes_template

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-migration-notes-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- Rollout segment:\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected migration notes template missing rollout segment to be rejected" >&2
	exit 1
fi
write_migration_notes_template

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["requiredMetadata"].remove("rollback_or_migration_note_uri")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing rollback or migration-note attachment metadata to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["requiredMetadata"].remove("artifact_sha256")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release artifact checksum attachment metadata to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["requiredMetadata"].remove("release_channel")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release channel attachment metadata to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("allowedReleaseChannels")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release attachment schema missing allowed release channels to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["requiredMetadata"].remove("rollout_segment")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing rollout segment attachment metadata to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$repo_root/docs/release-gates/daxis-release-attachment-template.md" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Record only the 64-character lowercase hexadecimal digest generated from the exact release evidence artifact bytes, for example with `shasum -a 256 path/to/artifact`.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
	echo "expected missing release artifact checksum format guidance to be rejected" >&2
	exit 1
fi

write_release_attachment_template
write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("checksumFormat")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release attachment checksumFormat schema to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("stableDefaultValidationCommand")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release stable-default attachment validation command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("stableDefaultDirectoryValidationCommand")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release stable-default attachment directory validation command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("stableDefaultArtifactValidationCommand")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release stable-default attachment artifact validation command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("stableDefaultArtifactDirectoryValidationCommand")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release stable-default attachment artifact directory validation command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"].pop("requiredReviewerRoles")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release attachment requiredReviewerRoles schema to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["releaseProcessItemIds"].remove("public_gcs_live_smoke")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release attachment schema to cover public GCS live smoke" >&2
	exit 1
fi

write_valid_manifest
python3 - "$external_proof_packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["stableDefaultPromotionGate"]["requiredReleaseProcessAttachments"].remove(
    "public_gcs_live_smoke"
)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
	echo "expected stable default promotion gate release-process drift from external proof packet to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$external_proof_packet" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["proofAttachmentSchema"].pop("acceptedDaxisWorktreeReviews")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
	echo "expected external proof packet missing accepted Daxis worktree reviews to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseAttachmentSchema"]["requiredReviewStates"].remove("rejected")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release attachment schema to require rejected review state" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"] = [
    item for item in manifest["bundleItems"] if item["id"] != "migration_notes"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing migration notes bundle item to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"] = [
    item for item in manifest["bundleItems"] if item["id"] != "release_notes"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing release notes bundle item to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"] = [
    item for item in manifest["bundleItems"] if item["id"] != "public_gcs_live_smoke"
]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing public GCS live smoke bundle item to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import copy
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"].append(copy.deepcopy(manifest["bundleItems"][0]))
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected duplicate release bundle item ids to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"][0]["evidence"] = ["docs/program/missing-release-evidence.md"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing evidence path to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "git_sha":
        item["status"] = "repo_verified"
        item.pop("releaseAttachment")
        item["verificationCommands"] = ["bash tests/conformance/verify_daxis_release_evidence.sh --list"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected git SHA to remain release-process evidence" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "git_sha":
        item["releaseAttachment"] = "Attach release identity."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected git SHA release attachment to name commit SHA and branch or tag" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "worker_artifact_size":
        item["releaseAttachment"] = "Attach a generic worker size report."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis default-worker size command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "public_gcs_live_smoke":
        item["status"] = "repo_verified"
        item.pop("releaseAttachment")
        item["verificationCommands"] = ["npm run test:browser:public-gcs-live -- --reporter=line"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected public GCS live smoke to remain release-process evidence" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "public_gcs_live_smoke":
        item["releaseAttachment"] = "Attach browser smoke output."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected public GCS live smoke attachment to name the live env command and skip-safe fallback" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "dependency_guardrail_result":
        item["verificationCommands"] = [
            "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
            "bash tests/conformance/verify_axon_web_datafusion_runtime.sh",
        ]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis default-worker dependency guardrail command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "daxis_compatibility_notes":
        item["verificationCommands"].remove("bash tests/conformance/verify_daxis_architecture_adr.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis architecture ADR compatibility-note command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "external_blockers":
        item["verificationCommands"].remove("bash tests/conformance/verify_daxis_external_state_test.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis external-state helper external-blocker command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "external_blockers":
        item["evidence"].remove("tests/conformance/verify_daxis_external_state.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing Daxis external-state helper evidence to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "rust_test_summary":
        item["verificationCommands"].append("bash tests/conformance/verify_stale_item_level_daxis_gate.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected item-level verification command missing from the release evidence runner to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "migration_notes":
        item["status"] = "repo_verified"
        item.pop("releaseAttachment")
        item["verificationCommands"] = ["bash tests/conformance/verify_daxis_release_evidence.sh --list"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected migration notes to remain release-process evidence" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "migration_notes":
        item["releaseAttachment"] = "Attach migration notes or no-breaking-change statement."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected migration notes release attachment to name the Daxis migration template" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "migration_notes":
        item["releaseAttachment"] = "Use docs/release-gates/daxis-release-migration-notes-template.md to attach migration notes or an explicit no-breaking-change statement."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected migration notes release attachment to include external proof status and stable promotion state" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "migration_notes":
        item["releaseAttachment"] = item["releaseAttachment"].replace(
            " for this release channel and rollout segment",
            "",
        )
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected migration notes release attachment to include release-channel and rollout-segment scope" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for item in manifest["bundleItems"]:
    if item["id"] == "release_notes":
        item["releaseAttachment"] = item["releaseAttachment"].replace(
            " for this release channel and rollout segment",
            "",
        )
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release notes release attachment to include release-channel and rollout-segment scope" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseEvidenceCommands"].remove("bash tests/conformance/verify_daxis_release_evidence.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected release evidence commands to include the full Daxis release evidence runner" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest.pop("stableDefaultPromotionPacketValidationCommand")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected missing stable-default promotion packet validation command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseEvidenceCommands"].append("bash tests/conformance/verify_daxis_release_evidence.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected duplicate release evidence commands to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["releaseEvidenceCommands"].append("bash tests/conformance/verify_stale_future_daxis_gate.sh")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected stale release evidence command to be rejected" >&2
	exit 1
fi

write_valid_manifest
python3 - "$manifest" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["bundleItems"][1]["status"] = "claimed_complete"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
	echo "expected invalid bundle item status to be rejected" >&2
	exit 1
fi

echo "Daxis release bundle manifest verifier regression coverage passed"
