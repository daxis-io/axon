#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_EXTERNAL_PROOF_REPO_ROOT:-$(pwd)}"
packet_file="${AXON_DAXIS_EXTERNAL_PROOF_PACKET_FILE:-docs/release-gates/daxis-external-proof-packet.json}"

packet_path() {
	if [[ "$packet_file" = /* ]]; then
		printf "%s\n" "$packet_file"
		return
	fi

	printf "%s/%s\n" "$repo_root" "$packet_file"
}

path="$(packet_path)"
if [[ ! -f "$path" ]]; then
	echo "missing Daxis external proof packet: $packet_file" >&2
	exit 1
fi

python3 - "$repo_root" "$path" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
path = Path(sys.argv[2])
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_unique_text_list(values: object, label: str) -> None:
    expect(isinstance(values, list), f"{label} must be a list")
    normalized = []
    for value in values:
        expect(isinstance(value, str) and value.strip(), f"{label} contains an invalid entry")
        normalized.append(value)
    duplicates = sorted(
        value
        for value in set(normalized)
        if normalized.count(value) > 1
    )
    expect(not duplicates, f"{label} has duplicate entries: {', '.join(duplicates)}")


def check_relative_path(path_text: str, label: str) -> None:
    candidate = Path(path_text)
    expect(not candidate.is_absolute() and ".." not in candidate.parts, f"unsafe {label}: {path_text}")
    expect((repo_root / candidate).exists(), f"missing {label}: {path_text}")


expect(packet.get("packet") == "daxis_external_proof_packet", "invalid external proof packet id")
expect(packet.get("scope"), "scope is required")

source_docs = packet.get("sourceDocs", [])
expect(source_docs, "sourceDocs must not be empty")
expect_unique_text_list(source_docs, "sourceDocs")
for source_doc in source_docs:
    check_relative_path(source_doc, "source doc")

required_source_docs = [
    (
        "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
        "Daxis architecture ADR",
    ),
    (
        "docs/program/daxis-first-class-integration-strategy.md",
        "Daxis first-class integration strategy",
    ),
    (
        "docs/release-gates/daxis-strategy-traceability.json",
        "Daxis strategy traceability matrix",
    ),
    (
        "docs/release-gates/daxis-release-bundle-manifest.json",
        "Daxis release bundle manifest",
    ),
    (
        "docs/release-gates/daxis-production-rollout-decisions.json",
        "Daxis production rollout decisions",
    ),
    (
        "docs/release-gates/daxis-operational-readiness.json",
        "Daxis operational readiness contract",
    ),
    (
        "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
        "external blocker register",
    ),
    (
        "docs/program/daxis-external-proof-handoff.md",
        "Daxis external proof handoff",
    ),
    (
        "docs/release-gates/daxis-external-proof-attachment-template.md",
        "Daxis external proof attachment template",
    ),
    (
        "docs/release-gates/daxis-dirty-worktree-review-template.json",
        "Daxis dirty worktree review template",
    ),
]
for required_source_doc, label in required_source_docs:
    expect(
        required_source_doc in source_docs,
        f"sourceDocs must include {label}",
    )

external_proof_handoff = "docs/program/daxis-external-proof-handoff.md"
handoff_text = (repo_root / external_proof_handoff).read_text(encoding="utf-8")
for required_text in [
    "docs/release-gates/daxis-external-proof-packet.json",
    "bash tests/conformance/verify_daxis_external_proof_packet.sh",
    "docs/release-gates/daxis-external-proof-attachment-template.md",
    "proofAttachmentSchema",
    "AXON_DAXIS_PLATFORM_REPO_ROOT=/path/to/daxis-platform bash tests/conformance/verify_daxis_external_state.sh",
    "bash tests/conformance/verify_daxis_external_state.sh --json",
    "daxis.external_state.v1",
    "git rev-parse HEAD",
    "git rev-parse --abbrev-ref HEAD",
    "git remote get-url origin",
    "git status --short",
    "cargo test -p daxis-query --test contracts",
    "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query",
    "stableDefaultPromotionGate",
    "requiredReleaseEvidenceArtifactCommand",
    "currentPromotionState",
    "blocked_external_proof_required",
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
    "server_fallback",
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
    "daxis_external_state_json_sha256",
    "daxis_worktree_review_json_sha256",
    "docs/release-gates/daxis-dirty-worktree-review-template.json",
    "proofAttachmentSchema.dirtyWorktreeReviewTemplatePath",
    "proofAttachmentSchema.allowedReleaseChannels",
    "proofAttachmentSchema.allowedDaxisWorktreeStatuses",
    "proofAttachmentSchema.allowedDaxisWorktreeReviews",
    "proofAttachmentSchema.acceptedDaxisWorktreeReviews",
    "proofAttachmentSchema.checksumFormat",
    "proofAttachmentSchema.dirtyWorktreeReviewChecksumFormat",
    "proofAttachmentSchema.requiredReviewerRoles",
    "proofAttachmentSchema.requiredEnvironmentClass",
    "proofAttachmentSchema.stableDefaultValidationCommand",
    "proofAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "strict local proof validation parses helper JSON and requires attachment metadata to match its schema version, Daxis commit SHA, ref, origin remote URL, worktree status, contract-test status, and architecture-scan status",
    "requiredReleaseAttachmentSchemaFields",
    "requiredProofAttachmentSchemaFields",
    "environment_class",
    "release_channel",
    "allowedReleaseChannels",
    "allowedDaxisWorktreeStatuses",
    "allowedDaxisWorktreeReviews",
    "acceptedDaxisWorktreeReviews",
    "daxis_worktree_review_json_uri",
    "daxis_worktree_review_json_sha256",
    "axon_release_commit_sha",
    "axon_release_ref",
    "rollout_segment",
    "daxis_stable_default_release_identity_verified=true",
    "one Axon release commit",
    "one Axon release ref",
    "production-environment-scoped",
    "64-character lowercase hexadecimal digest",
    "shasum -a 256 path/to/external-state.json",
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md",
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments",
    "daxis_architecture_docs",
    "daxis_names_axon_default_browser_engine",
    "daxis_descriptor_endpoint",
    "daxis_frontend_flow",
    "daxis_read_access_plan_endpoint",
    "storage_cors_proxy_validation",
    "production_dashboards",
    "production_runbooks",
    "rollout_controls",
    "production_table_compatibility_dashboard",
]:
    expect(
        required_text in handoff_text,
        f"external proof handoff missing required text: {required_text}",
    )

daxis_verification_plan = packet.get("daxisVerificationPlan", {})
expect(isinstance(daxis_verification_plan, dict), "daxisVerificationPlan is required")
expect(daxis_verification_plan.get("purpose"), "daxisVerificationPlan purpose is required")

axon_helper_command = daxis_verification_plan.get("axonHelperCommand")
expect(
    axon_helper_command == "bash tests/conformance/verify_daxis_external_state.sh",
    "daxisVerificationPlan must name the Axon Daxis external-state helper",
)
json_summary_command = daxis_verification_plan.get("jsonSummaryCommand")
expect(
    json_summary_command == "bash tests/conformance/verify_daxis_external_state.sh --json",
    "daxisVerificationPlan must name the Axon Daxis external-state JSON summary command",
)
json_artifact_command = daxis_verification_plan.get("jsonArtifactCommand")
expect(
    json_artifact_command
    == "bash tests/conformance/verify_daxis_external_state.sh --write-json path/to/external-state.json",
    "daxisVerificationPlan must name the Axon Daxis external-state JSON artifact command",
)
external_state_helper = repo_root / "tests/conformance/verify_daxis_external_state.sh"
expect(external_state_helper.is_file(), "missing Daxis external-state helper")

active_architecture_paths = daxis_verification_plan.get("activeArchitecturePaths", [])
expect(
    isinstance(active_architecture_paths, list) and active_architecture_paths,
    "daxisVerificationPlan activeArchitecturePaths must not be empty",
)
expect_unique_text_list(active_architecture_paths, "daxisVerificationPlan activeArchitecturePaths")
for required_path in [
    "README.md",
    "CLAUDE.md",
    "docs/architecture/daxis-control-plane-architecture.md",
    "crates/daxis-query/src/lib.rs",
    "crates/daxis-query/tests/contracts.rs",
]:
    expect(
        required_path in active_architecture_paths,
        f"daxisVerificationPlan missing active architecture path: {required_path}",
    )
for path_text in active_architecture_paths:
    candidate = Path(path_text)
    expect(
        not candidate.is_absolute() and ".." not in candidate.parts,
        f"unsafe Daxis active architecture path: {path_text}",
    )

daxis_verification_commands = daxis_verification_plan.get("commands", [])
expect(
    isinstance(daxis_verification_commands, list) and daxis_verification_commands,
    "daxisVerificationPlan commands must not be empty",
)
expect_unique_text_list(daxis_verification_commands, "daxisVerificationPlan commands")
expected_daxis_verification_commands = [
    "git rev-parse HEAD",
    "git rev-parse --abbrev-ref HEAD",
    "git remote get-url origin",
    "git status --short",
    "cargo test -p daxis-query --test contracts",
    "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query",
]
for required_command in expected_daxis_verification_commands:
    expect(
        required_command in daxis_verification_commands,
        f"daxisVerificationPlan missing command: {required_command}",
    )
unsupported_daxis_verification_commands = sorted(
    set(daxis_verification_commands) - set(expected_daxis_verification_commands)
)
expect(
    not unsupported_daxis_verification_commands,
    "daxisVerificationPlan has unsupported commands: "
    + ", ".join(unsupported_daxis_verification_commands),
)
try:
    helper_commands = subprocess.check_output(
        ["bash", str(external_state_helper), "--list"],
        cwd=repo_root,
        stderr=subprocess.PIPE,
        text=True,
    ).splitlines()
except subprocess.CalledProcessError as error:
    fail(
        "Daxis external-state helper --list failed: "
        + error.stderr.strip()
    )
expect(
    helper_commands == expected_daxis_verification_commands,
    "Daxis external-state helper --list must match daxisVerificationPlan commands",
)

proof_attachment_schema = packet.get("proofAttachmentSchema", {})
expect(isinstance(proof_attachment_schema, dict), "proofAttachmentSchema is required")
expect(proof_attachment_schema.get("purpose"), "proofAttachmentSchema purpose is required")

proof_attachment_template = proof_attachment_schema.get("templatePath")
expect(
    isinstance(proof_attachment_template, str) and proof_attachment_template,
    "proofAttachmentSchema templatePath is required",
)
expect(
    proof_attachment_template in source_docs,
    "sourceDocs missing proof attachment template",
)
check_relative_path(proof_attachment_template, "proof attachment template")
template_text = (repo_root / proof_attachment_template).read_text(encoding="utf-8")
template_lines = template_text.splitlines()
expect(
    "Do not attach browser-visible secrets" in template_text,
    "proof attachment template must warn against browser-visible secrets",
)
dirty_worktree_review_template = proof_attachment_schema.get("dirtyWorktreeReviewTemplatePath")
expect(
    isinstance(dirty_worktree_review_template, str) and dirty_worktree_review_template,
    "proofAttachmentSchema dirtyWorktreeReviewTemplatePath is required",
)
expect(
    dirty_worktree_review_template in source_docs,
    "sourceDocs missing dirty worktree review template",
)
check_relative_path(dirty_worktree_review_template, "dirty worktree review template")
try:
    with open(repo_root / dirty_worktree_review_template, encoding="utf-8") as handle:
        dirty_worktree_review_template_json = json.load(handle)
except json.JSONDecodeError as error:
    fail(f"invalid dirty worktree review template JSON: {error}")
expect(
    isinstance(dirty_worktree_review_template_json, dict),
    "dirty worktree review template must be a JSON object",
)
for field in [
    "schema_version",
    "review_state",
    "reviewed_at",
    "reviewer",
    "reviewer_role",
    "rollout_segment",
    "daxis_commit_sha",
    "daxis_ref",
    "daxis_origin_remote_url",
    "daxis_worktree_status",
    "daxis_worktree_review",
    "daxis_worktree_status_lines",
    "reviewed_paths",
    "notes",
]:
    expect(field in dirty_worktree_review_template_json, f"dirty worktree review template missing field: {field}")
expect(
    dirty_worktree_review_template_json.get("schema_version") == "daxis.dirty_worktree_review.v1",
    "dirty worktree review template schema_version must be daxis.dirty_worktree_review.v1",
)
expect(
    dirty_worktree_review_template_json.get("review_state") == "accepted",
    "dirty worktree review template review_state must be accepted",
)
expect(
    dirty_worktree_review_template_json.get("daxis_worktree_status") == "dirty",
    "dirty worktree review template daxis_worktree_status must be dirty",
)
expect(
    dirty_worktree_review_template_json.get("daxis_worktree_review") == "dirty_reviewed",
    "dirty worktree review template daxis_worktree_review must be dirty_reviewed",
)
dirty_template_status_lines = dirty_worktree_review_template_json.get("daxis_worktree_status_lines", [])
expect(
    isinstance(dirty_template_status_lines, list)
    and all(isinstance(line, str) and line.strip() for line in dirty_template_status_lines),
    "dirty worktree review template daxis_worktree_status_lines must contain non-empty strings",
)
dirty_template_reviewed_paths = dirty_worktree_review_template_json.get("reviewed_paths", [])
expect(
    isinstance(dirty_template_reviewed_paths, list)
    and all(isinstance(path, str) and path.strip() for path in dirty_template_reviewed_paths),
    "dirty worktree review template reviewed_paths must contain non-empty strings",
)
for reviewed_path in ["path/to/modified-file", "path/to/untracked-file"]:
    expect(
        reviewed_path in dirty_template_reviewed_paths,
        f"dirty worktree review template missing reviewed path: {reviewed_path}",
    )
for required_text in [
    "Use the matching guidance for `item_id`.",
    "Attach Daxis external-state helper output for the rollout segment before accepting any item.",
    "Set `environment_class` to `production`; development or staging proof cannot satisfy stable default routing.",
    "Set `release_channel` to the Daxis rollout channel that the proof supports.",
    "The only allowed `release_channel` values are `experimental`, `integration`, `candidate`, and `stable`; stable default promotion requires `stable`.",
    "Set `axon_release_commit_sha` and `axon_release_ref` to the exact Axon release commit and branch or tag under review.",
    "Helper output must include the Daxis commit SHA, branch or detached-ref label, origin remote URL, and working-tree status from `git rev-parse HEAD`, `git rev-parse --abbrev-ref HEAD`, `git remote get-url origin`, and `git status --short`.",
    "Attach the helper JSON summary generated by `bash tests/conformance/verify_daxis_external_state.sh --json`.",
    "To write an attachment-ready artifact and print its digest, run `bash tests/conformance/verify_daxis_external_state.sh --write-json path/to/external-state.json`.",
    "The helper JSON summary must use `schema_version` `daxis.external_state.v1`.",
    "Attach the SHA-256 digest of that helper JSON summary in `daxis_external_state_json_sha256`.",
    "Record only the 64-character lowercase hexadecimal digest generated from the exact helper JSON bytes, for example with `shasum -a 256 path/to/external-state.json`.",
    "Strict local proof validation parses helper JSON and requires attachment metadata to match its schema version, Daxis commit SHA, ref, origin remote URL, worktree status, contract-test status, and architecture-scan status.",
    "Classify the Daxis working-tree review as `clean`, `dirty_reviewed`, or `dirty_rejected`; do not accept proof from a dirty Daxis checkout without owner review that ties every modified or untracked path to the rollout segment.",
    "Set `daxis_worktree_status` to the helper JSON `daxis_worktree_status` value: `clean` or `dirty`.",
    "Set `daxis_worktree_review` to `clean`, `dirty_reviewed`, or `dirty_rejected`; stable default promotion accepts only `clean` or `dirty_reviewed`.",
    "When `daxis_worktree_status` is `dirty`, attach a `daxis.dirty_worktree_review.v1` JSON artifact in `daxis_worktree_review_json_uri` and its exact SHA-256 digest in `daxis_worktree_review_json_sha256`.",
    "Use `docs/release-gates/daxis-dirty-worktree-review-template.json` as the starting shape for the dirty review artifact.",
    "That review JSON must use `review_state` `accepted`, must match the helper JSON Daxis commit SHA, ref, origin remote URL, worktree status, worktree review, and `daxis_worktree_status_lines`, and must list each dirty path in `reviewed_paths`.",
    "Before stable default promotion, validate a completed proof attachment with `bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md`.",
    "Before stable default promotion, validate the completed proof attachment set with `bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments`.",
    "Daxis architecture doc links or commit SHAs",
    "Daxis browser read-compute terminology diff",
    "Daxis product architecture reference naming Axon as the default browser read engine",
    "release-channel or rollout approval",
    "POST /v1/query/delta/snapshot-descriptor",
    "expiry, snapshot mismatch, path escape, and malformed descriptor negative test output",
    "request and correlation ID propagation logs",
    "Daxis frontend open/query/cancel test output",
    "worker command capture showing no credential profile identifiers or secrets",
    "POST /v1/catalog/read-access-plan",
    "policy-denied and policy-unknown test output",
    "production XML endpoint CORS validation output",
    "URL redaction evidence for logs and UI events",
    "dashboard URLs",
    "oncall ownership and escalation policy",
    "rollout configuration schema",
    "server fallback routing proof",
    "compatibility dashboard URL",
    "blocked or fallback table-class list",
    "Daxis product owner",
    "Daxis platform owner",
    "Daxis catalog/storage owner",
    "Daxis web platform owner",
    "Daxis security owner",
    "Daxis SRE owner",
    "Axon runtime / engine owner",
]:
    expect(
        required_text in template_text,
        f"proof attachment template missing required guidance: {required_text}",
    )

required_metadata = proof_attachment_schema.get("requiredMetadata", [])
expect(
    isinstance(required_metadata, list) and required_metadata,
    "proofAttachmentSchema requiredMetadata must not be empty",
)
expect_unique_text_list(required_metadata, "proofAttachmentSchema requiredMetadata")
for metadata_field in [
    "item_id",
    "milestone",
    "owner",
    "captured_at",
    "environment",
    "environment_class",
    "release_channel",
    "axon_release_commit_sha",
    "axon_release_ref",
    "rollout_segment",
    "artifact_uri",
    "verification_command_or_url",
    "exit_status_or_review_status",
    "rollback_evidence_uri",
    "daxis_commit_sha",
    "daxis_ref",
    "daxis_origin_remote_url",
    "daxis_worktree_status",
    "daxis_worktree_review",
    "daxis_external_state_schema_version",
    "daxis_external_state_json_uri",
    "daxis_external_state_json_sha256",
    "daxis_worktree_review_json_uri",
    "daxis_worktree_review_json_sha256",
]:
    expect(
        metadata_field in required_metadata,
        f"proofAttachmentSchema missing metadata field: {metadata_field}",
    )
    expect(
        metadata_field in template_text,
        f"proof attachment template missing metadata field: {metadata_field}",
    )

expect(
    proof_attachment_schema.get("requiredEnvironmentClass") == "production",
    "proofAttachmentSchema requiredEnvironmentClass must be production",
)
expect(
    "production" in template_text and "environment_class" in template_text,
    "proof attachment template missing production environment_class guidance",
)

expected_release_channels = ["experimental", "integration", "candidate", "stable"]
allowed_release_channels = proof_attachment_schema.get("allowedReleaseChannels", [])
expect_unique_text_list(allowed_release_channels, "proofAttachmentSchema allowedReleaseChannels")
expect(
    allowed_release_channels == expected_release_channels,
    "proofAttachmentSchema allowedReleaseChannels must define experimental, integration, candidate, stable",
)
for release_channel in expected_release_channels:
    expect(
        release_channel in template_text,
        f"proof attachment template missing allowed release channel: {release_channel}",
    )

expected_worktree_statuses = ["clean", "dirty"]
allowed_worktree_statuses = proof_attachment_schema.get("allowedDaxisWorktreeStatuses", [])
expect_unique_text_list(allowed_worktree_statuses, "proofAttachmentSchema allowedDaxisWorktreeStatuses")
expect(
    allowed_worktree_statuses == expected_worktree_statuses,
    "proofAttachmentSchema allowedDaxisWorktreeStatuses must define clean and dirty helper states",
)

expected_worktree_reviews = ["clean", "dirty_reviewed", "dirty_rejected"]
allowed_worktree_reviews = proof_attachment_schema.get("allowedDaxisWorktreeReviews", [])
expect_unique_text_list(allowed_worktree_reviews, "proofAttachmentSchema allowedDaxisWorktreeReviews")
expect(
    allowed_worktree_reviews == expected_worktree_reviews,
    "proofAttachmentSchema allowedDaxisWorktreeReviews must define clean, dirty_reviewed, dirty_rejected",
)

accepted_worktree_reviews = proof_attachment_schema.get("acceptedDaxisWorktreeReviews", [])
expect_unique_text_list(accepted_worktree_reviews, "proofAttachmentSchema acceptedDaxisWorktreeReviews")
expect(
    accepted_worktree_reviews == ["clean", "dirty_reviewed"],
    "proofAttachmentSchema acceptedDaxisWorktreeReviews must accept clean and dirty_reviewed only",
)

checksum_format = proof_attachment_schema.get("checksumFormat", {})
expect(isinstance(checksum_format, dict), "proofAttachmentSchema checksumFormat is required")
expected_checksum_format = {
    "field": "daxis_external_state_json_sha256",
    "algorithm": "sha256",
    "encoding": "lowercase_hex",
    "length": 64,
    "sourceBytes": "exact_helper_json_bytes",
}
for key, expected_value in expected_checksum_format.items():
    expect(
        checksum_format.get(key) == expected_value,
        f"proofAttachmentSchema checksumFormat {key} must be {expected_value}",
    )

dirty_worktree_review_checksum_format = proof_attachment_schema.get("dirtyWorktreeReviewChecksumFormat", {})
expect(
    isinstance(dirty_worktree_review_checksum_format, dict),
    "proofAttachmentSchema dirtyWorktreeReviewChecksumFormat is required",
)
expected_dirty_worktree_review_checksum_format = {
    "field": "daxis_worktree_review_json_sha256",
    "algorithm": "sha256",
    "encoding": "lowercase_hex",
    "length": 64,
    "sourceBytes": "exact_dirty_worktree_review_json_bytes",
}
for key, expected_value in expected_dirty_worktree_review_checksum_format.items():
    expect(
        dirty_worktree_review_checksum_format.get(key) == expected_value,
        f"proofAttachmentSchema dirtyWorktreeReviewChecksumFormat {key} must be {expected_value}",
    )

expected_stable_default_validation_command = (
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default path/to/completed-proof-attachment.md"
)
expected_stable_default_directory_validation_command = (
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --stable-default-dir path/to/completed-proof-attachments"
)
expected_stable_default_artifact_validation_command = (
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default path/to/completed-proof-attachment.md"
)
expected_stable_default_artifact_directory_validation_command = (
    "bash tests/conformance/verify_daxis_external_proof_attachment.sh --artifact-root path/to/artifacts --require-local-artifacts --stable-default-dir path/to/completed-proof-attachments"
)
expected_stable_default_promotion_packet_validation_command = (
    "bash tests/conformance/verify_daxis_stable_default_promotion_packet.sh --artifact-root path/to/artifacts --release-attachments path/to/completed-release-attachments --proof-attachments path/to/completed-proof-attachments --release-evidence-log path/to/release-evidence.log --release-evidence-sha256 <sha256> --release-evidence-exit-status 0"
)
expect(
    proof_attachment_schema.get("stableDefaultValidationCommand")
    == expected_stable_default_validation_command,
    "proofAttachmentSchema stableDefaultValidationCommand must name the stable-default proof attachment validator",
)
expect(
    proof_attachment_schema.get("stableDefaultDirectoryValidationCommand")
    == expected_stable_default_directory_validation_command,
    "proofAttachmentSchema stableDefaultDirectoryValidationCommand must name the stable-default proof attachment directory validator",
)
expect(
    proof_attachment_schema.get("stableDefaultArtifactValidationCommand")
    == expected_stable_default_artifact_validation_command,
    "proofAttachmentSchema stableDefaultArtifactValidationCommand must name the stable-default proof attachment artifact validator",
)
expect(
    proof_attachment_schema.get("stableDefaultArtifactDirectoryValidationCommand")
    == expected_stable_default_artifact_directory_validation_command,
    "proofAttachmentSchema stableDefaultArtifactDirectoryValidationCommand must name the stable-default proof attachment artifact directory validator",
)
expect(
    (repo_root / "tests/conformance/verify_daxis_external_proof_attachment.sh").is_file(),
    "missing stable-default proof attachment validator",
)
expect(
    (repo_root / "tests/conformance/verify_daxis_stable_default_promotion_packet.sh").is_file(),
    "missing stable-default promotion packet validator",
)
expect(
    (repo_root / "tests/conformance/verify_daxis_stable_default_promotion_packet_test.sh").is_file(),
    "missing stable-default promotion packet validator regression test",
)
expect(
    expected_stable_default_validation_command in template_text,
    "proof attachment template missing stable-default validation command",
)
expect(
    expected_stable_default_directory_validation_command in template_text,
    "proof attachment template missing stable-default directory validation command",
)
expect(
    expected_stable_default_artifact_validation_command in template_text,
    "proof attachment template missing stable-default artifact validation command",
)
expect(
    expected_stable_default_artifact_directory_validation_command in template_text,
    "proof attachment template missing stable-default artifact directory validation command",
)
expect(
    expected_stable_default_validation_command in handoff_text,
    "external proof handoff missing stable-default validation command",
)
expect(
    expected_stable_default_directory_validation_command in handoff_text,
    "external proof handoff missing stable-default directory validation command",
)
expect(
    expected_stable_default_artifact_validation_command in handoff_text,
    "external proof handoff missing stable-default artifact validation command",
)
expect(
    expected_stable_default_artifact_directory_validation_command in handoff_text,
    "external proof handoff missing stable-default artifact directory validation command",
)
expect(
    expected_stable_default_promotion_packet_validation_command in handoff_text,
    "external proof handoff missing stable-default promotion packet validation command",
)

required_review_states = proof_attachment_schema.get("requiredReviewStates", [])
expect_unique_text_list(required_review_states, "proofAttachmentSchema requiredReviewStates")
for review_state in ["attached", "reviewed", "accepted", "rejected"]:
    expect(
        review_state in required_review_states,
        f"proofAttachmentSchema missing review state: {review_state}",
    )
    expect(
        review_state in template_text,
        f"proof attachment template missing review state: {review_state}",
    )

required_reviewer_roles = proof_attachment_schema.get("requiredReviewerRoles", [])
expected_reviewer_roles = [
    "Daxis product owner",
    "Daxis platform owner",
    "Daxis catalog/storage owner",
    "Daxis web platform owner",
    "Daxis security owner",
    "Daxis SRE owner",
    "Axon runtime / engine owner",
]
expect_unique_text_list(required_reviewer_roles, "proofAttachmentSchema requiredReviewerRoles")
expect(
    required_reviewer_roles == expected_reviewer_roles,
    "proofAttachmentSchema requiredReviewerRoles must match the external proof attachment review table",
)
for reviewer_role in expected_reviewer_roles:
    expect(
        reviewer_role in template_text,
        f"proof attachment template missing reviewer role: {reviewer_role}",
    )

external_items = packet.get("externalItems", [])
expect(external_items, "externalItems must not be empty")
external_item_keys = [
    f"{item.get('milestone')}.{item.get('id')}"
    for item in external_items
]
duplicate_external_item_keys = sorted(
    item_key
    for item_key in set(external_item_keys)
    if item_key != "None.None" and external_item_keys.count(item_key) > 1
)
expect(
    not duplicate_external_item_keys,
    f"duplicate external proof items: {', '.join(duplicate_external_item_keys)}",
)
item_ids = {item.get("id") for item in external_items}
external_item_by_id = {
    item.get("id"): item
    for item in external_items
    if isinstance(item, dict) and item.get("id")
}
for required in [
    "daxis_architecture_docs",
    "daxis_names_axon_default_browser_engine",
    "daxis_descriptor_endpoint",
    "daxis_frontend_flow",
    "daxis_read_access_plan_endpoint",
    "storage_cors_proxy_validation",
    "production_dashboards",
    "production_runbooks",
    "rollout_controls",
    "production_table_compatibility_dashboard",
]:
    expect(required in item_ids, f"missing external proof item: {required}")
    expect(required in template_text, f"proof attachment template missing external proof item: {required}")
for item in external_items:
    item_id = item.get("id")
    milestone = item.get("milestone")
    expect(
        any(f"`{item_id}`" in line and f"| {milestone}" in line for line in template_lines),
        f"proof attachment template missing milestone {milestone} for external proof item: {item_id}",
    )

stable_default_gate = packet.get("stableDefaultPromotionGate", {})
expect(isinstance(stable_default_gate, dict), "stableDefaultPromotionGate is required")
expect(stable_default_gate.get("purpose"), "stableDefaultPromotionGate purpose is required")
expect(
    stable_default_gate.get("currentPromotionState") == "blocked_external_proof_required",
    "stableDefaultPromotionGate currentPromotionState must remain blocked_external_proof_required until accepted external proof is attached",
)

required_release_process_attachments = stable_default_gate.get("requiredReleaseProcessAttachments", [])
expect(
    isinstance(required_release_process_attachments, list) and required_release_process_attachments,
    "stableDefaultPromotionGate requiredReleaseProcessAttachments must not be empty",
)
expect(
    len(required_release_process_attachments) == len(set(required_release_process_attachments)),
    "stableDefaultPromotionGate requiredReleaseProcessAttachments must be unique",
)
for item_id in ["git_sha", "worker_artifact_size", "public_gcs_live_smoke", "release_notes", "migration_notes"]:
    expect(
        item_id in required_release_process_attachments,
        f"stableDefaultPromotionGate missing release-process attachment: {item_id}",
    )

release_bundle_manifest_path = repo_root / "docs/release-gates/daxis-release-bundle-manifest.json"
with open(release_bundle_manifest_path, encoding="utf-8") as handle:
    release_bundle_manifest = json.load(handle)
operational_readiness_path = repo_root / "docs/release-gates/daxis-operational-readiness.json"
with open(operational_readiness_path, encoding="utf-8") as handle:
    operational_readiness = json.load(handle)
operational_artifact_command = (
    operational_readiness.get("releaseEvidenceAutomation", {}).get("artifactCommand")
)
expect(
    operational_artifact_command
    == stable_default_gate.get("requiredReleaseEvidenceArtifactCommand"),
    "operational readiness release evidence artifact command must match stableDefaultPromotionGate",
)
expect(
    operational_artifact_command
    == release_bundle_manifest.get("releaseEvidenceArtifactCommand"),
    "operational readiness release evidence artifact command must match release bundle manifest",
)
release_process_items = {
    item["id"]
    for item in release_bundle_manifest.get("bundleItems", [])
    if item.get("id") and item.get("status") == "release_process_required"
}
expect(release_process_items, "release bundle manifest has no release-process items")

release_attachment_schema_items = set(
    release_bundle_manifest.get("releaseAttachmentSchema", {}).get("releaseProcessItemIds", [])
)
expect(
    release_attachment_schema_items == release_process_items,
    "release bundle manifest releaseAttachmentSchema must match release-process bundle items",
)

stable_gate_release_items = set(required_release_process_attachments)
missing_release_process_items = sorted(release_process_items - stable_gate_release_items)
extra_release_process_items = sorted(stable_gate_release_items - release_process_items)
expect(
    not missing_release_process_items,
    f"stableDefaultPromotionGate missing release-process item ids from bundle manifest: {', '.join(missing_release_process_items)}",
)
expect(
    not extra_release_process_items,
    f"stableDefaultPromotionGate lists release-process item ids not in bundle manifest: {', '.join(extra_release_process_items)}",
)

required_external_proof_item_ids = stable_default_gate.get("requiredExternalProofItemIds", [])
expect(
    isinstance(required_external_proof_item_ids, list) and required_external_proof_item_ids,
    "stableDefaultPromotionGate requiredExternalProofItemIds must not be empty",
)
expect(
    len(required_external_proof_item_ids) == len(set(required_external_proof_item_ids)),
    "stableDefaultPromotionGate requiredExternalProofItemIds must be unique",
)
missing_gate_items = sorted(item_ids - set(required_external_proof_item_ids))
extra_gate_items = sorted(set(required_external_proof_item_ids) - item_ids)
expect(
    not missing_gate_items,
    f"stableDefaultPromotionGate missing external proof item ids: {', '.join(missing_gate_items)}",
)
expect(
    not extra_gate_items,
    f"stableDefaultPromotionGate lists unknown external proof item ids: {', '.join(extra_gate_items)}",
)
expect(
    stable_default_gate.get("requiredReviewState") == "accepted",
    "stableDefaultPromotionGate requiredReviewState must be accepted",
)
expect(
    "accepted" in required_review_states,
    "proofAttachmentSchema must include accepted review state for stableDefaultPromotionGate",
)
expect(
    stable_default_gate.get("requiredReleaseEvidenceCommand") == "bash tests/conformance/verify_daxis_release_evidence.sh",
    "stableDefaultPromotionGate must require the full Daxis release evidence runner",
)
expect(
    stable_default_gate["requiredReleaseEvidenceCommand"] in release_bundle_manifest.get("releaseEvidenceCommands", []),
    "release bundle manifest must list the stableDefaultPromotionGate release evidence command",
)
expected_release_evidence_artifact_command = (
    "bash tests/conformance/verify_daxis_release_evidence.sh --write-log path/to/release-evidence.log"
)
expect(
    stable_default_gate.get("requiredReleaseEvidenceArtifactCommand")
    == expected_release_evidence_artifact_command,
    "stableDefaultPromotionGate must require the digest-pinned release evidence artifact writer",
)
expect(
    release_bundle_manifest.get("releaseEvidenceArtifactCommand")
    == stable_default_gate["requiredReleaseEvidenceArtifactCommand"],
    "release bundle manifest artifact command must match the external proof gate",
)
expect(
    stable_default_gate.get("stableDefaultPromotionPacketValidationCommand")
    == expected_stable_default_promotion_packet_validation_command,
    "stableDefaultPromotionGate must name the stable-default promotion packet validator",
)
expect(
    release_bundle_manifest.get("stableDefaultPromotionPacketValidationCommand")
    == expected_stable_default_promotion_packet_validation_command,
    "release bundle manifest stable-default promotion packet command must match the external proof gate",
)
expect(
    stable_default_gate.get("requiredReleaseChannel") == "stable",
    "stableDefaultPromotionGate must require stable release channel",
)
expect(
    stable_default_gate.get("requiredRollbackState") == "server_fallback",
    "stableDefaultPromotionGate must require server_fallback rollback",
)

expected_release_attachment_schema_fields = [
    "artifact_sha256",
    "release_channel",
    "rollout_segment",
    "releaseAttachmentSchema.allowedReleaseChannels",
    "releaseAttachmentSchema.checksumFormat",
    "releaseAttachmentSchema.requiredReviewerRoles",
    "releaseAttachmentSchema.stableDefaultValidationCommand",
    "releaseAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactValidationCommand",
    "releaseAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand",
]
required_release_attachment_schema_fields = stable_default_gate.get("requiredReleaseAttachmentSchemaFields", [])
expect_unique_text_list(
    required_release_attachment_schema_fields,
    "stableDefaultPromotionGate requiredReleaseAttachmentSchemaFields",
)
expect(
    required_release_attachment_schema_fields == expected_release_attachment_schema_fields,
    "stableDefaultPromotionGate requiredReleaseAttachmentSchemaFields mismatch",
)
release_attachment_schema = release_bundle_manifest.get("releaseAttachmentSchema", {})
expect(
    "artifact_sha256" in release_attachment_schema.get("requiredMetadata", []),
    "stableDefaultPromotionGate release bundle schema missing artifact_sha256 metadata",
)
expect(
    "release_channel" in release_attachment_schema.get("requiredMetadata", []),
    "stableDefaultPromotionGate release bundle schema missing release_channel metadata",
)
expect(
    "rollout_segment" in release_attachment_schema.get("requiredMetadata", []),
    "stableDefaultPromotionGate release bundle schema missing rollout_segment metadata",
)
expect(
    release_attachment_schema.get("allowedReleaseChannels") == expected_release_channels,
    "stableDefaultPromotionGate release bundle schema missing allowed release-channel policy",
)
expect(
    isinstance(release_attachment_schema.get("checksumFormat"), dict),
    "stableDefaultPromotionGate release bundle schema missing checksumFormat",
)
expect(
    isinstance(release_attachment_schema.get("requiredReviewerRoles"), list)
    and release_attachment_schema["requiredReviewerRoles"],
    "stableDefaultPromotionGate release bundle schema missing requiredReviewerRoles",
)
for field_name in [
    "stableDefaultValidationCommand",
    "stableDefaultDirectoryValidationCommand",
    "stableDefaultArtifactValidationCommand",
    "stableDefaultArtifactDirectoryValidationCommand",
]:
    expect(
        isinstance(release_attachment_schema.get(field_name), str)
        and release_attachment_schema[field_name],
        f"stableDefaultPromotionGate release bundle schema missing {field_name}",
    )

expected_proof_attachment_schema_fields = [
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
    "proofAttachmentSchema.stableDefaultValidationCommand",
    "proofAttachmentSchema.stableDefaultDirectoryValidationCommand",
    "proofAttachmentSchema.stableDefaultArtifactValidationCommand",
    "proofAttachmentSchema.stableDefaultArtifactDirectoryValidationCommand",
]
required_proof_attachment_schema_fields = stable_default_gate.get("requiredProofAttachmentSchemaFields", [])
expect_unique_text_list(
    required_proof_attachment_schema_fields,
    "stableDefaultPromotionGate requiredProofAttachmentSchemaFields",
)
expect(
    required_proof_attachment_schema_fields == expected_proof_attachment_schema_fields,
    "stableDefaultPromotionGate requiredProofAttachmentSchemaFields mismatch",
)
expect(
    "daxis_external_state_json_sha256" in required_metadata,
    "stableDefaultPromotionGate proof schema missing daxis_external_state_json_sha256 metadata",
)
expect(
    "daxis_worktree_review_json_sha256" in required_metadata,
    "stableDefaultPromotionGate proof schema missing daxis_worktree_review_json_sha256 metadata",
)
expect(
    "release_channel" in required_metadata,
    "stableDefaultPromotionGate proof schema missing release_channel metadata",
)
expect(
    "environment_class" in required_metadata,
    "stableDefaultPromotionGate proof schema missing environment_class metadata",
)
expect(
    "axon_release_commit_sha" in required_metadata,
    "stableDefaultPromotionGate proof schema missing axon_release_commit_sha metadata",
)
expect(
    "axon_release_ref" in required_metadata,
    "stableDefaultPromotionGate proof schema missing axon_release_ref metadata",
)
expect(
    "daxis_worktree_status" in required_metadata,
    "stableDefaultPromotionGate proof schema missing daxis_worktree_status metadata",
)
expect(
    "daxis_worktree_review" in required_metadata,
    "stableDefaultPromotionGate proof schema missing daxis_worktree_review metadata",
)
expect(
    proof_attachment_schema.get("allowedReleaseChannels") == expected_release_channels,
    "stableDefaultPromotionGate proof schema missing allowed release-channel policy",
)
expect(
    proof_attachment_schema.get("allowedDaxisWorktreeStatuses") == expected_worktree_statuses,
    "stableDefaultPromotionGate proof schema missing allowed Daxis worktree status policy",
)
expect(
    proof_attachment_schema.get("allowedDaxisWorktreeReviews") == expected_worktree_reviews,
    "stableDefaultPromotionGate proof schema missing allowed Daxis worktree review policy",
)
expect(
    proof_attachment_schema.get("acceptedDaxisWorktreeReviews") == ["clean", "dirty_reviewed"],
    "stableDefaultPromotionGate proof schema missing accepted Daxis worktree review policy",
)
expect(
    proof_attachment_schema.get("requiredEnvironmentClass") == "production",
    "stableDefaultPromotionGate proof schema must require production environment_class",
)
expect(
    isinstance(proof_attachment_schema.get("checksumFormat"), dict),
    "stableDefaultPromotionGate proof schema missing checksumFormat",
)
expect(
    isinstance(proof_attachment_schema.get("dirtyWorktreeReviewChecksumFormat"), dict),
    "stableDefaultPromotionGate proof schema missing dirtyWorktreeReviewChecksumFormat",
)
expect(
    isinstance(proof_attachment_schema.get("requiredReviewerRoles"), list)
    and proof_attachment_schema["requiredReviewerRoles"],
    "stableDefaultPromotionGate proof schema missing requiredReviewerRoles",
)
for field_name in [
    "stableDefaultValidationCommand",
    "stableDefaultDirectoryValidationCommand",
    "stableDefaultArtifactValidationCommand",
    "stableDefaultArtifactDirectoryValidationCommand",
]:
    expect(
        isinstance(proof_attachment_schema.get(field_name), str)
        and proof_attachment_schema[field_name],
        f"stableDefaultPromotionGate proof schema missing {field_name}",
    )

blocker_register = stable_default_gate.get("blockerRegister")
expect(
    blocker_register == "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
    "stableDefaultPromotionGate must name the external blocker register",
)
expect(
    blocker_register in source_docs,
    "sourceDocs missing stableDefaultPromotionGate blocker register",
)
check_relative_path(blocker_register, "stable default blocker register")
blocker_register_text = (repo_root / blocker_register).read_text(encoding="utf-8")
blocker_register_lines = blocker_register_text.splitlines()
for required_text in [
    "stableDefaultPromotionGate",
    "blocked_external_proof_required",
    "accepted",
    "server_fallback",
]:
    expect(
        required_text in blocker_register_text,
        f"stable default blocker register missing required text: {required_text}",
    )
for item_id in required_external_proof_item_ids:
    expect(
        item_id in blocker_register_text,
        f"stable default blocker register missing external proof item: {item_id}",
    )
    milestone = external_item_by_id[item_id].get("milestone")
    expect(
        any(f"`{item_id}`" in line and f"| {milestone}" in line for line in blocker_register_lines),
        f"stable default blocker register missing milestone {milestone} for external proof item: {item_id}",
    )

rollout_decisions_path = repo_root / "docs/release-gates/daxis-production-rollout-decisions.json"
with open(rollout_decisions_path, encoding="utf-8") as handle:
    rollout_register = json.load(handle)
expect(
    rollout_register.get("register") == "daxis_production_rollout_decisions",
    "rollout decision register id mismatch",
)
rollout_stable_default_gate = (
    rollout_register.get("decisions", {})
    .get("releaseChannels", {})
    .get("stableDefaultGate", {})
)
expect(
    isinstance(rollout_stable_default_gate, dict) and rollout_stable_default_gate,
    "rollout decision register missing stableDefaultGate",
)
expect(
    rollout_stable_default_gate.get("externalProofPacket")
    == "docs/release-gates/daxis-external-proof-packet.json",
    "rollout stableDefaultGate must link the external proof packet",
)
expect(
    rollout_stable_default_gate.get("releaseBundleManifest")
    == "docs/release-gates/daxis-release-bundle-manifest.json",
    "rollout stableDefaultGate must link the release bundle manifest",
)
for field in [
    "requiredReleaseProcessAttachments",
    "requiredExternalProofItemIds",
    "requiredReviewState",
    "requiredReleaseEvidenceCommand",
    "requiredReleaseEvidenceArtifactCommand",
    "requiredReleaseChannel",
    "requiredRollbackState",
    "currentPromotionState",
    "blockerRegister",
]:
    expect(
        rollout_stable_default_gate.get(field) == stable_default_gate.get(field),
        f"rollout stableDefaultGate {field} must match stableDefaultPromotionGate",
    )

traceability_path = repo_root / "docs/release-gates/daxis-strategy-traceability.json"
with open(traceability_path, encoding="utf-8") as handle:
    matrix = json.load(handle)
external_traceability_by_key = {
    f"{milestone}.{entry['id']}": entry
    for milestone, payload in matrix.get("milestones", {}).items()
    for entry in payload.get("deliverables", []) + payload.get("exitCriteria", [])
    if entry.get("status") == "external_required"
}
external_traceability_items = set(external_traceability_by_key)
packet_items = {f"{item.get('milestone')}.{item.get('id')}" for item in external_items}
missing_from_packet = sorted(external_traceability_items - packet_items)
extra_in_packet = sorted(packet_items - external_traceability_items)
expect(
    not missing_from_packet,
    f"external proof packet missing traceability items: {', '.join(missing_from_packet)}",
)
expect(
    not extra_in_packet,
    f"external proof packet has items not in traceability matrix: {', '.join(extra_in_packet)}",
)

for item in external_items:
    item_id = item.get("id")
    item_key = f"{item.get('milestone')}.{item_id}"
    expect(item_id, "external item missing id")
    expect(item.get("milestone") in {"M0", "M1", "M2", "M3", "M4"}, f"{item_id} has invalid milestone")
    expect(item.get("owner"), f"{item_id} missing owner")
    traceability_entry = external_traceability_by_key.get(item_key, {})
    expect(
        item.get("owner") == traceability_entry.get("externalOwner"),
        f"{item_key} owner must match traceability externalOwner",
    )
    expect(item.get("requiredProofArtifacts"), f"{item_id} missing required proof artifacts")
    expect(item.get("acceptanceChecks"), f"{item_id} missing acceptance checks")
    expect(item.get("rollbackEvidence"), f"{item_id} missing rollback evidence")
    axon_references = item.get("axonReferences", [])
    expect(axon_references, f"{item_id} missing Axon references")
    for evidence_path in axon_references:
        check_relative_path(evidence_path, f"Axon reference for {item_id}")

handoff = packet.get("handoffChecklist", [])
expect_unique_text_list(handoff, "handoffChecklist")
for required in [
    "Attach Axon release evidence output.",
    "Attach Daxis external-state helper output.",
    "Attach Daxis service endpoint test results.",
    "Attach Daxis rollout-control proof.",
    "Attach Daxis dashboard and oncall proof.",
    "Confirm server fallback rollback path.",
    "Confirm stableDefaultPromotionGate accepted state.",
]:
    expect(required in handoff, f"missing handoff checklist item: {required}")

print("Daxis external proof packet verified")
PY
