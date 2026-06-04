#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_RELEASE_BUNDLE_REPO_ROOT:-$(pwd)}"
manifest_file="${AXON_DAXIS_RELEASE_BUNDLE_MANIFEST_FILE:-docs/release-gates/daxis-release-bundle-manifest.json}"

manifest_path() {
  if [[ "$manifest_file" = /* ]]; then
    printf "%s\n" "$manifest_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$manifest_file"
}

path="$(manifest_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis release bundle manifest: $manifest_file" >&2
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
    manifest = json.load(handle)


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


expect(manifest.get("manifest") == "daxis_release_bundle_manifest", "invalid Daxis release bundle manifest id")
expect(manifest.get("strategy") == "docs/program/daxis-first-class-integration-strategy.md", "unexpected strategy path")
check_relative_path(manifest["strategy"], "strategy document")

source_docs = manifest.get("sourceDocs", [])
expect(source_docs, "sourceDocs must not be empty")
expect_unique_text_list(source_docs, "sourceDocs")
for required_source in [
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/program/browser-datafusion-runtime-parity.md",
    "docs/program/browser-delta-compatibility-matrix.md",
    "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
    "docs/release-gates/daxis-production-rollout-decisions.json",
    "docs/release-gates/daxis-operational-readiness.json",
    "docs/release-gates/daxis-strategy-traceability.json",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/release-gates/daxis-release-attachment-template.md",
    "docs/release-gates/daxis-release-notes-template.md",
    "docs/release-gates/daxis-release-migration-notes-template.md",
]:
    expect(required_source in source_docs, f"sourceDocs missing {required_source}")
for source_doc in source_docs:
    check_relative_path(source_doc, "source doc")

release_evidence_doc = "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md"
expect(
    release_evidence_doc in source_docs,
    "sourceDocs missing release evidence document",
)
release_evidence_text = (repo_root / release_evidence_doc).read_text(encoding="utf-8")
for required_text in [
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "docs/release-gates/daxis-release-bundle-manifest.json",
    "docs/release-gates/daxis-release-attachment-template.md",
    "release_process_required",
    "stableDefaultPromotionGate",
    "cargo test -p wasm-datafusion-poc --test daxis_query_corpus",
    "bash tests/conformance/verify_daxis_query_corpus_coverage.sh",
    "cargo test -p wasm-datafusion-session",
    "bash tests/conformance/verify_browser_worker_dependency_boundary.sh",
    "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm",
    "bash tests/conformance/verify_daxis_pr_checklist.sh",
    "bash tests/perf/report_datafusion_wasm_size_test.sh",
    "npm exec -- playwright test --config=playwright.config.ts --grep \"Daxis descriptor-resolver|preserves cancellation errors|surfaces unsupported feature errors\"",
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
    "AXON_LIVE_PUBLIC_GCS_TABLE_URI",
    "docs/release-gates/daxis-release-migration-notes-template.md",
    "breaking-change plans or no-breaking-change statements",
    "external proof packet status",
    "currentPromotionState",
]:
    expect(
        required_text in release_evidence_text,
        f"release evidence document missing required text: {required_text}",
    )

release_evidence_runner = repo_root / "tests/conformance/verify_daxis_release_evidence.sh"
expect(release_evidence_runner.is_file(), "missing Daxis release evidence runner")
try:
    listed_release_commands = subprocess.check_output(
        ["bash", str(release_evidence_runner), "--list"],
        cwd=repo_root,
        stderr=subprocess.PIPE,
        text=True,
    ).splitlines()
except subprocess.CalledProcessError as error:
    fail(
        "Daxis release evidence runner --list failed: "
        + error.stderr.strip()
    )

for listed_command in listed_release_commands:
    command = listed_command.strip()
    if not command:
        continue
    expect(
        command in release_evidence_text,
        f"release evidence document missing listed runner command: {command}",
    )
listed_release_command_set = {
    command.strip()
    for command in listed_release_commands
    if command.strip()
}

bundle_items = manifest.get("bundleItems", [])
expect(bundle_items, "bundleItems must not be empty")
bundle_item_ids = [item.get("id") for item in bundle_items]
duplicate_item_ids = sorted(
    item_id
    for item_id in set(bundle_item_ids)
    if item_id and bundle_item_ids.count(item_id) > 1
)
expect(
    not duplicate_item_ids,
    f"duplicate release bundle item ids: {', '.join(duplicate_item_ids)}",
)
item_by_id = {item.get("id"): item for item in bundle_items}
required_item_ids = [
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
for item_id in required_item_ids:
    expect(item_id in item_by_id, f"missing release bundle item: {item_id}")

release_attachment_schema = manifest.get("releaseAttachmentSchema", {})
expect(isinstance(release_attachment_schema, dict), "releaseAttachmentSchema is required")
expect(release_attachment_schema.get("purpose"), "releaseAttachmentSchema purpose is required")

release_attachment_template = release_attachment_schema.get("templatePath")
expect(
    isinstance(release_attachment_template, str) and release_attachment_template,
    "releaseAttachmentSchema templatePath is required",
)
expect(
    release_attachment_template in source_docs,
    "sourceDocs missing release attachment template",
)
check_relative_path(release_attachment_template, "release attachment template")
template_text = (repo_root / release_attachment_template).read_text(encoding="utf-8")
expect(
    "Do not attach browser-visible secrets" in template_text,
    "release attachment template must warn against browser-visible secrets",
)
for required_text in [
    "Use the matching guidance for `item_id`.",
    "Attach the exact Axon release commit SHA.",
    "Attach the branch or tag name in `release_ref`.",
    "release packet URI, commit permalink, or tag permalink",
    "rollback or prior-release reference",
    "AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh",
    "optional size toolchain is unavailable",
    "why size evidence is deferred and where the blocker is tracked",
    "AXON_LIVE_PUBLIC_GCS_TABLE_URI=gs://... npm run test:browser:public-gcs-live -- --reporter=line",
    "skip-safe output and the external blocker record",
    "fixture ownership, runner identity, and CI variable provisioning",
    "docs/release-gates/daxis-release-notes-template.md",
    "query-result semantics",
    "fallback behavior",
    "supported SQL and Delta feature claims",
    "descriptor validation",
    "public error taxonomy",
    "external proof packet status",
    "currentPromotionState",
    "docs/release-gates/daxis-release-migration-notes-template.md",
    "Include external proof packet status and stable default promotion state.",
    "no-breaking-change statement",
    "Release owner",
    "Runtime / engine owner",
    "Daxis product owner",
    "Daxis query platform owner",
    "Daxis catalog/storage owner",
    "Daxis security owner",
    "Daxis SRE owner",
]:
    expect(
        required_text in template_text,
        f"release attachment template missing required guidance: {required_text}",
    )

release_process_item_ids = release_attachment_schema.get("releaseProcessItemIds", [])
expect(
    isinstance(release_process_item_ids, list) and release_process_item_ids,
    "releaseAttachmentSchema releaseProcessItemIds must not be empty",
)
expect(
    len(release_process_item_ids) == len(set(release_process_item_ids)),
    "releaseAttachmentSchema releaseProcessItemIds must be unique",
)
for item_id in ["git_sha", "worker_artifact_size", "public_gcs_live_smoke", "release_notes", "migration_notes"]:
    expect(
        item_id in release_process_item_ids,
        f"releaseAttachmentSchema missing release-process item id: {item_id}",
    )
    expect(
        item_id in template_text,
        f"release attachment template missing release-process item id: {item_id}",
    )

required_metadata = release_attachment_schema.get("requiredMetadata", [])
expect(
    isinstance(required_metadata, list) and required_metadata,
    "releaseAttachmentSchema requiredMetadata must not be empty",
)
for metadata_field in [
    "item_id",
    "release_commit_sha",
    "release_ref",
    "owner",
    "captured_at",
    "artifact_uri",
    "verification_command_or_statement",
    "exit_status_or_review_status",
    "rollback_or_migration_note_uri",
]:
    expect(
        metadata_field in required_metadata,
        f"releaseAttachmentSchema missing metadata field: {metadata_field}",
    )
    expect(
        metadata_field in template_text,
        f"release attachment template missing metadata field: {metadata_field}",
    )

required_review_states = release_attachment_schema.get("requiredReviewStates", [])
for review_state in ["attached", "reviewed", "accepted", "rejected"]:
    expect(
        review_state in required_review_states,
        f"releaseAttachmentSchema missing review state: {review_state}",
    )
    expect(
        review_state in template_text,
        f"release attachment template missing review state: {review_state}",
    )

git_sha = item_by_id["git_sha"]
expect(git_sha.get("status") == "release_process_required", "git_sha must remain release-process evidence")
git_sha_attachment = git_sha.get("releaseAttachment", "").lower()
expect("commit sha" in git_sha_attachment, "git_sha release attachment must name the release commit SHA")
expect("branch or tag" in git_sha_attachment, "git_sha release attachment must name the release branch or tag")

daxis_default_worker_size_command = "AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh"
worker_artifact_size = item_by_id["worker_artifact_size"]
expect(worker_artifact_size.get("status") == "release_process_required", "worker_artifact_size must remain release-process evidence")
expect(
    daxis_default_worker_size_command in worker_artifact_size.get("releaseAttachment", ""),
    "worker_artifact_size release attachment must name the Daxis default-worker size command",
)

public_gcs_live_smoke = item_by_id["public_gcs_live_smoke"]
public_gcs_attachment = public_gcs_live_smoke.get("releaseAttachment", "")
public_gcs_attachment_lower = public_gcs_attachment.lower()
expect(public_gcs_live_smoke.get("status") == "release_process_required", "public_gcs_live_smoke must remain release-process evidence")
expect(
    "AXON_LIVE_PUBLIC_GCS_TABLE_URI" in public_gcs_attachment,
    "public_gcs_live_smoke release attachment must name AXON_LIVE_PUBLIC_GCS_TABLE_URI",
)
expect(
    "npm run test:browser:public-gcs-live -- --reporter=line" in public_gcs_attachment,
    "public_gcs_live_smoke release attachment must name the public GCS live smoke command",
)
expect(
    "skip-safe" in public_gcs_attachment_lower,
    "public_gcs_live_smoke release attachment must name the skip-safe fallback output",
)
expect(
    "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md" in public_gcs_live_smoke.get("evidence", []),
    "public_gcs_live_smoke evidence must include the release evidence doc",
)
expect(
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md" in public_gcs_live_smoke.get("evidence", []),
    "public_gcs_live_smoke evidence must include the external blocker register",
)

release_notes_template = "docs/release-gates/daxis-release-notes-template.md"
expect(
    release_notes_template in source_docs,
    "sourceDocs missing Daxis release notes template",
)
check_relative_path(release_notes_template, "release notes template")
release_notes_template_text = (repo_root / release_notes_template).read_text(encoding="utf-8")
for required_text in [
    "Do not attach browser-visible secrets",
    "## Release Identity",
    "Axon commit SHA:",
    "Branch or tag:",
    "Release channel:",
    "Release owner:",
    "Daxis rollout decision link:",
    "Migration notes link:",
    "## Compatibility Summary",
    "This Axon release does not change Daxis-facing query results, fallback behavior, supported SQL claims, Delta feature handling, descriptor validation, or public error taxonomy.",
    "## Required Release Note Sections",
    "### Query Result Semantics",
    "### Query Metrics And Observability",
    "Daxis result metrics",
    "`rows_returned`, `arrow_ipc_bytes`, `scan_bytes`, `duration_ms`, `files_touched`, `files_skipped`, `row_groups_touched`, `row_groups_skipped`, `footer_reads`, `snapshot_bootstrap_duration_ms`, and `access_mode`",
    "Daxis dashboard or telemetry action required",
    "### Fallback Behavior",
    "### Supported SQL And Delta Features",
    "### Descriptor Validation",
    "### Error And Fallback Taxonomy",
    "### Runtime Budgets And Worker Artifact",
    "### Security And Browser Trust Boundary",
    "## Required Evidence",
    "Daxis release evidence runner:",
    "Daxis release bundle manifest verifier:",
    "Daxis compatibility notes:",
    "External proof packet status:",
    "Stable default promotion state:",
    "Migration notes or no-breaking-change statement:",
    "Browser matrix result when worker behavior changed:",
    "Contract artifact verifier when public contracts changed:",
    "## Owner Signoff",
    "Release owner:",
    "Runtime / engine owner:",
    "Daxis product owner:",
    "Daxis query platform owner:",
    "Daxis catalog/storage owner:",
    "Daxis security owner:",
    "Daxis SRE owner:",
]:
    expect(
        required_text in release_notes_template_text,
        f"release notes template missing required text: {required_text}",
    )

migration_template = "docs/release-gates/daxis-release-migration-notes-template.md"
expect(
    migration_template in source_docs,
    "sourceDocs missing Daxis release migration notes template",
)
check_relative_path(migration_template, "release migration notes template")
migration_template_text = (repo_root / migration_template).read_text(encoding="utf-8")
for required_text in [
    "Do not attach browser-visible secrets",
    "## Release Identity",
    "Axon commit SHA:",
    "Branch or tag:",
    "Release channel:",
    "Release owner:",
    "Daxis rollout decision link:",
    "## Compatibility Classification",
    "No Daxis-facing breaking change.",
    "Breaking Daxis-facing change with migration required before rollout.",
    "This Axon release does not introduce breaking changes to Daxis-facing contracts, runtime semantics, fallback vocabulary, compatibility claims, browser artifact selection, or rollout requirements.",
    "## Change Inventory",
    "Public Rust contracts and JSON Schema",
    "TypeScript SDK examples and worker envelopes",
    "Descriptor resolver or read-access-plan behavior",
    "Object-grant route envelopes or audit fixtures",
    "Fallback, block, or runtime-error vocabulary",
    "Supported SQL, Delta features, or table eligibility",
    "Runtime budgets, worker artifact, or default worker SKU",
    "Browser dependency, credential, or signing guardrails",
    "Release channel, rollout controls, or external blockers",
    "## Required Evidence",
    "Contract artifact verifier:",
    "Daxis release evidence runner:",
    "Browser matrix result:",
    "Worker artifact size output:",
    "Daxis production rollout decision:",
    "External proof packet status:",
    "Stable default promotion state:",
    "## Migration Plan",
    "Affected Daxis services, surfaces, or owners:",
    "Old contract or runtime behavior:",
    "New contract or runtime behavior:",
    "Required Daxis code or configuration changes:",
    "Backward compatibility window:",
    "Staged rollout sequence:",
    "Required dashboard or alert updates:",
    "User or operator communication:",
    "Rollback plan:",
    "## Owner Signoff",
    "Release owner:",
    "Runtime / engine owner:",
    "Daxis product owner:",
    "Daxis query platform owner:",
    "Daxis catalog/storage owner:",
    "Daxis security owner:",
    "Daxis SRE owner:",
]:
    expect(
        required_text in migration_template_text,
        f"release migration notes template missing required text: {required_text}",
    )

migration_notes = item_by_id["migration_notes"]
expect(migration_notes.get("status") == "release_process_required", "migration_notes must remain release-process evidence")
migration_notes_attachment = migration_notes.get("releaseAttachment", "")
for required_text in [
    migration_template,
    "external proof packet status",
    "stableDefaultPromotionGate",
    "currentPromotionState",
]:
    expect(
        required_text in migration_notes_attachment,
        f"migration_notes release attachment missing required guidance: {required_text}",
    )
expect(
    migration_template in migration_notes.get("evidence", []),
    "migration_notes evidence must include the Daxis migration notes template",
)

release_notes = item_by_id["release_notes"]
expect(release_notes.get("status") == "release_process_required", "release_notes must remain release-process evidence")
release_notes_attachment = release_notes.get("releaseAttachment", "")
for required_text in [
    release_notes_template,
    "query results",
    "Daxis result metrics and observability fields",
    "fallback behavior",
    "SQL and Delta support",
    "descriptor validation",
    "error taxonomy",
    "external proof packet status",
    "currentPromotionState",
]:
    expect(
        required_text in release_notes_attachment,
        f"release_notes release attachment missing required guidance: {required_text}",
    )
expect(
    release_notes_template in release_notes.get("evidence", []),
    "release_notes evidence must include the Daxis release notes template",
)

daxis_default_worker_guardrail_command = "env AXON_BROWSER_DEPENDENCY_PACKAGE=axon-web-wasm bash tests/security/verify_browser_dependency_guardrails.sh target/wasm32-unknown-unknown/release/axon_web_wasm.wasm"
rust_test_summary = item_by_id["rust_test_summary"]
expect(
    "cargo fmt --check" in rust_test_summary.get("verificationCommands", []),
    "rust_test_summary verification commands must include Rust formatting",
)
expect(
    "cargo test -p query-router -p native-query-runtime -p delta-control-plane -p wasm-query-runtime -p wasm-query-session -p wasm-http-object-store -p wasm-parquet-engine -p wasm-delta-snapshot -p browser-sdk -p browser-engine-worker"
    in rust_test_summary.get("verificationCommands", []),
    "rust_test_summary verification commands must include host tests for router, runtime, control-plane, browser crates, SDK, and worker",
)

dependency_guardrail_result = item_by_id["dependency_guardrail_result"]
expect(
    daxis_default_worker_guardrail_command in dependency_guardrail_result.get("verificationCommands", []),
    "dependency_guardrail_result verification commands must include the Daxis default-worker dependency guardrail",
)

supported_sql_delta_feature_matrix = item_by_id["supported_sql_delta_feature_matrix"]
expect(
    "cargo test -p wasm-datafusion-poc --test daxis_query_corpus"
    in supported_sql_delta_feature_matrix.get("verificationCommands", []),
    "supported_sql_delta_feature_matrix verification commands must include the Daxis query corpus runtime test",
)
expect(
    "bash tests/conformance/verify_daxis_query_corpus_coverage.sh"
    in supported_sql_delta_feature_matrix.get("verificationCommands", []),
    "supported_sql_delta_feature_matrix verification commands must include Daxis query corpus compatibility coverage",
)
expect(
    "tests/conformance/verify_daxis_query_corpus_coverage.sh"
    in supported_sql_delta_feature_matrix.get("evidence", []),
    "supported_sql_delta_feature_matrix evidence must include the Daxis query corpus coverage verifier",
)

daxis_compatibility_notes = item_by_id["daxis_compatibility_notes"]
expect(
    "bash tests/conformance/verify_daxis_strategy_document.sh"
    in daxis_compatibility_notes.get("verificationCommands", []),
    "daxis_compatibility_notes verification commands must include the Daxis strategy document verifier",
)
expect(
    "bash tests/conformance/verify_daxis_architecture_adr.sh"
    in daxis_compatibility_notes.get("verificationCommands", []),
    "daxis_compatibility_notes verification commands must include the Daxis architecture ADR verifier",
)

external_blockers = item_by_id["external_blockers"]
expect(
    "bash tests/conformance/verify_daxis_external_state_test.sh"
    in external_blockers.get("verificationCommands", []),
    "external_blockers verification commands must include the Daxis external-state helper regression",
)
for required_evidence in [
    "tests/conformance/verify_daxis_external_state.sh",
    "tests/conformance/verify_daxis_external_state_test.sh",
]:
    expect(
        required_evidence in external_blockers.get("evidence", []),
        f"external_blockers evidence must include the Daxis external-state helper path: {required_evidence}",
    )

for item in bundle_items:
    item_id = item.get("id")
    expect(item_id, "release bundle item missing id")
    expect(item.get("status") in {"repo_verified", "release_process_required", "external_required"}, f"{item_id} has invalid status")
    expect(item.get("owner"), f"{item_id} missing owner")
    expect(item.get("description"), f"{item_id} missing description")
    evidence = item.get("evidence", [])
    expect(evidence, f"{item_id} missing evidence")
    for evidence_path in evidence:
        check_relative_path(evidence_path, f"evidence for {item_id}")
    if item.get("status") == "repo_verified":
        expect(item.get("verificationCommands"), f"{item_id} missing verification commands")
    for command in item.get("verificationCommands", []):
        expect(
            isinstance(command, str) and command.strip(),
            f"{item_id} has invalid verification command",
        )
        expect(
            command in listed_release_command_set,
            f"{item_id} verification command is not listed by the Daxis release evidence runner: {command}",
        )
    if item.get("status") == "release_process_required":
        expect(item.get("releaseAttachment"), f"{item_id} missing release attachment")
    if item.get("status") == "external_required":
        expect(item.get("externalOwner"), f"{item_id} missing external owner")
        expect(item.get("externalProof"), f"{item_id} missing external proof")

release_process_items = {
    item["id"]
    for item in bundle_items
    if item.get("id") and item.get("status") == "release_process_required"
}
schema_release_process_items = set(release_process_item_ids)
missing_schema_items = sorted(release_process_items - schema_release_process_items)
stale_schema_items = sorted(schema_release_process_items - release_process_items)
expect(
    not missing_schema_items,
    f"releaseAttachmentSchema missing release-process item ids: {', '.join(missing_schema_items)}",
)
expect(
    not stale_schema_items,
    f"releaseAttachmentSchema lists non-release-process item ids: {', '.join(stale_schema_items)}",
)

external_proof_packet_path = repo_root / "docs/release-gates/daxis-external-proof-packet.json"
with open(external_proof_packet_path, encoding="utf-8") as handle:
    external_proof_packet = json.load(handle)
expect(
    external_proof_packet.get("packet") == "daxis_external_proof_packet",
    "external proof packet id mismatch",
)
stable_default_gate = external_proof_packet.get("stableDefaultPromotionGate", {})
expect(
    isinstance(stable_default_gate, dict) and stable_default_gate,
    "external proof packet missing stableDefaultPromotionGate",
)
expect(
    stable_default_gate.get("requiredReleaseProcessAttachments") == release_process_item_ids,
    "external proof stableDefaultPromotionGate release-process attachments must match releaseAttachmentSchema",
)
expect(
    stable_default_gate.get("requiredReviewState") == "accepted",
    "external proof stableDefaultPromotionGate must require accepted review state",
)
expect(
    stable_default_gate.get("requiredReleaseEvidenceCommand")
    == "bash tests/conformance/verify_daxis_release_evidence.sh",
    "external proof stableDefaultPromotionGate must require the full release evidence runner",
)
expect(
    stable_default_gate.get("requiredReleaseEvidenceCommand") in manifest.get("releaseEvidenceCommands", []),
    "releaseEvidenceCommands must include the external proof stableDefaultPromotionGate release evidence command",
)
expect(
    stable_default_gate.get("requiredRollbackState") == "server_fallback",
    "external proof stableDefaultPromotionGate must require server_fallback rollback",
)
expect(
    stable_default_gate.get("blockerRegister") == "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
    "external proof stableDefaultPromotionGate must link the external blocker register",
)
expect(
    stable_default_gate["blockerRegister"] in source_docs,
    "sourceDocs missing external proof stableDefaultPromotionGate blocker register",
)

release_commands = manifest.get("releaseEvidenceCommands", [])
expect_unique_text_list(release_commands, "releaseEvidenceCommands")
expected_release_commands = [
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
]
for required_command in expected_release_commands:
    expect(required_command in release_commands, f"releaseEvidenceCommands missing {required_command}")
unexpected_release_commands = sorted(set(release_commands) - set(expected_release_commands))
expect(
    not unexpected_release_commands,
    f"releaseEvidenceCommands has unsupported commands: {', '.join(unexpected_release_commands)}",
)

print("Daxis release bundle manifest verified")
PY
