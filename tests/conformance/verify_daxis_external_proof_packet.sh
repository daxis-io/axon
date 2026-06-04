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
    "git rev-parse HEAD",
    "git rev-parse --abbrev-ref HEAD",
    "git remote get-url origin",
    "git status --short",
    "cargo test -p daxis-query --test contracts",
    "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query",
    "stableDefaultPromotionGate",
    "currentPromotionState",
    "blocked_external_proof_required",
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "server_fallback",
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
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
expect(
    "Do not attach browser-visible secrets" in template_text,
    "proof attachment template must warn against browser-visible secrets",
)
for required_text in [
    "Use the matching guidance for `item_id`.",
    "Attach Daxis external-state helper output for the rollout segment before accepting any item.",
    "Helper output must include the Daxis commit SHA, branch or detached-ref label, origin remote URL, and working-tree status from `git rev-parse HEAD`, `git rev-parse --abbrev-ref HEAD`, `git remote get-url origin`, and `git status --short`.",
    "Classify the Daxis working-tree status as `clean`, `dirty_reviewed`, or `dirty_rejected`; do not accept proof from a dirty Daxis checkout without owner review that ties every modified or untracked path to the rollout segment.",
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
]:
    expect(
        metadata_field in required_metadata,
        f"proofAttachmentSchema missing metadata field: {metadata_field}",
    )
    expect(
        metadata_field in template_text,
        f"proof attachment template missing metadata field: {metadata_field}",
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
expect(
    stable_default_gate.get("requiredRollbackState") == "server_fallback",
    "stableDefaultPromotionGate must require server_fallback rollback",
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
