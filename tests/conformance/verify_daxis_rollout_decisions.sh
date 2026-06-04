#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_ROLLOUT_REPO_ROOT:-$(pwd)}"
decision_file="${AXON_DAXIS_ROLLOUT_DECISION_FILE:-docs/release-gates/daxis-production-rollout-decisions.json}"

decision_path() {
  if [[ "$decision_file" = /* ]]; then
    printf "%s\n" "$decision_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$decision_file"
}

path="$(decision_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis rollout decision register: $decision_file" >&2
  exit 1
fi

python3 - "$repo_root" "$path" <<'PY'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
path = Path(sys.argv[2])
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def expect_unique(values: list[str], field: str) -> None:
    seen = set()
    for value in values:
        expect(value not in seen, f"{field} contains duplicate entry: {value}")
        seen.add(value)


def resolve_repo_path(value: str, field: str) -> Path:
    candidate = Path(value)
    expect(not candidate.is_absolute() and ".." not in candidate.parts, f"unsafe {field} path: {value}")
    resolved = repo_root / candidate
    expect(resolved.is_file(), f"missing {field}: {value}")
    return resolved


expect(register.get("register") == "daxis_production_rollout_decisions", "invalid rollout decision register id")

source_docs = register.get("sourceDocs", [])
expect_unique(source_docs, "sourceDocs")
for source_doc in source_docs:
    resolve_repo_path(source_doc, "source doc")
for required_source in [
    "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/program/daxis-operational-maturity.md",
    "docs/release-gates/daxis-browser-datafusion-budget-profile.json",
    "docs/release-gates/daxis-contract-artifacts.sha256",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/release-gates/daxis-operational-readiness.json",
    "docs/release-gates/daxis-release-bundle-manifest.json",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
]:
    expect(required_source in source_docs, f"sourceDocs missing {required_source}")

decisions = register.get("decisions", {})
for key in [
    "endpointNames",
    "initialAccessMode",
    "signedUrlTtlAndRefresh",
    "descriptorRefreshSnapshotPolicy",
    "browserBudgets",
    "contractVersioning",
    "releaseChannels",
    "initialTableEligibility",
    "dashboardAlertThresholds",
]:
    expect(key in decisions, f"missing rollout decision: {key}")
    expect(decisions[key].get("status") in {"decided", "external_signoff_required"}, f"invalid status for {key}")
    expect(decisions[key].get("evidence"), f"missing evidence for {key}")
    if decisions[key].get("status") == "external_signoff_required":
        expect(decisions[key].get("externalOwner"), f"{key} missing external owner")

endpoints = decisions["endpointNames"]
expect(endpoints.get("descriptorResolver") == "POST /v1/query/delta/snapshot-descriptor", "descriptor endpoint mismatch")
expect(endpoints.get("readAccessPlan") == "POST /v1/catalog/read-access-plan", "read-access-plan endpoint mismatch")
expect(endpoints.get("objectGrantRoutes") == [
    "POST /object-grants/{grantId}/list",
    "POST /object-grants/{grantId}/head",
    "POST /object-grants/{grantId}/batch-sign",
    "GET /object-grants/{grantId}/range",
], "object grant route set mismatch")

access = decisions["initialAccessMode"]
expect(access.get("default") == "signed_url", "initial access mode should prefer signed_url")
expect("proxy" in access.get("allowedModes", []), "proxy must remain an allowed access mode")
for fallback_mode in ["sql_fallback_required", "server_fallback", "blocked"]:
    expect(fallback_mode in access.get("fallbackModes", []), f"{fallback_mode} must remain a fallback mode")
expect("fallback/block states" in access.get("decision", ""), "initial access decision must preserve fallback/block states")

ttl = decisions["signedUrlTtlAndRefresh"]
expect(ttl.get("status") == "external_signoff_required", "TTL policy requires external signoff")
expect(ttl.get("sameSnapshotRequired") is True, "refresh must preserve the same snapshot")
expect(isinstance(ttl.get("candidateTtlSeconds"), int) and ttl["candidateTtlSeconds"] > 0, "candidate TTL must be positive")
expect(
    isinstance(ttl.get("refreshLeadTimeSeconds"), int)
    and 0 < ttl["refreshLeadTimeSeconds"] < ttl["candidateTtlSeconds"],
    "refresh lead time must be positive and less than candidate TTL",
)

budgets = decisions["browserBudgets"]
for field in ["maxScanBytes", "maxOutputIpcBytes", "maxRowsReturned"]:
    expect(isinstance(budgets.get(field), int) and budgets[field] > 0, f"budget {field} must be positive")
expect(budgets.get("maxBatchesInFlight") == 1, "maxBatchesInFlight should stay at 1")

versioning = decisions["contractVersioning"]
expect("sha256_manifest" in versioning.get("mechanisms", []), "contract versioning must include sha256_manifest")
expect("json_schema" in versioning.get("mechanisms", []), "contract versioning must include json_schema")
expect("openapi" in versioning.get("mechanisms", []), "contract versioning must include openapi")

channels = decisions["releaseChannels"].get("channels", [])
expect(channels == ["experimental", "integration", "candidate", "stable"], "release channel order mismatch")
promotion_rule = decisions["releaseChannels"].get("promotionRule", "")
for phrase in ["passing release evidence", "no broadened browser trust boundary", "Daxis-owned rollout controls"]:
    expect(phrase in promotion_rule, f"release promotion rule missing: {phrase}")
stable_default_gate = decisions["releaseChannels"].get("stableDefaultGate", {})
expect(isinstance(stable_default_gate, dict) and stable_default_gate, "release channels missing stableDefaultGate")
expect(
    stable_default_gate.get("externalProofPacket") == "docs/release-gates/daxis-external-proof-packet.json",
    "stableDefaultGate must link the external proof packet",
)
expect(
    stable_default_gate.get("releaseBundleManifest") == "docs/release-gates/daxis-release-bundle-manifest.json",
    "stableDefaultGate must link the release bundle manifest",
)
expected_release_process_attachments = [
    "git_sha",
    "worker_artifact_size",
    "public_gcs_live_smoke",
    "release_notes",
    "migration_notes",
]
required_release_process_attachments = stable_default_gate.get("requiredReleaseProcessAttachments", [])
expect_unique(required_release_process_attachments, "stableDefaultGate requiredReleaseProcessAttachments")
expect(
    required_release_process_attachments == expected_release_process_attachments,
    "stableDefaultGate requiredReleaseProcessAttachments mismatch",
)
expected_external_proof_item_ids = [
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
]
required_external_proof_item_ids = stable_default_gate.get("requiredExternalProofItemIds", [])
expect_unique(required_external_proof_item_ids, "stableDefaultGate requiredExternalProofItemIds")
expect(
    required_external_proof_item_ids == expected_external_proof_item_ids,
    "stableDefaultGate requiredExternalProofItemIds mismatch",
)
expect(stable_default_gate.get("requiredReviewState") == "accepted", "stableDefaultGate requires accepted review state")
expect(
    stable_default_gate.get("requiredReleaseEvidenceCommand") == "bash tests/conformance/verify_daxis_release_evidence.sh",
    "stableDefaultGate must require full release evidence",
)
expect(stable_default_gate.get("requiredRollbackState") == "server_fallback", "stableDefaultGate must require server_fallback rollback")
expect(
    stable_default_gate.get("blockerRegister") == "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
    "stableDefaultGate must link the external blocker register",
)
expect(
    stable_default_gate.get("currentPromotionState") == "blocked_external_proof_required",
    "stableDefaultGate currentPromotionState must remain blocked_external_proof_required until accepted external proof is attached",
)
resolve_repo_path(stable_default_gate["blockerRegister"], "stableDefaultGate blockerRegister")

release_bundle_path = resolve_repo_path(stable_default_gate["releaseBundleManifest"], "stableDefaultGate releaseBundleManifest")
with open(release_bundle_path, encoding="utf-8") as handle:
    release_bundle_manifest = json.load(handle)
expect(
    release_bundle_manifest.get("manifest") == "daxis_release_bundle_manifest",
    "stableDefaultGate release bundle manifest id mismatch",
)
expect(
    stable_default_gate["requiredReleaseEvidenceCommand"] in release_bundle_manifest.get("releaseEvidenceCommands", []),
    "stableDefaultGate release evidence command missing from release bundle manifest",
)

external_proof_path = resolve_repo_path(stable_default_gate["externalProofPacket"], "stableDefaultGate externalProofPacket")
with open(external_proof_path, encoding="utf-8") as handle:
    external_proof_packet = json.load(handle)
expect(
    external_proof_packet.get("packet") == "daxis_external_proof_packet",
    "stableDefaultGate external proof packet id mismatch",
)
external_proof_gate = external_proof_packet.get("stableDefaultPromotionGate", {})
expect(
    isinstance(external_proof_gate, dict) and external_proof_gate,
    "stableDefaultGate external proof packet missing stableDefaultPromotionGate",
)
expect(
    external_proof_gate.get("requiredReviewState") == stable_default_gate["requiredReviewState"],
    "stableDefaultGate external proof review state mismatch",
)
expect(
    external_proof_gate.get("requiredReleaseProcessAttachments")
    == stable_default_gate["requiredReleaseProcessAttachments"],
    "stableDefaultGate external proof release-process attachment mismatch",
)
expect(
    external_proof_gate.get("requiredExternalProofItemIds")
    == stable_default_gate["requiredExternalProofItemIds"],
    "stableDefaultGate external proof item id mismatch",
)
expect(
    external_proof_gate.get("requiredReleaseEvidenceCommand")
    == stable_default_gate["requiredReleaseEvidenceCommand"],
    "stableDefaultGate external proof release evidence command mismatch",
)
expect(
    external_proof_gate.get("requiredRollbackState") == stable_default_gate["requiredRollbackState"],
    "stableDefaultGate external proof rollback state mismatch",
)
expect(
    external_proof_gate.get("currentPromotionState") == stable_default_gate["currentPromotionState"],
    "stableDefaultGate external proof current promotion state mismatch",
)
expect(
    external_proof_gate.get("blockerRegister") == stable_default_gate["blockerRegister"],
    "stableDefaultGate external proof blocker register mismatch",
)

eligibility = decisions["initialTableEligibility"]
for field in ["delta_or_parquet", "no_row_filters", "no_column_masks", "no_governed_views", "browser_supported_delta_features"]:
    expect(field in eligibility.get("requiredTraits", []), f"missing initial table eligibility trait: {field}")

thresholds = decisions["dashboardAlertThresholds"]
for field in ["fallbackRateWarnPercent", "fallbackRatePagePercent", "workerStartupFailurePageCount", "corsFailurePageCount"]:
    expect(isinstance(thresholds.get(field), int) and thresholds[field] >= 0, f"threshold {field} must be a non-negative integer")

print("Daxis rollout decision register verified")
PY
