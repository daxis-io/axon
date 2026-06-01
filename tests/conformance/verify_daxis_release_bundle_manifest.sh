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


def check_relative_path(path_text: str, label: str) -> None:
    candidate = Path(path_text)
    expect(not candidate.is_absolute() and ".." not in candidate.parts, f"unsafe {label}: {path_text}")
    expect((repo_root / candidate).exists(), f"missing {label}: {path_text}")


expect(manifest.get("manifest") == "daxis_release_bundle_manifest", "invalid Daxis release bundle manifest id")
expect(manifest.get("strategy") == "docs/program/daxis-first-class-integration-strategy.md", "unexpected strategy path")
check_relative_path(manifest["strategy"], "strategy document")

source_docs = manifest.get("sourceDocs", [])
expect(source_docs, "sourceDocs must not be empty")
for required_source in [
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
    "docs/release-gates/daxis-production-rollout-decisions.json",
    "docs/release-gates/daxis-operational-readiness.json",
]:
    expect(required_source in source_docs, f"sourceDocs missing {required_source}")
for source_doc in source_docs:
    check_relative_path(source_doc, "source doc")

bundle_items = manifest.get("bundleItems", [])
expect(bundle_items, "bundleItems must not be empty")
item_by_id = {item.get("id"): item for item in bundle_items}
required_item_ids = [
    "git_sha",
    "contract_versions_schema_hashes",
    "rust_test_summary",
    "typescript_test_summary",
    "wasm_target_checks",
    "browser_matrix_result",
    "worker_artifact_size",
    "dependency_guardrail_result",
    "known_fallback_reasons",
    "supported_sql_delta_feature_matrix",
    "daxis_compatibility_notes",
    "migration_notes",
    "external_blockers",
]
for item_id in required_item_ids:
    expect(item_id in item_by_id, f"missing release bundle item: {item_id}")

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

migration_template = "docs/release-gates/daxis-release-migration-notes-template.md"
migration_notes = item_by_id["migration_notes"]
expect(migration_notes.get("status") == "release_process_required", "migration_notes must remain release-process evidence")
expect(
    migration_template in migration_notes.get("releaseAttachment", ""),
    "migration_notes release attachment must name the Daxis migration notes template",
)
expect(
    migration_template in migration_notes.get("evidence", []),
    "migration_notes evidence must include the Daxis migration notes template",
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
    if item.get("status") == "release_process_required":
        expect(item.get("releaseAttachment"), f"{item_id} missing release attachment")
    if item.get("status") == "external_required":
        expect(item.get("externalOwner"), f"{item_id} missing external owner")
        expect(item.get("externalProof"), f"{item_id} missing external proof")

release_commands = manifest.get("releaseEvidenceCommands", [])
for required_command in [
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
]:
    expect(required_command in release_commands, f"releaseEvidenceCommands missing {required_command}")

print("Daxis release bundle manifest verified")
PY
