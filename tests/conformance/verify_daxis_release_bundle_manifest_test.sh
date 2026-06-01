#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
manifest="$repo_root/docs/release-gates/daxis-release-bundle-manifest.json"

mkdir -p "$repo_root/docs/program" "$repo_root/docs/release-gates" "$repo_root/tests/conformance"
for doc in \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/release-gates/daxis-release-migration-notes-template.md \
  docs/release-gates/browser-wasm-delta-gcs-release-evidence.md \
  docs/release-gates/browser-wasm-delta-gcs-external-blockers.md \
  docs/release-gates/daxis-production-rollout-decisions.json \
  docs/release-gates/daxis-operational-readiness.json \
  docs/program/browser-datafusion-runtime-parity.md \
  docs/program/browser-delta-compatibility-matrix.md \
  tests/conformance/verify_daxis_release_evidence.sh; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_manifest() {
  python3 - "$manifest" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
item_ids = [
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
items = []
for item_id in item_ids:
    status = "repo_verified"
    item = {
        "id": item_id,
        "status": status,
        "owner": "Runtime / engine team",
        "description": f"{item_id} release bundle fixture",
        "evidence": ["docs/program/daxis-first-class-integration-strategy.md"],
        "verificationCommands": ["bash tests/conformance/verify_daxis_release_evidence.sh --list"],
    }
    if item_id == "git_sha":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Attach the release commit SHA and branch or tag name."
    if item_id == "worker_artifact_size":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Attach output from AXON_DF_SIZE_PACKAGE=axon-web-wasm AXON_DF_SIZE_WASM_STEM=axon_web_wasm AXON_DF_BROTLI_BUDGET_BYTES=6291456 bash tests/perf/report_datafusion_wasm_size.sh."
    if item_id == "migration_notes":
        item["status"] = "release_process_required"
        item.pop("verificationCommands")
        item["releaseAttachment"] = "Use docs/release-gates/daxis-release-migration-notes-template.md to attach migration notes or an explicit no-breaking-change statement."
        item["evidence"] = [
            "docs/program/daxis-first-class-integration-strategy.md",
            "docs/release-gates/daxis-release-migration-notes-template.md",
        ]
    if item_id == "external_blockers":
        item["evidence"] = ["docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"]
    items.append(item)

manifest = {
    "manifest": "daxis_release_bundle_manifest",
    "strategy": "docs/program/daxis-first-class-integration-strategy.md",
    "sourceDocs": [
        "docs/program/daxis-first-class-integration-strategy.md",
        "docs/release-gates/browser-wasm-delta-gcs-release-evidence.md",
        "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md",
        "docs/release-gates/daxis-production-rollout-decisions.json",
        "docs/release-gates/daxis-operational-readiness.json",
    ],
    "bundleItems": items,
    "releaseEvidenceCommands": [
        "bash tests/conformance/verify_daxis_release_evidence.sh --list",
        "bash tests/conformance/verify_daxis_release_bundle_manifest.sh",
    ],
}
path.parent.mkdir(parents=True, exist_ok=True)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
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
manifest["bundleItems"][1]["status"] = "claimed_complete"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle)
PY

if verify_fixture; then
  echo "expected invalid bundle item status to be rejected" >&2
  exit 1
fi

echo "Daxis release bundle manifest verifier regression coverage passed"
