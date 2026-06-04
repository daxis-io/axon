#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
register="$repo_root/docs/release-gates/daxis-production-rollout-decisions.json"

for doc in \
  docs/adr/ADR-0008-daxis-browser-read-compute-contract.md \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/program/daxis-operational-maturity.md \
  docs/release-gates/daxis-browser-datafusion-budget-profile.json \
  docs/release-gates/daxis-contract-artifacts.sha256 \
  docs/release-gates/daxis-external-proof-packet.json \
  docs/release-gates/daxis-operational-readiness.json \
  docs/release-gates/daxis-release-bundle-manifest.json \
  docs/release-gates/browser-wasm-delta-gcs-external-blockers.md; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_release_bundle_manifest() {
  cat >"$repo_root/docs/release-gates/daxis-release-bundle-manifest.json" <<'JSON'
{
  "manifest": "daxis_release_bundle_manifest",
  "releaseEvidenceCommands": [
    "bash tests/conformance/verify_daxis_release_evidence.sh",
    "bash tests/conformance/verify_daxis_release_evidence.sh --list",
    "bash tests/conformance/verify_daxis_release_bundle_manifest.sh"
  ]
}
JSON
}

write_valid_external_proof_packet() {
  cat >"$repo_root/docs/release-gates/daxis-external-proof-packet.json" <<'JSON'
{
  "packet": "daxis_external_proof_packet",
  "stableDefaultPromotionGate": {
    "requiredReleaseProcessAttachments": [
      "git_sha",
      "worker_artifact_size",
      "public_gcs_live_smoke",
      "release_notes",
      "migration_notes"
    ],
    "requiredExternalProofItemIds": [
      "daxis_architecture_docs",
      "daxis_names_axon_default_browser_engine",
      "daxis_descriptor_endpoint",
      "daxis_frontend_flow",
      "daxis_read_access_plan_endpoint",
      "storage_cors_proxy_validation",
      "production_dashboards",
      "production_runbooks",
      "rollout_controls",
      "production_table_compatibility_dashboard"
    ],
    "requiredReviewState": "accepted",
    "requiredReleaseEvidenceCommand": "bash tests/conformance/verify_daxis_release_evidence.sh",
    "requiredRollbackState": "server_fallback",
    "currentPromotionState": "blocked_external_proof_required",
    "blockerRegister": "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"
  }
}
JSON
}

write_valid_register() {
  write_valid_external_proof_packet
  write_valid_release_bundle_manifest
  cat >"$register" <<'JSON'
{
  "register": "daxis_production_rollout_decisions",
  "sourceDocs": [
    "docs/adr/ADR-0008-daxis-browser-read-compute-contract.md",
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/program/daxis-operational-maturity.md",
    "docs/release-gates/daxis-browser-datafusion-budget-profile.json",
    "docs/release-gates/daxis-contract-artifacts.sha256",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/release-gates/daxis-operational-readiness.json",
    "docs/release-gates/daxis-release-bundle-manifest.json",
    "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"
  ],
  "decisions": {
    "endpointNames": {
      "status": "decided",
      "descriptorResolver": "POST /v1/query/delta/snapshot-descriptor",
      "readAccessPlan": "POST /v1/catalog/read-access-plan",
      "objectGrantRoutes": [
        "POST /object-grants/{grantId}/list",
        "POST /object-grants/{grantId}/head",
        "POST /object-grants/{grantId}/batch-sign",
        "GET /object-grants/{grantId}/range"
      ],
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "initialAccessMode": {
      "status": "decided",
      "default": "signed_url",
      "allowedModes": ["signed_url", "proxy"],
      "fallbackModes": ["sql_fallback_required", "server_fallback", "blocked"],
      "decision": "Preserve explicit fallback/block states when browser access is not allowed.",
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "signedUrlTtlAndRefresh": {
      "status": "external_signoff_required",
      "candidateTtlSeconds": 900,
      "refreshLeadTimeSeconds": 60,
      "sameSnapshotRequired": true,
      "externalOwner": "Daxis security engineering and storage platform",
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "descriptorRefreshSnapshotPolicy": {
      "status": "decided",
      "sameSnapshotRequired": true,
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "browserBudgets": {
      "status": "decided",
      "maxScanBytes": 67108864,
      "maxOutputIpcBytes": 16777216,
      "maxBatchesInFlight": 1,
      "maxRowsReturned": 100000,
      "evidence": ["docs/release-gates/daxis-browser-datafusion-budget-profile.json"]
    },
    "contractVersioning": {
      "status": "decided",
      "mechanisms": ["sha256_manifest", "json_schema", "openapi"],
      "evidence": ["docs/release-gates/daxis-contract-artifacts.sha256"]
    },
    "releaseChannels": {
      "status": "decided",
      "channels": ["experimental", "integration", "candidate", "stable"],
      "promotionRule": "Promote only with passing release evidence, no broadened browser trust boundary, and Daxis-owned rollout controls for the target segment.",
      "stableDefaultGate": {
        "externalProofPacket": "docs/release-gates/daxis-external-proof-packet.json",
        "releaseBundleManifest": "docs/release-gates/daxis-release-bundle-manifest.json",
        "requiredReleaseProcessAttachments": [
          "git_sha",
          "worker_artifact_size",
          "public_gcs_live_smoke",
          "release_notes",
          "migration_notes"
        ],
        "requiredExternalProofItemIds": [
          "daxis_architecture_docs",
          "daxis_names_axon_default_browser_engine",
          "daxis_descriptor_endpoint",
          "daxis_frontend_flow",
          "daxis_read_access_plan_endpoint",
          "storage_cors_proxy_validation",
          "production_dashboards",
          "production_runbooks",
          "rollout_controls",
          "production_table_compatibility_dashboard"
        ],
        "requiredReviewState": "accepted",
        "requiredReleaseEvidenceCommand": "bash tests/conformance/verify_daxis_release_evidence.sh",
        "requiredRollbackState": "server_fallback",
        "currentPromotionState": "blocked_external_proof_required",
        "blockerRegister": "docs/release-gates/browser-wasm-delta-gcs-external-blockers.md"
      },
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "initialTableEligibility": {
      "status": "decided",
      "requiredTraits": [
        "delta_or_parquet",
        "no_row_filters",
        "no_column_masks",
        "no_governed_views",
        "browser_supported_delta_features"
      ],
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "dashboardAlertThresholds": {
      "status": "external_signoff_required",
      "fallbackRateWarnPercent": 10,
      "fallbackRatePagePercent": 25,
      "workerStartupFailurePageCount": 5,
      "corsFailurePageCount": 3,
      "externalOwner": "Daxis SRE / production engineering",
      "evidence": ["docs/program/daxis-operational-maturity.md"]
    }
  }
}
JSON
}

verify_fixture() {
  AXON_DAXIS_ROLLOUT_REPO_ROOT="$repo_root" \
    AXON_DAXIS_ROLLOUT_DECISION_FILE="$register" \
    bash tests/conformance/verify_daxis_rollout_decisions.sh >/dev/null 2>&1
}

write_valid_register
verify_fixture

python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["sourceDocs"].append("docs/program/daxis-first-class-integration-strategy.md")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected duplicate rollout decision source docs to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["sourceDocs"].remove("docs/release-gates/daxis-browser-datafusion-budget-profile.json")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing browser DataFusion budget profile source doc to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["endpointNames"]["descriptorResolver"] = "POST /wrong"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected wrong descriptor endpoint to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["initialAccessMode"]["fallbackModes"].remove("sql_fallback_required")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing SQL fallback mode to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["initialAccessMode"]["fallbackModes"].remove("blocked")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing blocked fallback mode to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["signedUrlTtlAndRefresh"].pop("externalOwner")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing TTL external owner to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["releaseChannels"]["channels"] = ["experimental", "stable"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing release channels to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["releaseChannels"]["promotionRule"] = "Promote with passing tests."
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected weak promotion rule to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["releaseChannels"].pop("stableDefaultGate")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing stable default rollout gate linkage to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["releaseChannels"]["stableDefaultGate"].pop("currentPromotionState")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected stable default rollout gate missing current promotion state to be rejected" >&2
  exit 1
fi

write_valid_register
python3 - "$repo_root/docs/release-gates/daxis-release-bundle-manifest.json" <<'PY'
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
  echo "expected stable default release-evidence command to be required by release bundle manifest" >&2
  exit 1
fi

write_valid_register
python3 - "$repo_root/docs/release-gates/daxis-external-proof-packet.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["stableDefaultPromotionGate"]["requiredReviewState"] = "reviewed"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected rollout stable default gate to require accepted external proof gate state" >&2
  exit 1
fi

write_valid_register
python3 - "$repo_root/docs/release-gates/daxis-external-proof-packet.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    packet = json.load(handle)
packet["stableDefaultPromotionGate"]["blockerRegister"] = "docs/release-gates/stale-external-blockers.md"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(packet, handle)
PY

if verify_fixture; then
  echo "expected rollout stable default gate to match the external proof blocker register" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["releaseChannels"]["stableDefaultGate"]["requiredReleaseProcessAttachments"].remove("public_gcs_live_smoke")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected rollout stable default gate to include every external proof release-process attachment" >&2
  exit 1
fi

write_valid_register
python3 - "$register" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    register = json.load(handle)
register["decisions"]["initialTableEligibility"]["requiredTraits"].remove("no_column_masks")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing eligibility trait to be rejected" >&2
  exit 1
fi

echo "Daxis rollout decision verifier regression coverage passed"
