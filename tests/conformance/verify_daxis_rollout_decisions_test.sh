#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
register="$repo_root/docs/release-gates/daxis-production-rollout-decisions.json"

for doc in \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/program/daxis-operational-maturity.md \
  docs/release-gates/daxis-browser-datafusion-budget-profile.json \
  docs/release-gates/daxis-contract-artifacts.sha256 \
  docs/release-gates/daxis-operational-readiness.json; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_register() {
  cat >"$register" <<'JSON'
{
  "register": "daxis_production_rollout_decisions",
  "sourceDocs": [
    "docs/program/daxis-first-class-integration-strategy.md",
    "docs/program/daxis-operational-maturity.md",
    "docs/release-gates/daxis-browser-datafusion-budget-profile.json",
    "docs/release-gates/daxis-contract-artifacts.sha256",
    "docs/release-gates/daxis-operational-readiness.json"
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
      "fallbackModes": ["server_fallback"],
      "evidence": ["docs/program/daxis-first-class-integration-strategy.md"]
    },
    "signedUrlTtlAndRefresh": {
      "status": "external_signoff_required",
      "candidateTtlSeconds": 900,
      "sameSnapshotRequired": true,
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
register["decisions"]["initialTableEligibility"]["requiredTraits"].remove("no_column_masks")
with open(path, "w", encoding="utf-8") as handle:
    json.dump(register, handle)
PY

if verify_fixture; then
  echo "expected missing eligibility trait to be rejected" >&2
  exit 1
fi

echo "Daxis rollout decision verifier regression coverage passed"
