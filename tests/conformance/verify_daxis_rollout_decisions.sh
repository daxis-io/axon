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


expect(register.get("register") == "daxis_production_rollout_decisions", "invalid rollout decision register id")

for source_doc in register.get("sourceDocs", []):
    source_path = Path(source_doc)
    expect(not source_path.is_absolute() and ".." not in source_path.parts, f"unsafe source doc path: {source_doc}")
    expect((repo_root / source_path).is_file(), f"missing source doc: {source_doc}")

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
expect("server_fallback" in access.get("fallbackModes", []), "server_fallback must remain a fallback mode")

ttl = decisions["signedUrlTtlAndRefresh"]
expect(ttl.get("status") == "external_signoff_required", "TTL policy requires external signoff")
expect(ttl.get("sameSnapshotRequired") is True, "refresh must preserve the same snapshot")
expect(isinstance(ttl.get("candidateTtlSeconds"), int) and ttl["candidateTtlSeconds"] > 0, "candidate TTL must be positive")

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

eligibility = decisions["initialTableEligibility"]
for field in ["delta_or_parquet", "no_row_filters", "no_column_masks", "no_governed_views", "browser_supported_delta_features"]:
    expect(field in eligibility.get("requiredTraits", []), f"missing initial table eligibility trait: {field}")

thresholds = decisions["dashboardAlertThresholds"]
for field in ["fallbackRateWarnPercent", "fallbackRatePagePercent", "workerStartupFailurePageCount", "corsFailurePageCount"]:
    expect(isinstance(thresholds.get(field), int) and thresholds[field] >= 0, f"threshold {field} must be a non-negative integer")

print("Daxis rollout decision register verified")
PY
