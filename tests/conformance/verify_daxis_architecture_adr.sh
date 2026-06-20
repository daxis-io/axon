#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_ADR_REPO_ROOT:-$(pwd)}"
adr_file="${AXON_DAXIS_ADR_FILE:-docs/adr/ADR-0008-daxis-browser-read-compute-contract.md}"
adr_index="${AXON_DAXIS_ADR_INDEX:-docs/adr/README.md}"

resolve_path() {
  local path="$1"
  if [[ "$path" = /* ]]; then
    printf "%s\n" "$path"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$path"
}

adr_path="$(resolve_path "$adr_file")"
index_path="$(resolve_path "$adr_index")"

if [[ ! -f "$adr_path" ]]; then
  echo "missing Daxis architecture ADR: $adr_file" >&2
  exit 1
fi

if [[ ! -f "$index_path" ]]; then
  echo "missing ADR index: $adr_index" >&2
  exit 1
fi

python3 - "$repo_root" "$adr_path" "$index_path" <<'PY'
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
adr_path = Path(sys.argv[2])
index_path = Path(sys.argv[3])
adr = adr_path.read_text(encoding="utf-8")
index = index_path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def check_repo_path(path_text: str) -> None:
    candidate = Path(path_text)
    expect(not candidate.is_absolute() and ".." not in candidate.parts, f"unsafe referenced path: {path_text}")
    expect((repo_root / candidate).exists(), f"missing referenced path: {path_text}")


expected_index_entry = (
    "- [ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane]"
    "(./ADR-0008-daxis-browser-read-compute-contract.md)"
)
expect(expected_index_entry in index, "ADR index missing Daxis browser read compute ADR")

expect(adr.startswith("# ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane"), "unexpected ADR title")
for required in [
    "- Status:",
    "- Date:",
    "- Decision owner:",
    "- Delivery DRI:",
    "- Approver:",
    "## Decision",
    "## Context",
    "## Policy",
    "## Consequences",
    "## Acceptance Standard",
    "## References",
]:
    expect(required in adr, f"ADR missing section or field: {required}")

adr_lower = adr.lower()
for required_text in [
    "axon is the browser read-compute engine for daxis",
    "daxis remains the trusted control plane",
    "server-resolved descriptors",
    "brokered object grants",
    "browser must never receive raw cloud credentials",
    "server_fallback",
    "production proof remains external",
]:
    expect(required_text in adr_lower, f"ADR missing required decision text: {required_text}")

for path_text in [
    "docs/integrations/daxis/daxis-first-class-integration-strategy.md",
    "docs/release-gates/daxis-production-rollout-decisions.json",
    "docs/release-gates/daxis-strategy-traceability.json",
    "docs/release-gates/daxis-external-proof-packet.json",
    "docs/integrations/daxis/daxis-external-proof-handoff.md",
]:
    expect(path_text in adr, f"ADR missing reference to {path_text}")
    check_repo_path(path_text)

print("Daxis architecture ADR verified")
PY
