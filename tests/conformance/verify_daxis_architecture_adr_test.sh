#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
adr="$repo_root/docs/adr/ADR-0008-daxis-browser-read-compute-contract.md"
index="$repo_root/docs/adr/README.md"

mkdir -p "$repo_root/docs/adr" "$repo_root/docs/program" "$repo_root/docs/release-gates"
for doc in \
  docs/program/daxis-first-class-integration-strategy.md \
  docs/release-gates/daxis-production-rollout-decisions.json \
  docs/release-gates/daxis-strategy-traceability.json \
  docs/release-gates/daxis-external-proof-packet.json \
  docs/program/daxis-external-proof-handoff.md; do
  mkdir -p "$repo_root/$(dirname "$doc")"
  printf '# test fixture\n' >"$repo_root/$doc"
done

write_valid_adr() {
  cat >"$index" <<'MD'
# ADR Index

- [ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane](./ADR-0008-daxis-browser-read-compute-contract.md)
MD

  cat >"$adr" <<'MD'
# ADR-0008: Daxis Browser Read Compute Uses Axon Contracts And Daxis Control Plane

- Status: Proposed
- Date: 2026-05-30
- Decision owner: Runtime / engine team and Daxis platform owners
- Delivery DRI: Runtime / engine team
- Approver: Query platform lead

## Decision

Axon is the browser read-compute engine for Daxis. Daxis remains the trusted control plane. The production integration starts with server-resolved descriptors and later adds brokered object grants. The browser must never receive raw cloud credentials. Daxis must preserve server_fallback.

## Context

The integration strategy separates runtime execution from Daxis policy and production service ownership.

## Policy

Use public Axon contracts.

## Consequences

Production proof remains external.

## Acceptance Standard

The ADR is implemented only when release evidence and external proof gates pass.

## References

- `docs/program/daxis-first-class-integration-strategy.md`
- `docs/release-gates/daxis-production-rollout-decisions.json`
- `docs/release-gates/daxis-strategy-traceability.json`
- `docs/release-gates/daxis-external-proof-packet.json`
- `docs/program/daxis-external-proof-handoff.md`
MD
}

verify_fixture() {
  AXON_DAXIS_ADR_REPO_ROOT="$repo_root" \
    AXON_DAXIS_ADR_FILE="$adr" \
    AXON_DAXIS_ADR_INDEX="$index" \
    bash tests/conformance/verify_daxis_architecture_adr.sh >/dev/null 2>&1
}

write_valid_adr
verify_fixture

rm "$adr"
if verify_fixture; then
  echo "expected missing Daxis architecture ADR to be rejected" >&2
  exit 1
fi

write_valid_adr
python3 - "$index" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
path.write_text("# ADR Index\n", encoding="utf-8")
PY

if verify_fixture; then
  echo "expected missing ADR index entry to be rejected" >&2
  exit 1
fi

write_valid_adr
python3 - "$adr" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("Daxis remains the trusted control plane. ", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
  echo "expected missing Daxis control-plane boundary to be rejected" >&2
  exit 1
fi

echo "Daxis architecture ADR verifier regression coverage passed"
