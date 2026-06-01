#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
template="$repo_root/.github/pull_request_template.md"

mkdir -p "$repo_root/.github"

write_valid_template() {
  cat >"$template" <<'TEMPLATE'
# Pull Request

## Summary

## Verification

## Daxis-Relevant Changes

- [ ] The Daxis/Axon trust boundary is unchanged or explicitly documented.
- [ ] Browser code does not receive raw credentials or signing capability.
- [ ] Public contract changes are additive or have a migration path.
- [ ] TypeScript and Rust contract shapes stay aligned.
- [ ] Unsupported behavior returns a structured error, fallback, or block reason.
- [ ] Native/browser parity tests cover any new supported query behavior.
- [ ] Browser dependency guardrails still pass.
- [ ] WASM target checks still pass.
- [ ] Worker artifact size impact is known.
- [ ] Browser matrix coverage is updated when worker behavior changes.
- [ ] Relevant docs and release evidence are updated.
TEMPLATE
}

verify_fixture() {
  AXON_DAXIS_PR_CHECKLIST_REPO_ROOT="$repo_root" \
    bash tests/conformance/verify_daxis_pr_checklist.sh >/dev/null 2>&1
}

write_valid_template
verify_fixture

python3 - "$template" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("- [ ] Worker artifact size impact is known.\n", "")
path.write_text(text, encoding="utf-8")
PY

if verify_fixture; then
  echo "expected missing Daxis worker artifact size checklist item to be rejected" >&2
  exit 1
fi

rm -f "$template"

if verify_fixture; then
  echo "expected missing pull request template to be rejected" >&2
  exit 1
fi

echo "Daxis PR checklist verifier regression coverage passed"
