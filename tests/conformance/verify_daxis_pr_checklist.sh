#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_DAXIS_PR_CHECKLIST_REPO_ROOT:-$(pwd)}"
template_file="${AXON_DAXIS_PR_CHECKLIST_TEMPLATE:-.github/pull_request_template.md}"

template_path() {
  if [[ "$template_file" = /* ]]; then
    printf "%s\n" "$template_file"
    return
  fi

  printf "%s/%s\n" "$repo_root" "$template_file"
}

path="$(template_path)"
if [[ ! -f "$path" ]]; then
  echo "missing Daxis pull request checklist template: $template_file" >&2
  exit 1
fi

python3 - "$path" "$template_file" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
template_file = sys.argv[2]
text = path.read_text(encoding="utf-8")


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


required = [
    "## Daxis-Relevant Changes",
    "- [ ] The Daxis/Axon trust boundary is unchanged or explicitly documented.",
    "- [ ] Browser code does not receive raw credentials or signing capability.",
    "- [ ] Public contract changes are additive or have a migration path.",
    "- [ ] TypeScript and Rust contract shapes stay aligned.",
    "- [ ] Unsupported behavior returns a structured error, fallback, or block reason.",
    "- [ ] Native/browser parity tests cover any new supported query behavior.",
    "- [ ] Browser dependency guardrails still pass.",
    "- [ ] WASM target checks still pass.",
    "- [ ] Worker artifact size impact is known.",
    "- [ ] Browser matrix coverage is updated when worker behavior changes.",
    "- [ ] Relevant docs and release evidence are updated.",
]

for line in required:
    if line not in text:
        fail(f"{template_file} missing Daxis PR checklist item: {line}")

print("Daxis PR checklist verified")
PY
