#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
generator="$script_dir/generate_wasm_stack_attestation.sh"
verifier="$script_dir/verify_wasm_stack_attestation.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

printf '%s' 'qualified-wasm' >"$tmpdir/bundle.wasm"
printf '%s' 'qualified-compressed-wasm' >"$tmpdir/bundle.wasm.br"
printf '%s' 'schema = 2' >"$tmpdir/wasm-stack.lock.toml"
printf '%s' 'version = 4' >"$tmpdir/Cargo.lock"
axon_commit="0123456789abcdef0123456789abcdef01234567"

bash "$generator" \
  --output "$tmpdir/attestation.json" \
  --axon-commit "$axon_commit" \
  --stack-lock "$tmpdir/wasm-stack.lock.toml" \
  --cargo-lock "$tmpdir/Cargo.lock" \
  --wasm "$tmpdir/bundle.wasm" \
  --compressed-asset "$tmpdir/bundle.wasm.br"

verify=(
  bash "$verifier"
  --attestation "$tmpdir/attestation.json"
  --axon-commit "$axon_commit"
  --stack-lock "$tmpdir/wasm-stack.lock.toml"
  --cargo-lock "$tmpdir/Cargo.lock"
  --wasm "$tmpdir/bundle.wasm"
  --compressed-asset "$tmpdir/bundle.wasm.br"
)
"${verify[@]}"

for input in \
  "$tmpdir/wasm-stack.lock.toml" \
  "$tmpdir/Cargo.lock" \
  "$tmpdir/bundle.wasm" \
  "$tmpdir/bundle.wasm.br"; do
  backup="$tmpdir/$(basename "$input").original"
  cp "$input" "$backup"
  printf '%s' tampered >>"$input"
  if "${verify[@]}" >/dev/null 2>&1; then
    echo "attestation verification accepted tampered bytes: $(basename "$input")" >&2
    exit 1
  fi
  mv "$backup" "$input"
done

wrong_commit="1123456789abcdef0123456789abcdef01234567"
verify_wrong_commit=("${verify[@]}")
for index in "${!verify_wrong_commit[@]}"; do
  if [[ "${verify_wrong_commit[$index]}" == "$axon_commit" ]]; then
    verify_wrong_commit[index]="$wrong_commit"
  fi
done
if "${verify_wrong_commit[@]}" >/dev/null 2>&1; then
  echo "attestation verification accepted the wrong Axon commit" >&2
  exit 1
fi

cp "$tmpdir/attestation.json" "$tmpdir/attestation.original.json"
python3 - "$tmpdir/attestation.json" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
attestation = json.loads(path.read_text())
attestation["unexpected"] = "field"
path.write_text(json.dumps(attestation) + "\n")
PY
if "${verify[@]}" >/dev/null 2>&1; then
  echo "attestation verification accepted an unexpected field" >&2
  exit 1
fi
mv "$tmpdir/attestation.original.json" "$tmpdir/attestation.json"

echo "WASM stack attestation positive, identity, schema, and all-input tamper coverage passed"
