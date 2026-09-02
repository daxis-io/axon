#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
generator="$script_dir/generate_wasm_stack_attestation.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

printf '%s' 'wasm-bundle' >"$tmpdir/bundle.wasm"
printf '%s' 'compressed-bundle' >"$tmpdir/bundle.wasm.br"
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

python3 - "$tmpdir/attestation.json" "$axon_commit" "$tmpdir" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

attestation_path = Path(sys.argv[1])
axon_commit = sys.argv[2]
root = Path(sys.argv[3])
attestation = json.loads(attestation_path.read_text())

assert attestation["schema"] == "axon.wasm-stack-attestation.v1"
assert attestation["axon_commit"] == axon_commit
for field, filename in (
    ("stack_lock_sha256", "wasm-stack.lock.toml"),
    ("cargo_lock_sha256", "Cargo.lock"),
    ("wasm_sha256", "bundle.wasm"),
    ("compressed_asset_sha256", "bundle.wasm.br"),
):
    expected = hashlib.sha256((root / filename).read_bytes()).hexdigest()
    assert attestation[field] == expected, (field, attestation[field], expected)
PY

invalid_output="$tmpdir/invalid-attestation.json"
if bash "$generator" \
  --output "$invalid_output" \
  --axon-commit invalid \
  --stack-lock "$tmpdir/wasm-stack.lock.toml" \
  --cargo-lock "$tmpdir/Cargo.lock" \
  --wasm "$tmpdir/bundle.wasm" \
  --compressed-asset "$tmpdir/bundle.wasm.br" >/dev/null 2>&1; then
  echo "attestation generator accepted an invalid Axon commit" >&2
  exit 1
fi
if [[ -e "$invalid_output" ]]; then
  echo "attestation generator left partial output after invalid input" >&2
  exit 1
fi

echo "WASM stack release attestation generator positive and invalid-input coverage passed"
