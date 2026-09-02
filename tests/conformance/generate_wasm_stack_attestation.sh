#!/usr/bin/env bash

set -euo pipefail

output=""
axon_commit=""
stack_lock=""
cargo_lock=""
wasm=""
compressed_asset=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:-}"; shift 2 ;;
    --axon-commit) axon_commit="${2:-}"; shift 2 ;;
    --stack-lock) stack_lock="${2:-}"; shift 2 ;;
    --cargo-lock) cargo_lock="${2:-}"; shift 2 ;;
    --wasm) wasm="${2:-}"; shift 2 ;;
    --compressed-asset) compressed_asset="${2:-}"; shift 2 ;;
    *) echo "attestation generation failed: unknown argument $1" >&2; exit 2 ;;
  esac
done

python3 - "$output" "$axon_commit" "$stack_lock" "$cargo_lock" "$wasm" "$compressed_asset" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile

output_raw, axon_commit, stack_raw, cargo_raw, wasm_raw, compressed_raw = sys.argv[1:]
if not re.fullmatch(r"[0-9a-f]{40}", axon_commit):
    raise SystemExit("attestation generation failed: --axon-commit must be a 40-character lowercase hexadecimal revision")
paths = {
    "stack_lock_sha256": Path(stack_raw),
    "cargo_lock_sha256": Path(cargo_raw),
    "wasm_sha256": Path(wasm_raw),
    "compressed_asset_sha256": Path(compressed_raw),
}
if not output_raw:
    raise SystemExit("attestation generation failed: --output is required")
for field, path in paths.items():
    if not path.is_file():
        raise SystemExit(f"attestation generation failed: missing {field} input: {path}")

attestation = {"schema": "axon.wasm-stack-attestation.v1", "axon_commit": axon_commit}
for field, path in paths.items():
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    attestation[field] = digest.hexdigest()

output = Path(output_raw)
output.parent.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, delete=False) as handle:
    json.dump(attestation, handle, indent=2, sort_keys=True)
    handle.write("\n")
    temporary = Path(handle.name)
temporary.replace(output)
PY
