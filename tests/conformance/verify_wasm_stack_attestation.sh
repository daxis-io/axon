#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "missing required tool: rg" >&2
  exit 127
fi

attestation=""
axon_commit=""
stack_lock=""
cargo_lock=""
wasm=""
compressed_asset=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --attestation) attestation="${2:-}"; shift 2 ;;
    --axon-commit) axon_commit="${2:-}"; shift 2 ;;
    --stack-lock) stack_lock="${2:-}"; shift 2 ;;
    --cargo-lock) cargo_lock="${2:-}"; shift 2 ;;
    --wasm) wasm="${2:-}"; shift 2 ;;
    --compressed-asset) compressed_asset="${2:-}"; shift 2 ;;
    *) echo "attestation verification failed: unknown argument $1" >&2; exit 2 ;;
  esac
done

python3 - "$attestation" "$axon_commit" "$stack_lock" "$cargo_lock" "$wasm" "$compressed_asset" <<'PY'
import hashlib
import json
from pathlib import Path
import re
import sys

attestation_raw, axon_commit, stack_raw, cargo_raw, wasm_raw, compressed_raw = sys.argv[1:]
if not re.fullmatch(r"[0-9a-f]{40}", axon_commit):
    raise SystemExit("attestation verification failed: --axon-commit must be a 40-character lowercase hexadecimal revision")

attestation_path = Path(attestation_raw)
if not attestation_path.is_file():
    raise SystemExit(f"attestation verification failed: missing attestation: {attestation_path}")
try:
    attestation = json.loads(attestation_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"attestation verification failed: cannot parse attestation: {error}") from error

expected_fields = {
    "schema",
    "axon_commit",
    "stack_lock_sha256",
    "cargo_lock_sha256",
    "wasm_sha256",
    "compressed_asset_sha256",
}
if set(attestation) != expected_fields:
    raise SystemExit("attestation verification failed: unexpected attestation fields")
if attestation.get("schema") != "axon.wasm-stack-attestation.v1":
    raise SystemExit("attestation verification failed: unsupported schema")
if attestation.get("axon_commit") != axon_commit:
    raise SystemExit("attestation verification failed: Axon commit mismatch")

inputs = {
    "stack_lock_sha256": Path(stack_raw),
    "cargo_lock_sha256": Path(cargo_raw),
    "wasm_sha256": Path(wasm_raw),
    "compressed_asset_sha256": Path(compressed_raw),
}
for field, path in inputs.items():
    if not path.is_file():
        raise SystemExit(f"attestation verification failed: missing {field} input: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    if attestation.get(field) != digest.hexdigest():
        raise SystemExit(f"attestation verification failed: {field} mismatch")

print("WASM stack attestation verified")
PY
