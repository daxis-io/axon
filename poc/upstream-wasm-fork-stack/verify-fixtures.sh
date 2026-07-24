#!/usr/bin/env bash

set -euo pipefail

POC_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURE_ROOT="${POC_ROOT}/fixtures"

python3 - "${FIXTURE_ROOT}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
manifest_path = root / "manifest.json"
if not manifest_path.is_file():
    raise SystemExit(f"missing fixture manifest: {manifest_path}")

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("schema_version") != 1:
    raise SystemExit("fixture manifest schema_version must be 1")
if manifest.get("generator") != {"arrow": "58.3.0", "parquet": "58.3.0"}:
    raise SystemExit("fixture manifest must record released Arrow and Parquet 58.3.0")

tables = manifest.get("tables", [])
if [table.get("codec") for table in tables] != ["snappy", "zstd"]:
    raise SystemExit("fixture manifest must contain independent Snappy and zstd tables")

for table in tables:
    if table.get("expected", {}).get("rows") != [
        {"category": "alpha", "total": 7},
        {"category": "beta", "total": 10},
    ]:
        raise SystemExit(f"unexpected query result for {table.get('name')}")
    for record in table.get("files", []):
        path = root / record["path"]
        body = path.read_bytes()
        if len(body) != record["bytes"]:
            raise SystemExit(f"fixture byte length mismatch: {path}")
        digest = hashlib.sha256(body).hexdigest()
        if digest != record["sha256"]:
            raise SystemExit(f"fixture SHA-256 mismatch: {path}")

print("released-crate Delta fixtures verified")
PY
