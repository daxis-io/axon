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
if manifest.get("generator") != {
    "arrow": "58.3.0",
    "parquet": "58.3.0",
    "deltalake": "0.32.4",
}:
    raise SystemExit("fixture manifest must record released Arrow, Parquet, and Delta versions")

tables = manifest.get("tables", [])
if [table.get("name") for table in tables] != ["snappy", "zstd", "checkpointed"]:
    raise SystemExit("fixture manifest must contain the snappy, zstd, and checkpointed tables")

# Independent oracle: these totals are asserted here, never read from the manifest.
expected_rows = {
    "snappy": [{"category": "alpha", "total": 7}, {"category": "beta", "total": 10}],
    "zstd": [{"category": "alpha", "total": 7}, {"category": "beta", "total": 10}],
    # Only reachable by replaying the checkpoint; see the checkpoint checks below.
    "checkpointed": [{"category": "alpha", "total": 18}, {"category": "beta", "total": 23}],
}

for table in tables:
    name = table.get("name")
    if table.get("expected", {}).get("rows") != expected_rows[name]:
        raise SystemExit(f"unexpected query result for {name}")
    for record in table.get("files", []):
        path = root / record["path"]
        body = path.read_bytes()
        if len(body) != record["bytes"]:
            raise SystemExit(f"fixture byte length mismatch: {path}")
        digest = hashlib.sha256(body).hexdigest()
        if digest != record["sha256"]:
            raise SystemExit(f"fixture SHA-256 mismatch: {path}")

# The checkpointed table only proves checkpoint replay while the earlier commits
# needed to reconstruct the checkpoint state stay deleted. If they came back, a
# reader that ignored the checkpoint could still pass, and the fixture would be
# worthless.
checkpointed = next(table for table in tables if table["name"] == "checkpointed")
checkpoint = checkpointed.get("checkpoint", {})
version = checkpoint.get("version")
if not isinstance(version, int) or version < 1:
    raise SystemExit("checkpointed fixture must record an integer checkpoint version >= 1")
if checkpoint.get("latest_version", 0) <= version:
    raise SystemExit("checkpointed fixture must carry at least one commit after the checkpoint")

root_prefix = checkpointed.get("root", "checkpointed")
paths = {record["path"] for record in checkpointed.get("files", [])}
if f"{root_prefix}/_delta_log/{version:020}.checkpoint.parquet" not in paths:
    raise SystemExit("checkpointed fixture is missing its checkpoint parquet")
if f"{root_prefix}/_delta_log/_last_checkpoint" not in paths:
    raise SystemExit("checkpointed fixture is missing _last_checkpoint")
for subsumed in range(version):
    if f"{root_prefix}/_delta_log/{subsumed:020}.json" in paths:
        raise SystemExit(
            f"checkpointed fixture still carries commit {subsumed}, which the checkpoint subsumes; "
            "the fixture would no longer distinguish checkpoint replay"
        )

print("released-crate Delta fixtures verified")
PY
