#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

repo_root="$tmpdir/repo"
inventory="$tmpdir/upstream-patch-inventory.md"

mkdir -p "$repo_root/crates/demo"

run_guard() {
  AXON_PATCH_REPO_ROOT="$repo_root" \
    AXON_PATCH_INVENTORY_FILE="$inventory" \
    bash tests/conformance/verify_patch_inventory_state.sh
}

cat > "$repo_root/Cargo.toml" <<'EOF_WORKSPACE'
[workspace]
members = []
EOF_WORKSPACE

cat > "$inventory" <<'EOF_INVENTORY'
# Upstream Patch Inventory

| Patch | Local Path | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- |
| Example: browser range-read fix | `vendor/datafusion-wasm-bindings` | Runtime / engine team | `proposed` | Remove after upstream release with fix | `TBD` |
EOF_INVENTORY

if run_guard; then
  echo "expected template inventory rows to be rejected" >&2
  exit 1
fi

cat > "$inventory" <<'EOF_INVENTORY'
# Upstream Patch Inventory

Current state: no private downstream patches are checked into this repository.

## Verification

- No `vendor/` directory exists in this repository.
- No workspace manifest contains a `[patch]` section.
EOF_INVENTORY

run_guard >/dev/null

mkdir -p "$repo_root/vendor/foo"

if run_guard; then
  echo "expected explicit no-patches inventory to be rejected when vendoring exists" >&2
  exit 1
fi

rm -rf "$repo_root/vendor"

cat > "$repo_root/Cargo.toml" <<'EOF_WORKSPACE'
[workspace]
members = []

[patch.crates-io]
foo = { path = "vendor/foo" }
EOF_WORKSPACE

cat > "$inventory" <<'EOF_INVENTORY'
# Upstream Patch Inventory

| Patch | Local Path | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- |
EOF_INVENTORY

if run_guard; then
  echo "expected [patch] sections without declared patch rows to be rejected" >&2
  exit 1
fi

cat > "$inventory" <<'EOF_INVENTORY'
# Upstream Patch Inventory

| Note | Value |
| --- | --- |
| unrelated | table |
EOF_INVENTORY

if run_guard; then
  echo "expected unrelated markdown tables to not count as patch inventory rows" >&2
  exit 1
fi

mkdir -p "$repo_root/vendor/foo"

cat > "$inventory" <<'EOF_INVENTORY'
# Upstream Patch Inventory

| Patch | Local Path | Owner | Upstream Disposition | Removal Condition | Tracking Issue |
| --- | --- | --- | --- | --- | --- |
| browser range-read fix | `vendor/foo` | Runtime / engine team | `proposed` | Remove after upstream release with fix | `AXON-123` |
EOF_INVENTORY

run_guard >/dev/null

echo "patch inventory guard regression coverage passed"
