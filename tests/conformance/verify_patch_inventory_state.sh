#!/usr/bin/env bash

set -euo pipefail

repo_root="${AXON_PATCH_REPO_ROOT:-.}"
inventory_file="${AXON_PATCH_INVENTORY_FILE:-$repo_root/docs/program/upstream-patch-inventory.md}"

if [[ ! -f "$inventory_file" ]]; then
  echo "patch inventory guard failed: missing inventory file $inventory_file" >&2
  exit 1
fi

if rg -n 'Example:|TBD' "$inventory_file" >/dev/null; then
  echo "patch inventory guard failed: template/example rows must be removed" >&2
  rg -n 'Example:|TBD' "$inventory_file" >&2
  exit 1
fi

vendor_dir=""
if [[ -d "$repo_root" ]]; then
  vendor_dir="$(
    find "$repo_root" \
      -path "$repo_root/.git" -prune -o \
      -path "$repo_root/target" -prune -o \
      -type d -name vendor -print -quit
  )"
fi

patch_section_files="$(
  rg -l '^\[patch(\.|])' "$repo_root/Cargo.toml" "$repo_root/crates" --glob 'Cargo.toml' 2>/dev/null || true
)"

declares_no_patches=0
if rg -n '^Current state: no private downstream patches are checked into this repository\.$' "$inventory_file" >/dev/null; then
  declares_no_patches=1
fi

if [[ -n "$vendor_dir" || -n "$patch_section_files" ]]; then
  if [[ $declares_no_patches -eq 1 ]]; then
    echo "patch inventory guard failed: inventory says no patches exist, but vendoring or [patch] sections were detected" >&2
    [[ -n "$vendor_dir" ]] && echo "vendor directory: $vendor_dir" >&2
    [[ -n "$patch_section_files" ]] && printf 'patch section files:\n%s\n' "$patch_section_files" >&2
    exit 1
  fi

  row_count="$(
    awk '
      /^\| Patch \| Local Path \| Owner \| Upstream Disposition \| Removal Condition \| Tracking Issue \|$/ {
        in_patch_table = 1
        next
      }
      in_patch_table && /^\| --- \| --- \| --- \| --- \| --- \| --- \|$/ { next }
      in_patch_table && /^\|/ {
        count++
        next
      }
      in_patch_table {
        in_patch_table = 0
      }
      END { print count + 0 }
    ' "$inventory_file"
  )"
  if [[ "$row_count" -eq 0 ]]; then
    echo "patch inventory guard failed: vendoring or [patch] sections require at least one declared inventory row" >&2
    exit 1
  fi
else
  if [[ $declares_no_patches -ne 1 ]]; then
    echo "patch inventory guard failed: inventory must explicitly declare the no-patches state when no vendoring or [patch] sections exist" >&2
    exit 1
  fi
fi

echo "patch inventory state verified"
