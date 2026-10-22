#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
gate="$script_dir/report_shipped_wasm_size.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

# Writes a minimal valid module: the header, one type section, and optionally a name section.
write_module() {
  local path="$1" with_names="$2"
  mkdir -p "$(dirname "$path")"
  python3 - "$path" "$with_names" <<'PY'
import sys

path, with_names = sys.argv[1], sys.argv[2] == "1"
module = bytearray(b"\0asm\x01\0\0\0")
module += bytes([1, 4, 1, 0x60, 0, 0])  # type section: one () -> () signature
if with_names:
    payload = b"\x04name" + bytes([1, 5, 4]) + b"main"  # name section with a module name
    module += bytes([0, len(payload)]) + payload
open(path, "wb").write(module)
PY
}

expect_failure() {
  local description="$1" expected="$2"
  shift 2
  local output
  if output="$("$@" 2>&1)"; then
    echo "shipped WASM gate accepted ${description}" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    echo "shipped WASM gate rejected ${description} for the wrong reason:" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
}

stripped="$tmpdir/stripped"
write_module "$stripped/assets/axon_web_wasm_bg-abc123.wasm" 0
bash "$gate" "$stripped" | grep -q "shipped WASM gate passed"

named="$tmpdir/named"
write_module "$named/assets/axon_web_wasm_bg-abc123.wasm" 1
expect_failure "a module with a name section" "--remove-name-section" bash "$gate" "$named"

expect_failure "a Brotli size above the budget" "Brotli budget exceeded" \
  env AXON_SHIPPED_WASM_BROTLI_BUDGET_BYTES=1 bash "$gate" "$stripped"
expect_failure "a non-numeric budget" "unsigned integer" \
  env AXON_SHIPPED_WASM_BROTLI_BUDGET_BYTES=6MiB bash "$gate" "$stripped"

two="$tmpdir/two"
write_module "$two/assets/a.wasm" 0
write_module "$two/assets/b.wasm" 0
expect_failure "two WASM assets" "exactly one WASM asset" bash "$gate" "$two"
mkdir -p "$tmpdir/none/assets"
expect_failure "a build without a WASM asset" "exactly one WASM asset" bash "$gate" "$tmpdir/none"

truncated="$tmpdir/truncated"
mkdir -p "$truncated/assets"
printf '\0asm\1\0\0\0\1\10' >"$truncated/assets/engine.wasm"
expect_failure "a truncated module" "past the end of the module" bash "$gate" "$truncated"

# The gate must run in CI on the app build that the deployment ships.
ci="$repo_root/.github/workflows/ci.yml"
if ! awk '/run: npm run build$/ { built = 1 } built && /report_shipped_wasm_size\.sh apps\/axon-web\/dist/ { found = 1 } END { exit !found }' "$ci"; then
  echo "CI does not gate the shipped WASM asset after npm run build" >&2
  exit 1
fi

echo "shipped WASM gate coverage passed"
