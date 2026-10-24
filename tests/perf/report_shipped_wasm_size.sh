#!/usr/bin/env bash

# Gates the exact WASM asset a production build ships.
#
# `report_datafusion_wasm_size.sh` rebuilds and measures a copy of the engine; this script instead
# measures the one `*.wasm` file in a built app's `assets/` directory, which is the byte stream the
# deployment serves. It fails when that asset still carries the debug name section (dead weight on
# the wire and roughly doubled V8 memory per compiled module) or when its Brotli -q 11 size exceeds
# the budget.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: report_shipped_wasm_size.sh <dist-directory>" >&2
  exit 2
fi
dist_root="$1"
brotli_budget_bytes="${AXON_SHIPPED_WASM_BROTLI_BUDGET_BYTES:-6291456}"

if [[ ! "$brotli_budget_bytes" =~ ^[0-9]+$ ]]; then
  echo "AXON_SHIPPED_WASM_BROTLI_BUDGET_BYTES must be an unsigned integer byte count: ${brotli_budget_bytes}" >&2
  exit 1
fi

for tool in brotli python3; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "missing required tool: $tool" >&2
    exit 1
  fi
done

if [[ ! -d "${dist_root}/assets" ]]; then
  echo "shipped WASM gate failed: '${dist_root}/assets' is not a directory" >&2
  exit 1
fi
wasm_assets=()
while IFS= read -r -d '' asset; do
  wasm_assets+=("$asset")
done < <(find "${dist_root}/assets" -type f -name '*.wasm' -print0)
if [[ "${#wasm_assets[@]}" -ne 1 ]]; then
  echo "shipped WASM gate failed: expected exactly one WASM asset in ${dist_root}/assets, found ${#wasm_assets[@]}" >&2
  exit 1
fi
wasm="${wasm_assets[0]}"

# Prints the byte size of the `name` custom section (0 when absent); fails on a malformed module.
name_section_bytes="$(
  python3 - "$wasm" <<'PY'
import sys


def leb128(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise SystemExit("shipped WASM gate failed: truncated LEB128 value")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return value, offset


data = open(sys.argv[1], "rb").read()
if data[:8] != b"\0asm\x01\0\0\0":
    raise SystemExit("shipped WASM gate failed: asset is not a version 1 WebAssembly module")
offset = 8
name_bytes = 0
while offset < len(data):
    section_id = data[offset]
    size, body = leb128(data, offset + 1)
    end = body + size
    if end > len(data):
        raise SystemExit("shipped WASM gate failed: section extends past the end of the module")
    if section_id == 0:
        length, start = leb128(data, body)
        if data[start : start + length] == b"name":
            name_bytes += size
    offset = end
print(name_bytes)
PY
)"

raw_bytes="$(wc -c < "$wasm" | tr -d '[:space:]')"
brotli_bytes="$(brotli -c -q 11 "$wasm" | wc -c | tr -d '[:space:]')"

summary="$(
  cat <<SUMMARY
### Shipped WASM asset

| Measure | Bytes |
| --- | ---: |
| \`${wasm#"${dist_root}/"}\` as served | ${raw_bytes} |
| name section | ${name_section_bytes} |
| Brotli -q 11 of the served asset | ${brotli_bytes} |
| Brotli budget | ${brotli_budget_bytes} |
SUMMARY
)"
printf '%s\n' "$summary"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  printf '%s\n' "$summary" >>"$GITHUB_STEP_SUMMARY"
fi

if ((name_section_bytes > 0)); then
  echo "shipped WASM gate failed: the served asset carries a ${name_section_bytes}-byte name section; build it with wasm-bindgen --remove-name-section" >&2
  exit 1
fi
if ((brotli_bytes > brotli_budget_bytes)); then
  echo "shipped WASM Brotli budget exceeded: ${brotli_bytes} bytes > ${brotli_budget_bytes} bytes" >&2
  exit 1
fi
echo "shipped WASM gate passed"
