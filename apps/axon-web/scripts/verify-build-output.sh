#!/usr/bin/env bash

# Guards the browser query path's build output.
#
# The coordinator worker spawns a nested child worker. If the bundler is given a URL it cannot
# statically analyse, it copies the child's TypeScript source through as a plain asset instead of
# compiling it. Dev servers transpile on the fly and hide this; in production the browser receives
# TypeScript, fails to parse it, and every query dies with "child worker crashed".

set -euo pipefail

dist_root="${1:-}"
expected_runtime_tier="${2:-${AXON_BROWSER_RUNTIME_BUILD_TIER:-standard}}"
if [[ -z "${dist_root}" ]]; then
  echo "usage: verify-build-output.sh <dist-directory>" >&2
  exit 2
fi
if [[ ! -d "${dist_root}/assets" ]]; then
  echo "FAIL: '${dist_root}/assets' is not a directory" >&2
  exit 1
fi

failures=0

fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

pass() {
  echo "ok: $1"
}

# 1. No TypeScript may ever reach the browser.
stray_ts=$(find "${dist_root}" -name '*.ts' -not -name '*.d.ts' | head -20)
if [[ -n "${stray_ts}" ]]; then
  fail "TypeScript sources were emitted into the build output:
${stray_ts}"
else
  pass "no TypeScript sources in the build output"
fi

# 2. Both workers must exist as compiled JavaScript.
coordinator=$(find "${dist_root}/assets" -name 'sandbox-query-worker-*.js' | head -1)
child=$(find "${dist_root}/assets" -name 'sandbox-query-child-worker-*.js' | head -1)
[[ -n "${coordinator}" ]] || fail "no compiled coordinator worker chunk (sandbox-query-worker-*.js)"
[[ -n "${child}" ]] || fail "no compiled child worker chunk (sandbox-query-child-worker-*.js)"
if [[ -n "${coordinator}" && -n "${child}" ]]; then
  pass "compiled coordinator and child worker chunks are present"
fi

# 3. The coordinator must point at the compiled child, not at a source file.
if [[ -n "${coordinator}" ]]; then
  if grep -qE 'sandbox-query-child-worker-[A-Za-z0-9_-]+\.ts' "${coordinator}"; then
    fail "the coordinator references a .ts child worker; it must reference the compiled .js chunk"
  elif grep -qE 'sandbox-query-child-worker-[A-Za-z0-9_-]+\.js' "${coordinator}"; then
    pass "the coordinator references the compiled child worker chunk"
  else
    fail "the coordinator references no child worker chunk at all"
  fi
fi

# 4. The engine bundle must ship.
wasm=$(find "${dist_root}/assets" -name '*.wasm' | head -1)
if [[ -n "${wasm}" ]]; then
  pass "wasm bundle is present ($(basename "${wasm}"))"
else
  fail "no .wasm bundle in the build output"
fi

# 5. The artifact must declare the same runtime tier that was selected for this build.
if node --experimental-strip-types "$(dirname "$0")/browser-runtime-build.ts" verify "${dist_root}" "${expected_runtime_tier}"; then
  pass "browser runtime artifact matches expected '${expected_runtime_tier}' tier"
else
  fail "browser runtime artifact does not match expected '${expected_runtime_tier}' tier"
fi

if [[ "${failures}" -ne 0 ]]; then
  echo "${failures} build-output check(s) failed" >&2
  exit 1
fi
echo "all build-output checks passed"
