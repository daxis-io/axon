#!/usr/bin/env bash

# Guards a deployed axon-web origin.
#
# A catch-all SPA rewrite answers every unknown path with index.html and a 200. Workers and WASM
# assets requested against such a host receive HTML instead of a 404, so a missing asset presents
# as an unexplained worker crash rather than a missing file. This asserts the opposite: real assets
# resolve with the right content type, and missing ones actually 404.

set -euo pipefail

deploy_url="${1:-}"
dist_root="${2:-}"
if [[ -z "${deploy_url}" || -z "${dist_root}" ]]; then
  echo "usage: verify-deployment.sh <deployment-url> <dist-directory>" >&2
  exit 2
fi
deploy_url="${deploy_url%/}"

failures=0

fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

# Protected deployments (Vercel Deployment Protection) answer every request with a redirect to an
# SSO login, so the assets are unreachable without a bypass. Send the automation bypass header when
# one is configured.
curl_args=(-sS --max-time 60)
if [[ -n "${VERCEL_AUTOMATION_BYPASS_SECRET:-}" ]]; then
  curl_args+=(-H "x-vercel-protection-bypass: ${VERCEL_AUTOMATION_BYPASS_SECRET}")
fi

# Prints "<status> <content-type>" for a URL.
probe() {
  curl "${curl_args[@]}" -o /dev/null -w '%{http_code} %{content_type}' "$1"
}

# A protected deployment is not a defect, so report it and skip rather than failing every check.
redirect_target=$(curl "${curl_args[@]}" -o /dev/null -w '%{redirect_url}' "${deploy_url}/")
if [[ "${redirect_target}" == *"vercel.com/sso"* ]]; then
  echo "Deployment protection is enabled on ${deploy_url} and no bypass secret is configured;"
  echo "skipping asset verification. Set VERCEL_AUTOMATION_BYPASS_SECRET to verify protected"
  echo "deployments (Project Settings -> Deployment Protection -> Protection Bypass for Automation)."
  exit 0
fi

expect_asset() {
  local path="$1" expected_type="$2" result status content_type
  result=$(probe "${deploy_url}${path}")
  status="${result%% *}"
  content_type="${result#* }"
  if [[ "${status}" != "200" ]]; then
    fail "${path} returned ${status}, expected 200"
    return
  fi
  if [[ "${content_type}" != *"${expected_type}"* ]]; then
    fail "${path} served as '${content_type}', expected '${expected_type}'"
    return
  fi
  echo "ok: ${path} -> ${status} ${content_type}"
}

asset_path() {
  local match
  match=$(find "${dist_root}/assets" -name "$1" | head -1)
  [[ -n "${match}" ]] || return 1
  echo "/assets/$(basename "${match}")"
}

expect_asset "/" "text/html"

for pattern in 'sandbox-query-worker-*.js' 'sandbox-query-child-worker-*.js'; do
  if path=$(asset_path "${pattern}"); then
    expect_asset "${path}" "javascript"
  else
    fail "no local build artifact matching ${pattern}; cannot verify the deployment"
  fi
done

if path=$(asset_path '*.wasm'); then
  expect_asset "${path}" "application/wasm"
else
  fail "no local .wasm build artifact; cannot verify the deployment"
fi

# The SPA rewrite must not swallow missing assets.
missing=$(probe "${deploy_url}/assets/deliberately-missing-asset.js")
missing_status="${missing%% *}"
if [[ "${missing_status}" == "404" ]]; then
  echo "ok: a missing asset returns 404"
else
  fail "a missing asset returned ${missing_status} instead of 404; a catch-all rewrite is masking absent files"
fi

# History-API routes must still reach the app.
expect_asset "/connect" "text/html"

if [[ "${failures}" -ne 0 ]]; then
  echo "${failures} deployment check(s) failed" >&2
  exit 1
fi
echo "all deployment checks passed"
