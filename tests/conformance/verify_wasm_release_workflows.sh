#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "missing required tool: rg" >&2
  exit 127
fi

repo_root="${AXON_WASM_RELEASE_REPO_ROOT:-$(git rev-parse --show-toplevel)}"
ci="$repo_root/.github/workflows/ci.yml"
rehearsal="$repo_root/.github/workflows/upgrade-rehearsal.yml"
deploy="$repo_root/.github/workflows/deploy-axon-web.yml"

require() {
  local pattern="$1"
  local file="$2"
  local message="$3"
  if ! rg -Uq -- "$pattern" "$file"; then
    echo "WASM release workflow verification failed: $message" >&2
    exit 1
  fi
}

require 'wasm-stack-release-qualification:' "$ci" "CI has no release qualification fan-in job"
require 'needs:[[:space:]]+\[rust, browser-external-memory-artifact, browser-datafusion-wasm-size\]' "$ci" "release qualification does not depend on every required Axon gate"
require '--production --ci' "$ci" "ordinary CI does not automatically enforce final mode for a populated lock"
require '--production --final' "$ci" "release qualification does not run the production verifier in final mode"
require 'Report unavailable deployment credentials[[:space:][:print:]]*exit 1' "$ci" "a populated final lock can silently skip release qualification when deployment credentials are absent"
require 'axon-web-qualified-prebuilt' "$ci" "CI does not publish the qualified Vercel prebuilt artifact"
require 'axon-web-qualified-release-evidence' "$ci" "CI does not publish release attestation evidence"
require 'vercel build --prod' "$ci" "CI does not build the qualified production-environment artifact"
require 'Generate immutable WASM stack release attestation[[:space:][:print:]]*compressed_bytes > 6291456' "$ci" "the exact production artifact is not checked against the Brotli size budget"
require '--production --ci' "$rehearsal" "upgrade rehearsal does not automatically enforce final mode for a populated lock"

if rg -q '^  push:' "$deploy"; then
  echo "WASM release workflow verification failed: deployment still runs directly on push" >&2
  exit 1
fi
if rg -q 'vercel build' "$deploy"; then
  echo "WASM release workflow verification failed: deployment rebuilds instead of consuming the qualified artifact" >&2
  exit 1
fi
require 'actions/github-script@' "$deploy" "deployment does not verify the selected CI run identity"
require 'actions/download-artifact@v4' "$deploy" "deployment does not download qualified artifacts"
require 'run-id:.*qualified_ci_run_id' "$deploy" "deployment artifact download is not bound to the selected CI run"
require '--production --final' "$deploy" "deployment does not repeat final stack verification"
require 'verify_wasm_stack_attestation\.sh' "$deploy" "deployment does not verify the release attestation against downloaded bytes"
require 'vercel deploy --prebuilt --prod --skip-domain' "$deploy" "staging does not create an unaliased production-environment deployment"
require 'vercel promote .*--yes' "$deploy" "production does not promote the previously staged immutable deployment"
require 'axon-web-staged-production' "$deploy" "staged deployment identity is not persisted for later promotion"

echo "WASM release qualification and exact-artifact promotion workflow contract passed"
