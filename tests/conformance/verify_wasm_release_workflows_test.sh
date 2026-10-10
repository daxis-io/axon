#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verifier="$script_dir/verify_wasm_release_workflows.sh"
repo_root="$(git rev-parse --show-toplevel)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

bash "$verifier"

mkdir -p "$tmpdir/repo/.github/workflows"
cp "$repo_root/.github/workflows/ci.yml" "$tmpdir/repo/.github/workflows/ci.yml"
cp "$repo_root/.github/workflows/upgrade-rehearsal.yml" "$tmpdir/repo/.github/workflows/upgrade-rehearsal.yml"
cp "$repo_root/.github/workflows/deploy-axon-web.yml" "$tmpdir/repo/.github/workflows/deploy-axon-web.yml"
printf '%s\n' '      - run: vercel build --prod' >>"$tmpdir/repo/.github/workflows/deploy-axon-web.yml"
if AXON_WASM_RELEASE_REPO_ROOT="$tmpdir/repo" bash "$verifier" >/dev/null 2>&1; then
  echo "release workflow verifier accepted a deployment rebuild" >&2
  exit 1
fi

reset_workflows() {
  cp "$repo_root/.github/workflows/ci.yml" "$tmpdir/repo/.github/workflows/ci.yml"
  cp "$repo_root/.github/workflows/upgrade-rehearsal.yml" "$tmpdir/repo/.github/workflows/upgrade-rehearsal.yml"
  cp "$repo_root/.github/workflows/deploy-axon-web.yml" "$tmpdir/repo/.github/workflows/deploy-axon-web.yml"
}

expect_rejected() {
  local description="$1"
  if AXON_WASM_RELEASE_REPO_ROOT="$tmpdir/repo" bash "$verifier" >/dev/null 2>&1; then
    echo "release workflow verifier accepted ${description}" >&2
    exit 1
  fi
}

reset_workflows
sed -i.bak '/AXON_RUNTIME_BUILD_REQUIRE_CLEAN/d' "$tmpdir/repo/.github/workflows/ci.yml"
expect_rejected "a qualified build that is not bound to a clean exact commit"

reset_workflows
sed -i.bak 's/static "$QUALIFIED_COMMIT"/static/' "$tmpdir/repo/.github/workflows/deploy-axon-web.yml"
expect_rejected "a deployment that does not check the artifact's recorded commit"

echo "WASM release workflow negative rebuild and commit-identity coverage passed"
