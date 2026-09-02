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

echo "WASM release workflow negative rebuild coverage passed"
