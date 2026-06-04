#!/usr/bin/env bash

set -euo pipefail

daxis_repo_root="${AXON_DAXIS_PLATFORM_REPO_ROOT:-../daxis-platform}"

commands=(
  "git rev-parse HEAD"
  "git rev-parse --abbrev-ref HEAD"
  "git remote get-url origin"
  "git status --short"
  "cargo test -p daxis-query --test contracts"
  "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query"
)

required_paths=(
  "README.md"
  "CLAUDE.md"
  "docs/architecture/daxis-control-plane-architecture.md"
  "crates/daxis-query/src/lib.rs"
  "crates/daxis-query/tests/contracts.rs"
)

list_commands() {
  printf "%s\n" "${commands[@]}"
}

run_step() {
  printf '+'
  local arg
  for arg in "$@"; do
    printf ' %q' "$arg"
  done
  printf '\n'
  "$@"
}

verify_daxis_paths() {
  local path
  for path in "${required_paths[@]}"; do
    if [[ ! -f "$daxis_repo_root/$path" ]]; then
      echo "missing Daxis external-state path: $path" >&2
      exit 1
    fi
  done
}

run_checks() {
  if [[ ! -d "$daxis_repo_root" ]]; then
    echo "missing Daxis repository root: $daxis_repo_root" >&2
    exit 1
  fi

  verify_daxis_paths
  (
    cd "$daxis_repo_root"
    run_step git rev-parse HEAD
    run_step git rev-parse --abbrev-ref HEAD
    run_step git remote get-url origin
    run_step git status --short
    run_step cargo test -p daxis-query --test contracts
    run_step rg -n "Axon browser WASM|Browser read compute|Headless query gateway" \
      README.md \
      CLAUDE.md \
      docs/architecture/daxis-control-plane-architecture.md \
      crates/daxis-query
  )
}

case "${1:-}" in
  --list)
    list_commands
    ;;
  "")
    run_checks
    ;;
  *)
    echo "usage: $0 [--list]" >&2
    exit 2
    ;;
esac
