#!/usr/bin/env bash

set -euo pipefail

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

daxis_repo="$tmpdir/daxis-platform"
stub_bin="$tmpdir/bin"
cargo_log="$tmpdir/cargo.log"
git_log="$tmpdir/git.log"

mkdir -p \
  "$daxis_repo/docs/architecture" \
  "$daxis_repo/crates/daxis-query/src" \
  "$daxis_repo/crates/daxis-query/tests" \
  "$stub_bin"

cat >"$stub_bin/cargo" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$CARGO_LOG"
exit 0
SH
chmod +x "$stub_bin/cargo"

cat >"$stub_bin/git" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$GIT_LOG"
case "$*" in
  "rev-parse HEAD")
    printf '%s\n' "0123456789abcdef0123456789abcdef01234567"
    ;;
  "rev-parse --abbrev-ref HEAD")
    printf '%s\n' "main"
    ;;
  "remote get-url origin")
    printf '%s\n' "https://github.com/daxis-io/daxis-platform.git"
    ;;
  "status --short")
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
SH
chmod +x "$stub_bin/git"

write_valid_daxis_repo() {
  cat >"$daxis_repo/README.md" <<'EOF'
# Daxis

Browser read compute uses Axon browser WASM for bounded interactive reads.
EOF
  cat >"$daxis_repo/CLAUDE.md" <<'EOF'
# Daxis Notes

Headless query gateway routes validated SQL to Axon or server fallback.
EOF
  cat >"$daxis_repo/docs/architecture/daxis-control-plane-architecture.md" <<'EOF'
# Daxis Control Plane Architecture

Browser read compute is Axon browser WASM when policy allows it.
EOF
  cat >"$daxis_repo/crates/daxis-query/src/lib.rs" <<'EOF'
//! Headless query gateway for Daxis query surfaces.
EOF
  cat >"$daxis_repo/crates/daxis-query/tests/contracts.rs" <<'EOF'
// Axon browser WASM contract coverage lives here.
EOF
}

run_helper() {
  CARGO_LOG="$cargo_log" \
    GIT_LOG="$git_log" \
    AXON_DAXIS_PLATFORM_REPO_ROOT="$daxis_repo" \
    PATH="$stub_bin:$PATH" \
    bash tests/conformance/verify_daxis_external_state.sh "$@"
}

write_valid_daxis_repo

list_output="$(run_helper --list)"
for expected in \
  "git rev-parse HEAD" \
  "git rev-parse --abbrev-ref HEAD" \
  "git remote get-url origin" \
  "git status --short" \
  "cargo test -p daxis-query --test contracts" \
  "rg -n \"Axon browser WASM|Browser read compute|Headless query gateway\" README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query"; do
  if [[ "$list_output" != *"$expected"* ]]; then
    echo "Daxis external-state helper is missing command: $expected" >&2
    exit 1
  fi
done

run_helper >/dev/null
if [[ "$(cat "$cargo_log")" != "test -p daxis-query --test contracts" ]]; then
  echo "Daxis external-state helper did not run the Daxis contract test command" >&2
  exit 1
fi
expected_git_log=$'rev-parse HEAD\nrev-parse --abbrev-ref HEAD\nremote get-url origin\nstatus --short'
if [[ "$(cat "$git_log")" != "$expected_git_log" ]]; then
  echo "Daxis external-state helper did not capture Daxis git identity, branch, remote, and status" >&2
  exit 1
fi

write_valid_daxis_repo
cat >"$daxis_repo/README.md" <<'EOF'
# Daxis

Remote execution only.
EOF
cat >"$daxis_repo/CLAUDE.md" <<'EOF'
# Daxis Notes

Remote execution only.
EOF
cat >"$daxis_repo/docs/architecture/daxis-control-plane-architecture.md" <<'EOF'
# Daxis Control Plane Architecture

Remote execution only.
EOF
cat >"$daxis_repo/crates/daxis-query/src/lib.rs" <<'EOF'
//! Query service.
EOF
cat >"$daxis_repo/crates/daxis-query/tests/contracts.rs" <<'EOF'
// Contract tests.
EOF

if run_helper >/dev/null 2>&1; then
  echo "expected missing Daxis architecture terminology to be rejected" >&2
  exit 1
fi

write_valid_daxis_repo
rm "$daxis_repo/crates/daxis-query/tests/contracts.rs"

if run_helper >/dev/null 2>&1; then
  echo "expected missing Daxis contract test path to be rejected" >&2
  exit 1
fi

if run_helper --unknown >/dev/null 2>&1; then
  echo "expected unknown Daxis external-state helper option to be rejected" >&2
  exit 1
fi

echo "Daxis external-state helper regression coverage passed"
