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
    if [[ -n "${GIT_STATUS_OUTPUT:-}" ]]; then
      printf '%s\n' "$GIT_STATUS_OUTPUT"
    fi
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

: >"$cargo_log"
: >"$git_log"
json_output="$(run_helper --json)"
python3 - "$json_output" <<'PY'
import json
import sys

summary = json.loads(sys.argv[1])
expected_paths = [
    "README.md",
    "CLAUDE.md",
    "docs/architecture/daxis-control-plane-architecture.md",
    "crates/daxis-query/src/lib.rs",
    "crates/daxis-query/tests/contracts.rs",
]

assert summary["schema_version"] == "daxis.external_state.v1"
assert summary["daxis_commit_sha"] == "0123456789abcdef0123456789abcdef01234567"
assert summary["daxis_ref"] == "main"
assert summary["daxis_origin_remote_url"] == "https://github.com/daxis-io/daxis-platform.git"
assert summary["daxis_worktree_status"] == "clean"
assert summary["daxis_worktree_status_lines"] == []
assert summary["daxis_worktree_review_required"] == "clean"
assert summary["active_architecture_paths"] == expected_paths
assert summary["contract_test"]["command"] == "cargo test -p daxis-query --test contracts"
assert summary["contract_test"]["exit_status"] == 0
assert summary["architecture_scan"]["command"] == (
    'rg -n "Axon browser WASM|Browser read compute|Headless query gateway" '
    "README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query"
)
assert summary["architecture_scan"]["exit_status"] == 0
PY

dirty_json_output="$(
  GIT_STATUS_OUTPUT=$' M docs/architecture/daxis-control-plane-architecture.md\n?? tmp-proof.md' \
    run_helper --json
)"
python3 - "$dirty_json_output" <<'PY'
import json
import sys

summary = json.loads(sys.argv[1])
assert summary["daxis_worktree_status"] == "dirty"
assert summary["daxis_worktree_status_lines"] == [
    " M docs/architecture/daxis-control-plane-architecture.md",
    "?? tmp-proof.md",
]
assert summary["daxis_worktree_review_required"] == "dirty_reviewed_or_dirty_rejected"
PY

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
