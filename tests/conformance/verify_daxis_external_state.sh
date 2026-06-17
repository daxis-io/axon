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

run_json_summary() {
	if [[ ! -d "$daxis_repo_root" ]]; then
		echo "missing Daxis repository root: $daxis_repo_root" >&2
		exit 1
	fi

	verify_daxis_paths
	(
		cd "$daxis_repo_root"
		local commit_sha
		local daxis_ref
		local origin_remote_url
		local worktree_status_output
		local contract_test_output
		local contract_test_status
		local architecture_scan_output
		local architecture_scan_status

		commit_sha="$(git rev-parse HEAD)"
		daxis_ref="$(git rev-parse --abbrev-ref HEAD)"
		origin_remote_url="$(git remote get-url origin)"
		worktree_status_output="$(git status --short)"

		set +e
		contract_test_output="$(cargo test -p daxis-query --test contracts 2>&1)"
		contract_test_status=$?
		architecture_scan_output="$(
			rg -n "Axon browser WASM|Browser read compute|Headless query gateway" \
				README.md \
				CLAUDE.md \
				docs/architecture/daxis-control-plane-architecture.md \
				crates/daxis-query 2>&1
		)"
		architecture_scan_status=$?
		set -e

		DAXIS_REPO_ROOT="$PWD" \
			DAXIS_COMMIT_SHA="$commit_sha" \
			DAXIS_REF="$daxis_ref" \
			DAXIS_ORIGIN_REMOTE_URL="$origin_remote_url" \
			DAXIS_WORKTREE_STATUS_OUTPUT="$worktree_status_output" \
			DAXIS_CONTRACT_TEST_OUTPUT="$contract_test_output" \
			DAXIS_CONTRACT_TEST_STATUS="$contract_test_status" \
			DAXIS_ARCHITECTURE_SCAN_OUTPUT="$architecture_scan_output" \
			DAXIS_ARCHITECTURE_SCAN_STATUS="$architecture_scan_status" \
			python3 - <<'PY'
import json
import os

worktree_status_lines = [
    line
    for line in os.environ["DAXIS_WORKTREE_STATUS_OUTPUT"].splitlines()
    if line
]
worktree_status = "dirty" if worktree_status_lines else "clean"

summary = {
    "schema_version": "daxis.external_state.v1",
    "daxis_repo_root": os.environ["DAXIS_REPO_ROOT"],
    "daxis_commit_sha": os.environ["DAXIS_COMMIT_SHA"],
    "daxis_ref": os.environ["DAXIS_REF"],
    "daxis_origin_remote_url": os.environ["DAXIS_ORIGIN_REMOTE_URL"],
    "daxis_worktree_status": worktree_status,
    "daxis_worktree_status_lines": worktree_status_lines,
    "daxis_worktree_review_required": (
        "dirty_reviewed_or_dirty_rejected" if worktree_status == "dirty" else "clean"
    ),
    "active_architecture_paths": [
        "README.md",
        "CLAUDE.md",
        "docs/architecture/daxis-control-plane-architecture.md",
        "crates/daxis-query/src/lib.rs",
        "crates/daxis-query/tests/contracts.rs",
    ],
    "contract_test": {
        "command": "cargo test -p daxis-query --test contracts",
        "exit_status": int(os.environ["DAXIS_CONTRACT_TEST_STATUS"]),
        "output": os.environ["DAXIS_CONTRACT_TEST_OUTPUT"],
    },
    "architecture_scan": {
        "command": (
            'rg -n "Axon browser WASM|Browser read compute|Headless query gateway" '
            "README.md CLAUDE.md docs/architecture/daxis-control-plane-architecture.md crates/daxis-query"
        ),
        "exit_status": int(os.environ["DAXIS_ARCHITECTURE_SCAN_STATUS"]),
        "output": os.environ["DAXIS_ARCHITECTURE_SCAN_OUTPUT"],
    },
}

print(json.dumps(summary, sort_keys=True))
PY

		if [[ "$contract_test_status" -ne 0 || "$architecture_scan_status" -ne 0 ]]; then
			exit 1
		fi
	)
}

sha256_file() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
		return
	fi

	shasum -a 256 "$1" | awk '{print $1}'
}

write_json_artifact() {
	local output_path="$1"
	if [[ -z "$output_path" ]]; then
		echo "usage: $0 --write-json <path>" >&2
		exit 2
	fi

	local output_dir
	output_dir="$(dirname "$output_path")"
	mkdir -p "$output_dir"

	local temp_path
	temp_path="$output_dir/.external-state.$(basename "$output_path").$$.tmp"
	if run_json_summary >"$temp_path"; then
		mv "$temp_path" "$output_path"
	else
		rm -f "$temp_path"
		return 1
	fi

	printf 'daxis_external_state_json_path=%s\n' "$output_path"
	printf 'daxis_external_state_json_sha256=%s\n' "$(sha256_file "$output_path")"
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
--json)
	run_json_summary
	;;
--write-json)
	if [[ $# -ne 2 ]]; then
		echo "usage: $0 --write-json <path>" >&2
		exit 2
	fi
	write_json_artifact "$2"
	;;
"")
	run_checks
	;;
*)
	echo "usage: $0 [--list|--json|--write-json <path>]" >&2
	exit 2
	;;
esac
