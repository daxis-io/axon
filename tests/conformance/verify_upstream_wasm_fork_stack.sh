#!/usr/bin/env bash

set -euo pipefail

mode="final"
repo_root="${AXON_UPSTREAM_WASM_POC_REPO_ROOT:-}"
metadata_file="${AXON_UPSTREAM_WASM_POC_METADATA_FILE:-}"

usage() {
  cat <<'EOF'
Usage: verify_upstream_wasm_fork_stack.sh [--bootstrap|--allow-unset|--final]
                                           [--repo-root PATH]
                                           [--metadata-file PATH]

Bootstrap mode permits UNSET candidate/stack revisions and an absent browser Cargo.lock while leaf
forks are being built. Final mode is the default and rejects either condition.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap | --allow-unset)
      mode="bootstrap"
      shift
      ;;
    --final)
      mode="final"
      shift
      ;;
    --repo-root)
      [[ $# -ge 2 ]] || {
        echo "stack verifier failed: --repo-root requires a path" >&2
        exit 2
      }
      repo_root="$2"
      shift 2
      ;;
    --metadata-file)
      [[ $# -ge 2 ]] || {
        echo "stack verifier failed: --metadata-file requires a path" >&2
        exit 2
      }
      metadata_file="$2"
      shift 2
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      echo "stack verifier failed: unknown argument $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo_root" ]]; then
  repo_root="$(git rev-parse --show-toplevel)"
fi

python3 - "$repo_root" "$mode" "$metadata_file" <<'PY'
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections import defaultdict, deque
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit, urlunsplit

try:
    import tomllib
except ModuleNotFoundError as error:
    raise SystemExit(
        "stack verifier failed: Python 3.11 or newer with tomllib is required"
    ) from error


def fail(message: str) -> None:
    raise SystemExit(f"stack verifier failed: {message}")


def positive_timeout_seconds(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError:
        fail(f"{name} must be an integer number of seconds")
    if not 1 <= value <= 120:
        fail(f"{name} must be between 1 and 120 seconds")
    return value


def load_toml(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"missing file: {path}")
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot parse {path}: {error}")


def normalized_git_url(value: str) -> str:
    if value.startswith("git+"):
        value = value[4:]
    value = value.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    if value.endswith(".git"):
        value = value[:-4]
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc:
        path = parsed.path.rstrip("/")
        return urlunsplit(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                path,
                "",
                "",
            )
        )
    return value


repo_root = Path(sys.argv[1]).resolve()
mode = sys.argv[2]
metadata_override = Path(sys.argv[3]).resolve() if sys.argv[3] else None
poc_root = repo_root / "poc" / "upstream-wasm-fork-stack"
stack_lock_path = poc_root / "stack.lock.toml"
browser_manifest = poc_root / "Cargo.toml"
browser_lock = poc_root / "Cargo.lock"
stack = load_toml(stack_lock_path)

required_repositories = (
    "arrow_rs",
    "object_store",
    "datafusion",
    "delta_kernel",
    "delta_rs",
)
revision_pattern = re.compile(r"^[0-9a-f]{40}$")
git_environment = os.environ.copy()
git_environment["GIT_TERMINAL_PROMPT"] = "0"
git_remote_timeout_seconds = positive_timeout_seconds(
    "AXON_UPSTREAM_WASM_GIT_TIMEOUT_SECONDS",
    30,
)


def run_remote_git(arguments: list[str], operation: str) -> subprocess.CompletedProcess[str]:
    last_diagnostic = "remote Git operation failed"
    for attempt in range(2):
        try:
            process = subprocess.run(
                ["git", *arguments],
                check=False,
                env=git_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=git_remote_timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            last_diagnostic = f"timed out after {git_remote_timeout_seconds} seconds"
        else:
            if process.returncode == 0:
                return process
            last_diagnostic = process.stderr.strip() or (
                f"git exited with status {process.returncode}"
            )
        if attempt == 0:
            continue
        fail(f"{operation}: {last_diagnostic}")
    raise AssertionError("bounded Git retry loop did not terminate")

if stack.get("schema") != 1:
    fail("stack.lock.toml schema must be 1")
if stack.get("target") != "wasm32-unknown-unknown":
    fail("stack.lock.toml target must be wasm32-unknown-unknown")
if not revision_pattern.fullmatch(str(stack.get("axon_base_rev", ""))):
    fail("axon_base_rev must be a 40-character lowercase hexadecimal revision")

repositories = stack.get("repositories")
if not isinstance(repositories, dict):
    fail("stack.lock.toml must contain a repositories table")

validated: list[tuple[str, str, dict[str, str]]] = []
for name in required_repositories:
    entry = repositories.get(name)
    if not isinstance(entry, dict):
        fail(f"missing repository entry: {name}")
    for url_field in ("canonical", "fork"):
        value = entry.get(url_field)
        if not isinstance(value, str) or not value.strip():
            fail(f"{name}.{url_field} must be a non-empty URL")

    revisions: dict[str, str] = {}
    for field in ("base_rev", "candidate_rev", "stack_rev"):
        value = entry.get(field)
        if value == "UNSET":
            if field == "base_rev":
                fail(f"{name}.{field} cannot be UNSET")
            if mode == "final":
                fail(f"{name}.{field} is UNSET in final mode")
            revisions[field] = value
            continue
        if not isinstance(value, str) or not revision_pattern.fullmatch(value):
            fail(
                f"{name}.{field} must be a 40-character lowercase hexadecimal revision"
            )
        revisions[field] = value
    validated.append((name, entry["fork"], revisions))

remote_refs: dict[str, set[str]] = {}
for name, fork, revisions in validated:
    if fork not in remote_refs:
        process = run_remote_git(
            ["ls-remote", fork],
            f"cannot inspect fork for {name}",
        )
        remote_refs[fork] = {
            line.split()[0]
            for line in process.stdout.splitlines()
            if line.split()
        }

    published_revision = revisions["stack_rev"]
    if published_revision != "UNSET" and published_revision not in remote_refs[fork]:
        fail(f"{name}.stack_rev is not reachable from its fork: {published_revision}")

    ancestor_candidates = [
        (field, revision)
        for field, revision in revisions.items()
        if revision != "UNSET" and revision not in remote_refs[fork]
    ]
    if not ancestor_candidates:
        continue
    if published_revision == "UNSET":
        field, revision = ancestor_candidates[0]
        fail(f"{name}.{field} is not reachable from its fork: {revision}")

    with TemporaryDirectory(prefix="axon-upstream-wasm-reachability-") as directory:
        repository = Path(directory) / "repository.git"
        initialized = subprocess.run(
            ["git", "init", "--bare", str(repository)],
            check=False,
            env=git_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if initialized.returncode != 0:
            fail(
                f"cannot initialize reachability check for {name}: "
                + (initialized.stderr.strip() or "git init failed")
            )
        run_remote_git(
            [
                "-C",
                str(repository),
                "fetch",
                "--no-tags",
                "--filter=blob:none",
                fork,
                f"{published_revision}:refs/axon/published-stack",
            ],
            f"cannot inspect published stack ancestry for {name}",
        )
        for field, revision in ancestor_candidates:
            reachable = subprocess.run(
                [
                    "git",
                    "-C",
                    str(repository),
                    "merge-base",
                    "--is-ancestor",
                    revision,
                    "refs/axon/published-stack",
                ],
                check=False,
                env=git_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if reachable.returncode != 0:
                fail(f"{name}.{field} is not reachable from its fork: {revision}")

if poc_root.exists():
    for manifest in sorted(poc_root.rglob("Cargo.toml")):
        for line_number, line in enumerate(
            manifest.read_text(encoding="utf-8").splitlines(), start=1
        ):
            code = line.split("#", 1)[0]
            if re.search(r"\bbranch\s*=", code):
                relative = manifest.relative_to(repo_root)
                fail(f"mutable branch dependency in {relative}:{line_number}")

if mode == "final" and not browser_lock.is_file():
    fail(f"missing browser Cargo lock: {browser_lock}")

if browser_lock.is_file():
    cargo_lock = load_toml(browser_lock)
    lock_packages = cargo_lock.get("package", [])
    if not isinstance(lock_packages, list):
        fail(f"invalid package list in {browser_lock}")

    for name, fork, revisions in validated:
        stack_revision = revisions["stack_rev"]
        if stack_revision == "UNSET":
            continue
        fork_url = normalized_git_url(fork)
        matching_sources = {
            package["source"]
            for package in lock_packages
            if isinstance(package, dict)
            and isinstance(package.get("source"), str)
            and normalized_git_url(package["source"]) == fork_url
        }
        if not matching_sources:
            fail(f"Cargo.lock has no source for {name}: {fork}")
        for source in sorted(matching_sources):
            locked_revision = source.rsplit("#", 1)[-1] if "#" in source else ""
            if locked_revision != stack_revision:
                fail(
                    f"Cargo.lock source mismatch for {name}: "
                    f"expected {stack_revision}, found {locked_revision or source}"
                )

metadata: dict | None = None
if metadata_override is not None:
    try:
        metadata = json.loads(metadata_override.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot parse metadata file {metadata_override}: {error}")
elif browser_manifest.is_file() and browser_lock.is_file():
    process = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(browser_manifest),
            "--format-version",
            "1",
            "--filter-platform",
            "wasm32-unknown-unknown",
            "--locked",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        fail(
            "target-filtered cargo metadata failed: "
            + (process.stderr.strip() or "unknown cargo failure")
        )
    try:
        metadata = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        fail(f"cargo metadata returned invalid JSON: {error}")
elif mode == "final":
    fail(f"missing browser workspace manifest: {browser_manifest}")

active_package_count = 0
if metadata is not None:
    packages = {
        package["id"]: package
        for package in metadata.get("packages", [])
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    resolve = metadata.get("resolve")
    if not isinstance(resolve, dict):
        fail("target-filtered metadata has no resolve graph")
    nodes = {
        node["id"]: node
        for node in resolve.get("nodes", [])
        if isinstance(node, dict) and isinstance(node.get("id"), str)
    }
    queue = deque(metadata.get("workspace_members", []))
    active: set[str] = set()
    while queue:
        package_id = queue.popleft()
        if package_id in active:
            continue
        active.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            continue
        for dependency in node.get("deps", []):
            kinds = dependency.get("dep_kinds", [])
            if not kinds or any(
                kind.get("kind") in (None, "normal", "build")
                for kind in kinds
                if isinstance(kind, dict)
            ):
                dependency_id = dependency.get("pkg")
                if isinstance(dependency_id, str):
                    queue.append(dependency_id)

    active_package_count = len(active)
    guarded: defaultdict[str, set[tuple[str, str]]] = defaultdict(set)
    for package_id in active:
        package = packages.get(package_id)
        if package is None:
            continue
        name = package.get("name", "")
        is_arrow = name == "arrow" or name.startswith("arrow-")
        is_datafusion = name == "datafusion" or name.startswith("datafusion-")
        is_other_guarded = name in {
            "parquet",
            "object_store",
            "delta_kernel",
            "buoyant_kernel",
        }
        if not (is_arrow or is_datafusion or is_other_guarded):
            continue
        guarded[name].add(
            (
                str(package.get("version", "")),
                normalized_git_url(str(package.get("source") or "path")),
            )
        )

    for package_name, identities in sorted(guarded.items()):
        if len(identities) > 1:
            formatted = ", ".join(
                f"{version}@{source}" for version, source in sorted(identities)
            )
            fail(
                f"duplicate browser package source for {package_name}: {formatted}"
            )

    kernel_identities = set()
    for kernel_name in ("delta_kernel", "buoyant_kernel"):
        kernel_identities.update(guarded.get(kernel_name, set()))
    if len(kernel_identities) > 1:
        formatted = ", ".join(
            f"{version}@{source}" for version, source in sorted(kernel_identities)
        )
        fail(f"duplicate browser kernel source: {formatted}")

print(
    "upstream WASM fork stack verified "
    f"mode={mode} repositories={len(validated)} "
    f"graph_packages={active_package_count}"
)
PY
