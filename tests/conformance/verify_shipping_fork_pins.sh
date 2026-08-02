#!/usr/bin/env bash

set -euo pipefail

repo_root=""
policy_file=""

usage() {
  cat <<'EOF'
Usage: verify_shipping_fork_pins.sh [--repo-root PATH] [--policy-file PATH]

Verifies that shipping Cargo manifests use only approved immutable Daxis fork revisions, that the
root lockfile resolves those exact revisions, and that each revision is the tip of its approved
publication ref.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root)
      [[ $# -ge 2 ]] || {
        echo "shipping fork pin verifier failed: --repo-root requires a path" >&2
        exit 2
      }
      repo_root="$2"
      shift 2
      ;;
    --policy-file)
      [[ $# -ge 2 ]] || {
        echo "shipping fork pin verifier failed: --policy-file requires a path" >&2
        exit 2
      }
      policy_file="$2"
      shift 2
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      echo "shipping fork pin verifier failed: unknown argument $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$repo_root" ]]; then
  repo_root="$(git rev-parse --show-toplevel)"
fi
if [[ -z "$policy_file" ]]; then
  policy_file="$repo_root/.github/shipping-fork-pins.toml"
fi

python3 - "$repo_root" "$policy_file" <<'PY'
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

try:
    import tomllib
except ModuleNotFoundError as error:
    raise SystemExit(
        "shipping fork pin verifier failed: Python 3.11 or newer with tomllib is required"
    ) from error


def fail(message: str) -> None:
    raise SystemExit(f"shipping fork pin verifier failed: {message}")


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


def split_git_source(value: str) -> tuple[str, str, str]:
    if value.startswith("git+"):
        value = value[4:]
    without_fragment, fragment_separator, fragment = value.partition("#")
    repository, query_separator, query = without_fragment.partition("?")
    return (
        repository.rstrip("/"),
        query if query_separator else "",
        fragment if fragment_separator else "",
    )


def repository_identity(value: str) -> str:
    repository, _, _ = split_git_source(value)
    scp_match = None
    if "://" not in repository:
        scp_match = re.fullmatch(
            r"(?:[^/@:]+@)?(?P<host>[^/:]+):(?P<path>.+)",
            repository,
        )

    if scp_match is not None:
        scheme = "ssh"
        hostname = scp_match.group("host").lower().rstrip(".")
        port = None
        path = scp_match.group("path")
    else:
        parsed = urlsplit(repository)
        scheme = parsed.scheme.lower()
        if scheme not in {"file", "https", "ssh"}:
            fail(f"unsupported Git repository scheme: {scheme or 'missing'}")
        if scheme == "file":
            if parsed.username is not None or parsed.port is not None:
                fail("file Git repository URL cannot contain user information or a port")
            file_path = unquote(parsed.path).rstrip("/")
            if not file_path:
                fail("file Git repository URL must contain a path")
            return f"file://{file_path}"
        hostname = (parsed.hostname or "").lower().rstrip(".")
        if not hostname:
            fail("Git repository URL must contain a hostname")
        try:
            port = parsed.port
        except ValueError as error:
            fail(f"invalid Git repository port: {error}")
        if (scheme == "https" and port == 443) or (scheme == "ssh" and port == 22):
            port = None
        path = parsed.path

    path_parts = [unquote(part) for part in path.strip("/").split("/")]
    if not path_parts or any(part in {"", ".", ".."} for part in path_parts):
        fail("Git repository URL must contain a canonical repository path")
    if path_parts[-1].lower().endswith(".git"):
        path_parts[-1] = path_parts[-1][:-4]
    if not path_parts[-1]:
        fail("Git repository URL must contain a repository name")

    if hostname == "github.com":
        if len(path_parts) != 2:
            fail("GitHub repository URL must identify exactly one owner and repository")
        path_parts = [part.lower() for part in path_parts]

    host_identity = hostname if port is None else f"{hostname}:{port}"
    return f"{host_identity}/{'/'.join(path_parts)}"


dependency_table_names = ("dependencies", "dev-dependencies", "build-dependencies")


def git_dependencies_in_table(value: object, location: str):
    if not isinstance(value, dict):
        return
    for dependency_name, dependency in value.items():
        if isinstance(dependency, dict) and isinstance(dependency.get("git"), str):
            yield f"{location}.{dependency_name}", dependency


def manifest_git_dependencies(manifest: dict):
    for table_name in dependency_table_names:
        yield from git_dependencies_in_table(manifest.get(table_name), table_name)

    workspace = manifest.get("workspace")
    if isinstance(workspace, dict):
        yield from git_dependencies_in_table(
            workspace.get("dependencies"),
            "workspace.dependencies",
        )

    targets = manifest.get("target")
    if isinstance(targets, dict):
        for target_name, target in targets.items():
            if not isinstance(target, dict):
                continue
            for table_name in dependency_table_names:
                yield from git_dependencies_in_table(
                    target.get(table_name),
                    f"target.{target_name}.{table_name}",
                )

    patches = manifest.get("patch")
    if isinstance(patches, dict):
        for source_name, dependencies in patches.items():
            yield from git_dependencies_in_table(
                dependencies,
                f"patch.{source_name}",
            )

    yield from git_dependencies_in_table(manifest.get("replace"), "replace")


repo_root = Path(sys.argv[1]).resolve()
policy_path = Path(sys.argv[2]).resolve()
revision_pattern = re.compile(r"^[0-9a-f]{40}$")
publication_ref_pattern = re.compile(r"^refs/(heads|tags)/[^\s]+$")
git_environment = os.environ.copy()
git_environment["GIT_TERMINAL_PROMPT"] = "0"
git_remote_timeout_seconds = positive_timeout_seconds(
    "AXON_SHIPPING_FORK_GIT_TIMEOUT_SECONDS",
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

policy = load_toml(policy_path)
if policy.get("schema") != 1:
    fail("policy schema must be 1")
raw_pins = policy.get("pins")
if not isinstance(raw_pins, list) or not raw_pins:
    fail("policy must contain at least one pin")

approved: dict[str, tuple[str, str, str]] = {}
for index, pin in enumerate(raw_pins):
    if not isinstance(pin, dict):
        fail(f"pin {index} must be a table")
    repository = pin.get("repository")
    revision = pin.get("revision")
    publication_ref = pin.get("publication_ref")
    if not isinstance(repository, str) or not repository:
        fail(f"pin {index} repository must be a non-empty URL")
    repository_url, repository_query, repository_fragment = split_git_source(repository)
    if repository_query or repository_fragment:
        fail(f"approved repository URL cannot contain a query or fragment: {repository_url}")
    identity = repository_identity(repository_url)
    if identity in approved:
        fail(f"duplicate approved fork repository: {identity}")
    if not isinstance(revision, str) or not revision_pattern.fullmatch(revision):
        fail(f"approved revision must be a 40-character lowercase hexadecimal SHA: {identity}")
    if not isinstance(publication_ref, str) or not publication_ref_pattern.fullmatch(
        publication_ref
    ):
        fail(f"invalid publication ref for {identity}")
    checked_ref = subprocess.run(
        ["git", "check-ref-format", publication_ref],
        check=False,
        env=git_environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if checked_ref.returncode != 0:
        fail(f"invalid publication ref for {identity}: {publication_ref}")
    approved[identity] = (revision, publication_ref, repository_url)

manifest_paths = [repo_root / "Cargo.toml"]
for parent in (repo_root / "crates", repo_root / "apps"):
    if parent.is_dir():
        manifest_paths.extend(sorted(parent.rglob("Cargo.toml")))

manifest_repositories: set[str] = set()
for manifest_path in manifest_paths:
    manifest = load_toml(manifest_path)
    relative = manifest_path.relative_to(repo_root)
    for location, dependency in manifest_git_dependencies(manifest):
        repository = repository_identity(str(dependency["git"]))
        if repository not in approved:
            fail(f"unapproved Git repository in {relative}:{location}: {repository}")
        revision = dependency.get("rev")
        if (
            not isinstance(revision, str)
            or not revision_pattern.fullmatch(revision)
            or "branch" in dependency
            or "tag" in dependency
        ):
            fail(f"mutable or unpinned fork dependency in {relative}:{location}")
        approved_revision, _, _ = approved[repository]
        if revision != approved_revision:
            fail(
                f"manifest revision mismatch for {repository}: "
                f"expected {approved_revision}, found {revision}"
            )
        manifest_repositories.add(repository)

lock_path = repo_root / "Cargo.lock"
lock = load_toml(lock_path)
lock_repositories: set[str] = set()
packages = lock.get("package")
if not isinstance(packages, list):
    fail(f"invalid package list in {lock_path}")
for package in packages:
    if not isinstance(package, dict) or not isinstance(package.get("source"), str):
        continue
    source = package["source"]
    if not source.startswith("git+"):
        continue
    _, source_query, locked_revision = split_git_source(source)
    repository = repository_identity(source)
    if repository not in approved:
        fail(f"unapproved Git repository in Cargo.lock: {repository}")
    approved_revision, _, _ = approved[repository]
    requested_revisions = parse_qs(source_query).get("rev", [])
    if requested_revisions != [approved_revision] or locked_revision != approved_revision:
        fail(
            f"Cargo.lock source mismatch for {repository}: "
            f"expected {approved_revision}, found {source}"
        )
    lock_repositories.add(repository)

for repository, (revision, publication_ref, repository_url) in approved.items():
    if repository not in manifest_repositories:
        fail(f"approved fork is unused by shipping manifests: {repository}")
    if repository not in lock_repositories:
        fail(f"Cargo.lock has no source for approved fork: {repository}")
    process = run_remote_git(
        ["ls-remote", repository_url, publication_ref, f"{publication_ref}^{{}}"],
        f"cannot inspect approved fork {repository}",
    )
    published_refs = {
        fields[1]: fields[0]
        for line in process.stdout.splitlines()
        if len(fields := line.split()) == 2
    }
    comparison_ref = publication_ref
    peeled_ref = f"{publication_ref}^{{}}"
    if publication_ref.startswith("refs/tags/") and peeled_ref in published_refs:
        comparison_ref = peeled_ref
    published_revision = published_refs.get(comparison_ref)
    if published_revision != revision:
        found = published_revision or "missing"
        fail(
            f"published ref mismatch for {repository} {publication_ref}: "
            f"expected {revision}, found {found}"
        )

print(
    "shipping fork pins verified "
    f"repositories={len(approved)} manifests={len(manifest_paths)}"
)
PY
