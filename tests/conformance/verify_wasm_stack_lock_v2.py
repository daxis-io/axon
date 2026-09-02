#!/usr/bin/env python3

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

try:
    import tomllib
except ModuleNotFoundError as error:
    raise SystemExit(
        "stack verifier failed: Python 3.11 or newer with tomllib is required"
    ) from error


COMPONENTS = ("arrow_rs", "object_store", "datafusion", "delta_kernel", "delta_rs")
REVISION = re.compile(r"^[0-9a-f]{40}$")
TARGET = "wasm32-unknown-unknown"


def fail(message: str) -> None:
    raise SystemExit(f"stack verifier failed: {message}")


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
        return urlunsplit(
            (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "")
        )
    return value


def git(arguments: list[str], operation: str, *, timeout: int = 30) -> str:
    environment = os.environ.copy()
    environment["GIT_TERMINAL_PROMPT"] = "0"
    attempts = 2 if any(argument in {"ls-remote", "fetch"} for argument in arguments) else 1
    diagnostic = "git operation failed"
    for attempt in range(attempts):
        try:
            process = subprocess.run(
                ["git", *arguments],
                check=False,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            diagnostic = f"timed out after {timeout} seconds"
        else:
            if process.returncode == 0:
                return process.stdout
            diagnostic = process.stderr.strip() or f"git exited with {process.returncode}"
        if attempt + 1 == attempts:
            fail(f"{operation}: {diagnostic}")
    raise AssertionError("bounded Git retry loop did not terminate")


def refs(url: str, *names: str) -> dict[str, str]:
    output = git(["ls-remote", "--tags", url, *names], f"cannot inspect tags at {url}")
    parsed: dict[str, str] = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) == 2:
            parsed[fields[1]] = fields[0]
    return parsed


def tag_commit(url: str, tag: str, *, require_annotated: bool) -> str:
    reference = f"refs/tags/{tag}"
    remote = refs(url, reference, f"{reference}^{{}}")
    direct = remote.get(reference)
    peeled = remote.get(f"{reference}^{{}}")
    if direct is None:
        fail(f"missing tag {tag} at {url}")
    if require_annotated and peeled is None:
        fail(f"tag {tag} at {url} must be annotated")
    return peeled or direct


def require_string(entry: dict, component: str, field: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        fail(f"{component}.{field} must be a non-empty string")
    return value


def active_metadata(repo_root: Path) -> dict:
    process = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(repo_root / "Cargo.toml"),
            "--format-version",
            "1",
            "--filter-platform",
            TARGET,
            "--locked",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        fail(f"target-filtered cargo metadata failed: {process.stderr.strip()}")
    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as error:
        fail(f"cargo metadata returned invalid JSON: {error}")


def reachable_packages(metadata: dict, root_names: list[str]) -> Iterable[dict]:
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
    workspace_members = set(metadata.get("workspace_members", []))
    roots_by_name: dict[str, list[str]] = {}
    for package_id in workspace_members:
        package = packages.get(package_id)
        if package is not None:
            roots_by_name.setdefault(str(package.get("name", "")), []).append(package_id)
    pending: list[str] = []
    for root_name in root_names:
        matches = roots_by_name.get(root_name, [])
        if len(matches) != 1:
            fail(
                f"WASM root {root_name!r} must identify exactly one workspace package; "
                f"found {len(matches)}"
            )
        pending.append(matches[0])
    active: set[str] = set()
    while pending:
        package_id = pending.pop()
        if package_id in active:
            continue
        active.add(package_id)
        node = nodes.get(package_id, {})
        for dependency in node.get("deps", []):
            kinds = dependency.get("dep_kinds", [])
            if not kinds or any(
                isinstance(kind, dict) and kind.get("kind") in (None, "normal", "build")
                for kind in kinds
            ):
                dependency_id = dependency.get("pkg")
                if isinstance(dependency_id, str):
                    pending.append(dependency_id)
    return (packages[package_id] for package_id in active if package_id in packages)


def matches(name: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def verify_no_external_path_packages(packages: Iterable[dict], repo_root: Path) -> None:
    for package in packages:
        if not isinstance(package, dict) or package.get("source") is not None:
            continue
        manifest_raw = package.get("manifest_path")
        if not isinstance(manifest_raw, str):
            fail(f"path package has no manifest_path: {package.get('name', '<unknown>')}")
        manifest = Path(manifest_raw).resolve()
        try:
            manifest.relative_to(repo_root)
        except ValueError:
            fail(
                "external local-path dependency in WASM workspace metadata: "
                f"{package.get('name', '<unknown>')} at {manifest}"
            )


def verify_final_component(
    component: str, entry: dict, repo_root: Path, packages: list[dict]
) -> None:
    base_sha = entry["base_sha"]
    candidate_sha = entry["candidate_sha"]
    stack_sha = entry["stack_sha"]
    daxis_url = entry["daxis_url"]
    expected_source = normalized_git_url(entry["expected_package_source"])
    expected_version = entry["release_version"]
    patterns = entry["package_patterns"]

    release = load_toml(repo_root / "wasm-stack.lock.toml")["release"]
    for tag_field in ("candidate_tag", "final_tag"):
        tag = release[tag_field]
        if tag_commit(daxis_url, tag, require_annotated=True) != stack_sha:
            fail(f"{component} {tag_field} does not peel to stack_sha")

    with tempfile.TemporaryDirectory(prefix=f"axon-{component}-ancestry-") as directory:
        repository = Path(directory) / "repo.git"
        git(["init", "--bare", str(repository)], f"cannot initialize {component} audit repository")
        for revision, local_ref in (
            (base_sha, "base"),
            (candidate_sha, "candidate"),
            (stack_sha, "stack"),
        ):
            git(
                ["-C", str(repository), "fetch", "--no-tags", daxis_url, f"{revision}:refs/axon/{local_ref}"],
                f"cannot fetch {component}.{local_ref}",
            )
        for ancestor, descendant in (("base", "candidate"), ("candidate", "stack")):
            process = subprocess.run(
                ["git", "-C", str(repository), "merge-base", "--is-ancestor", f"refs/axon/{ancestor}", f"refs/axon/{descendant}"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if process.returncode != 0:
                fail(f"{component}.{ancestor}_sha is not an ancestor of {descendant}_sha")
        messages = git(
            [
                "-C",
                str(repository),
                "log",
                "--format=%H%x00%an%x00%ae%x00%cn%x00%ce%x00%B%x00",
                "refs/axon/base..refs/axon/stack",
            ],
            f"cannot inspect {component} DCO range",
        ).split("\x00")
        for index in range(0, len(messages) - 5, 6):
            commit = messages[index].strip()
            author = (messages[index + 1].strip().casefold(), messages[index + 2].strip().casefold())
            committer = (messages[index + 3].strip().casefold(), messages[index + 4].strip().casefold())
            message = messages[index + 5]
            signoffs = {
                (match.group(1).strip().casefold(), match.group(2).strip().casefold())
                for match in re.finditer(r"(?mi)^Signed-off-by:\s+(.+?)\s*<([^>]+)>\s*$", message)
            }
            if commit and not signoffs:
                fail(f"{component} commit lacks DCO sign-off: {commit}")
            if commit and author not in signoffs and committer not in signoffs:
                fail(f"{component} commit has DCO sign-off identity mismatch: {commit}")

    selected = [package for package in packages if matches(str(package.get("name", "")), patterns)]
    if not selected:
        fail(f"WASM closure has no package for {component}")
    identities = {
        (
            str(package.get("version", "")),
            normalized_git_url(str(package.get("source") or "path")),
            str(package.get("source") or "").rsplit("#", 1)[-1],
        )
        for package in selected
    }
    if len(identities) != 1:
        fail(f"WASM closure has multiple source/version identities for {component}: {sorted(identities)}")
    version, source, revision = next(iter(identities))
    if version != expected_version or source != expected_source or revision != stack_sha:
        fail(
            f"WASM closure mismatch for {component}: expected {expected_version}@{expected_source}#{stack_sha}, "
            f"found {version}@{source}#{revision}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(add_help=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--bootstrap", "--allow-unset", action="store_true")
    mode.add_argument("--final", action="store_true")
    mode.add_argument("--ci", action="store_true")
    parser.add_argument("--repo-root", default=None)
    args = parser.parse_args()

    repo_root = Path(args.repo_root or subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()).resolve()
    lock_path = repo_root / "wasm-stack.lock.toml"
    lock = load_toml(lock_path)
    if lock.get("schema") != 2:
        fail("wasm-stack.lock.toml schema must be 2")
    if lock.get("target_triple") != TARGET:
        fail(f"target_triple must be {TARGET}")
    wasm_roots = lock.get("wasm_roots")
    if (
        not isinstance(wasm_roots, list)
        or not wasm_roots
        or not all(isinstance(root, str) and root for root in wasm_roots)
        or len(set(wasm_roots)) != len(wasm_roots)
    ):
        fail("wasm_roots must be a non-empty list of unique package names")
    release = lock.get("release")
    if not isinstance(release, dict):
        fail("wasm-stack.lock.toml must contain a release table")
    for field, expected in (
        ("branch", "axon-wasm/v1"),
        ("candidate_tag", "axon-wasm-v1.0.0-rc.1"),
        ("final_tag", "axon-wasm-v1.0.0"),
    ):
        if release.get(field) != expected:
            fail(f"release.{field} must be {expected}")

    components = lock.get("components")
    if not isinstance(components, dict):
        fail("wasm-stack.lock.toml must contain a components table")
    validated: dict[str, dict] = {}
    reachable_daxis_urls: set[str] = set()
    has_unset = False
    for component in COMPONENTS:
        entry = components.get(component)
        if not isinstance(entry, dict):
            fail(f"missing component entry: {component}")
        for field in (
            "canonical_url",
            "daxis_url",
            "release_version",
            "base_tag",
            "base_sha",
            "candidate_sha",
            "stack_sha",
            "target_triple",
            "expected_package_source",
        ):
            require_string(entry, component, field)
        if entry["target_triple"] != TARGET:
            fail(f"{component}.target_triple must be {TARGET}")
        if entry["daxis_url"] not in reachable_daxis_urls:
            if not git(
                ["ls-remote", entry["daxis_url"], "HEAD"],
                f"cannot reach Daxis fork for {component}",
            ).strip():
                fail(f"Daxis fork for {component} has no reachable HEAD")
            reachable_daxis_urls.add(entry["daxis_url"])
        patterns = entry.get("package_patterns")
        if not isinstance(patterns, list) or not patterns or not all(isinstance(pattern, str) and pattern for pattern in patterns):
            fail(f"{component}.package_patterns must be a non-empty string list")
        if not REVISION.fullmatch(entry["base_sha"]):
            fail(f"{component}.base_sha must be a 40-character lowercase hexadecimal revision")
        if tag_commit(entry["canonical_url"], entry["base_tag"], require_annotated=False) != entry["base_sha"]:
            fail(f"{component}.base_tag does not peel to base_sha")
        for field in ("candidate_sha", "stack_sha"):
            value = entry[field]
            if value == "UNSET":
                has_unset = True
                if args.final or (not args.bootstrap and not args.ci):
                    fail(f"{component}.{field} is UNSET in final mode")
            elif not REVISION.fullmatch(value):
                fail(f"{component}.{field} must be UNSET or a 40-character lowercase hexadecimal revision")
        validated[component] = entry

    for manifest in repo_root.rglob("Cargo.toml"):
        if any(part in {".git", ".worktrees", "target", "node_modules"} for part in manifest.parts):
            continue
        for line_number, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            if re.search(r"\bbranch\s*=", code):
                fail(f"mutable branch dependency in {manifest.relative_to(repo_root)}:{line_number}")

    metadata = active_metadata(repo_root)
    packages = list(reachable_packages(metadata, wasm_roots))
    verify_no_external_path_packages(packages, repo_root)
    if args.bootstrap or (args.ci and has_unset):
        print(f"production WASM stack verified mode=bootstrap components={len(validated)}")
        return

    for component, entry in validated.items():
        verify_final_component(component, entry, repo_root, packages)
    print(f"production WASM stack verified mode=final components={len(validated)} graph_packages={len(packages)}")


if __name__ == "__main__":
    main()
