#!/usr/bin/env bash

# Verifies that the repository pins one exact Rust release and that every workflow installs it.
#
# Shipped WASM bytes depend on rustc: the same sources produced an 11.6 MB name section with 1.95.0
# and a 48.3 MB one with 1.98.0. `rust-toolchain.toml` fixes the release for local builds, and each
# workflow toolchain install must request that same release explicitly instead of floating stable.

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "missing required tool: rg" >&2
  exit 127
fi

repo_root="${AXON_RUST_TOOLCHAIN_REPO_ROOT:-$(git rev-parse --show-toplevel)}"

python3 - "$repo_root" <<'PY'
import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError as error:
    raise SystemExit(
        "toolchain pin verification failed: Python 3.11 or newer with tomllib is required"
    ) from error


def fail(message: str) -> None:
    raise SystemExit(f"toolchain pin verification failed: {message}")


root = Path(sys.argv[1])
toolchain_file = root / "rust-toolchain.toml"
if not toolchain_file.is_file():
    fail("missing rust-toolchain.toml")
toolchain = tomllib.loads(toolchain_file.read_text(encoding="utf-8")).get("toolchain")
if not isinstance(toolchain, dict):
    fail("rust-toolchain.toml must contain a [toolchain] table")
channel = toolchain.get("channel")
if not isinstance(channel, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", channel):
    fail(f"rust-toolchain.toml channel must be an exact x.y.z release, found {channel!r}")
for component in ("rustfmt", "clippy"):
    if component not in toolchain.get("components", []):
        fail(f"rust-toolchain.toml must install the {component} component")
if "wasm32-unknown-unknown" not in toolchain.get("targets", []):
    fail("rust-toolchain.toml must install the wasm32-unknown-unknown target")

installs = 0
for workflow in sorted((root / ".github" / "workflows").glob("*.y*ml")):
    lines = workflow.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        location = f"{workflow.relative_to(root)}:{index + 1}"
        rustup_init = re.search(r"--default-toolchain\s+(\S+)", line)
        if rustup_init:
            installs += 1
            if rustup_init.group(1) != channel:
                fail(f"{location} installs {rustup_init.group(1)}, not {channel}")
        action = re.search(r"uses:\s*dtolnay/rust-toolchain@\S+", line)
        if not action:
            continue
        installs += 1
        # The step's keys sit at the column of `uses`; its inputs are the deeper lines that follow.
        step_column = action.start()
        requested = None
        for following in lines[index + 1 :]:
            if not following.strip():
                continue
            column = len(following) - len(following.lstrip(" "))
            if column < step_column or (column == step_column - 2 and following.lstrip().startswith("- ")):
                break
            match = re.match(r"\s*toolchain:\s*[\"']?([^\"'\s#]+)", following)
            if match:
                requested = match.group(1)
        if requested is None:
            fail(f"{location} installs a Rust toolchain without an explicit toolchain: {channel}")
        if requested != channel:
            fail(f"{location} installs {requested}, not {channel}")

if installs == 0:
    fail("no workflow installs a Rust toolchain")
print(f"Rust toolchain pinned to {channel} across {installs} workflow installs")
PY
