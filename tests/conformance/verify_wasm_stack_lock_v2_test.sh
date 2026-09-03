#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verifier="$script_dir/verify_upstream_wasm_fork_stack.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
export CARGO_HOME="$tmpdir/cargo-home"

seed="$tmpdir/seed"
mkdir -p "$seed"
git -C "$seed" init -q --initial-branch=main
git -C "$seed" config user.name "Axon stack lock v2 test"
git -C "$seed" config user.email "stack-lock-v2@example.invalid"
cat >"$seed/Cargo.toml" <<'EOF_COMPONENT_WORKSPACE'
[workspace]
members = ["arrow", "parquet", "object_store", "datafusion", "delta_kernel", "deltalake-browser"]
resolver = "2"
EOF_COMPONENT_WORKSPACE
for spec in \
  arrow:59.3.0 \
  parquet:59.3.0 \
  object_store:0.14.1 \
  datafusion:55.0.0 \
  delta_kernel:0.28.0 \
  deltalake-browser:0.32.4; do
  package="${spec%%:*}"
  version="${spec#*:}"
  mkdir -p "$seed/$package/src"
  cat >"$seed/$package/Cargo.toml" <<EOF_COMPONENT
[package]
name = "$package"
version = "$version"
edition = "2021"
EOF_COMPONENT
  printf '%s\n' 'pub fn qualified_component() {}' >"$seed/$package/src/lib.rs"
done
printf '%s\n' fixture >"$seed/README.md"
git -C "$seed" add Cargo.toml README.md arrow parquet object_store datafusion delta_kernel deltalake-browser
git -C "$seed" commit -q -s -m "test: seed stack lock v2 fixture"
base_sha="$(git -C "$seed" rev-parse HEAD)"
git -C "$seed" tag -a release-base -m "release base"
printf '%s\n' candidate >>"$seed/README.md"
git -C "$seed" add README.md
git -C "$seed" commit -q -s -m "feat: add target-safe compatibility"
candidate_sha="$(git -C "$seed" rev-parse HEAD)"
printf '%s\n' stack >>"$seed/README.md"
git -C "$seed" add README.md
git -C "$seed" commit -q -s -m "chore: wire qualified stack"
stack_sha="$(git -C "$seed" rev-parse HEAD)"
git -C "$seed" tag -a axon-wasm-v1.0.0-rc.1 -m "qualified release candidate"
git -C "$seed" tag -a axon-wasm-v1.0.0 -m "qualified final release"
bare="$tmpdir/component.git"
git clone -q --bare "$seed" "$bare"
component_url="file://$bare"

fixture="$tmpdir/repo"
mkdir -p "$fixture/crates/fixture/src"
cat >"$fixture/Cargo.toml" <<'EOF_MANIFEST'
[workspace]
members = ["crates/fixture"]
resolver = "2"
EOF_MANIFEST
cat >"$fixture/crates/fixture/Cargo.toml" <<'EOF_MEMBER'
[package]
name = "stack-v2-fixture"
version = "0.1.0"
edition = "2021"
EOF_MEMBER
printf '%s\n' 'pub fn fixture() {}' >"$fixture/crates/fixture/src/lib.rs"
cat >>"$fixture/crates/fixture/Cargo.toml" <<EOF_DEPENDENCIES

[dependencies]
arrow = { git = "$component_url", rev = "$stack_sha" }
parquet = { git = "$component_url", rev = "$stack_sha" }
object_store = { git = "$component_url", rev = "$stack_sha" }
datafusion = { git = "$component_url", rev = "$stack_sha" }
delta_kernel = { git = "$component_url", rev = "$stack_sha" }
deltalake-browser = { git = "$component_url", rev = "$stack_sha" }
EOF_DEPENDENCIES
cargo generate-lockfile --manifest-path "$fixture/Cargo.toml" >/dev/null

cat >"$fixture/wasm-stack.lock.toml" <<EOF_LOCK
schema = 2
target_triple = "wasm32-unknown-unknown"
wasm_roots = ["stack-v2-fixture"]

[release]
branch = "axon-wasm/v1"
candidate_tag = "axon-wasm-v1.0.0-rc.1"
final_tag = "axon-wasm-v1.0.0"

[components.arrow_rs]
canonical_url = "$component_url"
daxis_url = "$component_url"
release_version = "59.3.0"
base_tag = "release-base"
base_sha = "$base_sha"
candidate_sha = "UNSET"
stack_sha = "UNSET"
target_triple = "wasm32-unknown-unknown"
expected_package_source = "$component_url"
package_patterns = ["arrow", "arrow-*", "parquet"]

[components.object_store]
canonical_url = "$component_url"
daxis_url = "$component_url"
release_version = "0.14.1"
base_tag = "release-base"
base_sha = "$base_sha"
candidate_sha = "UNSET"
stack_sha = "UNSET"
target_triple = "wasm32-unknown-unknown"
expected_package_source = "$component_url"
package_patterns = ["object_store"]

[components.datafusion]
canonical_url = "$component_url"
daxis_url = "$component_url"
release_version = "55.0.0"
base_tag = "release-base"
base_sha = "$base_sha"
candidate_sha = "UNSET"
stack_sha = "UNSET"
target_triple = "wasm32-unknown-unknown"
expected_package_source = "$component_url"
package_patterns = ["datafusion", "datafusion-*"]

[components.delta_kernel]
canonical_url = "$component_url"
daxis_url = "$component_url"
release_version = "0.28.0"
base_tag = "release-base"
base_sha = "$base_sha"
candidate_sha = "UNSET"
stack_sha = "UNSET"
target_triple = "wasm32-unknown-unknown"
expected_package_source = "$component_url"
package_patterns = ["delta_kernel"]

[components.delta_rs]
canonical_url = "$component_url"
daxis_url = "$component_url"
release_version = "0.32.4"
base_tag = "release-base"
base_sha = "$base_sha"
candidate_sha = "UNSET"
stack_sha = "UNSET"
target_triple = "wasm32-unknown-unknown"
expected_package_source = "$component_url"
package_patterns = ["deltalake-browser"]
EOF_LOCK

bash "$verifier" --production --bootstrap --repo-root "$fixture" >/dev/null
ci_bootstrap_output="$(bash "$verifier" --production --ci --repo-root "$fixture")"
if [[ "$ci_bootstrap_output" != *"mode=bootstrap"* ]]; then
  echo "CI mode did not identify the UNSET lock as bootstrap-only" >&2
  exit 1
fi

unreachable_fork_fixture="$tmpdir/unreachable-fork-repo"
cp -R "$fixture" "$unreachable_fork_fixture"
python3 - "$unreachable_fork_fixture/wasm-stack.lock.toml" "$component_url" "file://$tmpdir/missing-fork.git" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
old_url, new_url = sys.argv[2:]
lines = []
for line in path.read_text().splitlines():
    if line.startswith("daxis_url = "):
        line = line.replace(old_url, new_url)
    lines.append(line)
path.write_text("\n".join(lines) + "\n")
PY
if bash "$verifier" --production --bootstrap --repo-root "$unreachable_fork_fixture" >/dev/null 2>&1; then
  echo "bootstrap verification accepted an unreachable Daxis fork" >&2
  exit 1
fi

real_git="$(command -v git)"
retry_bin="$tmpdir/retry-bin"
retry_state="$tmpdir/retry-state"
mkdir -p "$retry_bin"
cat >"$retry_bin/git" <<'EOF_RETRY_GIT'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "ls-remote" && ! -e "$AXON_RETRY_STATE" ]]; then
  : >"$AXON_RETRY_STATE"
  echo "transient remote failure" >&2
  exit 75
fi
exec "$AXON_REAL_GIT" "$@"
EOF_RETRY_GIT
chmod +x "$retry_bin/git"
PATH="$retry_bin:$PATH" \
AXON_REAL_GIT="$real_git" \
AXON_RETRY_STATE="$retry_state" \
  bash "$verifier" --production --bootstrap --repo-root "$fixture" >/dev/null

branch_fixture="$tmpdir/branch-repo"
cp -R "$fixture" "$branch_fixture"
cat >>"$branch_fixture/crates/fixture/Cargo.toml" <<EOF_BRANCH_DEPENDENCY
branch_probe = { git = "$component_url", branch = "main", package = "arrow" }
EOF_BRANCH_DEPENDENCY
if bash "$verifier" --production --bootstrap --repo-root "$branch_fixture" >/dev/null 2>&1; then
  echo "bootstrap verification accepted a mutable branch dependency" >&2
  exit 1
fi

external_helper="$tmpdir/external-helper"
mkdir -p "$external_helper/src"
cat >"$external_helper/Cargo.toml" <<'EOF_EXTERNAL_MANIFEST'
[package]
name = "external-helper"
version = "0.1.0"
edition = "2021"
EOF_EXTERNAL_MANIFEST
printf '%s\n' 'pub fn external_helper() {}' >"$external_helper/src/lib.rs"
external_fixture="$tmpdir/external-repo"
cp -R "$fixture" "$external_fixture"
cat >>"$external_fixture/crates/fixture/Cargo.toml" <<'EOF_EXTERNAL_DEPENDENCY'
external-helper = { path = "../../../external-helper" }
EOF_EXTERNAL_DEPENDENCY
cargo generate-lockfile --manifest-path "$external_fixture/Cargo.toml" >/dev/null
if bash "$verifier" --production --bootstrap --repo-root "$external_fixture" >/dev/null 2>&1; then
  echo "bootstrap verification accepted an external local-path dependency" >&2
  exit 1
fi

native_only_fixture="$tmpdir/native-only-path-repo"
cp -R "$fixture" "$native_only_fixture"
cat >>"$native_only_fixture/crates/fixture/Cargo.toml" <<'EOF_NATIVE_ONLY_DEPENDENCY'

[target.'cfg(not(target_arch = "wasm32"))'.dependencies]
native-helper = { package = "external-helper", path = "../../../external-helper" }
EOF_NATIVE_ONLY_DEPENDENCY
cargo generate-lockfile --manifest-path "$native_only_fixture/Cargo.toml" >/dev/null
if ! bash "$verifier" --production --bootstrap --repo-root "$native_only_fixture" >/dev/null; then
  echo "bootstrap verification rejected an out-of-scope native-only local-path dependency" >&2
  exit 1
fi

python3 - "$fixture/wasm-stack.lock.toml" "$candidate_sha" "$stack_sha" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
candidate, stack = sys.argv[2:]
text = path.read_text()
text = text.replace('candidate_sha = "UNSET"', f'candidate_sha = "{candidate}"')
text = text.replace('stack_sha = "UNSET"', f'stack_sha = "{stack}"')
path.write_text(text)
PY

bash "$verifier" --production --final --repo-root "$fixture" >/dev/null
ci_final_output="$(bash "$verifier" --production --ci --repo-root "$fixture")"
if [[ "$ci_final_output" != *"mode=final"* ]]; then
  echo "CI mode did not enforce final verification for a populated lock" >&2
  exit 1
fi

retry_fetch_bin="$tmpdir/retry-fetch-bin"
retry_fetch_state="$tmpdir/retry-fetch-state"
mkdir -p "$retry_fetch_bin"
cat >"$retry_fetch_bin/git" <<'EOF_RETRY_FETCH_GIT'
#!/usr/bin/env bash
set -euo pipefail
for argument in "$@"; do
  if [[ "$argument" == "fetch" && ! -e "$AXON_RETRY_FETCH_STATE" ]]; then
    : >"$AXON_RETRY_FETCH_STATE"
    echo "transient fetch failure" >&2
    exit 75
  fi
done
exec "$AXON_REAL_GIT" "$@"
EOF_RETRY_FETCH_GIT
chmod +x "$retry_fetch_bin/git"
PATH="$retry_fetch_bin:$PATH" \
AXON_REAL_GIT="$real_git" \
AXON_RETRY_FETCH_STATE="$retry_fetch_state" \
  bash "$verifier" --production --final --repo-root "$fixture" >/dev/null

stale_lock_fixture="$tmpdir/stale-lock-repo"
cp -R "$fixture" "$stale_lock_fixture"
mkdir -p "$stale_lock_fixture/crates/unlocked-helper/src"
cat >"$stale_lock_fixture/crates/unlocked-helper/Cargo.toml" <<'EOF_UNLOCKED_HELPER'
[package]
name = "unlocked-helper"
version = "0.1.0"
edition = "2021"
EOF_UNLOCKED_HELPER
printf '%s\n' 'pub fn unlocked_helper() {}' >"$stale_lock_fixture/crates/unlocked-helper/src/lib.rs"
cat >>"$stale_lock_fixture/crates/fixture/Cargo.toml" <<'EOF_UNLOCKED_DEPENDENCY'
unlocked-helper = { path = "../unlocked-helper" }
EOF_UNLOCKED_DEPENDENCY
if bash "$verifier" --production --final --repo-root "$stale_lock_fixture" >/dev/null 2>&1; then
  echo "final verification accepted a stale Cargo.lock" >&2
  exit 1
fi

lightweight_seed="$tmpdir/lightweight-seed"
git clone -q "$seed" "$lightweight_seed"
git -C "$lightweight_seed" tag -d axon-wasm-v1.0.0 >/dev/null
git -C "$lightweight_seed" tag axon-wasm-v1.0.0 "$stack_sha"
lightweight_bare="$tmpdir/lightweight.git"
git clone -q --bare "$lightweight_seed" "$lightweight_bare"
lightweight_url="file://$lightweight_bare"
lightweight_fixture="$tmpdir/lightweight-repo"
cp -R "$fixture" "$lightweight_fixture"
python3 - "$lightweight_fixture/wasm-stack.lock.toml" "$component_url" "$lightweight_url" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
old_url, new_url = sys.argv[2:]
lines = []
for line in path.read_text().splitlines():
    if line.startswith("daxis_url = ") or line.startswith("expected_package_source = "):
        line = line.replace(old_url, new_url)
    lines.append(line)
path.write_text("\n".join(lines) + "\n")
PY
lightweight_output=""
if lightweight_output="$(bash "$verifier" --production --final --repo-root "$lightweight_fixture" 2>&1)"; then
  echo "final verification accepted a lightweight final tag" >&2
  exit 1
fi
if [[ "$lightweight_output" != *"must be annotated"* ]]; then
  echo "lightweight final tag failed for the wrong reason" >&2
  printf '%s\n' "$lightweight_output" >&2
  exit 1
fi

missing_revision_fixture="$tmpdir/missing-revision-repo"
cp -R "$fixture" "$missing_revision_fixture"
python3 - "$missing_revision_fixture/wasm-stack.lock.toml" "$candidate_sha" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
candidate = sys.argv[2]
text = path.read_text().replace(
    f'candidate_sha = "{candidate}"',
    'candidate_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
)
path.write_text(text)
PY
if bash "$verifier" --production --final --repo-root "$missing_revision_fixture" >/dev/null 2>&1; then
  echo "final verification accepted a missing candidate revision" >&2
  exit 1
fi

unrelated_seed="$tmpdir/unrelated-seed"
git clone -q "$seed" "$unrelated_seed"
unrelated_tree="$(git -C "$unrelated_seed" rev-parse "HEAD^{tree}")"
unrelated_sha="$(
  GIT_AUTHOR_NAME="Axon stack lock v2 test" \
  GIT_AUTHOR_EMAIL="stack-lock-v2@example.invalid" \
  GIT_COMMITTER_NAME="Axon stack lock v2 test" \
  GIT_COMMITTER_EMAIL="stack-lock-v2@example.invalid" \
    git -C "$unrelated_seed" commit-tree "$unrelated_tree" \
      -m "test: unrelated candidate" \
      -m "Signed-off-by: Axon stack lock v2 test <stack-lock-v2@example.invalid>"
)"
git -C "$unrelated_seed" update-ref refs/heads/unrelated "$unrelated_sha"
unrelated_bare="$tmpdir/unrelated.git"
git clone -q --bare "$unrelated_seed" "$unrelated_bare"
unrelated_url="file://$unrelated_bare"
unrelated_fixture="$tmpdir/unrelated-repo"
cp -R "$fixture" "$unrelated_fixture"
python3 - "$unrelated_fixture/wasm-stack.lock.toml" "$component_url" "$unrelated_url" "$candidate_sha" "$unrelated_sha" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
old_url, new_url, old_candidate, new_candidate = sys.argv[2:]
lines = []
for line in path.read_text().splitlines():
    if line.startswith("daxis_url = "):
        line = line.replace(old_url, new_url)
    if line.startswith("candidate_sha = "):
        line = line.replace(old_candidate, new_candidate)
    lines.append(line)
path.write_text("\n".join(lines) + "\n")
PY
unrelated_output=""
if unrelated_output="$(bash "$verifier" --production --final --repo-root "$unrelated_fixture" 2>&1)"; then
  echo "final verification accepted an unrelated candidate revision" >&2
  exit 1
fi
if [[ "$unrelated_output" != *"base_sha is not an ancestor"* ]]; then
  echo "unrelated candidate failed for the wrong reason" >&2
  printf '%s\n' "$unrelated_output" >&2
  exit 1
fi

alternate_arrow="$tmpdir/alternate-arrow"
mkdir -p "$alternate_arrow/src"
git -C "$alternate_arrow" init -q --initial-branch=main
git -C "$alternate_arrow" config user.name "Alternate Arrow"
git -C "$alternate_arrow" config user.email "alternate-arrow@example.invalid"
cat >"$alternate_arrow/Cargo.toml" <<'EOF_ALTERNATE_ARROW'
[package]
name = "arrow"
version = "59.3.0"
edition = "2021"
EOF_ALTERNATE_ARROW
printf '%s\n' 'pub fn alternate_arrow() {}' >"$alternate_arrow/src/lib.rs"
git -C "$alternate_arrow" add Cargo.toml src/lib.rs
git -C "$alternate_arrow" commit -q -s -m "test: alternate Arrow source"
alternate_arrow_sha="$(git -C "$alternate_arrow" rev-parse HEAD)"
alternate_arrow_bare="$tmpdir/alternate-arrow.git"
git clone -q --bare "$alternate_arrow" "$alternate_arrow_bare"
alternate_arrow_url="file://$alternate_arrow_bare"
duplicate_fixture="$tmpdir/duplicate-source-repo"
cp -R "$fixture" "$duplicate_fixture"
cat >>"$duplicate_fixture/crates/fixture/Cargo.toml" <<EOF_DUPLICATE_SOURCE
arrow-duplicate = { package = "arrow", git = "$alternate_arrow_url", rev = "$alternate_arrow_sha" }
EOF_DUPLICATE_SOURCE
cargo generate-lockfile --manifest-path "$duplicate_fixture/Cargo.toml" >/dev/null
duplicate_output=""
if duplicate_output="$(bash "$verifier" --production --final --repo-root "$duplicate_fixture" 2>&1)"; then
  echo "final verification accepted duplicate Arrow source identities" >&2
  exit 1
fi
if [[ "$duplicate_output" != *"multiple source/version identities"* ]]; then
  echo "duplicate Arrow source failed for the wrong reason" >&2
  printf '%s\n' "$duplicate_output" >&2
  exit 1
fi

bad_dco_seed="$tmpdir/bad-dco-seed"
git clone -q "$seed" "$bad_dco_seed"
printf '%s\n' bad-dco >>"$bad_dco_seed/README.md"
git -C "$bad_dco_seed" add README.md
GIT_AUTHOR_NAME="Actual Author" \
GIT_AUTHOR_EMAIL="actual-author@example.invalid" \
GIT_COMMITTER_NAME="Actual Committer" \
GIT_COMMITTER_EMAIL="actual-committer@example.invalid" \
  git -C "$bad_dco_seed" commit -q \
    -m "test: mismatched DCO identity" \
    -m "Signed-off-by: Unrelated Person <unrelated@example.invalid>"
bad_dco_sha="$(git -C "$bad_dco_seed" rev-parse HEAD)"
GIT_COMMITTER_NAME="Axon stack lock v2 test" \
GIT_COMMITTER_EMAIL="stack-lock-v2@example.invalid" \
  git -C "$bad_dco_seed" tag -f -a axon-wasm-v1.0.0-rc.1 -m "bad DCO release candidate"
GIT_COMMITTER_NAME="Axon stack lock v2 test" \
GIT_COMMITTER_EMAIL="stack-lock-v2@example.invalid" \
  git -C "$bad_dco_seed" tag -f -a axon-wasm-v1.0.0 -m "bad DCO final release"
bad_dco_bare="$tmpdir/bad-dco.git"
git clone -q --bare "$bad_dco_seed" "$bad_dco_bare"
bad_dco_url="file://$bad_dco_bare"
bad_dco_fixture="$tmpdir/bad-dco-repo"
cp -R "$fixture" "$bad_dco_fixture"
python3 - "$bad_dco_fixture/wasm-stack.lock.toml" "$component_url" "$bad_dco_url" "$stack_sha" "$bad_dco_sha" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
old_url, new_url, old_stack, new_stack = sys.argv[2:]
lines = []
for line in path.read_text().splitlines():
    if line.startswith("daxis_url = "):
        line = line.replace(old_url, new_url)
    if line.startswith("stack_sha = "):
        line = line.replace(old_stack, new_stack)
    lines.append(line)
path.write_text("\n".join(lines) + "\n")
PY
bad_dco_output=""
if bad_dco_output="$(bash "$verifier" --production --final --repo-root "$bad_dco_fixture" 2>&1)"; then
  echo "final verification accepted a mismatched DCO identity" >&2
  exit 1
fi
if [[ "$bad_dco_output" != *"DCO sign-off identity mismatch"* ]]; then
  echo "mismatched DCO identity failed for the wrong reason" >&2
  printf '%s\n' "$bad_dco_output" >&2
  exit 1
fi

echo "production WASM stack lock v2 real locked-metadata coverage passed"
