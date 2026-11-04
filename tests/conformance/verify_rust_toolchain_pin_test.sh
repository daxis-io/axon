#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
verifier="$script_dir/verify_rust_toolchain_pin.sh"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

bash "$verifier" >/dev/null

write_fixture() {
  local root="$1"
  mkdir -p "$root/.github/workflows"
  cat >"$root/rust-toolchain.toml" <<'EOF_TOOLCHAIN'
[toolchain]
channel = "1.95.0"
components = ["rustfmt", "clippy"]
targets = ["wasm32-unknown-unknown"]
profile = "minimal"
EOF_TOOLCHAIN
  cat >"$root/.github/workflows/ci.yml" <<'EOF_WORKFLOW'
jobs:
  rust:
    steps:
      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.95.0
          targets: wasm32-unknown-unknown
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt
          toolchain: "1.95.0"
      - name: Install Rust in a container
        run: sh -s -- -y --profile minimal --default-toolchain 1.95.0
EOF_WORKFLOW
}

expect_failure() {
  local description="$1" expected="$2" root="$3"
  local output
  if output="$(AXON_RUST_TOOLCHAIN_REPO_ROOT="$root" bash "$verifier" 2>&1)"; then
    echo "toolchain pin verifier accepted ${description}" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected"* ]]; then
    echo "toolchain pin verifier rejected ${description} for the wrong reason:" >&2
    printf '%s\n' "$output" >&2
    exit 1
  fi
}

fixture="$tmpdir/pinned"
write_fixture "$fixture"
AXON_RUST_TOOLCHAIN_REPO_ROOT="$fixture" bash "$verifier" >/dev/null

floating="$tmpdir/floating"
write_fixture "$floating"
cat >>"$floating/.github/workflows/ci.yml" <<'EOF_FLOATING'
      - name: Install a floating toolchain
        uses: dtolnay/rust-toolchain@stable
        with:
          targets: wasm32-unknown-unknown
      - name: Next step
        run: cargo check
EOF_FLOATING
expect_failure "a toolchain step without an explicit toolchain" "ci.yml:16" "$floating"

drifted="$tmpdir/drifted"
write_fixture "$drifted"
sed -i.bak 's/toolchain: "1.95.0"/toolchain: "1.96.0"/' "$drifted/.github/workflows/ci.yml"
expect_failure "a workflow pinned to another release" "installs 1.96.0" "$drifted"

container="$tmpdir/container"
write_fixture "$container"
sed -i.bak 's/--default-toolchain 1.95.0/--default-toolchain stable/' "$container/.github/workflows/ci.yml"
expect_failure "a rustup-init install of another toolchain" "installs stable" "$container"

channel="$tmpdir/channel"
write_fixture "$channel"
sed -i.bak 's/channel = "1.95.0"/channel = "stable"/' "$channel/rust-toolchain.toml"
expect_failure "a floating toolchain file channel" "exact x.y.z release" "$channel"

target="$tmpdir/target"
write_fixture "$target"
sed -i.bak 's/targets = \["wasm32-unknown-unknown"\]/targets = []/' "$target/rust-toolchain.toml"
expect_failure "a toolchain file without the browser target" "wasm32-unknown-unknown" "$target"

missing="$tmpdir/missing"
write_fixture "$missing"
rm "$missing/rust-toolchain.toml"
expect_failure "a repository without a toolchain file" "missing rust-toolchain.toml" "$missing"

echo "Rust toolchain pin coverage passed"
