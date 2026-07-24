# Research Snapshot

## Snapshot Contract

This appendix records the graph used for the design. Repository SHAs identify source. Lock hashes,
toolchain versions, environment, and exact commands identify the resolved probe.

Date: 2026-07-23.

## Toolchain

```text
$ rustc -Vv
rustc 1.95.0 (59807616e 2026-04-14)
binary: rustc
commit-hash: 59807616e1fa2540724bfbac14d7976d7e4a3860
commit-date: 2026-04-14
host: aarch64-apple-darwin
release: 1.95.0
LLVM version: 22.1.2

$ cargo -V
cargo 1.95.0 (f2d3ce0bd 2026-03-21)

$ clang --version
Apple clang version 17.0.0 (clang-1700.6.4.2)
Target: arm64-apple-darwin25.5.0

$ rustup target list --installed
aarch64-apple-darwin
wasm32-unknown-unknown
```

`RUSTFLAGS`, target-specific Cargo rustflags, `CC`, `CXX`, and `AR` were unset for the focused
probes.

## Source And Lock Provenance

| Repository      | `git rev-parse HEAD`                       | Worktree and lock state                                               | SHA-256 of `Cargo.lock`                                            |
| --------------- | ------------------------------------------ | --------------------------------------------------------------------- | ------------------------------------------------------------------ |
| Arrow Rust      | `b46e7334b3d4233b35868d7d22b0807d3c38e147` | Clean source checkout; tracked lock used unchanged                    | `3893524f7182f275050cbb9cda5544429204191e022a0b1b102af9502ac6807e` |
| `object_store`  | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` | No tracked lock at this revision; generated lock used with `--locked` | `a26095d019ce075bde84a2ac8dae980c393ace84e451233cda6b51e3d1ad9a3e` |
| DataFusion      | `17634176a4f406a50b31a3711514831c9d3ecfa0` | Clean source checkout; tracked lock used unchanged                    | `8b76f74cbaa5a5570288dd3bfd7eed3cc190073c46eea266edccab02f5a383ef` |
| delta-kernel-rs | `03e2537d5a6203872d9785e31cc00f503ddeb8ec` | Clean source checkout; tracked lock used unchanged                    | `3028ddc22a8e6a0ab13eb9a796576211431403c7994b4643511556009d1c274b` |
| delta-rs        | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` | No tracked root lock; generated lock used for provenance              | `b7dc437ec6bc4e19f29a3a33fb913ec458d160d695bf5133cf8e18791ff51aa6` |

For checkouts without a tracked lock:

```bash
cargo generate-lockfile
sha256sum Cargo.lock
git status --short
```

The generated delta-rs lock resolved:

```text
buoyant_kernel 0.25.1
buoyant_kernel_engine 0.25.1
git revision 8ba063f8f84fec222000f66d40d70911d7c79675
```

The moving branch still pointed at that revision when this document was written.

## Relevant Resolved Versions

| Graph                         | `zstd-sys`          | `getrandom`                          | `rand`                              | `bzip2`                               | `liblzma` / `liblzma-sys`             | `ring`    | `aws-lc-rs` |
| ----------------------------- | ------------------- | ------------------------------------ | ----------------------------------- | ------------------------------------- | ------------------------------------- | --------- | ----------- |
| Arrow lock                    | `2.0.16+zstd.1.5.7` | `0.2.17`, `0.3.4`, `0.4.2`           | `0.9.4`, `0.10.1`                   | `0.6.1`                               | `0.4.6` / `0.4.6`                     | `0.17.14` | `1.16.3`    |
| Generated `object_store` lock | Absent              | `0.2.17`, `0.3.4`, `0.4.3`           | `0.10.2`                            | Absent                                | Absent                                | `0.17.14` | `1.17.3`    |
| DataFusion lock               | `2.0.16+zstd.1.5.7` | `0.2.17`, `0.3.4`, `0.4.2`           | `0.8.6`, `0.9.4`, `0.10.1`          | `0.6.1`                               | `0.4.7` / `0.4.6`                     | `0.17.14` | `1.16.3`    |
| delta-kernel lock             | `2.0.16+zstd.1.5.7` | `0.2.17`, `0.3.4`, `0.4.2`           | `0.8.5`, `0.9.2`, `0.10.0`          | Not selected in the recorded core row | Not selected in the recorded core row | `0.17.14` | `1.16.2`    |
| Generated delta-rs lock       | `2.0.16+zstd.1.5.7` | `0.1.16`, `0.2.17`, `0.3.4`, `0.4.3` | `0.7.3`, `0.8.7`, `0.9.5`, `0.10.2` | `0.6.1`                               | `0.4.7` / `0.4.7`                     | `0.17.14` | `1.17.3`    |

## Focused Probe Commands

All commands used `--locked` after the lock state above was established.

### Arrow Rust

```bash
cargo check \
  -p parquet \
  --target wasm32-unknown-unknown \
  --locked

cargo check \
  -p parquet \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4 \
  --locked

cargo check \
  -p parquet \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features zstd \
  --locked

cargo check \
  -p arrow-ipc \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features zstd \
  --locked
```

Recorded result:

- default Parquet and explicit zstd reached `zstd-sys` and failed in the configured Apple C
  toolchain path;
- Arrow IPC with zstd reached the same backend and failed;
- the Parquet profile with Snappy, Brotli, zlib-rs gzip, and LZ4 passed.

### `object_store`

```bash
cargo check \
  --target wasm32-unknown-unknown \
  --locked

cargo check \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features http-base \
  --locked

cargo check \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features http \
  --locked
```

Recorded result:

- the default filesystem feature profile compiled under source `cfg`;
- `http-base` reached `cloud-base -> rand -> getrandom` and failed backend selection;
- `http` added AWS-LC target and build-script exposure.

The repository's own pinned WASM tests enable several `getrandom` JS dev features, so an isolated
consumer is required to reproduce external behavior.

### DataFusion

```bash
cargo check \
  -p datafusion \
  --target wasm32-unknown-unknown \
  --locked

cargo tree \
  -p datafusion \
  --target wasm32-unknown-unknown \
  -e features \
  -i parquet \
  --locked

cargo tree \
  -p datafusion-physical-plan \
  --target wasm32-unknown-unknown \
  -e features \
  -i arrow-ipc \
  --locked
```

The graph confirmed the defaults-on Parquet overrides, explicit Arrow IPC `lz4,zstd` ownership, and
default compression edges to zstd, liblzma, and `async-compression` xz/zstd.

### delta-kernel-rs

```bash
cargo check \
  -p delta_kernel \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --locked

cargo check \
  -p delta_kernel \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow \
  --locked
```

The minimal core reached `rand -> getrandom`. The Arrow profile added Parquet defaults and
`object_store` provider features.

### delta-rs provenance

```bash
git rev-parse HEAD
git ls-remote \
  https://github.com/buoyant-data/delta-kernel-rs \
  refs/heads/buoyant/main
cargo generate-lockfile
sha256sum Cargo.lock
rg -n 'buoyant_kernel|buoyant_kernel_engine' Cargo.lock
```

The probe established fork resolution. It did not treat upstream delta-kernel revision `03e2537d` as
the kernel consumed by the pinned delta-rs source.

## Existing Upstream CI Evidence

The pinned Parquet WASM job installs Clang and `gcc-multilib` before building Parquet defaults. The
job demonstrates that the graph can build in that configured environment. It does not establish a
C-free or toolchain-independent WASM graph.

The pinned `object_store` job installs Clang for `ring`, skips cloud-feature builds for
`wasm32-unknown-unknown`, and supplies JS randomness in its Node test profile.

The pinned DataFusion browser job installs Clang and sets
`RUSTFLAGS='--cfg getrandom_backend="wasm_js" ...'`.

Primary source:

- [Arrow Parquet workflow](https://github.com/apache/arrow-rs/blob/b46e7334b3d4233b35868d7d22b0807d3c38e147/.github/workflows/parquet.yml)
- [`object_store` workflow](https://github.com/apache/arrow-rs-object-store/blob/84d24eb8efcec9448566de09e94d2d4b74b21ebe/.github/workflows/ci.yml)
- [DataFusion workflow](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/.github/workflows/rust.yml)

## Primary Standards

- [Cargo features and unification](https://doc.rust-lang.org/cargo/reference/features.html)
- [Cargo resolver](https://doc.rust-lang.org/cargo/reference/resolver.html)
- [Cargo target-specific dependencies](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#platform-specific-dependencies)
- [Rust conditional compilation](https://doc.rust-lang.org/reference/conditional-compilation.html)
- [`wasm32-unknown-unknown` target](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-unknown.html)
- [Fetch Standard](https://fetch.spec.whatwg.org/)
- [RFC 9110 HTTP Semantics](https://httpwg.org/specs/rfc9110.html)
