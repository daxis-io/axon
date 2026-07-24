# Upstream WASM Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Arrow Rust, Parquet, `object_store`, DataFusion, delta-kernel, and delta-rs compile and
run under the documented `wasm32-unknown-unknown` browser profile without changing native defaults
or requiring downstream feature suppression.

**Architecture:** Keep logical Cargo features stable and select implementation dependencies by exact
target. Arrow and Parquet compile unavailable-codec stubs. `object_store` keeps a host-neutral HTTP
core and adds an explicit browser host. DataFusion owns its direct feature list and a narrow browser
runtime tier. Delta keeps native and browser engines separate.

**Tech Stack:** Rust, Cargo resolver v2, `wasm32-unknown-unknown`, wasm-bindgen tests, Chrome, Firefox,
Fetch, HTTP Range and validators, GitHub Actions, Cargo metadata, Arrow IPC, Parquet, DataFusion, and
Delta Kernel.

---

## Execution Rules

- Use one isolated worktree per upstream repository.
- Fetch the repository's upstream default branch before starting each PR.
- Re-run the reproducer against that branch and record its commit.
- Use the exact target predicate
  `all(target_arch = "wasm32", target_os = "unknown")`.
- Preserve native feature lists in compatibility PRs.
- Add a required CI check only in the PR that makes it pass.
- Keep one owning concern per PR.
- Run locked graph checks and a newest-compatible resolution check.
- Do not use global `getrandom` `RUSTFLAGS` in the supported-profile jobs.
- Do not install Clang in the C-free graph jobs.

The paths below match the research revisions. Re-ground them against upstream `main` before editing.

## Task 1: Parquet Zstd Target Gate

**Repository:** `apache/arrow-rs`

**Files:**

- Modify: `parquet/Cargo.toml`
- Modify: `parquet/src/compression.rs`
- Create: `parquet/tests/wasm_codec_availability.rs`
- Modify: `.github/workflows/parquet.yml`
- Create: `dev/check_wasm_dependency_policy.sh`

**Step 1: Record the baseline**

Run:

```bash
git fetch upstream main
git rev-parse upstream/main
git status --short
rustc -Vv
cargo -V
sha256sum Cargo.lock
```

Expected: a clean isolated worktree and a recorded source and lock revision.

**Step 2: Add the failing feature-on fixture**

Add a WASM test that calls `create_codec(Compression::ZSTD(...))` with `parquet/zstd` enabled and
expects an error containing `zstd`, `wasm32-unknown-unknown`, and `backend`.

Add a compile job for:

```bash
cargo check \
  -p parquet \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 \
  --locked
```

Expected before the fix: failure through `zstd-sys`.

**Step 3: Move implementation dependencies**

Move the ordinary optional `zstd` dependency and native test dependency under:

```toml
[target.'cfg(not(all(
    target_arch = "wasm32",
    target_os = "unknown"
)))'.dependencies]
```

Keep:

```toml
default = ["arrow", "snap", "brotli", "flate2-zlib-rs", "lz4", "zstd", "base64", "simdutf8"]
zstd = ["dep:zstd"]
```

**Step 4: Split codec states**

Update `parquet/src/compression.rs` so:

- native zstd constructs `ZSTDCodec`;
- WASM feature-on returns the target-unavailable error;
- feature-off preserves the existing disabled-feature error;
- `cfg(test)` does not select the zstd module on `WASM_UNKNOWN`.

**Step 5: Add behavior tests**

Add tests for:

- feature disabled;
- native zstd round trip;
- WASM zstd unavailable;
- zstd footer and schema inspection before page decode;
- writer failure before output commit.

**Step 6: Add graph policy**

Make `dev/check_wasm_dependency_policy.sh` fail if active normal or build dependencies contain
`zstd-sys` for the explicit feature profile.

**Step 7: Verify**

Run:

```bash
cargo test -p parquet --locked
cargo test -p parquet --all-features --locked
cargo check -p parquet --target wasm32-unknown-unknown --locked
cargo check -p parquet --target wasm32-unknown-unknown --no-default-features \
  --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 \
  --locked
bash dev/check_wasm_dependency_policy.sh parquet
```

Expected: all pass; native tests use zstd; WASM graph omits `zstd-sys`.

**Step 8: Commit**

```bash
git add parquet/Cargo.toml parquet/src/compression.rs \
  parquet/tests/wasm_codec_availability.rs \
  .github/workflows/parquet.yml dev/check_wasm_dependency_policy.sh
git commit -s -m "Support feature-unified Parquet codecs on wasm32"
```

## Task 2: Arrow IPC Zstd Target Gate

**Repository:** `apache/arrow-rs`

**Files:**

- Modify: `arrow-ipc/Cargo.toml`
- Modify: `arrow-ipc/src/compression.rs`
- Modify: `arrow-ipc/src/reader.rs`
- Modify: `arrow-ipc/src/writer.rs`
- Create: `arrow-ipc/tests/wasm_codec_availability.rs`
- Modify: `.github/workflows/arrow.yml`
- Modify: `dev/check_wasm_dependency_policy.sh`

**Step 1: Add the failing checks**

Add a feature-on compile job:

```bash
cargo check \
  -p arrow-ipc \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --features lz4,zstd \
  --locked
```

Add tests for an uncompressed schema followed by a zstd record batch, plus a zstd writer request.

Expected before the fix: build failure through `zstd-sys`.

**Step 2: Move the dependency**

Move `arrow-ipc`'s optional `zstd` dependency to the inverse `WASM_UNKNOWN` table. Keep the public
`zstd` feature name.

**Step 3: Gate contexts and implementations**

Apply backend-availability gates to:

- `IpcWriteContext::compressor`;
- `DecompressionContext::decompressor`;
- zstd context constructors;
- `compress_zstd` and `decompress_zstd`;
- compression-level validation.

Compile target-unavailable stubs for WASM feature-on builds.

**Step 4: Verify reader and writer timing**

Prove:

- schema parsing succeeds before compressed batch decode;
- decompression reports the target and codec;
- writer failure occurs before the compressed message body enters the sink;
- native zstd and LZ4 tests still pass.

**Step 5: Verify**

Run:

```bash
cargo test -p arrow-ipc --all-features --locked
cargo check -p arrow-ipc --target wasm32-unknown-unknown \
  --no-default-features --features lz4,zstd --locked
bash dev/check_wasm_dependency_policy.sh arrow-ipc
```

**Step 6: Commit**

```bash
git add arrow-ipc .github/workflows/arrow.yml dev/check_wasm_dependency_policy.sh
git commit -s -m "Support feature-unified Arrow IPC codecs on wasm32"
```

## Task 3: `object_store` External Fixture And Manifest Repair

**Repository:** `apache/arrow-rs-object-store`

**Files:**

- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Modify: `src/client/mod.rs`
- Modify: `src/client/http/connection.rs`
- Modify: `src/client/crypto.rs`
- Modify: filesystem implementation modules selected by `fs`
- Create: `tests/wasm-consumer/Cargo.toml`
- Create: `tests/wasm-consumer/Cargo.lock`
- Create: `tests/wasm-consumer/src/lib.rs`
- Create: `ci/check_wasm_dependency_policy.sh`
- Modify: `.github/workflows/ci.yml`

**Step 1: Create the isolated fixture**

Give `tests/wasm-consumer/Cargo.toml` its own `[workspace]` table. Depend on the repository package
without inheriting root dev dependencies. Generate and commit the fixture lock so its required job
can use `--locked`.

Add feature profiles for:

- `http-base`;
- `http-base,reqwest,web`;
- `http`.

Run:

```bash
cargo check \
  --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown \
  --features http-base \
  --locked
```

Expected before the fix: failure through `rand -> getrandom`.

**Step 2: Define feature ownership**

Add `web` for exact `WASM_UNKNOWN` browser dependencies. Make `http-base` independent of `rand` and
Tokio runtime services. Keep the native logical `http` composition, including `aws-lc-rs`.

**Step 3: Target-gate native implementations**

Move direct AWS-LC, filesystem, and platform implementation dependencies under inverse
`WASM_UNKNOWN`. Add matching source gates. WASM filesystem construction or use returns
`Error::NotSupported`.

**Step 4: Preserve the Fetch adapter**

Keep the `HttpService for reqwest::Client` implementation that uses `spawn_local`. Add fixture code
that constructs the built-in connector under `web`.

**Step 5: Add graph assertions**

For the generic browser HTTP fixture, deny:

```text
aws-lc-sys
zstd-sys
liblzma-sys
openssl-sys
native-tls
ring
walkdir
```

**Step 6: Verify**

Run:

```bash
cargo test
cargo check --target wasm32-unknown-unknown
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http-base --locked
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http-base,reqwest,web --locked
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --features http --locked
bash ci/check_wasm_dependency_policy.sh
```

**Step 7: Commit**

```bash
git add Cargo.toml src tests/wasm-consumer ci/check_wasm_dependency_policy.sh \
  .github/workflows/ci.yml
git commit -s -m "Make HTTP object access target-safe on wasm32"
```

## Task 4: `object_store` Browser Retry Runtime

**Repository:** `apache/arrow-rs-object-store`

**Files:**

- Modify: `src/client/backoff.rs`
- Modify: `src/client/retry.rs`
- Modify: `src/client/mod.rs`
- Create: `src/client/runtime.rs`
- Modify: `tests/http.rs`
- Create: `tests/browser_retry.rs`
- Modify: `.github/workflows/ci.yml`

**Step 1: Write failing tests**

Add tests for:

- `http-base` backoff construction without entropy;
- deterministic jitter with an injected source;
- delayed retry through a browser timer;
- `NotSupported` only when a host-neutral client attempts a delayed retry without a sleep service.

Expected: the browser retry path lacks a host service, and base construction reaches the default RNG.

**Step 2: Add the internal runtime service**

Create a clock and sleep interface. Install native Tokio and browser JS implementations from the
matching feature profile.

**Step 3: Remove mandatory base entropy**

Use the existing `Backoff::new_with_rng` seam. Select deterministic or zero jitter when no source is
installed. Native and `web` profiles can inject seeded host randomness.

**Step 4: Run a real-browser retry**

Serve one transient response, advance through a nonzero delay, then return success. Assert attempt
count, elapsed clock behavior, and final body.

**Step 5: Verify and commit**

Run:

```bash
cargo test
wasm-pack test --headless --chrome --features http,web
wasm-pack test --headless --firefox --features http,web
```

Commit:

```bash
git add src/client tests .github/workflows/ci.yml
git commit -s -m "Add explicit retry runtime services for browser HTTP"
```

## Task 5: `object_store` Multipart Browser Scheduling

**Repository:** `apache/arrow-rs-object-store`

**Files:**

- Modify: `src/multipart.rs`
- Create: `src/task.rs`
- Create: `tests/browser_multipart.rs`
- Modify: `.github/workflows/ci.yml`

**Step 1: Add failing scheduler tests**

Cover bounded concurrency, first-error cancellation, completed-part ordering, local future
scheduling, and upload-buffer limits.

**Step 2: Add an internal scheduler**

Keep Tokio `JoinSet` for native code. Use local futures and a bounded futures collection for the
browser path. Do not change the public error enum.

**Step 3: Verify**

Run native multipart tests plus Chrome and Firefox browser tests. Confirm the active browser graph
does not add a multi-thread runtime.

**Step 4: Commit**

```bash
git add src/multipart.rs src/task.rs tests/browser_multipart.rs .github/workflows/ci.yml
git commit -s -m "Support bounded multipart scheduling in browsers"
```

## Task 6: `object_store` Browser Protocol Suite

**Repository:** `apache/arrow-rs-object-store`

**Files:**

- Modify: `src/client/get.rs`
- Modify: `src/client/header.rs`
- Modify: `src/http/mod.rs`
- Modify: `tests/http.rs`
- Create: `tests/browser_http_protocol.rs`
- Create: `tests/browser-server/`
- Create: `docs/browser-http.md`
- Modify: `.github/workflows/ci.yml`

**Step 1: Build the two-origin server**

Add controlled cases for `206`, ignored Range as `200`, `412`, `416`, validator change,
non-identity encoding, exposed-header omission, preflight denial, and redirects.

**Step 2: Write failing browser tests**

Lock the response policy from the design:

- ordinary non-full `200` is rejected;
- `If-Range` mismatch invalidates identity and replans;
- bounded full-body fallback requires an explicit byte ceiling;
- invalid `Content-Range` and encoded range responses fail.

**Step 3: Implement response validation**

Validate status, `Content-Range`, body length, object identity, and representation encoding before
returning bytes to a caller or cache.

**Step 4: Improve diagnostics**

Use qualified CORS or network-policy wording when Fetch exposes only a network error.

**Step 5: Verify and commit**

Run the suite in Chrome and Firefox, then commit the adapter, server, tests, workflow, and deployment
documentation together.

## Task 7: DataFusion Feature Ownership And Compression Graph

**Repository:** `apache/datafusion`

**Files:**

- Modify: `datafusion/core/Cargo.toml`
- Modify: `datafusion/common/Cargo.toml`
- Modify: `datafusion/datasource/Cargo.toml`
- Modify: `datafusion/datasource/src/file_compression_type.rs`
- Modify: `Cargo.toml`
- Create: `ci/scripts/check_wasm_dependency_policy.sh`
- Modify: `.github/workflows/rust.yml`

**Step 1: Add failing graph checks**

Add default WASM and feature-path jobs. Record reverse trees for Parquet and Arrow IPC.

Expected before the full upstream stack: native codec backends remain in the graph.

**Step 2: Replace Parquet default overrides**

Set `default-features = false` in the two direct manifests and add:

```text
arrow
async
object_store
snap
brotli
flate2-zlib-rs
lz4
zstd
base64
simdutf8
```

Keep `datafusion-physical-plan -> arrow-ipc[lz4,zstd]`.

**Step 3: Split compression implementations**

Move direct `zstd` and `liblzma` dependencies to inverse `WASM_UNKNOWN`. Give
`async-compression` native features `bzip2,gzip,xz,zstd,tokio` and WASM features
`bzip2,gzip,tokio`.

**Step 4: Add unavailable-operation errors**

In `file_compression_type.rs`, keep format recognition and return target-unavailable errors when a
WASM operation asks for zstd or xz.

**Step 5: Verify**

Run:

```bash
cargo test -p datafusion -p datafusion-common -p datafusion-datasource --locked
cargo check -p datafusion --target wasm32-unknown-unknown --locked
bash ci/scripts/check_wasm_dependency_policy.sh
```

Expected: no `zstd-sys` or `liblzma-sys` in the active WASM graph.

**Step 6: Commit**

```bash
git add Cargo.toml datafusion/core/Cargo.toml datafusion/common/Cargo.toml \
  datafusion/datasource ci/scripts/check_wasm_dependency_policy.sh \
  .github/workflows/rust.yml
git commit -s -m "Own target-safe Parquet and compression features"
```

## Task 8: DataFusion Browser Runtime Profile

**Repository:** `apache/datafusion`

**Files:**

- Modify: `datafusion/execution/src/disk_manager.rs`
- Modify: `datafusion/wasmtest/Cargo.toml`
- Modify: `datafusion/wasmtest/src/lib.rs`
- Modify: `datafusion/wasmtest/datafusion-wasm-app/`
- Modify: `.github/workflows/rust.yml`
- Create: `docs/source/user-guide/features/wasm.md`

**Step 1: Add failing profile tests**

Test in-memory SQL, browser HTTP Parquet, one partition, disk disabled, spill rejected, and
target-unavailable zstd.

Remove the global getrandom backend flag from the new supported-profile job. Keep any broader legacy
job separate.

**Step 2: Add explicit disk-disabled behavior**

Return an unsupported error before creating a temporary file or spill path.

**Step 3: Set the tested scheduler profile**

Configure one target partition and exercise the supported plan set. Do not claim safety for untested
task-spawning paths.

**Step 4: Verify**

Run native tests, the default WASM check, and Chrome and Firefox tests without Clang or global
getrandom flags.

**Step 5: Commit**

```bash
git add datafusion/execution datafusion/wasmtest docs/source/user-guide/features/wasm.md \
  .github/workflows/rust.yml
git commit -s -m "Define the initial DataFusion browser runtime profile"
```

## Task 9: delta-kernel Core And Arrow Integration

**Repository:** `delta-io/delta-kernel-rs`

**Files:**

- Modify: `kernel/Cargo.toml`
- Modify: `kernel/src/actions/mod.rs`
- Modify: `kernel/src/actions/deletion_vector.rs`
- Modify: `kernel/src/history_manager/mod.rs`
- Modify: `kernel/src/metrics/events.rs`
- Modify: `kernel/src/metrics/reporter.rs`
- Modify: `kernel/src/path.rs`
- Modify: `kernel/src/table_features/column_mapping.rs`
- Modify: `kernel/src/transaction/builder/create_table.rs`
- Modify: `kernel/src/transaction/write_context.rs`
- Modify: `default-engine/src/parquet.rs`
- Create: `ci/check_wasm_dependency_policy.sh`
- Modify: `.github/workflows/build.yml`

**Step 1: Add failing core and Arrow checks**

Run:

```bash
cargo check -p delta_kernel --target wasm32-unknown-unknown \
  --no-default-features --locked
cargo check -p delta_kernel --target wasm32-unknown-unknown \
  --no-default-features --features arrow --locked
```

Expected before the fix: core entropy failure, then codec and provider graph failures.

**Step 2: Classify entropy use**

For each `Uuid::new_v4`, `rand::rng`, and random ID call, record:

- read path;
- write path;
- temporary path;
- retry jitter;
- protocol identifier;
- metrics identifier.

Move native-engine and write-only uses out of the supported browser path. Reuse an engine service
when one already owns the operation. Add no public random capability unless multiple host
implementations need it.

**Step 3: Split `object_store_13` target features**

Preserve native provider features. On `WASM_UNKNOWN`, keep the type dependency with defaults off and
no cloud-provider batteries. Let the browser engine select HTTP or inject a store.

**Step 4: Add graph policy**

Deny native codec, native TLS, AWS-LC, and provider implementation packages in the core Arrow WASM
profile.

**Step 5: Verify and commit**

Run native kernel tests, both WASM checks, and the graph script. Commit core and default-engine
changes only after the native suite proves no behavior loss.

## Task 10: delta-rs Native And Browser Runtime Split

**Repository:** `delta-io/delta-rs`

**Files:**

- Modify: `Cargo.toml`
- Modify: `crates/core/Cargo.toml`
- Modify: `crates/deltalake/Cargo.toml`
- Create: browser-engine crate or module selected by the reviewed repository boundary
- Create: `tests/wasm-consumer/Cargo.toml`
- Create: `tests/wasm-consumer/Cargo.lock`
- Create: `tests/wasm-consumer/src/lib.rs`
- Create: `tests/browser_table_read.rs`
- Create: `ci/check_wasm_dependency_policy.sh`
- Modify: `.github/workflows/build.yml`

**Step 1: Pin provenance**

Replace the moving Buoyant branch in CI with the exact reviewed revision:

```text
8ba063f8f84fec222000f66d40d70911d7c79675
```

Keep a second job that patches to the intended upstream delta-kernel revision. Record both lock
hashes.

**Step 2: Add the failing browser fixture**

The fixture depends on release-style delta-rs packages and enables the browser read profile without
native cloud providers or the default engine. Its committed lock pins the graph used by the required
job.

Expected before the split: native engine, provider, codec, entropy, or filesystem dependencies enter
the target graph.

**Step 3: Separate dependency profiles**

Keep native defaults. Add a browser read profile that selects:

- delta-kernel core and Arrow integration;
- the browser engine;
- browser HTTP object access;
- read-only table operations.

Do not add Axon descriptors, broker policy, cache policy, or browser UI types.

**Step 4: Add browser table-read smoke**

In Chrome and Firefox:

- resolve a fixed Delta snapshot;
- read Parquet metadata;
- select an uncompressed or supported-codec column;
- execute a small projection and filter;
- return Arrow data;
- prove zstd data returns the target-unavailable error.

**Step 5: Add downstream graph policy**

Deny native codec, TLS, filesystem, spill, provider, and default-engine packages from the browser
fixture.

**Step 6: Verify**

Run:

```bash
cargo test --workspace
cargo check --manifest-path tests/wasm-consumer/Cargo.toml \
  --target wasm32-unknown-unknown --locked
bash ci/check_wasm_dependency_policy.sh
```

Run the browser smoke against the pinned fork and the upstream-patched graph.

**Step 7: Commit**

```bash
git add Cargo.toml crates tests/wasm-consumer tests/browser_table_read.rs \
  ci/check_wasm_dependency_policy.sh .github/workflows/build.yml
git commit -s -m "Separate native and browser Delta runtime profiles"
```

## Final Cross-Project Acceptance

After all ten PRs have release candidates:

1. Create an external workspace that depends on those release candidates with dependency defaults
   intact.
2. Build the default DataFusion and Delta browser profiles for `wasm32-unknown-unknown`.
3. Assert the active graph policy.
4. Run Chrome and Firefox against the two-origin object server.
5. Read independent Parquet and Arrow IPC fixtures.
6. Run the delta-rs table-read smoke with the pinned and upstream kernel graphs.
7. Record binary size, decode latency, peak memory, request count, and transferred bytes.
8. Publish the exact source revisions, lock hashes, toolchain, browser versions, commands, and support
   tier.

The cross-project gate passes when native defaults remain green, WASM builds need no Clang or global
entropy flags, unsupported codecs produce the documented errors, browser HTTP passes protocol tests,
and the downstream table read succeeds.
