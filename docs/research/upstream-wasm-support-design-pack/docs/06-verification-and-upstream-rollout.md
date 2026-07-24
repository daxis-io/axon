# Verification And Upstream Rollout

## Verification Principles

Each gate states the guarantee it proves:

| Gate class                | Proof                                                                                                |
| ------------------------- | ---------------------------------------------------------------------------------------------------- |
| Graph and compile         | Cargo resolves and Rust compiles the named feature profile for the target.                           |
| Browser runtime           | A real browser runs the named operation without native host assumptions.                             |
| Protocol interoperability | The adapter handles HTTP, CORS, validators, ranges, and encoding against a controlled second origin. |
| Downstream integration    | A consumer uses released-style dependencies to read a representative table or file.                  |

The suite uses locked and newest-compatible jobs. Locked jobs reproduce the reviewed graph.
Newest-compatible jobs expose registry drift inside declared version ranges.

## CI Matrix

| Repository             | Profile                      | Representative command or test                                                                                                                                                  | Guarantee                        |
| ---------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| Arrow Rust             | Native defaults              | `cargo test -p parquet -p arrow-ipc --locked`                                                                                                                                   | Native regression                |
| Arrow Rust             | Native all codec features    | `cargo test -p parquet --all-features --locked` plus `cargo test -p arrow-ipc --all-features --locked`                                                                          | Native codec regression          |
| Arrow Rust             | WASM Parquet default         | `cargo check -p parquet --target wasm32-unknown-unknown --locked`                                                                                                               | Graph and compile                |
| Arrow Rust             | WASM Parquet explicit codecs | `cargo check -p parquet --target wasm32-unknown-unknown --no-default-features --features arrow,async,object_store,snap,brotli,flate2-zlib-rs,lz4,zstd,base64,simdutf8 --locked` | Feature-unified graph            |
| Arrow Rust             | WASM Arrow IPC codecs        | `cargo check -p arrow-ipc --target wasm32-unknown-unknown --no-default-features --features lz4,zstd --locked`                                                                   | Feature-unified graph            |
| `object_store`         | Native defaults and HTTP     | `cargo test` plus a curated native `http` provider job                                                                                                                          | Native regression                |
| `object_store`         | WASM defaults                | `cargo check --target wasm32-unknown-unknown`                                                                                                                                   | Default graph                    |
| `object_store` fixture | WASM host-neutral HTTP       | `cargo check --manifest-path tests/wasm-consumer/Cargo.toml --target wasm32-unknown-unknown --features http-base --locked`                                                      | No JS or entropy requirement     |
| `object_store` fixture | WASM browser HTTP            | `cargo check --manifest-path tests/wasm-consumer/Cargo.toml --target wasm32-unknown-unknown --features http,web --locked`                                                       | Built-in browser host graph      |
| `object_store`         | Chrome and Firefox           | Two-origin Fetch, retry, range, identity, and upload suite                                                                                                                      | Browser runtime and protocol     |
| DataFusion             | Native defaults              | `cargo test -p datafusion --locked`                                                                                                                                             | Native regression                |
| DataFusion             | WASM defaults                | `cargo check -p datafusion --target wasm32-unknown-unknown --locked`                                                                                                            | Default graph                    |
| DataFusion             | Browser profile              | `wasm-pack test --headless` for Chrome and Firefox without global getrandom flags                                                                                               | Browser runtime                  |
| delta-kernel           | Core minimal                 | `cargo check -p delta_kernel --target wasm32-unknown-unknown --no-default-features --locked`                                                                                    | Engine-neutral core graph        |
| delta-kernel           | Arrow integration            | `cargo check -p delta_kernel --target wasm32-unknown-unknown --no-default-features --features arrow --locked`                                                                   | Arrow and Parquet consumer graph |
| delta-rs fixture       | Browser read engine          | Build an isolated consumer against the pinned kernel revision                                                                                                                   | Downstream graph                 |
| delta-rs browser smoke | Delta table read             | Resolve a fixture snapshot and read selected Parquet data in Chrome and Firefox                                                                                                 | Downstream integration           |

## Environment Policy

Required WASM graph jobs run:

- in a Linux image without an installed Clang package;
- with `RUSTFLAGS`, `CC`, and target-specific compiler variables unset;
- with `--locked` for a tracked repository lock or a committed fixture lock;
- with a separate generated-lock job for newest-compatible versions;
- without workspace dev dependencies supplying `getrandom/*/wasm_js`.

A no-Clang image does not prove graph absence. It complements the target-filtered dependency policy
below.

## Target-Filtered Graph Policy

Each repository should keep a small script that:

1. runs `cargo metadata --format-version 1 --filter-platform wasm32-unknown-unknown` with the target
   feature profile;
2. traverses active normal and build dependencies;
3. reports the path from the selected root package to every denied package;
4. fails when an unconditional deny entry is present;
5. records contextual entries for review.

Unconditional deny list for the supported browser profiles:

```text
zstd-sys
liblzma-sys
aws-lc-sys
openssl-sys
native-tls
```

Contextual policy:

| Package or feature | Rule                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------- |
| `ring`             | Denied in the generic HTTP browser fixture; a future browser crypto-provider fixture may define another rule. |
| `walkdir`          | Denied when checking the host-neutral and browser HTTP profiles.                                              |
| `tempfile`         | Denied in the DataFusion browser runtime profile.                                                             |
| `object_store/fs`  | Denied in downstream browser-engine fixtures.                                                                 |

Human investigation can use:

```bash
cargo tree \
  --target wasm32-unknown-unknown \
  -e features \
  -i zstd-sys
```

The script, its policy file, and its fixture manifest land with the production change that makes the
new required check pass.

## Independent Golden Fixtures

Same-implementation round trips do not prove wire compatibility. Every supported codec needs:

- a fixture produced by an independent native implementation;
- a real-browser read;
- a browser write where writing is supported;
- native validation of browser-written output in another job.

### Parquet zstd unavailable case

Use a fixed zstd Parquet file and prove:

- footer and schema inspection succeeds;
- reading the compressed column returns the target-unavailable error;
- an independent uncompressed column remains readable when file layout permits;
- configuring or first using a zstd writer fails before output is committed.

### Arrow IPC zstd unavailable case

Use an IPC stream with an uncompressed schema message and a zstd record batch. Prove:

- schema parsing succeeds;
- record-batch decompression returns the target-unavailable error;
- compressor and decompressor messages name codec, operation, and target.

### Supported codecs

Keep native-generated fixtures for:

- Snappy Parquet;
- Brotli Parquet;
- gzip Parquet through zlib-rs;
- LZ4 Parquet and Arrow IPC;
- uncompressed Parquet and Arrow IPC.

## Browser Protocol Suite

Run the application and object server on different origins. Cover:

| Case                                 | Expected result                                                        |
| ------------------------------------ | ---------------------------------------------------------------------- |
| `bytes=N-M` valid response           | `206` accepted after `Content-Range` and body-length validation.       |
| `bytes=N-` valid response            | `206` accepted.                                                        |
| Suffix range                         | Preflight observed; behavior follows server policy.                    |
| Multiple ranges                      | Preflight observed or client rejects unsupported request construction. |
| Ordinary Range ignored as `200`      | Rejected before full-body buffering.                                   |
| `If-Range` match                     | `206` accepted under the same strong identity.                         |
| `If-Range` mismatch                  | `200` causes identity invalidation and replan.                         |
| `If-Match` mismatch                  | `412` becomes object-changed.                                          |
| Invalid or missing `Content-Range`   | Rejected.                                                              |
| Non-identity ranged response         | Rejected.                                                              |
| Required response header not exposed | Capability-specific diagnostic.                                        |
| CORS preflight failure               | Qualified browser network-policy diagnostic.                           |
| Retry after transient response       | Browser timer path executes.                                           |
| Multipart or upload limit            | Memory and concurrency bounds enforced.                                |

## Mergeable PR Stack

Every PR includes the production fix needed by its new required check. A reproducer may appear first
as a non-blocking diagnostic in a tracking issue, but a known-red job does not become required before
the fix.

### 1. Parquet zstd target gate and CI

- Move `zstd` and its dev dependency under inverse `WASM_UNKNOWN`.
- Gate the codec module and `cfg(test)` paths.
- Add feature-disabled, native-functional, and WASM-unavailable tests.
- Add the default and explicit-codec WASM checks.
- Deny `zstd-sys` in the active WASM graph.

### 2. Arrow IPC zstd target gate and CI

- Target-gate the dependency and compressor/decompressor context fields.
- Add target-unavailable stubs for read and write.
- Add the feature-on WASM check.
- Keep native compression regressions and golden IPC coverage.

### 3. `object_store` external fixture and manifest repair

- Add an isolated external consumer in the same PR that makes it pass.
- Make `http-base` host-neutral.
- Add the explicit `web` profile.
- Target-gate filesystem and direct crypto implementation dependencies.
- Keep native `http` feature composition.

### 4. `object_store` browser retry runtime

- Complete the clock and sleep service.
- Remove mandatory host RNG from base jitter.
- Add a real-browser delayed-retry test.

### 5. `object_store` multipart scheduling

- Replace the browser `JoinSet` assumption behind an internal scheduler.
- Preserve cancellation, concurrency bounds, and the public error type.

### 6. `object_store` browser protocol suite

- Add the two-origin server.
- Cover ranges, validators, response encoding, CORS, redirects, and upload bounds.
- Publish the browser HTTP deployment contract.

### 7. DataFusion feature ownership and compression graph

- Replace defaults-on Parquet edges with the explicit capability list.
- Keep explicit Arrow IPC codec ownership.
- Target-gate direct zstd and liblzma implementations.
- Split `async-compression` backend features by target.
- Add target-unavailable errors.

### 8. DataFusion browser runtime profile

- Disable disk and spill.
- Remove the global getrandom flag from the supported-profile job.
- Set one target partition.
- Add Chrome and Firefox execution tests for the published profile.

### 9. delta-kernel core and Arrow integration

- Inventory entropy and UUID uses.
- Gate write-only or native-engine operations.
- Add the core no-feature WASM check.
- Add Arrow integration without provider batteries in the active WASM graph.

### 10. delta-rs runtime split

- Pin the kernel fork revision used by CI.
- Separate native and browser engine dependencies.
- Add a browser table-read smoke.
- Add a second smoke patched to the intended upstream delta-kernel revision.

Cloud-provider browser builders follow this stack after public or signed HTTP is stable.

## Release Gates

A crate release can claim compile support after its graph jobs pass. Browser runtime support needs
Chrome and Firefox proof. Protocol support needs the two-origin suite. Downstream support needs the
DataFusion or delta-rs smoke against release-style dependencies.

Release notes must name:

- the target and feature profile;
- codecs that compile but lack a backend;
- browser runtime limits;
- any change to accidental C-backed WASM behavior;
- the tested dependency revisions.
