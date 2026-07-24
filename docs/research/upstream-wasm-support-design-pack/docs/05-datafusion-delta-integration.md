# DataFusion And Delta Integration

## DataFusion Owns Its Direct Features

Parquet remains responsible for compiling when its defaults or `zstd` feature enter a WASM graph.
DataFusion still owns the capabilities it requests from its direct dependencies.

At the pinned revision:

- the workspace Parquet dependency disables defaults and enables `arrow`, `async`, and
  `object_store`;
- `datafusion/core/Cargo.toml` overrides that dependency with `default-features = true`;
- `datafusion/common/Cargo.toml` has the same defaults-on override;
- `datafusion-physical-plan` enables `arrow-ipc` features `lz4,zstd` for spill support.

Primary source:

- [DataFusion workspace manifest](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/Cargo.toml)
- [`datafusion` crate manifest](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/datafusion/core/Cargo.toml)
- [`datafusion-common` manifest](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/datafusion/common/Cargo.toml)
- [`datafusion-physical-plan` manifest](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/datafusion/physical-plan/Cargo.toml)

## Parquet Feature Ownership

Both defaults-on direct edges should use the same explicit list:

```toml
parquet = {
    workspace = true,
    optional = true,
    default-features = false,
    features = [
        "arrow",
        "async",
        "object_store",
        "snap",
        "brotli",
        "flate2-zlib-rs",
        "lz4",
        "zstd",
        "base64",
        "simdutf8",
    ],
}
```

`base64` and `simdutf8` preserve capabilities in the pinned Parquet default set. Maintainers should
remove either only through a separate capability decision.

Keep:

```toml
arrow-ipc = {
    workspace = true,
    features = ["lz4", "zstd"],
}
```

The spill owner has documented why it needs both codecs. Arrow IPC's target-aware backend makes that
explicit ownership safe.

The ownership statement is:

| Concern                                               | Owner                                |
| ----------------------------------------------------- | ------------------------------------ |
| Unified Parquet features compile on the target        | `parquet`                            |
| DataFusion names the Parquet capabilities it promises | `datafusion` and `datafusion-common` |
| Spill format selects IPC codecs                       | `datafusion-physical-plan`           |
| Unified IPC codec features compile on the target      | `arrow-ipc`                          |

## DataFusion Compression

The `datafusion` default `compression` feature enables direct `zstd` and `liblzma` dependencies.
`datafusion-datasource` enables `async-compression` with `bzip2`, `gzip`, `xz`, `zstd`, and `tokio`.

The logical `compression` feature should remain. Dependency implementations become target-specific:

```toml
[target.'cfg(not(all(
    target_arch = "wasm32",
    target_os = "unknown"
)))'.dependencies]
zstd = { workspace = true, optional = true }
liblzma = { workspace = true, optional = true }
async-compression = {
    version = "0.4.40",
    optional = true,
    features = ["bzip2", "gzip", "xz", "zstd", "tokio"],
}

[target.wasm32-unknown-unknown.dependencies]
async-compression = {
    version = "0.4.40",
    optional = true,
    features = ["bzip2", "gzip", "tokio"],
}
```

Source paths for zstd and xz return target-unavailable errors on WASM. Gzip and the current Rust
bzip2 backend remain available where browser tests qualify them.

Primary source:

- [`datafusion-datasource` compression features](https://github.com/apache/datafusion/blob/17634176a4f406a50b31a3711514831c9d3ecfa0/datafusion/datasource/Cargo.toml)

## Initial DataFusion Browser Tier

Supported profile:

| Capability    | Initial contract                                                                                                        |
| ------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Data          | In-memory Arrow plus browser HTTP Parquet.                                                                              |
| Disk          | Disk manager disabled.                                                                                                  |
| Spill         | Unsupported; no temporary files.                                                                                        |
| Partitions    | One target partition.                                                                                                   |
| Scheduling    | Paths covered by browser tests; no claim for all Tokio task paths.                                                      |
| Codecs        | Uncompressed, Snappy, Brotli, gzip through zlib-rs, and LZ4; zstd returns target-unavailable until a decoder qualifies. |
| Object access | `object_store` browser HTTP profile or an injected browser-safe store.                                                  |

Disk, spill, local filesystem, untested repartition paths, and native compression paths return
unsupported errors before they invoke host facilities.

The DataFusion WASM job at the pinned revision uses Clang and a global
`getrandom_backend="wasm_js"` `RUSTFLAGS`. The new CI tier must add a build without either. The
existing job can remain as broader compatibility coverage.

## delta-kernel Boundary

The upstream `delta_kernel` crate has no default features, but its core manifest includes mandatory
`rand 0.9`. Its Arrow features select Parquet and `object_store 0.13.2`; the pinned
`object_store_13` declaration enables `aws`, `azure`, `gcp`, and `http`.

Primary source:

- [`delta_kernel` manifest](https://github.com/delta-io/delta-kernel-rs/blob/03e2537d5a6203872d9785e31cc00f503ddeb8ec/kernel/Cargo.toml)
- [`delta_kernel_default_engine` manifest](https://github.com/delta-io/delta-kernel-rs/blob/03e2537d5a6203872d9785e31cc00f503ddeb8ec/default-engine/Cargo.toml)

### Core entropy audit

Classify every random or UUID call:

| Class                     | First action                                                                                                   |
| ------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Read-path required        | Route through an existing engine or runtime service if one fits.                                               |
| Write-only                | Target-gate the operation when browser writes are outside scope.                                               |
| Temporary path generation | Keep in the native engine or replace with an engine-supplied path.                                             |
| Retry jitter              | Use the object-store or runtime jitter service.                                                                |
| Protocol identifier       | Determine whether uniqueness or cryptographic entropy is required, then place the service at the narrow owner. |

A new public random capability follows only if more than one consumer needs to supply it.

### Arrow integration

Keep the `object_store` type dependency but select provider features by target:

```toml
[target.'cfg(not(all(
    target_arch = "wasm32",
    target_os = "unknown"
)))'.dependencies.object_store_13]
package = "object_store"
version = "0.13.2"
optional = true
features = ["aws", "azure", "gcp", "http"]

[target.wasm32-unknown-unknown.dependencies.object_store_13]
package = "object_store"
version = "0.13.2"
optional = true
default-features = false
```

The browser engine chooses HTTP or injects its own `ObjectStore`. The kernel core does not infer a
browser host from the target.

### Engine split

```text
delta_kernel
    |
    +-- delta_kernel_default_engine
    |       native Tokio, reqwest TLS, filesystem, UUID
    |
    +-- browser engine
            explicit Fetch object access, browser timer, local scheduling
```

The default engine should not accumulate WASM exceptions. The browser engine can live upstream or in
a downstream crate during incubation, but it implements stable kernel engine interfaces.

## delta-rs Provenance And Runtime Split

At delta-rs revision `3f562682`, the workspace does not consume upstream delta-kernel revision
`03e2537d` direct. It aliases `delta_kernel` and `delta_kernel_default_engine` to packages on the
moving `buoyant-data/delta-kernel-rs` branch `buoyant/main`.

The branch resolved to `8ba063f8f84fec222000f66d40d70911d7c79675` during this research. The
delta-rs revision has no tracked root `Cargo.lock`.

Primary source:

- [delta-rs workspace manifest](https://github.com/delta-io/delta-rs/blob/3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5/Cargo.toml)
- [Resolved Buoyant kernel revision](https://github.com/buoyant-data/delta-kernel-rs/tree/8ba063f8f84fec222000f66d40d70911d7c79675)

Downstream smoke must state which graph it tests:

| Smoke                          | Kernel source                                                              |
| ------------------------------ | -------------------------------------------------------------------------- |
| Reproducible delta-rs baseline | Exact Buoyant fork revision.                                               |
| Upstream adoption test         | `[patch]` or branch update to the intended upstream delta-kernel revision. |

delta-rs should expose separate dependency profiles for:

- native engine and cloud providers;
- browser read engine and browser HTTP;
- shared table and protocol logic.

Native defaults remain native. A browser feature does not add Axon descriptors, credentials, or
cache policy to delta-rs.
