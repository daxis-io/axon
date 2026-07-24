# Arrow And Parquet Codec Architecture

## Decision

`parquet/zstd` and `arrow-ipc/zstd` remain logical wire-format features. Native targets keep the
`zstd 0.13` implementation. `wasm32-unknown-unknown` omits that dependency and compiles an
unavailable-backend path.

Unsupported codecs fail when the operation first instantiates or invokes the codec, before a writer
commits compressed output or a reader decodes compressed input.

## Why The Owning Crates Must Fix This

The pinned Parquet default set includes `zstd`, and its ordinary optional dependency is
`zstd 0.13`. Arrow IPC also declares `zstd 0.13` as an optional ordinary dependency. Both reach
`zstd-sys`.

A DataFusion edge can enable either logical feature after another consumer disabled defaults. Cargo
unions those features. Parquet and Arrow IPC must compile under that union.

Primary source:

- [Parquet manifest at the research revision](https://github.com/apache/arrow-rs/blob/b46e7334b3d4233b35868d7d22b0807d3c38e147/parquet/Cargo.toml)
- [Parquet codec factory](https://github.com/apache/arrow-rs/blob/b46e7334b3d4233b35868d7d22b0807d3c38e147/parquet/src/compression.rs)
- [Arrow IPC manifest](https://github.com/apache/arrow-rs/blob/b46e7334b3d4233b35868d7d22b0807d3c38e147/arrow-ipc/Cargo.toml)
- [Arrow IPC compression implementation](https://github.com/apache/arrow-rs/blob/b46e7334b3d4233b35868d7d22b0807d3c38e147/arrow-ipc/src/compression.rs)

## Manifest Shape

Parquet and Arrow IPC should move the native zstd dependency to the inverse target table:

```toml
[target.'cfg(not(all(
    target_arch = "wasm32",
    target_os = "unknown"
)))'.dependencies]
zstd = {
    version = "0.13",
    default-features = false,
    optional = true,
}

[features]
zstd = ["dep:zstd"]
```

Native test-only use needs the same target policy:

```toml
[target.'cfg(not(all(
    target_arch = "wasm32",
    target_os = "unknown"
)))'.dev-dependencies]
zstd = {
    version = "0.13",
    default-features = false,
}
```

The public feature and Parquet's default list do not change.

## Source Gates

Parquet's pinned source uses `#[cfg(any(feature = "zstd", test))]`. That gate is unsafe after the
dependency moves because a WASM test build sets `cfg(test)` while the native dependency is absent.

Every implementation gate must include backend availability:

```rust
#[cfg(all(
    any(feature = "zstd", test),
    not(all(
        target_arch = "wasm32",
        target_os = "unknown"
    ))
))]
mod zstd_codec;
```

The codec factory needs three branches:

```rust
// Native backend available.
#[cfg(all(
    feature = "zstd",
    not(all(target_arch = "wasm32", target_os = "unknown"))
))]

// Logical feature enabled, backend unavailable.
#[cfg(all(
    feature = "zstd",
    target_arch = "wasm32",
    target_os = "unknown"
))]

// Logical feature disabled.
#[cfg(not(feature = "zstd"))]
```

Arrow IPC needs the same audit across:

- `IpcWriteContext` compressor state;
- `DecompressionContext` decompressor state;
- `compress_zstd` and `decompress_zstd`;
- write-option validation and compression-level checks;
- file and stream reader paths;
- file and stream writer paths.

## Error Contract

| State                                                      | Expected behavior                                        |
| ---------------------------------------------------------- | -------------------------------------------------------- |
| `zstd` feature disabled                                    | Preserve the existing feature-disabled error.            |
| `zstd` enabled on a supported native target                | Construct and use the native codec.                      |
| `zstd` enabled on `wasm32-unknown-unknown` with no backend | Return an error naming the operation, codec, and target. |

Suggested Parquet text:

```text
cannot create Parquet zstd codec: feature "zstd" is enabled, but no backend is available for target wasm32-unknown-unknown
```

Suggested Arrow IPC text:

```text
cannot create Arrow IPC zstd decompressor: feature "zstd" is enabled, but no backend is available for target wasm32-unknown-unknown
```

The first PR does not need a new public error variant. Existing crate error types can carry precise
messages. A public availability API can follow if applications need to inspect support before
starting an operation.

## Reader And Writer Timing

### Parquet reader

Parquet metadata and schema can be read without decoding every data page. A zstd file should open,
expose footer and schema data, and fail only when the reader requests a zstd-compressed page.

Tests must prove:

- footer and schema inspection succeeds;
- an uncompressed selected column can still be read when its pages do not require zstd;
- a zstd-compressed selected column returns the target-unavailable error.

### Parquet writer

`create_codec` or its equivalent returns the error before page bytes enter the output sink. Existing
infallible builder methods do not need to become fallible.

### Arrow IPC reader

An uncompressed schema message can be parsed before a compressed record batch. The first zstd
record-batch decode returns the target-unavailable error.

### Arrow IPC writer

Compressor creation or the first compressed-message operation returns the error before the writer
commits a compressed body.

## Codec Policy Matrix

| Codec and target        | Pinned behavior                                                                    | First-stack policy                                                                         | Long-term policy                                                            |
| ----------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| Native zstd             | `zstd 0.13 -> zstd-sys`; Parquet default, Arrow IPC optional                       | Preserve encode, decode, defaults, and performance path.                                   | Keep native backend under logical `zstd`.                                   |
| Native Brotli           | Parquet `brotli` dependency, enabled by default                                    | Preserve encode and decode.                                                                | Keep existing backend.                                                      |
| WASM zstd before fix    | Logical feature selects `zstd-sys`; focused builds need a C-capable setup or fail. | Remove native dependency from active graph; return target-unavailable on encode or decode. | Replace the unavailable decode branch with a qualified pure-Rust decoder.   |
| WASM zstd future decode | No qualified backend in this design                                                | Deferred.                                                                                  | Use the existing `zstd` feature; choose the pure-Rust decoder by target.    |
| WASM zstd future encode | No qualified backend in this design                                                | Unavailable error.                                                                         | Defer until an encoder meets compatibility, size, latency, and ratio gates. |
| WASM Brotli             | Focused pinned check passes with the Rust `brotli` crate                           | Support encode and decode.                                                                 | Retain golden browser coverage.                                             |
| WASM Snappy             | Focused pinned check passes with `snap`                                            | Support encode and decode.                                                                 | Retain golden browser coverage.                                             |
| WASM gzip               | Focused pinned check passes with `flate2-zlib-rs`                                  | Support encode and decode through the Rust zlib-rs backend.                                | Keep backend selection explicit.                                            |
| WASM LZ4                | Focused pinned check passes with `lz4_flex`                                        | Support frame and raw forms covered by Parquet tests.                                      | Retain independent fixtures.                                                |
| Uncompressed            | No codec dependency                                                                | Support read and write.                                                                    | Baseline interoperability path.                                             |

## Pure-Rust Zstd Qualification

The follow-up should evaluate a decoder such as
[`ruzstd`](https://github.com/KillingSpark/zstd-rs) behind an internal implementation seam. The
project describes its decoder as complete and reports lower speed than native zstd. That evidence is
enough for evaluation, not adoption.

Qualification requires:

- Parquet and Arrow IPC golden corpora produced by common native writers;
- malformed, truncated, and oversized frame tests;
- frame checksum behavior;
- bounded decoder memory;
- browser bundle-size measurements;
- browser decode latency against representative files;
- dictionary and unsupported-option policy;
- fuzz or property coverage;
- native-reader validation for any browser-written output.

Backend selection should stay internal. Public `zstd-native` and `zstd-rust` features would create a
new feature-unification problem unless users have a demonstrated need to choose the backend.

## Existing C-Backed WASM Behavior

Some users may have made `zstd-sys` work with a configured C toolchain. Target-gating changes that
accidental path. Arrow maintainers should record one of two policies before merge:

- declare C-backed `wasm32-unknown-unknown` zstd unsupported and adopt the target-unavailable branch;
- preserve an advanced opt-in backend under a separate, explicit support contract.

The first policy fits Rust's target documentation, which states that this target has no matching
C/C++ target toolchain. The release note must still call out the behavior change.
