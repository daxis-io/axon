# Executive Brief

## Outcome

Arrow, Parquet, and `object_store` can support `wasm32-unknown-unknown` without weakening native
defaults. The required change is target-aware implementation selection inside each owning crate.
DataFusion and Delta consumers then clean up the features and runtime services they choose.

The first upstream stack should make the present logical features safe:

- `parquet/zstd` remains part of Parquet's native default set.
- `arrow-ipc/zstd` remains the feature used by DataFusion spill code.
- `object_store/http` remains the built-in HTTP profile.
- Native builds retain the same zstd and AWS-LC implementations.
- WASM builds omit native backends and retain precise operation-time errors.

## Support Model

The design publishes four separate support levels.

| Level                     | Guarantee                                                                                                                           |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Graph and compile         | The selected crate and feature set resolves and compiles for `wasm32-unknown-unknown` without C toolchains or global backend flags. |
| Browser runtime           | The named operation runs in Chrome and Firefox under the supported runtime profile.                                                 |
| Protocol interoperability | Cross-origin range, validator, response-header, and encoding rules pass against a controlled HTTP server.                           |
| Downstream integration    | DataFusion and delta-rs can read a tested Delta or Parquet table through browser HTTP.                                              |

A compile check does not claim support for every DataFusion plan, object-store provider, Delta
operation, or browser deployment.

## Initial Browser Runtime Profile

The first DataFusion browser profile includes:

- in-memory execution;
- a disabled disk manager with no spill or local filesystem;
- one target partition;
- public or signed browser HTTP object access;
- tested Parquet and Arrow IPC codecs;
- read-only Delta snapshot and table access.

Other paths return an unsupported error or remain outside the published support claim until a browser
test covers them.

## Ownership

| Owner          | Obligation                                                                                                 |
| -------------- | ---------------------------------------------------------------------------------------------------------- |
| `parquet`      | A unified codec feature must not make an unsupported target fail to compile.                               |
| `arrow-ipc`    | Compressor and decompressor paths must distinguish disabled features from unavailable backends.            |
| `object_store` | The base HTTP layer must avoid hidden OS, JS, entropy, filesystem, and runtime requirements.               |
| DataFusion     | Direct dependencies must use explicit feature lists, and browser execution must have a named runtime tier. |
| delta-kernel   | Core and Arrow integration must compile without the native default engine.                                 |
| delta-rs       | Native and browser engines must have separate dependency profiles and pinned kernel provenance.            |

## First Work

The first two PRs target Parquet and Arrow IPC zstd. They provide the lowest-risk proof of the design:
native defaults stay the same, feature-unified WASM builds compile, and compressed data fails at the
first backend use with a target-specific error.

The next `object_store` PR lands an external-consumer fixture with the manifest repair that makes it
pass. It keeps the public native `http` composition while moving AWS-LC, filesystem code, and entropy
behind target or host boundaries.

DataFusion and Delta work follows the owning-crate fixes. Those consumers still need manifest
hygiene, but they do not carry the compatibility burden for an unsafe upstream feature.

## Deferred Work

The first stack does not add a public codec-provider interface. Parquet already has an internal codec
factory, and Arrow IPC has focused compressor and decompressor seams. Target-selected internal
backends solve the measured problem with less API surface.

Pure-Rust zstd decode remains a follow-up. Qualification must cover golden files, malformed input,
checksums, dictionaries, memory ceilings, browser size, and decode latency. A qualified decoder
should become the WASM implementation of the existing `zstd` feature. The initial follow-up should
keep zstd encoding unavailable.

Browser cloud-provider builders also remain deferred. Public or signed HTTP establishes Fetch,
CORS, range, identity, retry, and memory correctness without pulling credential discovery and signing
into the first review sequence.
