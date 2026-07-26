# Delta Kernel Browser-WASM Architecture Decision

- Date: 2026-07-26
- Canonical repository: `delta-io/delta-kernel-rs`
- Canonical base: `2403501198e9b132b714c9945fb3175c0364b1dd`
- Historical candidate: `c9a475f3394adc5296c4f16587c1f69c6e87213e`
- Design venue: [delta-kernel-rs#252](https://github.com/delta-io/delta-kernel-rs/issues/252)
- Decision: stop source implementation and seek agreement on a narrow adapter/capability seam.

## Answers To The Architecture Gate

| Question | Answer | Current-head evidence |
| --- | --- | --- |
| Can target-safe Arrow data/evaluation remain a narrow feature/profile seam? | No, not in the current crate layout. | The selectable Arrow surface also closes over Parquet, provider-heavy `object_store`, ambient random/time facilities, and native execution assumptions. |
| Can browser code prefetch asynchronously and satisfy synchronous Kernel storage/JSON handlers from memory? | Yes. | The public synchronous handler contracts can consume an immutable in-memory object/range cache populated before Kernel evaluation. No handler contract needs to become async. |
| Can a real Arrow-backed engine compile without cloud batteries, read-path entropy, Tokio multithreading, filesystem implementations, or native TLS? | No on the current head. | The exact target graph contains `getrandom`, `zstd-sys`, `ring`, cloud providers, filesystem implementations, native TLS/crypto, and Tokio features outside the browser profile. |
| Can the gap be closed without changing native defaults or restructuring broad native-core surfaces? | No. | Removing the denied closure currently crosses engine selection, data/evaluation ownership, storage/provider selection, and read-path runtime behavior. |

## Proposed Maintainer Decision

Keep Kernel's synchronous `StorageHandler`, `JsonHandler`, and `ParquetHandler` contracts. Browser
code owns asynchronous prefetch and passes a bounded, table-root-confined in-memory implementation
to Kernel. Separately select:

1. an Arrow data/evaluation adapter that does not imply Parquet, cloud providers, filesystem
   support, native TLS, or a native scheduler; and
2. explicit runtime capabilities for entropy, clock, spill/filesystem, and scheduling.

Native defaults remain unchanged and continue to select the existing native capabilities. The
test-only `SyncEngine` is not promoted as a public compatibility layer.

## Acceptance Evidence For A Future Source Branch

A future Kernel branch is unblocked only after maintainers agree that the adapter/capability split
is an acceptable review boundary. It must then prove:

- one Arrow and Kernel source universe;
- synchronous reads backed by bounded asynchronous prefetch;
- table-root confinement and exact range handling;
- no denied native dependency or multithread runtime in the exact browser graph;
- native default and all-feature compatibility; and
- downstream snapshot replay without fork-only or internal interfaces.

Until that decision is accepted, the POC candidate and tag remain immutable evidence and no
current-head Kernel source branch is published.
