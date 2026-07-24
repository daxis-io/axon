# Upstream Patch Inventory

Axon's shipping workspace contains no private downstream patch, Daxis fork revision, vendored
source, or dependency-level `[patch]` table. The excluded upstream-WASM POC workspace uses public
forks and immutable revisions until compatible canonical releases are available.

The complete proof and CI classification are recorded in
[`docs/release-gates/upstream-wasm-fork-poc-evidence.md`](../release-gates/upstream-wasm-fork-poc-evidence.md).
All rows are owned by the runtime / engine team and tracked by
[daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2).

## Active POC Patches

| Patch                                                      | Public fork                                                                           | Compatibility base                         | Candidate revision                         | Stack revision                             | Draft Daxis PR                                                 | Upstream disposition | Removal condition                                                                                                                                                                                                 |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | -------------------------------------------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Arrow and Parquet target-safe codec backends               | [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs)                           | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `f24c67c536e98f85f2ed8a289a6eb1d55916ffb9` | `39a02b83b6d7ddc41a3fc0dbe541604aebbe1fcc` | [#1](https://github.com/daxis-io/arrow-rs/pull/1)              | `proposed`           | Adopt an upstream Arrow release containing both codec slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                                                                 |
| `object_store` target-safe HTTP, retry, and range protocol | [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `1d000072b4490e4736b9aab02731cde33a2c10fa` | `54109e3e6797522ccb2ee5d43cad2a9cd0013074` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) | `proposed`           | Adopt an upstream `object_store` release containing the HTTP-manifest, retry, and range slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                               |
| DataFusion browser feature ownership and runtime profile   | [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion)                       | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `693aa0b5d2a3c925db963776a472d6144352116e` | `0d1d77af6a5974af10dfbe1e3790f3dce05ae617` | [#1](https://github.com/daxis-io/datafusion/pull/1)            | `proposed`           | Adopt upstream DataFusion releases containing the feature-ownership and browser-runtime slices after the Arrow and `object_store` prerequisites, pass the locked browser rehearsal, and remove the fork revision. |
| Delta Kernel read-only core target safety                  | [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs)             | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `9d60a72f19e678c30de6b6869cf5c62aa3576de8` | `79c48fe4e22431efb144da4a0db46bb8ff5ce9d8` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2)       | `temporary`          | Design and land a target-safe core against current canonical Kernel without broad native-core surgery, adopt the compatible release, pass downstream replay, and remove the fork revision.                        |
| delta-rs browser-engine incubation                         | [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs)                           | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `aa61ca76d699c5dac4e192af6f925b9273a6b0b4` | `bc45f077347988804e8d42249238710f90b8db97` | [#1](https://github.com/daxis-io/delta-rs/pull/1)              | `proposed`           | Land the browser-engine boundary after the Kernel redesign and preceding graph slices, adopt compatible releases, pass Axon's browser proof, and remove the fork revision.                                        |

Every accepted stack revision is also protected by the annotated tag
`daxis-poc/wasm32-browser-e2e-2026-07-23`.

## Canonical Forward Branches

No canonical PR was opened.

| Order | Concern branch                                          | Revision                                   | Readiness                                                                                         |
| ----: | ------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------- |
|     1 | Arrow `upstream/wasm32-parquet-zstd`                    | `d9d6bb626981299425671c5bd8d0b15ab67c1db6` | Ready for repository-owner review.                                                                |
|     2 | Arrow `upstream/wasm32-arrow-ipc-zstd`                  | `7eca801309b97ea379e633fad955b001dd534951` | Ready after or with slice 1.                                                                      |
|     3 | `object_store` `upstream/wasm32-http-manifest`          | `4a89dd1fc831d19570de7e84b85daefda94017d6` | CI green.                                                                                         |
|     4 | `object_store` `upstream/wasm32-browser-retry`          | `5eeda43613bfba9298ed255d724af5e0e0238eec` | CI green after slice 3.                                                                           |
|     5 | `object_store` `upstream/wasm32-browser-range-protocol` | `3dabb144a265999df74eabd19af3d22266c0c9bc` | CI green after slices 3-4.                                                                        |
|     6 | DataFusion `upstream/wasm32-feature-ownership`          | `2ae13de39344056ae0a91a4e97110b3737450bce` | Native and inherited CI green; exact compiler-independent WASM remains ordered after slices 1-5.  |
|     7 | DataFusion `upstream/wasm32-browser-runtime`            | `bb6f1012676c0c28935fe2b2768a2f8444bd8799` | DataFusion-owned checks pass; standalone browser jobs expose the ordered canonical prerequisites. |
|     8 | Delta Kernel `upstream/wasm32-core-target-safety`       | Not published                              | Blocked on a current-canonical target-safe-core redesign.                                         |
|     9 | delta-rs `upstream/wasm32-browser-engine-incubation`    | `aae56e2eb7db7a96e04d04805df83309bc54fcbc` | Candidate-clean; ordered after slices 1-8.                                                        |

Allowed `Upstream disposition` values are `proposed`, `opened`, `merged`, `wontfix`, and
`temporary`.
