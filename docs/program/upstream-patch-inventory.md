# Upstream Patch Inventory

Axon's shipping workspace contains no private downstream patch, Daxis fork revision, vendored
source, or dependency-level `[patch]` table. The excluded upstream-WASM POC workspace uses public
forks and immutable revisions until compatible canonical releases are available.

Current state: no private downstream patches are checked into this repository.

The complete proof and CI classification are recorded in
[`docs/release-gates/upstream-wasm-fork-poc-evidence.md`](../release-gates/upstream-wasm-fork-poc-evidence.md).
All rows are owned by the runtime / engine team and tracked by
[daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2).

## Active POC Patches

| Patch                                                      | Public fork                                                                           | Compatibility base                         | Candidate revision                         | Stack revision                             | Draft Daxis PR                                                 | Upstream disposition | Removal condition                                                                                                                                                                                                 |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------ | ------------------------------------------ | -------------------------------------------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Arrow and Parquet target-safe codec backends               | [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs)                           | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `f24c67c536e98f85f2ed8a289a6eb1d55916ffb9` | `52c8fb2e9c28b9d89d08c313e1bc938a35c29c99` | [#1](https://github.com/daxis-io/arrow-rs/pull/1)              | `proposed`           | Adopt an upstream Arrow release containing both codec slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                                                                 |
| `object_store` target-safe HTTP, retry, and range protocol | [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `502ec006d58e11f0921a173210d54a4485d1f5a3` | `ab9fda65805487edf5487e63082cab8111f0a178` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) | `proposed`           | Adopt an upstream `object_store` release containing the HTTP-manifest, retry, and range slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                               |
| DataFusion browser feature ownership and runtime profile   | [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion)                       | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `693aa0b5d2a3c925db963776a472d6144352116e` | `54a376b161a059d08c806d3e959b87802a85ec4f` | [#1](https://github.com/daxis-io/datafusion/pull/1)            | `proposed`           | Adopt upstream DataFusion releases containing the feature-ownership and browser-runtime slices after the Arrow and `object_store` prerequisites, pass the locked browser rehearsal, and remove the fork revision. |
| Delta Kernel read-only core target safety                  | [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs)             | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `c9a475f3394adc5296c4f16587c1f69c6e87213e` | `056f7223af0c5c6d6e56502615c3943cfb94132a` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2)       | `temporary`          | Design and land a target-safe core against current canonical Kernel without broad native-core surgery, adopt the compatible release, pass downstream replay, and remove the fork revision.                        |
| delta-rs browser-engine incubation                         | [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs)                           | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `af7764c098bf0edf92e16de3f865b84b706780f6` | `8e5e163f67b9c85e5a71e2671e35e8991a09e4bc` | [#1](https://github.com/daxis-io/delta-rs/pull/1)              | `proposed`           | Land the browser-engine boundary after the Kernel redesign and preceding graph slices, adopt compatible releases, pass Axon's browser proof, and remove the fork revision.                                        |

Every accepted stack revision is also protected by the annotated tag
`daxis-poc/wasm32-browser-e2e-2026-07-30`. The earlier `2026-07-27`, `2026-07-25`, and
`2026-07-23` tags remain immutable as superseded evidence freezes.

The `object_store` candidate revision above carries the resumed-range correction backported from
the current-upstream clean branch. The delta-rs candidate adds checkpoint replay and its stack
revision repins that candidate with the accepted dependency graph. The other three candidate
revisions are unchanged.

## Canonical Forward Branches

No canonical PR was opened.

| Order | Concern branch                                          | Revision                                   | Readiness                                                                                         |
| ----: | ------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------- |
|     1 | Arrow `upstream/wasm32-parquet-zstd`                    | `d9d6bb626981299425671c5bd8d0b15ab67c1db6` | Ready for repository-owner review.                                                                |
|     2 | Arrow `upstream/wasm32-arrow-ipc-zstd`                  | `7eca801309b97ea379e633fad955b001dd534951` | Ready after or with slice 1.                                                                      |
|     3 | `object_store` `upstream/wasm32-http-manifest`          | `4a89dd1fc831d19570de7e84b85daefda94017d6` | CI green.                                                                                         |
|     4 | `object_store` `upstream/wasm32-browser-retry`          | `5eeda43613bfba9298ed255d724af5e0e0238eec` | CI green after slice 3.                                                                           |
|     5 | `object_store` `upstream/wasm32-browser-range-protocol` | `9b5ffc710d5c7fb38068e8a16dbe29446593a84b` | CI green after slices 3-4; includes the strong-validator retry correction.                       |
|     6 | DataFusion `upstream/wasm32-feature-ownership`          | `2ae13de39344056ae0a91a4e97110b3737450bce` | Native and inherited CI green; exact compiler-independent WASM remains ordered after slices 1-5.  |
|     7 | DataFusion `upstream/wasm32-browser-runtime`            | `bb6f1012676c0c28935fe2b2768a2f8444bd8799` | DataFusion-owned checks pass; standalone browser jobs expose the ordered canonical prerequisites. |
|     8 | Delta Kernel `upstream/wasm32-core-target-safety`       | Not published                              | Blocked on a current-canonical target-safe-core redesign.                                         |
|     9 | delta-rs `upstream/wasm32-browser-engine-incubation`    | `e0fa37143e6888c06623c6a43adf1c801a189ca0` | Candidate-clean; includes bounded IPC and path confinement; ordered after slices 1-8.             |

## 2026-07-26 Current-Head Canonicalization

The detailed commands and conclusions are in
[`upstream-wasm-canonicalization-evidence.md`](../release-gates/upstream-wasm-canonicalization-evidence.md).
These additive refs do not replace or move the historical evidence refs above.

| Order | Owner | Replacement Daxis ref | Revision | Disposition and removal condition |
| ---: | --- | --- | --- | --- |
| 1 | Arrow | `upstream/2026-07-26/wasm32-parquet-zstd` | `b7d32cebec8bf10d085ca0dc12898600086c895f` | Locally verified bounded adaptation. Remove after a canonical Arrow release carries the contract and Axon's locked rehearsal passes. |
| 2 | Arrow | `upstream/2026-07-26/wasm32-arrow-ipc-zstd` | `6f9fabc97f321243ccb575fc49b0b26027072245` | Locally verified clean transplant plus corrected compressed fixture. Same release/rehearsal removal gate. |
| 3 | `object_store` | `upstream/2026-07-26/wasm32-http-manifest` | `31efb0908735a2e12bbf39554dd7fdc0555adfe3` | Locally verified runtime seam and Chrome/Firefox transient-retry producer; workflow inputs are immutable. Remove after canonical release and locked browser protocol proof. |
| 4-5 | `object_store` | `upstream/2026-07-26/wasm32-browser-retry`; `upstream/2026-07-26/wasm32-browser-range-protocol` | `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d` | Shared and S3-path tests require retry `206`, unchanged total size, exact outstanding range, strong validator, and `If-Range`; Chrome/Firefox prove the producer path. Arbitrary fallback is deferred to issue #806. |
| 6 | DataFusion | `upstream/2026-07-26/wasm32-feature-ownership` | `b7bb98c99a50f3043c40996b7add77dcf526c7fe` | Prepared, not publishable: blocked first by runtime `tempfile`, then by canonical Arrow releases. |
| 7 | DataFusion | `upstream/2026-07-26/wasm32-browser-runtime` | `343e72cad98cbac9b3686efdf5d219ab0b6fbc1a` | Held locally, not publishable: exact graph stops on canonical Arrow `zstd-sys`; workflow inputs are immutable and lint-clean. |
| 8 | Kernel | none | none | Redesign required; remove only after the adapter/capability decision lands and is released. |
| 9 | delta-rs | none | historical `e0fa37143e6888c06623c6a43adf1c801a189ca0` | Blocked on Kernel and compatible leaf releases. |

Allowed `Upstream disposition` values are `proposed`, `opened`, `merged`, `wontfix`, and
`temporary`.
