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
| Arrow and Parquet target-safe codec backends               | [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs)                           | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `f24c67c536e98f85f2ed8a289a6eb1d55916ffb9` | `518663f4fb39ec0be672718432bbd7bb8a5456fc` | [#1](https://github.com/daxis-io/arrow-rs/pull/1)              | `proposed`           | Adopt an upstream Arrow release containing both codec slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                                                                 |
| `object_store` target-safe HTTP, retry, and range protocol | [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `1d6cb49ba43e219ab50d33284c69d56cfa48aba0` | `a04240eafdc5833e34fe21d4b348ee399177def6` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) | `proposed`           | Adopt an upstream `object_store` release containing the HTTP-manifest, retry, and range slices, pass Axon's locked browser rehearsal, and remove the fork revision.                                               |
| DataFusion browser feature ownership and runtime profile   | [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion)                       | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `693aa0b5d2a3c925db963776a472d6144352116e` | `aa1d3bfb591e0e10594160119d8899d4b856c3f5` | [#1](https://github.com/daxis-io/datafusion/pull/1)            | `proposed`           | Adopt upstream DataFusion releases containing the feature-ownership and browser-runtime slices after the Arrow and `object_store` prerequisites, pass the locked browser rehearsal, and remove the fork revision. |
| Delta Kernel read-only core target safety                  | [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs)             | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `c9a475f3394adc5296c4f16587c1f69c6e87213e` | `bbccfb394bf4a3eac54e125d71996a66a5a0e13a` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2)       | `temporary`          | Design and land a target-safe core against current canonical Kernel without broad native-core surgery, adopt the compatible release, pass downstream replay, and remove the fork revision.                        |
| delta-rs browser-engine incubation                         | [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs)                           | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `0611f31ee39ef9942c04c6ccaeb44897d8ca923e` | `be60607f67951459e886915e8104273880dcc5cb` | [#1](https://github.com/daxis-io/delta-rs/pull/1)              | `proposed`           | Land the browser-engine boundary after the Kernel redesign and preceding graph slices, adopt compatible releases, pass Axon's browser proof, and remove the fork revision.                                        |

Every accepted stack revision is also protected by the annotated tag
`daxis-poc/wasm32-browser-e2e-2026-07-25`. The earlier `2026-07-23` tag remains immutable as the
superseded pre-audit evidence freeze.

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
| 3 | `object_store` | `upstream/2026-07-26/wasm32-http-manifest` | `927dfa69b1c7962cd532413171f13e5ad928cb1d` | Locally verified runtime seam and Chrome/Firefox transient-retry producer. Remove after canonical release and locked browser protocol proof. |
| 4-5 | `object_store` | `upstream/2026-07-26/wasm32-browser-retry`; `upstream/2026-07-26/wasm32-browser-range-protocol` | `26b0b443355943c5288e5dd27fcddd889a3e2635` | Chrome/Firefox prove strong-validator `If-Range` resume and strict retry `200` rejection. Arbitrary fallback is deferred to issue #806. |
| 6 | DataFusion | `upstream/2026-07-26/wasm32-feature-ownership` | `b7bb98c99a50f3043c40996b7add77dcf526c7fe` | Prepared, not publishable: blocked first by runtime `tempfile`, then by canonical Arrow releases. |
| 7 | DataFusion | `upstream/2026-07-26/wasm32-browser-runtime` | `f8fc53db63d13c437523301605ff4234c4d848e3` | Prepared, not publishable: exact graph stops on canonical Arrow `zstd-sys`. |
| 8 | Kernel | none | none | Redesign required; remove only after the adapter/capability decision lands and is released. |
| 9 | delta-rs | none | historical `e0fa37143e6888c06623c6a43adf1c801a189ca0` | Blocked on Kernel and compatible leaf releases. |

Allowed `Upstream disposition` values are `proposed`, `opened`, `merged`, `wontfix`, and
`temporary`.
