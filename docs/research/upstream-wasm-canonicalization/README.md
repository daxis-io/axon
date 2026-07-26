# Upstream WASM Canonicalization Research Matrix

- Research date: 2026-07-26
- Axon control branch: `chore/upstream-wasm-canonicalization`
- Axon base: `d1a31ec22479bb7d2fb380bfd61e00fd2f7881e8`
- Umbrella issue: [daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2)
- Historical POC evidence:
  [`upstream-wasm-fork-poc-evidence.md`](../../release-gates/upstream-wasm-fork-poc-evidence.md)

This matrix reclassifies the nine POC concerns against freshly fetched canonical heads. It does not
infer readiness from the release-based POC or mechanically replay the 2026-07-23 plan.

## Control Plane Snapshot

| Repository | Canonical head | Daxis default head | Fork parent | Open Daxis draft |
| --- | --- | --- | --- | --- |
| `apache/arrow-rs` | `87cd2e526511ce75726bceb59033dfe4078a095d` | `f9bf62845ca459c16938359e9378b34a4d8c51d9` | `apache/arrow-rs` | [#1](https://github.com/daxis-io/arrow-rs/pull/1) |
| `apache/arrow-rs-object-store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` | `apache/arrow-rs-object-store` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) |
| `apache/datafusion` | `88365ddd62b17c1eabd20ed0b064f626f9e77686` | `a0a6836e4cc9f07be52cc8d1380f19ad411d67d8` | `apache/datafusion` | [#1](https://github.com/daxis-io/datafusion/pull/1) |
| `delta-io/delta-kernel-rs` | `2403501198e9b132b714c9945fb3175c0364b1dd` | `7bfb06587add017187a1b14b1195ef8f6a95ca9d` | `delta-io/delta-kernel-rs` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2) |
| `delta-io/delta-rs` | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` | `delta-io/delta-rs` | [#1](https://github.com/daxis-io/delta-rs/pull/1) |

All canonical repositories use `main`. The authenticated GitHub account is `ethan-tyler`; it has
`ADMIN` on all five Daxis forks, `READ` on the Apache repositories, and `WRITE` on canonical
`delta-io/delta-rs`. No canonical mutation is authorized by this phase.

The immutable POC tag `daxis-poc/wasm32-browser-e2e-2026-07-25` resolves to:

| Fork | Tag object | Peeled stack commit |
| --- | --- | --- |
| Arrow | `e4bedac9f2ae70eccb337820aaed3af9eb1b06f2` | `518663f4fb39ec0be672718432bbd7bb8a5456fc` |
| `object_store` | `a27fa8d1b38d30d5656740be4cd24126bddbd825` | `a04240eafdc5833e34fe21d4b348ee399177def6` |
| DataFusion | `caad12f028d6d3f41cb560fd00c849305325128a` | `aa1d3bfb591e0e10594160119d8899d4b856c3f5` |
| Delta Kernel | `8276fcb4324018507f972a8ca374f411b58306a2` | `bbccfb394bf4a3eac54e125d71996a66a5a0e13a` |
| delta-rs | `b12f79271511e5eb35d7f13331e948e1ca74dff4` | `be60607f67951459e886915e8104273880dcc5cb` |

## Disposition Summary

| Order | Concern | Classification | Current decision |
| ---: | --- | --- | --- |
| 1 | Arrow `wasm32-parquet-zstd` | bounded semantic adaptation | Execute a reduced transplant; drop the obsolete object-writer exclusion. |
| 2 | Arrow `wasm32-arrow-ipc-zstd` | clean current-head transplant | Execute after concern 1 as a separate branch/PR. |
| 3 | `object_store` `wasm32-http-manifest` | bounded semantic adaptation | Retain the current-head feature/runtime seam; add full current-base proof and browser runtime coverage. |
| 4 | `object_store` `wasm32-browser-retry` | blocked by another concern | Do not submit without concern 5's strong-validator rule. |
| 5 | `object_store` `wasm32-browser-range-protocol` | bounded semantic adaptation | Execute validator/range correctness with concern 4; defer public `200` fallback policy to issue #806. |
| 6 | DataFusion `wasm32-feature-ownership` | bounded semantic adaptation | Refresh only after the Arrow and `object_store` contracts stabilize. |
| 7 | DataFusion `wasm32-browser-runtime` | bounded semantic adaptation | Refresh after concern 6; current CI still uses Clang and global entropy flags. |
| 8 | Delta Kernel `wasm32-core-target-safety` | redesign required | Stop implementation and prepare a design decision for issue #252. |
| 9 | delta-rs `wasm32-browser-engine-incubation` | blocked by another concern | Preserve the clean forward evidence; do not advance until concern 8 has an accepted seam. |

## 1. Arrow: `wasm32-parquet-zstd`

- Comparison base: current `87cd2e526511ce75726bceb59033dfe4078a095d`; historical forward base
  `f7dfcd25aabeb01641fe4b6c35ab964fdf0b24aa`.
- Historical forward tip: `d9d6bb626981299425671c5bd8d0b15ab67c1db6`.
- Retain: `f68a32a05d2a57f7c2422d3c25a56d6055ecf8e3`,
  `5793dfad6795be215b01ab97c3c2046ff1c617b9`, and
  `d9d6bb626981299425671c5bd8d0b15ab67c1db6`.
- Drop: `b23db2e51c8df4257fa1549ae427be3def1e7641`, which removes a browser
  object-store writer that current upstream now supports.
- No equivalent canonical commit or PR exists. Historical
  [arrow-rs#1414](https://github.com/apache/arrow-rs/pull/1414) supports compiling Zstd with a C
  toolchain; it does not provide the compiler-free, feature-unified browser graph.
- Current path: `parquet/Cargo.toml`, `parquet/src/compression.rs`,
  `parquet/tests/wasm_codec_availability.rs`, `.github/workflows/parquet.yml`, and
  `dev/check_wasm_dependency_policy.sh`.
- Real failure proof: a Zstd Parquet fixture must expose footer/schema/row-count metadata, then fail
  at page decode with a codec/operation/target diagnostic. Writer failure must precede output
  commitment.
- Native impact: none to the default feature set or native Zstd backend.
- WASM impact: remove `zstd`, `zstd-safe`, and `zstd-sys` from the exact target graph while retaining
  the public `zstd` feature.
- Maintainer boundary: open a dedicated Arrow issue before a canonical PR and explicitly seek
  agreement on "feature enabled, target backend unavailable."
- Removal condition: an Arrow release containing the target-selected Parquet backend contract
  passes the Axon locked browser rehearsal.

## 2. Arrow: `wasm32-arrow-ipc-zstd`

- Historical/current-head-applicable commit:
  `7eca801309b97ea379e633fad955b001dd534951`.
- No equivalent canonical commit or PR exists. Open
  [arrow-rs#8917](https://github.com/apache/arrow-rs/issues/8917) confirms that dependency feature
  unification can activate IPC compression unexpectedly, but does not implement this fix.
- Current path: `arrow-ipc/Cargo.toml`, `arrow-ipc/src/compression.rs`,
  `arrow-ipc/src/writer.rs`, fixture/test files, `.github/workflows/arrow.yml`, and the shared graph
  policy script.
- Real failure proof: decode a real Zstd IPC stream through schema then fail at record-batch
  decompression; a failed writer must roll back the shared output buffer.
- Native impact: default features remain empty; native Zstd contexts and round trips remain.
- WASM impact: remove `zstd`, `zstd-safe`, and `zstd-sys`; retain LZ4 and the public feature.
- Maintainer boundary: separate Arrow IPC issue and PR after concern 1.
- Removal condition: an Arrow release containing the IPC target-backend contract passes the Axon
  locked browser rehearsal.

## 3. `object_store`: `wasm32-http-manifest`

- Canonical base: `84d24eb8efcec9448566de09e94d2d4b74b21ebe`.
- Current-head branch: `4a89dd1fc831d19570de7e84b85daefda94017d6`, 15 commits directly atop
  the canonical base.
- The old candidate is not a transplant: it targets pre-0.14 feature ownership and reqwest 0.12.
- Current path: root features/dependencies, client connector/request body, crypto/runtime selection,
  `tests/wasm-consumer`, graph policy, and WASM workflow.
- Public seam: `http-base`, `reqwest`, and explicit `web`; browser must not be inferred solely from
  the Rust target.
- Real consumer proof: the nested downstream Cargo fixture proves three feature profiles and their
  dependency graph. Add a real browser consumer that constructs `HttpStore`, receives a retriable
  response, sleeps through the browser timer, and performs a second Fetch.
- Native impact: native provider feature composition remains unchanged.
- WASM impact: no native TLS/crypto, cloud-provider batteries, filesystem implementations, or Tokio
  multithread scheduler.
- Overlap: [object_store#26](https://github.com/apache/arrow-rs-object-store/issues/26),
  [#624](https://github.com/apache/arrow-rs-object-store/issues/624),
  [PR #625](https://github.com/apache/arrow-rs-object-store/pull/625), and
  [#759](https://github.com/apache/arrow-rs-object-store/issues/759).
- Maintainer boundary: feature/runtime seam and downstream consumer first; compiler-free workflow
  separately if requested.
- Removal condition: an `object_store` release containing the accepted browser HTTP/runtime profile
  passes the locked graph and browser protocol rehearsal.

## 4. `object_store`: `wasm32-browser-retry`

- Current-head branch: `5eeda43613bfba9298ed255d724af5e0e0238eec`.
- Patch-equivalent current adaptation: `02d18c3`; old candidate correction: `1d00007`.
- Real producer/consumer path: `HttpResponseBody` to `GetContext::retry_stream`, then
  `GetResult::bytes()` or stream consumption. A deliberately clean EOF before declared length must
  enter retry and resume `hello` plus `world`.
- Block: `5eeda43` can resume using a weak or malformed ETag. That can splice two representations.
- Required rule: only a syntactically valid strong ETag admits retry; send `If-Range`; reject a
  missing/changed validator or `200` retry response.
- Native impact: this state machine is shared by native and browser paths.
- WASM impact: retry delay must use the explicit browser runtime, not native Tokio time or entropy.
- Maintainer boundary: submit only on top of concern 5's validator contract.
- Removal condition: an upstream release contains clean-EOF detection plus strong-validator resume
  semantics and passes native and browser transport tests.
- Current-head execution result: concerns 4 and 5 share
  `26b0b443355943c5288e5dd27fcddd889a3e2635`. A deterministic CORS producer passed the real
  truncated-body resume path in Chrome and Firefox.

## 5. `object_store`: `wasm32-browser-range-protocol`

- Current-head branch: `9b5ffc710d5c7fb38068e8a16dbe29446593a84b`.
- Concern-specific commits:
  `75aa85b`, `b3fa7a4`, `7bb14f8`, `a921dd0`, `5939071`, `3dabb14`, and
  `9b5ffc7`, on top of concerns 3 and 4.
- Real proof: `CleanEofClient`/`NonStrongEofClient` plus native `MockServer` exercise actual
  `HttpStore` Range, `If-Range`, ETag mutation, enclosing ranges, length, and identity encoding.
- Correctness contract: strong entity-tag grammar, exact/enclosing `206`, declared and actual body
  length, identity representation, no fallback after an `If-Range` retry.
- Bounded fallback: historical implementation is disabled by default and bounds retained
  accumulation, but the public policy overlaps open
  [object_store#806](https://github.com/apache/arrow-rs-object-store/issues/806).
- Adaptation: retain validator/range correctness and concern 4 retry. Remove the public arbitrary
  subrange `200` fallback from the submission branch. Prepare a separate issue-#806 proposal for
  exact-full-range `200` and optional bounded fallback.
- Native impact: stricter representation correctness; no default buffering behavior.
- WASM impact: browser Fetch behavior must be executed in Chrome and Firefox.
- Maintainer boundary: one correctness PR based on concern 3; fallback is a separate decision.
- Removal condition: an upstream release contains the validator/range/retry contract; any fallback
  removal condition is separately tied to issue #806.
- Current-head execution result: `upstream/2026-07-26/wasm32-browser-retry` and
  `upstream/2026-07-26/wasm32-browser-range-protocol` both resolve to
  `26b0b443355943c5288e5dd27fcddd889a3e2635`. Browser logs prove `Range: bytes=5-9`,
  `If-Range: "v1"`, successful `206` resume, and rejection of a retry `200`.

## 6. DataFusion: `wasm32-feature-ownership`

- Canonical base: `88365ddd62b17c1eabd20ed0b064f626f9e77686`.
- Historical forward branch: `2ae13de39344056ae0a91a4e97110b3737450bce`, based on
  `f40d99ac8b10e03a41374706e9fa07194a922ca9`.
- Current upstream still directly enables Parquet defaults, `zstd`, `liblzma`, `tempfile`, and
  filesystem `object_store` features in the browser closure.
- Current upstream issue evidence:
  [DataFusion#13815](https://github.com/apache/datafusion/issues/13815) records the DataFusion 54
  browser regression; merged [PR #19860](https://github.com/apache/datafusion/pull/19860) enables
  compression in the existing WASM test but does not provide a compiler-free graph.
- Current path: `datafusion/common/Cargo.toml`, `datafusion/core/Cargo.toml`,
  `datafusion/datasource/Cargo.toml`, `file_compression_type.rs`, and the dependency-policy script.
- Real proof: the supported browser session must read/query Parquet; xz/zstd file streams must
  distinguish feature-disabled from target-backend-unavailable.
- Native impact: preserve default features and native codec behavior.
- WASM impact: no native codecs, filesystem, cloud batteries, or multithread runtime; exactly one
  Arrow/Parquet/`object_store` universe.
- Maintainer boundary: refresh only against accepted leaf contracts and current `AGENTS.md` checks.
- Removal condition: a DataFusion release containing the target-owned compression graph passes the
  locked browser query rehearsal.
- Current-head execution result: prepared at
  `b7bb98c99a50f3043c40996b7add77dcf526c7fe`. Its exact graph stops on current
  `datafusion-execution -> tempfile`, which is owned by concern 7.

## 7. DataFusion: `wasm32-browser-runtime`

- Historical forward branch: `bb6f1012676c0c28935fe2b2768a2f8444bd8799`.
- Current canonical WASM workflow installs Clang and runs both browsers with a global
  `RUSTFLAGS='--cfg getrandom_backend="wasm_js"'`.
- Current `datafusion-execution` still enables `object_store/fs`, `tempfile`, and Tokio filesystem
  support. The historical DiskManager patch therefore needs reconciliation with current spill APIs.
- Real proof: one-partition, disk-disabled DataFusion must perform projection/filter/order/aggregate
  over an HTTP Parquet object in Chrome and Firefox. A filesystem spill request must fail before
  filesystem access.
- Native impact: native `DiskManager` defaults and custom spill factories remain unchanged.
- WASM impact: no Clang, global entropy flag, filesystem, spill, or native threads.
- Maintainer boundary: browser profile documentation and owned workflow after concern 6.
- Removal condition: a DataFusion release supplies the tested runtime profile and passes the locked
  browser rehearsal without compiler or global entropy flags.
- Current-head execution result: prepared at
  `f8fc53db63d13c437523301605ff4234c4d848e3`. The runtime patch removes `tempfile`,
  but the clean canonical graph then stops on released Arrow 59.1.0 `zstd-sys`. Global getrandom
  flags were removed; Chrome/Firefox proof is deferred until concerns 1 and 2 are canonical
  dependencies.

## 8. Delta Kernel: `wasm32-core-target-safety`

- Canonical base: `2403501198e9b132b714c9945fb3175c0364b1dd`.
- Historical candidate: `c9a475f3394adc5296c4f16587c1f69c6e87213e`.
- Histories diverge by `216 82`; no concern-bearing forward branch exists.
- [Kernel#252](https://github.com/delta-io/delta-kernel-rs/issues/252) is the current design venue.
  Closed [PR #318](https://github.com/delta-io/delta-kernel-rs/pull/318) was only a prototype.
- A synchronous prefetched adapter remains viable: browser code fetches asynchronously, then
  supplies in-memory `StorageHandler`, `JsonHandler`, and `ParquetHandler` implementations.
- A real current Arrow engine is not viable under the required graph: Arrow features pull Parquet
  and provider-heavy `object_store`; read paths also use ambient UUID/random/time facilities.
- Current exact-WASM failures include `getrandom`, `zstd-sys`, and `ring`; the graph includes cloud
  providers, filesystem, native TLS/crypto, and Tokio.
- Decision: propose a separately selectable Arrow adapter plus explicit runtime capabilities.
  Preserve synchronous handlers and native defaults. Do not promote the test-only `SyncEngine`.
- Stop condition met: the work cannot be completed without broad native-core restructuring.
- Focused decision:
  [`kernel-architecture-decision.md`](kernel-architecture-decision.md).
- Removal condition: Kernel maintainers accept a narrow adapter/capability design, it lands, and a
  released profile passes prefetched replay and the denied graph.

## 9. delta-rs: `wasm32-browser-engine-incubation`

- Canonical base: `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5`.
- Current-head forward tip: `e0fa37143e6888c06623c6a43adf1c801a189ca0`.
- Historical candidate: `0611f31ee39ef9942c04c6ccaeb44897d8ca923e`.
- The branch is candidate-clean and additive, but imports fork-only `SyncEngine` and Kernel internal
  Arrow/state interfaces. Its exact CI run
  [30182193705](https://github.com/daxis-io/delta-rs/actions/runs/30182193705) fails the
  compiler-free graph through `zstd-sys`.
- Real proof retained by the branch: version-zero replay, table-root confinement, projection and
  aggregation, valid Arrow IPC, and an 8 MiB serialized-output cap.
- Residual risks: version-zero only, no checkpoints, non-request-scoped metrics, no query working
  memory bound, no cancellation/deadlines, and ignored filter pushdown.
- Decision: preserve the branch as evidence, redesign around accepted canonical Kernel handlers,
  and do not advance or publish a stable crate.
- Removal condition: Kernel concern 8 lands, compatible leaf releases exist, and the rebased browser
  crate passes compiler-free graph plus Chrome/Firefox proof without internal/fork-only interfaces.

## Minimum Canonical PR Order

1. Arrow Parquet reduced target-backend adaptation.
2. Arrow IPC clean target-backend transplant.
3. `object_store` explicit HTTP/runtime feature seam.
4. `object_store` strong-validator, Range, and clean-EOF retry correctness.
5. Optional `object_store` `200` policy only after issue #806 consensus.
6. DataFusion compression/feature ownership.
7. DataFusion browser runtime profile.
8. Delta Kernel design decision and, only after acceptance, small adapter/capability PRs.
9. delta-rs browser incubation after Kernel and released leaf contracts.

No canonical PR may be opened until the user separately authorizes it.
