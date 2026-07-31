# Upstream WASM Canonicalization Evidence

- Evidence date: 2026-07-26
- Axon base: `d1a31ec22479bb7d2fb380bfd61e00fd2f7881e8`
- Axon branch: `chore/upstream-wasm-canonicalization`
- Umbrella issue: [daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2)
- Prior exact-head POC CI:
  [30184555394](https://github.com/daxis-io/axon/actions/runs/30184555394), successful at
  `d1a31ec22479bb7d2fb380bfd61e00fd2f7881e8`
- Canonical publication boundary: no canonical push or PR is authorized.
- Close-time head audit: Arrow advanced after the phase-start fetch to
  `8e7043bf937b60e3be8586ceb3cd00349b989e1e`. The intervening commit, `8e7043bf9` /
  #10135, adds only a Parquet row-selection benchmark and benchmark dependency; it does not overlap
  codec selection, IPC compression, or the retained WASM tests.
- DataFusion first advanced to `bb670fbabec111cf74e3ae3ee78d0abec65d7569`. Commit `551c592ca`
  adds object-store-backed spill example code and two additive `DiskManagerBuilder` methods;
  `bb670fbab` removes a legacy proto field. Neither removes the current `tempfile` browser edge or
  satisfies the held runtime contract, but `551c592ca` touches `disk_manager.rs`, so both prepared
  DataFusion branches require a fresh additive transplant and focused API review before execution.
  A correction-time read-only fetch observed `7576762766d0f8081d7cd487e92be4861cf0c485`;
  the four later commits affect casts, scalar values, Parquet filter pushdown, and hash-join dynamic
  filters, not the held feature/runtime seam. The other three canonical heads were unchanged.
- The prepared Arrow PRs must likewise be refreshed additively from the close-time or later head
  immediately before canonical submission.
- Compatibility-POC update, 2026-07-30 local: delta-rs candidate
  `af7764c098bf0edf92e16de3f865b84b706780f6` and stack
  `8e5e163f67b9c85e5a71e2671e35e8991a09e4bc` now prove checkpoint replay in Chrome and Firefox.
  This closes the compatibility fixture gap only. It does not change the dependency order below or
  clear the current-head Kernel design gate.

## Fresh Bases And Dispositions

| # | Concern | Canonical base | Disposition | Replacement ref / current result |
| ---: | --- | --- | --- | --- |
| 1 | Arrow Parquet Zstd | `87cd2e526511ce75726bceb59033dfe4078a095d` | bounded semantic adaptation | `upstream/2026-07-26/wasm32-parquet-zstd` at `b7d32cebec8bf10d085ca0dc12898600086c895f`; locally verified |
| 2 | Arrow IPC Zstd | `87cd2e526511ce75726bceb59033dfe4078a095d` | clean current-head transplant | `upstream/2026-07-26/wasm32-arrow-ipc-zstd` at `6f9fabc97f321243ccb575fc49b0b26027072245`; locally verified |
| 3 | `object_store` HTTP manifest | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` | bounded semantic adaptation | `upstream/2026-07-26/wasm32-http-manifest` at `31efb0908735a2e12bbf39554dd7fdc0555adfe3`; locally verified |
| 4 | `object_store` clean-EOF retry | same | blocked by another concern | Executed only with concern 5 at `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d` |
| 5 | `object_store` range protocol | same | bounded semantic adaptation | Strong validator, exact outstanding range/size, and `If-Range` retained at `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d`; arbitrary `200` fallback excluded |
| 6 | DataFusion feature ownership | `88365ddd62b17c1eabd20ed0b064f626f9e77686` | blocked by another concern | Prepared at `b7bb98c99a50f3043c40996b7add77dcf526c7fe`; exact graph stops on current `tempfile`, then slice 7 |
| 7 | DataFusion browser runtime | same | blocked by another concern | Held locally at `343e72cad98cbac9b3686efdf5d219ab0b6fbc1a`; runtime removes `tempfile`, exact graph then stops on canonical Arrow `zstd-sys` |
| 8 | Delta Kernel target safety | `2403501198e9b132b714c9945fb3175c0364b1dd` | redesign required | No branch; [architecture decision](../research/upstream-wasm-canonicalization/kernel-architecture-decision.md) |
| 9 | delta-rs browser incubation | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` | blocked by another concern | Historical `e0fa37143e6888c06623c6a43adf1c801a189ca0` preserved; no replacement |

No complete equivalent contract had landed for any of the nine concerns at the recorded canonical
heads. Current `object_store` already retries response-body errors and validates resumed ranges;
concerns 4 and 5 retain only the uncovered clean-EOF, strong-validator, and `If-Range` safety
contract. The two downstream stops are dependency/architecture gates, not ordinary build failures.

## Additive Revision And Lock Ledger

All replacement commits are DCO-signed. Historical POC, candidate, stack, forward, and tag refs
were not moved or rewritten.

| Repository / ref | Commits over base | Head | Lock SHA-256 |
| --- | ---: | --- | --- |
| Arrow Parquet | 10 | `b7d32cebec8bf10d085ca0dc12898600086c895f` | root `8acf545edbce2e4ab6cf56f363e108e6b3067c77f73e95e10fee40b101242801`; consumer `e1f6747feeac8a4c86c5bd7f181cb0a9ae034dcd820b1d4a968c8ee31bee0ff0` |
| Arrow IPC | 5 | `6f9fabc97f321243ccb575fc49b0b26027072245` | root `8acf545edbce2e4ab6cf56f363e108e6b3067c77f73e95e10fee40b101242801`; consumer `1db8b03ef7c1900e61763224a3281a754d4a7451ceabd53e2f3ed2add34d6c48` |
| `object_store` HTTP | 20 | `31efb0908735a2e12bbf39554dd7fdc0555adfe3` | consumer `6b17cbda8eeb8bc0af9b05741f9f479888e964e1508d0411268236ba49618913` |
| `object_store` validator retry | 26 | `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d` | consumer `6b17cbda8eeb8bc0af9b05741f9f479888e964e1508d0411268236ba49618913` |
| DataFusion feature ownership | 4 | `b7bb98c99a50f3043c40996b7add77dcf526c7fe` | root `62631d5ea4dca1112e7e15bc7c638e8ca77c46318dda28d392b39206951553aa` |
| DataFusion runtime | 16 | `343e72cad98cbac9b3686efdf5d219ab0b6fbc1a` | root `62631d5ea4dca1112e7e15bc7c638e8ca77c46318dda28d392b39206951553aa` |

## Red-Green And Local Verification

### Arrow Parquet

- Red: canonical head failed the feature-unified target check in `zstd-sys` because Clang had no
  `wasm32-unknown-unknown` backend.
- Green: the same library check and `dev/check_wasm_dependency_policy.sh parquet` passed.
- Real target behavior: the isolated WASM consumer read Zstd Parquet footer/schema/three-row
  metadata, then produced the feature-enabled/target-backend-unavailable page-decode error; 1 test
  passed under `wasm-bindgen-test-runner`.
- Native behavior: `wasm_codec_availability` passed; after initializing canonical test-data
  submodules, the full package passed 1,232 unit tests, 88 integration tests with 1 ignored, 3
  row-selection tests, 11 page-index tests, 2 bloom tests, the target test, and 67 doctests with 6
  ignored. All-target/all-feature Clippy passed with `-D warnings`.
- Candidate hygiene, formatting, and `git diff --check` passed.

### Arrow IPC

- Red: canonical head failed the exact target build in `zstd-sys`.
- The historical three-row fixture was discovered not to contain a compressed body. A replacement
  4,096-row fixture made the original decoder assertion fail, proving the test now reaches Zstd.
- Green: the isolated WASM consumer passed two tests: schema-before-decode failure and writer
  rollback before record-batch commitment.
- Native all-feature package proof passed 121 unit tests, 7 integration tests, the target-aware
  fixture test, and 10 doctests. All-target/all-feature Clippy passed with `-D warnings`.
- Dependency policy, formatting, candidate hygiene, and `git diff --check` passed.

### `object_store`

- The three exact consumer profiles (`http-base`; `http-base,reqwest,web`; and native-batteries
  `http`) compile, and `tests/wasm-consumer/check-wasm-graph.sh` reports a target-safe graph.
- A deterministic CORS producer and isolated downstream consumer executed in headless Chrome 150
  and Firefox 153. The HTTP branch passed a transient `503`, fixed 25 ms browser backoff, and second
  ranged Fetch in both browsers: 1 test per browser.
- The stacked retry branch passed 3 tests per browser. The producer rejects any resumed request
  other than `Range: bytes=5-9` with `If-Range: "v1"`, while the browser assertions require the
  valid `206` to resume `helloworld` and the retry `200` to fail as range-unsupported.
- A fresh audit found that `RetryableRequest::send` accepts any successful 2xx, so the shared
  resumed-read path—not only `HttpStore`—must enforce the retry protocol. The correction rejects
  non-206 responses, changed representation sizes, enclosing ranges, and partial
  prefixes/suffixes before yielding resumed bytes. The accepted path requires the exact
  outstanding range and original total size.
- The corrected shared client proof passed 11 of 11 tests, including a real clean-EOF resume and
  all malformed-response cases. A concrete S3 mock proves provider responses converge on the
  shared 200 rejection. Both existing HTTP retry tests pass.
- The final native package proof passed 245 unit tests with 6 ignored, 3 range-file tests, the HTTP
  integration test, and 54 doctests with 2 ignored.
- Formatting, candidate hygiene, and `git diff --check` passed. All-feature/all-target Clippy
  passed with the inherited current-head `clippy::enum_variant_names` lint explicitly allowed;
  the repository-prescribed unmodified command fails on canonical source
  `RequestBuilderError`, which this slice does not change.
- A no-default native unit-test run exposed a current upstream test-only `LocalFileSystem` import;
  it is not in the exact browser consumer graph and was not broadened into this patch.
- `actionlint` passes the final object-store workflow. The Debian image, checkout action, Rustup
  installer, and wasm-pack installer are immutable references; Rustup is checksum-verified and no
  downloaded script is piped to a shell.

### DataFusion

- Feature ownership's native `file_compression_type` proof passed 2 tests.
- Runtime native disk-manager proof passed 16 tests; the Node CORS preflight server test passed.
- The feature branch graph first fails on current `datafusion-execution -> tempfile`. The runtime
  branch removes that edge and then fails on `arrow-ipc/parquet -> zstd -> zstd-sys`.
- No Daxis Arrow pin or patch table was added. The runtime branch also removes the obsolete global
  `getrandom_backend="wasm_js"` flags from the existing workflow and README. Its held workflow now
  pins the Debian image and actions, checksum-verifies a versioned Rustup binary, and passes
  `actionlint`.
- Chrome/Firefox current-head execution is therefore intentionally not claimed; it resumes after
  canonical Arrow releases provide concerns 1 and 2.

## Exact Graph And Composite Conclusion

The verified leaf graphs contain no `zstd-sys`, `liblzma-sys`, `aws-lc-sys`, `openssl-sys`,
`native-tls`, `ring`, `hyper`, `walkdir`, `tempfile`, filesystem implementation, cloud-provider
battery, or Tokio multithread feature. They use no global getrandom `RUSTFLAGS`.

A clean current-head composite cannot yet be formed without Daxis dependency pins: DataFusion
still consumes released Arrow 59.1.0. The phase therefore stops before Kernel/delta-rs integration
and retains the successful release-based Chrome/Firefox proof in run 30184555394 as the latest
complete stack evidence. No current-head performance or browser-parity improvement is claimed.

## Candidate Canonical PR Stack

Paste-ready bodies, exact tests, and reviewer notes are in the
[canonical PR draft packets](../research/upstream-wasm-canonicalization/canonical-pr-drafts.md).

1. **Arrow:** `Support feature-unified Parquet codecs on wasm32`
   - Explain feature-enabled versus target-backend-unavailable semantics.
   - Include the compiler-free graph and executable metadata/page-decode proof.
2. **Arrow:** `Support feature-unified Arrow IPC codecs on wasm32`
   - Independent base; may follow PR 1 operationally.
   - Call out the corrected genuinely compressed fixture and writer rollback.
3. **object_store:** `Expose target-safe HTTP host capabilities`
   - Review the public `http-base` / `reqwest` / `web` ownership boundary separately from policy.
4. **object_store:** `Retry truncated reads only with a strong validator`
   - Stack on PR 3; combine clean EOF with strong ETag and `If-Range`.
   - Explicitly exclude arbitrary subrange `200` fallback; continue that discussion in issue #806.
5. **DataFusion:** `Own target-safe Parquet and compression features`
   - Wait for released/accepted Arrow and `object_store` contracts.
6. **DataFusion:** `Define the browser runtime profile`
   - Stack on PR 5; preserve native spill/default behavior and execute Chrome/Firefox.
7. **Delta Kernel:** design discussion on issue #252 before any source PR.
8. **delta-rs:** no PR until Kernel exposes accepted public seams.

## Fork Removal Conditions

- Arrow fork patches: remove only after canonical releases contain both codec contracts and the
  locked Axon browser rehearsal passes.
- `object_store`: remove after a canonical release contains the explicit HTTP/runtime seam plus
  strong-validator clean-EOF retry. Fallback policy has its own issue-#806 decision.
- DataFusion: remove after compatible leaf releases and a DataFusion release contain feature
  ownership plus the memory-only browser runtime and pass Chrome/Firefox.
- Kernel: remove after maintainers accept and release a narrow adapter/capability design and the
  prefetched synchronous engine passes the denied graph.
- delta-rs: remove after the Kernel boundary and all leaf releases exist and the incubation code no
  longer imports fork-only/internal Kernel APIs.

## Publication Record

An independent fresh audit initially returned **No-Go**. It identified the missing shared retry
status/size validation, lack of a provider-path regression, mutable workflow dependencies, and a
stale Axon run citation. After the additive corrections and all three exact-head runs completed
successfully, a new independent reviewer returned **Go** with no remaining MUST FIX or SHOULD FIX
findings.

The verified leaf refs were pushed only to the corresponding Daxis forks. Two Daxis-only draft PRs
exist solely to execute arrow-rs's pull-request workflows; they are explicitly marked “do not
merge” and do not target a canonical organization:

- [daxis-io/arrow-rs#3](https://github.com/daxis-io/arrow-rs/pull/3), Parquet
- [daxis-io/arrow-rs#2](https://github.com/daxis-io/arrow-rs/pull/2), Arrow IPC

The live runs found test-infrastructure gaps that local package gates did not cover. Ignored nested
consumer workspaces did not generate their own lockfiles; Rust 1.85 selected ICU 2.2 crates
requiring a newer compiler; Arrow's MSRV scanner required explicit nested-package metadata; the
Rust container lacked Node for the executable WASM tests; and its unchanged Parquet WASI-default
gate lacked Clang plus a WASI libc sysroot. Additive DCO corrections generated nested Arrow locks,
committed an `object_store` consumer lock compatible with Rust 1.85, declared consumer MSRV, and
provisioned Node. Parquet provisions Clang and `wasi-libc` only after the compiler-free browser
compile and executable test have passed. No evidence ref was rewritten.

| Published Daxis ref | Exact tested SHA | Terminal CI |
| --- | --- | --- |
| `arrow-rs:upstream/2026-07-26/wasm32-parquet-zstd` | `b7d32cebec8bf10d085ca0dc12898600086c895f` | 13/13 workflows success; focused [parquet 30214771945](https://github.com/daxis-io/arrow-rs/actions/runs/30214771945) |
| `arrow-rs:upstream/2026-07-26/wasm32-arrow-ipc-zstd` | `6f9fabc97f321243ccb575fc49b0b26027072245` | 13/13 workflows success; focused [arrow 30214218734](https://github.com/daxis-io/arrow-rs/actions/runs/30214218734) |
| `arrow-rs-object-store:upstream/2026-07-26/wasm32-http-manifest` | `31efb0908735a2e12bbf39554dd7fdc0555adfe3` | [30216703863](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30216703863), success: compiler-free and Chrome/Firefox jobs |
| `arrow-rs-object-store:upstream/2026-07-26/wasm32-browser-retry` | `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d` | [30216702076](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30216702076), success: compiler-free and Chrome/Firefox jobs |
| `arrow-rs-object-store:upstream/2026-07-26/wasm32-browser-range-protocol` | `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d` | [30216702116](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30216702116), success: compiler-free and Chrome/Firefox jobs |

The GitHub Actions artifact API reports `total_count: 0` for the corrected object-store runs; these
workflows emit logs and conclusions but no downloadable artifact, so no artifact hash applies.

The Arrow exact-head bundles were wholly successful:

- Parquet: [arrow](https://github.com/daxis-io/arrow-rs/actions/runs/30214771944),
  [arrow-flight](https://github.com/daxis-io/arrow-rs/actions/runs/30214771916),
  [audit](https://github.com/daxis-io/arrow-rs/actions/runs/30214771907),
  [dev](https://github.com/daxis-io/arrow-rs/actions/runs/30214771898),
  [dev-pr](https://github.com/daxis-io/arrow-rs/actions/runs/30214771061),
  [docs](https://github.com/daxis-io/arrow-rs/actions/runs/30214771917),
  [integration](https://github.com/daxis-io/arrow-rs/actions/runs/30214771895),
  [Miri](https://github.com/daxis-io/arrow-rs/actions/runs/30214771920),
  [parquet](https://github.com/daxis-io/arrow-rs/actions/runs/30214771945),
  [parquet-geospatial](https://github.com/daxis-io/arrow-rs/actions/runs/30214771963),
  [parquet-variant](https://github.com/daxis-io/arrow-rs/actions/runs/30214771925),
  [parquet-derive](https://github.com/daxis-io/arrow-rs/actions/runs/30214771940), and
  [rust](https://github.com/daxis-io/arrow-rs/actions/runs/30214771948).
- Arrow IPC: [arrow](https://github.com/daxis-io/arrow-rs/actions/runs/30214218734),
  [arrow-flight](https://github.com/daxis-io/arrow-rs/actions/runs/30214218736),
  [audit](https://github.com/daxis-io/arrow-rs/actions/runs/30214218775),
  [dev](https://github.com/daxis-io/arrow-rs/actions/runs/30214218773),
  [dev-pr](https://github.com/daxis-io/arrow-rs/actions/runs/30214217699),
  [docs](https://github.com/daxis-io/arrow-rs/actions/runs/30214218763),
  [integration](https://github.com/daxis-io/arrow-rs/actions/runs/30214218792),
  [Miri](https://github.com/daxis-io/arrow-rs/actions/runs/30214218772),
  [parquet](https://github.com/daxis-io/arrow-rs/actions/runs/30214218744),
  [parquet-geospatial](https://github.com/daxis-io/arrow-rs/actions/runs/30214218743),
  [parquet-variant](https://github.com/daxis-io/arrow-rs/actions/runs/30214218738),
  [parquet-derive](https://github.com/daxis-io/arrow-rs/actions/runs/30214218790), and
  [rust](https://github.com/daxis-io/arrow-rs/actions/runs/30214218823).

The Axon documentation branch is published at
`daxis-io/axon:chore/upstream-wasm-canonicalization`. The final pre-correction documentation SHA
`b260ae2864b0e1e900e79f4be7a99c5b2698ee38` has the exact-head generic run
[30215700836](https://github.com/daxis-io/axon/actions/runs/30215700836), which reported the
inherited zero-job failure rather than a failed test. The older
[30213192153](https://github.com/daxis-io/axon/actions/runs/30213192153) is retained only as
historical intermediate evidence. The local patch-inventory regression gate passed.

The exact revisions, CI links, dependency order, and next authorization gate were posted in
[Axon issue #2](https://github.com/daxis-io/axon/issues/2#issuecomment-5084902751). Final remote-ref
verification found every Daxis branch equal to its tested local SHA. No canonical organization
remote was mutated.

The dirty Axon root was rechecked after publication work. It remained at
`3e5aceda0c1eb2c0dea983c0e5849200447a363f`, retained the same pre-existing modified and untracked
paths, and contains no canonicalization plan, research, or evidence files. Closing SHA-256 values
for the three pre-existing modified files are:

- `068fb656085f061738a71bdc1267aa258f6d82536c3da7fed29a57421a0e3af7`
  (`browser_snapshot_preflight.rs`)
- `4f9c8995346d710310be7e632bc97ae04316dd90a95c6a31211e46c06280e3fb`
  (`tests/conformance/README.md`)
- `22da1de3a6de9f0bc4ee70b59bd43adf3a476ebefb2195976e57fe7542d1a339`
  (`browser_datafusion_engine_smoke.sh`)
