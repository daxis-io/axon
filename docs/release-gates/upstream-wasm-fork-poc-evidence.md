# Daxis Upstream-WASM Fork POC Evidence

- Technical POC status: complete after the resumed-range correction repin
- Canonical-upstream status: prepared, with the Delta Kernel and delta-rs slices blocked as
  described below
- Original POC date: 2026-07-23
- Audit closure date: 2026-07-25 local / 2026-07-26 UTC
- Resumed-range repin date: 2026-07-27 local / 2026-07-28 UTC
- Umbrella issue: [daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2)
- Axon compatibility base: `62d4c465e10dc329221023eaaf2c67c542c408ce`
- Current immutable POC tag in every fork: `daxis-poc/wasm32-browser-e2e-2026-07-27`
- Superseded tags retained without mutation: `daxis-poc/wasm32-browser-e2e-2026-07-25`,
  `daxis-poc/wasm32-browser-e2e-2026-07-23`
- Raw evidence root:
  `target/upstream-wasm-fork-poc-evidence/f2feffe7c851daed01802749b0f8eec16b5c265d6c2eaacceede075d1c9759f3/`

Each section below is an immutable record of what was proven at its own date. Where a revision,
tag, measurement, or artifact hash differs, the newest section is authoritative.

## Resumed-Range Correction Repin

### Why The Previous Stack Was Replaced

Canonical forward-port work on current upstream found a defect in the shared resumed-read retry
path that the accepted POC stack still carried. Before delivering any resumed byte, that path:

- accepted any successful 2xx continuation rather than requiring `206`;
- ignored a change in the total representation size reported by `Content-Range`; and
- accepted an enclosing range and trimmed the unwanted prefix, hiding a server that had ignored
  the requested range.

The fix landed on the current-upstream clean branch
`upstream/2026-07-26/wasm32-browser-retry` as `bc578ea1c52cbc572e6374c24a5fd731800ac17d`
("fix: validate resumed object ranges"). Note that the canonicalization record cites the branch
tip `d0066c218eaf3336bc6b5e5ca3141fe78e4fea8d`, which is a shellcheck one-liner with no source
change; `bc578ea` is the substantive commit.

That branch is rooted at current canonical `object_store`, not at the `0.13.2` compatibility base,
so the correction was backported onto the POC candidate rather than consumed directly. The
backport is the same change minus one hunk that belongs to the newer base
(`extensions: parts.extensions` on `GetResult`, which `0.13.2` does not have).

The POC's fork-only bounded full-object fallback
(`HttpBuilder::with_max_full_object_fallback_size`) is deliberately **not** included in the
correction. It concerns the initial request, not the resumed-read path, and the canonical lane
defers the arbitrary-`200` policy to upstream issue #806.

### Accepted Revisions

| Repository | Candidate revision | Stack revision | Tag object |
| ---------- | ------------------ | -------------- | ---------- |
| [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `502ec006d58e11f0921a173210d54a4485d1f5a3` | `ab9fda65805487edf5487e63082cab8111f0a178` | `2b35798aadda` |
| [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs) | unchanged (`f24c67c536e98f85f2ed8a289a6eb1d55916ffb9`) | `52c8fb2e9c28b9d89d08c313e1bc938a35c29c99` | `8cfa79bdf95d` |
| [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion) | unchanged (`693aa0b5d2a3c925db963776a472d6144352116e`) | `54a376b161a059d08c806d3e959b87802a85ec4f` | `3a123ac82bb1` |
| [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs) | unchanged (`c9a475f3394adc5296c4f16587c1f69c6e87213e`) | `056f7223af0c5c6d6e56502615c3943cfb94132a` | `5b9ed8813d15` |
| [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs) | unchanged (`af7764c098bf0edf92e16de3f865b84b706780f6`) | `2481e60db2a2fcfb0d5f723fd4fada1dcf05106c` | `6c7c5deffb5c` |

Only `object_store` carries a new candidate revision. The other four forks carry a single additive
stack commit that repins the corrected leaf; no candidate, stack, forward-port revision, or tag
was rewritten.

### Native Gate

The `object_store` candidate passes its default-feature suite (83 passed, 1 ignored) and its
all-feature suite (201 passed, 4 ignored, plus integration and doc tests), `cargo fmt --check`, and
`cargo clippy --all-targets --all-features` with no new diagnostics. The two surviving Clippy
warnings are pre-existing in `src/client/list.rs`.

Four focused tests cover the corrected path directly. Reverting only the three production guards
while keeping the tests fails three of them, each by accepting `b"world"` where the read must be
rejected; the fourth already failed correctly under the previous code's final `else` arm. An S3
mock (`test_range_rejects_s3_200_response`) proves provider paths converge on the same rejection.

One pre-existing POC-lane test, `test_retry_validates_content_range_and_sends_if_range`, asserted
the removed behavior: it answered an outstanding `5..10` with an enclosing `bytes 0-9/10`. Its
purpose is to prove the continuation carries `Range`, `If-Range`, and `Accept-Encoding`, so its
mock was corrected to a properly framed `bytes 5-9/10`. Enclosing-range rejection is now covered by
its own test.

### Browser Gate

The two-origin suite adds four scenarios in which the continuation's validator still matches but
its framing does not cover exactly the outstanding range. The 36-byte object truncates at 18, so
each is answered against an outstanding range of `18..36`. Chrome `150.0.7871.187` and Firefox
`144.0.2` produced identical results:

| Scenario | Continuation | GET statuses | Diagnostic |
| -------- | ------------ | ------------ | ---------- |
| `retry-non-partial` | `200` with a matching validator | `200`, `200` | `Server did not honor If-Range …` |
| `retry-changed-size` | `bytes 18-35/37` | `200`, `206` | `Retry response changed object size from 36 to 37` |
| `retry-enclosing` | `bytes 0-35/36` | `200`, `206` | `Requested 18..36, got 0..36` |
| `retry-shifted-range` | `bytes 19-35/36` | `200`, `206` | `Requested 18..36, got 19..36` |

Each records exactly two GETs, so the malformed continuation is rejected without a further request
and without delivering a resumed byte. Before the correction, `retry-enclosing` would have
succeeded and returned the complete object.

`retry-non-partial` is rejected by the HTTP store's own `If-Range` guard, which requires a `206`
before it will compare validators, so it never reaches the shared `NotPartial` check. That
diagnostic names the validator even when the rejection was caused by the status; the shared check
is covered by the unit tests and the S3 mock instead. The store-level message is worth tightening
before the canonical PR, and is recorded here rather than fixed in this repin.

The unchanged gates still pass: snapshot `0`, `alpha=7,beta=10`, row count `2`, one Arrow IPC
stream whose SHA-256 matches across both engines, `browser_wasm`, no native fallback, and the zstd
fixture reaching schema replay before failing at the first compressed page.

### Local Measurements

| Browser | Version | Cold end-to-end | Warm median | Warm max | WASM memory high-water |
| ------- | ------- | --------------: | ----------: | -------: | ---------------------: |
| Chrome | `150.0.7871.187` | 167.52 ms | 5.8 ms | 7.3 ms | 14,614,528 bytes |
| Firefox | `144.0.2` | 381.27 ms | 11.0 ms | 13.0 ms | 14,614,528 bytes |

The local bundle is 28,139,523 raw bytes, 6,688,799 gzip bytes, and 4,303,938 Brotli bytes, with
WASM SHA-256 `0a3f3e0d6f020241e174e30dd3ba94bf954b56215b93c85152a65a1dfcfe8970`.

| Artifact | SHA-256 |
| -------- | ------- |
| `stack.lock.toml` | `f2feffe7c851daed01802749b0f8eec16b5c265d6c2eaacceede075d1c9759f3` |
| Browser `Cargo.lock` | `6c433951fa8fe03f6e414f27a6cdd7f7bdb88b30623a665dea22ca3927bed988` |

### Graph And Boundary Gates

`verify_upstream_wasm_fork_stack.sh --final` reports
`mode=final repositories=5 graph_packages=250` — the same package count as the superseded stack.
The exact `wasm32-unknown-unknown` graph compiles `--locked`, `verify-browser-graph.sh` passes the
denied-dependency and single-source-universe policy, the released-crate fixtures verify unchanged,
and `cargo test -p query-contract -p browser-sdk` passes.

The two `object_store 0.13.2` entries in the browser `Cargo.lock` are expected, not a duplicate
universe: `delta-kernel-rs` declares `object_store_13_native` (crates.io) under
`cfg(not(all(target_arch = "wasm32", target_os = "unknown")))` and `object_store_13_stack` (the
fork) for the browser. A `Cargo.lock` is target-agnostic, so both appear; only the fork enters the
`wasm32` graph, which is what both verifiers check.

### Continuous Coverage

`.github/workflows/upstream-wasm-fork-poc.yml` previously triggered only on pushes to
`poc/upstream-wasm-fork-stack`. That branch is already an ancestor of `main`, so the trigger could
never fire again and the proof could go stale without any signal. It now runs on `pull_request` and
on pushes to `main`, filtered to `poc/**`, the stack verifiers, `crates/browser-sdk/**`, and
`crates/query-contract/**` — the last two because they are the only shipping surface the harness
consumes at the result boundary.

## Audit Closure

Preserved as the immutable 2026-07-25 freeze. Its revisions, tags, measurements, and artifact
hashes were superseded by the resumed-range repin above; the findings and corrections it records
remain in force and are carried forward by that stack.

### Findings And Additive Corrections

| Finding | Witnessed red | Correction and green proof |
| ------- | ------------- | -------------------------- |
| `object_store` could retry and stitch a truncated representation after a weak or malformed ETag. | A `W/"abc"` response attempted a second request. | Candidate `1d6cb49ba43e219ab50d33284c69d56cfa48aba0` accepts only an RFC entity-tag-shaped strong validator before continuation. Missing, weak, unquoted, unterminated, list-shaped, and whitespace-containing values all stop after one request; strong-validator retry and mutation rejection still pass. Twelve focused tests and the all-feature suite passed: 196 passed and 4 ignored, plus integration and doc tests. |
| The encoded-range browser scenario could fail at generic `200` range framing before reaching identity-encoding validation. | The previous server returned a gzip full-object `200`, and the harness accepted three unrelated alternative errors. | Axon `4837c331b98e911cb7f9d4d87c3094b942461bb8` returns a correctly framed four-byte `206` with `Content-Encoding: identity, identity`. Chrome and Firefox each record that `206` and then require `Range response used unsupported Content-Encoding "identity, identity"; expected identity`. |
| delta-rs enforced the 8 MiB IPC budget only after collecting and allocating the complete result. | The prior query path collected all batches before serialization and length validation. | Candidate `af7764c098bf0edf92e16de3f865b84b706780f6` uses `execute_stream` and a capped IPC writer. The regression test proves the buffer never exceeds its limit and the query stream is not polled after the first over-budget write. |
| Delta add paths could escape the configured table prefix. | `../outside.parquet` resolved outside the table root. | The same delta-rs candidate rejects cross-origin URLs, absolute paths, traversal, encoded traversal, and prefix escapes with `ActiveFileOutsideTable`, while allowing descendants of the table root. |
| Kernel synchronous storage ignored requested read ranges and panicked on `copy_atomic`. | A requested range returned the full object and atomic copy panicked. | Candidate `c9a475f3394adc5296c4f16587c1f69c6e87213e` delegates ranged reads to `ObjectStore::get_range` and returns a typed `Unsupported` error for atomic copy. Ten focused storage tests and the 7,432-test native nextest suite passed, with 20 skipped. |

Every correction is DCO-signed. Candidate diffs contain no Daxis dependency URL or immutable Daxis
revision; the only literal match in the delta-rs candidate is the candidate-hygiene command that
rejects those strings.

### Accepted Corrected Revisions

| Repository | Candidate revision | Stack revision | Draft Daxis PR | Corrected owned CI |
| ---------- | ------------------ | -------------- | -------------- | ------------------ |
| [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs) | `f24c67c536e98f85f2ed8a289a6eb1d55916ffb9` | `518663f4fb39ec0be672718432bbd7bb8a5456fc` | [#1](https://github.com/daxis-io/arrow-rs/pull/1) | The unchanged candidate CI remains green; the repinned stack is covered by the complete Axon pinned-graph run. |
| [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `1d6cb49ba43e219ab50d33284c69d56cfa48aba0` | `a04240eafdc5833e34fe21d4b348ee399177def6` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) | [Candidate browser 30181759233](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30181759233), [stack browser 30182216792](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30182216792). |
| [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion) | `693aa0b5d2a3c925db963776a472d6144352116e` | `aa1d3bfb591e0e10594160119d8899d4b856c3f5` | [#1](https://github.com/daxis-io/datafusion/pull/1) | [Browser 30182304149](https://github.com/daxis-io/datafusion/actions/runs/30182304149), [Dev 30182304133](https://github.com/daxis-io/datafusion/actions/runs/30182304133). |
| [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs) | `c9a475f3394adc5296c4f16587c1f69c6e87213e` | `bbccfb394bf4a3eac54e125d71996a66a5a0e13a` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2) | [Browser 30183035000](https://github.com/daxis-io/delta-kernel-rs/actions/runs/30183035000). |
| [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs) | `af7764c098bf0edf92e16de3f865b84b706780f6` | `be60607f67951459e886915e8104273880dcc5cb` | [#1](https://github.com/daxis-io/delta-rs/pull/1) | [Candidate browser 30182186031](https://github.com/daxis-io/delta-rs/actions/runs/30182186031), [stack browser 30183181311](https://github.com/daxis-io/delta-rs/actions/runs/30183181311). |

The Axon correction stack passed all three jobs in
[run 30183442839](https://github.com/daxis-io/axon/actions/runs/30183442839) at exact revision
`4837c331b98e911cb7f9d4d87c3094b942461bb8`:

| Axon job | Result |
| -------- | ------ |
| [Native defaults and Axon boundary](https://github.com/daxis-io/axon/actions/runs/30183442839/job/89743905964) | Success |
| [Pinned graph without a native compiler](https://github.com/daxis-io/axon/actions/runs/30183442839/job/89743905915) | Success |
| [Chrome and Firefox runtime](https://github.com/daxis-io/axon/actions/runs/30183442839/job/89743905909) | Success |

The exact-target verifier reports
`upstream WASM fork stack verified mode=final repositories=5 graph_packages=250`. The target graph
still has one Daxis source universe for Arrow, Parquet, `object_store`, DataFusion, and Kernel, and
the denied-dependency policy passes. The no-native-compiler container has raw `ld.lld` but no
Clang, C/C++ compiler, CMake, or global `RUSTFLAGS`.

The corrected lock hashes are:

| Artifact | SHA-256 |
| -------- | ------- |
| `stack.lock.toml` | `988adae4f505953bb22675cc5e564cf4da077d4bec1ca7059865167d3f8187ea` |
| Browser `Cargo.lock` | `38ce2730461a5498162091600b037dbfdaf136c706dca8acfc89c2e0e38150fe` |
| Fixture-generator `Cargo.lock` | `810944d8ff7bd159ad78fbb8e43f54d0a7b52c60ecd46763118937cf25609854` |
| Fixture manifest | `e02b28c246c5709bfb83eb4de75256ba2e9734ee5f545265c883bc2f047b7aa6` |
| `object_store` candidate and stack browser lock | `79caf990a17d936f92fba937c216939559e3d522211d9281c5051a57c2c077e4` |
| DataFusion stack root lock | `b4756004d72d52ccb5525452b5603baf97b152a3b4296f58cda214f9f91fd463` |
| Delta Kernel candidate browser lock | `5cca1727ca04e97cfefb65cb0111ddcc4d481b4abbd8bc7f9034f99fb752d2aa` |
| Delta Kernel stack browser lock | `7e784bc7daac79d028bfa92690abc80c5f53d8901ab4ceea0edd6ceaab02da99` |
| delta-rs stack browser lock | `9640bfbc8d2bdb7d7375e11ba441d88594326c6007f0a7a9ecf2896c291af8b2` |

### Corrected Browser And Artifact Evidence

Both engines returned snapshot `0`, `alpha=7,beta=10`, row count `2`, 840 exact-sized Arrow IPC
bytes with SHA-256 `993f5a3cf4ee02fa9e2103e60e1cfb9118d54e6a1a577b148913cc10081d8784`,
the existing Arrow stream content type, `browser_wasm`, and no native fallback. Both also reached
metadata/schema replay for the zstd fixture and failed only at the first compressed page with the
precise target-unavailable diagnostic.

Local measurements:

| Browser | Version | Cold end-to-end | Warm median | Warm max | WASM memory high-water |
| ------- | ------- | --------------: | ----------: | -------: | ---------------------: |
| Chrome | `150.0.7871.184` | 152.92 ms | 5.3 ms | 5.5 ms | 14,614,528 bytes |
| Firefox | `144.0.2` | 325.99 ms | 10.0 ms | 11.0 ms | 14,614,528 bytes |

The local bundle is 28,139,553 raw bytes, 6,585,735 gzip bytes, and 4,305,241 Brotli bytes, with
WASM SHA-256 `d72eb6d7950d825e000612e677a801bf417be1461d7c99daf922b370865c4de0`.
Local evidence is
`target/upstream-wasm-fork-poc-evidence/988adae4f505953bb22675cc5e564cf4da077d4bec1ca7059865167d3f8187ea/browser-evidence.json`
with SHA-256 `ef9a9ba92d1f1eaea9abbd9884619cd06b6f21e94d2fae9b2f5c8ad4ee2f9a16`.

CI measurements:

| Browser | Version | Cold end-to-end | Warm median | Warm max | WASM memory high-water |
| ------- | ------- | --------------: | ----------: | -------: | ---------------------: |
| Chrome | `150.0.7871.186` | 352.68 ms | 16.9 ms | 23.5 ms | 14,614,528 bytes |
| Firefox | `144.0.2` | 1,173.32 ms | 35.0 ms | 36.0 ms | 14,614,528 bytes |

The CI bundle is 28,128,847 raw bytes, 6,687,628 gzip bytes, and 4,304,174 Brotli bytes, with WASM
SHA-256 `c6e9b62653b74d0718309ade38834b7dab9dd7b03450fd0c5e0eb6d6e9db37f6`.
CI used Rust/Cargo 1.95.0, wasm-bindgen 0.2.114, and Node 22.23.1. Local evidence used the same
Rust/Cargo and wasm-bindgen with Node 25.4.0.

Downloaded run artifacts are under
`target/upstream-wasm-fork-poc-evidence/988adae4f505953bb22675cc5e564cf4da077d4bec1ca7059865167d3f8187ea/ci-run-30183442839/`.

| Artifact file | SHA-256 |
| ------------- | ------- |
| Browser `browser-evidence.json` | `c986f7f428cd3ea7bfb7074ef95d012e6ebef9b7ed85d7bbada9b7ea8cb7decd` |
| Browser `dependency-tree.txt` | `a33416f9e0ce69d2a93e05f283b0c8c744541faac770abecac34fdabe7f30c0c` |
| Browser `lock-sha256.txt` | `617f65ea1ec147c7e28907e2192890c1ebff7abb0c4474aa8be81e45086431eb` |
| Graph `dependency-tree.txt` | `ef6ad0e06fdf32da69fee94983e510cd402132be33cf7c397db584721ed86bda` |
| Graph `lock-sha256.txt` | `617f65ea1ec147c7e28907e2192890c1ebff7abb0c4474aa8be81e45086431eb` |
| Graph `rustc.txt` | `4fdff2578428e9c5c08ddd7a0d3079c1a106b1cdaa46e73e46f7cd32b0fb9cad` |
| Graph `cargo.txt` | `c10ec31b8c6e6e2693cf65fc1971b41edbc3da1ae7db6f9f3f36c4823f8dcab5` |

The encoded-range proof is no longer inferred: for each browser the artifact records one GET,
`Range: bytes=1-4`, status `206`, four transferred bytes, and the exact
`Content-Encoding "identity, identity"; expected identity` diagnostic.

### Current Tags And Forward-Port State

The new annotated tag resolves as follows:

| Fork | Tag object | Peeled stack commit |
| ---- | ---------- | ------------------- |
| Arrow | `e4bedac9f2ae70eccb337820aaed3af9eb1b06f2` | `518663f4fb39ec0be672718432bbd7bb8a5456fc` |
| `object_store` | `a27fa8d1b38d30d5656740be4cd24126bddbd825` | `a04240eafdc5833e34fe21d4b348ee399177def6` |
| DataFusion | `caad12f028d6d3f41cb560fd00c849305325128a` | `aa1d3bfb591e0e10594160119d8899d4b856c3f5` |
| Delta Kernel | `8276fcb4324018507f972a8ca374f411b58306a2` | `bbccfb394bf4a3eac54e125d71996a66a5a0e13a` |
| delta-rs | `b12f79271511e5eb35d7f13331e948e1ca74dff4` | `be60607f67951459e886915e8104273880dcc5cb` |

Canonical remotes were refreshed after the technical proof:

| Repository | Canonical head |
| ---------- | -------------- |
| Arrow | `cd47d4a421b671fbdb78dac0d3896e9e4f9055c3` |
| `object_store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| DataFusion | `e3e2cb227928ffa498c2845db6ce2aa86ee174b4` |
| Delta Kernel | `2403501198e9b132b714c9945fb3175c0364b1dd` |
| delta-rs | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

The corrected `object_store` forward branch is
`upstream/wasm32-browser-range-protocol` at
`9b5ffc710d5c7fb38068e8a16dbe29446593a84b`; it is DCO-signed, candidate-clean, and contains the
current canonical head. The corrected delta-rs branch is
`upstream/wasm32-browser-engine-incubation` at
`e0fa37143e6888c06623c6a43adf1c801a189ca0`; it is likewise clean and contains the current
canonical head. The Arrow and DataFusion forward branches remain the previously verified clean
slices, but their canonical heads advanced after that verification, so repository owners must
refresh them before opening canonical PRs. The Delta Kernel branch remains deliberately
unpublished because the compatibility-base-to-current redesign still crosses the broad-surgery
stop condition. No canonical PR was opened.

Inherited workflow failures are not substituted for the owned gates above. In particular, Axon's
generic `ci.yml` still fails at workflow creation with zero jobs; DataFusion's full inherited
dependency/Clippy workflows report the known target-only `getrandom` cargo-machete diagnostic and
`object_store::Path::child` deprecations; Kernel's inherited all-feature Clippy reports the known
dead-code and `new_without_default` diagnostics. The POC-owned browser workflows, default-native
checks, exact-target checks, and complete Axon run are green.

The dirty Axon root remains at `3e5aceda0c1eb2c0dea983c0e5849200447a363f` with 61 modified or
untracked files and root `Cargo.lock` SHA-256
`0f8630bdea0dca3fdaa0186a46c31ee0651d067a0b300cf9192c9ec6dd4f5d33`. The audit-closeout dirty-file
inventory hashes to `5eef7bdfe653493f2d903614726adb4795165c834ea6f8449131d38b88214d60`;
all audit implementation and documentation changes stayed in the isolated POC worktree.

## Historical 2026-07-23 Evidence Freeze

The material below preserves the original immutable freeze. Where a revision, tag, measurement,
canonical head, or artifact hash differs, the audit-closure section above is authoritative.

## Control Plane And Isolation

GitHub authentication was refreshed before mutation and again at evidence freeze. Account
`ethan-tyler` has active `daxis-io` admin membership. All five repositories are public forks with
their expected parents:

| Fork                                                                                  | Parent                         | Default branch | Default head at evidence freeze            |
| ------------------------------------------------------------------------------------- | ------------------------------ | -------------- | ------------------------------------------ |
| [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs)                           | `apache/arrow-rs`              | `main`         | `f9bf62845ca459c16938359e9378b34a4d8c51d9` |
| [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `apache/arrow-rs-object-store` | `main`         | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion)                       | `apache/datafusion`            | `main`         | `a0a6836e4cc9f07be52cc8d1380f19ad411d67d8` |
| [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs)             | `delta-io/delta-kernel-rs`     | `main`         | `7bfb06587add017187a1b14b1195ef8f6a95ca9d` |
| [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs)                           | `delta-io/delta-rs`            | `main`         | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

The pre-existing `daxis-io/delta-kernel-rs` default branch was not changed.
No conflicting POC base, candidate, stack, tag, or forward-port branch name was present before its
first publication.

The dirty Axon root remained at
`3e5aceda0c1eb2c0dea983c0e5849200447a363f`. Its 61 modified or untracked files were hashed before
work began and the sorted SHA-256 inventory remained byte-for-byte identical at evidence freeze.
All Axon mutations were made in
`.worktrees/upstream-wasm-fork-poc/axon`, created from the refreshed `origin/main` revision
`62d4c465e10dc329221023eaaf2c67c542c408ce`.
The four authorized documentation sets were copied byte-for-byte and published as the isolated
foundation commit `d83672fee18abe6d125a67b3dabced9b73b33e5b`.

The browser POC is an excluded nested workspace. Its released-crate fixture generator is a second,
also excluded, nested workspace with its own lock. Daxis fork revisions do not appear in Axon's
shipping manifests, production worker, or root `Cargo.lock`. The root lock remains identical to
`origin/main`, with SHA-256
`0f8630bdea0dca3fdaa0186a46c31ee0651d067a0b300cf9192c9ec6dd4f5d33`.

Before POC source changes, `cargo build --workspace --locked` passed in the isolated worktree.
`cargo test --workspace --locked` reproduced four pre-existing
`browser_snapshot_preflight` metrics failures concerning touched-file parity for pruned scans and
nonzero fetched-byte accounting. The focused test reproduced the same four failures. The dirty
root contains overlapping user-owned work in that test; none of it was copied or modified, and
those baseline failures are classified separately from the POC's required gates.

## Compatibility Bases

| Component            | Release ref       | Tag object                                 | Peeled commit                              | Immutable base branch            |
| -------------------- | ----------------- | ------------------------------------------ | ------------------------------------------ | -------------------------------- |
| Arrow / Parquet      | `58.3.0`          | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `poc/base/arrow-58.3.0`          |
| `object_store`       | `v0.13.2`         | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `poc/base/object-store-0.13.2`   |
| DataFusion           | `53.1.0`          | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `poc/base/datafusion-53.1.0`     |
| Buoyant Delta Kernel | `buoyant-v0.22.2` | `61ee6af059ebda666940cb9d7b805d818cdd5af6` | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `poc/base/buoyant-kernel-0.22.2` |
| delta-rs             | `rust-v0.32.4`    | `2c37b2df127086256042968474b06b28f2ec3aae` | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `poc/base/delta-rs-0.32.4`       |

Each base revision is reachable from its fork. The base branches were not rewritten.

## Accepted Revision Ledger

Candidate branches contain only clean, DCO-signed candidate commits. Stack branches add Daxis-only
dependency wiring with immutable 40-character `rev` values. No candidate diff contains a Daxis URL
or revision, no dependency-level `[patch]` table is used, and every correction was an additive
commit followed by a repin.

Every fork uses `poc/wasm32-browser-candidate` for the candidate and
`poc/wasm32-browser-stack` for Daxis-only wiring.

The owner for all five rows is the runtime / engine team. Compatible bases are recorded above;
dispositions and removal conditions are maintained in the upstream patch inventory.

| Repository                                                                            | Candidate revision                         | Stack revision                             | Draft Daxis PR                                                 | Candidate / stack CI                                                                                                                                                                                                                                                             |
| ------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------ | -------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs)                           | `f24c67c536e98f85f2ed8a289a6eb1d55916ffb9` | `39a02b83b6d7ddc41a3fc0dbe541604aebbe1fcc` | [#1](https://github.com/daxis-io/arrow-rs/pull/1)              | [candidate](https://github.com/daxis-io/arrow-rs/actions/runs/30066099978), [dev](https://github.com/daxis-io/arrow-rs/actions/runs/30066099967), plus the complete Axon pinned-graph run                                                                                        |
| [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `1d000072b4490e4736b9aab02731cde33a2c10fa` | `54109e3e6797522ccb2ee5d43cad2a9cd0013074` | [#1](https://github.com/daxis-io/arrow-rs-object-store/pull/1) | [candidate HTTP](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30072242617), [candidate range](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30072240432), [stack](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30072265567) |
| [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion)                       | `693aa0b5d2a3c925db963776a472d6144352116e` | `0d1d77af6a5974af10dfbe1e3790f3dce05ae617` | [#1](https://github.com/daxis-io/datafusion/pull/1)            | [browser stack](https://github.com/daxis-io/datafusion/actions/runs/30073362262), [dev stack](https://github.com/daxis-io/datafusion/actions/runs/30073362290)                                                                                                                   |
| [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs)             | `9d60a72f19e678c30de6b6869cf5c62aa3576de8` | `79c48fe4e22431efb144da4a0db46bb8ff5ce9d8` | [#2](https://github.com/daxis-io/delta-kernel-rs/pull/2)       | [browser stack](https://github.com/daxis-io/delta-kernel-rs/actions/runs/30072563608)                                                                                                                                                                                            |
| [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs)                           | `aa61ca76d699c5dac4e192af6f925b9273a6b0b4` | `bc45f077347988804e8d42249238710f90b8db97` | [#1](https://github.com/daxis-io/delta-rs/pull/1)              | [candidate browser](https://github.com/daxis-io/delta-rs/actions/runs/30071533095), [candidate hygiene](https://github.com/daxis-io/delta-rs/actions/runs/30071533105), [final stack](https://github.com/daxis-io/delta-rs/actions/runs/30073759546)                             |

Every linked POC-owned candidate and stack run in this table concluded successfully at the exact
recorded candidate or stack revision.

The nested-workspace lock hashes are:

| Artifact                       | SHA-256                                                            |
| ------------------------------ | ------------------------------------------------------------------ |
| `stack.lock.toml`              | `9ce6b301657278bf6883727073ddbe1ee206c92088512dcb68da64ea3193a9a3` |
| Browser `Cargo.lock`           | `e6d5921bb8caf57c2495fa98c67cbd80b543af74d0e30c36f5b70bf60e9e6591` |
| Fixture-generator `Cargo.lock` | `810944d8ff7bd159ad78fbb8e43f54d0a7b52c60ecd46763118937cf25609854` |
| Fixture manifest               | `e02b28c246c5709bfb83eb4de75256ba2e9734ee5f545265c883bc2f047b7aa6` |

Per-fork lock hashes at the accepted revisions are:

| Repository / branch                             | Lock SHA-256                                                       |
| ----------------------------------------------- | ------------------------------------------------------------------ |
| `object_store` candidate and stack nested locks | `79caf990a17d936f92fba937c216939559e3d522211d9281c5051a57c2c077e4` |
| DataFusion candidate root lock                  | `1be32482823a8a2a315253dfa967b40c962842cefca8dab44cd8f69716a60448` |
| DataFusion stack root lock                      | `5b1fef57df8fe09b1f0a172d73538e0c073d9c965cf3ab603cb06b7dd2941a10` |
| Delta Kernel candidate nested lock              | `5cca1727ca04e97cfefb65cb0111ddcc4d481b4abbd8bc7f9034f99fb752d2aa` |
| Delta Kernel stack nested lock                  | `92829fbc5a9310f017637dd5db21b087207feeca2e1952ebe8640a4607f74816` |
| delta-rs stack nested lock                      | `0582135c66aa5c61b74ff050c975f4cbef745e5329bfda6410193dcfe81a9887` |

Arrow and the delta-rs candidate do not track a corresponding lock.

## Implemented Interfaces

Arrow/Parquet keeps the public logical zstd features and native defaults while excluding native
zstd backends only for exact `wasm32-unknown-unknown` builds. Parquet page decoding and Arrow IPC
compression/decompression distinguish a disabled codec feature from a codec whose backend is
unavailable on the target. Browser Parquet also excludes object-store writer code that would
reintroduce native-only dependencies.

`object_store` separates host-neutral HTTP support from explicit browser host capabilities. The
Fetch path covers timers, local scheduling, retry jitter/entropy, `If-Range`, identity encoding,
CORS diagnostics, content-length and `Content-Range` validation, validator mismatch, clean-EOF
retry, and a bounded full-object fallback. Ordinary partial-range `200`, encoded range bodies,
invalid lengths, missing lengths, and over-bound fallbacks are rejected.

DataFusion owns its Parquet/compression feature composition and provides a disk-disabled,
single-partition browser profile. Browser-inactive filesystem dependencies, production
`tempfile`, spill, and multi-thread runtime features are absent from the exact target graph. Its
browser tests cover HTTP Parquet projection, filter, order, aggregation, and precise unsupported
zstd/xz behavior.

Delta Kernel's POC candidate moves shared Arrow data/evaluation support under the existing
`arrow-58`/`internal-api` profile, keeps native execution under `default-engine-base`, and removes
read-path entropy, filesystem URL helpers, blocking waits, native clocks, cloud batteries, and
Tokio multi-thread support from the exact browser target. Browser code prefetches asynchronously;
Kernel handlers remain synchronous and replay the prefetched version-0 log through cache-backed
storage and JSON handlers.

delta-rs adds a separate `deltalake-browser` crate. It does not depend on `deltalake-core` or the
native `deltalake` facade. `BrowserDeltaTable::open` asynchronously prefetches the version-0 log,
replays it through the synchronous in-memory Kernel engine, and records active files.
`query_ipc` lets DataFusion read selected Parquet ranges asynchronously and returns an Arrow IPC
stream, row count, fetched bytes, and request count.

The test-only Axon adapter creates the existing browser query envelope from an exact-sized
`Uint8Array`, records `browser_wasm`, request/transfer metrics, snapshot version, and
`application/vnd.apache.arrow.stream`, rejects any native fallback, and enforces the 8 MiB result
budget.

## Deterministic Fixtures

The generator is locked to released Arrow/Parquet `58.3.0` crates and produces independent
checkpoint-free Snappy and zstd Delta tables:

| Fixture file                                  | Bytes | SHA-256                                                            |
| --------------------------------------------- | ----: | ------------------------------------------------------------------ |
| `snappy/_delta_log/00000000000000000000.json` |   618 | `f57e2c0deac64ad9598fcd20de430d2c60f58295efed8edc28687ccf0033e4d8` |
| `snappy/part-00000.snappy.parquet`            |   779 | `55deb795ce237f5eecdc6ba8114f779213cf501775564408593ded80363c6a5d` |
| `zstd/_delta_log/00000000000000000000.json`   |   616 | `2d34cad334f9463e54ccc9fe71b902306decd16c2eb55d2be872377e35570a04` |
| `zstd/part-00000.zstd.parquet`                |   804 | `09d504a26937fcff3d4ddd6c6bf1afacdf83bdf36a7f4301d92f0c48129c2a66` |

Both tables contain rows `(alpha, 2)`, `(beta, 3)`, `(alpha, 5)`, and `(beta, 7)`. The expected SQL
result is `alpha=7,beta=10`, ordered by category.

## Final Graph And Native Gates

`tests/conformance/verify_upstream_wasm_fork_stack.sh --final` reports:

```text
upstream WASM fork stack verified mode=final repositories=5 graph_packages=259
```

The verifier and candidate-hygiene checks collectively reject missing repository entries, malformed
or unreachable revisions, `UNSET` in final mode, missing browser locks, mutable branch
dependencies, lock/revision mismatch, Daxis pins in candidates, and duplicate
Arrow/DataFusion/Kernel source universes. Bootstrap mode alone permits `UNSET` and a missing
browser lock; its regression suite proves the mode distinction.

The target-filtered browser graph contains exactly one source universe for Arrow, Parquet,
`object_store`, DataFusion, and Kernel. It contains none of:

- `zstd-sys`, `liblzma-sys`, `aws-lc-sys`, `openssl-sys`, `native-tls`, `ring`, `hyper`,
  `walkdir`, or `tempfile`;
- filesystem object-store implementations or AWS, Azure, and GCP provider batteries;
- Tokio's `rt-multi-thread` feature.

Native-default checks passed in all five accepted forks without changing their default feature
composition. Component-focused tests covered Arrow IPC/Parquet codecs, object-store HTTP/range
behavior, DataFusion disk-manager and datasource-compression behavior, an Arrow-backed Kernel
engine, and delta-rs's browser-only query boundary. The Axon nested-workspace tests, stack-verifier
regression suite, `query-contract` tests, and `browser-sdk` tests all pass.

The exact `wasm32-unknown-unknown` graph builds in `debian:bookworm-slim` with Rust 1.95.0, raw
`ld.lld`, and no Clang, C/C++ compiler, CMake, or global `RUSTFLAGS`.

## Browser Runtime And Measurements

The two-origin harness serves the page and data from different loopback origins. Chrome and
Firefox return identical snapshot/query results and identical valid Arrow IPC bytes:

- snapshot version: `0`;
- rows: `alpha=7,beta=10`;
- row count: `2`;
- IPC bytes: `840`;
- IPC SHA-256: `993f5a3cf4ee02fa9e2103e60e1cfb9118d54e6a1a577b148913cc10081d8784`;
- content type: `application/vnd.apache.arrow.stream`;
- execution marker: `browser_wasm`;
- native fallback: `false`;
- cold logical object requests / bytes fetched: `3` / `696`;
- cold network GET requests / transferred bytes: `4` / `1,314` (CORS preflights are not counted as
  GETs).

Each result is well below the 8 MiB POC result budget.

Local measurements use one cold run, one discarded warmup, and five measured warm runs:

| Browser | Version          | Cold end-to-end | Warm median | Warm max | WASM memory high-water |
| ------- | ---------------- | --------------: | ----------: | -------: | ---------------------: |
| Chrome  | `150.0.7871.184` |       166.52 ms |      5.1 ms |   5.6 ms |        6,160,384 bytes |
| Firefox | `144.0.2`        |       335.60 ms |     10.0 ms |  10.0 ms |        6,160,384 bytes |

The local release bundle is 28,134,370 raw bytes, 6,582,844 gzip bytes, and 4,308,743 Brotli
bytes. Its WASM SHA-256 is
`0a3765b35ab07907ed95e216d40de1754daa0bda0b175c019d4533fc6e429c8c`.

The accepted CI run independently recorded:

| Browser | Version          | Cold end-to-end | Warm median | Warm max | WASM memory high-water |
| ------- | ---------------- | --------------: | ----------: | -------: | ---------------------: |
| Chrome  | `150.0.7871.186` |       345.84 ms |     17.6 ms |  25.7 ms |        6,160,384 bytes |
| Firefox | `144.0.2`        |     1,177.69 ms |     37.0 ms |  39.0 ms |        6,160,384 bytes |

The CI release bundle is 28,125,932 raw bytes, 6,685,391 gzip bytes, and 4,303,781 Brotli bytes.
Its WASM SHA-256 is
`869f961d420ec80950c3589d2949e4c8004f6f6961fc65b8b17d79fe784044f5`.
Measurements are evidence-completeness results, not product-budget assertions; no numeric product
threshold was specified.

## Protocol Verdict

Chrome and Firefox both passed:

- cross-origin Fetch with explicit CORS exposure;
- `206` and exact `Content-Range` validation;
- ETag capture and `If-Range` retry;
- clean-EOF retry with a bounded second range;
- validator mismatch rejection;
- identity-encoding enforcement;
- a bounded full-object `200` fallback;
- rejection of ordinary partial-range `200`;
- rejection of encoded, invalid-range, invalid-length, missing-length, and over-bound bodies.

The zstd table replays its Delta metadata and schema successfully without page decoding. Its first
compressed-page use fails with:

```text
cannot create Parquet zstd codec: feature "zstd" is enabled, but no backend is available for target wasm32-unknown-unknown
```

That is distinct from the feature-disabled diagnostic.

## Axon CI And Raw Evidence

[Axon run 30074789411](https://github.com/daxis-io/axon/actions/runs/30074789411) passed at exact
revision `b0a91a6f111b7f9d221202086819d9dd63ebd7c3`:

| Job                                                                                                                 | Result  |
| ------------------------------------------------------------------------------------------------------------------- | ------- |
| [Native defaults and Axon boundary](https://github.com/daxis-io/axon/actions/runs/30074789411/job/89423064477)      | Success |
| [Pinned graph without a native compiler](https://github.com/daxis-io/axon/actions/runs/30074789411/job/89423064484) | Success |
| [Chrome and Firefox runtime](https://github.com/daxis-io/axon/actions/runs/30074789411/job/89423064550)             | Success |

Run `30074067981` was not accepted: it exposed missing `ripgrep` and Git safe-directory setup in
the minimal container. Revision `b0a91a6f111b7f9d221202086819d9dd63ebd7c3` corrected both
additively, and only the succeeding exact-SHA run is used as release evidence.

Local browser evidence:

`target/upstream-wasm-fork-poc-evidence/9ce6b301657278bf6883727073ddbe1ee206c92088512dcb68da64ea3193a9a3/browser-evidence.json`

SHA-256:
`be52f1a72a0f8647088b8b7de16a0f92dbe1fc47bc01552933db4219781392ff`.

Downloaded CI evidence:

`target/upstream-wasm-fork-poc-evidence/9ce6b301657278bf6883727073ddbe1ee206c92088512dcb68da64ea3193a9a3/ci-run-30074789411/`

| Artifact file                   | SHA-256                                                            |
| ------------------------------- | ------------------------------------------------------------------ |
| Graph `dependency-tree.txt`     | `088e5819c838066f6739fc4390d3fb84469cefb85585b0e96ff2839e7475f0f3` |
| Graph `lock-sha256.txt`         | `1bee0d388b91ef56869731b36f62c42713ccbd1e8ab9d22bd138526855fec357` |
| Graph `rustc.txt`               | `4fdff2578428e9c5c08ddd7a0d3079c1a106b1cdaa46e73e46f7cd32b0fb9cad` |
| Graph `cargo.txt`               | `c10ec31b8c6e6e2693cf65fc1971b41edbc3da1ae7db6f9f3f36c4823f8dcab5` |
| Browser `dependency-tree.txt`   | `95796c26a29e50da93655c0d31b151eac40d79d33bf3952f6f6fcb8d6a82cbb2` |
| Browser `browser-evidence.json` | `610e1eaf2a4f46386ff1c20a416a1a8eb607f878556cf6f8aeb78308f9d11395` |
| Browser `lock-sha256.txt`       | `1bee0d388b91ef56869731b36f62c42713ccbd1e8ab9d22bd138526855fec357` |

Local toolchain: Rust/Cargo 1.95.0, Node 25.4.0, wasm-bindgen 0.2.114, Chrome
150.0.7871.184, and Firefox 144.0.2. CI used the same Rust/Cargo and wasm-bindgen versions with
Node 22.23.1, Chrome 150.0.7871.186, and Firefox 144.0.2.

## Immutable Stack Tags

The annotated tag `daxis-poc/wasm32-browser-e2e-2026-07-23` resolves as follows:

| Fork           | Tag object                                 | Peeled stack commit                        |
| -------------- | ------------------------------------------ | ------------------------------------------ |
| Arrow          | `e927bb6fb70f67b39470681265441ab8bd58a08b` | `39a02b83b6d7ddc41a3fc0dbe541604aebbe1fcc` |
| `object_store` | `b18da03078866ceb71321e615847db791ed6c0fc` | `54109e3e6797522ccb2ee5d43cad2a9cd0013074` |
| DataFusion     | `7107a67ae032b48889ffe3869a67a4530b452bc0` | `0d1d77af6a5974af10dfbe1e3790f3dce05ae617` |
| Delta Kernel   | `9e7cfb84a9435d135b16fb18e25e0bfcb864eed0` | `79c48fe4e22431efb144da4a0db46bb8ff5ce9d8` |
| delta-rs       | `62ab9e0fb3cf8305aff3629dccec321a67995983` | `bc45f077347988804e8d42249238710f90b8db97` |

## Inherited Workflow Classification

The acceptance gates are exact-target and native-default gates owned by this POC. Inherited
workflows outside that scope are retained as diagnostics:

- [Arrow Parquet run 30066099938](https://github.com/daxis-io/arrow-rs/actions/runs/30066099938)
  fails its `wasm32-wasip1` job because the candidate intentionally excludes native zstd only for
  exact `wasm32-unknown-unknown`; WASI was not a requested target.
- [Arrow Rust run 30066099997](https://github.com/daxis-io/arrow-rs/actions/runs/30066099997)
  fails its inherited MSRV job while installing an unpinned `cargo-msrv` against current AWS
  dependencies requiring Rust 1.94.1. The accepted native-default and exact browser-target gates do
  not depend on that installer.
- [Delta Kernel standard build 30072563549](https://github.com/daxis-io/delta-kernel-rs/actions/runs/30072563549)
  reports a dead-code warning for `validate_latest_commit_file` in one all-feature cfg combination
  and Clippy's `new_without_default` for `SyncEngine::new`. Default-native compatibility and the
  exact browser stack pass; all-feature Clippy was not an acceptance gate.
- Other inherited workflows requiring PR context, unavailable secrets, or unrelated upstream
  checks are not substituted for the POC-owned candidate, stack, and Axon runs above.

## Canonical Forward-Port Preparation

Canonical remotes were refreshed before and after preparing forward branches. Final observed heads
were:

| Repository     | Canonical head                             |
| -------------- | ------------------------------------------ |
| Arrow          | `f7dfcd25aabeb01641fe4b6c35ab964fdf0b24aa` |
| `object_store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| DataFusion     | `f40d99ac8b10e03a41374706e9fa07194a922ca9` |
| Delta Kernel   | `9f411b405ea52d787ee4896a9fbdc19d37f2c0a7` |
| delta-rs       | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

The final already-landed scan still found only partial reusable work, notably object_store's
reqwest-backed Fetch adapter and range-retry correction
`4d042dc6136e8eccdc559979663f6773419e83d3`. None of the complete target-safe stack, protocol,
Kernel, or browser-engine changes had landed canonically.

No canonical PR was opened. The prepared DCO-signed branches contain no Daxis dependency pins:

| Order | Concern                            | Branch / revision                                                                         | Verification and disposition                                                                                                                                                                                                                                                                          |
| ----: | ---------------------------------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|     1 | Parquet zstd target split          | `upstream/wasm32-parquet-zstd` at `d9d6bb626981299425671c5bd8d0b15ab67c1db6`              | Native zstd/default checks, exact WASM build, and policy pass locally. Canonical workflow branch filters did not trigger.                                                                                                                                                                             |
|     2 | Arrow IPC zstd target split        | `upstream/wasm32-arrow-ipc-zstd` at `7eca801309b97ea379e633fad955b001dd534951`            | Native zstd/default checks, exact WASM build, and policy pass locally. Canonical workflow branch filters did not trigger.                                                                                                                                                                             |
|     3 | Host-neutral HTTP manifest         | `upstream/wasm32-http-manifest` at `4a89dd1fc831d19570de7e84b85daefda94017d6`             | [CI success](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30076813929).                                                                                                                                                                                                             |
|     4 | Browser retry                      | `upstream/wasm32-browser-retry` at `5eeda43613bfba9298ed255d724af5e0e0238eec`             | [CI success](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30076822278).                                                                                                                                                                                                             |
|     5 | Browser range protocol             | `upstream/wasm32-browser-range-protocol` at `3dabb144a265999df74eabd19af3d22266c0c9bc`    | [CI success](https://github.com/daxis-io/arrow-rs-object-store/actions/runs/30076835664).                                                                                                                                                                                                             |
|     6 | DataFusion feature ownership       | `upstream/wasm32-feature-ownership` at `2ae13de39344056ae0a91a4e97110b3737450bce`         | Native checks and final inherited CI pass, including headless Chrome and Firefox.                                                                                                                                                                                                                     |
|     7 | DataFusion browser runtime         | `upstream/wasm32-browser-runtime` at `bb6f1012676c0c28935fe2b2768a2f8444bd8799`           | Dev and dependency CI pass; 24/25 inherited Rust jobs pass, and standalone WASM jobs stop on canonical prerequisites. A disposable integration using the Arrow forward branches builds the exact target and passes dependency policy.                                                                 |
|     8 | Delta Kernel core target safety    | Not published; canonical head `9f411b405ea52d787ee4896a9fbdc19d37f2c0a7`                  | Blocked. The first POC commit conflicts in seven files (six source/manifest surfaces plus `Cargo.lock`) while touching 14 files; canonical has 205 unique commits against 74 on the Buoyant side. A mechanical port would require the broad native-core surgery forbidden by the POC stop conditions. |
|     9 | delta-rs browser-engine incubation | `upstream/wasm32-browser-engine-incubation` at `aae56e2eb7db7a96e04d04805df83309bc54fcbc` | [Candidate cleanliness succeeds; standalone pinned graph exposes ordered prerequisites](https://github.com/daxis-io/delta-rs/actions/runs/30076691603). Publish only after the Kernel redesign and preceding Arrow/`object_store`/DataFusion slices.                                                  |

The object-store forward CI initially exposed a current `rust-toolchain` override and a raw-LLD
`-B...gcc-ld` argument. Additive commits pin the intended 1.85.0 CI toolchain explicitly and
translate the compiler-search argument; the three final runs above are green.

DataFusion's first dependency run identified a cargo-machete false positive for target-only
`getrandom`; an additive commit records that intentional selector. The first forward-port
[Rust run 30076904837](https://github.com/daxis-io/datafusion/actions/runs/30076904837)
then exposed an unconditional current-main `tempfile` import in the public `test_util` module.
Commit `f714f81cc8771dd5eef3c72e65d88e13a57c8e4d` keeps those filesystem-only helpers off the exact
browser target while preserving their native API. Its superseding
[Rust run 30079325660](https://github.com/daxis-io/datafusion/actions/runs/30079325660)
reached the browser harness and showed that the inherited xz test still unwrapped the deliberate
target-unavailable error. After that failed job and 15 successful jobs were captured, the remaining
nine jobs were cancelled in favor of the final revision.

Commit `2ae13de39344056ae0a91a4e97110b3737450bce` keeps the successful xz round trip native-only and
asserts that browser xz and zstd failures identify the operation, codec, and target. The browser
runtime branch already contained the identical assertion, so no empty correction commit was added
there.

The final forward-port runs are:

- Feature ownership: [Dev 30080008354](https://github.com/daxis-io/datafusion/actions/runs/30080008354),
  [Dependencies 30080008299](https://github.com/daxis-io/datafusion/actions/runs/30080008299),
  and [Rust 30080008310](https://github.com/daxis-io/datafusion/actions/runs/30080008310)
  all succeed at `2ae13de39344056ae0a91a4e97110b3737450bce`. Rust is 25/25 green,
  including the headless Chrome and Firefox WASM job.
- Browser runtime: [Dev 30079328877](https://github.com/daxis-io/datafusion/actions/runs/30079328877)
  and [Dependencies 30079328812](https://github.com/daxis-io/datafusion/actions/runs/30079328812)
  succeed at `bb6f1012676c0c28935fe2b2768a2f8444bd8799`.
  [Rust 30079328919](https://github.com/daxis-io/datafusion/actions/runs/30079328919)
  has 24 successful jobs and one expected failure: its inherited wasm-pack job stops at canonical
  `getrandom` 0.4 before DataFusion runtime code.
  [Browser WASM 30079328900](https://github.com/daxis-io/datafusion/actions/runs/30079328900)
  records the remaining ordered prerequisites: the compiler-free job reaches canonical Arrow
  `zstd-sys`, while Chrome and Firefox reach canonical `getrandom` without its browser feature.
  No DataFusion-owned failure remains.

Standalone DataFusion browser builds against canonical dependencies are expected to expose
`zstd-sys`, `getrandom` 0.4 without its browser selector, `ring`, and `hyper`. This is the evidence
for the stated upstream order, not authorization to duplicate those fixes in DataFusion. With the
published Arrow forward branches integrated in a disposable worktree, the exact DataFusion WASM
build and denied-dependency policy pass.

The delta-rs forward slice was adapted to current DataFusion 54 and Buoyant Kernel 0.25.1 and does
not carry obsolete `TableProvider::as_any`. Its browser-safe Kernel feature set currently produces
about 40 compile errors because current Kernel Arrow conversion code still references modules
gated by `default-engine-base`; the standalone exact graph also reaches the preceding canonical
Arrow and object-store blockers. This is why slice 9 is ordered after slice 8 rather than widened
to patch those dependencies locally.

## Remaining Risks And Non-Goals

- The technical POC is complete on its immutable release-based stack; canonical Delta Kernel
  upstream readiness is blocked on a deliberate target-safe-core redesign.
- The DataFusion and delta-rs standalone forward branches require the earlier Arrow and
  `object_store` slices; the delta-rs slice additionally requires the Kernel redesign.
- Exact `wasm32-unknown-unknown` is supported. `wasm32-wasip1` remains intentionally out of scope.
- Kernel all-feature Clippy warnings remain outside the accepted native-default profile.
- Bundle, latency, memory, request, and transfer measurements have no product pass/fail budget.
- Pure-Rust zstd, multipart upload, writes, credential discovery, filesystem/spill, native
  threads, generalized multi-partition execution, production dependency replacement, and new
  public codec/random capability APIs remain excluded.

The recommended canonical PR order is the nine-row order above. No canonical PR should be opened
until the repository owners accept that ordering and, for Delta Kernel, the redesign boundary.
