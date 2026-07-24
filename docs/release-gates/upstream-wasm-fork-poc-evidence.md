# Daxis Upstream-WASM Fork POC Evidence

- Status: In progress
- Evidence date: 2026-07-23
- Umbrella issue: [daxis-io/axon#2](https://github.com/daxis-io/axon/issues/2)
- Axon branch: `poc/upstream-wasm-fork-stack`
- Axon compatibility base: `62d4c465e10dc329221023eaaf2c67c542c408ce`
- Raw evidence root: `target/upstream-wasm-fork-poc-evidence/<stack-lock-hash>/`

This record separates graph viability, browser runtime viability, protocol interoperability,
downstream viability, native compatibility, product measurements, and upstream readiness. A passing
result in one class is not evidence for another class.

## Control-Plane Snapshot

GitHub authentication was refreshed immediately before mutation:

- account: `ethan-tyler`;
- organization: `daxis-io`;
- membership: active;
- role: admin;
- token scopes reported by `gh auth status`: `admin:public_key`, `gist`, `read:org`, and `repo`.

All five repositories are public GitHub forks with the expected parent:

| Fork | Parent | Default branch head before POC branches |
| --- | --- | --- |
| [`daxis-io/arrow-rs`](https://github.com/daxis-io/arrow-rs) | `apache/arrow-rs` | `f9bf62845ca459c16938359e9378b34a4d8c51d9` |
| [`daxis-io/arrow-rs-object-store`](https://github.com/daxis-io/arrow-rs-object-store) | `apache/arrow-rs-object-store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| [`daxis-io/datafusion`](https://github.com/daxis-io/datafusion) | `apache/datafusion` | `a0a6836e4cc9f07be52cc8d1380f19ad411d67d8` |
| [`daxis-io/delta-kernel-rs`](https://github.com/daxis-io/delta-kernel-rs) | `delta-io/delta-kernel-rs` | `7bfb06587add017187a1b14b1195ef8f6a95ca9d` |
| [`daxis-io/delta-rs`](https://github.com/daxis-io/delta-rs) | `delta-io/delta-rs` | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

The existing Delta Kernel default branch is unrelated history and remains unchanged.

## Compatibility Bases

| Component | Release ref | Tag object | Peeled commit | Immutable base branch |
| --- | --- | --- | --- | --- |
| Arrow / Parquet | `58.3.0` | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `poc/base/arrow-58.3.0` |
| `object_store` | `v0.13.2` | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `poc/base/object-store-0.13.2` |
| DataFusion | `53.1.0` | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `poc/base/datafusion-53.1.0` |
| Buoyant Delta Kernel | `buoyant-v0.22.2` | `61ee6af059ebda666940cb9d7b805d818cdd5af6` | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `poc/base/buoyant-kernel-0.22.2` |
| delta-rs | `rust-v0.32.4` | `2c37b2df127086256042968474b06b28f2ec3aae` | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `poc/base/delta-rs-0.32.4` |

Each base branch was verified from its fork after push. Base branches are immutable for this POC.

## Canonical Heads At Start

| Component | Canonical head |
| --- | --- |
| Arrow / Parquet | `f9bf62845ca459c16938359e9378b34a4d8c51d9` |
| `object_store` | `84d24eb8efcec9448566de09e94d2d4b74b21ebe` |
| DataFusion | `a0a6836e4cc9f07be52cc8d1380f19ad411d67d8` |
| Delta Kernel | `8547538d22d23a532cd07f31f3b9ec1e379bd750` |
| Buoyant compatibility source | `c3a868f16cb9b01d1d13f6f66acea96a584c5813` |
| delta-rs | `3f562682c5a9dd55693b7f7bbd2a2f749fdf38e5` |

Arrow and DataFusion advanced after the planning snapshot. The intervening Arrow coalesce change
and DataFusion LEAD/LAG evaluation and protobuf-serde changes do not touch the planned WASM
surfaces. The other canonical heads remained at the planning snapshot.

## Axon Isolation And Baseline

The Axon root remained at `3e5aceda0c1eb2c0dea983c0e5849200447a363f`. A SHA-256 snapshot of
all 61 dirty root files was identical before and after worktree creation, documentation copy,
commit, and push.

The isolated Axon worktree was created from
`62d4c465e10dc329221023eaaf2c67c542c408ce`. The four authorized documentation sets were copied
byte-for-byte and published in commit `d83672fee18abe6d125a67b3dabced9b73b33e5b`. Its root
`Cargo.lock` SHA-256 is `0f8630bdea0dca3fdaa0186a46c31ee0651d067a0b300cf9192c9ec6dd4f5d33`.

Baseline command results before POC source changes:

| Command | Result |
| --- | --- |
| `cargo build --workspace --locked` | Passed. |
| `cargo test --workspace --locked` | Failed in four pre-existing `browser_snapshot_preflight` metrics assertions. |
| `cargo test -p delta-control-plane --test browser_snapshot_preflight --locked -- --test-threads=1` | Reproduced the same four failures: touched-file parity for pruned scans and nonzero fetched-byte accounting. |

The dirty root contains overlapping user-owned work in that test. It was not copied or modified.
The POC uses an excluded nested workspace and records the baseline failures separately from its
required gates.

## Revision Ledger

`UNSET` is permitted only while bootstrapping. Normal and final verification reject it.

| Repository | Owner | Base | Candidate revision | Stack revision | Disposition | Removal condition | Cargo lock SHA-256 | Candidate CI | Stack CI |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `daxis-io/arrow-rs` | Runtime / engine team | `913bab26ba9bed8fc2bc1acda300cc52345b0da1` | `UNSET` | `UNSET` | `proposed` | Accepted upstream release adopted and locked Axon rehearsal passes. | `UNSET` | `UNSET` | `UNSET` |
| `daxis-io/arrow-rs-object-store` | Runtime / engine team | `7a65b75b0d26fd8a282999462cb7030fb85fdcc3` | `UNSET` | `UNSET` | `proposed` | Accepted upstream release adopted and locked Axon rehearsal passes. | `UNSET` | `UNSET` | `UNSET` |
| `daxis-io/datafusion` | Runtime / engine team | `eae7bf4fa1c037c0a065d1f36d0669f5bb97a9cf` | `UNSET` | `UNSET` | `proposed` | Accepted upstream release adopted and locked Axon rehearsal passes. | `UNSET` | `UNSET` | `UNSET` |
| `daxis-io/delta-kernel-rs` | Runtime / engine team | `f4602a43fe886f45cc3523360bc2488b8f3a2e58` | `UNSET` | `UNSET` | `proposed` | Compatible upstream release adopted and downstream browser rehearsal passes. | `UNSET` | `UNSET` | `UNSET` |
| `daxis-io/delta-rs` | Runtime / engine team | `df72cc6d3fba014a77243ce80514a6122b46a89b` | `UNSET` | `UNSET` | `proposed` | Browser engine lands in its canonical home, a compatible release is adopted, and Axon's proof passes. | `UNSET` | `UNSET` | `UNSET` |

Corrections are additive commits followed by downstream repins. Published candidate or stack
revisions are never force-pushed or rewritten.

## Evidence Verdicts

| Verdict | Status | Evidence |
| --- | --- | --- |
| Graph viability | Pending | Locked WASM build and dependency-policy report. |
| Browser runtime viability | Pending | Chrome and Firefox happy-path query. |
| Protocol interoperability | Pending | Cross-origin Range, validator, retry, and error suites. |
| Downstream viability | Pending | delta-rs browser crate through the Axon-hosted harness. |
| Native compatibility | Pending | Native default matrix for all five forks. |
| Product viability | Pending | Bundle size, latency, memory, request count, and transferred bytes. |
| Upstream readiness | Pending | Nine clean forward-port concern branches against refreshed canonical heads. |

## CI Runs And Artifacts

No candidate or stack CI run is accepted until its exact head SHA, URL, conclusion, logs, artifacts,
and artifact SHA-256 values are recorded here.
