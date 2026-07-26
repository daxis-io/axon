# Canonical PR Draft Packets

These packets are preparation only. No canonical issue or PR has been opened, and no canonical
remote has been mutated. Before submission, re-fetch the canonical repository, confirm that the
recorded base is still current, open or confirm the repository-required issue, and replace Daxis CI
links with the terminal exact-head runs from the evidence ledger.

## Submission Order

1. Arrow Parquet codec backend selection.
2. Arrow IPC codec backend selection.
3. `object_store` target-safe HTTP host capabilities.
4. `object_store` strong-validator truncated-read retry, based on item 3.
5. DataFusion feature ownership only after accepted/released items 1-4.
6. DataFusion browser runtime after item 5.
7. Delta Kernel design discussion before source changes.
8. delta-rs only after an accepted Kernel seam.

## Arrow PR 1

**Title:** `Support feature-unified Parquet codecs on wasm32`

**Base:** `apache/arrow-rs@87cd2e526511ce75726bceb59033dfe4078a095d`

**Prepared ref:** `daxis-io/arrow-rs:upstream/2026-07-26/wasm32-parquet-zstd`

**Draft body:**

Cargo feature unification can enable Parquet Zstd in a browser-WASM consumer even when the browser
target cannot build or link the native Zstd backend. This change preserves the public codec feature
and native default composition while selecting the backend by target. On
`wasm32-unknown-unknown`, metadata remains readable and a codec operation returns an explicit
"feature enabled, target backend unavailable" error.

The patch deliberately does not remove the object-store writer: current upstream supports that
surface. It adds an isolated downstream WASM consumer, a genuinely Zstd-compressed Parquet fixture,
and a graph policy that rejects native build dependencies.

**Tests to report:**

- `dev/check_wasm_dependency_policy.sh parquet`
- exact feature-unified `wasm32-unknown-unknown` check
- executable WASM fixture test: footer/schema/row count succeeds, page decode returns the
  target-backend error
- full `parquet` unit/integration/doctest suite
- all-target/all-feature Clippy and formatting
- Daxis exact-head CI for the prepared ref

**Reviewer notes:**

- Confirm the intended public semantic distinction between a disabled feature and an enabled codec
  without a backend on this target.
- Confirm native and WASI dependency selection remains unchanged.
- Review the isolated consumer and graph policy independently from the backend selection.

## Arrow PR 2

**Title:** `Support feature-unified Arrow IPC codecs on wasm32`

**Base:** `apache/arrow-rs@87cd2e526511ce75726bceb59033dfe4078a095d`

**Prepared ref:** `daxis-io/arrow-rs:upstream/2026-07-26/wasm32-arrow-ipc-zstd`

**Draft body:**

Feature unification can also activate Arrow IPC Zstd in a browser graph. This change keeps the
public IPC compression feature and all native behavior, removes the native Zstd chain from the
exact browser target, and returns the same explicit target-backend-unavailable error at the codec
operation.

The fixture contains an actually compressed 4,096-row record batch. The browser test reads the
schema before decompression fails, and the writer test proves failed compression rolls back the
shared output buffer before committing partial batch bytes.

**Tests to report:**

- exact `arrow-ipc` LZ4/Zstd WASM graph and policy
- two executable WASM tests: schema-before-decode failure and writer rollback
- native all-feature unit/integration/doctest suite
- all-target/all-feature Clippy and formatting
- Daxis exact-head CI for the prepared ref

**Reviewer notes:**

- This PR is independent of PR 1 at the source level and may be reviewed separately.
- Confirm rollback is desirable for all targets and does not change successful native output.
- Keep runtime compression policy from arrow-rs issue #8917 out of this patch.

## object_store PR 1

**Title:** `Expose target-safe HTTP host capabilities`

**Base:** `apache/arrow-rs-object-store@84d24eb8efcec9448566de09e94d2d4b74b21ebe`

**Prepared ref:** `daxis-io/arrow-rs-object-store:upstream/2026-07-26/wasm32-http-manifest`

**Draft body:**

This change separates host-neutral HTTP/object/retry policy from transport and JavaScript-host
capabilities. `http-base` owns protocol types, `reqwest` selects the transport, and explicit `web`
selects Fetch, browser timers, the browser clock, scheduling, and jitter. Browser behavior is not
inferred solely from `wasm32-unknown-unknown`.

Native provider feature composition and defaults are preserved. An isolated consumer proves the
host-neutral, browser, and batteries-included profiles. A deterministic CORS producer executes a
transient `503`, fixed nonzero browser delay, and second ranged Fetch in headless Chrome and
Firefox.

**Tests to report:**

- `tests/wasm-consumer/check-wasm-graph.sh`
- Rust 1.85 compiler-free consumer check with no C/C++/Clang/CMake
- Chrome and Firefox transient-response retry test
- native package tests, docs, formatting, RAT/audit, and repository CI
- Daxis exact-head CI for the prepared ref

**Reviewer notes:**

- Review `http-base` / `reqwest` / `web` ownership as the main public API decision.
- Confirm `http-base` without a retry runtime fails explicitly when delayed retry is required.
- Compare with issues #1, #26, #624, #759 and stale PR #625; do not infer that WASM always means a
  browser.

## object_store PR 2

**Title:** `Retry truncated reads only with a strong validator`

**Base:** the accepted head of `Expose target-safe HTTP host capabilities`

**Prepared refs:** `daxis-io/arrow-rs-object-store:upstream/2026-07-26/wasm32-browser-retry` and
`upstream/2026-07-26/wasm32-browser-range-protocol` resolve to the same tested commit.

**Draft body:**

Current response-body retry covers transport errors and resumed-range validation, but a clean EOF
before the declared length is not retried, and representation splicing must not be admitted by a
weak or malformed validator. This change detects clean EOF/overrun, admits resume only for a valid
strong ETag, sends `If-Range`, and requires the retry response to preserve the validator and satisfy
strict `206`, `Content-Range`, length, and identity-encoding checks.

The patch intentionally excludes arbitrary subrange `200` fallback. That buffering policy remains
separate under issue #806. Deterministic Chrome and Firefox tests prove `Range: bytes=5-9`,
`If-Range: "v1"`, successful `206` resume to `helloworld`, and rejection of a retry `200`.

**Tests to report:**

- strong entity-tag grammar and non-strong no-retry cases
- clean-EOF producer/consumer resume
- native `HttpStore` `Range`/`If-Range`, validator mutation, and range validation
- Chrome and Firefox valid-resume and retry-`200` rejection tests
- full graph/native/format/Clippy/RAT/audit gates inherited from PR 1
- Daxis exact-head CI for the prepared ref

**Reviewer notes:**

- Treat strong-validator admission as a correctness boundary, not a browser-only optimization.
- Confirm a retry `200` can never enter fallback or splice representations.
- Keep exact-full-range `200` and optional bounded fallback policy out of this PR.

## Held Downstream Packets

- **DataFusion feature ownership:** hold until the Arrow and `object_store` contracts are accepted
  or released. The prepared branch still stops on the current runtime-owned `tempfile` edge.
- **DataFusion browser runtime:** hold until compatible Arrow releases exist. The prepared runtime
  removes `tempfile` and global getrandom flags, then stops on released Arrow 59.1 `zstd-sys`.
- **Delta Kernel:** open a design discussion on issue #252 for a separately selectable Arrow
  adapter and explicit runtime capabilities. Do not submit source while broad native-core surgery
  is required.
- **delta-rs:** do not prepare a source PR until Kernel exposes an accepted public seam and all leaf
  releases are compatible.
