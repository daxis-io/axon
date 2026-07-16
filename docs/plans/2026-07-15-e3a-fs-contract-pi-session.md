# E3A Filesystem Contract PI Session

## Baseline and preflight

- Worktree: `.worktrees/e3a-fs-contract-pi`.
- Branch: `chore/e3a-fs-contract-pi`.
- Initial refreshed remote baseline: `origin/main` at
  `837195684acaf303a20a69da501b9f7bc5391289`.
- Final integration baseline: `origin/main` advanced during implementation to
  `b11a44bbce3ac2b0299507dc83328baec1f57bba` and then advanced again before the
  fresh-session closeout to `30d7e8bdc729902a7a5f50cd3d30feb7efdd2348` with
  the non-overlapping `feat: project cache and readahead metrics` commit. The
  reviewed filesystem commits were autosquashed and rebased onto that final
  baseline before the complete verification bundle ran.
- The dirty root checkout is not part of implementation or verification. All
  edits, generation, tests, and commits happen in the isolated worktree.
- Preflight fetched `origin/main` on 2026-07-15 and confirmed all five
  execution-contract commits are consecutive ancestors of the baseline:
  - `9f511ea docs: plan e3a exec contract pi`
  - `f192047 chore(web): add execution contract messages`
  - `a2c4ad2 test(web): cover execution contract parity`
  - `74e0b3d chore(contract): generate execution rust contracts`
  - `8371956 docs(contract): document execution contract handoff`
- Preflight confirmed the seven execution-contract proof artifacts:
  - `apps/axon-web/proto/axon/exec/v1/exec.proto`
  - `apps/axon-web/src/generated/contracts/protobuf/axon/exec/v1/exec_pb.ts`
  - `apps/axon-web/src/generated/contracts/exec-codegen.test.ts`
  - `crates/contract-proto/src/generated/axon.exec.v1.mod.rs`
  - `crates/contract-proto/tests/exec_smoke.rs`
  - `tests/perf/report_exec_contract_worker_artifact.sh`
  - `docs/plans/2026-07-11-e3a-exec-contract-pi-session.md`
- Preflight found no `axon/fs/v1` proto or generated package and no overlapping
  filesystem-contract implementation on the initial baseline. Final fetches
  against `b11a44b` and `30d7e8b` confirmed the same absence.
- Fresh setup installed the absent `apps/axon-web/node_modules` without changing
  tracked files.
- Baseline checks passed before source edits:
  - `cd apps/axon-web && npm test -- src/generated/contracts` (3 files, 22 tests)
  - `cargo test -p axon-contract-proto --locked` (3 smoke tests)

## Scope and ownership boundary

This PI adds a normalized, messages-only `axon.fs.v1` contract for identifying
filesystem roots, listing directories, statting entries, and resolving file
reads. It generates deterministic protobuf-es TypeScript and Buffa Rust output
and proves the surface on host and `wasm32-unknown-unknown`.

The provider boundary remains explicit:

- `CatalogProvider` discovers catalog objects.
- `DataAccessResolver` resolves table reads.
- `ExecutionProvider` runs and previews queries.
- `FileSystemProvider` lists entries, stats entries, and resolves file reads.

`VolumeNode.ref` identifies a catalog volume. A later E8 adapter converts that
identity into an `FsRootRef`; catalog messages never list files or carry file
read resolutions.

## Public contract decisions

`apps/axon-web/proto/axon/fs/v1/fs.proto` defines no service and contains:

- `FsBackendKind`: unspecified, Unity Catalog volume, object-store prefix,
  local folder, and document.
- `FsRootRef`: `provider_id`, opaque `root_id`, and `backend_kind`. The IDs
  select a registered provider/root. They cannot contain handles, credentialed
  or signed URLs, credentials, secrets, or tokens.
- `FsEntryKind`: unspecified, file, and directory.
- `FsEntry`: `name`, provider-relative `path`, `kind`, optional `size_bytes`,
  optional `google.protobuf.Timestamp modified_at`, and optional
  `content_type`.
- `ListDirectoryRequest`: root, normalized directory path, and
  `axon.common.v1.PageRequest`.
- `ListDirectoryResponse`: direct-child entries and
  `axon.common.v1.PageInfo`.
- `StatRequest` and `StatResponse`: root plus normalized path, returning one
  normalized entry.
- `ResolveReadRequest`: root, path, optional `start`, and optional `end`.
- `ObjectReadResolution`: oneof
  `axon.dataaccess.v1.ObjectGrantSignedUrl signed_url`,
  `axon.dataaccess.v1.ObjectGrantRangeRequest proxy_range`, or
  `axon.common.v1.ProviderError denied`.

The contract reuses common pagination/error messages and data-access object
grant messages. It does not duplicate those shapes or introduce a transport.

## Mapping inventory

- Unity Catalog volumes: `apps/axon-web/proto/axon/catalog/v1/catalog.proto`
  defines `VolumeNode.ref`. E8 will map that catalog identity to a registered
  Unity Catalog volume `FsRootRef`; storage locations do not become roots by
  copying URLs into this contract.
- Object storage: `apps/axon-web/src/services/object-storage.ts` and
  `apps/axon-web/tests/object-storage.spec.ts` provide the existing S3/GCS
  `<Contents>` fixture style. Object `<Contents>` normalize to file entries;
  `<CommonPrefixes>` normalize to directory entries. The inline common-prefix
  projection in the filesystem parity test is representative normalization,
  because the existing fixture set has no actual `<CommonPrefixes>` example.
  `Key` becomes provider-relative `path`, the final segment becomes `name`,
  `Size` becomes optional `size_bytes`, and `LastModified` becomes optional
  `modified_at`.
- Local files: `apps/axon-web/src/services/local-delta.ts` and
  `apps/axon-web/src/persistence/handle-store.ts` already retain `File.name`,
  `File.size`, `File.lastModified`, and `File.type`-derived metadata. E8 will
  normalize those fields into `FsEntry`; browser file/directory handles never
  enter protobuf messages.
- File-read resolution: `apps/axon-web/proto/axon/dataaccess/v1/dataaccess.proto`
  supplies the signed-URL and proxy-range messages. Denials reuse the common
  provider error taxonomy.
- Document roots have no legacy wire mapping in this PI. They are a normalized
  backend identity for a later provider implementation.

Existing runtime sources and fixtures are read-only inputs for contract tests.
This PI adds no mapper or provider implementation.

## Normalization rules

- The empty list path denotes the root. Every other path uses `/`, has no
  leading or trailing slash, backslash, empty segment, `.` or `..` segment,
  NUL, or URL encoding. An entry name equals the final path segment.
- Listings return immediate children only. Empty directories return an empty
  `entries` array with pagination metadata.
- `size_bytes` presence distinguishes missing metadata from a known zero-byte
  file. Empty MIME strings normalize to absent `content_type`.
- Local `lastModified` epoch milliseconds and object `LastModified` timestamps
  normalize to protobuf timestamps and RFC 3339 UTC protobuf JSON strings.
- An unspecified read range has neither endpoint. A range has both endpoints
  and denotes the half-open interval `[start, end)`. One-sided ranges normalize
  to `ProviderErrorCode.INVALID` rather than a resolution arm.
- Optional endpoints preserve explicit zero, including `start = 0` in a proxy
  range resolution.
- Pagination cursors are opaque and scoped to root/path. An empty cursor starts
  pagination; `page_size = 0` requests the provider default.
- Root references and directory metadata may be cached. Read resolutions,
  grants, and signed URLs are short-lived and must not be persisted or embedded
  in list/stat messages.
- Protobuf JSON uses generated lowerCamelCase field names, enum symbols,
  decimal strings for `uint64`, and RFC 3339 UTC strings for timestamps.

## Planned file boundaries

- Plan and final handoff:
  - `docs/plans/2026-07-15-e3a-fs-contract-pi-session.md`
- Protobuf source:
  - `apps/axon-web/proto/axon/fs/v1/fs.proto`
- Generated TypeScript:
  - `apps/axon-web/src/generated/contracts/protobuf/axon/fs/v1/fs_pb.ts`
- TypeScript parity tests:
  - `apps/axon-web/src/generated/contracts/fs-codegen.test.ts`
- Rust codegen drift gate:
  - `apps/axon-web/buf.gen.contracts.rust.yaml`
  - `apps/axon-web/scripts/contract-codegen.mjs`
  - `apps/axon-web/scripts/generate-rust-contracts.mjs`
  - `apps/axon-web/scripts/check-rust-contract-codegen.mjs`
  - `apps/axon-web/scripts/codegen-check.mjs`
  - `apps/axon-web/src/settings/contract-codegen.test.ts`
- Rust contract proof:
  - `crates/contract-proto/src/generated/axon.fs.v1.fs.rs`
  - generated view/oneof companions when emitted
  - `crates/contract-proto/src/generated/axon.fs.v1.mod.rs`
  - `crates/contract-proto/src/lib.rs`
  - `crates/contract-proto/tests/fs_smoke.rs`

No prior generated package should change in the TypeScript or Rust contract
slices.

## Test strategy

The TypeScript parity test covers:

- file and directory entries, empty directories, stat, and pagination;
- all backend kinds;
- missing size versus present zero-byte size;
- absent empty content type;
- local `lastModified` and object `LastModified` timestamp normalization;
- `<Contents>` file projection, representative inline `<CommonPrefixes>`
  directory projection, and rejection of non-immediate descendants;
- signed-URL, proxy-range, and denied resolution arms;
- signed expiry as a decimal JSON string, proxy `start = 0` through protobuf
  JSON round trips, and a one-sided request paired with a denied
  code/message/correlation ID;
- no secret-like fields in root, entry, list, stat, or read-request metadata,
  with the intentional signed/proxy/denied resolution arms pinned separately;
  and
- `file_axon_fs_v1_fs.services` is empty.

The Rust smoke test proves message names and both export paths, file/directory
entries, pagination, `Some(0)` size and range presence, Buffa Timestamp use, and
signed/proxy/denied binary round trips.

## Commit slices

1. `docs: plan e3a filesystem contract pi`
   - Commit this document before source edits.
2. `chore(web): add filesystem contract messages`
   - Add `fs.proto` and only its deterministic generated TypeScript output.
   - Prove no common, catalog, dataaccess, or exec TypeScript churn.
3. `test(web): cover filesystem contract parity`
   - Add the focused TypeScript normalization and descriptor coverage.
4. `chore(contract): generate filesystem rust contracts`
   - Regenerate common, dataaccess, exec, and fs through pinned
     `buf.build/anthropics/buffa:v0.8.0` with clean output.
   - Add fs generated output, nested/compatibility exports, and Rust smoke tests
     without changing prior generated packages.
   - Add a canonical Buffa template and temporary regenerate/diff gate to
     `npm run codegen:check`.
5. `docs(contract): document filesystem contract handoff`
   - Record commit IDs, verification evidence, normalization boundaries,
     generator status, blockers, final package state, and M4 readiness.

## Failure behavior

- Any preflight overlap, generated drift outside the planned package, lint,
  compile, test, format, security, or artifact failure stops the affected slice
  until its cause is understood.
- If Buffa generation or host/wasm compilation fails, remove unproven fs
  generated output, omit Slice 4, record the exact error and the documented
  prost fallback, and mark the PI incomplete. Do not claim E3A completion or M4
  readiness.
- No required check is skipped without its exact reason in the final handoff.
- Do not push, open a pull request, merge into the root checkout, or modify the
  root checkout.

## Verification bundle

Run from the isolated worktree after Slice 4:

```bash
cd apps/axon-web && npm run build:wasm
cd apps/axon-web && npm run codegen:config:check
cd apps/axon-web && npm run codegen:contracts:check
cd apps/axon-web && npm run codegen:contracts:rust:check
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts src/settings/contract-codegen.test.ts
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && buf lint
cd apps/axon-web && buf format --diff --exit-code
cargo test -p query-contract --locked object_grant
cargo test -p axon-contract-proto --locked
cargo check -p axon-contract-proto --locked
cargo check -p axon-contract-proto --target wasm32-unknown-unknown --locked
cargo fmt --check -p axon-contract-proto
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
bash tests/perf/report_browser_worker_artifact.sh
bash tests/security/verify_browser_dependency_guardrails.sh
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

After the final documentation update, rerun `format:check`, diff/status, and log
checks. Fetch `origin/main` again before reporting. Stop on upstream fs overlap;
otherwise rebase non-overlapping upstream movement and rerun the complete bundle.

## Exit gates

- Exactly five correctly ordered local commits exist on top of the final
  `origin/main`, and the worktree is clean.
- Generated TypeScript and Rust output is deterministic with no prior-package
  churn.
- All declarations are documented, every backend/resolution arm is covered,
  explicit zero presence survives JSON and binary round trips, and timestamps
  use protobuf Timestamp semantics.
- The filesystem descriptor contains no service and no secret-bearing field.
- Buffa `0.8.1` and `buffa-types` `0.8.1` remain pinned and pass locked host and
  wasm checks.
- Runtime adapters, E8 UI/provider adoption, transports, persistence, and other
  deferred work remain outside this PI.
- Recommend E3A M4 only when all five slices and every exit gate pass.

## Completed slice commits

- `9baf5fb docs: plan e3a filesystem contract pi`
- `df46acd chore(web): add filesystem contract messages`
- `19f3084 test(web): cover filesystem contract parity`
- `ceaf8e0 chore(contract): generate filesystem rust contracts`
- Slice 5 is this filesystem-contract handoff documentation commit.

## Final handoff

### Implemented package state

- `axon.fs.v1` contains two enums and eight messages, with no service:
  `FsBackendKind`, `FsRootRef`, `FsEntryKind`, `FsEntry`, directory list
  request/response, stat request/response, read-resolution request, and the
  signed/proxy/denied resolution oneof.
- Protobuf-es generated the checked-in TypeScript package through pinned
  `buf.build/bufbuild/es:v2.12.1` revision 1.
- `buf.build/anthropics/buffa:v0.8.0` revision 1 generated the five checked-in
  Rust files: the message module, owned oneof, view, view oneof, and package
  module.
- `buf.gen.contracts.rust.yaml` pins that plugin version and revision. The
  aggregate codegen gate regenerates common, dataaccess, exec, and fs into a
  temporary directory before diffing all 18 checked-in Rust files.
- `crates/contract-proto/src/lib.rs` exposes both `axon::fs::v1` and
  compatibility module `axon_fs_v1`.
- `buffa = "=0.8.1"` and `buffa-types = "=0.8.1"` remain unchanged. `Cargo.lock`
  did not churn, and common, dataaccess, and exec generated Rust remain
  byte-identical to final `origin/main`.
- Contract TypeScript generation changed only
  `axon/fs/v1/fs_pb.ts`; common, catalog, dataaccess, and exec TypeScript did not
  churn.

### Final normalization boundaries

- Paths are provider-relative contract values. The proto declarations record
  the empty-root-list exception and prohibit leading/trailing slashes,
  backslashes, empty segments, `.`/`..`, NUL, and URL encoding. Runtime path
  validation belongs to the deferred E8 adapters.
- The object-storage test uses the existing `<Contents>` fixture shape for a
  file and an inline `<CommonPrefixes>` projection for a directory. The latter
  is representative normalization because no legacy common-prefix fixture
  exists. The representative mapper rejects paths whose parent differs from the
  requested directory.
- Local file normalization uses only `File.name`, `size`, `lastModified`, and
  `type`. Empty MIME types become absent; handles remain outside messages.
- Missing size remains absent while a known zero-byte file stays present as
  `0n`, protobuf JSON `"0"`, and Rust `Some(0)`.
- Local epoch milliseconds and object `LastModified` normalize through
  `google.protobuf.Timestamp` and produce RFC 3339 UTC protobuf JSON.
- Protobuf JSON uses standard lowerCamelCase fs names, enum symbols, and decimal
  strings for `uint64`; no fs `json_name` compatibility overrides were added.
- Read ranges are absent/absent or the half-open interval `[start, end)`. The
  tests preserve explicit `start = 0` in requests and proxy resolutions. A
  structured INVALID denial represents one-sided-range rejection; adapter
  enforcement remains deferred.
- List/stat metadata cannot carry grants, URLs, credentials, tokens, or browser
  handles. Read resolutions are intentionally short-lived and must not be
  cached or persisted with directory metadata.

### Review remediation

- The first batch review found that path-bearing proto comments did not yet
  state every traversal-sensitive rule. The comments were expanded, both
  language bindings were regenerated, and the fix was autosquashed into Slice 2.
- The same review found that the metadata secret-field guard omitted
  `ResolveReadRequestSchema`. The schema is now included and the fix was
  autosquashed into Slice 3.
- Focused TypeScript coverage first failed on the deliberately unimplemented
  representative object-list mapper, then passed after the mapper was added.
- A final static review repeated those two findings from stale intermediate
  lines. Direct inspection of final `HEAD` confirmed both remedies are present;
  the review reported no other material descriptor, API, oneof, presence,
  service, or export-integrity issue.
- A fresh-session audit found that Rust output was manually reproducible but not
  covered by the aggregate drift gate. A pinned Buffa template, generator,
  temporary regenerate/diff checker, and stale-output cleanup test now close
  that gap without changing generated output.
- The audit also noted that the parity examples represented, but did not
  enforce, immediate-child and one-sided-range semantics. The representative
  list mapper now rejects non-child paths, and the denied-arm example explicitly
  pairs a one-sided request with `ProviderErrorCode.INVALID`; E8 runtime
  enforcement remains deferred.
- A second fresh-session audit found that `origin/main` had advanced by one
  non-overlapping commit that changes `exec.proto`, an explicit Rust-generator
  input. All five slices were rebased onto `30d7e8b`; the upstream exec bindings
  remain unchanged by this branch, and the complete bundle was rerun on the
  combined tree.
- That audit also found that the Buffa template pinned the plugin version but
  omitted the repository-standard plugin revision. A focused assertion first
  failed on the omission, then passed after `revision: 1` was added; real pinned
  Buffa generation also passed and matched all 18 generated Rust files.
- The audit found contradictory handoff wording that described the exact
  aggregate codegen rerun as rate-limited while claiming no blocked check. The
  historical rate limit and its later successful rerun are now recorded
  separately below.

### Final verification evidence

The original verification bundle passed from the isolated worktree after
rebasing onto `b11a44b`. Fresh-session remediation then rebased all five slices
onto `30d7e8b` and reran the complete bundle and Rust drift proof:

- `npm run build:wasm` completed successfully.
- `npm run codegen:config:check`, `npm run codegen:contracts:check`, and the
  original aggregate `npm run codegen:check` passed before remediation.
- `npm run codegen:contracts:rust:check` passed against the real pinned Buffa
  plugin after remediation and matched all 18 generated Rust files. Its focused
  integration test first failed while the checker was absent, then passed while
  proving stale-output failure and temporary-directory cleanup.
- A historical pre-closeout aggregate rerun passed its config and TypeScript
  drift stages, then the third remote generation was rejected by
  [Buf's unauthenticated 10-request hourly quota](https://buf.build/docs/bsr/rate-limits/)
  with `resource_exhausted`. The CLI is not logged in. The Rust stage had already
  passed independently with the same checker, identifying an external quota
  event rather than generated drift. After the documented leaky bucket refilled,
  the final component and aggregate drift checks passed on the rebased tree.
- The updated focused Vitest command passed 5 files and 34 tests. Ten are the
  filesystem tests, including immediate-child rejection and one-sided-range
  denial representation; two exercise the Rust drift gate.
- `npm exec -- tsc --noEmit`, ESLint (including a forced check of the normally
  ignored filesystem parity test), Prettier (including that same file), Buf
  lint, and Buf format all passed.
- `cargo test -p query-contract --locked object_grant` passed five focused
  object-grant tests across the contract and handoff suites.
- `cargo test -p axon-contract-proto --locked` passed all six smoke tests: three
  existing exec tests and three new fs tests.
- Locked host and `wasm32-unknown-unknown` checks for `axon-contract-proto` and
  `cargo fmt --check -p axon-contract-proto` passed.
- The locked release `browser-engine-worker` wasm build passed. The artifact is
  622,354 bytes against the 750,000-byte budget.
- Browser dependency and bundle guardrails passed.
- `git diff --check origin/main...HEAD` passed, the worktree was clean, and the
  four pre-handoff commits were correctly ordered.
- The build and artifact commands emit the existing
  `ArrowReaderOptions::page_index` deprecation warning in
  `wasm-parquet-engine`; it is non-blocking and unrelated to this package.

No required source, generated-output, compile, test, format, security, or
artifact check remains blocked or skipped. The final fetch left `origin/main`
at `30d7e8b`, confirmed it is an ancestor of the five filesystem commits, and
found no upstream filesystem-package overlap.

### Ownership and deferred work

- `CatalogProvider` discovers catalog objects.
- `DataAccessResolver` resolves table reads.
- `ExecutionProvider` runs and previews queries.
- `FileSystemProvider` lists/stats entries and resolves file reads.

`VolumeNode.ref` remains the catalog volume identity; E8 will adapt it to an
`FsRootRef`. E8 provider adapters and UI, runtime path/range enforcement,
transport/client/server code, persistence, grant caching, and all other runtime
adoption remain deferred.

All five slices and exit gates are complete. The E3A filesystem contract PI is
complete and E3A M4 is ready to proceed.
