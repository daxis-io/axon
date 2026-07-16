# E3A Filesystem Contract PI Session

## Baseline and preflight

- Worktree: `.worktrees/e3a-fs-contract-pi`.
- Branch: `chore/e3a-fs-contract-pi`.
- Refreshed remote baseline: `origin/main` at
  `837195684acaf303a20a69da501b9f7bc5391289`.
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
  filesystem-contract implementation on `origin/main`.
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
- `<Contents>` file projection and representative inline `<CommonPrefixes>`
  directory projection;
- signed-URL, proxy-range, and denied resolution arms;
- signed expiry as a decimal JSON string, proxy `start = 0` through protobuf
  JSON round trips, and denied code/message/correlation ID;
- no secret-like fields in the root, entry, list, stat, read-request, or
  resolution schemas; and
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
cd apps/axon-web && npm run codegen:check
cd apps/axon-web && npm test -- src/generated/contracts
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

- Slice 1 is this filesystem-contract PI plan commit.

## Final handoff

To be completed in Slice 5.
