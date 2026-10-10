# Qualifying And Deploying Main

- Date: 2026-10-10
- Applies to: the governed release path in `.github/workflows/ci.yml` and
  `.github/workflows/deploy-axon-web.yml`

## Current state

`Deploy axon-web` only stages and promotes the `axon-web-qualified-prebuilt` artifact that the
`Qualified WASM stack release artifact` CI job uploads for a `main` push. That job builds the
artifact only when every component in [`wasm-stack.lock.toml`](../../wasm-stack.lock.toml) that is
linked into the WASM closure has both `candidate_sha` and `stack_sha` set. Today each
`candidate_sha` is `UNSET`, so main CI is green but produces nothing to deploy; the release job
reports this as a `Release not qualified` warning with a run summary that lists what is missing.

The last production deployment predates this governed path: `Deploy axon-web` run 33353379053
promoted `52006f0` on 2026-08-31. Every later main commit is undeployed until the actions below are
complete.

The lock describes the stack main builds: the Daxis Arrow/Parquet 58.3.0, `object_store` 0.13.2,
and DataFusion 53.1.0 forks at the revisions pinned in the root `Cargo.toml`, with Delta Kernel and
delta-rs declared absent from the closure. In every mode, CI checks those versions, sources, and
pinned revisions against the locked WASM closure, so the lock cannot drift from the build.

## Owner actions

These are repository-owner and fork-owner actions; nothing in this repository performs them.

1. For each linked Daxis fork (`daxis-io/arrow-rs` at `52c8fb2e9c28b9d89d08c313e1bc938a35c29c99`,
   `daxis-io/arrow-rs-object-store` at `ab9fda65805487edf5487e63082cab8111f0a178`, and
   `daxis-io/datafusion` at `8c5b4526a6b31ebec56f32c173f14be74240da49`):
   1. Confirm that the upstream `base_sha` recorded in the lock is an ancestor of the pinned
      `stack_sha`, and choose a `candidate_sha` on that path (it may equal `stack_sha`).
   2. Confirm that every commit in `base_sha..stack_sha` has a `Signed-off-by` trailer naming its
      author or committer. Final mode rejects any other commit, so a missing sign-off means
      re-publishing the fork history before release.
   3. Create the annotated tags `axon-wasm-v1.0.0-rc.1` and `axon-wasm-v1.0.0` at `stack_sha`, and
      protect the `axon-wasm/v1` branch. These names are fixed by the verifier and the strategy;
      using them for this stack means the later canonical-stack release needs new release names in
      both.
2. Replace each `candidate_sha = "UNSET"` in `wasm-stack.lock.toml`, run
   `bash tests/conformance/verify_upstream_wasm_fork_stack.sh --production --final`, and merge.
3. Confirm the `VERCEL_TOKEN`, `VERCEL_ORG_ID`, and `VERCEL_PROJECT_ID` repository secrets exist.
   Once the lock is complete, the release job fails rather than skips without them.
4. On that commit's successful main CI run, confirm the release job uploaded
   `axon-web-qualified-prebuilt` and `axon-web-qualified-release-evidence`.
5. Dispatch `Deploy axon-web` with `environment: staged`, the CI run ID, and the commit. Then
   dispatch it with `environment: production` and the staged run ID.
6. Confirm that the production `axon-runtime-build.json` reports that commit as `source_commit`
   with `source_dirty: false`. CI and the deploy workflow both refuse an artifact whose manifest
   names another commit or a dirty build.
