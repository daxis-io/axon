# Qualifying And Deploying Main

- Date: 2026-11-03
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
      author or committer. Final mode rejects any other commit. The arrow-rs pin passes; the
      object_store pin `ab9fda65` and the DataFusion pin `8c5b4526` do not. Each is the tip of its
      range and its one commit without a sign-off, so tagging the current pins makes
      `--production --final` fail with `object_store commit lacks DCO sign-off`, then with the same
      error for DataFusion. For those two forks, sign off the tip without changing its tree or
      parents (`git commit --amend --no-edit --signoff`), push the result to a new ref so the
      current pin stays fetchable for older main commits, and repin main to it: the `rev` entries
      in the root `Cargo.toml`, `Cargo.lock`, `stack_sha` in `wasm-stack.lock.toml`, and in
      `.github/shipping-fork-pins.toml` both the `revision` and the `publication_ref`, which must
      name the new ref, because `verify_shipping_fork_pins.sh` requires each approved revision to
      be the tip of its publication ref.
   3. Create the annotated tags `axon-wasm-v1.0.0-rc.1` and `axon-wasm-v1.0.0` at `stack_sha`, and
      protect the `axon-wasm/v1` branch. These names are fixed by the verifier and the strategy;
      using them for this stack means the later canonical-stack release needs new release names in
      both.
2. Replace each `candidate_sha = "UNSET"` in `wasm-stack.lock.toml`, run
   `bash tests/conformance/verify_upstream_wasm_fork_stack.sh --production --final`, and merge.
3. Confirm the `VERCEL_TOKEN`, `VERCEL_ORG_ID`, and `VERCEL_PROJECT_ID` repository secrets exist.
   Once the lock is complete, the release job fails rather than skips without them.
   Protected deployments also require a valid `VERCEL_AUTOMATION_BYPASS_SECRET`. Blocked access
   fails verification, including when a configured bypass is rejected; it never counts as a pass.
4. On that commit's successful main CI run, confirm the release job uploaded
   `axon-web-qualified-prebuilt` and `axon-web-qualified-release-evidence`.
5. Dispatch `Deploy axon-web` with `environment: staged`, the CI run ID, and the commit. Then
   dispatch it with `environment: production` and the staged run ID. Both runs must pass the
   `Verify staged deployment` step before any production promotion. It checks routes, worker/WASM
   MIME types, missing-asset 404s, and the exact runtime manifest, then runs a real query through
   the bundled sample in Chromium. The promotion run repeats this check against the selected
   immutable staged URL. Bypass headers stay on the staged origin, including across redirects.
6. Confirm that the production `axon-runtime-build.json` reports that commit as `source_commit`
   with `source_dirty: false`. CI and the deploy workflow both refuse an artifact whose manifest
   names another commit or a dirty build.

## Local readiness checkpoint — November 3, 2026

The local `fix/production-readiness-20261103` branch starts at combined candidate
`a71d5754fad6a241fb0092fd6785f36ab4d8aef6`, which includes PRs #139–#142. Those PRs remain
unmerged upstream at this checkpoint. This branch adds the supported Ubuntu 24.04 browser
runner, mandatory staged query verification, runtime-manifest comparison with bounded edge
propagation retries, and the deferred local commit-history import that fixes eager WASM loading.

| Gate | State | Evidence and limits |
| --- | --- | --- |
| Unit and SDK suites | **passed** | 684 unit tests, 156 SDK tests; generated WASM is retained, not rebuilt |
| TypeScript, affected ESLint/Prettier, release workflow and toolchain contracts | **passed** | Local commands; not GitHub CI |
| Startup regression | **passed** | Both prior lazy-startup failures reproduced before the snapshot-loader fix and passed afterwards |
| Deployment access and identity regressions | **passed** | Blocked/rejected bypasses and wrong manifests fail; stale edge manifests retry; a two-origin browser regression proves the bypass does not follow a cross-origin redirect |
| Coordinator recovery and private WebKit | **passed** | Five focused browser cases passed; four existing browser-specific skips; non-spilling private-WebKit queries run with spill unavailable |
| First expanded editor matrix | **failed** | 136 passed, 7 failed, 19 skipped; retained under `apps/axon-web/test-results/production-readiness-editor/` |
| Browser fixture repairs | **passed** | 18 passed, 3 capability skips; IndexedDB fault injection patches the prototype, external font/analytics requests are mocked, and clipboard permission coverage is Chromium-specific |
| Final editor matrix | **passed** | Chromium, Firefox, WebKit: 143 passed, 22 explicit skips, zero failures; retained under `apps/axon-web/test-results/production-readiness-editor-final/`; skips include stress fixtures, browser-specific cases, clipboard permissions, and WebKit directory-handle persistence |
| Persistent WebKit OPFS lifecycle | **passed** | Existing real 4 MiB worker lifecycle test; separate from private-WebKit unavailability |
| Governed release artifact | **blocked** | Fork DCO repairs, release pins/tags, and successful main CI are still required |
| Canonical Kernel/checkpoint stack | **blocked** | Separate candidate; enclosing qualification reports `RuntimeError: retained browsers changed`; passing functional substages do not close the runtime-identity audit |
| Fresh native/WASM builds, ten-spill qualification, matched performance, real protected Vercel staging/promotion, and private-data host proof | **untested** | Not admitted by these local checks; no production or provider operation was performed |

The pinned WebKit cannot structured-clone a real directory handle (`DataCloneError`), so the
directory-handle persistence test is explicitly skipped there. Session import and reselect
remain covered. Private WebKit and persistent WebKit have separate OPFS expectations. The
browser suite's stress/fixture-dependent skips are not qualification passes.

For bounded local browser checks using installed browsers and retained WASM:

```bash
cd apps/axon-web
npm run dev:server -- --port 5174 --strictPort
# In another terminal:
AXON_EDITOR_BROWSER_MATRIX=1 AXON_WEBKIT_PRIVATE_OPFS=1 \
  npm run test:browser:editor-smoke -- --output=test-results/production-readiness-editor-final
PLAYWRIGHT_BASE_URL=https://127.0.0.1:5174 \
  npx playwright test --config=playwright.config.ts \
  --grep 'private WebKit reports|bounds queued SQL|editor query session reopens'
```

`npm run test:browser:production-bundle` normally starts a local preview of `dist`. Setting
`PLAYWRIGHT_BASE_URL` instead tests an existing deployment, requires valid TLS for HTTPS,
and starts no local server. Use a fresh production bundle for the source being qualified.
The release gate never rebuilds the downloaded qualified artifact during deployment.
