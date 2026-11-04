# Qualifying And Deploying Main

- Date: 2026-11-03
- Applies to: the governed release path in `.github/workflows/ci.yml` and
  `.github/workflows/deploy-axon-web.yml`

## Current state

`Deploy axon-web` only stages and promotes the `axon-web-qualified-prebuilt` artifact that the
`Qualified WASM stack release artifact` CI job uploads for a `main` push. That job builds the
artifact only when every component in [`wasm-stack.lock.toml`](../../wasm-stack.lock.toml) that is
linked into the WASM closure has both `candidate_sha` and `stack_sha` set. This candidate now
populates both fields and publishes the required fork source tags. It still needs successful
final-mode CI and integration into main before a qualified artifact can be produced. Main at
`a04309b` still has the earlier bootstrap lock and no qualifying deployment artifact.

The last production deployment predates this governed path: `Deploy axon-web` run 33353379053
promoted `52006f0` on 2026-08-31. Every later main commit is undeployed until the actions below are
complete.

The lock describes the candidate stack: the Daxis Arrow/Parquet 58.3.0, `object_store` 0.13.2,
and DataFusion 53.1.0 forks at the revisions pinned in the root `Cargo.toml`, with Delta Kernel and
delta-rs declared absent from the closure. In every mode, CI checks those versions, sources, and
pinned revisions against the locked WASM closure, so the lock cannot drift from the build.

## Fork publication checkpoint

The `axon-wasm/v1` branches are published and protected against force pushes and deletion,
including administrator enforcement. Annotated `axon-wasm-v1.0.1-rc.1` and
`axon-wasm-v1.0.1` source tags are published at these exact commits:

| Fork | Published source commit | DCO range |
| --- | --- | --- |
| `daxis-io/arrow-rs` | `7fda638045973b2aee44eee78fd5146e2e3575ca` | 12 commits passed |
| `daxis-io/arrow-rs-object-store` | `73f659ffe91e0798527fc8eadcd36caedcbd6f84` | 17 commits passed |
| `daxis-io/datafusion` | `1634256cab2470eb4e6f966ebc5c4381c9799a7c` | 26 commits passed |

The object-store and initial DataFusion replacements add the original author's sign-off while
preserving the original source trees and ordered parents. Arrow then updates its root manifest's
object-store pin; DataFusion updates only its root manifest and lockfile to request the signed
object-store and updated Arrow revisions. All nine git dependencies across every fork manifest
select the same release identities. Old tips remain on their original publication refs. Local
fork checkouts and unrelated work were preserved.
The root Cargo manifest, lockfile, shipping-pin policy, and stack lock agree on the new identities.

The superseded `axon-wasm-v1.0.0-rc.1` and `axon-wasm-v1.0.0` source tags remain immutable.
CI at `a9ed9fd` rejected the hand-updated Axon lockfile because DataFusion still requested the
old object-store revision. CI at `fd15315` exposed the same old revision in Arrow's manifest.
The `v1.0.1` source set aligns the complete fork dependency chain; only fresh locked-graph and
artifact checks can qualify it.

The locked native workspace check and fresh browser WASM build passed in
[CI run 41772389780](https://github.com/daxis-io/axon/actions/runs/41772389780) at `b7f5249` before
tag publication. All six remote annotated tag objects and peeled commits were then verified;
the earlier tag objects are unchanged. The three upstream POC jobs passed on the same Axon
commit. These partial shipping-build results and separate POC proofs do not qualify an Axon
release artifact; the complete CI run and successful main artifact job are still required.

These source tags designate the current DataFusion 53/Arrow 58 stack. The separate canonical
stack requires new release names and corresponding verifier/strategy changes. Source provenance
does not qualify an Axon executable or a production deployment.

The production dependency audit also found Seroval `1.5.4` under TanStack Router. Updating only
that lockfile entry to compatible `1.7.7` clears the production audit (zero advisories). All 684
unit tests and the TypeScript check pass with the update. The maintainer documents the
[patched deserializer release](https://github.com/lxsmnsyc/seroval/security/advisories/GHSA-97r2-qh9m-474h).
Fresh artifact and bundle checks must cover this updated JavaScript dependency before release.

## Remaining owner actions

1. Require full fresh CI on the Seroval-updated commit in
   [integration PR #143](https://github.com/daxis-io/axon/pull/143): the locked graph, Rust/WASM
   builds, browser artifact, production-bundle smoke, ten-spill qualification, and final-mode
   provenance must pass before integration. Confirm the production dependency audit remains clear.
   The earlier local repinned metadata attempt was stopped during a test-data submodule fetch
   with about 2 GiB free; it is blocked evidence, not a passed local graph check. No artifacts
   were deleted.
2. Integrate only after those gates pass. PR #143 contains #139–#142; retire their separate
   integration paths after the combined changes land.
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

## Earlier local readiness checkpoint at `4f09d15` — November 3, 2026

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
| Governed release artifact | **blocked** | Fork DCO repairs, protected source branches, and release tags are complete; integrate into main and require successful final-mode main CI |
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
