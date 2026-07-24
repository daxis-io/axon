# Public S3 Current-Main Browser Evidence Refresh

> **Execution boundary:** work only in
> `/Users/ethanurbanski/axon/.worktrees/public-s3-evidence-refresh` on
> `perf/public-s3-evidence-refresh`. Keep the root checkout read-only. Create
> exactly two local commits; do not push, merge, tune runtime policy, or mutate
> cloud state.

## Goal

Refresh the pinned public-S3 browser-performance evidence on current `origin/main`
after the streaming-memory bounds landed. The result must distinguish historical
unavailable fields from measured zeros, prove browser-WASM execution without
fallback, capture terminal and peak owned-memory state, and leave an inspectable
artifact even when the readahead decision gate fails.

## Preflight and base proof

On 2026-07-23:

- `git fetch origin main` left `origin/main` at
  `62d4c465e10dc329221023eaaf2c67c542c408ce`.
- The prior root-checkout commit
  `3e5aceda0c1eb2c0dea983c0e5849200447a363f` is an ancestor:
  `git merge-base --is-ancestor 3e5aceda... origin/main` exited zero.
- `git rev-list --count 3e5aceda...origin/main` returned `6`.
- Those six commits are the streaming-memory plan, coordinator staging bound,
  DataFusion operator-memory bound, owned-memory plateau proof, isolated
  coordinator-capacity proof, and verification record.
- No newer evidence-refresh commit has landed. The requested branch and
  worktree path were absent before creation.
- `.gitignore` ignores `.worktrees/`.
- The root checkout was dirty and six commits behind; it is not an implementation
  source.
- The isolated worktree was created from `origin/main` and begins clean.
- The initial shell did not export either live-S3 environment variable. The
  deterministic missing-env gate will run with both explicitly blank, and the
  final live command will use the pinned public fixture values below.
- Disk preflight reported 118 GiB available.

## Existing surfaces and evidence inputs

Keep the implementation in the existing public-S3 worker interceptor and artifact
builder in `apps/axon-web/tests/public-s3-live.spec.ts`.

Current-main already provides:

- the public-S3 Playwright config and `test:browser:public-s3-live` command;
- required cache, coalescing, readahead, row, physical-byte, Arrow IPC byte, and
  optional Arrow IPC chunk metrics;
- `scan_overfetch_bytes`, coordinator staging peak/limit, and cursor pending/chunk
  peaks on the browser query-metrics event;
- the browser-only `owned_memory_metrics` event with coordinator and DataFusion
  current/peak/limit ownership;
- successful response metadata with `browser_wasm` target plus structured fallback
  response and event paths;
- the canonical performance SQL and artifact writer.

Historical comparison inputs:

1. The embedded pre-cache baseline already in the spec.
2. The verified 2026-07-16 artifact at
   `apps/axon-web/test-results/public-s3-live-public-S3-l-0761d--table-root-in-browser-WASM-chromium/public-s3-live-uat-evidence.json`,
   identified by SHA-256
   `0dbda0ae8f7018f739fbaf57897aebc1dfa5083927c8bc6691f9a494424a7152`.
3. The new current-main artifact produced by this slice.

Historical `scan_overfetch_bytes` and owned-memory fields were not collected.
Represent them as unavailable (`null`), never as inferred zero.

## Pinned fixture provenance

- URI:
  `s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table`
- Region: `us-east-2`
- Fixture revision: `s3-browser-perf-v1`
- Manifest SHA-256:
  `18d1c4c3b5e1ce78ce156ce51247a94a46e44401cad9688ec0d14ceaa01b6ab3`
- Inventory SHA-256:
  `05f6c5823a88c49559eef70072165b584dfe3c320ae8a435c6f6f82f30d719a9`
- Inventory: 21 required objects, 8 active files, 82,057,700 active data
  bytes, and exactly 1,048,576 rows.

The companion `COUNT(*)` proof must equal `1,048,576` on every fresh-browser run,
not merely match the first nonempty scalar result.

## Evidence contract

### Query metrics

Require finite, nonnegative safe integers for every current metric and preserve:

- physical bytes, bootstrap/scan footer reads, scan data reads, duplicate reads;
- coalesced reads and fetched gap bytes;
- footer cache hits/misses and avoided reads;
- identity-present and identity-missing reads;
- range-cache hits/misses, bytes reused/stored, validation misses, and
  degraded-identity reads;
- readahead requests and fetched/used/wasted bytes;
- `scan_overfetch_bytes`;
- rows emitted;
- Arrow IPC bytes and Arrow IPC chunk count;
- coordinator staging peak and limit;
- cursor peak pending encoded bytes and peak chunk bytes.

Arrow IPC chunk count is required for this current-main evidence rather than
remaining optional.

### Owned memory

Capture the terminal `owned_memory_metrics` event for each query and store:

- coordinator current reserved, peak reserved, and limit bytes;
- DataFusion current owned, peak owned, and limit bytes.

Each value must be a finite, nonnegative safe integer. Both terminal current
ownership values must be zero, and each peak must be no greater than its
corresponding limit.

### Execution and fallback

Capture the successful worker response for each query and require target
`browser_wasm`. Fail if either a fallback event was observed or the successful
response contains a fallback reason. Preserve the current performance SQL exactly
for historical comparability.

### Comparison shape

The performance artifact must contain three labeled revisions:

- `pre_cache`;
- `verified_2026_07_16`, including the artifact SHA-256;
- `current_main`, including the current commit.

The comparison includes cache, scan/physical bytes, overfetch, readahead, row,
Arrow IPC byte/chunk, coordinator/cursor peak, and owned-memory current/peak/limit
fields. Historical unavailable values remain `null`.

## Redaction and artifact safety

Continue stripping URI userinfo, query, and fragment components before evidence
serialization. After building the full artifact, scan its serialized form and fail
if it contains:

- URI usernames or passwords;
- AWS access-key identifiers (`AKIA...` or `ASIA...`);
- `aws_access_key_id`, `aws_secret_access_key`, or `aws_session_token`;
- signed-query names or material such as `X-Amz-*`, `Signature`, `Credential`,
  `Security-Token`, or `token=`.

Do not print environment-variable values. Keep
`apps/axon-web/test-results/` ignored and uncommitted.

## Vertical TDD sequence

### Cycle 1: projection and comparison

1. Extend the pure projection/comparison tests first.
2. Run the public-S3 spec with both live variables blank.
3. Confirm the new assertions fail because the required fields and three-way
   comparison are absent.
4. Implement only the projection, historical comparison, and serialization
   changes needed to make the tests pass.
5. Rerun the same spec and confirm green.

### Cycle 2: owned-memory and terminal validation

1. Add pure terminal-validation tests and live-capture assertions first.
2. Run the public-S3 spec and confirm failure because owned-memory/success/fallback
   capture is absent.
3. Extend the existing interceptor and query capture helper.
4. Rerun the spec and focused owned-memory SDK tests.

## Verification commands

From `apps/axon-web`:

```bash
npm install
npm run verify:s3-perf-fixture
npm run test:s3-perf-fixture
bash scripts/verify-s3-perf-fixture.sh --stage ../../target/fixtures/s3-perf-pinned/table
npm run build:fixture
npm run build:wasm

AXON_LIVE_PUBLIC_S3_TABLE_URI= AXON_LIVE_PUBLIC_S3_REGION= \
npm run test:browser:public-s3-live -- --reporter=line

npm run test:sdk -- --grep owned-memory
npm run lint
npx tsc --noEmit
npm run format:check
```

Run the live suite with:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI=s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
CI=1 \
npm run test:browser:public-s3-live -- --reporter=line
```

If port 5173 is occupied, do not kill its owner. Create a temporary uncommitted
Playwright config with the same test selection and an isolated base URL, and use
a Vite command bound to that port. Record the exact command and port, then remove
or leave ignored/untracked only as needed for diagnosis. Retry a loopback or
anonymous-network failure once with required approval; a repeated failure is an
external gate.

## Artifact and decision ordering

The live performance test writes and attaches
`public-s3-live-uat-evidence.json` before evaluating the readahead stop condition.
This guarantees that a failed efficiency gate still leaves inspectable evidence.

Decision rules:

- If readahead waste exceeds use, preserve the artifact, document the result,
  stop without tuning, and recommend a separate readahead diagnostic before
  page-index work.
- If scan reads or physical bytes are zero, document that the workload missed the
  target and make no savings claim.
- If readahead remains zero while scan work is exercised, describe the result as
  no-overfetch evidence, not a latency improvement. Keep page-index byte-savings
  A/B research as the next slice.
- If provenance, checksums, CORS, anonymous access, loopback, or the environment
  blocks live proof, make no current-performance claim and recommend rerunning
  this evidence gate before the next optimization.

## Documentation

After the final live run, update
`docs/plans/2026-06-23-browser-query-performance-investigation.md` with:

- fixture, staging, and all-live-tests-executed results;
- the canonical artifact path and SHA-256;
- pre-cache versus 2026-07-16 versus current-main metrics;
- current cache, overfetch, IPC, coordinator/cursor, and owned-memory values;
- the exact live command and any temporary port;
- the decision-rule outcome and one next recommendation;
- a correction to the stale current-status claim that the performance-audit work
  remains local-only, while retaining genuinely historical local-only wording.

Complete this dated plan with the final execution handoff in the same documentation
commit.

## Commit boundaries

1. `test(perf): complete public S3 evidence contract`
   - `docs/plans/2026-07-23-public-s3-live-evidence-refresh.md`
   - `apps/axon-web/tests/public-s3-live.spec.ts`
2. `docs(perf): record current public S3 evidence`
   - `docs/plans/2026-06-23-browser-query-performance-investigation.md`
   - final execution handoff in this dated plan

Before each commit, stage only the named paths and run
`git diff --cached --check`. Finish with:

```bash
git diff --check origin/main...HEAD
git log --oneline origin/main..HEAD
git status --short --branch
```

The final branch must contain exactly two local commits and a clean worktree.

## Non-goals

No cache/readahead policy changes, cloud mutations, Rust/protobuf/SDK changes,
page-index tuning, worker-pool/WCRPC work, public API changes, unrelated cleanup,
push, merge, or pull request.
