# Public S3 Current-Main Browser Evidence Refresh

> **Execution boundary:** work only in
> `/Users/ethanurbanski/axon/.worktrees/public-s3-evidence-refresh` on
> `perf/public-s3-evidence-refresh`. Keep the root checkout read-only. Create
> exactly two local commits; do not push, merge, tune runtime policy, or mutate
> cloud state.

> **Publication override — 2026-07-25:** after the completed audit follow-up, the
> user explicitly authorized a direct push to `origin/main`. This supersedes only
> the original no-push boundary. The branch was rebased without conflict onto
> `origin/main` at `ee6a430afe99144c5e5780952b45a335d15e89c3` and the complete
> evidence gate was rerun before publication.

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

The coordinator staging peak must not exceed its emitted per-query staging
limit. Cursor pending encoded and transport-chunk peaks must not exceed the
runtime's fixed 8 MiB and 1 MiB bounds, respectively.

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

## Execution handoff

The implementation stayed in the isolated worktree and changed only this plan,
`apps/axon-web/tests/public-s3-live.spec.ts`, and the canonical browser-performance
plan.

### Evidence-contract implementation

Commit `dee3a2f` (`test(perf): complete public S3 evidence contract`) extends the
existing worker interceptor and artifact builder. It:

- requires every range, cache, overfetch, IPC, coordinator, and cursor metric;
- records the full metric set for pre-cache, verified 2026-07-16, and current-main
  revisions, with `null` for historical fields that were not collected;
- captures request-correlated owned-memory and browser-WASM success events;
- rejects non-integer, negative, missing, terminally owned, over-limit, and fallback
  evidence;
- gates the canonical performance artifact on the exact pinned URI and region;
- requires all fresh-browser `COUNT(*)` results to equal `1,048,576`;
- redacts URI userinfo, query, and fragment content, then rejects AWS access-key,
  credential, token, signed-query, and `X-Amz-*` material in the serialized
  artifact;
- writes and attaches the performance artifact before enforcing the readahead
  decision gate.

The TDD cycles produced these expected failures before implementation:

- projection and comparison coverage failed while overfetch, memory, and chunk
  fields were absent;
- owned-memory validation failed while current ownership and peak limits were not
  enforced;
- review hardening failed four tests for incomplete historical comparison,
  unmodeled `X-Amz-*` fields, lookalike fixture acceptance, and interleaved request
  correlation.
- the 2026-07-25 audit follow-up failed three independent tests while coordinator
  peaks above the emitted staging limit and cursor peaks above the 8 MiB pending
  and 1 MiB transport bounds were still accepted.

The final missing-environment run passed 13 contract tests and skipped the three
live tests.

### Fixture and build verification

From `apps/axon-web`:

| Command                                                                                                                                               | Result                                                                   |
| ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `npm run verify:s3-perf-fixture`                                                                                                                      | Passed; 21 required objects                                              |
| `npm run test:s3-perf-fixture`                                                                                                                        | Passed; the tamper regression printed its expected manifest mismatch     |
| `bash scripts/verify-s3-perf-fixture.sh --stage ../../target/fixtures/s3-perf-pinned/table`                                                           | Rejected before network access because the safety check rejects `..`     |
| `bash scripts/verify-s3-perf-fixture.sh --stage /Users/ethanurbanski/axon/.worktrees/public-s3-evidence-refresh/target/fixtures/s3-perf-pinned/table` | Passed after one approved anonymous-network retry; staged all 21 objects |
| `npm run build:fixture`                                                                                                                               | Passed                                                                   |
| `npm run build:wasm`                                                                                                                                  | Passed                                                                   |
| `AXON_LIVE_PUBLIC_S3_TABLE_URI= AXON_LIVE_PUBLIC_S3_REGION= npm run test:browser:public-s3-live -- --reporter=line`                                   | 13 passed, 3 skipped                                                     |
| `npm run test:sdk -- --grep owned-memory`                                                                                                             | 2 passed                                                                 |
| `npm run lint`                                                                                                                                        | Passed                                                                   |
| `npx tsc --noEmit`                                                                                                                                    | Passed                                                                   |
| `npm run format:check`                                                                                                                                | Passed                                                                   |

The validated provenance remained:

- URI:
  `s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table`
- region: `us-east-2`
- manifest SHA-256:
  `18d1c4c3b5e1ce78ce156ce51247a94a46e44401cad9688ec0d14ceaa01b6ab3`
- inventory SHA-256:
  `05f6c5823a88c49559eef70072165b584dfe3c320ae8a435c6f6f82f30d719a9`
- inventory: 21 objects, 8 active files, 82,057,700 bytes, and 1,048,576
  rows.

### Live browser result

The final run used the required command:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI=s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
CI=1 \
npm run test:browser:public-s3-live -- --reporter=line
```

All 16 Chromium tests passed, including the anonymous list/log/range smoke, three
fresh-browser `COUNT(*)` runs, and the performance query. The run used port 5173.
The initial sandboxed attempt could not bind loopback and returned `EPERM`. The
final approved run used the same command. Port 5173 had no owner, so no process
was killed and no temporary Playwright config was needed.

Canonical artifact:

```text
apps/axon-web/test-results/public-s3-live-public-S3-l-352ff-adahead-comparison-evidence-chromium/public-s3-live-uat-evidence.json
```

SHA-256: `b403ab279dc38d95cc487a3f48d9d5d8a38f629931045a9c878136b5d5949326`.

The publication artifact records base
`ee6a430afe99144c5e5780952b45a335d15e89c3`. Its measured metrics are unchanged
from the pre-publication audit run; the new SHA reflects the updated provenance
field.

Repeat-query artifact:

```text
apps/axon-web/test-results/public-s3-live-public-S3-l-3c4bf-ross-fresh-browser-runtimes-chromium/public-s3-repeat-query-evidence.json
```

SHA-256: `68c02460d60c70e3af32dc57b588936466ca29b356880a7e939635ae12026f12`.

The performance query recorded:

- 22,677,645 physical bytes across 160 logical scan data ranges;
- 32 coalesced reads, zero gap bytes, zero duplicate reads, and zero scan
  overfetch;
- 128 cache misses, 22,677,645 bytes stored, zero reuse, and zero validation or
  degraded-identity misses;
- zero readahead requests and zero fetched, used, or wasted readahead bytes;
- 1,048,576 rows, 36,744 Arrow IPC bytes, and one IPC chunk;
- a 36,744-byte coordinator staging peak under the 8,388,608-byte staging limit;
- 36,288-byte cursor pending and transport-chunk peaks;
- zero terminal coordinator reserved/staged ownership, with peaks of 8,388,608
  reserved bytes and 36,744 staged bytes under the 33,554,432-byte limit;
- zero terminal DataFusion ownership and a 4,815,095-byte peak under the
  67,108,864-byte limit;
- `browser_wasm`, no fallback event, and no response fallback reason.

All three fresh-browser count runs returned `1,048,576`, reported browser-WASM
execution without fallback, and ended with zero terminal ownership. The serialized
artifact scan found no credential or signed-query material.

### Decision

The performance workload exercised scan reads and physical bytes. Readahead and
scan overfetch remained zero. Record the run as no-overfetch evidence, not a
latency improvement. Keep page-index byte-savings A/B research as the next slice;
make no cache, readahead, or page-index policy change from this run.

`apps/axon-web/test-results/` remains ignored and uncommitted.
