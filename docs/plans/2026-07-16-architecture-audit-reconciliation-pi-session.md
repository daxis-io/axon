# Architecture Audit Reconciliation PI Session

## Baselines and preflight

- Date: 2026-07-16.
- Source worktree: `.worktrees/architecture-audit-revisions`.
- Source branch: `docs/architecture-audit-revisions`.
- Source HEAD: `b11a44bbce3ac2b0299507dc83328baec1f57bba`.
- Target worktree: `.worktrees/architecture-audit-reconciliation-pi`.
- Target branch: `docs/architecture-audit-reconciliation-pi`.
- Original replay baseline: `origin/main` at
  `0b255507e4f72dd8746f6c9cb8def7b41c718ef0`.
- Final integration baseline after fresh-session remediation: `origin/main` at
  `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`.
- The dirty root checkout is outside this PI. It is not a source, target, or
  verification workspace and must remain untouched.
- The source HEAD is an ancestor of the final target baseline and is seven
  commits behind it with no source-only commits. None of the 17 tracked source
  documents changed in that upstream range, and the checksummed tracked patch
  applies cleanly to the target worktree.

The seven upstream commits are:

1. `30d7e8bdc729902a7a5f50cd3d30feb7efdd2348 feat: project cache and readahead metrics`
2. `9baf5fb94159dd229aa833aea48295345f336f54 docs: plan e3a filesystem contract pi`
3. `df46acd65e91f688664e073dd6e5a892c689ce4d chore(web): add filesystem contract messages`
4. `19f308464f9914fc6fe407b1ef2c66f28dc7c785 test(web): cover filesystem contract parity`
5. `ceaf8e023d87c89807f245dad5e8a8ce2ec4f846 chore(contract): generate filesystem rust contracts`
6. `0b255507e4f72dd8746f6c9cb8def7b41c718ef0 docs(contract): document filesystem contract handoff`
7. `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a perf(parquet): add bounded identity-safe readahead`

The five filesystem commits, from `9baf5fb` through `0b25550`, were each
confirmed as ancestors of both integration baselines. Together they landed the
`axon/fs/v1` schema, generated TypeScript and Rust contracts, parity and smoke
tests, code-generation checks, and the final filesystem handoff.

## Source inventory and immutable backup

The source audit found exactly 17 modified tracked Markdown files and one
untracked Markdown plan. The tracked diff contains 1,773 insertions and 1,144
deletions. No source-code, generated, lock, or build-artifact path is present.

Tracked source documents:

- `docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md`
- `docs/adr/ADR-0004-native-runtime-is-correctness-oracle-and-mandatory-fallback.md`
- `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md`
- `docs/adr/ADR-0009-axon-is-the-lakehouse-workbench.md`
- `docs/adr/ADR-0010-pluggable-catalog-providers.md`
- `docs/adr/README.md`
- `docs/plans/2026-06-20-e0-frontend-foundation-execution-plan.md`
- `docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md`
- `docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md`
- `docs/plans/2026-07-11-e3a-exec-contract-pi-session.md`
- `docs/program/axon-workbench-architecture.md`
- `docs/program/browser-embedding-deployment.md`
- `docs/program/browser-owned-descriptor-materialization.md`
- `docs/program/browser-uc-brokered-runtime-contract.md`
- `docs/program/provider-model.md`
- `docs/program/rich-lakehouse-workbench-planning-prompts.md`
- `docs/program/rich-lakehouse-workbench-strategy.md`

Untracked source document:

- `docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md`

Before target-worktree creation, the source material was copied to
`/tmp/axon-architecture-audit-revisions.H0cjGc`:

- `tracked.patch`:
  `79512f49ba9bd7c0b408bb33040512cf00d11ee101e90eea2126bc79249fc046`
- `2026-07-15-e9-execution-provider-vertical-slice-plan.md`:
  `f84c274babeabdae32487fa9aebbb74086f0167504bbe9a2602b5b36773fa99e`
- `SHA256SUMS` records both SHA-256 values and passed
  `shasum -a 256 -c SHA256SUMS` before replay.

The source worktree is evidence only. This PI must never edit, stash, clean,
reset, rebase, commit, or otherwise mutate it. Its HEAD, status, and both
backup checksums must be rechecked after the final target commit.

## Semantic reconciliation

The patch applies textually, but three upstream facts require semantic
reconciliation:

1. `30d7e8b` added cache and bounded-readahead worker metrics, and `7681f1d`
   later landed bounded, strong-identity Parquet scan readahead. Neither commit
   defined selected-source integrity, a resolved-binding lifecycle, persistence
   rules, or E9 adoption sequencing. Those authority and lifecycle decisions
   remain documentation work in this PI.
2. The five-commit filesystem chain landed `axon/fs/v1`, deterministic
   TypeScript and Rust output, parity and smoke tests, and its handoff. Current
   guidance must describe that package as landed contract substrate while
   keeping E8 providers, adapters, UI, and runtime adoption deferred.
3. The source revisions predate that chain and still contain forward-looking
   claims that the filesystem package is absent, forbidden, or must be created
   later. Historical implementation evidence stays historical, but those
   forward-looking claims must be corrected.

Every current architecture and program document must use the same ownership
model:

1. `CatalogProvider` discovers.
2. `DataAccessResolver` resolves.
3. `ExecutionProvider` runs.
4. `axon/fs/v1` is landed contract substrate.
5. E8 provider, adapter, UI, and runtime adoption remain deferred.
6. Remote execution and Cedar integration remain gated on concrete consumers.

## Commit boundaries

### Commit 1 — `docs: plan architecture audit reconciliation`

Create and commit this session document before replaying source edits. It
records the immutable source evidence, refreshed baseline, semantic conflicts,
file boundaries, verification commands, non-goals, and exit gates.

### Commit 2 — `docs(adr): sharpen workbench authority boundaries`

Replay and reconcile only:

- `docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md`
- `docs/adr/ADR-0004-native-runtime-is-correctness-oracle-and-mandatory-fallback.md`
- `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md`
- `docs/adr/ADR-0009-axon-is-the-lakehouse-workbench.md`
- `docs/adr/ADR-0010-pluggable-catalog-providers.md`
- `docs/adr/README.md`

Preserve the accepted status of ADR-0002, ADR-0009, and ADR-0010 and the
proposed status of ADR-0004 and ADR-0008. The slice must make selected-source
authority, binding resolution, capability lifetime, fallback, host/profile
ownership, cache ownership, and audit ownership explicit. ADR-0010 and the ADR
index must accurately reflect landed `axon/fs/v1` substrate.

### Commit 3 — `docs(program): tighten workbench execution strategy`

Replay and reconcile only:

- `docs/program/axon-workbench-architecture.md`
- `docs/program/browser-embedding-deployment.md`
- `docs/program/browser-owned-descriptor-materialization.md`
- `docs/program/browser-uc-brokered-runtime-contract.md`
- `docs/program/provider-model.md`
- `docs/program/rich-lakehouse-workbench-planning-prompts.md`
- `docs/program/rich-lakehouse-workbench-strategy.md`

The seven documents must agree on current versus target architecture, narrow
seam ownership, one resolved-binding lifecycle, cache and audit boundaries,
current worker IPC versus target handoff, source-profile composition, and the
landed-contract/deferred-adoption distinction.

### Commit 4 — `docs(plan): define e9 vertical execution slices`

Replay and reconcile only:

- `docs/plans/2026-06-20-e0-frontend-foundation-execution-plan.md`
- `docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md`
- `docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md`
- `docs/plans/2026-07-11-e3a-exec-contract-pi-session.md`
- `docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md`

Every architecture-review baseline in these plans must use the full refreshed
baseline SHA. Historical status remains explicit. Current sequencing is:

1. E9 Slice 1 fixes selected-source integrity and execution-local lifecycle
   without protobuf changes, mapping domain `execution_id` onto existing worker
   correlation fields.
2. One intentional E3A correction PI follows Slice 1.
3. E9 Slice 2 adopts local Delta and public-object resolver/executor seams.
4. E9 Slice 3 composes E1 and E6 for one session-proxied Unity Catalog browser
   query.
5. Remote execution waits for a concrete host.
6. E8 runtime adoption waits for a read-only volume consumer.

### Commit 5 — `docs: document architecture reconciliation handoff`

Update this document only after the first four commits exist. Record their
SHAs, this handoff message, all 19 branch files, the resolution of each semantic
conflict, fresh verification evidence, intentional skips or exact blockers,
remaining design decisions, and the E9 Slice 1 readiness verdict.

## Verification plan

The target worktree uses the web package's pinned Prettier and
`apps/axon-web/.prettierrc.json`. `npm ci` is permitted only when the target
worktree lacks that existing toolchain. Format only the 19 changed Markdown
files, then check those same files.

Run a dependency-free Node link checker over every changed Markdown file. It
must ignore external and anchor-only URLs, resolve relative local paths from
each source document, strip query and fragment suffixes, support directory
targets, and fail on every missing local target.

Run and record:

```bash
git diff --name-only origin/main...HEAD
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

The final search gates must find no:

- stale `b11a44b` architecture-review baseline;
- current claim that `axon/fs/v1` is absent, future, forbidden, or still needs
  creation;
- sequence that puts the E3A correction PI before E9 Slice 1; or
- claim that E8 provider, adapter, UI, or runtime adoption has landed.

Manually inspect every E9 ordering reference against the six-step sequence in
Commit 4. Recheck the immutable source HEAD and status and verify both backup
checksums after the final target commit.

Product, Rust, WASM, code-generation, and browser tests are intentionally out
of scope because this branch changes documentation only. They are recorded as
scope skips, not passing checks.

## Non-goals

- Do not change public APIs, protobuf schemas, generated contracts, package
  locks, application code, tests, build artifacts, or runtime behavior.
- Do not implement E9, E8, remote execution, Cedar enforcement, provider
  adapters, UI, cache policy, or the E3A correction wire shape.
- Do not rewrite historical evidence as though later artifacts existed at the
  time of the recorded implementation.
- Do not mutate the source worktree or the dirty root checkout.
- Do not push, open a pull request, merge, or publish this branch.

## Exit gates

- The source worktree retains its original HEAD and exact status.
- Both backup checksums still verify.
- The branch diff contains exactly the 18 reconciled source documents plus this
  session document and no other path.
- ADR statuses and historical implementation evidence remain accurate.
- All current guidance reflects the `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`
  ancestry and the landed `axon/fs/v1` substrate.
- Formatting, changed-link checks, stale-language searches, the exact
  allowlist, `git diff --check`, and manual ordering review all pass.
- Exactly five ordered commits exist on top of the refreshed baseline and the
  target worktree is clean.
- No required check is skipped without its exact reason being recorded.

Passing every gate unblocks E9 Slice 1 only. The intentional E3A correction PI
remains a mandatory gate between E9 Slice 1 and E9 Slice 2.

## Handoff record

The first four commits are:

1. `ba30d2859749c64246683f0a65fbf811625dd4bd docs: plan architecture audit reconciliation`
2. `3bceba91060bf02f6932dfd0b1729e24daf7f682 docs(adr): sharpen workbench authority boundaries`
3. `75617ae68b1ef771903f944dc92665f2ce4fb401 docs(program): tighten workbench execution strategy`
4. `4089d6c790ffd9f8b1f1cb7471f474de75dddc14 docs(plan): define e9 vertical execution slices`

The handoff commit message is
`docs: document architecture reconciliation handoff`.

### Reconciled file set

ADR documents:

- `docs/adr/ADR-0002-browser-access-uses-signed-https-or-proxy-never-cloud-secrets.md`
- `docs/adr/ADR-0004-native-runtime-is-correctness-oracle-and-mandatory-fallback.md`
- `docs/adr/ADR-0008-daxis-browser-read-compute-contract.md`
- `docs/adr/ADR-0009-axon-is-the-lakehouse-workbench.md`
- `docs/adr/ADR-0010-pluggable-catalog-providers.md`
- `docs/adr/README.md`

Program documents:

- `docs/program/axon-workbench-architecture.md`
- `docs/program/browser-embedding-deployment.md`
- `docs/program/browser-owned-descriptor-materialization.md`
- `docs/program/browser-uc-brokered-runtime-contract.md`
- `docs/program/provider-model.md`
- `docs/program/rich-lakehouse-workbench-planning-prompts.md`
- `docs/program/rich-lakehouse-workbench-strategy.md`

Execution plans:

- `docs/plans/2026-06-20-e0-frontend-foundation-execution-plan.md`
- `docs/plans/2026-06-20-e1-catalog-providers-execution-plan.md`
- `docs/plans/2026-06-20-e3a-provider-contract-surfaces-execution-plan.md`
- `docs/plans/2026-07-11-e3a-exec-contract-pi-session.md`
- `docs/plans/2026-07-15-e9-execution-provider-vertical-slice-plan.md`

Reconciliation record:

- `docs/plans/2026-07-16-architecture-audit-reconciliation-pi-session.md`

That is the exact set of 18 source documents plus this session document.

## Conflict resolution

- **Metrics and readahead:** `30d7e8b` is recorded as landed cache and
  bounded-readahead observation, while `7681f1d` is recorded as the bounded,
  strong-identity Parquet scan readahead implementation. The documents do not
  treat either change as selected-source, capability-lifetime, admission,
  cancellation, or terminal-state semantics.
- **Filesystem substrate:** all current reviews list the five landed packages,
  including `axon/fs/v1` TypeScript and Buffa Rust proof. They classify the
  filesystem package as messages-only substrate and keep E8 provider, adapter,
  UI, and runtime adoption behind a real read-only volume consumer.
- **Historical handoff:** the exec-contract PI retains its original baselines,
  commit slices, test evidence, and contemporary non-goals as historical
  sections. Its current interpretation and forward work now reflect the landed
  filesystem chain and the revised E9 sequence.
- **Sequencing:** every current ordering reference puts E9 Slice 1 first with no
  protobuf changes and maps domain `execution_id` to existing worker correlation
  fields. The intentional E3A correction PI follows and gates E9 Slice 2. Slice
  2 adopts local/public seams; Slice 3 composes E1 and E6. Remote execution and
  E8 adoption then remain independently consumer-gated.

## Verification evidence

Pre-handoff evidence:

- `origin/main` was fetched before preflight and again before handoff. It
  remained `0b255507e4f72dd8746f6c9cb8def7b41c718ef0`; the six-commit upstream
  range and no-overlap result did not change.
- The source worktree remained at
  `b11a44bbce3ac2b0299507dc83328baec1f57bba` with the original 17 modified
  Markdown files and untracked E9 plan. Its regenerated binary diff and E9 plan
  compare byte-for-byte with the backup.
- `shasum -a 256 -c SHA256SUMS` returned `OK` for both backup entries.
- `npm ci` installed the absent web toolchain without tracked changes. The
  package-resolved Prettier is 3.8.3.
- The pinned Prettier with `apps/axon-web/.prettierrc.json` reports all 19 files
  formatted.
- The dependency-free Node link checker inspected 80 relative local Markdown
  targets across all 19 files and found no missing target.
- The exact-diff allowlist contains 19 Markdown files: the 18 source documents
  plus this session document.
- Search gates over the 18 reconciled source documents found no stale
  architecture-review baseline, obsolete filesystem-package claim, ordering
  that puts the E3A correction before Slice 1, or positive claim that E8
  provider, adapter, UI, or runtime adoption has landed.
- Manual ordering review found every ordering reference in the E3A prompt,
  strategy, E0/E1/E3A plans, exec handoff, and E9 plan consistent with the same
  six-step sequence.
- `git diff --check origin/main...HEAD` passed before the handoff edit.

Post-handoff closure on the initial handoff commit produced:

- the same Prettier result and `markdown_links_ok=80 files=19`;
- `allowlist_ok=19`, `markdown_only=OK`, and `diff_check=OK`;
- exactly five commits in the planned order and with the planned messages;
- an empty `git status --short`; and
- `OK` for both backup checksums plus byte-for-byte source patch and E9-plan
  comparison.

The closure bundle was then repeated on the final amend-ready documentation
tree. The handoff amendment changes only this evidence and verdict text; it does
not add a sixth commit or alter another document.

Fresh-session review and remediation:

- A later fetch observed `origin/main` advance by one path-disjoint commit from
  `0b255507e4f72dd8746f6c9cb8def7b41c718ef0` to
  `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a`. The commit changed eight Rust
  files and none of the 19 branch documents.
- The five-commit branch rebased cleanly onto `7681f1d`. All five current
  architecture-review baselines now use the full integration SHA, and the E9
  current-state section distinguishes the landed readahead implementation from
  the earlier metrics projection.
- The fresh review found one outcome-taxonomy contradiction: strategy text
  incorrectly grouped invalid and expired resolution failures with `denied` and
  `remote_required`. The strategy now reserves `denied` for policy refusal,
  limits `remote_required` to a known enforcement owner, and maps invalid,
  expired, unsafe, or unknown inputs to typed resolution errors.
- The review also found that the ordering evidence overstated full-sequence
  repetition in documents that contain only relevant subsets. The evidence now
  states that every ordering reference is consistent with the six-step
  sequence.
- The taxonomy and baseline changes were folded into their original program and
  plan commits. The evidence correction is recorded in this handoff, which
  remains the fifth commit.

Intentional scope skips:

- Product, Rust, WASM, code-generation, and browser tests were not run. This PI
  changes documentation only, so those commands would not validate the changed
  surface. This is a scope skip, not a passing result.
- No push, pull request, merge, or root-checkout mutation was attempted.

The two fresh-review findings were resolved. No blocker remains.

## Remaining decisions

- The intentional E3A correction PI must choose the exact wire shape for
  canonical identity, the one binding arm, resolution/admission/terminal
  algebras, capability lifetime, and `execution_id` compatibility mapping.
- A portable remote transport remains undefined until a concrete host supplies
  identity, admission, backpressure, cancellation, retry, persistence, and audit
  requirements.
- Cedar remains deferred until a standalone policy consumer justifies its
  schema, policy lifecycle, bundle cost, and conformance suite.
- E8 still needs one read-only volume adapter, UI, and runtime consumer over the
  landed filesystem messages.
- Persistent governed-byte caching still needs enforcement-owner policy,
  principal/session namespacing, revocation behavior, logout invalidation,
  retention, and audit ownership.

## Readiness verdict

Ready for E9 Slice 1. The five-commit, clean-worktree, formatting, link,
stale-language, allowlist, diff, source-immutability, and checksum gates passed
together. This verdict unblocks only the no-protobuf selected-source and
lifecycle slice. The intentional E3A correction PI remains a mandatory gate
before E9 Slice 2.
