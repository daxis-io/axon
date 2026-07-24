# Browser Query Performance Workstream Inventory

**Captured:** 2026-07-23

**Remote baseline:** `origin/main` at
`6cca364465fc4fa714ff7403b6df7e3f229c6e8f` after a fresh fetch.

This inventory classifies performance-related refs and worktrees. It does not
delete, reset, rebase, or otherwise modify any pre-existing user state.
`Ahead/behind` values are relative to the remote baseline above.

## Active Resolution

| Ref | Captured head | Ahead/behind | Worktree state | Disposition |
| --- | --- | ---: | --- | --- |
| `perf/resolve-performance-audit` | `a02c346a71ad7663b2c5a69bdacc797a7d07958f` | 15/0 | clean at the implementation checkpoint | Active local-only resolution branch. It contains the recurring WASM size gate, reproducible S3 fixture contract, current-main worker-pool decision, page-index policy migration, speculative-overfetch budget, streaming lifecycle/cancellation fixes, cursor/coordinator memory bounds, and real-browser performance evidence. |
| `perf/internal-arrow-ipc-cursor` | `6cca364465fc4fa714ff7403b6df7e3f229c6e8f` | 0/0 | clean | Its work is the current remote baseline. Archive candidate after the active resolution branch is accepted. |

## Landed Or Patch-Equivalent Performance Chain

These branch tips are ancestors of `origin/main`, except where explicitly
marked patch-equivalent. Their clean worktrees are archive candidates; no
archive action is part of this audit.

| Ref | Head | Remote relation | Worktree state |
| --- | --- | --- | --- |
| `perf/range-identity` | `82cbbb982cf77f427bee7ff25311787800df8ecb` | ancestor; 95 behind | clean |
| `perf/scan-trace-metrics` | `10c75b5578814a5df441d513530c3f62b1338617` | ancestor; 76 behind | clean |
| `perf/range-small-gap-coalescing` | `f915e27b2dc839f1139cdfced3e0d7a7c0c9851d` | ancestor; 73 behind | clean |
| `perf/shared-range-cache-substrate` | `6c3a6c5c4d2db7505c48b1716660a21e6df83fb1` | ancestor; 24 behind | clean |
| `perf/browser-parquet-cache-integration` | `38c287c25684f0bb24d07ddffc0b062cbeaeec31` | ancestor; 25 behind | clean |
| `perf/parquet-shared-range-cache` | `b11a44bbce3ac2b0299507dc83328baec1f57bba` | ancestor; 18 behind | clean |
| `perf/cache-readahead-metrics` | `30d7e8bdc729902a7a5f50cd3d30feb7efdd2348` | ancestor; 17 behind | clean |
| `perf/bounded-parquet-readahead` | `7681f1dfa5bdaaae3ff2ccff79cc8be76ec1503a` | ancestor; 11 behind | clean |
| `test/public-s3-cache-readahead-evidence` | `1278f6b866263a4512b1496814bd6444d27c3a86` | ancestor; 5 behind | clean |
| `feature/public-s3-live-support` | `087caee842dcda8f0e3e76b38644504b838a210b` | ancestor; 55 behind | clean |
| `perf/range-coalescing-telemetry` | `b28e7bb926ee0865d2faee6fe1e59f04a1b0febd` | 1 ahead/73 behind, but `git cherry origin/main` marks the sole commit patch-equivalent | clean |

## Superseded Clean Experiments

| Ref | Head and relation | Reason | Disposition |
| --- | --- | --- | --- |
| `browser-range-cache-substrate` | `0e80b44b0ba1997b7bfd0ecdea7ce8159cc0ae18`; 1 ahead/32 behind | First combined cache approach. The later landed substrate, Parquet integration, shared cache, metrics, and bounded-readahead chain supersedes its shape. | Clean archive candidate after owner confirmation; do not merge it into current main. |
| `perf/browser-query-s3-telemetry` | `f2296e5b19e6f45d1bff46f8c1143b77fef68791`; 1 ahead/43 behind | Docs-only evidence plan superseded by the pinned `s3-browser-perf-v1` manifest, validator, provenance, and current canonical record. | Clean archive candidate after owner confirmation. |
| `fix/streaming-audit-remediation` | `9e45d68736b12b574bc8d5b16a6661a354a003d3`; 8 ahead/0 behind | The lifecycle and cancel-generation patches are patch-equivalent on the active branch. Its unique cursor, full user-agent memory, host-evidence, and cancellation-authority proofs were also incorporated and rerun there. | Retain until `perf/resolve-performance-audit` is accepted, then treat as a clean archive candidate. |

## Research To Retain, Not Ship

`perf/wcrpc-value-experiment` is clean at
`2c01c2f3639ce1938b2c0b3bd72ec6f88fa014b3`, 9 commits ahead and
11 behind `origin/main`. It contains four committed raw JSON series totaling
67,220 lines.

The active resolution branch intentionally ports only the compact deterministic
analysis, current-main two-coordinator compatibility probe, and decision note.
The production decision remains inconclusive and WCRPC remains unjustified.
Keep this branch as historical research source until its raw evidence has an
explicit archive owner; do not merge the raw corpus into the product branch.

## Dirty User-Owned State

These locations must not be archived or deleted by performance cleanup:

- Root checkout `main` at `da2aa8786ec825e17f9400a8f732544dc5c9db40`
  is 4 ahead/73 behind and dirty. It contains modified runtime/conformance/perf
  files plus untracked plans, research, UAT, and `.DS_Store` paths.
- `perf/s3-browser-perf-fixture` at
  `b25f35a7165a0a6b2c3c67ea68d322423335d566` is 43 behind and has
  modified fixture/package/live-test/canonical-plan files plus an untracked
  generator and range-cache/readahead plan. The active branch implements a
  reproducible fixture independently; that does not authorize discarding this
  prototype.
- `perf/prebootstrap-pruning` at
  `1bc5cd0e31b841f00ac93c43a46e56d220e79836` is an ancestor 94
  commits behind, but its worktree has 14 modified SDK, worker, contract,
  runtime, and test files.
- `feature/local-delta-browser-cache` at
  `95926902a379586d1a76206036ce7352cf3826bf` is an ancestor 110
  commits behind, but its worktree has modified app, Playwright, local-Delta,
  and release documentation plus an untracked Playwright config.

## Recommended Cleanup Order

1. Land or otherwise retain the active resolution branch.
2. Confirm an owner and storage location for the WCRPC raw research.
3. Archive only clean, landed, patch-equivalent, or explicitly superseded
   worktrees.
4. Review every dirty worktree with its owner. Never infer disposability from an
   old branch tip when the worktree has uncommitted state.
