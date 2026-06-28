# E0 M2F-M2H Run Client-Store PI Plan

## Summary

- Start from `origin/main` at `975c2ac75017ad9d5306a483db18cced16d59cc0`; root checkout is untrusted and currently `ahead 3, behind 2`.
- Create fresh worktree: `git fetch origin main`, then `git worktree add .worktrees/e0-m2-run-pi-session -b chore/e0-m2-run-pi-session origin/main`.
- Prior audit: `975c2ac`, `856edf6`, `6b71b3b`, `d6fac0e`, and `67f446f` are ancestors of `origin/main`; `apps/axon-web/src/state/slices` has no `run`, `session`, or `localAccess` slice.
- Baseline checks already passed on the clean M2E worktree: `npm test -- src/state src/editor/App.test.ts`, `npm run codegen:check`, and `bash tests/security/verify_browser_dependency_guardrails.sh`.

## Key Changes

- M2F: add a non-persisted `run` slice with `runState`, `resultData`, `resultPageRun`, `loadingMoreRows`, `metrics`, `events`, `plan`, and `capabilities`.
- M2F: add pure actions for start, elapsed update, event append, success, error, cancel, reset, local-reselect clear, load-more start, load-more success with stale-page discard, and load-more finish/error.
- Move `RunUiState` ownership from `editor/components/Results.tsx` to `state/slices/run.ts`; update `Results.tsx` to import the type only.
- Register `RunSlice` in `state/store.ts`, add selectors/actions in `state/hooks.ts`, and keep `partialize` limited to `layout`, `settings`, and `tabs`.
- M2G: replace local `App.tsx` run-related `useState` calls with store selectors/actions while keeping `AbortController`, interval handles, dynamic `import('../services/query.ts')`, history writes, toasts, catalog/commits/history/saved/query source, and query semantics in `App.tsx`.
- Preserve local Delta reselect, fallback-event filtering, `sameQueryResultPageRun` load-more guard, stale batch discard toast, `tabActions.markActiveClean`, cancel text, and history append on success/error.
- M2H: add `editor/components/RunResultsPanel.tsx` to subscribe to changing run result state. `App.tsx` should select only stable primitives such as `selectRunIsRunning` for topbar/editor wiring so the 80 ms elapsed timer does not rerender the whole shell.
- M2H verification note: the render boundary is verified by code structure. `App.tsx` subscribes to `selectRunIsRunning` and stable actions only; `RunResultsPanel.tsx` owns subscriptions for `runState`, results, pagination, metrics, events, and plan. Capability popover data stays in `App.tsx` as a direct store read so it does not add an unused results-panel subscription.

## Commit Plan

1. `chore(web): add run lifecycle client-store slice`
   - Files: this plan doc, `state/slices/run.ts`, `state/slices/run.test.ts`, `state/hooks.ts`, `state/store.ts`, and the type import update in `Results.tsx`.
2. `chore(web): wire run lifecycle through client store`
   - Files: `editor/App.tsx`, plus `editor/App.test.ts` only if a clean exported helper or existing test seam can cover the wiring without brittle UI rendering.
3. `perf(web): scope run timer updates to results state`
   - Files: `editor/App.tsx`, `editor/components/RunResultsPanel.tsx`, and focused selector/render-boundary tests if practical. Otherwise the selector boundary is documented here and verified by code structure.

## Test Plan

- If checks fail because fresh artifacts are missing, run `npm install` and `npm run build:wasm` in `apps/axon-web`, then retry.
- Slice 1 tests: `run.test.ts` covers initial state, run start, event append, metrics update, success, error, cancel, reset, load-more state, stale-page discard, and non-persistence.
- After each slice run:
  - `cd apps/axon-web && npm test -- src/state`
  - `cd apps/axon-web && npm exec -- tsc --noEmit`
  - `cd apps/axon-web && npm run lint`
  - `cd apps/axon-web && npm run format:check`
  - `cd apps/axon-web && npm run codegen:check`
- Final verification:
  - `cd apps/axon-web && npm run test:browser:editor-smoke`
  - `bash tests/security/verify_browser_dependency_guardrails.sh`
  - `git diff --check origin/main...HEAD`
  - `git status --short`

## Assumptions

- Do not push.
- Do not start M3: no TanStack Query migration for catalog, commits, history, saved, or query source.
- Do not introduce `session` or `localAccess` slices in this PI unless a tiny prerequisite is unavoidable; current inspection found neither exists.
- Do not persist run results, events, metrics, plans, capabilities, engine status, session handles, or local-access status.
- The render-scope fix is satisfied when elapsed updates are subscribed below `App.tsx`, while `App.tsx` uses stable boolean/status selectors and direct store reads for imperative callbacks where needed.
