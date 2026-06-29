# E0 M3 Server State PI Session

## Baseline Audit

- Source of truth: `origin/main` at `76f974cef7a2737533d523eb261883f24ec51406`.
- Worktree: `.worktrees/e0-m3-server-state-pi-session` on `chore/e0-m3-server-state-pi-session`.
- Root checkout is divergent and is not proof for this slice.
- Prior E0 M2 work is present on the baseline:
  - `apps/axon-web/src/state/slices/run.ts` exists.
  - Client persistence remains limited to layout, settings, and tabs; run state is not persisted.
- M3 server-state work is not present on the baseline:
  - No `apps/axon-web/src/query/catalog.ts`.
  - No `apps/axon-web/src/query/local.ts`.
  - `apps/axon-web/src/editor/App.tsx` still imports catalog subscriptions and local metadata persistence directly.

## Scope

### Slice 1: `chore(web): add catalog query adapters`

Files:

- `apps/axon-web/src/query/keys.ts`
- `apps/axon-web/src/query/keys.test.ts`
- `apps/axon-web/src/query/catalog.ts`
- `apps/axon-web/src/query/catalog.test.ts`
- `apps/axon-web/src/editor/AppProviders.tsx`
- `docs/plans/2026-06-29-e0-m3-server-state-pi-session.md`

Work:

- Add deterministic source identity keys derived from the same identity as `sameQuerySource`.
- Add catalog root/source/table-derived/commits keys.
- Add local root/history/saved keys.
- Add catalog and commits query options.
- Install a StrictMode-safe runtime bridge from `AppProviders.tsx` that writes runtime catalog descriptors to TanStack Query and invalidates the matching commits query.

### Slice 2: `chore(web): read catalog and commits from query cache`

Files:

- `apps/axon-web/src/editor/App.tsx`
- Existing focused tests as required.

Work:

- Replace direct catalog loads/subscriptions in `App.tsx` with `useQuery(catalogQueryOptions(querySource))` and `useQuery(commitsQueryOptions(querySource))`.
- Remove direct `App.tsx` usage of `loadCatalog`, `subscribeCatalog`, `snapshotCatalog`, and `subscribeCommits`.
- Keep query execution, run store, engine store, connections, tabs, local Delta reselect, toasts, and UI behavior unchanged.

### Slice 3: `chore(web): add local metadata query adapters`

Files:

- `apps/axon-web/src/query/local.ts`
- `apps/axon-web/src/query/local.test.ts`

Work:

- Wrap `loadHistory`, `appendHistory`, `loadSaved`, `saveQuery`, and `deleteSaved`.
- Preserve legacy metadata migration behavior in the existing persistence modules.
- Add query options plus cache-write mutation helpers.
- Update QueryClient data from returned entries so fallback metadata writes still update in-memory UI data.

### Slice 4: `chore(web): read local metadata from query cache`

Files:

- `apps/axon-web/src/editor/App.tsx`
- Existing focused tests as required.

Work:

- Replace local `history` and `saved` state with local metadata query data.
- Route successful and failed query-run history writes through the local query helper path.
- Route SaveDialog writes through the saved query helper path.
- Remove direct `App.tsx` usage of `loadHistory`, `loadSaved`, `appendHistory`, and `saveQuery`.

## Non-Goals

- No push.
- No `persistQueryClient`.
- No TanStack Router, UC, Delta Sharing, provider, auth, E1, worker/session handle, or `localAccess` work.
- No new dependency.
- Commits, runtime catalog descriptors, signed URLs, grants, run results, and openable handles remain memory-only.

## Verification

Baseline gates:

- `cd apps/axon-web && npm test -- src/query src/state src/editor/App.test.ts`
- `cd apps/axon-web && npm run codegen:check`

After each slice:

- `cd apps/axon-web && npm test -- src/query src/state`
- `cd apps/axon-web && npm exec -- tsc --noEmit`
- `cd apps/axon-web && npm run lint`
- `cd apps/axon-web && npm run format:check`

Final gates:

- `cd apps/axon-web && npm run codegen:check`
- `cd apps/axon-web && npm run test:browser:editor-smoke`
- `bash tests/security/verify_browser_dependency_guardrails.sh`
- `rg "subscribeCatalog|subscribeCommits|snapshotCatalog|loadHistory|loadSaved" apps/axon-web/src/editor/App.tsx`
- `git diff --check origin/main...HEAD`
- `git status --short`

If the fresh worktree lacks dependencies or generated WASM bindings, run `npm install` and `npm run build:wasm` in `apps/axon-web`, then retry the failed gate.
