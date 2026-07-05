# E0 M5 Cache Policy Cleanup PI Session

## Baseline

- Worktree: `.worktrees/e0-m5-cache-policy-pi`.
- Branch: `chore/e0-m5-cache-policy-pi`.
- Remote baseline: `origin/main` at `b25f35a7165a0a6b2c3c67ea68d322423335d566`.
- The root checkout is dirty/diverged and is not part of implementation or verification.
- Preflight confirmed `origin/main` contains:
  - `62a9b9c` M4 routing/catalog navigation.
  - `b256e14` M5 first query-cache persistence.
  - Settings surface commits through `b25f35a`.
- Preflight confirmed these files exist on `origin/main`:
  - `apps/axon-web/src/editor/router.tsx`
  - `apps/axon-web/src/editor/SettingsPage.tsx`
  - `apps/axon-web/src/query/persistence.ts`
  - `apps/axon-web/src/query/persistence.test.ts`
  - `apps/axon-web/src/editor/AppProviders.tsx`
  - `apps/axon-web/src/query/client.ts`

## Scope

- Finalize E0 M5 query-cache policy, source-scoped catalog purge hooks, migrated server-state cleanup, and boundary docs.
- Keep TanStack Query as the in-memory server-state authority.
- Persist only safe query families through the current IndexedDB persister.
- Keep persisted cache lifetime aligned with `AXON_QUERY_CACHE_MAX_AGE_MS`.
- Do not introduce durable auth/session state.
- Do not start E1 catalog providers, E3A provider messages, UC auth, Delta Sharing auth, Monaco, Arrow grid work, server/proxy work, or provider execution work.

## Commit Slices

1. `chore(web): tune query cache policy`
   - Files:
     - `apps/axon-web/src/query/client.ts`
     - `apps/axon-web/src/query/client.test.ts`
     - `apps/axon-web/src/query/catalog.ts`
     - `apps/axon-web/src/query/catalog.test.ts`
     - `apps/axon-web/src/query/local.ts`
     - `apps/axon-web/src/query/local.test.ts`
     - `apps/axon-web/src/query/persistence.ts`
     - `apps/axon-web/src/query/persistence.test.ts`
   - Add/tighten per-resource `staleTime`, `gcTime`, and retry behavior for catalog, commits, history, and saved-query adapters.
   - Keep `shouldPersistAxonQuery` as the single persisted-cache allow/deny policy.
   - Prove only cheap successful catalog/local metadata can persist.
   - Do not widen persisted keys or allow sensitive fields.
   - Tests:
     - `cd apps/axon-web && npm test -- src/query`

2. `chore(web): purge catalog cache on source discard`
   - Files:
     - `apps/axon-web/src/query/catalog.ts`
     - `apps/axon-web/src/query/catalog.test.ts`
     - `apps/axon-web/src/query/index.ts`
     - `apps/axon-web/src/services/query-source.ts`
     - `apps/axon-web/src/services/query-source.test.ts`
     - `apps/axon-web/src/state/slices/connections.ts`
     - `apps/axon-web/src/state/slices/connections.test.ts`
     - `apps/axon-web/src/editor/App.tsx`
     - `apps/axon-web/src/editor/ConnectPage.tsx`
     - `apps/axon-web/src/editor/ConnectPage.test.ts`
   - Add source extraction for discarded connected catalogs.
   - Add a narrow helper that removes source-scoped catalog query cache and clears matching runtime catalog state.
   - Treat auth/session-style failures as source discard triggers without storing auth/session state.
   - Do not purge local history or saved queries.
   - Tests:
     - `cd apps/axon-web && npm test -- src/query src/services/query-source.test.ts src/state/slices/connections.test.ts src/editor/ConnectPage.test.ts`

3. `chore(web): clean up migrated server-state paths`
   - Files:
     - `apps/axon-web/src/services/catalog.ts`
     - `apps/axon-web/src/services/snapshot.ts`
     - `apps/axon-web/src/services/query.ts`
   - Remove only unused singleton subscriber exports left behind by M3/M4/M5 migration.
   - Preserve legitimate query adapter bridges and `subscribeEngineStatus`.
   - Static proof:
     - `rg "subscribeCatalog|subscribeCommits|subscribeSession" apps/axon-web/src`
   - Tests:
     - `cd apps/axon-web && npm test -- src/query src/editor/App.test.ts`

4. `docs(web): document query cache boundaries`
   - Files:
     - `apps/axon-web/src/query/README.md`
     - `docs/plans/2026-07-05-e0-m5-cache-policy-cleanup-pi-session.md`
   - Document persisted-query-cache contract:
     - allowed keys
     - denied fields/shapes
     - `local_delta` exclusion
     - signed URL/token/descriptor rejection
     - maxAge/buster
     - IndexedDB-unavailable behavior
     - source-scoped purge behavior
   - Record final verification commands and any environment limits.

## Completed Slice Commits

- `02c282a chore(web): tune query cache policy`
- `68dee7e chore(web): purge catalog cache on source discard`
- `b6d2c91 chore(web): clean up migrated server-state paths`
- Slice 4 is this docs commit.

## Verification

Install/build prerequisites in the worktree as needed:

```bash
cd apps/axon-web && npm install
cd apps/axon-web && npm run build:wasm
cd apps/axon-web && npm run build:fixture
```

Final checks:

```bash
cd apps/axon-web && npm test -- src/query src/editor src/state src/settings
cd apps/axon-web && npm run lint
cd apps/axon-web && npm run format:check
cd apps/axon-web && npm exec -- tsc --noEmit
cd apps/axon-web && npm run codegen:check
cargo build -p browser-engine-worker --target wasm32-unknown-unknown --release --locked
bash tests/security/verify_browser_dependency_guardrails.sh
cd apps/axon-web && npm run dev -- --host 127.0.0.1 --port 5174
cd apps/axon-web && PLAYWRIGHT_BASE_URL=http://127.0.0.1:5174 npm run test:browser:editor-smoke
git diff --check origin/main...HEAD
git status --short
git log --oneline --reverse origin/main..HEAD
```

Stop the Vite process after browser smoke.

Known environment limits observed in this worktree:

- Fresh `apps/axon-web` dependencies were installed with `npm install`; npm reported existing audit findings outside this M5 scope.
- `npm exec -- tsc --noEmit` requires generated WASM bindings first; run `npm run build:wasm` before treating missing `src/wasm/axon_web_wasm` as a product regression.
- Browser smoke requires a Vite process bound to `127.0.0.1:5174`, then that process must be stopped after Playwright completes.

## Exit Gates

- Four commits exist with the requested commit messages.
- `git diff --check origin/main...HEAD` exits clean.
- `git status --short` is clean in the worktree.
- Final response lists checks passed, checks skipped with reasons, and the commit list.
- Do not push.
