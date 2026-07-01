# E0 M4 Routing And Catalog Navigation PI Session

## Baseline

- Worktree: `.worktrees/e0-m4-routing-catalog-navigation`.
- Branch: `chore/e0-m4-routing-catalog-navigation`.
- Remote baseline: `origin/main` at `087caee842dcda8f0e3e76b38644504b838a210b`.
- The root checkout is not part of the implementation or verification path.

## Scope

- Replace the editor's custom History API route switch with TanStack Router.
- Preserve `/` and `/connect` behavior.
- Add `/catalogs`, `/catalog/$catalogId/$schemaName/$tableName`, and `/saved/$savedId`.
- Make catalog table and saved query routes drive the existing editor state.
- Keep connect mutation side effects, query execution, auth/provider work, persisted query cache, and worker/session ownership out of scope.

## Commit Slices

1. `chore(web): add router foundation`
   - Add `@tanstack/react-router`.
   - Replace `src/editor/router.ts` with a TanStack Router setup.
   - Add route/path helpers and pure route tests.
   - Render `AppRouter` from `main.tsx` inside existing `AppProviders`.

2. `chore(web): add catalogs route`
   - Add `CatalogsPage`.
   - Use connected-catalog state and route helper navigation.
   - Keep `/connect` mutation side effects unchanged.

3. `chore(web): wire catalog table deep links`
   - Resolve route params to connected table refs.
   - Select valid route tables through `connectionActions.selectTable`.
   - Render invalid table routes as an empty state linking to `/catalogs`.
   - Make sidebar and connected-panel table picks navigate.

4. `chore(web): wire saved query deep links`
   - Resolve `/saved/$savedId` from `savedQueriesQueryOptions`.
   - Add tab open/reuse behavior for saved queries.
   - Persist only the saved-query id with existing tab fields.
   - Render missing saved ids as an empty state.

## Verification

- Before edits:
  - `npm test -- src/query src/state`
  - `npm exec -- tsc --noEmit`
  - `npm run codegen:check`
- After slices:
  - `npm test -- src/editor src/query src/state`
  - `npm exec -- tsc --noEmit`
  - `npm run lint`
  - `npm run format:check`
- Final:
  - `npm run codegen:check`
  - `npm run test:browser:editor-smoke`
  - `bash tests/security/verify_browser_dependency_guardrails.sh`
  - `git diff --check origin/main...HEAD`
  - `git status --short`
