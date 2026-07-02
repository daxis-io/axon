# E0 M4B Settings Surface PI Session

## Baseline

- Worktree: `.worktrees/e0-m4-settings-surface`.
- Branch: `chore/e0-m4-settings-surface`.
- Remote baseline: `origin/main` at `bd706be553085500e775074e89a2ce6d625ea066`.
- The root checkout is dirty and is not part of the implementation or verification path.
- Pre-edit verification:
  - `npm test -- src/editor src/query src/state src/settings`
  - `npm run build:wasm`
  - `npm exec -- tsc --noEmit`
  - `npm run codegen:check`

## Scope

- Add a routed `/settings` surface for the existing honored settings state.
- Preserve workspace query execution, tabs, connected catalogs, run lifecycle, engine status, and TanStack server-state behavior.
- Keep persisted client state limited to existing `layout`, `settings`, and `tabs`.
- Replace the floating Tweaks panel only after the routed settings surface has equivalent controls.
- Do not add new settings fields, AJV, JSON Forms, auth/provider work, router rewrites beyond `/settings`, or durable query-cache persistence.

## Commit Slices

1. `chore(web): add settings route shell`
   - Extend editor route templates and href typing with `/settings`.
   - Add a lazy settings route and compact page shell.
   - Add settings navigation from workspace, connect, and catalogs chrome.
   - Keep `TweaksPanel` mounted and functional.

2. `chore(web): add settings validation model`
   - Add `src/settings/settings-model.ts`.
   - Add tests for valid partial patches, invalid JSON, sanitization, unsupported-key stripping, and browser-only execution target coercion.
   - Reuse existing settings guards, defaults, and generated enum/default mappings.

3. `chore(web): add settings controls`
   - Add Appearance, Typography, Execution, and Advanced raw JSON sections.
   - Read settings from store selectors and write through `settingsActions`.
   - Keep raw JSON in local draft state until Apply, validate first, then apply via `updateAppearance` and `updateExecution`.

4. `chore(web): replace tweaks panel with settings surface`
   - Remove `TweaksPanel` usage and source.
   - Preserve keyboard save/query/run behavior.
   - Update browser smoke coverage to open `/settings`, change appearance, reload, and verify persistence.

## Verification

- After each slice:
  - `npm test -- src/settings src/state src/editor`
  - `npm exec -- tsc --noEmit`
  - `npm run lint`
  - `npm run format:check`
- Final:
  - `npm run codegen:check`
  - `npm run test:browser:editor-smoke`
  - `bash tests/security/verify_browser_dependency_guardrails.sh`
  - `git diff --check origin/main...HEAD`
  - `git status --short`
