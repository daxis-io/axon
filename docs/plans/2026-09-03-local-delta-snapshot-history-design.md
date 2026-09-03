# Local Delta Snapshot History Design

## Problem

The Snapshot tab shows commit history for the bundled sample but not for a user-selected local Delta table. The local table reader already validates and reads every available JSON commit while resolving the snapshot, but the commit-history query rejects every source except `manifest`. The same tab also labels `plan.files.length` as "Active files," so it shows an em dash before an execution plan contains file details even though resolved catalog metadata already has the snapshot file count.

## Selected design

Move Delta commit-action parsing and rollup into one pure service. Both manifest-backed logs and local `File` logs will provide `{ relativePath, text }` records to that service. The rollup will accept the resolved snapshot version, ignore JSON versions newer than a pinned snapshot, sort newest-first, and mark the resolved version as current. Missing historical JSON files remain a valid partial history; the UI reports only the commits that are actually available.

`LocalDeltaRuntime` will retain typed `CommitEntry` values, never raw log bytes or handles. A read-only local-runtime accessor will return the cached entries for an exact registry and resolved snapshot version. `loadCommits` will use those cached entries for a local source, while preserving the existing manifest fetch path. When query-session setup publishes runtime state it will also publish the local commit entries, causing the existing React Query bridge to refresh the commit query after local resolution. This supports both an immediately imported table and a persisted table reopened during execution without broadening generated data-access contracts.

The Snapshot UI will receive `tableMeta.file_count` as `tableFileCount` and render that value. Query-plan file details remain in the Plan tab. Empty commit history will use neutral source-independent copy: "No Delta commit history is available for this snapshot."

## Error and lifecycle behavior

Local log JSON validation remains authoritative in `local-delta.ts`: malformed selected logs still fail the local table import. Manifest history keeps its current best-effort behavior and skips commit objects that cannot be fetched. Commit entries are ordinary metadata and are cleared with the existing local runtime cache; no file bytes, blob URLs, credentials, or handles enter persisted catalog state.

## Verification

- Pure unit coverage for action rollup, newest-first ordering, add/remove counts, and pinned-snapshot filtering.
- Local-runtime coverage proving imported JSON commits become typed history.
- Commit-loader coverage proving runtime-published local commits are returned while manifest loading stays intact.
- Browser regression proving an uploaded table shows four commits and two active files in the Snapshot tab.
- Existing manifest fixture, local runtime, catalog query, full unit, lint, type/build, and focused Chromium checks.
