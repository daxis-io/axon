# Issue #132: Catalog Run Index Execution Plan

## Context

GitHub issue daxis-io/arco#132 reports a downstream Daxis catalog-read path that cold-scans every canonical authoritative run JSON object under `authoritative-runs/runs/{run_id}.json`, then filters by organization and target table in memory. Cache reuse reduces repeated calls, but a cold cache remains O(total runs) and gets worse as unrelated organizations and tables accumulate.

Arco does not currently use that flat object layout. The orchestration compactor writes tenant/workspace-scoped state under the scoped storage prefix and publishes a manifest pointer to selected Parquet artifacts. That avoids bucket-wide listing for Arco's native system table reads, but it still does not publish an org/table-addressable read model for catalog consumers that need to answer "latest authoritative runs for this org/table" without enumerating every run.

## Goal

Add an Arco-owned catalog run projection that is:

- scoped by the existing tenant/workspace storage boundary;
- selected by the orchestration manifest pointer, not by prefix discovery;
- derived from canonical orchestration run/task state, not a second source of truth;
- queryable by catalog readers through a stable system table surface;
- cheap to cold-read for a single tenant/workspace and filter by namespace/table.

## Non-Goals

- Do not recreate the downstream Daxis `authoritative-runs/runs/{run_id}.json` layout in Arco.
- Do not make catalog readers scan the event log or all run artifacts on demand.
- Do not persist new cache-only state that can diverge from the orchestration fold.
- Do not expose L0-only rows as authoritative system-table state until they are selected by a published base manifest.

## Proposed Design

### Projection

Introduce a new orchestration compactor projection table:

- Parquet artifact: `catalog_run_index.parquet`
- System table: `system.orchestration.catalog_runs`
- Fold state map: keyed by `(target_namespace, target_table, run_id, task_key)`

The projection should contain one row per catalog-addressable run/task target. It is a read model over existing canonical state. Rebuilding it from the fold must be deterministic.

Initial row shape:

- `tenant_id`
- `workspace_id`
- `target_namespace`
- `target_table`
- `run_id`
- `task_key`
- `run_state`
- `task_state`
- `asset_key`
- `partition_key`
- `materialization_id`
- `delta_table`
- `delta_version`
- `delta_partition`
- `execution_lineage_ref`
- `reference_id`
- `source_type`
- `created_at`
- `updated_at`
- `row_version`

`target_namespace` and `target_table` should be derived conservatively:

- Prefer explicit run labels when present:
  - `arco.catalog.namespace`
  - `arco.catalog.table`
  - `arco.catalog.reference_id`
  - `arco.catalog.source_type`
- Fall back to a task `asset_key` split at the last `.` only when that produces both a namespace and table.
- Omit rows with no catalog target rather than publishing ambiguous entries.

### Lifecycle Updates

The fold should refresh catalog index rows for a run after events that can change the public catalog read model:

- run creation/trigger labels and timestamps;
- task definition or asset target creation;
- task start/finish state changes;
- output visibility/materialization updates;
- run completion/cancellation state changes.

Refreshing should remove stale rows for the run before inserting the latest derived rows. This keeps target changes, retry state, and materialization metadata consistent without per-event hand-maintained partial updates.

### Manifest and System Table Boundary

The compactor should publish `catalog_run_index.parquet` alongside the existing base snapshot tables and include it in:

- `TablePaths`
- `RowCounts`
- `write_state_parquet`
- base-state loading
- L0-state loading
- base/delta merge
- delta computation
- empty-delta detection
- golden schema contracts

The API should expose only the manifest-selected base artifact as `system.orchestration.catalog_runs`, matching the current system-table visibility boundary. L0-only updates must not appear through the API until compaction publishes a new base snapshot.

## Execution Slices

1. Contract tests first
   - Add a golden Parquet schema for `catalog_run_index.parquet`.
   - Add flow-level tests that build orchestration state with catalog-targeted tasks and assert the projected rows.
   - Add a system-table-path test that verifies the manifest-selected base table includes `catalog_runs`.
   - Add an API test that queries `system.orchestration.catalog_runs` from a published base and preserves the existing L0-only fail-closed behavior.

2. Projection implementation
   - Add `CatalogRunIndexRow` and the fold-state map.
   - Add deterministic refresh helpers that derive rows from `RunRow` and `TaskRow`.
   - Wire refresh calls into relevant fold event handlers.
   - Add merge/delta helpers for the projection.

3. Storage and schema implementation
   - Add Parquet write/read support and schema contract output.
   - Add manifest `TablePaths` and `RowCounts` fields.
   - Include the table in compactor base and L0 artifact writes/reads.

4. API exposure
   - Add `catalog_runs` to the orchestration system-table allowlist.
   - Register the manifest-selected Parquet artifact through the existing system table provider path.
   - Document the table in the system catalog reference.

5. Verification
   - Run focused `arco-flow` orchestration compactor tests.
   - Run focused `arco-api` system table tests.
   - Run schema contract tests.
   - Inspect `git diff` for scope and accidental unrelated changes.

## Risks and Guardrails

- Schema churn: keep the first schema explicit and versioned through the existing golden-schema workflow.
- Scope confusion: use `tenant_id`/`workspace_id` as Arco's native org scope and avoid adding a parallel org identifier unless a caller contract requires it.
- Cold-read regression: system-table registration must use manifest table paths, not object listing.
- Read-after-write semantics: preserve the current base snapshot boundary for API visibility until the product explicitly chooses to expose L0 overlay reads.
- Ambiguous targets: do not publish a catalog row if target namespace/table cannot be derived deterministically.

## Acceptance Criteria

- A catalog-targeted run/task produces a `catalog_runs` system-table row after base compaction.
- The row includes tenant/workspace scope, target namespace/table, run/task state, and latest materialization metadata.
- Querying `system.orchestration.catalog_runs` does not require listing all run artifacts.
- Existing orchestration system-table L0-only visibility semantics remain unchanged.
- Schema contracts and focused orchestration/API tests pass.
