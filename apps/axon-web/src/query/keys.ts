import type {
  ActiveConnectedTableRef,
  AvailableQuerySourceSelection,
  QuerySourceIdentity,
  QuerySourceSelection,
  QueryTableSource,
} from '../services/query-source.ts';
import { querySourceIdentity } from '../services/query-source.ts';

export { querySourceIdentity };
export type { QuerySourceIdentity };

export type SelectedQuerySourceIdentity = Readonly<{
  kind: AvailableQuerySourceSelection['kind'];
  ref: Readonly<ActiveConnectedTableRef>;
  source: QuerySourceIdentity;
  snapshotVersion: number | null;
}>;

export function selectedQuerySourceIdentity(
  selection: AvailableQuerySourceSelection,
  snapshotVersion?: number,
): SelectedQuerySourceIdentity {
  return {
    kind: selection.kind,
    ref: {
      catalogId: selection.ref.catalogId,
      schemaName: selection.ref.schemaName,
      tableName: selection.ref.tableName,
    },
    source: querySourceIdentity(selection.source),
    snapshotVersion: snapshotVersion ?? null,
  };
}

function catalogRootKey() {
  return ['catalog'] as const;
}

function catalogSourceKey(source: QueryTableSource) {
  return [...catalogRootKey(), 'source', querySourceIdentity(source)] as const;
}

export const queryKeys = {
  catalog: {
    root: catalogRootKey,
    source: catalogSourceKey,
    tableDerived: (source: QueryTableSource) =>
      [...catalogSourceKey(source), 'table-derived'] as const,
    commits: (source: QueryTableSource) => [...catalogSourceKey(source), 'commits'] as const,
    unavailable: (
      selection: Extract<QuerySourceSelection, { kind: 'unavailable' }>,
      resource: 'catalog' | 'commits',
    ) =>
      [
        ...catalogRootKey(),
        'unavailable',
        resource,
        selection.reason,
        selection.ref?.catalogId ?? null,
        selection.ref?.schemaName ?? null,
        selection.ref?.tableName ?? null,
      ] as const,
  },
  local: {
    root: () => ['local'] as const,
    history: () => [...queryKeys.local.root(), 'history'] as const,
    saved: () => [...queryKeys.local.root(), 'saved'] as const,
  },
} as const;
