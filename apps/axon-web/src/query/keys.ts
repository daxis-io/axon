import type { QuerySourceSelection, QueryTableSource } from '../services/query-source.ts';
import type { PublicObjectStorageProvider } from '../services/object-storage.ts';

export type QuerySourceIdentity =
  | readonly ['manifest', string, string, string, string]
  | readonly ['local_delta', string, string, string, string]
  | readonly ['object_store_table_root', PublicObjectStorageProvider, string];

export function querySourceIdentity(source: QueryTableSource): QuerySourceIdentity {
  if (source.kind === 'manifest') {
    return [
      'manifest',
      source.catalogName,
      source.schemaName,
      source.tableName,
      source.manifestUrl,
    ] as const;
  }

  if (source.kind === 'local_delta') {
    return [
      'local_delta',
      source.catalogName,
      source.schemaName,
      source.tableName,
      source.localRegistryId,
    ] as const;
  }

  return ['object_store_table_root', source.provider, source.tableUri] as const;
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
