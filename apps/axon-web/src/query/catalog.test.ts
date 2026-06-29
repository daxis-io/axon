import { QueryClient } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  clearQueryRuntimeState,
  publishQueryRuntimeState,
} from '../services/query-runtime-state.ts';
import type { QueryTableSource } from '../services/query-source.ts';
import type { Catalog } from '../services/types.ts';
import { catalogQueryOptions, commitsQueryOptions, installCatalogQueryBridge } from './catalog';
import { queryKeys } from './keys';

const source: QueryTableSource = {
  kind: 'manifest',
  catalogName: 'catalog-a',
  schemaName: 'schema-a',
  tableName: 'table-a',
  manifestUrl: '/manifest-a.json',
  storage: 'gs://bucket/table-a',
  region: 'browser-local',
  snapshot: 7,
  rows: 123,
  files: 4,
  size: '2 MB',
  protocol: 'r3/w7',
};

function runtimeCatalog(name = 'runtime-catalog'): Catalog {
  return {
    name,
    region: 'browser-local',
    storage: 'gs://bucket/table-a',
    tables: [
      {
        name: 'table-a',
        uri: 'gs://bucket/table-a',
        kind: 'delta',
        snapshot: 8,
        size_bytes: 2048,
        row_count: 456,
        file_count: 5,
        row_group_count: 2,
        partition_columns: [],
        protocol: {
          minReaderVersion: 3,
          minWriterVersion: 7,
          features: [],
        },
        columns: [],
      },
    ],
  };
}

describe('catalog query adapters', () => {
  beforeEach(() => {
    clearQueryRuntimeState();
  });

  afterEach(() => {
    clearQueryRuntimeState();
  });

  it('builds catalog query options with source-stable table-derived keys and summary initial data', async () => {
    const options = catalogQueryOptions(source);

    expect(options.queryKey).toEqual(queryKeys.catalog.tableDerived(source));
    expect(options.initialData).toMatchObject({
      name: 'catalog-a',
      region: 'browser-local',
      storage: 'gs://bucket/table-a',
      tables: [
        expect.objectContaining({
          name: 'table-a',
          snapshot: 7,
          row_count: 123,
          file_count: 4,
        }),
      ],
    });

    const client = new QueryClient();

    await expect(client.fetchQuery(options)).resolves.toEqual(options.initialData);
  });

  it('builds commits query options under the matching source key', async () => {
    const options = commitsQueryOptions(source);
    const client = new QueryClient();

    expect(options.queryKey).toEqual(queryKeys.catalog.commits(source));
    await expect(client.fetchQuery(options)).resolves.toEqual([]);
  });

  it('publishes runtime catalog data to the matching query key and invalidates commits once per client', () => {
    const client = new QueryClient();
    const invalidateQueries = vi.spyOn(client, 'invalidateQueries');
    const cleanupA = installCatalogQueryBridge(client);
    const cleanupB = installCatalogQueryBridge(client);
    const catalog = runtimeCatalog();

    publishQueryRuntimeState({ source, catalog, manifest: { objects: [] } }, 10);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toEqual(catalog);
    expect(invalidateQueries).toHaveBeenCalledTimes(1);
    expect(invalidateQueries).toHaveBeenCalledWith({
      queryKey: queryKeys.catalog.commits(source),
      exact: true,
    });

    cleanupA();
    publishQueryRuntimeState({ source, catalog: runtimeCatalog('still-installed') }, 11);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toEqual(
      runtimeCatalog('still-installed'),
    );
    expect(invalidateQueries).toHaveBeenCalledTimes(2);

    cleanupB();
    publishQueryRuntimeState({ source, catalog: runtimeCatalog('uninstalled') }, 12);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toEqual(
      runtimeCatalog('still-installed'),
    );
    expect(invalidateQueries).toHaveBeenCalledTimes(2);
  });
});
