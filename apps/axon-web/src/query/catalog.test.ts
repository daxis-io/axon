import { QueryClient } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  clearQueryRuntimeState,
  getQueryRuntimeState,
  publishQueryRuntimeState,
} from '../services/query-runtime-state.ts';
import type { QueryTableSource } from '../services/query-source.ts';
import { snapshotCatalog } from '../services/catalog.ts';
import type { Catalog } from '../services/types.ts';
import {
  catalogQueryOptions,
  commitsQueryOptions,
  purgeCatalogSourceCache,
  purgeCatalogSourceCacheForError,
  installCatalogQueryBridge,
} from './catalog';
import {
  AXON_CATALOG_QUERY_STALE_TIME_MS,
  AXON_COMMITS_QUERY_STALE_TIME_MS,
  AXON_QUERY_GC_TIME_MS,
  shouldRetryQuery,
} from './client';
import { queryKeys } from './keys';

const catalogServiceMocks = vi.hoisted(() => ({
  loadCatalog: vi.fn(),
}));
const snapshotServiceMocks = vi.hoisted(() => ({
  loadCommits: vi.fn(),
}));

vi.mock('../services/catalog.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/catalog.ts')>();
  return {
    ...actual,
    loadCatalog: catalogServiceMocks.loadCatalog,
  };
});

vi.mock('../services/snapshot.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../services/snapshot.ts')>();
  return {
    ...actual,
    loadCommits: snapshotServiceMocks.loadCommits,
  };
});

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

const otherSource: QueryTableSource = {
  ...source,
  tableName: 'other-table',
  manifestUrl: '/manifest-other.json',
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
    catalogServiceMocks.loadCatalog.mockImplementation((querySource: QueryTableSource) =>
      Promise.resolve(snapshotCatalog(querySource)),
    );
    snapshotServiceMocks.loadCommits.mockResolvedValue([]);
  });

  afterEach(() => {
    clearQueryRuntimeState();
    catalogServiceMocks.loadCatalog.mockReset();
    snapshotServiceMocks.loadCommits.mockReset();
  });

  it('builds catalog query options with source-stable table-derived keys and summary initial data', async () => {
    const options = catalogQueryOptions(source);

    expect(options.queryKey).toEqual(queryKeys.catalog.tableDerived(source));
    expect(options.initialDataUpdatedAt).toBe(0);
    expect(options.staleTime).toBe(AXON_CATALOG_QUERY_STALE_TIME_MS);
    expect(options.gcTime).toBe(AXON_QUERY_GC_TIME_MS);
    expect(options.retry).toBe(shouldRetryQuery);
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
    expect(options.initialDataUpdatedAt).toBe(0);
    expect(options.staleTime).toBe(AXON_COMMITS_QUERY_STALE_TIME_MS);
    expect(options.gcTime).toBe(AXON_QUERY_GC_TIME_MS);
    expect(options.retry).toBe(shouldRetryQuery);
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

  it('purges source-scoped catalog cache without clearing local metadata or other sources', () => {
    const client = new QueryClient();
    const sourceCatalog = runtimeCatalog('source-catalog');
    const otherCatalog = runtimeCatalog('other-catalog');
    const history = [{ id: 'history-1' }];
    const saved = [{ id: 'saved-1' }];

    client.setQueryData(queryKeys.catalog.tableDerived(source), sourceCatalog);
    client.setQueryData(queryKeys.catalog.commits(source), [{ v: 1 }]);
    client.setQueryData(queryKeys.catalog.tableDerived(otherSource), otherCatalog);
    client.setQueryData(queryKeys.local.history(), history);
    client.setQueryData(queryKeys.local.saved(), saved);
    publishQueryRuntimeState({ source, catalog: sourceCatalog }, 10);

    purgeCatalogSourceCache(client, source);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.commits(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.tableDerived(otherSource))).toEqual(otherCatalog);
    expect(client.getQueryData(queryKeys.local.history())).toEqual(history);
    expect(client.getQueryData(queryKeys.local.saved())).toEqual(saved);
    expect(getQueryRuntimeState(source)).toBeUndefined();
  });

  it('purges source-scoped catalog cache for auth or session style failures only', () => {
    const client = new QueryClient();
    const sourceCatalog = runtimeCatalog('source-catalog');

    client.setQueryData(queryKeys.catalog.tableDerived(source), sourceCatalog);

    expect(purgeCatalogSourceCacheForError(client, source, { response: { status: 403 } })).toBe(
      true,
    );
    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toBeUndefined();

    client.setQueryData(queryKeys.catalog.tableDerived(source), sourceCatalog);
    expect(purgeCatalogSourceCacheForError(client, source, { status: 404 })).toBe(false);
    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toEqual(sourceCatalog);
  });

  it('purges source-scoped catalog cache when the catalog query fails with an auth or session status', async () => {
    const client = new QueryClient();
    const sourceCatalog = runtimeCatalog('source-catalog');
    const otherCatalog = runtimeCatalog('other-catalog');
    const error = { response: { status: 403 } };

    catalogServiceMocks.loadCatalog.mockRejectedValue(error);
    client.setQueryData(queryKeys.catalog.tableDerived(source), sourceCatalog);
    client.setQueryData(queryKeys.catalog.commits(source), [{ v: 1 }]);
    client.setQueryData(queryKeys.catalog.tableDerived(otherSource), otherCatalog);
    publishQueryRuntimeState({ source, catalog: sourceCatalog }, 10);

    const options = catalogQueryOptions(source);
    if (typeof options.queryFn !== 'function') {
      throw new Error('expected catalog query function');
    }

    await expect(
      options.queryFn({
        client,
        queryKey: options.queryKey,
        signal: new AbortController().signal,
        meta: undefined,
      }),
    ).rejects.toBe(error);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.commits(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.tableDerived(otherSource))).toEqual(otherCatalog);
    expect(getQueryRuntimeState(source)).toBeUndefined();
  });

  it('purges source-scoped catalog cache when the commits query fails with an auth or session status', async () => {
    const client = new QueryClient();
    const sourceCatalog = runtimeCatalog('source-catalog');
    const otherCatalog = runtimeCatalog('other-catalog');
    const error = { statusCode: 440 };

    snapshotServiceMocks.loadCommits.mockRejectedValue(error);
    client.setQueryData(queryKeys.catalog.tableDerived(source), sourceCatalog);
    client.setQueryData(queryKeys.catalog.commits(source), [{ v: 1 }]);
    client.setQueryData(queryKeys.catalog.tableDerived(otherSource), otherCatalog);
    publishQueryRuntimeState({ source, catalog: sourceCatalog }, 10);

    const options = commitsQueryOptions(source);
    if (typeof options.queryFn !== 'function') {
      throw new Error('expected commits query function');
    }

    await expect(
      options.queryFn({
        client,
        queryKey: options.queryKey,
        signal: new AbortController().signal,
        meta: undefined,
      }),
    ).rejects.toBe(error);

    expect(client.getQueryData(queryKeys.catalog.tableDerived(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.commits(source))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.tableDerived(otherSource))).toEqual(otherCatalog);
    expect(getQueryRuntimeState(source)).toBeUndefined();
  });
});
