import { QueryClient, type QueryKey } from '@tanstack/react-query';
import {
  persistQueryClientRestore,
  persistQueryClientSave,
} from '@tanstack/react-query-persist-client';
import { IDBFactory, IDBObjectStore as FakeIDBObjectStore } from 'fake-indexeddb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { QueryTableSource } from '../services/query-source.ts';
import type { Catalog, CommitEntry, HistoryEntry, SavedQuery } from '../services/types.ts';
import { queryKeys } from './keys';
import {
  AXON_QUERY_CACHE_APP_VERSION,
  AXON_QUERY_CACHE_BUSTER,
  AXON_QUERY_CACHE_MAX_AGE_MS,
  AXON_QUERY_CACHE_SCHEMA_VERSION,
  createAxonQueryPersistOptions,
} from './persistence';

const manifestSource: QueryTableSource = {
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

const gcsSource: QueryTableSource = {
  kind: 'object_store_table_root',
  provider: 'gcs',
  catalogName: 'public-gcs',
  schemaName: 'main',
  tableName: 'events',
  tableUri: 'gs://public-bucket/events',
  storage: 'gs://public-bucket/events',
  region: 'us-central1',
};

const s3Source: QueryTableSource = {
  kind: 'object_store_table_root',
  provider: 's3',
  catalogName: 'public-s3',
  schemaName: 'main',
  tableName: 'events',
  tableUri: 's3://public-bucket/events',
  storage: 's3://public-bucket/events',
  region: 'us-east-1',
};

const localDeltaSource: QueryTableSource = {
  kind: 'local_delta',
  catalogName: 'local',
  schemaName: 'main',
  tableName: 'events',
  localRegistryId: 'registry-1',
  storage: 'browser-cache://events',
  region: 'browser-local',
};

function createTestClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        gcTime: AXON_QUERY_CACHE_MAX_AGE_MS,
        retry: false,
      },
    },
  });
}

function catalogData(name = 'catalog-a'): Catalog {
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

function commitEntry(): CommitEntry {
  return {
    v: 8,
    ts: '2026-01-01T00:00:00.000Z',
    op: 'WRITE',
    author: 'system',
    adds: 1,
    removes: 0,
    current: true,
    note: 'new data',
  };
}

function historyEntry(): HistoryEntry {
  return {
    id: 'history-1',
    time: '00:00:00',
    iso: '2026-01-01T00:00:00.000Z',
    ms: 12,
    rows: 3,
    status: 'ok',
    target: 'browser_wasm',
    fallback: null,
    sql: 'select * from events',
  };
}

function savedQuery(): SavedQuery {
  return {
    id: 'saved-1',
    name: 'Events',
    owner: 'you',
    edited: '12:00',
    target: 'browser_wasm',
    sql: 'select * from events',
  };
}

function emptyPersistedClient(buster = AXON_QUERY_CACHE_BUSTER) {
  return {
    timestamp: Date.now(),
    buster,
    clientState: { mutations: [], queries: [] },
  };
}

async function persistEntries(
  entries: ReadonlyArray<readonly [QueryKey, unknown]>,
  indexedDB = new IDBFactory(),
): Promise<QueryClient> {
  const sourceClient = createTestClient();
  for (const [queryKey, data] of entries) {
    sourceClient.setQueryData(queryKey, data);
  }

  const saveOptions = createAxonQueryPersistOptions({ indexedDB });
  await persistQueryClientSave({
    queryClient: sourceClient,
    persister: saveOptions.persister,
    buster: saveOptions.buster,
    dehydrateOptions: saveOptions.dehydrateOptions,
  });

  const restoreClient = createTestClient();
  const restoreOptions = createAxonQueryPersistOptions({ indexedDB });
  await persistQueryClientRestore({
    queryClient: restoreClient,
    persister: restoreOptions.persister,
    buster: restoreOptions.buster,
    maxAge: restoreOptions.maxAge,
  });
  return restoreClient;
}

describe('query cache persistence', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('restores allowed manifest catalog data into a fresh query client', async () => {
    const queryKey = queryKeys.catalog.tableDerived(manifestSource);
    const data = catalogData('manifest-catalog');

    const restored = await persistEntries([[queryKey, data]]);

    expect(restored.getQueryData(queryKey)).toEqual(data);
  });

  it('restores allowed public GCS and S3 catalog query families', async () => {
    const gcsKey = queryKeys.catalog.tableDerived(gcsSource);
    const s3Key = queryKeys.catalog.commits(s3Source);
    const gcsData = catalogData('gcs-catalog');
    const s3Data = [commitEntry()];

    const restored = await persistEntries([
      [gcsKey, gcsData],
      [s3Key, s3Data],
    ]);

    expect(restored.getQueryData(gcsKey)).toEqual(gcsData);
    expect(restored.getQueryData(s3Key)).toEqual(s3Data);
  });

  it('restores allowed local history and saved query cache entries', async () => {
    const historyKey = queryKeys.local.history();
    const savedKey = queryKeys.local.saved();
    const history = [historyEntry()];
    const saved = [savedQuery()];

    const restored = await persistEntries([
      [historyKey, history],
      [savedKey, saved],
    ]);

    expect(restored.getQueryData(historyKey)).toEqual(history);
    expect(restored.getQueryData(savedKey)).toEqual(saved);
  });

  it('does not persist failed queries or mutations', async () => {
    const indexedDB = new IDBFactory();
    const queryKey = queryKeys.catalog.tableDerived(manifestSource);
    const sourceClient = createTestClient();

    await expect(
      sourceClient.fetchQuery({
        queryKey,
        queryFn: async () => {
          throw new Error('catalog unavailable');
        },
      }),
    ).rejects.toThrow('catalog unavailable');

    const saveOptions = createAxonQueryPersistOptions({ indexedDB });
    expect(saveOptions.dehydrateOptions?.shouldDehydrateMutation?.({} as never)).toBe(false);
    await persistQueryClientSave({
      queryClient: sourceClient,
      persister: saveOptions.persister,
      buster: saveOptions.buster,
      dehydrateOptions: saveOptions.dehydrateOptions,
    });

    const restoreClient = createTestClient();
    const restoreOptions = createAxonQueryPersistOptions({ indexedDB });
    await persistQueryClientRestore({
      queryClient: restoreClient,
      persister: restoreOptions.persister,
      buster: restoreOptions.buster,
      maxAge: restoreOptions.maxAge,
    });

    expect(restoreClient.getQueryData(queryKey)).toBeUndefined();
  });

  it.each([
    [
      'local_delta catalog keys',
      queryKeys.catalog.tableDerived(localDeltaSource),
      catalogData('local-delta'),
    ],
    ['unknown query families', ['unknown', 'family'], { value: 'not allowed' }],
    ['run-result-shaped payloads', queryKeys.local.history(), { result: { rows: [] } }],
    ['descriptor payloads', queryKeys.local.saved(), { descriptor: { table: 'events' } }],
    ['object-list payloads', queryKeys.local.saved(), { objects: [{ path: '000.json' }] }],
    ['handle payloads', queryKeys.local.saved(), { fileHandle: { name: 'events' } }],
    ['grant payloads', queryKeys.local.saved(), { grantId: 'grant-1' }],
    ['token payloads', queryKeys.local.saved(), { accessToken: 'token-1' }],
    [
      'signed URL strings',
      queryKeys.local.saved(),
      { url: 'https://example.com/table?X-Amz-Signature=abc' },
    ],
  ])('does not restore %s', async (_label, queryKey, data) => {
    const restored = await persistEntries([[queryKey as QueryKey, data]]);

    expect(restored.getQueryData(queryKey as QueryKey)).toBeUndefined();
  });

  it('treats IndexedDB open failures as no-op persistence operations', async () => {
    const indexedDB = {
      open() {
        throw new Error('open failed');
      },
    } as unknown as IDBFactory;
    const options = createAxonQueryPersistOptions({ indexedDB });

    await expect(options.persister.persistClient(emptyPersistedClient())).resolves.toBeUndefined();
    await expect(options.persister.restoreClient()).resolves.toBeUndefined();
    await expect(options.persister.removeClient()).resolves.toBeUndefined();
  });

  it.each([
    ['write', 'put', 'persistClient'] as const,
    ['read', 'get', 'restoreClient'] as const,
    ['remove', 'delete', 'removeClient'] as const,
  ])('treats IndexedDB %s failures as no-op persistence operations', async (_label, method, op) => {
    vi.spyOn(FakeIDBObjectStore.prototype, method).mockImplementation(() => {
      throw new Error(`${method} failed`);
    });
    const options = createAxonQueryPersistOptions({ indexedDB: new IDBFactory() });

    if (op === 'persistClient') {
      await expect(
        options.persister.persistClient(emptyPersistedClient()),
      ).resolves.toBeUndefined();
    } else if (op === 'restoreClient') {
      await expect(options.persister.restoreClient()).resolves.toBeUndefined();
    } else {
      await expect(options.persister.removeClient()).resolves.toBeUndefined();
    }
  });

  it('treats unavailable IndexedDB as a no-op persistence layer', async () => {
    Object.defineProperty(globalThis, 'indexedDB', {
      configurable: true,
      value: undefined,
    });
    const options = createAxonQueryPersistOptions();

    await expect(
      options.persister.persistClient(emptyPersistedClient(options.buster)),
    ).resolves.toBeUndefined();
    await expect(options.persister.restoreClient()).resolves.toBeUndefined();
    await expect(options.persister.removeClient()).resolves.toBeUndefined();
  });

  it('uses non-empty schema and buster constants tied to the app/cache version', () => {
    expect(AXON_QUERY_CACHE_SCHEMA_VERSION).toBeGreaterThan(0);
    expect(AXON_QUERY_CACHE_APP_VERSION).toBe('axon-web@0.1.0');
    expect(AXON_QUERY_CACHE_BUSTER).toContain(AXON_QUERY_CACHE_APP_VERSION);
    expect(AXON_QUERY_CACHE_BUSTER).toContain(`v${AXON_QUERY_CACHE_SCHEMA_VERSION}`);
    expect(AXON_QUERY_CACHE_MAX_AGE_MS).toBe(6 * 60 * 60 * 1000);
  });
});
