import { describe, expect, it } from 'vitest';
import * as QuerySourceModule from './query-source.ts';
import type {
  ActiveConnectedTableRef,
  QueryCatalogCandidate,
  QuerySourceSelection,
} from './query-source.ts';
import { SAMPLE_QUERY_SOURCE, querySourcesForCatalog } from './query-source.ts';

type ResolveQuerySourceSelection = (
  catalogs: QueryCatalogCandidate[],
  selectedRef?: ActiveConnectedTableRef,
) => QuerySourceSelection;

function resolveSelection(
  catalogs: QueryCatalogCandidate[],
  selectedRef?: ActiveConnectedTableRef,
): QuerySourceSelection | undefined {
  const resolve = (
    QuerySourceModule as typeof QuerySourceModule & {
      resolveQuerySourceSelection?: ResolveQuerySourceSelection;
    }
  ).resolveQuerySourceSelection;
  return resolve?.(catalogs, selectedRef);
}

function catalog({
  id = 'workspace',
  alias = 'Workspace',
  kind = 'object_store',
  provider = 'gcs',
  schema = 'default',
  table = 'events',
  manifestUrl = '/manifests/events.json',
  uri,
  localRegistryId,
  storage,
  region,
}: {
  id?: string;
  alias?: string;
  kind?: string;
  provider?: string;
  schema?: string;
  table?: string;
  manifestUrl?: string;
  uri?: string;
  localRegistryId?: string;
  storage?: string;
  region?: string;
} = {}): QueryCatalogCandidate {
  return {
    id,
    alias,
    kind,
    provider,
    storage: storage ?? uri ?? 'gs://workspace/events',
    region: region ?? (provider === 's3' ? 'us-east-2' : 'us-central1'),
    schemas: [
      {
        name: schema,
        tables: [
          {
            name: table,
            manifestUrl,
            uri,
            localRegistryId,
          },
        ],
      },
    ],
  };
}

function ref(
  catalogId = 'workspace',
  schemaName = 'default',
  tableName = 'events',
): ActiveConnectedTableRef {
  return { catalogId, schemaName, tableName };
}

describe('resolveQuerySourceSelection', () => {
  it('exports the authoritative resolver', () => {
    expect(resolveSelection([], undefined)).toEqual({
      kind: 'unavailable',
      reason: 'empty',
    });
  });

  it('does not choose a queryable table when the explicit selection is missing', () => {
    expect(resolveSelection([catalog()], undefined)).toEqual({
      kind: 'unavailable',
      reason: 'missing',
    });
  });

  it('does not replace a stale selection with another table', () => {
    const stale = ref('removed-catalog');

    expect(resolveSelection([catalog()], stale)).toEqual({
      kind: 'unavailable',
      reason: 'stale',
      ref: stale,
    });
  });

  it('reports an exactly selected but unsupported table as unqueryable', () => {
    const selected = ref();
    const unqueryable = catalog({ manifestUrl: '', uri: 's3://wrong-provider/events' });

    expect(resolveSelection([unqueryable], selected)).toEqual({
      kind: 'unavailable',
      reason: 'unqueryable',
      ref: selected,
    });
  });

  it('returns sample only for the explicitly selected sample fixture table', () => {
    const sampleRef = ref('sample-lake-fixture', 'prod_like', 'events');
    const sample = catalog({
      id: sampleRef.catalogId,
      alias: SAMPLE_QUERY_SOURCE.catalogName,
      schema: sampleRef.schemaName,
      table: sampleRef.tableName,
      manifestUrl: SAMPLE_QUERY_SOURCE.manifestUrl,
      storage: SAMPLE_QUERY_SOURCE.storage,
      region: SAMPLE_QUERY_SOURCE.region,
    });

    expect(resolveSelection([sample], sampleRef)).toEqual({
      kind: 'sample',
      ref: sampleRef,
      source: expect.objectContaining(SAMPLE_QUERY_SOURCE),
    });
  });

  it('treats a non-fixture catalog with copied sample source fields as a resource', () => {
    const selected = ref('connected-copy', 'prod_like', 'events');
    const copiedSample = catalog({
      id: selected.catalogId,
      alias: SAMPLE_QUERY_SOURCE.catalogName,
      schema: selected.schemaName,
      table: selected.tableName,
      manifestUrl: SAMPLE_QUERY_SOURCE.manifestUrl,
      storage: SAMPLE_QUERY_SOURCE.storage,
      region: SAMPLE_QUERY_SOURCE.region,
    });

    expect(resolveSelection([copiedSample], selected)).toMatchObject({
      kind: 'resource',
      ref: selected,
    });
  });

  it.each([
    {
      label: 'local Delta',
      candidate: catalog({ manifestUrl: '', localRegistryId: 'local-events' }),
      expected: { kind: 'local_delta', localRegistryId: 'local-events' },
    },
    {
      label: 'public GCS',
      candidate: catalog({ manifestUrl: '', uri: 'gs://workspace/events' }),
      expected: {
        kind: 'object_store_table_root',
        provider: 'gcs',
        tableUri: 'gs://workspace/events',
      },
    },
    {
      label: 'public S3',
      candidate: catalog({
        provider: 's3',
        manifestUrl: '',
        uri: 's3://workspace/events',
      }),
      expected: {
        kind: 'object_store_table_root',
        provider: 's3',
        tableUri: 's3://workspace/events',
      },
    },
  ])('resolves the exact selected $label resource', ({ candidate, expected }) => {
    expect(resolveSelection([candidate], ref())).toEqual({
      kind: 'resource',
      ref: ref(),
      source: expect.objectContaining(expected),
    });
  });
});

describe('querySourcesForCatalog', () => {
  it('extracts queryable table sources from a connected catalog', () => {
    const catalog: QueryCatalogCandidate = {
      id: 'workspace',
      alias: 'Workspace',
      kind: 'object_store',
      provider: 'gcs',
      storage: 'gs://workspace',
      region: 'us',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'manifested',
              manifestUrl: '/manifests/manifested.json',
              source: {
                storage: 'gs://workspace/manifested',
                region: 'us',
              },
            },
            {
              name: 'rooted',
              uri: 'gs://workspace/rooted',
            },
            {
              name: 'local',
              localRegistryId: 'local-registry',
              source: {
                storage: 'browser-cache://local',
                region: 'browser-local',
              },
            },
            {
              name: 'not-queryable',
              uri: 's3://wrong-provider/not-queryable',
            },
          ],
        },
      ],
    };

    const sources = querySourcesForCatalog(catalog);

    expect(sources).toHaveLength(3);
    expect(sources).toEqual([
      expect.objectContaining({
        kind: 'manifest',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'manifested',
        manifestUrl: '/manifests/manifested.json',
        storage: 'gs://workspace/manifested',
        region: 'us',
      }),
      expect.objectContaining({
        kind: 'object_store_table_root',
        provider: 'gcs',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'rooted',
        tableUri: 'gs://workspace/rooted',
        storage: 'gs://workspace/rooted',
        region: 'us',
      }),
      expect.objectContaining({
        kind: 'local_delta',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'local',
        localRegistryId: 'local-registry',
        storage: 'browser-cache://local',
        region: 'browser-local',
      }),
    ]);
  });
});
