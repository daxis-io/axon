import { create } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  TableNodeSchema,
  TableType,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  CanonicalResourceRefSchema,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import { createPublicObjectStorageCanonicalTable } from './canonical-table-identity.ts';
import * as QuerySourceModule from './query-source.ts';
import type {
  ActiveConnectedTableRef,
  QueryCatalogCandidate,
  QuerySourceSelection,
} from './query-source.ts';
import {
  SAMPLE_QUERY_SOURCE,
  querySourceIdentity,
  querySourcesForCatalog,
} from './query-source.ts';

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
  catalogMetadataJson,
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
  catalogMetadataJson?: Readonly<Record<string, unknown>>;
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
            catalogMetadataJson,
            logicalTable: testTable(id, table),
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
  void schemaName;
  return testTable(catalogId, tableName);
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
    const sampleRef = QuerySourceModule.SAMPLE_QUERY_SOURCE_REF;
    const sample = catalog({
      id: sampleRef.resource!.connectionId,
      alias: SAMPLE_QUERY_SOURCE.catalogName,
      schema: SAMPLE_QUERY_SOURCE.schemaName,
      table: SAMPLE_QUERY_SOURCE.tableName,
      manifestUrl: SAMPLE_QUERY_SOURCE.manifestUrl,
      storage: SAMPLE_QUERY_SOURCE.storage,
      region: SAMPLE_QUERY_SOURCE.region,
    });
    sample.schemas[0]!.tables[0]!.logicalTable = sampleRef;
    Object.assign(sample.schemas[0]!.tables[0]!, {
      snapshot: 3,
      rows: 6,
      files: 1,
      size: 'fixture',
      protocol: 'r2/w5',
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
      id: selected.resource!.connectionId,
      alias: SAMPLE_QUERY_SOURCE.catalogName,
      schema: 'prod_like',
      table: selected.name,
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

  it('carries generated metadata JSON into local/public query sources unchanged', () => {
    const catalogMetadataJson = {
      storageLocation: 'gs://workspace/events',
      latestSnapshotVersion: '0',
    };
    const candidate = catalog({
      manifestUrl: '',
      uri: 'gs://workspace/events',
      catalogMetadataJson,
    });

    expect(resolveSelection([candidate], ref())).toMatchObject({
      kind: 'resource',
      source: { catalogMetadataJson },
    });
  });

  it('selects the exact same-named canonical table instead of the first display-name match', () => {
    const connectionId = 'axon-connection://public-gcs/workspace';
    const first = createPublicObjectStorageCanonicalTable({
      provider: 'gcs',
      connectionId,
      normalizedTableUri: 'gs://workspace/first/events',
      tableName: 'events',
    });
    const second = createPublicObjectStorageCanonicalTable({
      provider: 'gcs',
      connectionId,
      normalizedTableUri: 'gs://workspace/second/events',
      tableName: 'events',
    });
    const candidate = catalog({
      id: connectionId,
      manifestUrl: '',
      uri: 'gs://workspace/first/events',
    });
    candidate.schemas[0]!.tables = [
      {
        name: 'events',
        uri: 'gs://workspace/first/events',
        logicalTable: first,
      },
      {
        name: 'events',
        uri: 'gs://workspace/second/events',
        logicalTable: second,
      },
    ];

    expect(
      resolveSelection([candidate], second as unknown as ActiveConnectedTableRef),
    ).toMatchObject({
      kind: 'resource',
      ref: second,
      source: {
        kind: 'object_store_table_root',
        tableUri: 'gs://workspace/second/events',
      },
    });
  });
});

function testTable(connectionId: string, tableName: string): ActiveConnectedTableRef {
  return create(TableNodeSchema, {
    resource: create(CanonicalResourceRefSchema, {
      connectionId,
      providerNamespace: 'axon.test/v1',
      kind: ResourceKind.TABLE,
      identity: { case: 'canonicalLocator', value: `test://${connectionId}/${tableName}` },
    }),
    tableType: TableType.TABLE,
    name: tableName,
  });
}

describe('querySourcesForCatalog', () => {
  it('keeps canonical source identity stable when the catalog alias changes', () => {
    const first = querySourcesForCatalog(
      catalog({
        id: 'axon-connection://public-gcs/workspace',
        alias: 'Workspace',
        manifestUrl: '',
        uri: 'gs://workspace/events',
      }),
    )[0]!;
    const renamed = querySourcesForCatalog(
      catalog({
        id: 'axon-connection://public-gcs/workspace',
        alias: 'Renamed workspace',
        manifestUrl: '',
        uri: 'gs://workspace/events',
      }),
    )[0]!;

    expect(querySourceIdentity(renamed)).toEqual(querySourceIdentity(first));
  });

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
