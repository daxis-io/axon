import { create, toJson } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  ColumnNodeSchema,
  TableMetadataSchema,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
} from './canonical-table-identity.ts';
import { loadCatalog, snapshotCatalog } from './catalog.ts';
import { SAMPLE_QUERY_SOURCE, type QueryTableSource } from './query-source.ts';

function metadataJson(storageLocation: string, table: TableNode) {
  return toJson(
    TableMetadataSchema,
    create(TableMetadataSchema, {
      table,
      columns: [
        create(ColumnNodeSchema, {
          name: 'event_id',
          type: 'long',
          nullable: false,
        }),
      ],
      partitionColumns: ['event_date'],
      rowCount: 0n,
      sizeBytes: 0n,
      fileCount: 0n,
      latestSnapshotVersion: 0n,
      minReaderVersion: 1,
      minWriterVersion: 2,
      storageLocation,
    }),
  ) as Readonly<Record<string, unknown>>;
}

function context(signal = new AbortController().signal) {
  return {
    signal,
    correlationId: 'catalog-load-test',
  };
}

describe('loadCatalog', () => {
  it('routes local generated metadata through provider discovery and preserves explicit zero', async () => {
    const source: QueryTableSource = {
      kind: 'local_delta',
      catalogName: 'renamed-local-alias',
      schemaName: 'analytics',
      tableName: 'events',
      localRegistryId: 'local-registry',
      storage: 'Local folder: events',
      region: 'browser-local',
      catalogMetadataJson: metadataJson(
        'browser-local://delta-table/events',
        createLocalDeltaCanonicalTable({
          registryId: 'local-registry',
          tableName: 'events',
        }),
      ),
    };

    await expect(loadCatalog(source, context())).resolves.toEqual({
      name: 'local-delta',
      region: 'browser-local',
      storage: 'browser-local://delta-table/events',
      tables: [
        expect.objectContaining({
          name: 'events',
          uri: 'browser-local://delta-table/events',
          snapshot: 0,
          row_count: 0,
          size_bytes: 0,
          file_count: 0,
          partition_columns: [{ name: 'event_date', type: 'unknown', pruning: 'stats' }],
          columns: [
            {
              name: 'event_id',
              type: 'long',
              role: 'data',
              nullable: false,
            },
          ],
        }),
      ],
    });
  });

  it('routes anonymous public metadata through the matching public provider identity', async () => {
    const source: QueryTableSource = {
      kind: 'object_store_table_root',
      provider: 'gcs',
      catalogName: 'display-only',
      schemaName: 'main',
      tableName: 'events',
      tableUri: 'gs://public-bucket/events',
      storage: 'gs://public-bucket/events',
      region: 'browser-local',
      catalogMetadataJson: metadataJson(
        'gs://public-bucket/events',
        createPublicObjectStorageCanonicalTable({
          provider: 'gcs',
          connectionId: 'axon-connection://public-gcs/public-bucket',
          normalizedTableUri: 'gs://public-bucket/events',
          tableName: 'events',
        }),
      ),
    };

    await expect(loadCatalog(source, context())).resolves.toMatchObject({
      name: 'public-gcs',
      storage: 'gs://public-bucket/events',
      tables: [{ name: 'events', uri: 'gs://public-bucket/events', snapshot: 0 }],
    });
  });

  it('fails closed when persisted generated metadata is malformed or identity-tampered', async () => {
    const source: QueryTableSource = {
      kind: 'object_store_table_root',
      provider: 's3',
      catalogName: 'public-s3',
      schemaName: 'main',
      tableName: 'events',
      tableUri: 's3://public-bucket/events',
      storage: 's3://public-bucket/events',
      region: 'us-east-1',
      catalogMetadataJson: metadataJson(
        's3://other-bucket/events',
        createPublicObjectStorageCanonicalTable({
          provider: 's3',
          connectionId: 'axon-connection://public-s3/us-east-1/public-bucket',
          normalizedTableUri: 's3://public-bucket/events',
          tableName: 'events',
        }),
      ),
    };

    await expect(loadCatalog(source, context())).rejects.toMatchObject({
      name: 'CatalogProviderError',
      kind: 'invalid_request',
    });
    await expect(
      loadCatalog(
        {
          ...source,
          catalogMetadataJson: { invalid: BigInt(1) },
        },
        context(),
      ),
    ).rejects.toThrow();
    const completeJson = metadataJson(
      's3://public-bucket/events',
      createPublicObjectStorageCanonicalTable({
        provider: 's3',
        connectionId: 'axon-connection://public-s3/us-east-1/public-bucket',
        normalizedTableUri: 's3://public-bucket/events',
        tableName: 'events',
      }),
    );
    const missingIdentityJson = { ...completeJson };
    delete missingIdentityJson.table;
    await expect(
      loadCatalog({ ...source, catalogMetadataJson: missingIdentityJson }, context()),
    ).rejects.toMatchObject({ kind: 'invalid_request' });
  });

  it('rejects capability-bearing local storage locations after metadata rehydration', async () => {
    const source: QueryTableSource = {
      kind: 'local_delta',
      catalogName: 'local',
      schemaName: 'main',
      tableName: 'events',
      localRegistryId: 'registry',
      storage: 'Local folder: events',
      region: 'browser-local',
      catalogMetadataJson: metadataJson(
        'browser-local://delta-table/events?access_token=secret',
        createLocalDeltaCanonicalTable({
          registryId: 'registry',
          tableName: 'events',
        }),
      ),
    };

    await expect(loadCatalog(source, context())).rejects.toMatchObject({
      kind: 'invalid_request',
    });
  });

  it('keeps the exact sample fixture on the isolated snapshot compatibility path', async () => {
    const fixture = { ...SAMPLE_QUERY_SOURCE, snapshot: 0 };

    await expect(loadCatalog(fixture, context())).resolves.toEqual(snapshotCatalog(fixture));
  });

  it('keeps legacy local records without generated metadata readable without using display labels as identity', async () => {
    const source: QueryTableSource = {
      kind: 'local_delta',
      catalogName: 'legacy-local',
      schemaName: 'main',
      tableName: 'events',
      localRegistryId: 'legacy registry/id',
      storage: 'Local folder: events',
      region: 'browser-local',
      snapshot: 0,
      rows: 0,
      files: 0,
    };

    await expect(loadCatalog(source, context())).resolves.toMatchObject({
      name: 'local-delta',
      storage: 'browser-local://delta-table/legacy%20registry%2Fid',
      tables: [
        {
          name: 'events',
          uri: 'browser-local://delta-table/legacy%20registry%2Fid',
          snapshot: 0,
          row_count: 0,
          file_count: 0,
        },
      ],
    });
  });

  it('propagates cancellation as AbortError without falling back to summary discovery', async () => {
    const controller = new AbortController();
    controller.abort();
    const source: QueryTableSource = {
      kind: 'local_delta',
      catalogName: 'local',
      schemaName: 'main',
      tableName: 'events',
      localRegistryId: 'registry',
      storage: 'Local folder: events',
      region: 'browser-local',
      catalogMetadataJson: metadataJson(
        'browser-local://delta-table/events',
        createLocalDeltaCanonicalTable({
          registryId: 'registry',
          tableName: 'events',
        }),
      ),
    };

    await expect(loadCatalog(source, context(controller.signal))).rejects.toMatchObject({
      name: 'AbortError',
      kind: 'cancelled',
    });
  });
});
