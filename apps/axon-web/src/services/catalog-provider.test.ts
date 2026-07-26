import { create, equals, toBinary } from '@bufbuild/protobuf';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  ColumnNodeSchema,
  GetTableMetadataResponseSchema,
  ListTablesResponseSchema,
  TableMetadataSchema,
  TableNodeSchema,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  PartitionColumnType,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  PageRequestSchema,
  ProviderErrorCode,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  CatalogProviderError,
  createLocalDeltaCatalogProvider,
  createPublicObjectStorageCatalogProvider,
  discoverFlatCatalog,
  type CatalogProvider,
} from './catalog-provider.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
  type PublicObjectStorageCanonicalTableInput,
} from './canonical-table-identity.ts';
import { publicObjectStorageCatalogMetadata } from './object-storage.ts';

const page = create(PageRequestSchema);

function context(overrides: Partial<{ signal: AbortSignal; deadlineEpochMs: number }> = {}) {
  return {
    signal: overrides.signal ?? new AbortController().signal,
    correlationId: 'catalog-test-correlation',
    deadlineEpochMs: overrides.deadlineEpochMs,
  };
}

function localProvider() {
  return createLocalDeltaCatalogProvider({
    registryId: 'opaque/local id',
    schemaName: 'analytics',
    tableName: 'events',
    metadata: create(TableMetadataSchema, {
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
      storageLocation: 'browser-local://delta-table/table-root',
    }),
  });
}

describe('local CatalogProvider', () => {
  it('routes the local connection flow through provider discovery instead of runtime discovery DTOs', () => {
    const connectModal = readFileSync(
      fileURLToPath(new URL('../editor/connect/ConnectModal.tsx', import.meta.url)),
      'utf8',
    );

    expect(connectModal).toContain('createLocalDeltaCatalogProvider');
    expect(connectModal).toContain('discoverFlatCatalog');
    expect(connectModal).not.toContain('form.localDelta.discovery');
    expect(connectModal).not.toContain('runtime.discovery');
  });

  it('threads one caller-owned signal through local/public acquisition before registration', () => {
    const connectModal = readFileSync(
      fileURLToPath(new URL('../editor/connect/ConnectModal.tsx', import.meta.url)),
      'utf8',
    );

    expect(connectModal).toMatch(
      /resolvePublicObjectStorageDescriptor\(\{[\s\S]*?signal: controller\.signal,[\s\S]*?\}\)/,
    );
    expect(connectModal).toMatch(
      /preflightPublicObjectStorageDescriptorRangeRead\(\{[\s\S]*?signal: controller\.signal,[\s\S]*?\}\)/,
    );
    expect(connectModal).toMatch(
      /registerPublicObjectStorageRuntimeCache\(\{[\s\S]*?signal: controller\.signal,[\s\S]*?\}\)/,
    );
    expect(connectModal).toContain('openLocalDeltaTableFromDirectoryHandle(handle, { signal })');
    expect(connectModal).toContain('openLocalDeltaTableFromFileList(files, { signal })');
    expect(connectModal).toContain('discoveryController.current === controller');
  });

  it('returns one generated flat hierarchy with terminating pages and explicit zero metadata', async () => {
    const provider = localProvider();

    const catalogs = await provider.listCatalogs(page, context());
    const schemas = await provider.listSchemas(catalogs.catalogs[0]!, page, context());
    const tables = await provider.listTables(schemas.schemas[0]!, page, context());
    const metadata = await provider.getTableMetadata(tables.tables[0]!, context());

    expect(catalogs.catalogs).toEqual([
      {
        $typeName: 'axon.catalog.v1.CatalogNode',
        connectionId: 'axon-connection://local-delta/opaque%2Flocal%20id',
        name: 'local-delta',
        comment: '',
      },
    ]);
    expect(schemas.schemas[0]).toMatchObject({
      connectionId: 'axon-connection://local-delta/opaque%2Flocal%20id',
      catalog: 'local-delta',
      name: 'analytics',
    });
    expect(tables.tables[0]?.resource).toMatchObject({
      connectionId: 'axon-connection://local-delta/opaque%2Flocal%20id',
      providerNamespace: 'axon.local-delta/v1',
      identity: { case: 'providerObjectId', value: 'opaque/local id' },
    });
    expect(catalogs.page?.nextCursor).toBeUndefined();
    expect(schemas.page?.nextCursor).toBeUndefined();
    expect(tables.page?.nextCursor).toBeUndefined();
    expect(metadata.table).toMatchObject({
      rowCount: 0n,
      sizeBytes: 0n,
      fileCount: 0n,
      latestSnapshotVersion: 0n,
      minReaderVersion: 1,
      minWriterVersion: 2,
      storageLocation: 'browser-local://delta-table/table-root',
    });
    expect(metadata.table?.columns).toMatchObject([{ name: 'event_id', type: 'long' }]);
    expect(metadata.table?.partitionColumns).toEqual(['event_date']);
  });

  it('aggregates generated responses and preserves semantic and binary E9 table identity', async () => {
    const discovered = await discoverFlatCatalog(localProvider(), page, context());
    const expected = createLocalDeltaCanonicalTable({
      registryId: 'opaque/local id',
      tableName: 'events',
    });

    expect(equals(TableNodeSchema, discovered.table, expected)).toBe(true);
    expect(toBinary(TableNodeSchema, discovered.table)).toEqual(
      toBinary(TableNodeSchema, expected),
    );
    expect(equals(TableNodeSchema, discovered.metadata.table!, expected)).toBe(true);
  });

  it('preserves absent optional metadata independently from explicit zero', async () => {
    const provider = createLocalDeltaCatalogProvider({
      registryId: 'missing-optionals',
      schemaName: 'default',
      tableName: 'events',
      metadata: create(TableMetadataSchema, {
        storageLocation: 'browser-local://delta-table/table-root',
      }),
    });

    const snapshot = await discoverFlatCatalog(provider, page, context());

    expect(snapshot.metadata.rowCount).toBeUndefined();
    expect(snapshot.metadata.sizeBytes).toBeUndefined();
    expect(snapshot.metadata.fileCount).toBeUndefined();
    expect(snapshot.metadata.latestSnapshotVersion).toBeUndefined();
  });

  it('rejects non-empty cursors and malformed local identities with generated safe errors', async () => {
    const provider = localProvider();

    await expect(
      provider.listCatalogs(create(PageRequestSchema, { cursor: 'next' }), context()),
    ).rejects.toMatchObject({
      kind: 'invalid_request',
      retryable: false,
      detail: {
        code: ProviderErrorCode.INVALID,
        correlationId: 'catalog-test-correlation',
      },
    });
    expect(() =>
      createLocalDeltaCatalogProvider({
        registryId: '',
        schemaName: 'default',
        tableName: 'events',
        metadata: create(TableMetadataSchema),
      }),
    ).toThrow(CatalogProviderError);
  });

  it('rejects cancellation as AbortError and expired deadlines before discovery', async () => {
    const provider = localProvider();
    const controller = new AbortController();
    controller.abort();

    await expect(
      provider.listCatalogs(page, context({ signal: controller.signal })),
    ).rejects.toMatchObject({
      name: 'AbortError',
      kind: 'cancelled',
      retryable: false,
      detail: { code: ProviderErrorCode.UNSPECIFIED },
    });
    await expect(
      provider.listCatalogs(page, context({ deadlineEpochMs: Date.now() - 1 })),
    ).rejects.toMatchObject({
      name: 'CatalogProviderError',
      kind: 'deadline_exceeded',
      retryable: false,
      detail: { code: ProviderErrorCode.UNSPECIFIED },
    });
  });

  it('fails closed when callers supply mismatched generated parents', async () => {
    const provider = localProvider();
    const snapshot = await discoverFlatCatalog(provider, page, context());
    const impostor = create(TableNodeSchema, {
      ...snapshot.table,
      name: 'other',
    });

    await expect(provider.getTableMetadata(impostor, context())).rejects.toMatchObject({
      kind: 'invalid_request',
      detail: {
        code: ProviderErrorCode.INVALID,
        correlationId: 'catalog-test-correlation',
      },
    });
  });

  it('rejects a self-consistent table and metadata pair from another connection', async () => {
    const base = localProvider();
    const foreignTable = createLocalDeltaCanonicalTable({
      registryId: 'foreign-registry',
      tableName: 'events',
    });
    const provider: CatalogProvider = {
      ...base,
      async listTables(schema, request, discoveryContext) {
        await base.listTables(schema, request, discoveryContext);
        return create(ListTablesResponseSchema, { tables: [foreignTable] });
      },
      async getTableMetadata() {
        return create(GetTableMetadataResponseSchema, {
          table: create(TableMetadataSchema, {
            table: foreignTable,
            storageLocation: 'browser-local://delta-table/foreign',
          }),
        });
      },
    };

    await expect(discoverFlatCatalog(provider, page, context())).rejects.toMatchObject({
      kind: 'invalid_request',
      detail: { correlationId: 'catalog-test-correlation' },
    });
  });

  it('accepts explicitly empty continuation cursors while rejecting non-empty cursors', async () => {
    const base = localProvider();
    const withCursor = (nextCursor: string): CatalogProvider => ({
      ...base,
      async listCatalogs(request, discoveryContext) {
        const response = await base.listCatalogs(request, discoveryContext);
        response.page!.nextCursor = nextCursor;
        return response;
      },
      async listSchemas(catalog, request, discoveryContext) {
        const response = await base.listSchemas(catalog, request, discoveryContext);
        response.page!.nextCursor = nextCursor;
        return response;
      },
      async listTables(schema, request, discoveryContext) {
        const response = await base.listTables(schema, request, discoveryContext);
        response.page!.nextCursor = nextCursor;
        return response;
      },
    });

    await expect(discoverFlatCatalog(withCursor(''), page, context())).resolves.toBeDefined();
    await expect(discoverFlatCatalog(withCursor('next'), page, context())).rejects.toMatchObject({
      kind: 'invalid_request',
    });
  });
});

describe('public object storage CatalogProvider', () => {
  const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri: 'gs://Public-Bucket/events/table',
    snapshotVersion: 0n,
    partitionColumnTypes: {
      event_date: PartitionColumnType.STRING,
    },
    activeFiles: [
      create(BrowserHttpFileDescriptorSchema, {
        path: 'event_date=2026-07-26/part.parquet',
        url: 'https://storage.googleapis.com/Public-Bucket/events/table/part.parquet',
        sizeBytes: 7n,
        stats: JSON.stringify({ numRecords: 4 }),
      }),
    ],
  });

  it('routes the public connection flow through provider discovery outside React metadata code', () => {
    const connectModal = readFileSync(
      fileURLToPath(new URL('../editor/connect/ConnectModal.tsx', import.meta.url)),
      'utf8',
    );

    expect(connectModal).toContain('createPublicObjectStorageCatalogProvider');
    expect(connectModal).toContain('publicObjectStorageCatalogMetadata');
    expect(connectModal).not.toContain('objectStorageRuntimeFromDescriptor');
    expect(connectModal).not.toContain('objectStorage.discovery');
    const discoveryIndex = connectModal.indexOf(
      'const catalogDiscovery = await discoverFlatCatalog',
    );
    const ownershipGuardIndex = connectModal.indexOf(
      'if (connectionTestController.current !== controller) return',
      discoveryIndex,
    );
    const registrationIndex = connectModal.indexOf(
      'registerPublicObjectStorageRuntimeCache',
      discoveryIndex,
    );
    expect(discoveryIndex).toBeGreaterThan(-1);
    expect(ownershipGuardIndex).toBeGreaterThan(discoveryIndex);
    expect(registrationIndex).toBeGreaterThan(ownershipGuardIndex);
  });

  it.each([
    [
      'gcs' as const,
      'public-gcs',
      'axon-connection://public-gcs/Public-Bucket',
      'axon.public-gcs/v1',
      'gs://Public-Bucket/events/table',
    ],
    [
      's3' as const,
      'public-s3',
      'axon-connection://public-s3/us-east-2/public-bucket',
      'axon.public-s3/v1',
      's3://public-bucket/events/table',
    ],
  ])(
    'returns generated %s hierarchy and canonical identity',
    async (provider, catalogName, connectionId, providerNamespace, canonicalLocator) => {
      const metadata = publicObjectStorageCatalogMetadata(
        create(BrowserHttpSnapshotDescriptorSchema, {
          ...descriptor,
          tableUri: canonicalLocator,
        }),
      );
      const providerIdentity = provider === 's3' ? { provider, region: 'us-east-2' } : { provider };
      const catalogProvider = createPublicObjectStorageCatalogProvider({
        ...providerIdentity,
        connectionId,
        normalizedTableUri: canonicalLocator,
        schemaName: 'default',
        tableName: 'table',
        metadata,
      });

      const discovered = await discoverFlatCatalog(catalogProvider, page, context());

      expect(discovered.catalog).toMatchObject({ name: catalogName, connectionId });
      expect(discovered.table.resource).toMatchObject({
        connectionId,
        providerNamespace,
        identity: { case: 'canonicalLocator', value: canonicalLocator },
      });
      expect(discovered.metadata).toMatchObject({
        latestSnapshotVersion: 0n,
        rowCount: 4n,
        sizeBytes: 7n,
        fileCount: 1n,
        partitionColumns: ['event_date'],
        minReaderVersion: 1,
        minWriterVersion: 2,
        storageLocation: canonicalLocator,
      });
      expect(discovered.catalog).not.toHaveProperty('descriptorResolutionMetrics');
      expect(discovered.metadata).not.toHaveProperty('descriptorResolutionMetrics');
    },
  );

  it('uses the same normalized public table identity as E9', async () => {
    const provider = createPublicObjectStorageCatalogProvider({
      provider: 'gcs',
      connectionId: 'axon-connection://public-gcs/Public-Bucket',
      normalizedTableUri: descriptor.tableUri,
      schemaName: 'default',
      tableName: 'table',
      metadata: publicObjectStorageCatalogMetadata(descriptor),
    });
    const discovered = await discoverFlatCatalog(provider, page, context());
    const expected = createPublicObjectStorageCanonicalTable({
      provider: 'gcs',
      connectionId: 'axon-connection://public-gcs/Public-Bucket',
      normalizedTableUri: 'gs://Public-Bucket/events/table',
      tableName: 'table',
    });

    expect(equals(TableNodeSchema, discovered.table, expected)).toBe(true);
    expect(toBinary(TableNodeSchema, discovered.table)).toEqual(
      toBinary(TableNodeSchema, expected),
    );
  });

  it.each([
    {
      label: 'GCS bucket',
      input: {
        provider: 'gcs',
        connectionId: 'axon-connection://public-gcs/other-bucket',
        normalizedTableUri: 'gs://public-bucket/events/table',
        tableName: 'table',
      },
    },
    {
      label: 'S3 bucket',
      input: {
        provider: 's3',
        connectionId: 'axon-connection://public-s3/us-east-2/other-bucket',
        normalizedTableUri: 's3://public-bucket/events/table',
        region: 'us-east-2',
        tableName: 'table',
      },
    },
    {
      label: 'S3 region',
      input: {
        provider: 's3',
        connectionId: 'axon-connection://public-s3/us-west-2/public-bucket',
        normalizedTableUri: 's3://public-bucket/events/table',
        region: 'us-east-2',
        tableName: 'table',
      },
    },
  ])('rejects a mismatched canonical public $label', ({ input }) => {
    expect(() =>
      createPublicObjectStorageCanonicalTable(input as PublicObjectStorageCanonicalTableInput),
    ).toThrow('public connection ID did not match the normalized table root');
  });

  it.each([
    ['abfss', 'abfss://account/container/table'],
    ['r2', 'r2://bucket/table'],
    ['unity_catalog', 'https://catalog.example/table'],
    ['delta_share', 'https://share.example/table'],
    ['gcs', 'gs://user:secret@bucket/table'],
    ['gcs', 'gs://bucket/table?token=secret'],
    ['gcs', 'gs://bucket/table#fragment'],
    ['gcs', 'gs://bucket'],
  ])('rejects unsupported or unsafe public input %s %s', (provider, tableUri) => {
    expect(() =>
      createPublicObjectStorageCatalogProvider({
        provider: provider as 'gcs',
        connectionId: `axon-connection://public-${provider}/fixture`,
        normalizedTableUri: tableUri,
        schemaName: 'default',
        tableName: 'table',
        metadata: create(TableMetadataSchema, { storageLocation: tableUri }),
      }),
    ).toThrow(CatalogProviderError);
  });

  it('rejects metadata whose canonical root differs from the adapter root', () => {
    expect(() =>
      createPublicObjectStorageCatalogProvider({
        provider: 'gcs',
        connectionId: 'axon-connection://public-gcs/Public-Bucket',
        normalizedTableUri: descriptor.tableUri,
        schemaName: 'default',
        tableName: 'table',
        metadata: create(TableMetadataSchema, {
          storageLocation: 'gs://other-bucket/other-table',
        }),
      }),
    ).toThrow(CatalogProviderError);
  });
});
