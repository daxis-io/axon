import { create, equals, toBinary } from '@bufbuild/protobuf';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  ColumnNodeSchema,
  TableMetadataSchema,
  TableNodeSchema,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  PageRequestSchema,
  ProviderErrorCode,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  CatalogProviderError,
  createLocalDeltaCatalogProvider,
  discoverFlatCatalog,
} from './catalog-provider.ts';
import { createLocalDeltaCanonicalTable } from './canonical-table-identity.ts';

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
});
