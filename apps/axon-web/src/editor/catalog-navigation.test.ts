import { create, toJson } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  ColumnNodeSchema,
  DeltaProtocolFeatureSchema,
  TableMetadataSchema,
  TableType,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
} from '../services/canonical-table-identity.ts';
import type { ActiveConnectedTableRef, QueryCatalogCandidate } from '../services/query-source.ts';
import {
  catalogExplorerModel,
  catalogExplorerTableDetail,
  catalogTablePath,
  catalogTableResourceFromParams,
  catalogTableSqlPath,
  legacyCatalogTablePath,
  resolveCatalogTableRoute,
  resolveLegacyCatalogTableRoute,
  savedQueryPath,
  tableRefForRouteSelection,
  type CatalogTableRouteParams,
} from './catalog-navigation.ts';

describe('canonical catalog navigation', () => {
  it('round-trips encoded local provider-object IDs and public canonical locators', () => {
    const local = createLocalDeltaCanonicalTable({
      registryId: 'local registry/id',
      tableName: 'events',
    });
    const publicTable = createPublicObjectStorageCanonicalTable({
      provider: 'gcs',
      connectionId: 'axon-connection://public-gcs/public-bucket',
      normalizedTableUri: 'gs://public-bucket/sales%20ops/events',
      tableName: 'events',
    });

    expect(catalogTablePath(local)).toBe(
      '/catalog/axon-connection%3A%2F%2Flocal-delta%2Flocal%2520registry%252Fid/table/' +
        'axon.local-delta%2Fv1/provider-object-id/local%20registry%2Fid',
    );
    expect(catalogTableSqlPath(publicTable)).toBe(
      '/catalog/axon-connection%3A%2F%2Fpublic-gcs%2Fpublic-bucket/table/' +
        'axon.public-gcs%2Fv1/canonical-locator/gs%3A%2F%2Fpublic-bucket%2Fsales%2520ops%2Fevents/sql',
    );
    expect(catalogTableResourceFromParams(paramsFor(local))).toEqual(local.resource);
    expect(catalogTableResourceFromParams(paramsFor(publicTable))).toEqual(publicTable.resource);
  });

  it('selects the exact resource despite identical aliases and display coordinates', () => {
    const first = publicTable('gs://shared-bucket/first/events');
    const second = publicTable('gs://shared-bucket/second/events');
    const catalogs = [catalogWithTables([first, second])];

    expect(resolveCatalogTableRoute(catalogs, paramsFor(second))).toMatchObject({
      status: 'valid',
      ref: second,
      source: { tableUri: 'gs://shared-bucket/second/events' },
    });
  });

  it('fails closed for malformed, disconnected, stale, and connection-mismatched routes', () => {
    const selected = publicTable('gs://shared-bucket/events');
    const catalogs = [catalogWithTables([selected])];

    expect(
      resolveCatalogTableRoute(catalogs, {
        ...paramsFor(selected),
        identityArm: 'wrong-arm',
      }),
    ).toEqual({ status: 'invalid', reason: 'malformed_route' });
    expect(
      resolveCatalogTableRoute(catalogs, {
        ...paramsFor(selected),
        connectionId: 'axon-connection://public-gcs/disconnected',
      }),
    ).toMatchObject({ status: 'invalid', reason: 'disconnected_connection' });
    expect(
      resolveCatalogTableRoute(catalogs, {
        ...paramsFor(selected),
        identityValue: 'gs://shared-bucket/removed',
      }),
    ).toMatchObject({ status: 'invalid', reason: 'stale_resource' });
    expect(
      resolveCatalogTableRoute(catalogs, {
        ...paramsFor(selected),
        providerNamespace: 'axon.public-s3/v1',
      }),
    ).toMatchObject({ status: 'invalid', reason: 'stale_resource' });
  });

  it('keeps an exact non-queryable resource browseable but blocks its SQL route', () => {
    const selected = publicTable('gs://shared-bucket/events');
    const catalogs = [catalogWithTables([selected], { provider: 's3' })];

    expect(resolveCatalogTableRoute(catalogs, paramsFor(selected))).toEqual({
      status: 'valid',
      ref: selected,
      source: undefined,
    });
    expect(
      resolveCatalogTableRoute(catalogs, paramsFor(selected), { requireQueryable: true }),
    ).toMatchObject({
      status: 'invalid',
      reason: 'non_queryable',
    });
  });

  it('bridges a unique legacy display route and rejects ambiguous legacy coordinates', () => {
    const first = publicTable('gs://shared-bucket/first/events');
    const second = publicTable('gs://shared-bucket/second/events');
    const unique = catalogWithTables([first], { alias: 'Workspace' });
    const params = {
      catalogId: 'catalog-workspace',
      schemaName: 'default',
      tableName: 'events',
    };

    expect(legacyCatalogTablePath(params)).toBe('/catalog/catalog-workspace/default/events');
    expect(resolveLegacyCatalogTableRoute([unique], params)).toEqual({
      status: 'valid',
      ref: first,
      redirect: catalogTableSqlPath(first),
    });
    expect(resolveLegacyCatalogTableRoute([catalogWithTables([first, second])], params)).toEqual({
      status: 'invalid',
      reason: 'ambiguous_legacy_route',
    });
  });

  it('builds collision-free Explorer rows from canonical identities', () => {
    const first = publicTable('gs://shared-bucket/first/events');
    const second = publicTable('gs://shared-bucket/second/events');
    const model = catalogExplorerModel([catalogWithTables([first, second])], second);

    expect(model).toMatchObject({
      status: 'ready',
      catalogCount: 1,
      schemaCount: 1,
      tableCount: 2,
      queryableTableCount: 2,
    });
    expect(model.catalogs[0]!.schemas[0]!.tables).toEqual([
      expect.objectContaining({
        name: 'events',
        active: false,
        path: catalogTablePath(first),
        sqlPath: catalogTableSqlPath(first),
      }),
      expect.objectContaining({
        name: 'events',
        active: true,
        path: catalogTablePath(second),
        sqlPath: catalogTableSqlPath(second),
      }),
    ]);
    expect(model.catalogs[0]!.schemas[0]!.tables[0]!.key).not.toBe(
      model.catalogs[0]!.schemas[0]!.tables[1]!.key,
    );
  });

  it('mirrors only a changed valid route selection into presentation state', () => {
    const selected = publicTable('gs://shared-bucket/events');
    const resolution = resolveCatalogTableRoute(
      [catalogWithTables([selected])],
      paramsFor(selected),
    );

    expect(tableRefForRouteSelection(resolution)).toEqual(selected);
    expect(tableRefForRouteSelection(resolution, selected)).toBeUndefined();
    expect(
      tableRefForRouteSelection({ status: 'invalid', reason: 'malformed_route' }),
    ).toBeUndefined();
  });

  it('keeps saved query IDs encoded independently of catalog routing', () => {
    expect(savedQueryPath('saved/query 42')).toBe('/saved/saved%2Fquery%2042');
  });

  it('reports an empty Explorer model without substituting sample data', () => {
    expect(catalogExplorerModel([])).toEqual({
      status: 'empty',
      catalogCount: 0,
      schemaCount: 0,
      tableCount: 0,
      queryableTableCount: 0,
      catalogs: [],
    });
  });

  it('projects generated overview and semantic columns without resolving access', () => {
    const table = publicTable('gs://shared-bucket/events');
    table.comment = 'Curated event stream';
    const catalogs = [catalogWithTables([table])];
    catalogs[0]!.schemas[0]!.tables[0]!.catalogMetadataJson = toJson(
      TableMetadataSchema,
      create(TableMetadataSchema, {
        table,
        columns: [
          create(ColumnNodeSchema, {
            name: 'event_date',
            type: 'date',
            nullable: false,
            comment: 'UTC event date',
          }),
          create(ColumnNodeSchema, {
            name: 'payload',
            type: 'string',
            nullable: true,
          }),
        ],
        partitionColumns: ['event_date'],
        rowCount: 1_200n,
        sizeBytes: 44_040_192n,
        fileCount: 3n,
        latestSnapshotVersion: 12n,
        minReaderVersion: 2,
        minWriterVersion: 5,
        protocolFeatures: [
          create(DeltaProtocolFeatureSchema, {
            name: 'columnMapping',
            reader: true,
            writer: true,
          }),
        ],
        storageLocation: 'gs://shared-bucket/events',
      }),
    ) as Readonly<Record<string, unknown>>;

    expect(catalogExplorerTableDetail(catalogs, table)).toEqual({
      status: 'ready',
      ref: table,
      connectionAlias: 'Workspace',
      catalogName: 'Workspace',
      schemaName: 'default',
      tableName: 'events',
      tableKind: 'Table',
      comment: 'Curated event stream',
      queryable: true,
      sqlPath: catalogTableSqlPath(table),
      overview: {
        storageLocation: 'gs://shared-bucket/events',
        snapshot: 12,
        rows: 1200,
        files: 3,
        sizeBytes: 44_040_192,
        protocol: 'r2/w5',
        features: ['columnMapping'],
        partitions: ['event_date'],
      },
      columnsStatus: 'ready',
      columns: [
        {
          name: 'event_date',
          type: 'date',
          nullable: false,
          comment: 'UTC event date',
          partition: true,
        },
        {
          name: 'payload',
          type: 'string',
          nullable: true,
          comment: undefined,
          partition: false,
        },
      ],
    });
  });

  it('keeps generated views browseable but unavailable to the SQL editor', () => {
    const view = publicTable('gs://shared-bucket/views/events');
    view.tableType = TableType.VIEW;
    const catalogs = [catalogWithTables([view])];
    catalogs[0]!.schemas[0]!.tables[0]!.catalogMetadataJson = toJson(
      TableMetadataSchema,
      create(TableMetadataSchema, { table: view, storageLocation: 'logical view' }),
    ) as Readonly<Record<string, unknown>>;

    expect(catalogExplorerTableDetail(catalogs, view)).toMatchObject({
      status: 'ready',
      tableKind: 'View',
      queryable: false,
      sqlPath: undefined,
      columnsStatus: 'empty',
    });
  });

  it('distinguishes no selection, stale selection, and unavailable metadata', () => {
    const selected = publicTable('gs://shared-bucket/events');
    const catalogs = [catalogWithTables([selected])];

    expect(catalogExplorerTableDetail(catalogs)).toEqual({ status: 'no_selection' });
    expect(catalogExplorerTableDetail(catalogs, publicTable('gs://shared-bucket/removed'))).toEqual(
      {
        status: 'unavailable',
        reason: 'stale',
      },
    );
    expect(catalogExplorerTableDetail(catalogs, selected)).toMatchObject({
      status: 'metadata_unavailable',
      reason: 'missing',
      ref: selected,
    });

    catalogs[0]!.schemas[0]!.tables[0]!.catalogMetadataJson = { table: {} };
    expect(catalogExplorerTableDetail(catalogs, selected)).toMatchObject({
      status: 'metadata_unavailable',
      reason: 'invalid',
      ref: selected,
    });
  });
});

function publicTable(locator: string): ActiveConnectedTableRef {
  return createPublicObjectStorageCanonicalTable({
    provider: 'gcs',
    connectionId: 'axon-connection://public-gcs/shared-bucket',
    normalizedTableUri: locator,
    tableName: 'events',
  });
}

function catalogWithTables(
  tables: ActiveConnectedTableRef[],
  options: { alias?: string; provider?: string } = {},
): QueryCatalogCandidate {
  return {
    id: tables[0]!.resource!.connectionId,
    alias: options.alias ?? 'Workspace',
    kind: 'object_store',
    provider: options.provider ?? 'gcs',
    storage: 'gs://shared-bucket',
    region: 'us-central1',
    schemas: [
      {
        name: 'default',
        tables: tables.map((table) => ({
          name: table.name,
          uri:
            table.resource?.identity.case === 'canonicalLocator'
              ? table.resource.identity.value
              : undefined,
          logicalTable: table,
          snapshot: 12,
          rows: 1200,
          files: 3,
          size: '42 MB',
        })),
      },
    ],
  };
}

function paramsFor(table: ActiveConnectedTableRef): CatalogTableRouteParams {
  const resource = table.resource!;
  return {
    connectionId: resource.connectionId,
    providerNamespace: resource.providerNamespace,
    identityArm:
      resource.identity.case === 'providerObjectId' ? 'provider-object-id' : 'canonical-locator',
    identityValue: resource.identity.value ?? '',
  };
}
