import { create, toJson } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  TableMetadataSchema,
  TableNodeSchema,
  TableType,
} from '../../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  CanonicalResourceRefSchema,
  PageRequestSchema,
  ResourceKind,
} from '../../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserHttpSnapshotDescriptorSchema,
  type BrowserHttpSnapshotDescriptor,
} from '../../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  createLocalDeltaCatalogProvider,
  createPublicObjectStorageCatalogProvider,
  discoverFlatCatalog,
} from '../../services/catalog-provider.ts';
import { publicObjectStorageCatalogMetadata } from '../../services/object-storage.ts';
import { querySourcesForCatalog } from '../../services/query-source.ts';
import type { ConnectedCatalog, ConnectResult } from './types.ts';
import {
  buildCatalogFromResult,
  loadConnectedCatalogs,
  saveConnectedCatalogs,
  SAMPLE_CONNECTED_CATALOG,
  upsertConnectedCatalog,
} from './store.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';

class MemoryStorage implements Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> {
  private readonly records = new Map<string, string>();

  getItem(key: string): string | null {
    return this.records.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.records.set(key, value);
  }

  removeItem(key: string): void {
    this.records.delete(key);
  }
}

function catalog(id: string, alias = id): ConnectedCatalog {
  const storage = `gs://${id}/table`;
  const connectionId = `axon-connection://public-gcs/${encodeURIComponent(id)}`;
  const logicalTable = create(TableNodeSchema, {
    resource: create(CanonicalResourceRefSchema, {
      connectionId,
      providerNamespace: 'axon.public-gcs/v1',
      kind: ResourceKind.TABLE,
      identity: { case: 'canonicalLocator', value: storage },
    }),
    tableType: TableType.TABLE,
    name: id,
  });
  return {
    id: connectionId,
    catalogName: 'public-gcs',
    alias,
    kind: 'object_store',
    provider: 'gcs',
    storage,
    region: 'us',
    status: 'connected',
    connectedAt: id,
    schemas: [
      {
        name: 'default',
        tables: [
          {
            id: `default.${id}`,
            name: id,
            snapshot: 1,
            rows: 1,
            files: 1,
            size: '1 byte',
            protocol: 'r1/w1',
            uri: storage,
            logicalTable,
            catalogMetadataJson: toJson(
              TableMetadataSchema,
              create(TableMetadataSchema, {
                table: logicalTable,
                storageLocation: storage,
              }),
            ) as Readonly<Record<string, unknown>>,
            source: {
              id: `source-${id}`,
              kind: 'object_store',
              provider: 'gcs',
              storage,
              region: 'us',
              canonicalKey: `object_store|gcs|${storage}|||default|${id}`,
              connectedAt: id,
            },
          },
        ],
      },
    ],
  };
}

describe('connected catalog persistence', () => {
  let storage: MemoryStorage;

  beforeEach(() => {
    storage = new MemoryStorage();
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: storage,
    });
  });

  it('loads the sample catalog only when persisted data is missing', () => {
    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);

    storage.setItem(STORAGE_KEY, '{');

    expect(loadConnectedCatalogs()).toEqual([]);
  });

  it('restores generated view metadata for browsing without treating it as queryable', () => {
    const viewCatalog = catalog('weekly_events');
    const view = viewCatalog.schemas[0]!.tables[0]!;
    view.logicalTable!.tableType = TableType.VIEW;
    view.catalogMetadataJson = toJson(
      TableMetadataSchema,
      create(TableMetadataSchema, {
        table: view.logicalTable,
        storageLocation: view.uri,
      }),
    ) as Readonly<Record<string, unknown>>;

    saveConnectedCatalogs([viewCatalog]);

    const restoredCatalogs = loadConnectedCatalogs();
    const restored = restoredCatalogs[0]?.schemas[0]?.tables[0];
    expect(restored?.logicalTable?.tableType).toBe(TableType.VIEW);
    expect(restoredCatalogs.flatMap(querySourcesForCatalog)).toEqual([]);
  });

  it('restores a mixed table, view, and metadata-missing catalog on one connection', () => {
    const mixed = catalog('orders');
    const base = mixed.schemas[0]!.tables[0]!.logicalTable!;
    const view = create(TableNodeSchema, {
      resource: create(CanonicalResourceRefSchema, {
        connectionId: base.resource!.connectionId,
        providerNamespace: base.resource!.providerNamespace,
        kind: ResourceKind.TABLE,
        identity: { case: 'canonicalLocator', value: 'gs://orders/weekly-view' },
      }),
      tableType: TableType.VIEW,
      name: 'weekly_orders',
    });
    const metadataMissing = create(TableNodeSchema, {
      resource: create(CanonicalResourceRefSchema, {
        connectionId: base.resource!.connectionId,
        providerNamespace: base.resource!.providerNamespace,
        kind: ResourceKind.TABLE,
        identity: { case: 'canonicalLocator', value: 'gs://orders/metadata-missing' },
      }),
      tableType: TableType.TABLE,
      name: 'metadata_missing',
    });
    mixed.schemas[0]!.tables.push(
      {
        name: view.name,
        snapshot: 1,
        rows: 0,
        files: 0,
        size: 'logical',
        protocol: 'r1/w1',
        uri: 'gs://orders/weekly-view',
        logicalTable: view,
        catalogMetadataJson: toJson(
          TableMetadataSchema,
          create(TableMetadataSchema, {
            table: view,
            storageLocation: 'gs://orders/weekly-view',
          }),
        ) as Readonly<Record<string, unknown>>,
      },
      {
        name: metadataMissing.name,
        snapshot: 0,
        rows: 0,
        files: 0,
        size: 'not reported',
        protocol: 'not reported',
        uri: 'gs://orders/metadata-missing',
        logicalTable: metadataMissing,
      },
    );

    saveConnectedCatalogs([mixed]);

    const restored = loadConnectedCatalogs();
    expect(restored).toHaveLength(1);
    expect(restored[0]?.schemas[0]?.tables.map((table) => table.name)).toEqual([
      'orders',
      'weekly_orders',
      'metadata_missing',
    ]);
  });

  it('uses generated connection identity while keeping aliases mutable presentation', async () => {
    const first = await publicCatalog({
      bucket: 'shared-bucket',
      path: 'events',
      alias: 'Workspace',
    });
    const renamed = await publicCatalog({
      bucket: 'shared-bucket',
      path: 'events',
      alias: 'Renamed workspace',
    });

    const upsert = upsertConnectedCatalog([first], renamed);

    expect(first).toMatchObject({
      id: 'axon-connection://public-gcs/shared-bucket',
      catalogName: 'public-gcs',
      alias: 'Workspace',
    });
    expect(upsert.catalogs).toHaveLength(1);
    expect(upsert.catalogs[0]).toMatchObject({
      id: first.id,
      catalogName: 'public-gcs',
      alias: 'Renamed workspace',
    });
  });

  it('keeps same-alias connections and same-name canonical resources distinct', async () => {
    const firstConnection = await publicCatalog({
      bucket: 'first-bucket',
      path: 'events',
      alias: 'Workspace',
    });
    const secondConnection = await publicCatalog({
      bucket: 'second-bucket',
      path: 'events',
      alias: 'Workspace',
    });
    const sameConnectionOtherTable = await publicCatalog({
      bucket: 'first-bucket',
      path: 'archive/events',
      tableName: 'events',
      alias: 'Workspace',
    });

    const distinctConnections = upsertConnectedCatalog([firstConnection], secondConnection);
    expect(distinctConnections.catalogs.map((item) => item.id).sort()).toEqual([
      'axon-connection://public-gcs/first-bucket',
      'axon-connection://public-gcs/second-bucket',
    ]);

    const sameConnection = upsertConnectedCatalog(
      distinctConnections.catalogs,
      sameConnectionOtherTable,
    );
    const firstBucket = sameConnection.catalogs.find((item) => item.id === firstConnection.id);
    expect(firstBucket?.schemas[0]?.tables).toHaveLength(2);
    expect(
      firstBucket?.schemas[0]?.tables.map((table) => table.logicalTable?.resource?.identity.value),
    ).toEqual(['gs://first-bucket/events', 'gs://first-bucket/archive/events']);

    saveConnectedCatalogs(sameConnection.catalogs);
    expect(
      loadConnectedCatalogs()[0]?.schemas[0]?.tables.map(
        (table) => table.logicalTable?.resource?.identity.value,
      ),
    ).toEqual(['gs://first-bucket/events', 'gs://first-bucket/archive/events']);

    firstBucket!.schemas[0]!.tables[1]!.uri = 'gs://tampered-bucket/archive/events';
    firstBucket!.schemas[0]!.tables[1]!.source!.storage = 'gs://tampered-bucket/archive/events';
    saveConnectedCatalogs(sameConnection.catalogs);

    expect(loadConnectedCatalogs()[0]?.schemas[0]?.tables[1]?.uri).toBe(
      'gs://first-bucket/archive/events',
    );
  });

  it('splits a legacy alias-merged record by generated connection identity', async () => {
    const first = await publicCatalog({
      bucket: 'first-bucket',
      path: 'events',
      alias: 'Workspace',
    });
    const second = await publicCatalog({
      bucket: 'second-bucket',
      path: 'events',
      alias: 'Workspace',
    });
    first.id = 'catalog-workspace';
    second.id = 'catalog-workspace';

    storage.setItem(STORAGE_KEY, JSON.stringify([first, second]));

    expect(
      loadConnectedCatalogs()
        .map((item) => item.id)
        .sort(),
    ).toEqual([
      'axon-connection://public-gcs/first-bucket',
      'axon-connection://public-gcs/second-bucket',
    ]);
  });

  it('migrates the exact parent-version local, GCS, and S3 durable records', () => {
    const legacyCatalogs: ConnectedCatalog[] = [
      legacyCatalog({
        source: 'local',
        tableName: 'local_events',
        storage: 'Local folder: events',
        localRegistryId: 'local-events-registry',
      }),
      legacyCatalog({
        source: 'gcs',
        tableName: 'gcs_events',
        storage: 'gs://public-events/delta/events',
      }),
      legacyCatalog({
        source: 's3',
        tableName: 's3_events',
        storage: 's3://public-events/delta/events',
        region: 'us-east-1',
      }),
    ];

    storage.setItem(STORAGE_KEY, JSON.stringify(legacyCatalogs));

    const loaded = loadConnectedCatalogs().sort((left, right) => left.id.localeCompare(right.id));
    expect(loaded.map((item) => item.id)).toEqual([
      'axon-connection://local-delta/local-events-registry',
      'axon-connection://public-gcs/public-events',
      'axon-connection://public-s3/us-east-1/public-events',
    ]);
    expect(
      loaded
        .flatMap(querySourcesForCatalog)
        .map((source) => source.kind)
        .sort(),
    ).toEqual(['local_delta', 'object_store_table_root', 'object_store_table_root']);
  });

  it('splits a mixed legacy alias record using each table source envelope', () => {
    const local = legacyCatalog({
      source: 'local',
      tableName: 'local_events',
      storage: 'Local folder: events',
      localRegistryId: 'local-events-registry',
    }).schemas[0]!.tables[0]!;
    const gcs = legacyCatalog({
      source: 'gcs',
      tableName: 'gcs_events',
      storage: 'gs://gcs-events/delta/events',
    }).schemas[0]!.tables[0]!;
    const s3 = legacyCatalog({
      source: 's3',
      tableName: 's3_events',
      storage: 's3://s3-events/delta/events',
      region: 'us-west-2',
    }).schemas[0]!.tables[0]!;
    const mixed = legacyCatalog({
      source: 'gcs',
      tableName: 'ignored',
      storage: 'gs://ignored/delta/table',
    });
    mixed.schemas = [
      { name: 'local_schema', tables: [local] },
      { name: 'gcs_schema', tables: [gcs] },
      { name: 's3_schema', tables: [s3] },
    ];

    storage.setItem(STORAGE_KEY, JSON.stringify([mixed]));

    const loaded = loadConnectedCatalogs().sort((left, right) => left.id.localeCompare(right.id));
    expect(loaded).toMatchObject([
      {
        id: 'axon-connection://local-delta/local-events-registry',
        catalogName: 'local-delta',
        kind: 'local',
        provider: undefined,
      },
      {
        id: 'axon-connection://public-gcs/gcs-events',
        catalogName: 'public-gcs',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://gcs-events/delta/events',
      },
      {
        id: 'axon-connection://public-s3/us-west-2/s3-events',
        catalogName: 'public-s3',
        kind: 'object_store',
        provider: 's3',
        storage: 's3://s3-events/delta/events',
        region: 'us-west-2',
      },
    ]);
    expect(loaded.every((item) => querySourcesForCatalog(item).length === 1)).toBe(true);
  });

  it('does not persist session-handle local Delta tables', () => {
    const durable = catalog('durable');
    const sessionOnly = catalog('session-only');
    sessionOnly.kind = 'local';
    sessionOnly.storage = 'local';
    sessionOnly.region = 'browser-local';
    sessionOnly.schemas[0].tables[0].localPersistence = 'session_handles';

    saveConnectedCatalogs([sessionOnly, durable]);

    const raw = storage.getItem(STORAGE_KEY);
    expect(raw).not.toBeNull();
    const persisted = JSON.parse(raw ?? '[]') as ConnectedCatalog[];
    expect(persisted.map((item) => item.id)).toEqual(['axon-connection://public-gcs/durable']);
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual([
      'axon-connection://public-gcs/durable',
    ]);
  });

  it('rejects identity-tampered generated metadata before write and after read', () => {
    const tampered = catalog('events');
    tampered.storage = 'gs://bucket/events';
    tampered.schemas[0].tables[0].uri = 'gs://bucket/events';
    tampered.schemas[0].tables[0].source!.storage = 'gs://bucket/events';
    tampered.schemas[0].tables[0].catalogMetadataJson = toJson(
      TableMetadataSchema,
      create(TableMetadataSchema, {
        storageLocation: 'gs://other-bucket/events',
      }),
    ) as Readonly<Record<string, unknown>>;

    storage.setItem(STORAGE_KEY, JSON.stringify([catalog('previous')]));
    saveConnectedCatalogs([tampered]);
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual([
      'axon-connection://public-gcs/previous',
    ]);

    storage.setItem(STORAGE_KEY, JSON.stringify([tampered]));
    expect(loadConnectedCatalogs()).toEqual([]);
  });

  it.each([
    [
      'unknown public provider',
      (candidate: ConnectedCatalog) => {
        candidate.provider = 'r2' as ConnectedCatalog['provider'];
      },
    ],
    [
      'missing local registry identity',
      (candidate: ConnectedCatalog) => {
        candidate.kind = 'local';
        candidate.provider = undefined;
        candidate.schemas[0].tables[0].source!.kind = 'local';
        candidate.schemas[0].tables[0].localRegistryId = undefined;
      },
    ],
  ])('does not skip generated metadata validation for an %s', (_label, mutate) => {
    const malformed = catalog('events');
    malformed.schemas[0].tables[0].catalogMetadataJson = { table: {} };
    mutate(malformed);

    storage.setItem(STORAGE_KEY, JSON.stringify([catalog('previous')]));
    saveConnectedCatalogs([malformed]);
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual([
      'axon-connection://public-gcs/previous',
    ]);

    storage.setItem(STORAGE_KEY, JSON.stringify([malformed]));
    expect(loadConnectedCatalogs()).toEqual([]);
  });

  it('merges repeated canonical table identities while retaining safe metadata', () => {
    const duplicate = catalog('events');
    duplicate.schemas[0].tables.push({
      ...duplicate.schemas[0].tables[0],
    });

    saveConnectedCatalogs([duplicate]);
    expect(loadConnectedCatalogs()).toHaveLength(1);
    expect(loadConnectedCatalogs()[0]?.schemas[0]?.tables).toHaveLength(1);
  });

  it('projects provider-generated local discovery and persists only normalized metadata JSON', async () => {
    const catalogDiscovery = await discoverFlatCatalog(
      createLocalDeltaCatalogProvider({
        registryId: 'local-registry',
        schemaName: 'analytics',
        tableName: 'events',
        metadata: create(TableMetadataSchema, {
          rowCount: 0n,
          sizeBytes: 0n,
          fileCount: 0n,
          latestSnapshotVersion: 0n,
          minReaderVersion: 1,
          minWriterVersion: 2,
          storageLocation: 'browser-local://delta-table/events',
        }),
      }),
      create(PageRequestSchema),
      {
        signal: new AbortController().signal,
        correlationId: 'store-provider-test',
      },
    );
    const result: ConnectResult = {
      source: 'local',
      alias: 'workspace',
      selection: { analytics: 'all' },
      catalogDiscovery,
      discovered: {
        summary: 'stale handwritten discovery',
        schemas: [
          {
            name: 'wrong',
            included: true,
            tableCount: 1,
            tables: [
              {
                name: 'wrong',
                snapshot: 9,
                rows: 9,
                files: 9,
                size: 'unsafe',
                protocol: 'wrong',
              },
            ],
          },
        ],
      },
      form: {
        path: 'Local folder: events',
        detected: null,
        localDelta: {
          kind: 'local_delta',
          registryId: 'local-registry',
          persistence: 'metadata_only_reselect',
          tableRootName: 'events',
          tableName: 'events',
          schemaName: 'analytics',
          storageLabel: 'Local folder: events',
          descriptor: {} as BrowserHttpSnapshotDescriptor,
          catalogMetadata: create(TableMetadataSchema),
        },
        localCatalogDiscovery: catalogDiscovery,
        provider: 'gcs',
        uri: '',
        region: '',
        endpoint: '',
        objectStorage: null,
        uc_mode: 'databricks',
        uc_host: '',
        uc_bff_url: '',
        uc_session_label: '',
        uc_catalog: '',
        uc_schema_filter: '',
        ds_mode: 'profile',
        ds_profile_name: '',
        ds_endpoint: '',
        ds_share: '',
      },
    };

    const connected = buildCatalogFromResult(result);
    const table = connected.schemas[0]?.tables[0];

    expect(connected.schemas).toHaveLength(1);
    expect(connected.schemas[0]?.name).toBe('analytics');
    expect(table).toMatchObject({
      name: 'events',
      snapshot: 0,
      rows: 0,
      files: 0,
      protocol: 'r1/w2',
      localRegistryId: 'local-registry',
    });
    expect(table?.catalogMetadataJson).toBeDefined();
    expect(JSON.stringify(table?.catalogMetadataJson)).not.toContain('blob:');
    expect(JSON.stringify(table?.catalogMetadataJson)).not.toContain('descriptor');
  });

  it('keeps public descriptor metrics beside generated metadata rather than inside it', async () => {
    const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
      tableUri: 'gs://bucket/table',
      snapshotVersion: 0n,
    });
    const catalogDiscovery = await discoverFlatCatalog(
      createPublicObjectStorageCatalogProvider({
        provider: 'gcs',
        connectionId: 'axon-connection://public-gcs/bucket',
        normalizedTableUri: descriptor.tableUri,
        schemaName: 'default',
        tableName: 'table',
        metadata: publicObjectStorageCatalogMetadata(descriptor),
      }),
      create(PageRequestSchema),
      {
        signal: new AbortController().signal,
        correlationId: 'store-public-provider-test',
      },
    );
    const metrics = {
      descriptor_resolution_count: 1,
      delta_log_manifest_list_count: 2,
      delta_log_manifest_list_duration_ms: 3,
      snapshot_resolve_count: 1,
      snapshot_resolve_duration_ms: 4,
    };
    const result: ConnectResult = {
      source: 'object_store',
      alias: 'workspace',
      selection: { default: 'all' },
      catalogDiscovery,
      discovered: { summary: 'stale', schemas: [] },
      form: {
        path: '',
        detected: null,
        localDelta: null,
        localCatalogDiscovery: null,
        provider: 'gcs',
        uri: descriptor.tableUri,
        region: 'us-central1',
        endpoint: '',
        objectStorage: {
          tableUri: descriptor.tableUri,
          tableName: 'table',
          descriptorResolutionMetrics: metrics,
          catalogDiscovery,
        },
        uc_mode: 'databricks',
        uc_host: '',
        uc_bff_url: '',
        uc_session_label: '',
        uc_catalog: '',
        uc_schema_filter: '',
        ds_mode: 'profile',
        ds_profile_name: '',
        ds_endpoint: '',
        ds_share: '',
      },
    };

    const table = buildCatalogFromResult(result).schemas[0]?.tables[0];

    expect(table?.descriptorResolutionMetrics).toEqual(metrics);
    expect(JSON.stringify(table?.catalogMetadataJson)).not.toContain('descriptor_resolution_count');
  });
});

function legacyCatalog(input: {
  source: 'local' | 'gcs' | 's3';
  tableName: string;
  storage: string;
  region?: string;
  localRegistryId?: string;
}): ConnectedCatalog {
  const kind = input.source === 'local' ? 'local' : 'object_store';
  const provider = input.source === 'local' ? undefined : input.source;
  const region = input.source === 'local' ? 'browser-local' : (input.region ?? 'us');
  return {
    id: 'catalog-workspace',
    alias: 'workspace',
    kind,
    provider,
    storage: input.storage,
    path: input.source === 'local' ? input.storage : undefined,
    region,
    status: 'connected',
    connectedAt: 'parent version',
    schemas: [
      {
        name: 'default',
        tables: [
          {
            id: `default.${input.tableName}`,
            name: input.tableName,
            snapshot: 7,
            rows: 11,
            files: 2,
            size: '12 bytes',
            protocol: 'r1/w2',
            uri: input.source === 'local' ? input.tableName : input.storage,
            localRegistryId: input.localRegistryId,
            localPersistence: input.source === 'local' ? 'metadata_only_reselect' : undefined,
            source: {
              id: `source-${input.tableName}`,
              kind,
              provider,
              storage: input.storage,
              path: input.source === 'local' ? input.storage : undefined,
              region,
              canonicalKey: [
                kind,
                provider ?? '',
                input.storage,
                '',
                input.source === 'local' ? input.storage : '',
                'default',
                input.tableName,
              ].join('|'),
              connectedAt: 'parent version',
            },
          },
        ],
      },
    ],
  };
}

async function publicCatalog({
  bucket,
  path,
  tableName = 'events',
  alias,
}: {
  bucket: string;
  path: string;
  tableName?: string;
  alias: string;
}): Promise<ConnectedCatalog> {
  const tableUri = `gs://${bucket}/${path}`;
  const connectionId = `axon-connection://public-gcs/${encodeURIComponent(bucket)}`;
  const descriptor = create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri,
    snapshotVersion: 1n,
  });
  const catalogDiscovery = await discoverFlatCatalog(
    createPublicObjectStorageCatalogProvider({
      provider: 'gcs',
      connectionId,
      normalizedTableUri: tableUri,
      schemaName: 'default',
      tableName,
      metadata: publicObjectStorageCatalogMetadata(descriptor),
    }),
    create(PageRequestSchema),
    {
      signal: new AbortController().signal,
      correlationId: `store-${bucket}-${path}`,
    },
  );
  return buildCatalogFromResult({
    source: 'object_store',
    alias,
    selection: { default: 'all' },
    catalogDiscovery,
    discovered: { summary: 'generated', schemas: [] },
    form: {
      path: '',
      detected: null,
      localDelta: null,
      localCatalogDiscovery: null,
      provider: 'gcs',
      uri: tableUri,
      region: 'us-central1',
      endpoint: '',
      objectStorage: {
        tableUri,
        tableName,
        catalogDiscovery,
      },
      uc_mode: 'databricks',
      uc_host: '',
      uc_bff_url: '',
      uc_session_label: '',
      uc_catalog: '',
      uc_schema_filter: '',
      ds_mode: 'profile',
      ds_profile_name: '',
      ds_endpoint: '',
      ds_share: '',
    },
  });
}
