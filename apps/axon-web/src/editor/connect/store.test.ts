import { create, toJson } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import { TableMetadataSchema } from '../../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import { PageRequestSchema } from '../../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
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
import type { ConnectedCatalog, ConnectResult } from './types.ts';
import {
  buildCatalogFromResult,
  loadConnectedCatalogs,
  saveConnectedCatalogs,
  SAMPLE_CONNECTED_CATALOG,
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
  return {
    id,
    alias,
    kind: 'object_store',
    provider: 'gcs',
    storage: `gs://${id}`,
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
            source: {
              id: `source-${id}`,
              kind: 'object_store',
              provider: 'gcs',
              storage: `gs://${id}`,
              region: 'us',
              canonicalKey: `object_store|gcs|gs://${id}|||default|${id}`,
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

  it('loads the sample catalog when persisted data is missing or malformed', () => {
    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);

    storage.setItem(STORAGE_KEY, '{');

    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);
  });

  it('dedupes persisted catalogs when loading from the stable storage key', () => {
    storage.setItem(
      STORAGE_KEY,
      JSON.stringify([catalog('old', 'workspace'), catalog('new', 'workspace')]),
    );

    const loaded = loadConnectedCatalogs();

    expect(loaded).toHaveLength(1);
    expect(loaded[0].alias).toBe('workspace');
    expect(loaded[0].schemas[0].tables.map((table) => table.name).sort()).toEqual(['new', 'old']);
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
    expect(persisted.map((item) => item.id)).toEqual(['durable']);
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual(['durable']);
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
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual(['previous']);

    storage.setItem(STORAGE_KEY, JSON.stringify([tampered]));
    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);
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
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual(['previous']);

    storage.setItem(STORAGE_KEY, JSON.stringify([malformed]));
    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);
  });

  it('rejects duplicate table identities instead of validating metadata through the first match', () => {
    const duplicate = catalog('events');
    duplicate.schemas[0].tables.push({
      ...duplicate.schemas[0].tables[0],
      catalogMetadataJson: { table: {} },
    });

    storage.setItem(STORAGE_KEY, JSON.stringify([catalog('previous')]));
    saveConnectedCatalogs([duplicate]);
    expect(loadConnectedCatalogs().map((item) => item.id)).toEqual(['previous']);

    storage.setItem(STORAGE_KEY, JSON.stringify([duplicate]));
    expect(loadConnectedCatalogs()).toEqual([SAMPLE_CONNECTED_CATALOG]);
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
