import { create } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import { TableMetadataSchema } from '../../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import { PageRequestSchema } from '../../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import type { BrowserHttpSnapshotDescriptor } from '../../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  createLocalDeltaCatalogProvider,
  discoverFlatCatalog,
} from '../../services/catalog-provider.ts';
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
});
