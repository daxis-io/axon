import { IDBFactory } from 'fake-indexeddb';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HandleStore, type LocalDeltaHandleStoreRecord } from '../persistence/handle-store.ts';
import {
  hasLocalDeltaRuntime,
  loadLocalDeltaRuntime,
  openLocalDeltaTableFromDirectoryHandle,
  openLocalDeltaTableFromFileList,
  releaseLocalDeltaObjectUrls,
  unregisterLocalDeltaRuntime,
  type LocalFileSystemDirectoryHandle,
  type LocalDeltaPersistenceMode,
} from './local-delta.ts';
import {
  loadConnectedCatalogs,
  saveConnectedCatalogs,
  SAMPLE_CONNECTED_CATALOG,
} from '../editor/connect/store.ts';
import type { ConnectedCatalog } from '../editor/connect/types.ts';
import { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';

vi.mock('../wasm/axon_web_wasm.js', () => ({
  default: vi.fn(async () => undefined),
  resolve_delta_snapshot_from_manifest: vi.fn(
    async (_manifestJson: string, tableUri: string, snapshotVersion?: number) =>
      JSON.stringify({
        table_uri: tableUri,
        snapshot_version: snapshotVersion ?? 0,
        partition_column_types: {},
        active_files: [
          {
            path: 'part-00001.parquet',
            size_bytes: 7,
            partition_values: {},
            stats: JSON.stringify({ numRecords: 1 }),
          },
        ],
      }),
  ),
}));

const ACTIVE_LOCAL_DELTA_ID_KEY = 'axon-local-delta-active-id';
const LOCAL_DELTA_DB_NAME = 'axon-local-delta-registry';
const LOCAL_DELTA_STORE = 'tables';

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

let indexedDB: IDBFactory;
let storage: MemoryStorage;
let createObjectUrl: ReturnType<typeof vi.fn>;
let revokeObjectUrl: ReturnType<typeof vi.fn>;

beforeEach(() => {
  vi.clearAllMocks();
  indexedDB = new IDBFactory();
  storage = new MemoryStorage();
  createObjectUrl = vi.fn((file: File) => `blob:${file.name}:${file.size}`);
  revokeObjectUrl = vi.fn();

  Object.defineProperty(globalThis, 'indexedDB', {
    configurable: true,
    value: indexedDB,
  });
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: storage,
  });
  Object.defineProperty(URL, 'createObjectURL', {
    configurable: true,
    value: createObjectUrl,
  });
  Object.defineProperty(URL, 'revokeObjectURL', {
    configurable: true,
    value: revokeObjectUrl,
  });
});

afterEach(async () => {
  await unregisterLocalDeltaRuntime('metadata-only').catch(() => undefined);
  await unregisterLocalDeltaRuntime('session-only').catch(() => undefined);
  await unregisterLocalDeltaRuntime('directory-access').catch(() => undefined);
  releaseLocalDeltaObjectUrls();
  vi.restoreAllMocks();
});

describe('local Delta registry persistence', () => {
  it('materializes generated capability-free catalog metadata from normalized log facts', async () => {
    const runtime = await openLocalDeltaTableFromFileList(deltaTableFiles(), {
      registryId: 'metadata-only',
    });

    expect(runtime.catalogMetadata.table).toBeUndefined();
    expect(runtime.catalogMetadata).toMatchObject({
      columns: [{ name: 'value', type: 'long', nullable: true }],
      partitionColumns: [],
      rowCount: 1n,
      sizeBytes: 7n,
      fileCount: 1n,
      latestSnapshotVersion: 0n,
      minReaderVersion: 1,
      minWriterVersion: 2,
      protocolFeatures: [],
      storageLocation: 'browser-local://delta-table/table-root',
    });
    const serialized = JSON.stringify(runtime.catalogMetadata, (_key, value) =>
      typeof value === 'bigint' ? value.toString() : value,
    );
    expect(serialized).not.toContain('blob:');
    expect(serialized).not.toContain('descriptor');
    expect(serialized).not.toContain('metadata-only');
  });

  it('persists file-list imports as metadata-only records without bytes or object URLs', async () => {
    const putSpy = vi.spyOn(HandleStore.prototype, 'put');

    const runtime = await openLocalDeltaTableFromFileList(deltaTableFiles(), {
      registryId: 'metadata-only',
    });

    expect(runtime.persistence).toBe('metadata_only_reselect');
    expect(runtime.descriptor.activeFiles[0].url).toBe('blob:part-00001.parquet:7');
    expect(storage.getItem(ACTIVE_LOCAL_DELTA_ID_KEY)).toBe('metadata-only');
    expect(putSpy).toHaveBeenCalledTimes(1);

    const seamRecord = putSpy.mock.calls[0]?.[0] as LocalDeltaHandleStoreRecord | undefined;
    expect(seamRecord).toMatchObject({
      id: 'metadata-only',
      tableRootName: 'table-root',
      backend: 'metadata_only',
    });
    expect(seamRecord?.files.map((file) => file.relativePath).sort()).toEqual([
      '_delta_log/00000000000000000000.json',
      'part-00001.parquet',
    ]);
    expect(Object.keys(seamRecord?.files[0] ?? {}).sort()).toEqual([
      'lastModified',
      'mimeType',
      'relativePath',
      'sizeBytes',
    ]);
    expect(JSON.stringify(seamRecord)).not.toContain('blob:');
    expect(JSON.stringify(seamRecord)).not.toContain('signed');

    const persisted = await new HandleStore({
      databaseName: LOCAL_DELTA_DB_NAME,
      version: 1,
      storeName: LOCAL_DELTA_STORE,
      indexedDB,
    }).get('metadata-only');
    expect(persisted).toEqual(seamRecord);
  });

  it('falls back to session-only imports when IndexedDB is unavailable', async () => {
    Object.defineProperty(globalThis, 'indexedDB', {
      configurable: true,
      value: undefined,
    });
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    const runtime = await openLocalDeltaTableFromFileList(deltaTableFiles(), {
      registryId: 'session-only',
    });

    expect(runtime.persistence).toBe('session_handles');
    expect(storage.getItem(ACTIVE_LOCAL_DELTA_ID_KEY)).toBeNull();
    expect(hasLocalDeltaRuntime('session-only')).toBe(true);
    await expect(loadLocalDeltaRuntime('session-only')).resolves.toMatchObject({
      registryId: 'session-only',
      persistence: 'session_handles',
    });

    const durableCatalog = connectedCatalog('durable', 'metadata_only_reselect');
    saveConnectedCatalogs([connectedCatalog('session-only', runtime.persistence), durableCatalog]);
    expect(loadConnectedCatalogs().map((catalog) => catalog.id)).toEqual(['durable']);
  });

  it('materializes a requested snapshot once and reuses its generated descriptor and Blob URLs', async () => {
    Object.defineProperty(globalThis, 'indexedDB', {
      configurable: true,
      value: undefined,
    });
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    await openLocalDeltaTableFromFileList(deltaTableFiles(), {
      registryId: 'session-only',
    });

    const pinned = await loadLocalDeltaRuntime('session-only', {
      schemaName: 'default',
      tableName: 'events',
      snapshotVersion: 12,
    });
    const objectUrlCallsAfterPin = createObjectUrl.mock.calls.length;
    const reused = await loadLocalDeltaRuntime('session-only', {
      schemaName: 'default',
      tableName: 'events',
      snapshotVersion: 12,
    });

    expect(resolve_delta_snapshot_from_manifest).toHaveBeenCalledWith(
      expect.any(String),
      'browser-local://delta-table/table-root',
      12,
    );
    expect(pinned.descriptor).toMatchObject({
      tableUri: 'browser-local://delta-table/table-root',
      snapshotVersion: 12n,
      activeFiles: [
        {
          path: 'part-00001.parquet',
          url: 'blob:part-00001.parquet:7',
          sizeBytes: 7n,
        },
      ],
    });
    expect(reused.descriptor).toBe(pinned.descriptor);
    expect(createObjectUrl).toHaveBeenCalledTimes(objectUrlCallsAfterPin);
  });

  it('rechecks a retained directory handle on every execution without rebuilding the runtime', async () => {
    vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const queryPermission = vi.fn(async () => 'granted' as PermissionState);
    const directory = directoryHandle(queryPermission);
    const opened = await openLocalDeltaTableFromDirectoryHandle(directory, {
      registryId: 'directory-access',
      tableName: 'events',
    });
    const objectUrlCallsAfterOpen = createObjectUrl.mock.calls.length;

    const first = await loadLocalDeltaRuntime('directory-access', {
      tableName: 'events',
    });
    const second = await loadLocalDeltaRuntime('directory-access', {
      tableName: 'events',
    });

    expect(queryPermission).toHaveBeenCalledTimes(2);
    expect(first.descriptor).toBe(opened.descriptor);
    expect(second.descriptor).toBe(opened.descriptor);
    expect(createObjectUrl).toHaveBeenCalledTimes(objectUrlCallsAfterOpen);
  });
});

function deltaTableFiles(): File[] {
  return [
    fileWithBrowserPath(
      '_delta_log/00000000000000000000.json',
      [
        JSON.stringify({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }),
        JSON.stringify({
          metaData: {
            name: 'table_from_log',
            schemaString: JSON.stringify({
              type: 'struct',
              fields: [{ name: 'value', type: 'long', nullable: true, metadata: {} }],
            }),
            partitionColumns: [],
            configuration: {},
          },
        }),
      ].join('\n'),
      'application/json',
    ),
    fileWithBrowserPath('part-00001.parquet', 'parquet', 'application/vnd.apache.parquet'),
  ];
}

function fileWithBrowserPath(relativePath: string, body: string, type: string): File {
  const file = new File([body], relativePath.split('/').at(-1) ?? relativePath, {
    type,
    lastModified: 1_797_000_000_000,
  });
  Object.defineProperty(file, 'webkitRelativePath', {
    configurable: true,
    value: `table-root/${relativePath}`,
  });
  return file;
}

function connectedCatalog(id: string, persistence: LocalDeltaPersistenceMode): ConnectedCatalog {
  return {
    ...SAMPLE_CONNECTED_CATALOG,
    id,
    alias: id,
    kind: 'local',
    storage: 'local',
    region: 'browser-local',
    schemas: [
      {
        name: 'default',
        tables: [
          {
            id: `default.${id}`,
            name: id,
            snapshot: 0,
            rows: 1,
            files: 1,
            size: '7 bytes',
            protocol: 'r1/w2',
            uri: `browser-local://delta-table/${id}`,
            localPersistence: persistence,
          },
        ],
      },
    ],
  };
}

function directoryHandle(
  queryPermission: () => Promise<PermissionState>,
): LocalFileSystemDirectoryHandle {
  const files = deltaTableFiles();
  const commit = files.find((file) => file.name.endsWith('.json'))!;
  const data = files.find((file) => file.name.endsWith('.parquet'))!;
  const logDirectory: LocalFileSystemDirectoryHandle = {
    kind: 'directory',
    name: '_delta_log',
    async *entries() {
      yield [
        '00000000000000000000.json',
        {
          kind: 'file' as const,
          name: '00000000000000000000.json',
          getFile: async () => commit,
        },
      ];
    },
  };
  return {
    kind: 'directory',
    name: 'table-root',
    queryPermission,
    async *entries() {
      yield ['_delta_log', logDirectory];
      yield [
        'part-00001.parquet',
        {
          kind: 'file' as const,
          name: 'part-00001.parquet',
          getFile: async () => data,
        },
      ];
    },
  };
}
