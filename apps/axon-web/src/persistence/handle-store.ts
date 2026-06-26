import { KeyValueStore } from './key-value.ts';

export type LocalDeltaHandleStoreBackend = 'metadata_only' | 'directory_handle';

export type LocalDeltaHandleFileRecord = {
  relativePath: string;
  sizeBytes: number;
  lastModified?: number;
  mimeType?: string;
};

export type LocalDeltaHandleStoreRecord<TDirectoryHandle = unknown> = {
  id: string;
  tableRootName: string;
  importedAtEpochMs: number;
  backend: LocalDeltaHandleStoreBackend;
  files: LocalDeltaHandleFileRecord[];
  directoryHandle?: TDirectoryHandle;
};

export type HandleStoreOptions = {
  databaseName: string;
  version: number;
  storeName: string;
  indexedDB?: IDBFactory;
};

export class HandleStore<TDirectoryHandle = unknown> {
  private readonly records: KeyValueStore<LocalDeltaHandleStoreRecord<TDirectoryHandle>>;

  constructor(options: HandleStoreOptions) {
    this.records = new KeyValueStore<LocalDeltaHandleStoreRecord<TDirectoryHandle>>({
      databaseName: options.databaseName,
      version: options.version,
      namespace: options.storeName,
      stores: [options.storeName],
      indexedDB: options.indexedDB,
    });
  }

  get(id: string): Promise<LocalDeltaHandleStoreRecord<TDirectoryHandle> | undefined> {
    return this.records.get(id);
  }

  getAll(): Promise<LocalDeltaHandleStoreRecord<TDirectoryHandle>[]> {
    return this.records.getAll();
  }

  put(record: LocalDeltaHandleStoreRecord<TDirectoryHandle>): Promise<void> {
    return this.records.put(record);
  }

  delete(id: string): Promise<void> {
    return this.records.delete(id);
  }

  clear(): Promise<void> {
    return this.records.clear();
  }
}

export type DirectoryReadPermissionHandle = {
  queryPermission?(descriptor?: { mode?: 'read' }): Promise<PermissionState>;
  requestPermission?(descriptor?: { mode?: 'read' }): Promise<PermissionState>;
};

export async function ensureDirectoryReadPermission(
  handle: DirectoryReadPermissionHandle,
): Promise<boolean> {
  if (!handle.queryPermission) return true;

  let current: PermissionState;
  try {
    current = await handle.queryPermission({ mode: 'read' });
  } catch {
    return false;
  }

  if (current === 'granted') return true;
  if (!handle.requestPermission) return false;

  try {
    return (await handle.requestPermission({ mode: 'read' })) === 'granted';
  } catch {
    return false;
  }
}
