export type KeyValueRecord = { id: string };

export type KeyValueStoreOptions = {
  databaseName: string;
  version: number;
  namespace: string;
  stores?: readonly string[];
  indexedDB?: IDBFactory;
};

export class KeyValueStore<T extends KeyValueRecord> {
  private readonly databaseName: string;
  private readonly version: number;
  private readonly namespace: string;
  private readonly stores: readonly string[];
  private readonly indexedDB?: IDBFactory;

  constructor(options: KeyValueStoreOptions) {
    if (options.version < 1) {
      throw new Error('IndexedDB version must be at least 1');
    }
    this.databaseName = options.databaseName;
    this.version = options.version;
    this.namespace = options.namespace;
    this.stores = uniqueStoreNames([...(options.stores ?? []), options.namespace]);
    this.indexedDB = options.indexedDB;
  }

  async get(id: string): Promise<T | undefined> {
    return this.runTransaction<T | undefined>('readonly', (objectStore, setResult) => {
      const request = objectStore.get(id);
      request.onsuccess = () => setResult(request.result as T | undefined);
    });
  }

  async getAll(): Promise<T[]> {
    return this.runTransaction<T[]>('readonly', (objectStore, setResult) => {
      const request = objectStore.getAll();
      request.onsuccess = () => setResult(request.result as T[]);
    });
  }

  async put(record: T): Promise<void> {
    await this.runTransaction<void>('readwrite', (objectStore) => {
      objectStore.put(record);
    });
  }

  async replaceAll(records: T[]): Promise<void> {
    await this.runTransaction<void>('readwrite', (objectStore) => {
      objectStore.clear();
      for (const record of records) {
        objectStore.put(record);
      }
    });
  }

  async delete(id: string): Promise<void> {
    await this.runTransaction<void>('readwrite', (objectStore) => {
      objectStore.delete(id);
    });
  }

  async clear(): Promise<void> {
    await this.runTransaction<void>('readwrite', (objectStore) => {
      objectStore.clear();
    });
  }

  private openDb(): Promise<IDBDatabase> {
    const indexedDB = this.indexedDB ?? globalIndexedDB();
    if (!indexedDB) {
      return Promise.reject(new Error('IndexedDB is unavailable'));
    }

    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.databaseName, this.version);

      request.onerror = () => reject(request.error ?? new Error('IndexedDB open failed'));
      request.onsuccess = () => {
        const db = request.result;
        db.onversionchange = () => db.close();
        resolve(db);
      };
      request.onupgradeneeded = () => {
        const db = request.result;
        for (const store of this.stores) {
          if (!db.objectStoreNames.contains(store)) {
            db.createObjectStore(store, { keyPath: 'id' });
          }
        }
      };
    });
  }

  private async runTransaction<R>(
    mode: IDBTransactionMode,
    operation: (objectStore: IDBObjectStore, setResult: (result: R) => void) => void,
  ): Promise<R> {
    const db = await this.openDb();
    return new Promise((resolve, reject) => {
      let result: R;
      let settled = false;
      const fail = (error: unknown) => {
        if (settled) return;
        settled = true;
        db.close();
        reject(error);
      };

      try {
        const tx = db.transaction(this.namespace, mode);
        tx.oncomplete = () => {
          if (settled) return;
          settled = true;
          db.close();
          resolve(result);
        };
        tx.onerror = () => fail(tx.error ?? new Error(`IndexedDB ${this.namespace} failed`));
        tx.onabort = () => fail(tx.error ?? new Error(`IndexedDB ${this.namespace} aborted`));
        operation(tx.objectStore(this.namespace), (value) => {
          result = value;
        });
      } catch (error) {
        fail(error);
      }
    });
  }
}

type SyncStorage = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

export type LocalStorageKeyValueStoreOptions<T extends KeyValueRecord> = {
  storageKey: string;
  storage?: SyncStorage;
  fallback?: () => T[];
  afterRead?: (records: T[]) => T[];
  beforeWrite?: (records: T[]) => T[];
};

export type SyncKeyValueStore<T extends KeyValueRecord> = {
  get(id: string): T | undefined;
  getAll(): T[];
  put(record: T): void;
  replaceAll(records: T[]): void;
  delete(id: string): void;
  clear(): void;
};

export function createLocalStorageKeyValueStore<T extends KeyValueRecord>(
  options: LocalStorageKeyValueStoreOptions<T>,
): SyncKeyValueStore<T> {
  const fallback = () => options.fallback?.() ?? [];
  const readAll = (): T[] => {
    try {
      const storage = options.storage ?? globalLocalStorage();
      if (!storage) return fallback();
      const raw = storage.getItem(options.storageKey);
      if (!raw) return fallback();
      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) return fallback();
      return options.afterRead?.(parsed as T[]) ?? (parsed as T[]);
    } catch {
      return fallback();
    }
  };

  const writeAll = (records: T[]) => {
    try {
      const storage = options.storage ?? globalLocalStorage();
      if (!storage) return;
      storage.setItem(
        options.storageKey,
        JSON.stringify(options.beforeWrite?.(records) ?? records),
      );
    } catch {
      // localStorage can be disabled, quota-limited, or unavailable in private modes.
    }
  };

  return {
    get(id) {
      return readAll().find((record) => record.id === id);
    },
    getAll: readAll,
    put(record) {
      writeAll([record, ...readAll().filter((existing) => existing.id !== record.id)]);
    },
    replaceAll: writeAll,
    delete(id) {
      writeAll(readAll().filter((record) => record.id !== id));
    },
    clear() {
      try {
        const storage = options.storage ?? globalLocalStorage();
        storage?.removeItem(options.storageKey);
      } catch {
        // ignore
      }
    },
  };
}

function uniqueStoreNames(stores: string[]): readonly string[] {
  return Array.from(new Set(stores));
}

function globalIndexedDB(): IDBFactory | undefined {
  try {
    return globalThis.indexedDB;
  } catch {
    return undefined;
  }
}

function globalLocalStorage(): SyncStorage | undefined {
  try {
    return globalThis.localStorage;
  } catch {
    return undefined;
  }
}
