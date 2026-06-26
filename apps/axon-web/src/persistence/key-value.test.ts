import { IDBFactory } from 'fake-indexeddb';
import { describe, expect, it } from 'vitest';
import { KeyValueStore, createLocalStorageKeyValueStore } from './key-value.ts';

type TestRecord = {
  id: string;
  value: string;
};

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

describe('KeyValueStore', () => {
  it('keeps records with the same id isolated by namespace', async () => {
    const indexedDB = new IDBFactory();
    const databaseName = 'axon-key-value-isolation';
    const stores = ['history', 'saved'] as const;
    const history = new KeyValueStore<TestRecord>({
      databaseName,
      version: 1,
      namespace: 'history',
      stores,
      indexedDB,
    });
    const saved = new KeyValueStore<TestRecord>({
      databaseName,
      version: 1,
      namespace: 'saved',
      stores,
      indexedDB,
    });

    await history.put({ id: 'shared', value: 'history' });
    await saved.put({ id: 'shared', value: 'saved' });

    expect(await history.get('shared')).toEqual({ id: 'shared', value: 'history' });
    expect(await saved.get('shared')).toEqual({ id: 'shared', value: 'saved' });
    expect(await history.getAll()).toEqual([{ id: 'shared', value: 'history' }]);

    await history.replaceAll([
      { id: 'first', value: '1' },
      { id: 'second', value: '2' },
    ]);
    await history.delete('first');
    expect(await history.getAll()).toEqual([{ id: 'second', value: '2' }]);

    await history.clear();
    expect(await history.getAll()).toEqual([]);
    expect(await saved.get('shared')).toEqual({ id: 'shared', value: 'saved' });
  });

  it('adds stores on version upgrade without dropping existing namespace data', async () => {
    const indexedDB = new IDBFactory();
    const databaseName = 'axon-key-value-upgrade';
    const v1History = new KeyValueStore<TestRecord>({
      databaseName,
      version: 1,
      namespace: 'history',
      stores: ['history'],
      indexedDB,
    });

    await v1History.put({ id: 'kept', value: 'v1' });

    const v2Workspace = new KeyValueStore<TestRecord>({
      databaseName,
      version: 2,
      namespace: 'workspace',
      stores: ['history', 'workspace'],
      indexedDB,
    });
    await v2Workspace.put({ id: 'kept', value: 'workspace' });

    const v2History = new KeyValueStore<TestRecord>({
      databaseName,
      version: 2,
      namespace: 'history',
      stores: ['history', 'workspace'],
      indexedDB,
    });

    expect(await v2History.get('kept')).toEqual({ id: 'kept', value: 'v1' });
    expect(await v2Workspace.get('kept')).toEqual({ id: 'kept', value: 'workspace' });
  });
});

describe('createLocalStorageKeyValueStore', () => {
  it('falls back on unavailable data and applies write transforms synchronously', () => {
    const storage = new MemoryStorage();
    const store = createLocalStorageKeyValueStore<TestRecord>({
      storageKey: 'axon.connect.catalogs.v1',
      storage,
      fallback: () => [{ id: 'sample', value: 'fixture' }],
      beforeWrite: (records) => records.filter((record) => record.value !== 'session_handles'),
    });

    expect(store.getAll()).toEqual([{ id: 'sample', value: 'fixture' }]);

    storage.setItem('axon.connect.catalogs.v1', '{');
    expect(store.getAll()).toEqual([{ id: 'sample', value: 'fixture' }]);

    store.replaceAll([
      { id: 'durable', value: 'object_store' },
      { id: 'ephemeral', value: 'session_handles' },
    ]);

    expect(JSON.parse(storage.getItem('axon.connect.catalogs.v1') ?? '[]')).toEqual([
      { id: 'durable', value: 'object_store' },
    ]);
    expect(store.get('durable')).toEqual({ id: 'durable', value: 'object_store' });
    expect(store.get('ephemeral')).toBeUndefined();
  });
});
