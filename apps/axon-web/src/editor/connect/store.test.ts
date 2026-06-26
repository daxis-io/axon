import { beforeEach, describe, expect, it } from 'vitest';
import type { ConnectedCatalog } from './types.ts';
import { loadConnectedCatalogs, saveConnectedCatalogs, SAMPLE_CONNECTED_CATALOG } from './store.ts';

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
});
