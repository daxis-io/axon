import { QueryClient } from '@tanstack/react-query';
import { IDBFactory } from 'fake-indexeddb';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  clearMetadataRecords,
  getMetadataRecords,
  replaceMetadataRecords,
} from '../services/metadata.ts';
import type { HistoryEntry, SavedQuery } from '../services/types.ts';
import {
  appendHistoryEntry,
  historyQueryOptions,
  saveSavedQuery,
  savedQueriesQueryOptions,
} from './local';
import { queryKeys } from './keys';

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

function installBrowserStorage(indexedDB: IDBFactory | undefined = new IDBFactory()): void {
  Object.defineProperty(globalThis, 'indexedDB', {
    configurable: true,
    value: indexedDB,
  });
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: new MemoryStorage(),
  });
}

function historyEntry(index: number): HistoryEntry {
  const iso = new Date(Date.UTC(2026, 0, 1, 0, index)).toISOString();
  return {
    id: `h-${index}`,
    time: '00:00:00',
    iso,
    ms: index,
    rows: index,
    status: 'ok',
    target: 'browser_wasm',
    sql: `select ${index}`,
  };
}

function savedQuery(id: string, name = id): SavedQuery {
  return {
    id,
    name,
    owner: 'you',
    edited: '12:00',
    target: 'browser_wasm',
    sql: `select '${id}'`,
  };
}

describe('local metadata query adapters', () => {
  beforeEach(async () => {
    installBrowserStorage();
    await clearMetadataRecords('history');
    await clearMetadataRecords('saved');
    await clearMetadataRecords('workspace');
  });

  it('loads history and saved query data through query options', async () => {
    const history = [historyEntry(2), historyEntry(1)];
    const saved = [savedQuery('saved-a')];
    const client = new QueryClient();

    await replaceMetadataRecords('history', history);
    await replaceMetadataRecords('saved', saved);

    expect(historyQueryOptions().queryKey).toEqual(queryKeys.local.history());
    expect(savedQueriesQueryOptions().queryKey).toEqual(queryKeys.local.saved());
    await expect(client.fetchQuery(historyQueryOptions())).resolves.toEqual(history);
    await expect(client.fetchQuery(savedQueriesQueryOptions())).resolves.toEqual(saved);
  });

  it('updates the history query cache with appended entries', async () => {
    const client = new QueryClient();
    const existing = historyEntry(1);
    client.setQueryData(queryKeys.local.history(), [existing]);

    const appended = await appendHistoryEntry(client, {
      ms: 25,
      rows: 3,
      status: 'ok',
      target: 'browser_wasm',
      fallback: null,
      sql: 'select 3',
    });

    expect(client.getQueryData(queryKeys.local.history())).toEqual([appended, existing]);
    expect((await getMetadataRecords<HistoryEntry>('history'))[0]?.id).toBe(appended.id);
  });

  it('updates saved query cache entries and replaces older entries with the same name', async () => {
    const client = new QueryClient();
    const oldShared = savedQuery('old-shared', 'Shared');
    const other = savedQuery('other', 'Other');
    client.setQueryData(queryKeys.local.saved(), [oldShared, other]);

    const saved = await saveSavedQuery(client, {
      name: 'Shared',
      sql: 'select 42',
      target: 'browser_wasm',
    });

    expect(client.getQueryData(queryKeys.local.saved())).toEqual([saved, other]);
    expect((await getMetadataRecords<SavedQuery>('saved')).map((query) => query.name)).toEqual([
      'Shared',
    ]);
  });

  it('keeps cache updates when IndexedDB is unavailable during mutations', async () => {
    installBrowserStorage(undefined);
    const client = new QueryClient();

    const appended = await appendHistoryEntry(client, {
      ms: 25,
      rows: 0,
      status: 'error',
      target: 'browser_wasm',
      fallback: null,
      sql: 'select broken',
    });
    const saved = await saveSavedQuery(client, {
      name: 'Fallback',
      sql: 'select fallback',
      target: 'browser_wasm',
    });

    expect(client.getQueryData(queryKeys.local.history())).toEqual([appended]);
    expect(client.getQueryData(queryKeys.local.saved())).toEqual([saved]);
  });
});
