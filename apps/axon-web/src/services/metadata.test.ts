import { IDBFactory } from 'fake-indexeddb';
import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { appendHistory, loadHistory } from './history.ts';
import { clearMetadataRecords, getMetadataRecords, replaceMetadataRecords } from './metadata.ts';
import { loadSaved, saveQuery } from './saved.ts';
import type { HistoryEntry, SavedQuery } from './types.ts';

const HISTORY_LEGACY_KEY = 'axon-editor.history.v1';
const SAVED_LEGACY_KEY = 'axon-editor.saved.v1';
const indexedDB = new IDBFactory();

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

function installBrowserStorage(): MemoryStorage {
  const storage = new MemoryStorage();
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: storage,
  });
  return storage;
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

function savedQuery(id: string, edited: string): SavedQuery {
  return {
    id,
    name: id,
    owner: 'you',
    edited,
    target: 'browser_wasm',
    sql: `select '${id}'`,
  };
}

describe('metadata persistence', () => {
  beforeAll(() => {
    Object.defineProperty(globalThis, 'indexedDB', {
      configurable: true,
      value: indexedDB,
    });
  });

  beforeEach(async () => {
    installBrowserStorage();
    await clearMetadataRecords('history');
    await clearMetadataRecords('saved');
    await clearMetadataRecords('workspace');
  });

  it('keeps metadata records isolated across history, saved, and workspace stores', async () => {
    await replaceMetadataRecords('history', [historyEntry(1)]);
    await replaceMetadataRecords('saved', [savedQuery('h-1', '2026-01-02')]);
    await replaceMetadataRecords('workspace', [{ id: 'h-1', label: 'workspace' }]);

    expect(await getMetadataRecords<HistoryEntry>('history')).toEqual([historyEntry(1)]);
    expect(await getMetadataRecords<SavedQuery>('saved')).toEqual([
      savedQuery('h-1', '2026-01-02'),
    ]);
    expect(await getMetadataRecords<{ id: string; label: string }>('workspace')).toEqual([
      { id: 'h-1', label: 'workspace' },
    ]);
  });

  it('migrates legacy history in descending ISO order, caps records, and removes the legacy key', async () => {
    const storage = installBrowserStorage();
    const legacy = Array.from({ length: 102 }, (_, index) => historyEntry(index));
    storage.setItem(HISTORY_LEGACY_KEY, JSON.stringify(legacy));

    const loaded = await loadHistory();

    expect(loaded).toHaveLength(100);
    expect(loaded.slice(0, 3).map((entry) => entry.id)).toEqual(['h-101', 'h-100', 'h-99']);
    expect(loaded.at(-1)?.id).toBe('h-2');
    expect(storage.getItem(HISTORY_LEGACY_KEY)).toBeNull();
    expect(await getMetadataRecords<HistoryEntry>('history')).toHaveLength(100);
  });

  it('prefers existing indexed history and removes stale legacy history', async () => {
    const storage = installBrowserStorage();
    storage.setItem(HISTORY_LEGACY_KEY, JSON.stringify([historyEntry(1)]));
    await replaceMetadataRecords('history', [historyEntry(2)]);

    await expect(loadHistory()).resolves.toEqual([historyEntry(2)]);
    expect(storage.getItem(HISTORY_LEGACY_KEY)).toBeNull();
  });

  it('migrates legacy saved queries in edited order and removes the legacy key', async () => {
    const storage = installBrowserStorage();
    storage.setItem(
      SAVED_LEGACY_KEY,
      JSON.stringify([savedQuery('older', '2026-01-01'), savedQuery('newer', '2026-06-01')]),
    );

    await expect(loadSaved()).resolves.toEqual([
      savedQuery('newer', '2026-06-01'),
      savedQuery('older', '2026-01-01'),
    ]);
    expect(storage.getItem(SAVED_LEGACY_KEY)).toBeNull();
    expect((await getMetadataRecords<SavedQuery>('saved')).map((query) => query.id)).toEqual([
      'newer',
      'older',
    ]);
  });

  it('keeps appendHistory and saveQuery callable through the existing service APIs', async () => {
    const history = await appendHistory({
      ms: 12,
      rows: 1,
      status: 'ok',
      target: 'browser_wasm',
      sql: 'select 1',
    });
    const saved = await saveQuery({
      name: 'Example',
      sql: 'select 1',
      target: 'browser_wasm',
    });

    expect((await getMetadataRecords<HistoryEntry>('history'))[0]?.id).toBe(history.id);
    expect((await getMetadataRecords<SavedQuery>('saved'))[0]?.id).toBe(saved.id);
  });
});
