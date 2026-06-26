import { IDBFactory } from 'fake-indexeddb';
import { describe, expect, it } from 'vitest';
import {
  HandleStore,
  ensureDirectoryReadPermission,
  type LocalDeltaHandleStoreRecord,
} from './handle-store.ts';

type TestDirectoryHandle = {
  readonly kind: 'directory';
  readonly name: string;
  queryPermission?: (descriptor?: { mode?: 'read' }) => Promise<PermissionState>;
  requestPermission?: (descriptor?: { mode?: 'read' }) => Promise<PermissionState>;
};

function createStore(databaseName: string): HandleStore<TestDirectoryHandle> {
  return new HandleStore<TestDirectoryHandle>({
    databaseName,
    version: 1,
    storeName: 'tables',
    indexedDB: new IDBFactory(),
  });
}

function metadataRecord(id: string): LocalDeltaHandleStoreRecord<TestDirectoryHandle> {
  return {
    id,
    tableRootName: 'table-root',
    importedAtEpochMs: 1_797_000_000_000,
    backend: 'metadata_only',
    files: [
      {
        relativePath: '_delta_log/00000000000000000000.json',
        sizeBytes: 12,
        lastModified: 1_797_000_000_001,
        mimeType: 'application/json',
      },
      {
        relativePath: 'part-00001.parquet',
        sizeBytes: 48,
        lastModified: 1_797_000_000_002,
        mimeType: 'application/vnd.apache.parquet',
      },
    ],
  };
}

describe('HandleStore', () => {
  it('persists metadata-only local Delta registry records without byte-bearing fields', async () => {
    const store = createStore('axon-handle-store-metadata');
    const record = metadataRecord('metadata-only');

    await store.put(record);

    const stored = await store.get('metadata-only');
    expect(stored).toEqual(record);
    expect(Object.keys(stored?.files[0] ?? {}).sort()).toEqual([
      'lastModified',
      'mimeType',
      'relativePath',
      'sizeBytes',
    ]);
    expect(JSON.stringify(stored)).not.toContain('blob:');
    expect(JSON.stringify(stored)).not.toContain('signed');
  });

  it('persists directory-handle registry records using the same wire shape', async () => {
    const store = createStore('axon-handle-store-directory');
    const directoryHandle: TestDirectoryHandle = { kind: 'directory', name: 'table-root' };
    const record: LocalDeltaHandleStoreRecord<TestDirectoryHandle> = {
      ...metadataRecord('directory-handle'),
      backend: 'directory_handle',
      directoryHandle,
    };

    await store.put(record);

    expect(await store.get('directory-handle')).toEqual(record);
    expect(await store.getAll()).toEqual([record]);
  });

  it('deletes individual records and clears the handle store', async () => {
    const store = createStore('axon-handle-store-delete-clear');
    await store.put(metadataRecord('first'));
    await store.put(metadataRecord('second'));

    await store.delete('first');
    expect(await store.get('first')).toBeUndefined();
    expect((await store.getAll()).map((record) => record.id)).toEqual(['second']);

    await store.clear();
    expect(await store.getAll()).toEqual([]);
  });
});

describe('ensureDirectoryReadPermission', () => {
  it('accepts already-granted read permission without prompting', async () => {
    const calls: string[] = [];
    const handle: TestDirectoryHandle = {
      kind: 'directory',
      name: 'table-root',
      queryPermission: async (descriptor) => {
        calls.push(`query:${descriptor?.mode}`);
        return 'granted';
      },
      requestPermission: async () => {
        calls.push('request');
        return 'denied';
      },
    };

    await expect(ensureDirectoryReadPermission(handle)).resolves.toBe(true);
    expect(calls).toEqual(['query:read']);
  });

  it('prompts when permission is not already granted and accepts a granted prompt', async () => {
    const calls: string[] = [];
    const handle: TestDirectoryHandle = {
      kind: 'directory',
      name: 'table-root',
      queryPermission: async (descriptor) => {
        calls.push(`query:${descriptor?.mode}`);
        return 'prompt';
      },
      requestPermission: async (descriptor) => {
        calls.push(`request:${descriptor?.mode}`);
        return 'granted';
      },
    };

    await expect(ensureDirectoryReadPermission(handle)).resolves.toBe(true);
    expect(calls).toEqual(['query:read', 'request:read']);
  });

  it('treats denied permission and throwing prompts as unavailable', async () => {
    const denied: TestDirectoryHandle = {
      kind: 'directory',
      name: 'denied',
      queryPermission: async () => 'denied',
      requestPermission: async () => 'denied',
    };
    const throwingPrompt: TestDirectoryHandle = {
      kind: 'directory',
      name: 'throws',
      queryPermission: async () => 'prompt',
      requestPermission: async () => {
        throw new Error('prompt unavailable');
      },
    };

    await expect(ensureDirectoryReadPermission(denied)).resolves.toBe(false);
    await expect(ensureDirectoryReadPermission(throwingPrompt)).resolves.toBe(false);
  });
});
