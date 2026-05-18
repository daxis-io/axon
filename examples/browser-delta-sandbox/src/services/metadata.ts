export const METADATA_DB_NAME = 'axon-editor-metadata';

const METADATA_DB_VERSION = 1;
const STORE_NAMES = ['history', 'saved', 'workspace'] as const;

export type MetadataStoreName = (typeof STORE_NAMES)[number];

let dbPromise: Promise<IDBDatabase> | undefined;

function openMetadataDb(): Promise<IDBDatabase> {
  dbPromise ??= new Promise((resolve, reject) => {
    const request = indexedDB.open(METADATA_DB_NAME, METADATA_DB_VERSION);

    request.onerror = () => reject(request.error ?? new Error('metadata database open failed'));
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = () => {
      const db = request.result;
      for (const store of STORE_NAMES) {
        if (!db.objectStoreNames.contains(store)) {
          db.createObjectStore(store, { keyPath: 'id' });
        }
      }
    };
  });

  return dbPromise;
}

export async function getMetadataRecords<T>(store: MetadataStoreName): Promise<T[]> {
  const db = await openMetadataDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, 'readonly');
    const request = tx.objectStore(store).getAll();
    request.onerror = () => reject(request.error ?? new Error(`metadata ${store} read failed`));
    request.onsuccess = () => resolve(request.result as T[]);
  });
}

export async function replaceMetadataRecords<T extends { id: string }>(
  store: MetadataStoreName,
  records: T[],
): Promise<void> {
  const db = await openMetadataDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, 'readwrite');
    const objectStore = tx.objectStore(store);

    objectStore.clear();
    for (const record of records) {
      objectStore.put(record);
    }

    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error(`metadata ${store} write failed`));
    tx.onabort = () => reject(tx.error ?? new Error(`metadata ${store} write aborted`));
  });
}

export async function clearMetadataRecords(store: MetadataStoreName): Promise<void> {
  const db = await openMetadataDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(store, 'readwrite');
    const request = tx.objectStore(store).clear();
    request.onerror = () => reject(request.error ?? new Error(`metadata ${store} clear failed`));
    request.onsuccess = () => resolve();
  });
}
