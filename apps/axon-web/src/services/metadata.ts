import { KeyValueStore, type KeyValueRecord } from '../persistence/key-value.ts';

export const METADATA_DB_NAME = 'axon-editor-metadata';

const METADATA_DB_VERSION = 1;
const STORE_NAMES = ['history', 'saved', 'workspace'] as const;

export type MetadataStoreName = (typeof STORE_NAMES)[number];

const metadataStores: Record<MetadataStoreName, KeyValueStore<KeyValueRecord>> = Object.fromEntries(
  STORE_NAMES.map((store) => [
    store,
    new KeyValueStore<KeyValueRecord>({
      databaseName: METADATA_DB_NAME,
      version: METADATA_DB_VERSION,
      namespace: store,
      stores: STORE_NAMES,
    }),
  ]),
) as Record<MetadataStoreName, KeyValueStore<KeyValueRecord>>;

export async function getMetadataRecords<T>(store: MetadataStoreName): Promise<T[]> {
  return (await metadataStores[store].getAll()) as T[];
}

export async function replaceMetadataRecords<T extends { id: string }>(
  store: MetadataStoreName,
  records: T[],
): Promise<void> {
  await metadataStores[store].replaceAll(records);
}

export async function clearMetadataRecords(store: MetadataStoreName): Promise<void> {
  await metadataStores[store].clear();
}
