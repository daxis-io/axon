import type { DehydrateOptions, Query, QueryKey } from '@tanstack/react-query';
import type {
  PersistQueryClientOptions,
  PersistedClient,
  Persister,
} from '@tanstack/react-query-persist-client';
import { KeyValueStore } from '../persistence/key-value.ts';

export const AXON_QUERY_CACHE_SCHEMA_VERSION = 2;
export const AXON_QUERY_CACHE_APP_VERSION = 'axon-web@0.1.0';
export const AXON_QUERY_CACHE_BUSTER = `${AXON_QUERY_CACHE_APP_VERSION}/query-cache:v${AXON_QUERY_CACHE_SCHEMA_VERSION}`;
export const AXON_QUERY_CACHE_MAX_AGE_MS = 6 * 60 * 60 * 1000;

const QUERY_CACHE_DATABASE_NAME = 'axon-query-cache';
const QUERY_CACHE_NAMESPACE = 'query-cache';
const QUERY_CACHE_RECORD_ID = 'default';

type AxonQueryCacheRecord = {
  id: string;
  client: PersistedClient;
};

export type AxonQueryCachePersisterOptions = {
  indexedDB?: IDBFactory;
};

export type AxonQueryPersistOptions = Omit<PersistQueryClientOptions, 'queryClient'> & {
  buster: string;
  dehydrateOptions: DehydrateOptions;
  maxAge: number;
  persister: Persister;
};

export function createAxonQueryCachePersister(
  options: AxonQueryCachePersisterOptions = {},
): Persister {
  const store = new KeyValueStore<AxonQueryCacheRecord>({
    databaseName: QUERY_CACHE_DATABASE_NAME,
    version: AXON_QUERY_CACHE_SCHEMA_VERSION,
    namespace: QUERY_CACHE_NAMESPACE,
    indexedDB: options.indexedDB,
  });

  return {
    async persistClient(client) {
      try {
        await store.put({ id: QUERY_CACHE_RECORD_ID, client });
      } catch {
        // IndexedDB can be unavailable or quota-limited; query persistence is best-effort.
      }
    },
    async restoreClient() {
      try {
        return (await store.get(QUERY_CACHE_RECORD_ID))?.client;
      } catch {
        return undefined;
      }
    },
    async removeClient() {
      try {
        await store.delete(QUERY_CACHE_RECORD_ID);
      } catch {
        // ignore
      }
    },
  };
}

export function createAxonQueryPersistOptions(
  options: AxonQueryCachePersisterOptions & { persister?: Persister } = {},
): AxonQueryPersistOptions {
  return {
    persister: options.persister ?? createAxonQueryCachePersister(options),
    maxAge: AXON_QUERY_CACHE_MAX_AGE_MS,
    buster: AXON_QUERY_CACHE_BUSTER,
    dehydrateOptions: createAxonQueryDehydrateOptions(),
  };
}

export function shouldPersistAxonQuery(query: Query): boolean {
  return (
    query.state.status === 'success' &&
    isAllowedQueryKey(query.queryKey) &&
    !containsBlockedValue(query.queryKey) &&
    !containsBlockedValue(query.state.data)
  );
}

function createAxonQueryDehydrateOptions(): DehydrateOptions {
  return {
    shouldDehydrateMutation: () => false,
    shouldDehydrateQuery: shouldPersistAxonQuery,
  };
}

function isAllowedQueryKey(queryKey: QueryKey): boolean {
  if (isAllowedLocalQueryKey(queryKey)) {
    return true;
  }

  if (
    queryKey.length !== 4 ||
    queryKey[0] !== 'catalog' ||
    queryKey[1] !== 'source' ||
    !Array.isArray(queryKey[2]) ||
    !isAllowedCatalogLeaf(queryKey[3])
  ) {
    return false;
  }

  const sourceIdentity = queryKey[2];
  if (sourceIdentity[0] === 'manifest') {
    return sourceIdentity.length === 8;
  }

  if (sourceIdentity[0] === 'object_store_table_root') {
    return (
      sourceIdentity.length === 9 && (sourceIdentity[1] === 'gcs' || sourceIdentity[1] === 's3')
    );
  }

  return false;
}

function isAllowedLocalQueryKey(queryKey: QueryKey): boolean {
  return (
    queryKey.length === 2 &&
    queryKey[0] === 'local' &&
    (queryKey[1] === 'history' || queryKey[1] === 'saved')
  );
}

function isAllowedCatalogLeaf(value: unknown): boolean {
  return value === 'table-derived' || value === 'commits';
}

function containsBlockedValue(value: unknown, seen = new Set<object>()): boolean {
  if (typeof value === 'string') {
    return containsSignedUrlMarker(value);
  }

  if (Array.isArray(value)) {
    return value.some((item) => containsBlockedValue(item, seen));
  }

  if (typeof value !== 'object' || value === null) {
    return false;
  }

  if (seen.has(value)) {
    return false;
  }
  seen.add(value);

  for (const [key, child] of Object.entries(value)) {
    if (isBlockedFieldName(key) || containsBlockedValue(child, seen)) {
      return true;
    }
  }

  return false;
}

function containsSignedUrlMarker(value: string): boolean {
  const lower = value.toLowerCase();
  return (
    lower.includes('x-amz-signature') ||
    lower.includes('x-goog-signature') ||
    lower.includes('access_token=') ||
    lower.includes('refresh_token=') ||
    lower.includes('sig=') ||
    lower.includes('blob:')
  );
}

function isBlockedFieldName(fieldName: string): boolean {
  const tokens = fieldNameTokens(fieldName);
  const tokenSet = new Set(tokens);

  if (
    tokenSet.has('auth') ||
    tokenSet.has('token') ||
    tokenSet.has('secret') ||
    tokenSet.has('credential') ||
    tokenSet.has('credentials') ||
    tokenSet.has('grant') ||
    tokenSet.has('handle') ||
    tokenSet.has('handles') ||
    tokenSet.has('descriptor') ||
    tokenSet.has('descriptors') ||
    tokenSet.has('session') ||
    tokenSet.has('worker') ||
    tokenSet.has('events') ||
    tokenSet.has('metrics') ||
    tokenSet.has('plan') ||
    tokenSet.has('capabilities')
  ) {
    return true;
  }

  if (tokenSet.has('signed') && tokenSet.has('url')) {
    return true;
  }

  if (tokenSet.has('presigned') && tokenSet.has('url')) {
    return true;
  }

  if (tokenSet.has('active') && (tokenSet.has('file') || tokenSet.has('files'))) {
    return true;
  }

  if (tokenSet.has('object') && (tokenSet.has('list') || tokenSet.has('objects'))) {
    return true;
  }

  if (fieldName.toLowerCase() === 'objects') {
    return true;
  }

  if (tokenSet.has('run') && tokenSet.has('result')) {
    return true;
  }

  if (tokenSet.has('result') && tokenSet.has('page')) {
    return true;
  }

  return tokenSet.has('result');
}

function fieldNameTokens(fieldName: string): string[] {
  return fieldName
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}
