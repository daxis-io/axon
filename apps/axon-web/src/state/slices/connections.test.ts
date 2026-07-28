import { clone, create, toJson } from '@bufbuild/protobuf';
import { beforeEach, describe, expect, it } from 'vitest';
import type { ActiveConnectedTableRef } from '../../services/query-source.ts';
import type { ConnectedCatalog } from '../../editor/connect/types.ts';
import {
  TableMetadataSchema,
  TableNodeSchema,
} from '../../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
  localDeltaConnectionId,
} from '../../services/canonical-table-identity.ts';
import { selectActiveConnectedTableRef, selectAvailableConnectedCatalogs } from '../hooks.ts';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';

const CONNECTED_CATALOGS_STORAGE_KEY = 'axon.connect.catalogs.v1';

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

function catalog({
  id,
  alias = id,
  table = id,
  schema = 'default',
  localRegistryId,
  localPersistence,
}: {
  id: string;
  alias?: string;
  table?: string;
  schema?: string;
  localRegistryId?: string;
  localPersistence?: 'persisted_directory_handle' | 'metadata_only_reselect' | 'session_handles';
}): ConnectedCatalog {
  const isLocal = localRegistryId !== undefined;
  const storage = isLocal
    ? `browser-local://delta-table/${encodeURIComponent(id)}`
    : `gs://${id}/${table}`;
  const logicalTable = isLocal
    ? createLocalDeltaCanonicalTable({ registryId: localRegistryId, tableName: table })
    : createPublicObjectStorageCanonicalTable({
        provider: 'gcs',
        connectionId: publicConnectionId(id),
        normalizedTableUri: storage,
        tableName: table,
      });
  const connectionId = logicalTable.resource!.connectionId;

  return {
    id: connectionId,
    catalogName: isLocal ? 'local-delta' : 'public-gcs',
    alias,
    kind: isLocal ? 'local' : 'object_store',
    provider: isLocal ? undefined : 'gcs',
    storage,
    region: isLocal ? 'browser-local' : 'us',
    status: 'connected',
    connectedAt: id,
    schemas: [
      {
        name: schema,
        tables: [
          {
            id: `${schema}.${table}`,
            name: table,
            snapshot: 1,
            rows: 1,
            files: 1,
            size: '1 byte',
            protocol: 'r1/w1',
            uri: isLocal ? undefined : storage,
            localRegistryId,
            localPersistence,
            logicalTable,
            catalogMetadataJson: toJson(
              TableMetadataSchema,
              create(TableMetadataSchema, {
                table: logicalTable,
                storageLocation: storage,
              }),
            ) as Readonly<Record<string, unknown>>,
            source: {
              id: `source-${id}-${schema}-${table}`,
              kind: isLocal ? 'local' : 'object_store',
              provider: isLocal ? undefined : 'gcs',
              storage,
              region: isLocal ? 'browser-local' : 'us',
              canonicalKey: `${isLocal ? 'local' : 'object_store'}|${isLocal ? '' : 'gcs'}|${storage}|||${schema}|${table}`,
              connectedAt: id,
            },
          },
        ],
      },
    ],
  };
}

function activeRef(catalogId: string, table = catalogId): ActiveConnectedTableRef {
  return createPublicObjectStorageCanonicalTable({
    provider: 'gcs',
    connectionId: publicConnectionId(catalogId),
    normalizedTableUri: `gs://${catalogId}/${table}`,
    tableName: table,
  });
}

function publicConnectionId(id: string): string {
  return `axon-connection://public-gcs/${encodeURIComponent(id)}`;
}

function withSecondQueryableTable(
  connectedCatalog: ConnectedCatalog,
  tableName = 'second',
): ConnectedCatalog {
  const schema = connectedCatalog.schemas[0]!;
  const first = schema.tables[0]!;
  const logicalTable = clone(TableNodeSchema, first.logicalTable!);
  if (logicalTable.resource?.identity.case !== 'canonicalLocator') {
    throw new Error('second-table fixture requires a locator-backed table');
  }
  logicalTable.name = tableName;
  logicalTable.resource.identity.value = `${logicalTable.resource.identity.value}-${tableName}`;
  return {
    ...connectedCatalog,
    schemas: [
      {
        ...schema,
        tables: [
          first,
          {
            ...first,
            id: `${schema.name}.${tableName}`,
            name: tableName,
            uri: logicalTable.resource.identity.value,
            logicalTable,
            catalogMetadataJson: toJson(
              TableMetadataSchema,
              create(TableMetadataSchema, {
                table: logicalTable,
                storageLocation: logicalTable.resource.identity.value,
              }),
            ) as Readonly<Record<string, unknown>>,
            source: first.source
              ? {
                  ...first.source,
                  id: `source-${tableName}`,
                  canonicalKey: `${first.source.canonicalKey}-${tableName}`,
                }
              : undefined,
          },
        ],
      },
    ],
  };
}

describe('connections slice', () => {
  let localStorage: MemoryStorage;

  beforeEach(() => {
    localStorage = new MemoryStorage();
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: localStorage,
    });
  });

  it('loads connected catalogs from the legacy connection key', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'saved' })]),
    );

    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(store.getState().connections.catalogs.map((item) => item.id)).toEqual([
      publicConnectionId('saved'),
    ]);
  });

  it('materializes an exact initial selection only for a sole queryable table', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'saved' })]),
    );

    const soleStore = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(soleStore.getState().connections.selectedTableRef).toEqual(activeRef('saved'));

    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([withSecondQueryableTable(catalog({ id: 'multiple' }))]),
    );
    const multipleStore = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(multipleStore.getState().connections.selectedTableRef).toBeUndefined();
  });

  it('persists catalog actions through the legacy key without adding connections to client state', () => {
    const clientStorage = createMemoryClientStateStorage();
    const store = createAxonClientStore({ storage: clientStorage });

    store.getState().connectionActions.upsertCatalog(catalog({ id: 'connected' }));

    const persistedCatalogs = JSON.parse(
      localStorage.getItem(CONNECTED_CATALOGS_STORAGE_KEY) ?? '[]',
    ) as ConnectedCatalog[];
    expect(persistedCatalogs.map((item) => item.id)).toContain(publicConnectionId('connected'));

    const persistedClientState = JSON.parse(
      clientStorage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}',
    );
    expect(persistedClientState.state).not.toHaveProperty('connections');
    expect(persistedClientState.state).not.toHaveProperty('connectionActions');
  });

  it('keeps same-alias connections distinct and selects the incoming stable connection', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'existing', alias: 'workspace', table: 'old' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    const result = store
      .getState()
      .connectionActions.upsertCatalog(
        catalog({ id: 'incoming', alias: 'Workspace', table: 'orders' }),
      );

    expect(result.mergedCatalogId).toBe(publicConnectionId('incoming'));
    expect(result.tableCount).toBe(1);
    expect(store.getState().connections.selectedTableRef).toEqual(activeRef('incoming', 'orders'));
    expect(store.getState().connections.catalogs).toHaveLength(2);
  });

  it('requires an explicit click when a Connect result contains multiple queryable tables', () => {
    localStorage.setItem(CONNECTED_CATALOGS_STORAGE_KEY, '[]');
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    store
      .getState()
      .connectionActions.upsertCatalog(withSecondQueryableTable(catalog({ id: 'incoming' })));

    expect(store.getState().connections.selectedTableRef).toBeUndefined();
  });

  it('does not discard the active query when a same-alias distinct connection is added', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'existing', alias: 'workspace', table: 'events' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    store.getState().connectionActions.selectTable(activeRef('existing', 'events'));

    const result = store
      .getState()
      .connectionActions.upsertCatalog(
        catalog({ id: 'incoming', alias: 'workspace', table: 'events' }),
      );

    expect(result.replaced).toEqual([]);
    expect(result.shouldDiscardActiveQuerySession).toBe(false);
    expect(result.discardedSources).toEqual([]);
  });

  it('keeps a reconnected local runtime and unregisters it only on removal', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([
        catalog({
          id: 'local-existing',
          alias: 'workspace',
          table: 'events',
          localRegistryId: 'local-reg-1',
          localPersistence: 'persisted_directory_handle',
        }),
      ]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    const replacement = store.getState().connectionActions.upsertCatalog(
      catalog({
        id: 'local-incoming',
        alias: 'renamed workspace',
        table: 'events',
        localRegistryId: 'local-reg-1',
        localPersistence: 'metadata_only_reselect',
      }),
    );

    expect(replacement.localRegistryIdsToUnregister).toEqual([]);

    store.getState().connectionActions.upsertCatalog(
      catalog({
        id: 'local-remove',
        table: 'local-remove',
        localRegistryId: 'local-reg-2',
        localPersistence: 'metadata_only_reselect',
      }),
    );

    const removal = store
      .getState()
      .connectionActions.removeCatalog(localDeltaConnectionId('local-reg-2'));

    expect(removal.localRegistryIdsToUnregister).toEqual(['local-reg-2']);
  });

  it('clears selection instead of choosing another table when removing the active catalog', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'first' }), catalog({ id: 'second' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    store.getState().connectionActions.selectTable(activeRef('first'));

    const removal = store.getState().connectionActions.removeCatalog(publicConnectionId('first'));

    expect(removal.shouldDiscardActiveQuerySession).toBe(true);
    expect(removal.discardedSources).toEqual([
      expect.objectContaining({
        kind: 'object_store_table_root',
        catalogName: 'first',
        schemaName: 'default',
        tableName: 'first',
        tableUri: 'gs://first/first',
      }),
    ]);
    expect(store.getState().connections.selectedTableRef).toBeUndefined();

    store.getState().connectionActions.removeCatalog(publicConnectionId('second'));

    expect(store.getState().connections.selectedTableRef).toBeUndefined();
  });

  it('retains an exact active resource when another canonical table joins its connection', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'existing', alias: 'workspace', table: 'events' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    store.getState().connectionActions.selectTable(activeRef('existing', 'events'));

    store
      .getState()
      .connectionActions.upsertCatalog(
        withSecondQueryableTable(
          catalog({ id: 'existing', alias: 'renamed workspace', table: 'events' }),
        ),
      );

    expect(store.getState().connections.selectedTableRef).toEqual(activeRef('existing', 'events'));
  });

  it('retains the selected canonical resource when reconnecting it with new presentation', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'existing', alias: 'workspace', table: 'events' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    store.getState().connectionActions.selectTable(activeRef('existing', 'events'));

    store
      .getState()
      .connectionActions.upsertCatalog(
        catalog({ id: 'existing', alias: 'renamed workspace', table: 'events' }),
      );

    expect(store.getState().connections.selectedTableRef).toEqual(activeRef('existing', 'events'));
    expect(store.getState().connections.catalogs[0]?.alias).toBe('renamed workspace');
  });

  it('does not persist session-handle local Delta tables from store actions', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    store.getState().connectionActions.upsertCatalog(
      catalog({
        id: 'session-only',
        localRegistryId: 'local-reg-session',
        localPersistence: 'session_handles',
      }),
    );

    const persistedCatalogs = JSON.parse(
      localStorage.getItem(CONNECTED_CATALOGS_STORAGE_KEY) ?? '[]',
    ) as ConnectedCatalog[];
    expect(persistedCatalogs.map((item) => item.id)).not.toContain(
      localDeltaConnectionId('local-reg-session'),
    );
    expect(store.getState().connections.catalogs.map((item) => item.id)).toContain(
      localDeltaConnectionId('local-reg-session'),
    );
  });

  it('keeps derived connection selector references stable while inputs are unchanged', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'saved' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    const firstAvailable = selectAvailableConnectedCatalogs(store.getState());
    const firstActive = selectActiveConnectedTableRef(store.getState());

    expect(selectAvailableConnectedCatalogs(store.getState())).toBe(firstAvailable);
    expect(selectActiveConnectedTableRef(store.getState())).toBe(firstActive);

    store.getState().layoutActions.setSidebarW(320);

    expect(selectAvailableConnectedCatalogs(store.getState())).toBe(firstAvailable);
    expect(selectActiveConnectedTableRef(store.getState())).toBe(firstActive);
  });
});
