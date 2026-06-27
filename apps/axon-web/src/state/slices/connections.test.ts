import { beforeEach, describe, expect, it } from 'vitest';
import type { ActiveConnectedTableRef } from '../../services/query-source.ts';
import type { ConnectedCatalog } from '../../editor/connect/types.ts';
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
  const storage = isLocal ? `local://${id}` : `gs://${id}`;

  return {
    id,
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
            manifestUrl: isLocal ? undefined : `/manifests/${id}.json`,
            localRegistryId,
            localPersistence,
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
  return { catalogId, schemaName: 'default', tableName: table };
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

    expect(store.getState().connections.catalogs.map((item) => item.id)).toEqual(['saved']);
  });

  it('persists catalog actions through the legacy key without adding connections to client state', () => {
    const clientStorage = createMemoryClientStateStorage();
    const store = createAxonClientStore({ storage: clientStorage });

    store.getState().connectionActions.upsertCatalog(catalog({ id: 'connected' }));

    const persistedCatalogs = JSON.parse(
      localStorage.getItem(CONNECTED_CATALOGS_STORAGE_KEY) ?? '[]',
    ) as ConnectedCatalog[];
    expect(persistedCatalogs.map((item) => item.id)).toContain('connected');

    const persistedClientState = JSON.parse(
      clientStorage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}',
    );
    expect(persistedClientState.state).not.toHaveProperty('connections');
    expect(persistedClientState.state).not.toHaveProperty('connectionActions');
  });

  it('upserts catalogs and selects the first queryable incoming table with the merged catalog id', () => {
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

    expect(result.mergedCatalogId).toBe('existing');
    expect(result.tableCount).toBe(1);
    expect(store.getState().connections.selectedTableRef).toEqual({
      catalogId: 'existing',
      schemaName: 'default',
      tableName: 'orders',
    });
  });

  it('reports active-query discard when replacing the active catalog', () => {
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

    expect(result.replaced.map((item) => item.id)).toEqual(['existing']);
    expect(result.shouldDiscardActiveQuerySession).toBe(true);
  });

  it('returns local Delta registry ids when replacement or removal unregisters catalog data', () => {
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

    const replacement = store
      .getState()
      .connectionActions.upsertCatalog(
        catalog({ id: 'local-incoming', alias: 'workspace', table: 'events' }),
      );

    expect(replacement.localRegistryIdsToUnregister).toEqual(['local-reg-1']);

    store.getState().connectionActions.upsertCatalog(
      catalog({
        id: 'local-remove',
        table: 'local-remove',
        localRegistryId: 'local-reg-2',
        localPersistence: 'metadata_only_reselect',
      }),
    );

    const removal = store.getState().connectionActions.removeCatalog('local-remove');

    expect(removal.localRegistryIdsToUnregister).toEqual(['local-reg-2']);
  });

  it('selects the next queryable table when removing the active catalog, then clears selection', () => {
    localStorage.setItem(
      CONNECTED_CATALOGS_STORAGE_KEY,
      JSON.stringify([catalog({ id: 'first' }), catalog({ id: 'second' })]),
    );
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    store.getState().connectionActions.selectTable(activeRef('first'));

    const removal = store.getState().connectionActions.removeCatalog('first');

    expect(removal.shouldDiscardActiveQuerySession).toBe(true);
    expect(store.getState().connections.selectedTableRef).toEqual(activeRef('second'));

    store.getState().connectionActions.removeCatalog('second');

    expect(store.getState().connections.selectedTableRef).toBeUndefined();
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
    expect(persistedCatalogs.map((item) => item.id)).not.toContain('session-only');
    expect(store.getState().connections.catalogs.map((item) => item.id)).toContain('session-only');
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
