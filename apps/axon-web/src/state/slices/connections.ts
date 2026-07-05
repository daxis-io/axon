import { CONNECTOR_FEATURES } from '../../services/connector-features.ts';
import {
  firstQueryableTableRef,
  querySourcesForCatalog,
  resolveActiveTableRef,
  type ActiveConnectedTableRef,
  type QueryTableSource,
} from '../../services/query-source.ts';
import {
  buildCatalogFromResult,
  catalogsAvailableForFeatures,
  loadConnectedCatalogs,
  localRegistryIdsForCatalogs,
  saveConnectedCatalogs,
  upsertConnectedCatalog,
} from '../../editor/connect/store.ts';
import type { ConnectedCatalog, ConnectResult } from '../../editor/connect/types.ts';

export type ConnectionsState = {
  catalogs: ConnectedCatalog[];
  selectedTableRef?: ActiveConnectedTableRef;
  freshCatalogId: string | null;
};

export type ConnectionMutationResult = {
  catalogs: ConnectedCatalog[];
  replaced: ConnectedCatalog[];
  discardedSources: QueryTableSource[];
  localRegistryIdsToUnregister: string[];
  shouldDiscardActiveQuerySession: boolean;
  mergedCatalogId: string | null;
  catalogAlias: string | null;
  tableCount: number;
};

export type ConnectionActions = {
  connect(result: ConnectResult): ConnectionMutationResult;
  upsertCatalog(catalog: ConnectedCatalog): ConnectionMutationResult;
  removeCatalog(id: string): ConnectionMutationResult;
  selectTable(ref: ActiveConnectedTableRef): void;
  clearFreshCatalogId(): void;
};

export type ConnectionsSlice = {
  connections: ConnectionsState;
  connectionActions: ConnectionActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

type StoreGet<TState> = () => TState;

export function createConnectionsSlice<TState extends ConnectionsSlice>(
  set: StoreSet<TState>,
  get: StoreGet<TState>,
): ConnectionsSlice {
  const upsertCatalog = (catalog: ConnectedCatalog): ConnectionMutationResult => {
    const current = get().connections;
    const upsert = upsertConnectedCatalog(current.catalogs, catalog);
    const mergedCatalogId = mergedCatalogIdFor(upsert.catalogs, catalog);
    const firstIncomingTable = firstQueryableTableRef([catalog]);
    const selectedTableRef = firstIncomingTable
      ? { ...firstIncomingTable, catalogId: mergedCatalogId }
      : current.selectedTableRef;
    const localRegistryIdsToUnregister = localRegistryIdsForCatalogs(upsert.replaced);
    const shouldDiscardActiveQuerySession = upsert.replaced.some(
      (replaced) => replaced.id === current.selectedTableRef?.catalogId,
    );

    saveConnectedCatalogs(upsert.catalogs);
    set((state) => ({
      ...state,
      connections: {
        ...state.connections,
        catalogs: upsert.catalogs,
        selectedTableRef,
        freshCatalogId: mergedCatalogId,
      },
    }));

    return {
      catalogs: upsert.catalogs,
      replaced: upsert.replaced,
      discardedSources: querySourcesForCatalogs(upsert.replaced),
      localRegistryIdsToUnregister,
      shouldDiscardActiveQuerySession,
      mergedCatalogId,
      catalogAlias: catalog.alias,
      tableCount: tableCount(catalog),
    };
  };

  return {
    connections: {
      catalogs: loadConnectedCatalogs(),
      selectedTableRef: undefined,
      freshCatalogId: null,
    },
    connectionActions: {
      connect(result) {
        return upsertCatalog(buildCatalogFromResult(result));
      },
      upsertCatalog,
      removeCatalog(id) {
        const current = get().connections;
        const removed = current.catalogs.find((catalog) => catalog.id === id);
        if (!removed) {
          return emptyMutationResult(current.catalogs);
        }

        const catalogs = current.catalogs.filter((catalog) => catalog.id !== id);
        const availableCatalogs = catalogsAvailableForFeatures(catalogs, CONNECTOR_FEATURES);
        const selectedTableRef =
          current.selectedTableRef?.catalogId === id
            ? firstQueryableTableRef(availableCatalogs)
            : resolveActiveTableRef(availableCatalogs, current.selectedTableRef);
        const localRegistryIdsToUnregister = localRegistryIdsForCatalogs([removed]);
        const shouldDiscardActiveQuerySession = current.selectedTableRef?.catalogId === id;

        saveConnectedCatalogs(catalogs);
        set((state) => ({
          ...state,
          connections: {
            ...state.connections,
            catalogs,
            selectedTableRef,
            freshCatalogId:
              state.connections.freshCatalogId === id ? null : state.connections.freshCatalogId,
          },
        }));

        return {
          catalogs,
          replaced: [removed],
          discardedSources: querySourcesForCatalogs([removed]),
          localRegistryIdsToUnregister,
          shouldDiscardActiveQuerySession,
          mergedCatalogId: null,
          catalogAlias: removed.alias,
          tableCount: tableCount(removed),
        };
      },
      selectTable(ref) {
        set((state) => ({
          ...state,
          connections: {
            ...state.connections,
            selectedTableRef: ref,
          },
        }));
      },
      clearFreshCatalogId() {
        set((state) => ({
          ...state,
          connections: {
            ...state.connections,
            freshCatalogId: null,
          },
        }));
      },
    },
  };
}

function emptyMutationResult(catalogs: ConnectedCatalog[]): ConnectionMutationResult {
  return {
    catalogs,
    replaced: [],
    discardedSources: [],
    localRegistryIdsToUnregister: [],
    shouldDiscardActiveQuerySession: false,
    mergedCatalogId: null,
    catalogAlias: null,
    tableCount: 0,
  };
}

function querySourcesForCatalogs(catalogs: ConnectedCatalog[]): QueryTableSource[] {
  return catalogs.flatMap((catalog) => querySourcesForCatalog(catalog));
}

function mergedCatalogIdFor(catalogs: ConnectedCatalog[], incoming: ConnectedCatalog): string {
  return (
    catalogs.find((catalog) => catalogAliasKey(catalog) === catalogAliasKey(incoming))?.id ??
    incoming.id
  );
}

function catalogAliasKey(catalog: ConnectedCatalog): string {
  return catalog.alias.trim().toLowerCase();
}

function tableCount(catalog: ConnectedCatalog): number {
  return catalog.schemas.reduce((total, schema) => total + schema.tables.length, 0);
}
