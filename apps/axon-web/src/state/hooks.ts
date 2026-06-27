import { useStore } from 'zustand/react';
import { CONNECTOR_FEATURES } from '../services/connector-features.ts';
import { resolveActiveTableRef, type ActiveConnectedTableRef } from '../services/query-source.ts';
import { catalogsAvailableForFeatures } from '../editor/connect/store.ts';
import type { ConnectedCatalog } from '../editor/connect/types.ts';
import { axonClientStore, type AxonClientState } from './store.ts';

export const selectLayout = (state: AxonClientState) => state.layout;
export const selectLayoutActions = (state: AxonClientState) => state.layoutActions;
export const selectAppearanceSettings = (state: AxonClientState) => state.settings.appearance;
export const selectSettingsActions = (state: AxonClientState) => state.settingsActions;
export const selectDefaultTarget = (state: AxonClientState) =>
  state.settings.execution.defaultTarget;
export const selectConnectedCatalogs = (state: AxonClientState) => state.connections.catalogs;
export const selectSelectedTableRef = (state: AxonClientState) =>
  state.connections.selectedTableRef;
export const selectFreshCatalogId = (state: AxonClientState) => state.connections.freshCatalogId;
export const selectConnectionActions = (state: AxonClientState) => state.connectionActions;
export const selectTabs = (state: AxonClientState) => state.tabs;
export const selectActiveTab = (state: AxonClientState) =>
  state.tabs.items.find((tab) => tab.id === state.tabs.activeTabId) ?? state.tabs.items[0];
export const selectActiveSqlTab = (state: AxonClientState) => {
  const active = selectActiveTab(state);
  return active?.kind === 'sql' ? active : undefined;
};
export const selectTabActions = (state: AxonClientState) => state.tabsActions;

let availableCatalogInput: ConnectedCatalog[] | undefined;
let availableCatalogOutput: ConnectedCatalog[] | undefined;

export const selectAvailableConnectedCatalogs = (state: AxonClientState) => {
  if (
    availableCatalogInput === state.connections.catalogs &&
    availableCatalogOutput !== undefined
  ) {
    return availableCatalogOutput;
  }

  availableCatalogInput = state.connections.catalogs;
  availableCatalogOutput = catalogsAvailableForFeatures(
    state.connections.catalogs,
    CONNECTOR_FEATURES,
  );
  return availableCatalogOutput;
};

let activeCatalogInput: ConnectedCatalog[] | undefined;
let activeSelectedInput: ActiveConnectedTableRef | undefined;
let activeTableOutput: ActiveConnectedTableRef | undefined;
let activeTableOutputCached = false;

export const selectActiveConnectedTableRef = (state: AxonClientState) => {
  const availableCatalogs = selectAvailableConnectedCatalogs(state);
  if (
    activeTableOutputCached &&
    activeCatalogInput === availableCatalogs &&
    activeSelectedInput === state.connections.selectedTableRef
  ) {
    return activeTableOutput;
  }

  activeCatalogInput = availableCatalogs;
  activeSelectedInput = state.connections.selectedTableRef;
  activeTableOutput = resolveActiveTableRef(availableCatalogs, state.connections.selectedTableRef);
  activeTableOutputCached = true;
  return activeTableOutput;
};

export function useAxonClientStore<T>(selector: (state: AxonClientState) => T): T {
  return useStore(axonClientStore, selector);
}
