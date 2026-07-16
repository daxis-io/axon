import { useStore } from 'zustand/react';
import { CONNECTOR_FEATURES } from '../services/connector-features.ts';
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
export const selectUi = (state: AxonClientState) => state.ui;
export const selectUiActions = (state: AxonClientState) => state.uiActions;
export const selectEngineStatus = (state: AxonClientState) => state.engine.status;
export const selectEngineActions = (state: AxonClientState) => state.engineActions;
export const selectRun = (state: AxonClientState) => state.run;
export const selectRunState = (state: AxonClientState) => state.run.runState;
export const selectRunIsRunning = (state: AxonClientState) =>
  state.run.runState.status === 'running';
export const selectRunResultData = (state: AxonClientState) => state.run.resultData;
export const selectRunResultPageRun = (state: AxonClientState) => state.run.resultPageRun;
export const selectRunLoadingMoreRows = (state: AxonClientState) => state.run.loadingMoreRows;
export const selectRunMetrics = (state: AxonClientState) => state.run.metrics;
export const selectRunEvents = (state: AxonClientState) => state.run.events;
export const selectRunPlan = (state: AxonClientState) => state.run.plan;
export const selectRunCapabilities = (state: AxonClientState) => state.run.capabilities;
export const selectRunActions = (state: AxonClientState) => state.runActions;

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

export const selectActiveConnectedTableRef = (state: AxonClientState) =>
  state.connections.selectedTableRef;

export function useAxonClientStore<T>(selector: (state: AxonClientState) => T): T {
  return useStore(axonClientStore, selector);
}
