import { createStore, type StoreApi } from 'zustand/vanilla';
import {
  createJSONStorage,
  persist,
  type PersistStorage,
  type StateStorage,
} from 'zustand/middleware';
import {
  clampResultsH,
  clampSidebarW,
  createLayoutSlice,
  type LayoutSlice,
  type LayoutState,
} from './slices/layout.ts';
import { createConnectionsSlice, type ConnectionsSlice } from './slices/connections.ts';
import {
  createSettingsSlice,
  sanitizeAppearanceSettings,
  sanitizeExecutionSettings,
  type SettingsSlice,
  type SettingsState,
} from './slices/settings.ts';

export type AxonClientState = LayoutSlice & SettingsSlice & ConnectionsSlice;

export type PersistedAxonClientState = {
  layout: LayoutState;
  settings: SettingsState;
};

export type AxonClientStore = StoreApi<AxonClientState>;

export const CLIENT_STATE_STORAGE_KEY = 'axon.client-state.v1';

export type SyncClientStateStorage = {
  getItem(name: string): string | null;
  setItem(name: string, value: string): void;
  removeItem(name: string): void;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}

function noopStateStorage(): StateStorage<void> {
  return {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
  };
}

function safeStateStorage(storage: StateStorage<void>): StateStorage<void> {
  return {
    getItem(name) {
      try {
        return storage.getItem(name);
      } catch {
        return null;
      }
    },
    setItem(name, value) {
      try {
        storage.setItem(name, value);
      } catch {
        return undefined;
      }
    },
    removeItem(name) {
      try {
        storage.removeItem(name);
      } catch {
        return undefined;
      }
    },
  };
}

function browserLocalStorage(): StateStorage<void> {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return noopStateStorage();
    }

    const probeKey = `${CLIENT_STATE_STORAGE_KEY}.probe`;
    window.localStorage.setItem(probeKey, '1');
    window.localStorage.removeItem(probeKey);
    return window.localStorage;
  } catch {
    return noopStateStorage();
  }
}

function createClientPersistStorage(
  storage: StateStorage<void>,
): PersistStorage<PersistedAxonClientState> {
  return (
    createJSONStorage<PersistedAxonClientState>(() => safeStateStorage(storage)) ?? {
      getItem: () => null,
      setItem: () => undefined,
      removeItem: () => undefined,
    }
  );
}

function mergePersistedClientState(
  persistedState: unknown,
  currentState: AxonClientState,
): AxonClientState {
  if (!isRecord(persistedState)) {
    return currentState;
  }

  const persistedLayout = isRecord(persistedState.layout) ? persistedState.layout : {};
  const persistedSettings = isRecord(persistedState.settings) ? persistedState.settings : {};
  const persistedAppearance = isRecord(persistedSettings.appearance)
    ? persistedSettings.appearance
    : {};
  const persistedExecution = isRecord(persistedSettings.execution)
    ? persistedSettings.execution
    : {};

  return {
    ...currentState,
    layout: {
      sidebarW:
        typeof persistedLayout.sidebarW === 'number'
          ? clampSidebarW(persistedLayout.sidebarW)
          : currentState.layout.sidebarW,
      resultsH:
        typeof persistedLayout.resultsH === 'number'
          ? clampResultsH(persistedLayout.resultsH)
          : currentState.layout.resultsH,
    },
    settings: {
      appearance: sanitizeAppearanceSettings(persistedAppearance, currentState.settings.appearance),
      execution: sanitizeExecutionSettings(persistedExecution, currentState.settings.execution),
    },
  };
}

export function createMemoryClientStateStorage(
  initial?: Record<string, string>,
): SyncClientStateStorage {
  const items = new Map(Object.entries(initial ?? {}));

  return {
    getItem(name) {
      return items.get(name) ?? null;
    },
    setItem(name, value) {
      items.set(name, value);
    },
    removeItem(name) {
      items.delete(name);
    },
  };
}

export function createAxonClientStore(options?: { storage?: StateStorage<void> }): AxonClientStore {
  const storage = createClientPersistStorage(options?.storage ?? browserLocalStorage());

  return createStore<AxonClientState>()(
    persist(
      (set, get) => ({
        ...createLayoutSlice<AxonClientState>(set),
        ...createSettingsSlice<AxonClientState>(set),
        ...createConnectionsSlice<AxonClientState>(set, get),
      }),
      {
        name: CLIENT_STATE_STORAGE_KEY,
        version: 1,
        storage,
        partialize: (state): PersistedAxonClientState => ({
          layout: {
            sidebarW: state.layout.sidebarW,
            resultsH: state.layout.resultsH,
          },
          settings: {
            appearance: { ...state.settings.appearance },
            execution: { ...state.settings.execution },
          },
        }),
        merge: mergePersistedClientState,
      },
    ),
  );
}

export const axonClientStore = createAxonClientStore();
