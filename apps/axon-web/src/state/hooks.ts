import { useStore } from 'zustand/react';
import { axonClientStore, type AxonClientState } from './store.ts';

export const selectLayout = (state: AxonClientState) => state.layout;
export const selectLayoutActions = (state: AxonClientState) => state.layoutActions;
export const selectAppearanceSettings = (state: AxonClientState) => state.settings.appearance;
export const selectSettingsActions = (state: AxonClientState) => state.settingsActions;
export const selectDefaultTarget = (state: AxonClientState) =>
  state.settings.execution.defaultTarget;

export function useAxonClientStore<T>(selector: (state: AxonClientState) => T): T {
  return useStore(axonClientStore, selector);
}
