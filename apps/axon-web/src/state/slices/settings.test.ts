import {
  AccentColor,
  AppearanceDensity,
  AppearanceTheme,
  ExecutionTarget,
} from '../../generated/config/protobuf/axon/config/v1/settings_pb.ts';
import { describe, expect, it } from 'vitest';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';
import { selectDefaultTarget } from '../hooks.ts';
import {
  DEFAULT_APPEARANCE_SETTINGS,
  DEFAULT_EXECUTION_SETTINGS,
  accentColorToSetting,
  appearanceDensityToSetting,
  appearanceThemeToSetting,
  availableExecutionTargetValues,
  coerceDefaultTargetForAvailability,
  executionTargetToSetting,
} from './settings.ts';

describe('settings slice', () => {
  it('maps generated config defaults to app-local setting values', () => {
    expect(appearanceThemeToSetting(AppearanceTheme.LIGHT)).toBe('light');
    expect(appearanceDensityToSetting(AppearanceDensity.COMFORTABLE)).toBe('regular');
    expect(accentColorToSetting(AccentColor.BLUE)).toBe('#1F4FE0');
    expect(executionTargetToSetting(ExecutionTarget.BROWSER)).toBe('browser_wasm');

    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    expect(store.getState().settings.appearance).toEqual(DEFAULT_APPEARANCE_SETTINGS);
    expect(store.getState().settings.execution).toEqual(DEFAULT_EXECUTION_SETTINGS);
    expect(store.getState().settings.appearance).toEqual({
      theme: 'light',
      density: 'regular',
      accent: '#1F4FE0',
      uiFont: 'Geist',
      monoFont: 'Geist Mono',
    });
    expect(selectDefaultTarget(store.getState())).toBe('browser_wasm');
  });

  it('applies sparse appearance patches without replacing other appearance settings', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    store.getState().settingsActions.updateAppearance({
      theme: 'dark',
      accent: '#0F9D74',
    });

    expect(store.getState().settings.appearance).toEqual({
      ...DEFAULT_APPEARANCE_SETTINGS,
      theme: 'dark',
      accent: '#0F9D74',
    });

    store.getState().settingsActions.setAppearanceValue('monoFont', 'JetBrains Mono');
    expect(store.getState().settings.appearance.monoFont).toBe('JetBrains Mono');
    expect(store.getState().settings.appearance.uiFont).toBe('Geist');
  });

  it('applies sparse execution patches and exposes the default target selector', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(selectDefaultTarget(store.getState())).toBe('browser_wasm');

    store.getState().settingsActions.updateExecution({ defaultTarget: 'auto' });
    expect(store.getState().settings.execution).toEqual({ defaultTarget: 'auto' });
    expect(selectDefaultTarget(store.getState())).toBe('auto');

    store.getState().settingsActions.setDefaultTarget('native');
    expect(selectDefaultTarget(store.getState())).toBe('native');
  });

  it('limits default target choices to browser wasm when server fallback is unavailable', () => {
    expect(availableExecutionTargetValues(false)).toEqual(['browser_wasm']);
    expect(coerceDefaultTargetForAvailability('auto', false)).toBe('browser_wasm');
    expect(coerceDefaultTargetForAvailability('native', false)).toBe('browser_wasm');
    expect(coerceDefaultTargetForAvailability('browser_wasm', false)).toBe('browser_wasm');
  });

  it('keeps all default target choices available when server fallback is available', () => {
    expect(availableExecutionTargetValues(true)).toEqual(['auto', 'browser_wasm', 'native']);
    expect(coerceDefaultTargetForAvailability('auto', true)).toBe('auto');
    expect(coerceDefaultTargetForAvailability('native', true)).toBe('native');
    expect(coerceDefaultTargetForAvailability('browser_wasm', true)).toBe('browser_wasm');
  });

  it('persists settings and falls back to defaults when stored JSON is invalid', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });

    first.getState().settingsActions.updateAppearance({
      density: 'comfy',
      uiFont: 'IBM Plex Sans',
      monoFont: 'Fira Code',
    });
    first.getState().settingsActions.setDefaultTarget('native');

    const second = createAxonClientStore({ storage });
    expect(second.getState().settings).toEqual({
      appearance: {
        ...DEFAULT_APPEARANCE_SETTINGS,
        density: 'comfy',
        uiFont: 'IBM Plex Sans',
        monoFont: 'Fira Code',
      },
      execution: { defaultTarget: 'native' },
    });

    storage.setItem(CLIENT_STATE_STORAGE_KEY, '{not valid json');
    const fallback = createAxonClientStore({ storage });
    expect(fallback.getState().settings.appearance).toEqual(DEFAULT_APPEARANCE_SETTINGS);
    expect(fallback.getState().settings.execution).toEqual(DEFAULT_EXECUTION_SETTINGS);
  });
});
