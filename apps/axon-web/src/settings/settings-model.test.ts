import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_APPEARANCE_SETTINGS,
  DEFAULT_EXECUTION_SETTINGS,
  type SettingsState,
} from '../state/slices/settings.ts';
import { applySettingsState, parseSettingsPatchJson, settingsToJson } from './settings-model.ts';

describe('settings model', () => {
  it('serializes only the honored settings shape', () => {
    const settings: SettingsState = {
      appearance: {
        theme: 'dark',
        density: 'compact',
        accent: '#0F9D74',
        uiFont: 'IBM Plex Sans',
        monoFont: 'JetBrains Mono',
      },
      execution: { defaultTarget: 'native' },
    };

    expect(JSON.parse(settingsToJson(settings))).toEqual(settings);
  });

  it('accepts valid partial setting patches with defaults for omitted sections', () => {
    const result = parseSettingsPatchJson(
      JSON.stringify({
        appearance: {
          theme: 'dark',
          accent: '#7A3CD7',
        },
      }),
      { serverFallbackEnabled: true },
    );

    expect(result).toEqual({
      ok: true,
      settings: {
        appearance: {
          ...DEFAULT_APPEARANCE_SETTINGS,
          theme: 'dark',
          accent: '#7A3CD7',
        },
        execution: DEFAULT_EXECUTION_SETTINGS,
      },
    });
  });

  it('rejects invalid raw JSON', () => {
    const result = parseSettingsPatchJson('{not valid json', {
      serverFallbackEnabled: true,
    });

    expect(result).toEqual({
      ok: false,
      error: 'Settings JSON is not valid JSON.',
    });
  });

  it('sanitizes invalid values and strips unsupported keys', () => {
    const result = parseSettingsPatchJson(
      JSON.stringify({
        appearance: {
          theme: 'sepia',
          density: 'comfy',
          accent: '#ff00ff',
          uiFont: 'Inter Tight',
          monoFont: 'Nope Mono',
          unsupported: true,
        },
        execution: {
          defaultTarget: 'native',
          unsupported: true,
        },
        unsupported: true,
      }),
      { serverFallbackEnabled: true },
    );

    expect(result).toEqual({
      ok: true,
      settings: {
        appearance: {
          ...DEFAULT_APPEARANCE_SETTINGS,
          density: 'comfy',
          uiFont: 'Inter Tight',
        },
        execution: { defaultTarget: 'native' },
      },
    });
    if (result.ok) {
      expect(JSON.parse(settingsToJson(result.settings))).toEqual(result.settings);
    }
  });

  it('coerces auto and native execution targets to browser wasm without server fallback', () => {
    expect(
      parseSettingsPatchJson(JSON.stringify({ execution: { defaultTarget: 'auto' } }), {
        serverFallbackEnabled: false,
      }),
    ).toEqual({
      ok: true,
      settings: {
        appearance: DEFAULT_APPEARANCE_SETTINGS,
        execution: { defaultTarget: 'browser_wasm' },
      },
    });

    expect(
      parseSettingsPatchJson(JSON.stringify({ execution: { defaultTarget: 'native' } }), {
        serverFallbackEnabled: false,
      }),
    ).toEqual({
      ok: true,
      settings: {
        appearance: DEFAULT_APPEARANCE_SETTINGS,
        execution: { defaultTarget: 'browser_wasm' },
      },
    });
  });

  it('applies a parsed settings state through the settings actions boundary', () => {
    const updateAppearance = vi.fn();
    const updateExecution = vi.fn();
    const settings: SettingsState = {
      appearance: {
        theme: 'dark',
        density: 'compact',
        accent: '#C2410C',
        uiFont: 'IBM Plex Sans',
        monoFont: 'Fira Code',
      },
      execution: { defaultTarget: 'auto' },
    };

    applySettingsState({ updateAppearance, updateExecution }, settings);

    expect(updateAppearance).toHaveBeenCalledWith(settings.appearance);
    expect(updateExecution).toHaveBeenCalledWith(settings.execution);
  });
});
