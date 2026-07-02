import { describe, expect, it } from 'vitest';
import {
  DEFAULT_APPEARANCE_SETTINGS,
  DEFAULT_EXECUTION_SETTINGS,
  type SettingsState,
} from '../state/slices/settings.ts';
import { parseSettingsPatchJson, settingsToJson } from './settings-model.ts';

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
});
