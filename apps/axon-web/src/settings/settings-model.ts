import {
  DEFAULT_APPEARANCE_SETTINGS,
  DEFAULT_EXECUTION_SETTINGS,
  coerceDefaultTargetForAvailability,
  sanitizeAppearanceSettings,
  sanitizeExecutionSettings,
  type SettingsActions,
  type SettingsState,
} from '../state/slices/settings.ts';

export type ParseSettingsPatchJsonResult =
  | { ok: true; settings: SettingsState }
  | { ok: false; error: string };

type ParseSettingsPatchJsonOptions = {
  serverFallbackEnabled: boolean;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function honoredSettingsShape(settings: SettingsState): SettingsState {
  return {
    appearance: sanitizeAppearanceSettings(settings.appearance, DEFAULT_APPEARANCE_SETTINGS),
    execution: sanitizeExecutionSettings(settings.execution, DEFAULT_EXECUTION_SETTINGS),
  };
}

export function settingsToJson(settings: SettingsState): string {
  return JSON.stringify(honoredSettingsShape(settings), null, 2);
}

export function applySettingsState(
  settingsActions: Pick<SettingsActions, 'updateAppearance' | 'updateExecution'>,
  settings: SettingsState,
): void {
  settingsActions.updateAppearance(settings.appearance);
  settingsActions.updateExecution(settings.execution);
}

export function parseSettingsPatchJson(
  raw: string,
  options: ParseSettingsPatchJsonOptions,
): ParseSettingsPatchJsonResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false, error: 'Settings JSON is not valid JSON.' };
  }

  if (!isRecord(parsed)) {
    return { ok: false, error: 'Settings JSON must be an object.' };
  }

  const execution = sanitizeExecutionSettings(parsed.execution, DEFAULT_EXECUTION_SETTINGS);

  return {
    ok: true,
    settings: {
      appearance: sanitizeAppearanceSettings(parsed.appearance, DEFAULT_APPEARANCE_SETTINGS),
      execution: {
        defaultTarget: coerceDefaultTargetForAvailability(
          execution.defaultTarget,
          options.serverFallbackEnabled,
        ),
      },
    },
  };
}
