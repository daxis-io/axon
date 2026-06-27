import {
  AccentColor,
  AppearanceDensity,
  AppearanceTheme,
  ExecutionTarget,
} from '../../generated/config/protobuf/axon/config/v1/settings_pb.ts';

export const APPEARANCE_THEME_VALUES = ['light', 'dark'] as const;
export const APPEARANCE_DENSITY_VALUES = ['compact', 'regular', 'comfy'] as const;
export const ACCENT_VALUES = ['#1F4FE0', '#7A3CD7', '#0F9D74', '#C2410C'] as const;
export const UI_FONT_VALUES = ['Geist', 'Inter Tight', 'IBM Plex Sans'] as const;
export const MONO_FONT_VALUES = [
  'Geist Mono',
  'JetBrains Mono',
  'IBM Plex Mono',
  'Fira Code',
] as const;
const EXECUTION_TARGET_VALUES = ['auto', 'browser_wasm', 'native'] as const;
const BROWSER_ONLY_EXECUTION_TARGET_VALUES = ['browser_wasm'] as const;

export type AppearanceThemeSetting = (typeof APPEARANCE_THEME_VALUES)[number];
export type AppearanceDensitySetting = (typeof APPEARANCE_DENSITY_VALUES)[number];
export type AccentSetting = (typeof ACCENT_VALUES)[number];
export type UiFontSetting = (typeof UI_FONT_VALUES)[number];
export type MonoFontSetting = (typeof MONO_FONT_VALUES)[number];
export type DefaultTargetSetting = (typeof EXECUTION_TARGET_VALUES)[number];

export type AppearanceSettings = {
  theme: AppearanceThemeSetting;
  density: AppearanceDensitySetting;
  accent: AccentSetting;
  uiFont: UiFontSetting;
  monoFont: MonoFontSetting;
};

export type ExecutionSettings = {
  defaultTarget: DefaultTargetSetting;
};

export type SettingsState = {
  appearance: AppearanceSettings;
  execution: ExecutionSettings;
};

export type SettingsActions = {
  updateAppearance(patch: Partial<AppearanceSettings>): void;
  setAppearanceValue<K extends keyof AppearanceSettings>(
    key: K,
    value: AppearanceSettings[K],
  ): void;
  updateExecution(patch: Partial<ExecutionSettings>): void;
  setDefaultTarget(target: ExecutionSettings['defaultTarget']): void;
};

export type SettingsSlice = {
  settings: SettingsState;
  settingsActions: SettingsActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

const GENERATED_APPEARANCE_DEFAULTS = {
  theme: AppearanceTheme.LIGHT,
  density: AppearanceDensity.COMFORTABLE,
  accent: AccentColor.BLUE,
  monospaceFont: 'Geist Mono',
} as const;

const GENERATED_EXECUTION_DEFAULTS = {
  target: ExecutionTarget.BROWSER,
} as const;

function includes<const T extends readonly string[]>(
  values: T,
  value: unknown,
): value is T[number] {
  return typeof value === 'string' && values.includes(value);
}

export function isAppearanceThemeSetting(value: unknown): value is AppearanceThemeSetting {
  return includes(APPEARANCE_THEME_VALUES, value);
}

export function isAppearanceDensitySetting(value: unknown): value is AppearanceDensitySetting {
  return includes(APPEARANCE_DENSITY_VALUES, value);
}

export function isAccentSetting(value: unknown): value is AccentSetting {
  return includes(ACCENT_VALUES, value);
}

export function isUiFontSetting(value: unknown): value is UiFontSetting {
  return includes(UI_FONT_VALUES, value);
}

export function isMonoFontSetting(value: unknown): value is MonoFontSetting {
  return includes(MONO_FONT_VALUES, value);
}

export function isDefaultTargetSetting(value: unknown): value is DefaultTargetSetting {
  return includes(EXECUTION_TARGET_VALUES, value);
}

export function availableExecutionTargetValues(
  serverFallbackEnabled: boolean,
): readonly DefaultTargetSetting[] {
  return serverFallbackEnabled ? EXECUTION_TARGET_VALUES : BROWSER_ONLY_EXECUTION_TARGET_VALUES;
}

export function coerceDefaultTargetForAvailability(
  target: ExecutionSettings['defaultTarget'],
  serverFallbackEnabled: boolean,
): ExecutionSettings['defaultTarget'] {
  return serverFallbackEnabled || target === 'browser_wasm' ? target : 'browser_wasm';
}

export function appearanceThemeToSetting(theme: AppearanceTheme): AppearanceSettings['theme'] {
  return theme === AppearanceTheme.DARK ? 'dark' : 'light';
}

export function appearanceDensityToSetting(
  density: AppearanceDensity,
): AppearanceSettings['density'] {
  return density === AppearanceDensity.COMPACT ? 'compact' : 'regular';
}

export function accentColorToSetting(accent: AccentColor): AppearanceSettings['accent'] {
  switch (accent) {
    case AccentColor.GREEN:
      return '#0F9D74';
    case AccentColor.AMBER:
      return '#C2410C';
    case AccentColor.BLUE:
    case AccentColor.UNSPECIFIED:
    default:
      return '#1F4FE0';
  }
}

export function monospaceFontToSetting(font: string): AppearanceSettings['monoFont'] {
  return isMonoFontSetting(font) ? font : 'Geist Mono';
}

export function executionTargetToSetting(
  target: ExecutionTarget,
): ExecutionSettings['defaultTarget'] {
  switch (target) {
    case ExecutionTarget.AUTO:
      return 'auto';
    case ExecutionTarget.NATIVE:
      return 'native';
    case ExecutionTarget.BROWSER:
    case ExecutionTarget.UNSPECIFIED:
    default:
      return 'browser_wasm';
  }
}

export const DEFAULT_APPEARANCE_SETTINGS: AppearanceSettings = {
  theme: appearanceThemeToSetting(GENERATED_APPEARANCE_DEFAULTS.theme),
  density: appearanceDensityToSetting(GENERATED_APPEARANCE_DEFAULTS.density),
  accent: accentColorToSetting(GENERATED_APPEARANCE_DEFAULTS.accent),
  uiFont: 'Geist',
  monoFont: monospaceFontToSetting(GENERATED_APPEARANCE_DEFAULTS.monospaceFont),
};

export const DEFAULT_EXECUTION_SETTINGS: ExecutionSettings = {
  defaultTarget: executionTargetToSetting(GENERATED_EXECUTION_DEFAULTS.target),
};

export const DEFAULT_SETTINGS_STATE: SettingsState = {
  appearance: { ...DEFAULT_APPEARANCE_SETTINGS },
  execution: { ...DEFAULT_EXECUTION_SETTINGS },
};

export function sanitizeAppearanceSettings(
  input: unknown,
  fallback: AppearanceSettings = DEFAULT_APPEARANCE_SETTINGS,
): AppearanceSettings {
  const value = input && typeof input === 'object' ? (input as Record<string, unknown>) : {};

  return {
    theme: isAppearanceThemeSetting(value.theme) ? value.theme : fallback.theme,
    density: isAppearanceDensitySetting(value.density) ? value.density : fallback.density,
    accent: isAccentSetting(value.accent) ? value.accent : fallback.accent,
    uiFont: isUiFontSetting(value.uiFont) ? value.uiFont : fallback.uiFont,
    monoFont: isMonoFontSetting(value.monoFont) ? value.monoFont : fallback.monoFont,
  };
}

export function sanitizeExecutionSettings(
  input: unknown,
  fallback: ExecutionSettings = DEFAULT_EXECUTION_SETTINGS,
): ExecutionSettings {
  const value = input && typeof input === 'object' ? (input as Record<string, unknown>) : {};

  return {
    defaultTarget: isDefaultTargetSetting(value.defaultTarget)
      ? value.defaultTarget
      : fallback.defaultTarget,
  };
}

export function createSettingsSlice<TState extends SettingsSlice>(
  set: StoreSet<TState>,
): SettingsSlice {
  return {
    settings: {
      appearance: { ...DEFAULT_APPEARANCE_SETTINGS },
      execution: { ...DEFAULT_EXECUTION_SETTINGS },
    },
    settingsActions: {
      updateAppearance(patch) {
        set((state) => ({
          ...state,
          settings: {
            ...state.settings,
            appearance: sanitizeAppearanceSettings({
              ...state.settings.appearance,
              ...patch,
            }),
          },
        }));
      },
      setAppearanceValue(key, value) {
        set((state) => ({
          ...state,
          settings: {
            ...state.settings,
            appearance: sanitizeAppearanceSettings({
              ...state.settings.appearance,
              [key]: value,
            }),
          },
        }));
      },
      updateExecution(patch) {
        set((state) => ({
          ...state,
          settings: {
            ...state.settings,
            execution: sanitizeExecutionSettings({
              ...state.settings.execution,
              ...patch,
            }),
          },
        }));
      },
      setDefaultTarget(target) {
        set((state) => ({
          ...state,
          settings: {
            ...state.settings,
            execution: sanitizeExecutionSettings({
              ...state.settings.execution,
              defaultTarget: target,
            }),
          },
        }));
      },
    },
  };
}
