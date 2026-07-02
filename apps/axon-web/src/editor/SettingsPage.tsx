import { useEffect, useMemo, useState } from 'react';
import { SERVER_QUERY_FALLBACK_ENABLED } from '../services/server-fallback.ts';
import {
  selectAppearanceSettings,
  selectDefaultTarget,
  selectSettingsActions,
  useAxonClientStore,
} from '../state/hooks.ts';
import {
  ACCENT_VALUES,
  APPEARANCE_DENSITY_VALUES,
  APPEARANCE_THEME_VALUES,
  MONO_FONT_VALUES,
  UI_FONT_VALUES,
  availableExecutionTargetValues,
  coerceDefaultTargetForAvailability,
  type AccentSetting,
  type AppearanceDensitySetting,
  type AppearanceThemeSetting,
  type DefaultTargetSetting,
  type MonoFontSetting,
  type SettingsState,
  type UiFontSetting,
} from '../state/slices/settings.ts';
import {
  applySettingsState,
  parseSettingsPatchJson,
  settingsToJson,
} from '../settings/settings-model.ts';
import { IconChevR, IconSettings } from './components/icons.tsx';
import { navigate } from './router.tsx';

export function SettingsPage() {
  const appearance = useAxonClientStore(selectAppearanceSettings);
  const configuredDefaultTarget = useAxonClientStore(selectDefaultTarget);
  const settingsActions = useAxonClientStore(selectSettingsActions);
  const defaultTarget = coerceDefaultTargetForAvailability(
    configuredDefaultTarget,
    SERVER_QUERY_FALLBACK_ENABLED,
  );
  const executionTargets = availableExecutionTargetValues(SERVER_QUERY_FALLBACK_ENABLED);
  const settings = useMemo<SettingsState>(
    () => ({
      appearance,
      execution: { defaultTarget },
    }),
    [appearance, defaultTarget],
  );
  const currentJson = useMemo(() => settingsToJson(settings), [settings]);
  const [draftJson, setDraftJson] = useState(currentJson);
  const [draftDirty, setDraftDirty] = useState(false);
  const [advancedStatus, setAdvancedStatus] = useState<
    { kind: 'ok' | 'error'; message: string } | undefined
  >();

  useEffect(() => {
    if (!draftDirty) {
      setDraftJson(currentJson);
    }
  }, [currentJson, draftDirty]);

  const applyRawJson = () => {
    const result = parseSettingsPatchJson(draftJson, {
      serverFallbackEnabled: SERVER_QUERY_FALLBACK_ENABLED,
    });
    if (!result.ok) {
      setAdvancedStatus({ kind: 'error', message: result.error });
      return;
    }

    applySettingsState(settingsActions, result.settings);
    setDraftJson(settingsToJson(result.settings));
    setDraftDirty(false);
    setAdvancedStatus({ kind: 'ok', message: 'Applied' });
  };

  const resetRawJson = () => {
    setDraftJson(currentJson);
    setDraftDirty(false);
    setAdvancedStatus(undefined);
  };

  return (
    <div className="cc-page settings-page">
      <header className="cc-page-bar">
        <button className="cc-page-brand" onClick={() => navigate('/')} title="Back to workspace">
          <span className="brand-mark">A</span>
          <span className="brand-name">
            axon <span>· web</span>
          </span>
        </button>

        <span className="cc-page-crumb">/ Settings</span>

        <div className="cc-page-spacer" />

        <button className="cc-btn" onClick={() => navigate('/')}>
          Workspace <IconChevR size={11} />
        </button>
      </header>

      <main className="settings-main">
        <section className="settings-head">
          <div className="settings-title">
            <IconSettings size={16} />
            <span>Settings</span>
          </div>
          <div className="settings-summary" aria-label="Current settings">
            <span>{appearance.theme}</span>
            <span>{appearance.density}</span>
            <span>{defaultTarget}</span>
          </div>
        </section>

        <section className="settings-surface" aria-label="Settings controls">
          <SettingsSection title="Appearance">
            <SegmentedControl
              label="Mode"
              value={appearance.theme}
              options={APPEARANCE_THEME_VALUES}
              onChange={(value) => settingsActions.setAppearanceValue('theme', value)}
            />
            <ColorControl
              label="Accent"
              value={appearance.accent}
              options={ACCENT_VALUES}
              onChange={(value) => settingsActions.setAppearanceValue('accent', value)}
            />
            <SegmentedControl
              label="Density"
              value={appearance.density}
              options={APPEARANCE_DENSITY_VALUES}
              onChange={(value) => settingsActions.setAppearanceValue('density', value)}
            />
          </SettingsSection>

          <SettingsSection title="Typography">
            <SelectControl
              label="UI font"
              value={appearance.uiFont}
              options={UI_FONT_VALUES}
              onChange={(value) => settingsActions.setAppearanceValue('uiFont', value)}
            />
            <SelectControl
              label="Code font"
              value={appearance.monoFont}
              options={MONO_FONT_VALUES}
              onChange={(value) => settingsActions.setAppearanceValue('monoFont', value)}
            />
          </SettingsSection>

          <SettingsSection title="Execution">
            <SegmentedControl
              label="Default target"
              value={defaultTarget}
              options={executionTargets}
              onChange={settingsActions.setDefaultTarget}
            />
          </SettingsSection>

          <SettingsSection title="Advanced">
            <label className="settings-field">
              <span>Raw JSON</span>
              <textarea
                className="settings-json"
                spellCheck={false}
                value={draftJson}
                onChange={(event) => {
                  setDraftJson(event.target.value);
                  setDraftDirty(true);
                  setAdvancedStatus(undefined);
                }}
              />
            </label>
            <div className="settings-actions">
              <button className="cc-btn primary" onClick={applyRawJson}>
                Apply
              </button>
              <button className="cc-btn" onClick={resetRawJson} disabled={!draftDirty}>
                Reset
              </button>
              {advancedStatus && (
                <span className={'settings-status ' + advancedStatus.kind}>
                  {advancedStatus.message}
                </span>
              )}
            </div>
          </SettingsSection>
        </section>
      </main>
    </div>
  );
}

function SettingsSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <article className="settings-section">
      <h2>{title}</h2>
      <div className="settings-section-body">{children}</div>
    </article>
  );
}

function SegmentedControl<
  T extends AppearanceThemeSetting | AppearanceDensitySetting | DefaultTargetSetting,
>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: readonly T[];
  onChange: (value: T) => void;
}) {
  return (
    <div className="settings-field">
      <span>{label}</span>
      <div className="settings-seg" role="radiogroup" aria-label={label}>
        {options.map((option) => (
          <button
            key={option}
            type="button"
            role="radio"
            aria-checked={option === value}
            className={option === value ? 'active' : ''}
            onClick={() => onChange(option)}
          >
            {option}
          </button>
        ))}
      </div>
    </div>
  );
}

function SelectControl<T extends UiFontSetting | MonoFontSetting>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: readonly T[];
  onChange: (value: T) => void;
}) {
  return (
    <label className="settings-field">
      <span>{label}</span>
      <select
        className="settings-select"
        value={value}
        onChange={(event) => onChange(event.target.value as T)}
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </label>
  );
}

function ColorControl({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: AccentSetting;
  options: readonly AccentSetting[];
  onChange: (value: AccentSetting) => void;
}) {
  return (
    <div className="settings-field">
      <span>{label}</span>
      <div className="settings-swatches" role="radiogroup" aria-label={label}>
        {options.map((option) => (
          <button
            key={option}
            type="button"
            role="radio"
            aria-checked={option === value}
            className={option === value ? 'active' : ''}
            style={{ background: option }}
            title={option}
            aria-label={option}
            onClick={() => onChange(option)}
          />
        ))}
      </div>
    </div>
  );
}
