import { useEffect } from 'react';
import { selectAppearanceSettings, useAxonClientStore } from '../state/hooks.ts';
import { hexToSoft } from './lib/format.ts';

export function SettingsEffects() {
  const appearance = useAxonClientStore(selectAppearanceSettings);

  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute('data-theme', appearance.theme === 'dark' ? 'dark' : 'light');
    root.setAttribute('data-density', appearance.density);
    root.style.setProperty('--accent', appearance.accent);
    root.style.setProperty(
      '--accent-soft',
      hexToSoft(appearance.accent, appearance.theme === 'dark'),
    );
    root.style.setProperty('--ui', `"${appearance.uiFont}", ui-sans-serif, system-ui, sans-serif`);
    root.style.setProperty('--mono', `"${appearance.monoFont}", ui-monospace, Menlo, monospace`);
  }, [appearance]);

  return null;
}
