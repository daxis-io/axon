import {
  selectAppearanceSettings,
  selectDefaultTarget,
  useAxonClientStore,
} from '../state/hooks.ts';
import { IconChevR, IconSettings } from './components/icons.tsx';
import { navigate } from './router.tsx';

export function SettingsPage() {
  const appearance = useAxonClientStore(selectAppearanceSettings);
  const defaultTarget = useAxonClientStore(selectDefaultTarget);

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

        <section className="settings-shell" aria-label="Settings sections">
          <article className="settings-shell-row">
            <div>
              <h2>Appearance</h2>
              <p>
                {appearance.theme} · {appearance.density} · {appearance.accent}
              </p>
            </div>
          </article>
          <article className="settings-shell-row">
            <div>
              <h2>Typography</h2>
              <p>
                {appearance.uiFont} · {appearance.monoFont}
              </p>
            </div>
          </article>
          <article className="settings-shell-row">
            <div>
              <h2>Execution</h2>
              <p>{defaultTarget}</p>
            </div>
          </article>
          <article className="settings-shell-row">
            <div>
              <h2>Advanced</h2>
              <p>JSON</p>
            </div>
          </article>
        </section>
      </main>
    </div>
  );
}
