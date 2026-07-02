import { Analytics } from '@vercel/analytics/react';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { AppProviders } from './AppProviders.tsx';
import { AppRouter } from './router.tsx';
import { SettingsEffects } from './SettingsEffects.tsx';
import './styles/design-tokens.css';
import './connect/connect-styles.css';

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('axon-web: missing #root element in index.html');
}

createRoot(rootEl).render(
  <StrictMode>
    <AppProviders>
      <SettingsEffects />
      <AppRouter />
      <Analytics />
    </AppProviders>
  </StrictMode>,
);
