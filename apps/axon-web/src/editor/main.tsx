import { Analytics } from '@vercel/analytics/react';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App.tsx';
import { ConnectPage } from './ConnectPage.tsx';
import { useRoute } from './router.ts';
import './styles/design-tokens.css';
import './connect/connect-styles.css';

function Router() {
  const route = useRoute();
  if (route === '/connect') return <ConnectPage />;
  return <App />;
}

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('axon-web: missing #root element in index.html');
}

createRoot(rootEl).render(
  <StrictMode>
    <Router />
    <Analytics />
  </StrictMode>,
);
