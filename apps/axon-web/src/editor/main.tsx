import { Analytics } from '@vercel/analytics/react';
import { StrictMode, Suspense, lazy } from 'react';
import { createRoot } from 'react-dom/client';
import { AppProviders } from './AppProviders.tsx';
import { useRoute } from './router.ts';
import './styles/design-tokens.css';
import './connect/connect-styles.css';

const App = lazy(() => import('./App.tsx').then((module) => ({ default: module.App })));
const ConnectPage = lazy(() =>
  import('./ConnectPage.tsx').then((module) => ({ default: module.ConnectPage })),
);

function Router() {
  const route = useRoute();
  return <Suspense fallback={null}>{route === '/connect' ? <ConnectPage /> : <App />}</Suspense>;
}

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('axon-web: missing #root element in index.html');
}

createRoot(rootEl).render(
  <StrictMode>
    <AppProviders>
      <Router />
      <Analytics />
    </AppProviders>
  </StrictMode>,
);
