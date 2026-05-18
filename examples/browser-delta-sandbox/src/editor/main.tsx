import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App.tsx';
import './styles/design-tokens.css';
import './connect/connect-styles.css';

const rootEl = document.getElementById('root');
if (!rootEl) {
  throw new Error('Axon editor: missing #root element in editor.html');
}

createRoot(rootEl).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
