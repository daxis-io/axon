import { resolve } from 'node:path';
import basicSsl from '@vitejs/plugin-basic-ssl';
import react from '@vitejs/plugin-react';
import { defineConfig, type Plugin } from 'vite';

const browserMemoryEvidence = process.env.AXON_BROWSER_MEMORY_EVIDENCE === '1';

export default defineConfig({
  plugins: [blockLegacySandboxRoute(), basicSsl(), react()],
  server: {
    host: '127.0.0.1',
    headers: browserMemoryEvidence
      ? {
          'Cross-Origin-Embedder-Policy': 'require-corp',
          'Cross-Origin-Opener-Policy': 'same-origin',
        }
      : undefined,
    port: 5173,
    strictPort: true,
  },
  build: {
    rollupOptions: {
      input: {
        editor: resolve(__dirname, 'index.html'),
      },
    },
  },
});

function blockLegacySandboxRoute(): Plugin {
  return {
    name: 'axon-block-legacy-sandbox-route',
    configureServer(server) {
      server.middlewares.use(blockSandboxHtml);
    },
    configurePreviewServer(server) {
      server.middlewares.use(blockSandboxHtml);
    },
  };
}

function blockSandboxHtml(
  req: { url?: string },
  res: { statusCode: number; end: (body?: string) => void },
  next: () => void,
): void {
  if ((req.url ?? '').split('?')[0] === '/sandbox.html') {
    res.statusCode = 404;
    res.end('Not found');
    return;
  }
  next();
}
