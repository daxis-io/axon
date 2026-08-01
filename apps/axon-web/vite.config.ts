import { resolve } from 'node:path';
import basicSsl from '@vitejs/plugin-basic-ssl';
import react from '@vitejs/plugin-react';
import { defineConfig, type Plugin } from 'vite';
import {
  browserRuntimeBuildManifest,
  resolveBrowserRuntimeBuildTier,
} from './scripts/browser-runtime-build.ts';

const browserMemoryEvidence = process.env.AXON_BROWSER_MEMORY_EVIDENCE === '1';
const browserRuntimeBuildTier = resolveBrowserRuntimeBuildTier(
  process.env.AXON_BROWSER_RUNTIME_BUILD_TIER,
);

export default defineConfig({
  plugins: [emitBrowserRuntimeBuildManifest(), blockLegacySandboxRoute(), basicSsl(), react()],
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
  // Workers are constructed with `{ type: 'module' }`, so emit them as ES modules rather than the
  // default IIFE.
  worker: {
    format: 'es',
  },
  build: {
    rollupOptions: {
      input: {
        editor: resolve(__dirname, 'index.html'),
      },
    },
  },
});

function emitBrowserRuntimeBuildManifest(): Plugin {
  return {
    name: 'axon-browser-runtime-build-manifest',
    generateBundle() {
      this.emitFile({
        type: 'asset',
        fileName: 'axon-runtime-build.json',
        source: `${JSON.stringify(browserRuntimeBuildManifest(browserRuntimeBuildTier), null, 2)}\n`,
      });
    },
  };
}

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
