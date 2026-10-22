import { defineConfig, devices } from '@playwright/test';

// Exercises the built `dist` rather than the dev server: `vite preview` applies vercel.json's
// rewrites, headers, and NOT_FOUND responses (scripts/vercel-routing.ts) and, like production,
// sends no COOP/COEP headers. Run `npm run build` first; this config never builds and never reuses
// a server that something else started.
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:4173';
const port = new URL(baseURL).port || '443';

export default defineConfig({
  testDir: './tests',
  testMatch: /production-bundle\.spec\.ts/,
  workers: 1,
  timeout: 120_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: `npm exec -- vite preview --host 127.0.0.1 --port ${port} --strictPort`,
    url: baseURL,
    ignoreHTTPSErrors: true,
    reuseExistingServer: false,
    timeout: 60_000,
    env: { AXON_BROWSER_MEMORY_EVIDENCE: '0' },
  },
});
