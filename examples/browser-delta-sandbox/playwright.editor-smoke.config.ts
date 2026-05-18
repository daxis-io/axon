import { defineConfig, devices } from '@playwright/test';

// Standalone config for the editor smoke test. Assumes a Vite dev server is
// already running at https://127.0.0.1:5174 (start with: npx vite --port 5174).
export default defineConfig({
  testDir: './tests',
  testMatch: /editor-smoke\.spec\.ts/,
  workers: 1,
  timeout: 60_000,
  use: {
    baseURL: 'https://127.0.0.1:5174',
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
