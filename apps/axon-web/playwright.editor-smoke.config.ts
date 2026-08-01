import { defineConfig, devices } from '@playwright/test';

// Standalone config for the editor smoke test. It intentionally does not start
// Vite, so point PLAYWRIGHT_BASE_URL at an existing dev server when needed
// (for example, npm run dev serves https://127.0.0.1:5173).
const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5174';
const requestedBrowser = process.env.AXON_EDITOR_SMOKE_BROWSER;
const browserDevices = {
  chromium: devices['Desktop Chrome'],
  firefox: devices['Desktop Firefox'],
  webkit: devices['Desktop Safari'],
} as const;
if (requestedBrowser && !(requestedBrowser in browserDevices)) {
  throw new Error(
    `AXON_EDITOR_SMOKE_BROWSER must be chromium, firefox, or webkit; received ${requestedBrowser}`,
  );
}
const browserName = (requestedBrowser ?? 'chromium') as keyof typeof browserDevices;
const desktopDevice = browserDevices[browserName];

export default defineConfig({
  testDir: './tests',
  testMatch: /editor-smoke\.spec\.ts/,
  workers: 1,
  timeout: 60_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: browserName, use: { ...desktopDevice } }],
});
