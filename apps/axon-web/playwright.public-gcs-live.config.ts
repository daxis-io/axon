import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173';

export default defineConfig({
  testDir: './tests',
  testMatch: /public-gcs-live\.spec\.ts/,
  workers: 1,
  timeout: 90_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
