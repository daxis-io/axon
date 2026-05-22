import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  testMatch: /public-gcs-live\.spec\.ts/,
  workers: 1,
  timeout: 30_000,
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
});
