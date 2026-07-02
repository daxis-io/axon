import { defineConfig, devices } from '@playwright/test';

import publicGcsLiveConfig from './playwright.public-gcs-live.config';

const requestedPublicGcsLiveSpec = process.argv.some((arg) =>
  /(^|[/\\])public-gcs-live\.spec\.ts$/.test(arg),
);

const browserWorkerMatrixConfig = defineConfig({
  testDir: './tests',
  testMatch: /browser-worker-matrix\.spec\.ts/,
  workers: 1,
  timeout: 60_000,
  use: {
    baseURL: 'https://127.0.0.1:5173',
    ignoreHTTPSErrors: true,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },
  ],
  webServer: {
    command: 'npm run dev:server',
    url: 'https://127.0.0.1:5173',
    ignoreHTTPSErrors: true,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});

export default requestedPublicGcsLiveSpec ? publicGcsLiveConfig : browserWorkerMatrixConfig;
