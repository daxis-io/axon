import { defineConfig, devices } from '@playwright/test';

import publicGcsLiveConfig from './playwright.public-gcs-live.config';

const requestedPublicGcsLiveSpec = process.argv.some((arg) =>
  /(^|[/\\])public-gcs-live\.spec\.ts$/.test(arg),
);
const browserWorkerBaseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173';
const browserWorkerPort = new URL(browserWorkerBaseURL).port || '443';

const browserWorkerMatrixConfig = defineConfig({
  testDir: './tests',
  testMatch: /(?:browser-worker-matrix|internal-arrow-ipc-stream)\.spec\.ts/,
  workers: 1,
  timeout: 60_000,
  use: {
    baseURL: browserWorkerBaseURL,
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
    command: `npm run dev:server -- --port ${browserWorkerPort} --strictPort`,
    url: browserWorkerBaseURL,
    ignoreHTTPSErrors: true,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});

export default requestedPublicGcsLiveSpec ? publicGcsLiveConfig : browserWorkerMatrixConfig;
