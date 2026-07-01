import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173';
const liveTableUri = process.env.AXON_LIVE_PUBLIC_S3_TABLE_URI;
const liveRegion = process.env.AXON_LIVE_PUBLIC_S3_REGION;

export default defineConfig({
  testDir: './tests',
  testMatch: /public-s3-live\.spec\.ts/,
  workers: 1,
  timeout: 90_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer:
    liveTableUri && liveRegion
      ? {
          command: 'npm run dev',
          url: baseURL,
          ignoreHTTPSErrors: true,
          reuseExistingServer: !process.env.CI,
          timeout: 60_000,
        }
      : undefined,
});
