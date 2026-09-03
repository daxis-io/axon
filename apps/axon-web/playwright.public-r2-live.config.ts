import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173';
const endpoint = process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT;
const onboarding = process.env.AXON_LIVE_PUBLIC_R2_ONBOARDING_TABLE_URI;
const performance = process.env.AXON_LIVE_PUBLIC_R2_PERF_TABLE_URI;

export default defineConfig({
  testDir: './tests',
  testMatch: /public-r2-live\.spec\.ts/,
  workers: 1,
  timeout: 120_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer:
    endpoint && onboarding && performance
      ? {
          command: 'npm run dev',
          url: baseURL,
          ignoreHTTPSErrors: true,
          reuseExistingServer: !process.env.CI,
          timeout: 240_000,
        }
      : undefined,
});
