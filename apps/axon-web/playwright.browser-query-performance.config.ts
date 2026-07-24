import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5178';
const port = new URL(baseURL).port || '443';

export default defineConfig({
  testDir: './tests',
  testMatch: /browser-query-performance\.spec\.ts/,
  workers: 1,
  timeout: 120_000,
  use: {
    baseURL,
    ignoreHTTPSErrors: true,
  },
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        channel: 'chromium',
        launchOptions: {
          args: ['--enable-blink-features=ForceEagerMeasureMemory'],
        },
      },
    },
  ],
  webServer: {
    command: `AXON_BROWSER_MEMORY_EVIDENCE=1 npm run dev:server -- --port ${port} --strictPort`,
    url: baseURL,
    ignoreHTTPSErrors: true,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
