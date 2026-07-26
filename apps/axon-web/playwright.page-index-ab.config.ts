import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5181';
const port = new URL(baseURL).port || '443';

export default defineConfig({
  testDir: './tests',
  testMatch: /page-index-byte-savings-ab\.spec\.ts/,
  workers: 1,
  timeout: 180_000,
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
      },
    },
  ],
  webServer: {
    command: `npm run dev:server -- --port ${port} --strictPort`,
    url: baseURL,
    ignoreHTTPSErrors: true,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
