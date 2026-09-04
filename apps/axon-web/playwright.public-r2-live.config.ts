import { defineConfig, devices } from '@playwright/test';

const baseURL = process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173';
const endpoint = process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT;
const onboarding = process.env.AXON_LIVE_PUBLIC_R2_ONBOARDING_TABLE_URI;
const performance = process.env.AXON_LIVE_PUBLIC_R2_PERF_TABLE_URI;
const configured = [
  endpoint,
  onboarding,
  performance,
  process.env.AXON_LIVE_PUBLIC_R2_DAXIS_COMMIT,
  process.env.AXON_LIVE_PUBLIC_R2_RUNTIME_COMMIT,
  process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT_CLASS,
  process.env.AXON_LIVE_PUBLIC_R2_QUALIFICATION_CONTRACT,
].every(Boolean);

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
  webServer: configured
    ? {
        command: 'npm run build && npm exec -- vite preview --host 127.0.0.1 --port 5173',
        url: baseURL,
        ignoreHTTPSErrors: true,
        reuseExistingServer: false,
        // Qualification runners may build the complete WASM dependency graph without a warm cache.
        timeout: 900_000,
      }
    : undefined,
});
