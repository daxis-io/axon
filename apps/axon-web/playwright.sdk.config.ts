import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  testMatch: /(?:axon-browser-sdk|object-storage)\.spec\.ts/,
  timeout: 30_000,
});
