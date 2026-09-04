import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const requiredLiveEnvironment = {
  AXON_LIVE_PUBLIC_R2_ENDPOINT: 'https://pub-example.r2.dev',
  AXON_LIVE_PUBLIC_R2_ONBOARDING_TABLE_URI: 'r2://axon-public-data/fixtures/onboarding-v1/table',
  AXON_LIVE_PUBLIC_R2_PERF_TABLE_URI: 'r2://axon-public-data/fixtures/s3-browser-perf-v1/table',
  AXON_LIVE_PUBLIC_R2_DAXIS_COMMIT: '1'.repeat(40),
  AXON_LIVE_PUBLIC_R2_RUNTIME_COMMIT: '2'.repeat(40),
  AXON_LIVE_PUBLIC_R2_ENDPOINT_CLASS: 'r2.dev',
  AXON_LIVE_PUBLIC_R2_QUALIFICATION_CONTRACT: '/tmp/public-r2-contract.json',
};

describe('public R2 live Playwright configuration', () => {
  beforeEach(() => {
    vi.resetModules();
    for (const [name, value] of Object.entries(requiredLiveEnvironment)) vi.stubEnv(name, value);
  });

  afterEach(() => vi.unstubAllEnvs());

  it('budgets enough startup time for a cold browser WASM build', async () => {
    const { default: config } = await import('../playwright.public-r2-live.config.ts');

    expect(config.webServer).toMatchObject({ timeout: 900_000 });
  });
});
