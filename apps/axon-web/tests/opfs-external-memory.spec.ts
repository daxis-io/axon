import { expect, test, webkit } from '@playwright/test';

type ProbeResult = {
  available: boolean;
  reason?: string;
  actualBytes?: number;
  checksumMatches?: boolean;
  beforeCleanup?: {
    bytesWritten: number;
    bytesRead: number;
    activeBytes: number;
    activeScopes: number;
    activeFiles: number;
    activeHandles: number;
  };
  afterCleanup?: {
    activeBytes: number;
    activeScopes: number;
    activeFiles: number;
    activeHandles: number;
  };
  error?: string;
};

async function runWorkerProbe(page: import('@playwright/test').Page): Promise<ProbeResult> {
  await page.goto('/');
  return page.evaluate(
    () =>
      new Promise<ProbeResult>((resolve, reject) => {
        const worker = new Worker('/tests/opfs-spill-test-worker.ts', { type: 'module' });
        worker.addEventListener(
          'message',
          (event: MessageEvent<ProbeResult>) => {
            worker.terminate();
            resolve(event.data);
          },
          { once: true },
        );
        worker.addEventListener(
          'error',
          (event) => {
            worker.terminate();
            reject(new Error(event.message));
          },
          { once: true },
        );
      }),
  );
}

test('dedicated worker performs a real 4 MiB OPFS spill lifecycle', async ({
  browserName,
  page,
}) => {
  const result = await runWorkerProbe(page);
  if (browserName === 'webkit' && !result.available) {
    expect(result.reason ?? result.error).toBeTruthy();
    return;
  }

  expect(result).toMatchObject({
    available: true,
    actualBytes: 4 * 1024 * 1024,
    checksumMatches: true,
    beforeCleanup: {
      bytesWritten: 4 * 1024 * 1024,
      bytesRead: 4 * 1024 * 1024,
      activeBytes: 4 * 1024 * 1024,
      activeScopes: 1,
      activeFiles: 1,
      activeHandles: 0,
    },
    afterCleanup: {
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    },
  });
});

test('persistent WebKit context supports the real OPFS lifecycle', async ({
  browserName,
}, testInfo) => {
  test.skip(browserName !== 'webkit', 'persistent-context qualification is WebKit-specific');
  const baseURL = testInfo.project.use.baseURL;
  if (typeof baseURL !== 'string') throw new Error('Playwright baseURL is required');
  const context = await webkit.launchPersistentContext(testInfo.outputPath('webkit-profile'), {
    ignoreHTTPSErrors: true,
  });
  try {
    const page = context.pages()[0] ?? (await context.newPage());
    await page.goto(baseURL);
    const result = await runWorkerProbe(page);
    expect(result.available).toBe(true);
    expect(result.checksumMatches).toBe(true);
    expect(result.afterCleanup).toEqual({
      backend: 'opfs',
      bytesWritten: 4 * 1024 * 1024,
      bytesRead: 4 * 1024 * 1024,
      filesCreated: 1,
      activeBytes: 0,
      peakActiveBytes: 4 * 1024 * 1024,
      storageLimitBytes: 64 * 1024 * 1024,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
      mergePasses: 0,
      scopesDeleted: 1,
      abandonedScopesDeleted: 0,
    });
  } finally {
    await context.close();
  }
});
