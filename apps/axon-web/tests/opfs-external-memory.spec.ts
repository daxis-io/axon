import { expect, test, webkit } from '@playwright/test';
import { readFileSync } from 'node:fs';
import { writeFile } from 'node:fs/promises';

const SPILL_FORCING_UNION_SEGMENTS = 16;
const SPILL_FORCING_SQL = `SELECT
  event_id % 500 AS group_id,
  SUM(quantity_sum) AS quantity_sum,
  SUM(score_sum) AS score_sum,
  COUNT(*) AS row_count,
  MIN(event_id) AS min_event_id,
  MAX(event_id) AS max_event_id,
  SUM(event_id) AS event_id_sum
FROM (
  SELECT
    MIN(expanded_event_id) AS event_id,
    SUM(expanded_event_id) AS quantity_sum,
    SUM(expanded_event_id * 2) AS score_sum
  FROM (
    SELECT event_id + segment * 65536 AS expanded_event_id
    FROM spill_forcing_events
  ) AS expanded
  GROUP BY (expanded_event_id * 17) % 1048583
) AS grouped
GROUP BY event_id % 500
ORDER BY group_id`;

type ProbeResult = {
  available: boolean;
  mode?: string;
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
  beforeTermination?: {
    activeBytes: number;
    activeScopes: number;
    activeFiles: number;
    activeHandles: number;
  };
  afterSweep?: {
    activeBytes: number;
    activeScopes: number;
    activeFiles: number;
    activeHandles: number;
    abandonedScopesDeleted: number;
  };
};

async function runWorkerProbe(
  page: import('@playwright/test').Page,
  mode = 'lifecycle',
): Promise<ProbeResult> {
  await page.goto('/');
  return page.evaluate(
    (workerMode) =>
      new Promise<ProbeResult>((resolve, reject) => {
        const workerUrl = new URL('/tests/opfs-spill-test-worker.ts', location.href);
        workerUrl.searchParams.set('mode', workerMode);
        const worker = new Worker(workerUrl, { type: 'module' });
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
    mode,
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

test('a new worker scavenges an old unlocked scope left by worker termination', async ({
  browserName,
  page,
}) => {
  const abandoned = await runWorkerProbe(page, 'abandon');
  if (browserName === 'webkit' && !abandoned.available) {
    expect(abandoned.reason ?? abandoned.error).toBeTruthy();
    return;
  }
  expect(abandoned).toMatchObject({
    available: true,
    mode: 'abandon',
    beforeTermination: {
      activeBytes: 4,
      activeScopes: 1,
      activeFiles: 1,
      activeHandles: 0,
    },
  });

  const swept = await runWorkerProbe(page, 'sweep');
  expect(swept).toMatchObject({
    available: true,
    mode: 'sweep',
    afterSweep: {
      activeBytes: 0,
      activeScopes: 0,
      activeFiles: 0,
      activeHandles: 0,
    },
  });
  expect(swept.afterSweep?.abandonedScopesDeleted).toBeGreaterThanOrEqual(1);
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

test('private WebKit reports OPFS unavailable while a non-spilling query still succeeds', async ({
  browserName,
  page,
}) => {
  test.skip(browserName !== 'webkit', 'private-context qualification is WebKit-specific');
  const probe = await runWorkerProbe(page);
  expect(probe.available).toBe(false);
  expect(probe.reason ?? probe.error).toBeTruthy();

  const manifest = JSON.parse(
    readFileSync(
      new URL('../public/fixtures/prod-like/page-index-ab/manifest.json', import.meta.url),
      'utf8',
    ),
  ) as { url_path: string; size_bytes: number; row_count: number };
  const result = await page.evaluate(async (fixture) => {
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const workerUrl = new URL('/src/sandbox-query-worker.ts', location.href);
    workerUrl.searchParams.set('datafusion_spill_cap_mib', '576');
    const client = sdk.createAxonBrowserClient({
      worker: new Worker(workerUrl, { type: 'module', name: 'opfs-unavailable-small-query' }),
    });
    try {
      await client.openParquetDataset(
        'small_events',
        {
          table_uri: new URL('/fixtures/prod-like/page-index-ab', location.href).href,
          partition_column_types: {},
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          files: [
            {
              path: 'event-id.parquet',
              url: new URL(fixture.url_path, location.href).href,
              size_bytes: fixture.size_bytes,
              partition_values: {},
            },
          ],
        },
        { requestId: 'open-opfs-unavailable-small-query' },
      );
      const query = await client.query(
        'small_events',
        'SELECT MIN(event_id) AS min_event_id FROM small_events',
        {
          requestId: 'query-opfs-unavailable-small-query',
          preferredTarget: 'browser_wasm',
          delivery: 'chunked_buffers',
        },
      );
      return {
        externalMemory: query.response.capabilities.capabilities.browser_external_memory,
        rows: query.preview?.rows,
      };
    } finally {
      client.terminate();
    }
  }, manifest);

  expect(result.externalMemory).toBe('unsupported');
  expect(result.rows).toEqual([['0']]);
});

test('spill-forcing aggregate returns every expected row and leaves no active OPFS files', async ({
  browserName,
  page: fixturePage,
}, testInfo) => {
  test.setTimeout(10 * 60_000);
  test.skip(
    process.env.AXON_BROWSER_EXTERNAL_MEMORY_FULL_PARITY !== '1',
    'Set AXON_BROWSER_EXTERNAL_MEMORY_FULL_PARITY=1 after building the external-memory Wasm tier.',
  );
  const baseURL = testInfo.project.use.baseURL;
  if (typeof baseURL !== 'string') throw new Error('Playwright baseURL is required');
  const memoryProfileMiB = process.env.AXON_BROWSER_EXTERNAL_MEMORY_PROFILE_MIB ?? '64';
  if (memoryProfileMiB !== '64' && memoryProfileMiB !== '128') {
    throw new Error('AXON_BROWSER_EXTERNAL_MEMORY_PROFILE_MIB must be 64 or 128');
  }
  const repeatRuns = Number(process.env.AXON_BROWSER_EXTERNAL_MEMORY_REPEAT_RUNS ?? '1');
  if (repeatRuns !== 1 && repeatRuns !== 10) {
    throw new Error('AXON_BROWSER_EXTERNAL_MEMORY_REPEAT_RUNS must be 1 or 10');
  }
  const manifest = JSON.parse(
    readFileSync(
      new URL('../public/fixtures/prod-like/page-index-ab/manifest.json', import.meta.url),
      'utf8',
    ),
  ) as { url_path: string; size_bytes: number; row_count: number };
  expect(manifest.row_count).toBe(65_536);
  const persistentWebKitContext =
    browserName === 'webkit'
      ? await webkit.launchPersistentContext(testInfo.outputPath('full-parity-webkit-profile'), {
          ignoreHTTPSErrors: true,
        })
      : null;
  const page =
    persistentWebKitContext?.pages()[0] ??
    (persistentWebKitContext ? await persistentWebKitContext.newPage() : fixturePage);

  try {
    await page.goto(baseURL);
    const result = await page.evaluate(
      async ({ fixture, measureUserAgentMemory, memoryProfile, repeat, sql, unionSegments }) => {
        type ExternalMemory = {
          active_files?: number;
          bytes_written?: number;
          cleanup_count?: number;
          files_created?: number;
          peak_reservation_bytes?: number;
          working_set_limit_bytes?: number;
        };
        type MemoryMeasurement = { bytes: number };
        const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
        const workerUrl = new URL('/src/sandbox-query-worker.ts', location.href);
        workerUrl.searchParams.set('datafusion_memory_profile_mib', memoryProfile);
        workerUrl.searchParams.set('datafusion_spill_cap_mib', '576');
        let externalMemory: ExternalMemory | undefined;
        const client = sdk.createAxonBrowserClient({
          worker: new Worker(workerUrl, {
            type: 'module',
            name: 'external-memory-full-parity',
          }),
          onEvent: (event: unknown) => {
            const owned = (event as { owned_memory_metrics?: { external_memory?: ExternalMemory } })
              .owned_memory_metrics;
            if (owned?.external_memory) externalMemory = owned.external_memory;
          },
        });
        try {
          await client.openParquetDataset(
            'spill_forcing_events',
            {
              table_uri: new URL('/fixtures/prod-like/page-index-ab', location.href).href,
              partition_column_types: { segment: 'int64' },
              browser_compatibility: { capabilities: {} },
              required_capabilities: { capabilities: {} },
              files: Array.from({ length: unionSegments }, (_, segment) => ({
                path: `segment=${segment}/event-id.parquet`,
                url: new URL(fixture.url_path, location.href).href,
                size_bytes: fixture.size_bytes,
                partition_values: { segment: String(segment) },
              })),
            },
            { requestId: 'open-spill-forcing-full-parity' },
          );
          const capabilityProbe = await client.query(
            'spill_forcing_events',
            'SELECT event_id FROM spill_forcing_events LIMIT 1',
            {
              requestId: 'query-spill-capability-probe',
              preferredTarget: 'browser_wasm',
              delivery: 'chunked_buffers',
            },
          );
          if (
            capabilityProbe.response.capabilities.capabilities.browser_external_memory !==
            'supported'
          ) {
            throw new Error(
              `external-memory runtime was not enabled: ${JSON.stringify(capabilityProbe.response.capabilities)}`,
            );
          }
          const measure = (
            performance as Performance & {
              measureUserAgentSpecificMemory?: () => Promise<MemoryMeasurement>;
            }
          ).measureUserAgentSpecificMemory;
          if (measureUserAgentMemory && (!crossOriginIsolated || typeof measure !== 'function')) {
            throw new Error(
              '10-run qualification requires cross-origin-isolated user-agent memory measurement',
            );
          }
          const runs = [];
          let afterWarmupBytes: number | undefined;
          for (let index = 0; index < repeat; index += 1) {
            externalMemory = undefined;
            const query = await client.query('spill_forcing_events', sql, {
              requestId: `query-spill-forcing-full-parity-${index + 1}`,
              preferredTarget: 'browser_wasm',
              delivery: 'chunked_buffers',
              queryOptions: {
                collect_metrics: true,
                result_page: { limit: 501, offset: 0 },
              },
            });
            runs.push({
              columns: query.preview?.columns ?? [],
              rows: (query.preview?.rows ?? []).map((row: unknown[]) =>
                row.map((cell: unknown) => String(cell)),
              ),
              previewRowCount: query.preview?.row_count,
              responseMetrics: query.response.metrics,
              externalMemory: externalMemory as ExternalMemory | undefined,
            });
            if (measureUserAgentMemory && index === 0) {
              afterWarmupBytes = (await measure!.call(performance)).bytes;
            }
          }
          return {
            runs,
            memory: measureUserAgentMemory
              ? {
                  afterWarmupBytes,
                  afterTenRunsBytes: (await measure!.call(performance)).bytes,
                }
              : undefined,
          };
        } finally {
          client.terminate();
        }
      },
      {
        fixture: manifest,
        measureUserAgentMemory: repeatRuns === 10 && browserName === 'chromium',
        memoryProfile: memoryProfileMiB,
        repeat: repeatRuns,
        sql: SPILL_FORCING_SQL,
        unionSegments: SPILL_FORCING_UNION_SEGMENTS,
      },
    );

    const expandedRowCount = manifest.row_count * SPILL_FORCING_UNION_SEGMENTS;
    const expectedRows = Array.from({ length: 500 }, (_, groupId) => {
      const rowCount =
        groupId < expandedRowCount % 500
          ? Math.floor(expandedRowCount / 500) + 1
          : Math.floor(expandedRowCount / 500);
      const maxEventId = groupId + 500 * (rowCount - 1);
      const eventIdSum = (rowCount * (groupId + maxEventId)) / 2;
      return [groupId, eventIdSum, eventIdSum * 2, rowCount, groupId, maxEventId, eventIdSum].map(
        String,
      );
    });
    expect(result.runs).toHaveLength(repeatRuns);
    for (const run of result.runs) {
      expect(run.columns).toEqual([
        'group_id',
        'quantity_sum',
        'score_sum',
        'row_count',
        'min_event_id',
        'max_event_id',
        'event_id_sum',
      ]);
      expect(run.previewRowCount).toBe(500);
      expect(run.rows).toEqual(expectedRows);
      expect(run.responseMetrics.spill_bytes_written).toBeGreaterThan(0);
      expect(run.externalMemory).toMatchObject({
        active_files: 0,
        cleanup_count: expect.any(Number),
        files_created: expect.any(Number),
        working_set_limit_bytes: Number(memoryProfileMiB) * 1024 * 1024,
      });
      expect(run.externalMemory?.cleanup_count).toBeGreaterThan(0);
      expect(run.externalMemory?.files_created).toBeGreaterThan(0);
      expect(run.externalMemory?.peak_reservation_bytes).toBeLessThanOrEqual(
        Number(memoryProfileMiB) * 1024 * 1024,
      );
    }
    if (repeatRuns === 10) {
      const steadyStateReservationPeaks = result.runs
        .slice(-5)
        .map((run) => run.externalMemory?.peak_reservation_bytes);
      expect(new Set(steadyStateReservationPeaks).size).toBe(1);
    }
    if (repeatRuns === 10 && browserName === 'chromium') {
      const afterWarmupBytes = result.memory?.afterWarmupBytes;
      const afterTenRunsBytes = result.memory?.afterTenRunsBytes;
      expect(afterWarmupBytes).toEqual(expect.any(Number));
      expect(afterTenRunsBytes).toEqual(expect.any(Number));
      const retainedGrowthBytes = Math.max(0, afterTenRunsBytes! - afterWarmupBytes!);
      const retainedGrowthBudgetBytes = Number(
        process.env.AXON_BROWSER_EXTERNAL_MEMORY_RETAINED_GROWTH_BUDGET_BYTES ?? 64 * 1024 * 1024,
      );
      expect(Number.isSafeInteger(retainedGrowthBudgetBytes)).toBe(true);
      expect(retainedGrowthBudgetBytes).toBeGreaterThan(0);
      expect(retainedGrowthBytes).toBeLessThanOrEqual(retainedGrowthBudgetBytes);
      const evidencePath = testInfo.outputPath('external-memory-ten-run-qualification.json');
      await writeFile(
        evidencePath,
        `${JSON.stringify(
          {
            memoryProfileMiB: Number(memoryProfileMiB),
            runs: repeatRuns,
            afterWarmupBytes,
            afterTenRunsBytes,
            retainedGrowthBytes,
            retainedGrowthBudgetBytes,
            spill: result.runs.map((run) => ({
              bytesWritten: run.responseMetrics.spill_bytes_written,
              bytesRead: run.responseMetrics.spill_bytes_read,
              filesCreated: run.responseMetrics.spill_files_created,
              activeFiles: run.externalMemory?.active_files,
              peakReservationBytes: run.externalMemory?.peak_reservation_bytes,
            })),
          },
          null,
          2,
        )}\n`,
        'utf8',
      );
      await testInfo.attach('external-memory-ten-run-qualification', {
        path: evidencePath,
        contentType: 'application/json',
      });
    }
  } finally {
    await persistentWebKitContext?.close();
  }
});
