import { expect, test, type CDPSession, type Page } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const MIB = 1024 * 1024;
const REPEATED_QUERY_RUNS = 5;

type QueryMeasurement = {
  requestId: string;
  totalDurationMs: number;
  timeToArrowIpcReadyMs: number;
  firstPublicChunkMs: number;
  publicSuccessMs: number;
  resultBytes: number;
  resultChunks: number;
  publicArrowChunks: number;
  engineDurationMs: number;
  bytesFetched: number;
  coordinatorPeakStagedBytes: number;
  coordinatorStagingLimitBytes: number;
  cursorPeakPendingEncodedBytes: number;
  cursorPeakTransportChunkBytes: number;
};

type OverLimitMeasurement = {
  error: {
    name: string;
    message: string;
    queryError?: {
      code?: string;
      fallback_reason?: string;
      target?: string;
    };
  };
  publicArrowChunks: number;
  publicErrors: number;
  publicSuccesses: number;
};

type BrowserProbeHarness = {
  runQuery(requestId: string): Promise<QueryMeasurement>;
  runOverLimit(requestId: string, maxArrowIpcBytes: number): Promise<OverLimitMeasurement>;
  terminate(): void;
};

type BrowserProbeGlobal = typeof globalThis & {
  __axonBrowserQueryPerformance?: BrowserProbeHarness;
};

type ProbeSetup = {
  activeFileBytes: number;
  activeFileCount: number;
  fixtureName: string;
  openDurationMs: number;
  snapshotVersion: number;
  startupDurationMs: number;
  workerConstructionMs: number;
};

type PerformanceBudgets = {
  startupMs: number;
  coldQueryMs: number;
  warmQueryMs: number;
  repeatedFiveRunsMs: number;
  coordinatorStagingBytes: number;
  cursorPendingEncodedBytes: number;
  transportChunkBytes: number;
  postGcRetainedHeapDeltaBytes: number;
};

test('records real browser query timing, atomicity, and component memory bounds', async ({
  browser,
  page,
}, testInfo) => {
  const budgets: PerformanceBudgets = {
    startupMs: environmentBudget('AXON_BROWSER_QUERY_STARTUP_BUDGET_MS', 10_000),
    coldQueryMs: environmentBudget('AXON_BROWSER_QUERY_COLD_BUDGET_MS', 10_000),
    warmQueryMs: environmentBudget('AXON_BROWSER_QUERY_WARM_BUDGET_MS', 5_000),
    repeatedFiveRunsMs: environmentBudget(
      'AXON_BROWSER_QUERY_REPEATED_FIVE_RUNS_BUDGET_MS',
      10_000,
    ),
    coordinatorStagingBytes: environmentBudget(
      'AXON_BROWSER_QUERY_COORDINATOR_STAGING_BUDGET_BYTES',
      8 * MIB,
    ),
    cursorPendingEncodedBytes: environmentBudget(
      'AXON_BROWSER_QUERY_CURSOR_PENDING_BUDGET_BYTES',
      8 * MIB,
    ),
    transportChunkBytes: environmentBudget('AXON_BROWSER_QUERY_TRANSPORT_CHUNK_BUDGET_BYTES', MIB),
    postGcRetainedHeapDeltaBytes: environmentBudget(
      'AXON_BROWSER_QUERY_RETAINED_HEAP_BUDGET_BYTES',
      16 * MIB,
    ),
  };

  await page.goto('/');
  const setup = await installProbe(page);
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('HeapProfiler.enable');

  let cold: QueryMeasurement;
  let warm: QueryMeasurement;
  let repeated: QueryMeasurement[];
  let overLimit: OverLimitMeasurement;
  let heapBeforeBytes: number;
  let heapAfterBytes: number;
  try {
    cold = await runQuery(page, 'browser-query-performance-cold');
    warm = await runQuery(page, 'browser-query-performance-warm');
    heapBeforeBytes = await collectPageHeap(cdp);
    repeated = [];
    for (let index = 0; index < REPEATED_QUERY_RUNS; index += 1) {
      repeated.push(await runQuery(page, `browser-query-performance-repeat-${index + 1}`));
    }
    heapAfterBytes = await collectPageHeap(cdp);
    overLimit = await runOverLimit(page, 'browser-query-performance-over-limit', 1);
  } finally {
    await page.evaluate(() => {
      const harness = (globalThis as BrowserProbeGlobal).__axonBrowserQueryPerformance;
      harness?.terminate();
      delete (globalThis as BrowserProbeGlobal).__axonBrowserQueryPerformance;
    });
    await cdp.detach();
  }

  const rawHeapDeltaBytes = heapAfterBytes - heapBeforeBytes;
  const retainedHeapDeltaBytes = Math.max(0, rawHeapDeltaBytes);
  const repeatedTotalDurationMs = repeated.reduce(
    (total, measurement) => total + measurement.totalDurationMs,
    0,
  );
  const artifactPath = resolve(
    process.cwd(),
    process.env.AXON_BROWSER_QUERY_PERF_ARTIFACT ??
      '../../target/perf/browser-query-performance.json',
  );
  const artifact = {
    schemaVersion: 1,
    capturedAt: new Date().toISOString(),
    gitSha: process.env.AXON_BROWSER_QUERY_PERF_GIT_SHA ?? 'unknown',
    browser: {
      name: testInfo.project.name,
      version: browser.version(),
    },
    fixture: {
      name: setup.fixtureName,
      snapshotVersion: setup.snapshotVersion,
      activeFileCount: setup.activeFileCount,
      activeFileBytes: setup.activeFileBytes,
    },
    budgets,
    startup: {
      workerConstructionMs: setup.workerConstructionMs,
      openDurationMs: setup.openDurationMs,
      totalDurationMs: setup.startupDurationMs,
    },
    cold,
    warm,
    repeated: {
      runs: repeated,
      totalDurationMs: repeatedTotalDurationMs,
    },
    memory: {
      scope:
        'Chromium page and public SDK heap after GC; worker cursor and coordinator are covered by exact query counters',
      postGcBeforeBytes: heapBeforeBytes,
      postGcAfterBytes: heapAfterBytes,
      rawDeltaBytes: rawHeapDeltaBytes,
      retainedDeltaBytes: retainedHeapDeltaBytes,
    },
    atomicOverLimit: overLimit,
  };
  await mkdir(dirname(artifactPath), { recursive: true });
  await writeFile(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`, 'utf8');
  await testInfo.attach('browser-query-performance', {
    path: artifactPath,
    contentType: 'application/json',
  });

  expect(setup.startupDurationMs).toBeLessThanOrEqual(budgets.startupMs);
  assertQueryMeasurement(cold, budgets, budgets.coldQueryMs);
  assertQueryMeasurement(warm, budgets, budgets.warmQueryMs);
  for (const measurement of repeated) {
    assertQueryMeasurement(measurement, budgets, budgets.warmQueryMs);
  }
  expect(repeatedTotalDurationMs).toBeLessThanOrEqual(budgets.repeatedFiveRunsMs);
  expect(retainedHeapDeltaBytes).toBeLessThanOrEqual(budgets.postGcRetainedHeapDeltaBytes);
  expect(overLimit).toMatchObject({
    error: {
      name: 'AxonWorkerError',
      queryError: {
        code: 'fallback_required',
        fallback_reason: 'browser_runtime_constraint',
        target: 'browser_wasm',
      },
    },
    publicArrowChunks: 0,
    publicErrors: 1,
    publicSuccesses: 0,
  });
});

async function installProbe(page: Page): Promise<ProbeSetup> {
  return page.evaluate(async () => {
    type ManifestFile = {
      relative_path: string;
      url_path: string;
      size_bytes: number;
      partition_values: Record<string, string>;
    };
    type RawPublicMessage = {
      arrow_ipc_chunk?: { request_id?: string };
      error?: { request_id?: string };
      success?: { request_id?: string };
    };
    type TimedRawMessage = { atMs: number; value: RawPublicMessage };
    type RuntimeEvent = {
      progress?: {
        context?: { request_id?: string };
        stage?: string;
      };
    };
    type TimedRuntimeEvent = { atMs: number; value: RuntimeEvent };

    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const manifest = (await (
      await fetch('/fixtures/prod-like/delta-log-manifest.json')
    ).json()) as {
      name: string;
      expected_latest_version: number;
      data_files: ManifestFile[];
    };
    const maybeActiveFiles = ['B', 'D'].map((category) =>
      manifest.data_files.findLast((file) => file.partition_values.category === category),
    );
    if (maybeActiveFiles.some((file) => file === undefined)) {
      throw new Error('browser query performance fixture omitted active B or D data');
    }
    const activeFiles = maybeActiveFiles as ManifestFile[];
    const workerStartedAt = performance.now();
    const workerConstructionStartedAt = performance.now();
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const workerConstructionMs = performance.now() - workerConstructionStartedAt;
    const rawMessages: TimedRawMessage[] = [];
    const runtimeEvents: TimedRuntimeEvent[] = [];
    worker.addEventListener('message', (event: MessageEvent<RawPublicMessage>) => {
      rawMessages.push({ atMs: performance.now(), value: event.data });
    });
    const client = sdk.createAxonBrowserClient({
      worker,
      onEvent: (event: unknown) => {
        runtimeEvents.push({
          atMs: performance.now(),
          value: event as RuntimeEvent,
        });
      },
    });
    const tableName = 'browser_query_performance';
    const tableUri = new URL('/fixtures/prod-like/table', location.href).href;
    const openedAt = performance.now();
    await client.openDeltaTable(
      tableName,
      {
        table_uri: tableUri,
        snapshot_version: manifest.expected_latest_version,
        partition_column_types: { category: 'string' },
        browser_compatibility: { capabilities: {} },
        required_capabilities: { capabilities: {} },
        active_files: activeFiles.map((file) => ({
          path: file.relative_path,
          url: new URL(file.url_path, location.href).href,
          size_bytes: file.size_bytes,
          partition_values: file.partition_values,
        })),
      },
      { requestId: 'browser-query-performance-open' },
    );
    const openDurationMs = performance.now() - openedAt;
    const startupDurationMs = performance.now() - workerStartedAt;
    rawMessages.length = 0;
    runtimeEvents.length = 0;

    const requiredMetric = (value: unknown, field: string): number => {
      if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        throw new Error(`browser query performance metric '${field}' was not finite`);
      }
      return value;
    };
    const runQuery = async (requestId: string): Promise<QueryMeasurement> => {
      rawMessages.length = 0;
      runtimeEvents.length = 0;
      const startedAt = performance.now();
      const result = await client.query(
        tableName,
        `SELECT category, id, value FROM ${tableName} ORDER BY id`,
        { requestId },
      );
      const finishedAt = performance.now();
      const queryEvents = runtimeEvents.filter(
        (event) => event.value.progress?.context?.request_id === requestId,
      );
      const arrowReady = queryEvents.find(
        (event) => event.value.progress?.stage === 'arrow_ipc_ready',
      );
      const queryMessages = rawMessages.filter(
        (message) =>
          message.value.arrow_ipc_chunk?.request_id === requestId ||
          message.value.success?.request_id === requestId,
      );
      const firstChunk = queryMessages.find(
        (message) => message.value.arrow_ipc_chunk?.request_id === requestId,
      );
      const success = queryMessages.find(
        (message) => message.value.success?.request_id === requestId,
      );
      if (!arrowReady || !firstChunk || !success) {
        throw new Error(`browser query '${requestId}' omitted an atomic delivery milestone`);
      }
      const metrics = result.response.metrics;
      const measurement: QueryMeasurement = {
        requestId,
        totalDurationMs: finishedAt - startedAt,
        timeToArrowIpcReadyMs: arrowReady.atMs - startedAt,
        firstPublicChunkMs: firstChunk.atMs - startedAt,
        publicSuccessMs: success.atMs - startedAt,
        resultBytes: result.result.byte_length,
        resultChunks: result.result.chunk_count,
        publicArrowChunks: queryMessages.filter(
          (message) => message.value.arrow_ipc_chunk?.request_id === requestId,
        ).length,
        engineDurationMs: requiredMetric(metrics.duration_ms, 'duration_ms'),
        bytesFetched: requiredMetric(metrics.bytes_fetched, 'bytes_fetched'),
        coordinatorPeakStagedBytes: requiredMetric(
          metrics.coordinator_peak_staged_bytes,
          'coordinator_peak_staged_bytes',
        ),
        coordinatorStagingLimitBytes: requiredMetric(
          metrics.coordinator_staging_limit_bytes,
          'coordinator_staging_limit_bytes',
        ),
        cursorPeakPendingEncodedBytes: requiredMetric(
          metrics.cursor_peak_pending_encoded_bytes,
          'cursor_peak_pending_encoded_bytes',
        ),
        cursorPeakTransportChunkBytes: requiredMetric(
          metrics.cursor_peak_transport_chunk_bytes,
          'cursor_peak_transport_chunk_bytes',
        ),
      };
      rawMessages.length = 0;
      runtimeEvents.length = 0;
      return measurement;
    };
    const runOverLimit = async (
      requestId: string,
      maxArrowIpcBytes: number,
    ): Promise<OverLimitMeasurement> => {
      rawMessages.length = 0;
      runtimeEvents.length = 0;
      let captured:
        | {
            name: string;
            message: string;
            queryError?: {
              code?: string;
              fallback_reason?: string;
              target?: string;
            };
          }
        | undefined;
      try {
        await client.query(tableName, `SELECT category, id, value FROM ${tableName} ORDER BY id`, {
          requestId,
          queryOptions: {
            runtime_limits: { max_arrow_ipc_bytes: maxArrowIpcBytes },
          },
        });
      } catch (error) {
        const candidate = error as {
          name?: string;
          message?: string;
          queryError?: {
            code?: string;
            fallback_reason?: string;
            target?: string;
          };
        };
        captured = {
          name: String(candidate.name),
          message: String(candidate.message),
          queryError: candidate.queryError,
        };
      }
      if (!captured) throw new Error('over-limit browser query unexpectedly succeeded');
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 25));
      const measurement: OverLimitMeasurement = {
        error: captured,
        publicArrowChunks: rawMessages.filter(
          (message) => message.value.arrow_ipc_chunk?.request_id === requestId,
        ).length,
        publicErrors: rawMessages.filter((message) => message.value.error?.request_id === requestId)
          .length,
        publicSuccesses: rawMessages.filter(
          (message) => message.value.success?.request_id === requestId,
        ).length,
      };
      rawMessages.length = 0;
      runtimeEvents.length = 0;
      return measurement;
    };

    (globalThis as BrowserProbeGlobal).__axonBrowserQueryPerformance = {
      runQuery,
      runOverLimit,
      terminate: () => client.terminate(),
    };
    return {
      activeFileBytes: activeFiles.reduce((total, file) => total + file.size_bytes, 0),
      activeFileCount: activeFiles.length,
      fixtureName: manifest.name,
      openDurationMs,
      snapshotVersion: manifest.expected_latest_version,
      startupDurationMs,
      workerConstructionMs,
    };
  });
}

async function runQuery(page: Page, requestId: string): Promise<QueryMeasurement> {
  return page.evaluate(async (id) => {
    const harness = (globalThis as BrowserProbeGlobal).__axonBrowserQueryPerformance;
    if (!harness) throw new Error('browser query performance harness was not installed');
    return harness.runQuery(id);
  }, requestId);
}

async function runOverLimit(
  page: Page,
  requestId: string,
  maxArrowIpcBytes: number,
): Promise<OverLimitMeasurement> {
  return page.evaluate(
    async ({ id, maxBytes }) => {
      const harness = (globalThis as BrowserProbeGlobal).__axonBrowserQueryPerformance;
      if (!harness) throw new Error('browser query performance harness was not installed');
      return harness.runOverLimit(id, maxBytes);
    },
    { id: requestId, maxBytes: maxArrowIpcBytes },
  );
}

async function collectPageHeap(cdp: CDPSession): Promise<number> {
  await cdp.send('HeapProfiler.collectGarbage');
  const usage = await cdp.send('Runtime.getHeapUsage');
  return usage.usedSize;
}

function environmentBudget(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer`);
  }
  return value;
}

function assertQueryMeasurement(
  measurement: QueryMeasurement,
  budgets: PerformanceBudgets,
  durationBudgetMs: number,
): void {
  expect(Number.isFinite(measurement.totalDurationMs)).toBe(true);
  expect(measurement.totalDurationMs).toBeGreaterThan(0);
  expect(measurement.totalDurationMs).toBeLessThanOrEqual(durationBudgetMs);
  expect(measurement.timeToArrowIpcReadyMs).toBeGreaterThanOrEqual(0);
  expect(measurement.timeToArrowIpcReadyMs).toBeLessThanOrEqual(measurement.firstPublicChunkMs);
  expect(measurement.firstPublicChunkMs).toBeLessThanOrEqual(measurement.publicSuccessMs);
  expect(measurement.publicSuccessMs).toBeLessThanOrEqual(measurement.totalDurationMs);
  expect(measurement.resultBytes).toBeGreaterThan(0);
  expect(measurement.resultChunks).toBeGreaterThan(0);
  expect(measurement.publicArrowChunks).toBe(measurement.resultChunks);
  expect(measurement.coordinatorPeakStagedBytes).toBe(measurement.resultBytes);
  expect(measurement.coordinatorPeakStagedBytes).toBeLessThanOrEqual(
    measurement.coordinatorStagingLimitBytes,
  );
  expect(measurement.coordinatorStagingLimitBytes).toBeLessThanOrEqual(
    budgets.coordinatorStagingBytes,
  );
  expect(measurement.cursorPeakPendingEncodedBytes).toBeGreaterThan(0);
  expect(measurement.cursorPeakPendingEncodedBytes).toBeLessThanOrEqual(
    budgets.cursorPendingEncodedBytes,
  );
  expect(measurement.cursorPeakTransportChunkBytes).toBeGreaterThan(0);
  expect(measurement.cursorPeakTransportChunkBytes).toBeLessThanOrEqual(
    budgets.transportChunkBytes,
  );
}
