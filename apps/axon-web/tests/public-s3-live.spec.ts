import { readFile, writeFile } from 'node:fs/promises';

import { expect, test, type APIRequestContext, type Page } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const liveTableUri = process.env.AXON_LIVE_PUBLIC_S3_TABLE_URI;
const liveRegion = process.env.AXON_LIVE_PUBLIC_S3_REGION;
const liveOrigin =
  process.env.AXON_LIVE_PUBLIC_S3_ORIGIN ??
  new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173').origin;
const rangeReadMetricsCaptureKey = '__AXON_PUBLIC_S3_RANGE_READ_METRICS__';
const requiredLiveMetricKeys = [
  'bytes_fetched',
  'bootstrap_footer_range_reads',
  'scan_footer_range_reads',
  'scan_data_range_reads',
  'duplicate_range_reads',
  'coalesced_range_reads',
  'coalesced_gap_bytes_fetched',
  'footer_cache_hits',
  'footer_cache_misses',
  'footer_range_reads_avoided',
  'identity_present_range_reads',
  'identity_missing_range_reads',
  'range_cache_hits',
  'range_cache_misses',
  'range_cache_bytes_reused',
  'range_cache_bytes_stored',
  'range_cache_validation_misses',
  'range_cache_degraded_identity_reads',
  'range_readahead_requests',
  'range_readahead_bytes_fetched',
  'range_readahead_bytes_used',
  'range_readahead_wasted_bytes',
  'rows_emitted',
  'arrow_ipc_bytes',
] as const;
const optionalLiveMetricKeys = ['arrow_ipc_chunk_count'] as const;
const comparisonMetricKeys = [
  'bytes_fetched',
  'scan_data_range_reads',
  'coalesced_range_reads',
  'range_cache_bytes_reused',
  'range_readahead_bytes_fetched',
  'range_readahead_bytes_used',
  'range_readahead_wasted_bytes',
  'rows_emitted',
  'arrow_ipc_bytes',
] as const;

type RequiredLiveMetricKey = (typeof requiredLiveMetricKeys)[number];
type OptionalLiveMetricKey = (typeof optionalLiveMetricKeys)[number];
type ComparisonMetricKey = (typeof comparisonMetricKeys)[number];
type LiveMetricsInput = Partial<Record<RequiredLiveMetricKey | OptionalLiveMetricKey, number>>;
const preCacheComparison: Record<ComparisonMetricKey, number | null> = {
  bytes_fetched: 22_677_645,
  scan_data_range_reads: 160,
  coalesced_range_reads: 32,
  range_cache_bytes_reused: null,
  range_readahead_bytes_fetched: null,
  range_readahead_bytes_used: null,
  range_readahead_wasted_bytes: null,
  rows_emitted: 1_048_576,
  arrow_ipc_bytes: 36_744,
};
type PublicS3LiveRunEvidence = {
  run: number;
  scalar_result: string;
  metrics: ProjectedLiveMetrics;
};
type ProjectedLiveMetrics = Record<RequiredLiveMetricKey, number> &
  Partial<Record<OptionalLiveMetricKey, number>>;
type PublicS3EvidenceBase = {
  table_uri: string;
  table_name: string;
  browser_name: string;
  base_url: string;
  region: string;
  fixture_provenance?: PublicS3FixtureProvenance;
};
type PublicS3FixtureProvenance = {
  fixture_revision: string;
  table_uri: string;
  region: string;
  manifest_sha256: string;
  object_checksums_sha256: string;
  required_object_count: number;
  active_file_count: number;
  active_data_bytes: number;
};
type PublicS3PerformanceEvidence = PublicS3EvidenceBase & {
  metrics: ProjectedLiveMetrics;
  comparison: {
    pre_cache: Record<ComparisonMetricKey, number | null>;
    current: Record<ComparisonMetricKey, number>;
  };
};
type PublicS3RepeatEvidence = PublicS3EvidenceBase & {
  repeat_count: number;
  runs: PublicS3LiveRunEvidence[];
};

const pinnedFixtureProvenance: PublicS3FixtureProvenance = {
  fixture_revision: 's3-browser-perf-v1',
  table_uri: 's3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table',
  region: 'us-east-2',
  manifest_sha256: '18d1c4c3b5e1ce78ce156ce51247a94a46e44401cad9688ec0d14ceaa01b6ab3',
  object_checksums_sha256: '05f6c5823a88c49559eef70072165b584dfe3c320ae8a435c6f6f82f30d719a9',
  required_object_count: 21,
  active_file_count: 8,
  active_data_bytes: 82_057_700,
};

const exampleLiveMetrics: LiveMetricsInput = {
  ...completeLiveMetrics(),
  bytes_fetched: 42,
  bootstrap_footer_range_reads: 1,
  scan_footer_range_reads: 2,
  scan_data_range_reads: 3,
  duplicate_range_reads: 4,
  coalesced_range_reads: 5,
  coalesced_gap_bytes_fetched: 6,
  footer_cache_hits: 7,
  footer_cache_misses: 8,
  footer_range_reads_avoided: 9,
  identity_present_range_reads: 10,
  identity_missing_range_reads: 11,
  rows_emitted: 12,
  arrow_ipc_bytes: 13,
  arrow_ipc_chunk_count: 14,
  range_cache_hits: 15,
  range_cache_misses: 16,
  range_cache_bytes_reused: 17,
  range_cache_bytes_stored: 18,
  range_cache_validation_misses: 19,
  range_cache_degraded_identity_reads: 20,
  range_readahead_requests: 21,
  range_readahead_bytes_fetched: 22,
  range_readahead_bytes_used: 23,
  range_readahead_wasted_bytes: 24,
};

test('public S3 repeat evidence redacts URI secrets and preserves per-run metrics', () => {
  const evidence = buildPublicS3RepeatEvidence({
    tableUri:
      's3://embedded-user:embedded-password@live-bucket/customer/path/table?X-Amz-Signature=signed-secret&token=query-secret#fragment-secret',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    runs: [
      {
        run: 1,
        scalar_result: '42',
        metrics: exampleLiveMetrics,
      },
      {
        run: 2,
        scalar_result: '42',
        metrics: {
          ...completeLiveMetrics(),
          bytes_fetched: 24,
          bootstrap_footer_range_reads: 1,
          scan_footer_range_reads: 1,
          scan_data_range_reads: 1,
          duplicate_range_reads: 0,
          coalesced_range_reads: 0,
          coalesced_gap_bytes_fetched: 0,
          footer_cache_hits: 0,
          footer_cache_misses: 1,
          footer_range_reads_avoided: 0,
          identity_present_range_reads: 2,
          identity_missing_range_reads: 0,
          rows_emitted: 12,
        },
      },
    ],
  });

  expect(evidence.table_uri).toBe('s3://live-bucket/customer/path/table');
  expect(evidence.table_name).toBe('table');
  expect(evidence.browser_name).toBe('chromium');
  expect(evidence.base_url).toBe('https://127.0.0.1:5173');
  expect(evidence.region).toBe('us-east-2');
  expect(evidence.repeat_count).toBe(2);
  expect(evidence.runs).toHaveLength(2);
  expect(evidence.runs[0]).toEqual({
    run: 1,
    scalar_result: '42',
    metrics: {
      bytes_fetched: 42,
      bootstrap_footer_range_reads: 1,
      scan_footer_range_reads: 2,
      scan_data_range_reads: 3,
      duplicate_range_reads: 4,
      coalesced_range_reads: 5,
      coalesced_gap_bytes_fetched: 6,
      footer_cache_hits: 7,
      footer_cache_misses: 8,
      footer_range_reads_avoided: 9,
      identity_present_range_reads: 10,
      identity_missing_range_reads: 11,
      range_cache_hits: 15,
      range_cache_misses: 16,
      range_cache_bytes_reused: 17,
      range_cache_bytes_stored: 18,
      range_cache_validation_misses: 19,
      range_cache_degraded_identity_reads: 20,
      range_readahead_requests: 21,
      range_readahead_bytes_fetched: 22,
      range_readahead_bytes_used: 23,
      range_readahead_wasted_bytes: 24,
      rows_emitted: 12,
      arrow_ipc_bytes: 13,
      arrow_ipc_chunk_count: 14,
    },
  });
  expect(evidence.runs[1]).toMatchObject({
    run: 2,
    scalar_result: '42',
    metrics: { bytes_fetched: 24, rows_emitted: 12 },
  });
  const serializedEvidence = JSON.stringify(evidence);
  expect(serializedEvidence).not.toContain('embedded-user');
  expect(serializedEvidence).not.toContain('embedded-password');
  expect(serializedEvidence).not.toContain('X-Amz-Signature');
  expect(serializedEvidence).not.toContain('signed-secret');
  expect(serializedEvidence).not.toContain('query-secret');
  expect(serializedEvidence).not.toContain('fragment-secret');
});

test('public S3 performance evidence preserves comparison metrics', () => {
  const evidence = buildPublicS3PerformanceEvidence({
    tableUri: 's3://live-bucket/customer/path/table',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    metrics: exampleLiveMetrics,
  });

  expect(evidence.metrics).toEqual({
    bytes_fetched: 42,
    bootstrap_footer_range_reads: 1,
    scan_footer_range_reads: 2,
    scan_data_range_reads: 3,
    duplicate_range_reads: 4,
    coalesced_range_reads: 5,
    coalesced_gap_bytes_fetched: 6,
    footer_cache_hits: 7,
    footer_cache_misses: 8,
    footer_range_reads_avoided: 9,
    identity_present_range_reads: 10,
    identity_missing_range_reads: 11,
    range_cache_hits: 15,
    range_cache_misses: 16,
    range_cache_bytes_reused: 17,
    range_cache_bytes_stored: 18,
    range_cache_validation_misses: 19,
    range_cache_degraded_identity_reads: 20,
    range_readahead_requests: 21,
    range_readahead_bytes_fetched: 22,
    range_readahead_bytes_used: 23,
    range_readahead_wasted_bytes: 24,
    rows_emitted: 12,
    arrow_ipc_bytes: 13,
    arrow_ipc_chunk_count: 14,
  });
  expect(evidence.comparison).toEqual({
    pre_cache: {
      bytes_fetched: 22_677_645,
      scan_data_range_reads: 160,
      coalesced_range_reads: 32,
      range_cache_bytes_reused: null,
      range_readahead_bytes_fetched: null,
      range_readahead_bytes_used: null,
      range_readahead_wasted_bytes: null,
      rows_emitted: 1_048_576,
      arrow_ipc_bytes: 36_744,
    },
    current: {
      bytes_fetched: 42,
      scan_data_range_reads: 3,
      coalesced_range_reads: 5,
      range_cache_bytes_reused: 17,
      range_readahead_bytes_fetched: 22,
      range_readahead_bytes_used: 23,
      range_readahead_wasted_bytes: 24,
      rows_emitted: 12,
      arrow_ipc_bytes: 13,
    },
  });
});

test('public S3 performance evidence identifies the pinned fixture revision', async () => {
  const evidence = buildPublicS3PerformanceEvidence({
    tableUri: `${pinnedFixtureProvenance.table_uri}?X-Amz-Signature=secret`,
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: pinnedFixtureProvenance.region,
    metrics: exampleLiveMetrics,
  });
  const trackedProvenance = JSON.parse(
    await readFile(
      new URL('../public/fixtures/s3-perf/s3-perf-provenance.json', import.meta.url),
      'utf8',
    ),
  ) as PublicS3FixtureProvenance;

  expect(evidence.fixture_provenance).toEqual(pinnedFixtureProvenance);
  expect(trackedProvenance).toMatchObject(pinnedFixtureProvenance);
  expect(JSON.stringify(evidence)).not.toContain('X-Amz-Signature');
  expect(JSON.stringify(evidence)).not.toContain('secret');
});

test('public S3 live evidence requires finite nonnegative cache, readahead, and IPC metrics', () => {
  const requiredComparisonMetricKeys = [
    'range_cache_hits',
    'range_cache_misses',
    'range_cache_bytes_reused',
    'range_cache_bytes_stored',
    'range_cache_validation_misses',
    'range_cache_degraded_identity_reads',
    'range_readahead_requests',
    'range_readahead_bytes_fetched',
    'range_readahead_bytes_used',
    'range_readahead_wasted_bytes',
    'arrow_ipc_bytes',
  ] as const satisfies readonly RequiredLiveMetricKey[];

  for (const key of requiredComparisonMetricKeys) {
    const missing = completeLiveMetrics();
    delete missing[key];
    expect(() => buildEvidenceWithMetrics(missing)).toThrow(key);

    for (const invalidValue of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() =>
        buildEvidenceWithMetrics({ ...completeLiveMetrics(), [key]: invalidValue }),
      ).toThrow(key);
    }
  }
});

test.describe('public S3 live smoke', () => {
  test.skip(
    !liveTableUri || !liveRegion,
    'set AXON_LIVE_PUBLIC_S3_TABLE_URI and AXON_LIVE_PUBLIC_S3_REGION to run live public S3 smoke',
  );

  test('public S3 Delta table root supports anonymous list, log read, and range read', async ({
    request,
  }) => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 's3',
      tableUri: liveTableUri!,
      region: liveRegion!,
    });
    const manifest = await buildPublicDeltaLogManifest(root);
    expect(manifest.objects.length).toBeGreaterThan(0);

    const addPath = await firstAddPathFromDeltaLogs(request, manifest.objects, liveOrigin);
    const dataResponse = await request.get(publicObjectUrl(root, addPath), {
      headers: {
        Origin: liveOrigin,
        Range: 'bytes=0-15',
      },
    });
    expect(dataResponse.status()).toBe(206);
    expect(dataResponse.headers()['content-range']).toContain('bytes 0-15/');
    expectCorsAllowsOrigin(dataResponse.headers(), liveOrigin);
    expect(
      Buffer.from(await dataResponse.body())
        .subarray(0, 4)
        .toString('utf8'),
    ).toBe('PAR1');
  });

  test('app repeats a live public S3 query across fresh browser runtimes', async ({
    page,
    browserName,
    baseURL,
  }, testInfo) => {
    testInfo.setTimeout(240_000);
    const tableName = tableNameFromTableUri(liveTableUri!);
    const repeatedQuery = `SELECT COUNT(*) AS row_count FROM ${quoteSqlIdentifier(tableName)}`;
    const runtimeErrors = captureRuntimeErrors(page);
    const runs: Array<{ run: number; scalar_result: string; metrics: LiveMetricsInput }> = [];
    let expectedScalarResult: string | undefined;

    await installRangeReadMetricsCapture(page);
    await connectPublicS3Table(page);
    for (let run = 1; run <= 3; run += 1) {
      if (run > 1) await page.reload();

      await selectPersistedPublicTable(page, tableName);
      const runtimeErrorStart = runtimeErrors.length;
      await page.locator('.code-input').fill(repeatedQuery);
      await page.locator('.btn.primary', { hasText: 'Run' }).click();

      await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
        timeout: 60_000,
      });
      await expect(page.locator('table.grid')).toContainText('row_count');
      const scalarResult = (
        await page.locator('table.grid tbody tr').first().locator('td').last().innerText()
      ).trim();
      expect(scalarResult, `run ${run} returned a scalar COUNT(*) result`).not.toBe('');
      expectedScalarResult ??= scalarResult;
      expect(scalarResult, `run ${run} matched the first COUNT(*) result`).toBe(
        expectedScalarResult,
      );
      await expect(page.locator('.results')).not.toContainText(
        /(?:parquet|decode|worker).*(?:error|failed)|(?:error|failed).*(?:parquet|decode|worker)/i,
      );
      expect(
        runtimeErrors
          .slice(runtimeErrorStart)
          .filter((message) => /parquet|decode|worker/i.test(message)),
        `run ${run} emitted no Parquet, decode, or worker errors`,
      ).toEqual([]);

      runs.push({
        run,
        scalar_result: scalarResult,
        metrics: await latestCapturedRangeReadMetrics(page),
      });
    }

    const evidence = buildPublicS3RepeatEvidence({
      tableUri: liveTableUri!,
      tableName,
      browserName,
      baseURL: baseURL ?? liveOrigin,
      region: liveRegion!,
      runs,
    });
    const artifactPath = testInfo.outputPath('public-s3-repeat-query-evidence.json');
    await writeFile(artifactPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
    await testInfo.attach('public-s3-repeat-query-evidence', {
      path: artifactPath,
      contentType: 'application/json',
    });
  });

  test('performance fixture records cache and readahead comparison evidence', async ({
    page,
    browserName,
    baseURL,
  }, testInfo) => {
    test.skip(
      !redactTableUri(liveTableUri!).endsWith('/fixtures/s3-browser-perf/table'),
      'set AXON_LIVE_PUBLIC_S3_TABLE_URI to the s3-browser-perf fixture for performance evidence',
    );
    testInfo.setTimeout(240_000);
    const tableName = tableNameFromTableUri(liveTableUri!);

    await installRangeReadMetricsCapture(page);
    await connectPublicS3Table(page);
    await page.locator('.code-input').fill(`
SELECT event_id, event_ts, region, customer_id, amount, status
FROM ${quoteSqlIdentifier(tableName)}
WHERE amount > 100 AND status IN ('paid', 'shipped')
ORDER BY event_ts
LIMIT 1000
`);
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 90_000,
    });
    await expect(page.locator('table.grid')).toContainText('event_id');
    await expect(page.locator('table.grid')).toContainText('amount');

    const evidence = buildPublicS3PerformanceEvidence({
      tableUri: liveTableUri!,
      tableName,
      browserName,
      baseURL: baseURL ?? liveOrigin,
      region: liveRegion!,
      metrics: await latestCapturedRangeReadMetrics(page),
    });
    expect(evidence.metrics.bytes_fetched).toBeGreaterThan(0);
    expect(evidence.metrics.scan_data_range_reads).toBeGreaterThan(0);
    expect(evidence.metrics.rows_emitted).toBeGreaterThan(0);
    expect(evidence.fixture_provenance).toEqual(pinnedFixtureProvenance);
    const artifactPath = testInfo.outputPath('public-s3-live-uat-evidence.json');
    await writeFile(artifactPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
    await testInfo.attach('public-s3-live-uat-evidence', {
      path: artifactPath,
      contentType: 'application/json',
    });
  });
});

function captureRuntimeErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  return errors;
}

async function connectPublicS3Table(page: Page): Promise<void> {
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();
  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
  await configDialog.getByRole('button', { name: /AWS S3/ }).click();
  await configDialog
    .locator('.cc-input.mono.has-prefix')
    .fill(liveTableUri!.replace(/^s3:\/\//, ''));
  await configDialog.locator('select.cc-select').selectOption(liveRegion!);
  await configDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(configDialog).toContainText(/source check passed/i, { timeout: 60_000 });
  await configDialog.getByRole('button', { name: /Discover tables/ }).click();

  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  const recommended = reviewDialog.getByLabel('Use recommended organization');
  if (await recommended.isChecked()) await recommended.uncheck();
  await reviewDialog.getByLabel('Catalog alias').fill('live-public-s3');
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

  await expect(page.locator('.conn-pill')).toContainText('live-public-s3', {
    timeout: 30_000,
  });
}

async function selectPersistedPublicTable(page: Page, tableName: string): Promise<void> {
  await expect(page.locator('.conn-pill')).toContainText('live-public-s3', {
    timeout: 30_000,
  });
  await page.locator('.conn-pill').click();
  const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
  const activateTable = panel.getByRole('button', {
    name: `Activate live-public-s3 default ${tableName}`,
  });
  if (!(await activateTable.isVisible())) {
    await panel.getByRole('button', { name: 'Expand live-public-s3' }).click();
  }
  await expect(activateTable).toBeEnabled();
  await activateTable.click();
  await expect(page.locator('.queryref-bar .qref')).toContainText(tableName);
}

async function installRangeReadMetricsCapture(page: Page): Promise<void> {
  await page.addInitScript((captureKey) => {
    const scope = window as typeof window & Record<string, unknown>;
    const captured: unknown[] = [];
    Object.defineProperty(scope, captureKey, {
      value: captured,
      configurable: true,
    });

    const OriginalWorker = window.Worker;
    class InstrumentedWorker extends OriginalWorker {
      constructor(scriptURL: string | URL, options?: WorkerOptions) {
        super(scriptURL, options);
        this.addEventListener('message', (event: MessageEvent<unknown>) => {
          const data = event.data;
          if (
            data &&
            typeof data === 'object' &&
            'range_read_metrics' in data &&
            data.range_read_metrics &&
            typeof data.range_read_metrics === 'object'
          ) {
            captured.push(data.range_read_metrics);
          }
        });
      }
    }

    Object.defineProperty(window, 'Worker', {
      value: InstrumentedWorker,
      configurable: true,
      writable: true,
    });
  }, rangeReadMetricsCaptureKey);
}

async function latestCapturedRangeReadMetrics(page: Page): Promise<LiveMetricsInput> {
  await page.waitForFunction(
    (captureKey) => {
      const captured = (window as typeof window & Record<string, unknown>)[captureKey];
      return Array.isArray(captured) && captured.length > 0;
    },
    rangeReadMetricsCaptureKey,
    { timeout: 5_000 },
  );
  const metrics = await page.evaluate((captureKey) => {
    const captured = (window as typeof window & Record<string, unknown>)[captureKey];
    if (!Array.isArray(captured)) return null;
    return captured.at(-1) ?? null;
  }, rangeReadMetricsCaptureKey);
  expect(metrics, 'browser worker emitted range-read metrics for the live query').toBeTruthy();
  return metrics as LiveMetricsInput;
}

function buildPublicS3PerformanceEvidence(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
  metrics: LiveMetricsInput;
}): PublicS3PerformanceEvidence {
  const metrics = projectLiveMetrics(input.metrics);
  const currentComparison = Object.fromEntries(
    comparisonMetricKeys.map((key) => [key, metrics[key]]),
  ) as Record<ComparisonMetricKey, number>;
  return {
    ...buildEvidenceBase(input),
    metrics,
    comparison: {
      pre_cache: preCacheComparison,
      current: currentComparison,
    },
  };
}

function buildPublicS3RepeatEvidence(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
  runs: Array<{ run: number; scalar_result: string; metrics: LiveMetricsInput }>;
}): PublicS3RepeatEvidence {
  return {
    ...buildEvidenceBase(input),
    repeat_count: input.runs.length,
    runs: input.runs.map((run) => ({
      run: run.run,
      scalar_result: run.scalar_result,
      metrics: projectLiveMetrics(run.metrics),
    })),
  };
}

function buildEvidenceBase(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  region: string;
}): PublicS3EvidenceBase {
  const tableUri = redactTableUri(input.tableUri);
  const fixtureProvenance =
    tableUri === pinnedFixtureProvenance.table_uri &&
    input.region === pinnedFixtureProvenance.region
      ? pinnedFixtureProvenance
      : undefined;
  return {
    table_uri: tableUri,
    table_name: input.tableName,
    browser_name: input.browserName,
    base_url: input.baseURL,
    region: input.region,
    ...(fixtureProvenance === undefined ? {} : { fixture_provenance: fixtureProvenance }),
  };
}

function projectLiveMetrics(metrics: LiveMetricsInput): PublicS3LiveRunEvidence['metrics'] {
  const projected = Object.fromEntries(
    requiredLiveMetricKeys.map((key) => [key, requiredMetric(metrics, key)]),
  ) as PublicS3LiveRunEvidence['metrics'];
  for (const key of optionalLiveMetricKeys) {
    const value = metrics[key];
    if (typeof value === 'number') projected[key] = value;
  }
  return projected;
}

function requiredMetric(metrics: LiveMetricsInput, key: RequiredLiveMetricKey): number {
  const value = metrics[key];
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new Error(`live public S3 evidence requires finite nonnegative numeric metric '${key}'`);
  }
  return value;
}

function completeLiveMetrics(): LiveMetricsInput {
  return Object.fromEntries(requiredLiveMetricKeys.map((key) => [key, 0])) as LiveMetricsInput;
}

function buildEvidenceWithMetrics(metrics: LiveMetricsInput): PublicS3PerformanceEvidence {
  return buildPublicS3PerformanceEvidence({
    tableUri: 's3://live-bucket/customer/path/table',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
    region: 'us-east-2',
    metrics,
  });
}

function redactTableUri(tableUri: string): string {
  try {
    const url = new URL(tableUri);
    url.username = '';
    url.password = '';
    url.search = '';
    url.hash = '';
    return url.toString();
  } catch {
    return tableUri.split(/[?#]/, 1)[0];
  }
}

function tableNameFromTableUri(tableUri: string): string {
  try {
    const url = new URL(redactTableUri(tableUri));
    return url.pathname.split('/').filter(Boolean).at(-1) ?? 'public_table';
  } catch {
    return redactTableUri(tableUri).split('/').filter(Boolean).at(-1) ?? 'public_table';
  }
}

async function firstAddPathFromDeltaLogs(
  request: APIRequestContext,
  objects: Array<{ relative_path: string; url: string }>,
  origin: string,
): Promise<string> {
  const jsonLogs = objects.filter((object) => object.relative_path.endsWith('.json'));
  expect(
    jsonLogs.length,
    'live table must expose at least one JSON Delta log object',
  ).toBeGreaterThan(0);

  for (const logObject of jsonLogs) {
    const logResponse = await request.get(logObject.url, {
      headers: { Origin: origin },
    });
    expect(logResponse.status()).toBe(200);
    expectCorsAllowsOrigin(logResponse.headers(), origin);
    const addPath = addPathFromDeltaLog(await logResponse.text());
    if (addPath) return addPath;
  }

  throw new Error('Delta log objects did not contain an add action');
}

function addPathFromDeltaLog(log: string): string | undefined {
  for (const line of log.split('\n')) {
    if (!line.trim()) continue;
    const action = JSON.parse(line) as { add?: { path?: unknown } };
    if (typeof action.add?.path === 'string') return action.add.path;
  }
  return undefined;
}

function expectCorsAllowsOrigin(headers: Record<string, string>, origin: string): void {
  expect([origin, '*']).toContain(headers['access-control-allow-origin']);
}

function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
