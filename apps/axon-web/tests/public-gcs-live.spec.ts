import { writeFile } from 'node:fs/promises';

import { expect, test, type Page } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const liveTableUri = process.env.AXON_LIVE_PUBLIC_GCS_TABLE_URI;
const liveOrigin =
  process.env.AXON_LIVE_PUBLIC_GCS_ORIGIN ??
  new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173').origin;
const rangeReadMetricsCaptureKey = '__AXON_PUBLIC_GCS_RANGE_READ_METRICS__';
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
  'rows_emitted',
] as const;
const optionalLiveMetricKeys = ['arrow_ipc_bytes', 'arrow_ipc_chunk_count'] as const;

type RequiredLiveMetricKey = (typeof requiredLiveMetricKeys)[number];
type OptionalLiveMetricKey = (typeof optionalLiveMetricKeys)[number];
type LiveMetricsInput = Partial<Record<RequiredLiveMetricKey | OptionalLiveMetricKey, number>>;
type PublicGcsLiveEvidence = {
  table_uri: string;
  table_name: string;
  browser_name: string;
  base_url: string;
  metrics: Record<RequiredLiveMetricKey, number> & Partial<Record<OptionalLiveMetricKey, number>>;
};

test('public GCS live evidence artifact redacts table URI and preserves required metrics', () => {
  const evidence = buildPublicGcsLiveEvidence({
    tableUri: 'gs://live-bucket/customer/path/table?X-Goog-Signature=secret#frag',
    tableName: 'table',
    browserName: 'chromium',
    baseURL: 'https://127.0.0.1:5173',
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
      rows_emitted: 12,
      arrow_ipc_bytes: 13,
      arrow_ipc_chunk_count: 14,
    },
  });

  expect(evidence.table_uri).toBe('gs://live-bucket/customer/path/table');
  expect(evidence.table_name).toBe('table');
  expect(evidence.browser_name).toBe('chromium');
  expect(evidence.base_url).toBe('https://127.0.0.1:5173');
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
    rows_emitted: 12,
    arrow_ipc_bytes: 13,
    arrow_ipc_chunk_count: 14,
  });
});

test.describe('public GCS live smoke', () => {
  test.skip(!liveTableUri, 'set AXON_LIVE_PUBLIC_GCS_TABLE_URI to run live public GCS smoke');

  test('public GCS Delta table root supports anonymous list, log read, and range read', async ({
    request,
  }) => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'gcs',
      tableUri: liveTableUri!,
    });
    const manifest = await buildPublicDeltaLogManifest(root);
    expect(manifest.objects.length).toBeGreaterThan(0);

    const firstLog = manifest.objects.find((object) => object.relative_path.endsWith('.json'));
    expect(firstLog, 'live table must expose at least one JSON Delta log object').toBeTruthy();

    const logResponse = await request.get(firstLog!.url, {
      headers: { Origin: liveOrigin },
    });
    expect(logResponse.status()).toBe(200);
    expect(logResponse.headers()['access-control-allow-origin']).toBe(liveOrigin);

    const addPath = addPathFromDeltaLog(await logResponse.text());
    const dataResponse = await request.get(publicObjectUrl(root, addPath), {
      headers: {
        Origin: liveOrigin,
        Range: 'bytes=0-15',
      },
    });
    expect(dataResponse.status()).toBe(206);
    expect(dataResponse.headers()['content-range']).toContain('bytes 0-15/');
    expect(dataResponse.headers()['access-control-allow-origin']).toBe(liveOrigin);
    expect(
      Buffer.from(await dataResponse.body())
        .subarray(0, 4)
        .toString('utf8'),
    ).toBe('PAR1');
  });

  test('app connects and queries a live public GCS Delta table root in browser WASM', async ({
    page,
    browserName,
    baseURL,
  }, testInfo) => {
    const tableName = tableNameFromTableUri(liveTableUri!);

    await installRangeReadMetricsCapture(page);
    await page.goto('/');
    await page.getByRole('button', { name: /^Connect$/ }).click();
    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

    const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
    await configDialog
      .locator('.cc-input.mono.has-prefix')
      .fill(liveTableUri!.replace(/^gs:\/\//, ''));
    await configDialog.getByRole('button', { name: 'Test connection' }).click();
    await expect(configDialog).toContainText(/source check passed/i, { timeout: 60_000 });
    await configDialog.getByRole('button', { name: /Discover tables/ }).click();

    const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
    const recommended = reviewDialog.getByLabel('Use recommended organization');
    if (await recommended.isChecked()) await recommended.uncheck();
    await reviewDialog.getByLabel('Catalog alias').fill('live-public-gcs');
    await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

    await expect(page.locator('.conn-pill')).toContainText('live-public-gcs', {
      timeout: 30_000,
    });
    await page
      .locator('.code-input')
      .fill(`SELECT COUNT(*) AS row_count FROM ${quoteSqlIdentifier(tableName)}`);
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 60_000,
    });
    await expect(page.locator('table.grid')).toContainText('row_count');

    const evidence = buildPublicGcsLiveEvidence({
      tableUri: liveTableUri!,
      tableName,
      browserName,
      baseURL: baseURL ?? liveOrigin,
      metrics: await latestCapturedRangeReadMetrics(page),
    });
    const artifactPath = testInfo.outputPath('public-gcs-live-uat-evidence.json');
    await writeFile(artifactPath, `${JSON.stringify(evidence, null, 2)}\n`, 'utf8');
    await testInfo.attach('public-gcs-live-uat-evidence', {
      path: artifactPath,
      contentType: 'application/json',
    });
  });
});

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

function buildPublicGcsLiveEvidence(input: {
  tableUri: string;
  tableName: string;
  browserName: string;
  baseURL: string;
  metrics: LiveMetricsInput;
}): PublicGcsLiveEvidence {
  const metrics = Object.fromEntries(
    requiredLiveMetricKeys.map((key) => [key, requiredMetric(input.metrics, key)]),
  ) as PublicGcsLiveEvidence['metrics'];
  for (const key of optionalLiveMetricKeys) {
    const value = input.metrics[key];
    if (typeof value === 'number') metrics[key] = value;
  }
  return {
    table_uri: redactTableUri(input.tableUri),
    table_name: input.tableName,
    browser_name: input.browserName,
    base_url: input.baseURL,
    metrics,
  };
}

function requiredMetric(metrics: LiveMetricsInput, key: RequiredLiveMetricKey): number {
  const value = metrics[key];
  if (typeof value !== 'number') {
    throw new Error(`live public GCS evidence missing numeric metric '${key}'`);
  }
  return value;
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

function addPathFromDeltaLog(log: string): string {
  for (const line of log.split('\n')) {
    if (!line.trim()) continue;
    const action = JSON.parse(line) as { add?: { path?: unknown } };
    if (typeof action.add?.path === 'string') return action.add.path;
  }
  throw new Error('Delta log did not contain an add action');
}

function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
