import { writeFile } from 'node:fs/promises';

import { expect, test, type Page } from '@playwright/test';

import {
  parsePublicDeltaLogIndexV1,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const endpoint = process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT;
const onboardingTableUri = process.env.AXON_LIVE_PUBLIC_R2_ONBOARDING_TABLE_URI;
const performanceTableUri = process.env.AXON_LIVE_PUBLIC_R2_PERF_TABLE_URI;
const daxisCommit = process.env.AXON_LIVE_PUBLIC_R2_DAXIS_COMMIT;
const runtimeCommit = process.env.AXON_LIVE_PUBLIC_R2_RUNTIME_COMMIT;
const browserOrigin = new URL(
  process.env.AXON_LIVE_PUBLIC_R2_ORIGIN ??
    process.env.PLAYWRIGHT_BASE_URL ??
    'https://127.0.0.1:5173',
).origin;
const captureKey = '__AXON_PUBLIC_R2_QUERY_EVIDENCE__';

type QueryEvidence = {
  metrics: Record<string, number>;
  owned_memory: Record<string, unknown>;
  execution: {
    executed_on: string;
    fallback_event_observed: boolean;
    response_fallback_reason: unknown;
  };
};

test.describe('public R2 live qualification', () => {
  test.skip(
    !endpoint || !onboardingTableUri || !performanceTableUri,
    'set the public R2 endpoint and both pinned table URIs to run live qualification',
  );

  test('proves CORS, strong validators, range semantics, and the well-known index', async ({
    request,
  }) => {
    const root = parsePublicObjectStorageTableRoot({
      provider: 'r2',
      tableUri: onboardingTableUri!,
      endpoint,
    });
    const indexResponse = await request.get(
      publicObjectUrl(root, '_axon/public-delta-log-index.json'),
      { headers: { Origin: browserOrigin } },
    );
    expect(indexResponse.status()).toBe(200);
    expectCors(indexResponse.headers());
    const objects = parsePublicDeltaLogIndexV1(await indexResponse.json(), root);
    expect(objects.length).toBeGreaterThan(0);

    const logObject = objects.find((object) => object.relative_path.endsWith('.json'))!;
    const logResponse = await request.get(logObject.url, {
      headers: { Origin: browserOrigin },
    });
    expect(logResponse.status()).toBe(200);
    expectCors(logResponse.headers());

    const provenanceResponse = await request.get(
      publicObjectUrl(root, '_axon/fixture-provenance.json'),
      { headers: { Origin: browserOrigin } },
    );
    expect(provenanceResponse.status()).toBe(200);
    const provenance = (await provenanceResponse.json()) as {
      objects: Array<{ relative_path: string; size_bytes: number }>;
    };
    const dataObject = provenance.objects.find(
      (object) =>
        object.relative_path.endsWith('.parquet') &&
        !object.relative_path.startsWith('_delta_log/'),
    )!;
    const dataUrl = publicObjectUrl(root, dataObject.relative_path);
    const head = await request.head(dataUrl, { headers: { Origin: browserOrigin } });
    expect(head.status()).toBe(200);
    expectCors(head.headers());
    expect(Number(head.headers()['content-length'])).toBe(dataObject.size_bytes);
    expect(head.headers()['accept-ranges']).toBe('bytes');
    const etag = head.headers().etag;
    expect(etag).toMatch(/^".+"$/);

    const range = await request.get(dataUrl, {
      headers: { Origin: browserOrigin, Range: 'bytes=0-15' },
    });
    expect(range.status()).toBe(206);
    expect(range.headers()['content-range']).toBe(`bytes 0-15/${dataObject.size_bytes}`);
    expect(
      Buffer.from(await range.body())
        .subarray(0, 4)
        .toString('utf8'),
    ).toBe('PAR1');

    const ifRange = await request.get(dataUrl, {
      headers: { Origin: browserOrigin, Range: 'bytes=0-15', 'If-Range': etag! },
    });
    expect(ifRange.status()).toBe(206);
    expect(ifRange.headers().etag).toBe(etag);

    const unsatisfied = await request.get(dataUrl, {
      headers: { Origin: browserOrigin, Range: `bytes=${dataObject.size_bytes}-` },
    });
    expect(unsatisfied.status()).toBe(416);
    expect(unsatisfied.headers()['content-range']).toBe(`bytes */${dataObject.size_bytes}`);
  });

  test('queries the exact onboarding snapshot in browser WASM', async ({ page }) => {
    await connectPublicR2Table(page, onboardingTableUri!, 'live-r2-onboarding');
    const tableName = tableNameFromUri(onboardingTableUri!);
    await runScalarQuery(page, tableName, `SELECT COUNT(*) AS row_count FROM "${tableName}"`, '4');

    const persisted = JSON.parse(
      await page.evaluate(() => localStorage.getItem('axon.connect.catalogs.v1') ?? '[]'),
    ) as Array<{ schemas?: Array<{ tables?: Array<Record<string, unknown>> }> }>;
    expect(persisted[0]?.schemas?.[0]?.tables?.[0]).toMatchObject({
      snapshot: 3,
      rows: 4,
      files: 2,
    });
  });

  test('runs three fresh-runtime counts and the performance query without fallback', async ({
    page,
    browser,
    browserName,
  }, testInfo) => {
    testInfo.setTimeout(300_000);
    await installEvidenceCapture(page);
    const runtimeErrors = captureRuntimeErrors(page);
    const tableName = tableNameFromUri(performanceTableUri!);
    await connectPublicR2Table(page, performanceTableUri!, 'live-r2-performance');
    const runs: Array<{ run: number; scalar_result: string; evidence: QueryEvidence }> = [];

    for (let run = 1; run <= 3; run += 1) {
      if (run > 1) await page.reload();
      await selectPersistedTable(page, 'live-r2-performance', tableName);
      const scalar = await runScalarQuery(
        page,
        tableName,
        `SELECT COUNT(*) AS row_count FROM "${tableName}"`,
        '1048576',
      );
      const evidence = await latestEvidence(page);
      assertSuccessfulBrowserEvidence(evidence);
      runs.push({ run, scalar_result: scalar, evidence });
    }

    await page.locator('.code-input').fill(`
SELECT event_id, event_ts, region, customer_id, amount, status
FROM "${tableName}"
WHERE amount > 100 AND status IN ('paid', 'shipped')
ORDER BY event_ts
LIMIT 1000
`);
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 90_000 });
    await expect(page.locator('table.grid')).toContainText('event_id');
    const filteredEvidence = await latestEvidence(page);
    assertSuccessfulBrowserEvidence(filteredEvidence);
    expect(runtimeErrors.filter((message) => /parquet|decode|worker/i.test(message))).toEqual([]);

    const artifact = {
      schema_version: 1,
      endpoint_class: endpoint!.endsWith('.r2.dev') ? 'r2.dev' : 'custom-domain',
      endpoint_origin: new URL(endpoint!).origin,
      bucket: new URL(performanceTableUri!.replace('r2://', 'https://')).hostname,
      daxis_commit: daxisCommit ?? 'UNSET',
      axon_runtime_commit: runtimeCommit ?? 'UNSET',
      fixture_revision: 's3-browser-perf-v1',
      browser_name: browserName,
      browser_version: browser.version(),
      runs,
      filtered_query: filteredEvidence,
    };
    const serialized = JSON.stringify(artifact, null, 2);
    expect(serialized).not.toMatch(/x-amz-(?:credential|signature|security-token)|bearer/i);
    const artifactPath = testInfo.outputPath('public-r2-live-qualification.json');
    await writeFile(artifactPath, `${serialized}\n`, 'utf8');
    await testInfo.attach('public-r2-live-qualification', {
      path: artifactPath,
      contentType: 'application/json',
    });
  });
});

function expectCors(headers: Record<string, string>): void {
  expect(headers['access-control-allow-origin']).toBe(browserOrigin);
}

function tableNameFromUri(uri: string): string {
  return uri.split('/').filter(Boolean).at(-1) ?? 'table';
}

async function connectPublicR2Table(page: Page, tableUri: string, alias: string): Promise<void> {
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();
  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();
  const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
  await configDialog.getByRole('button', { name: /Cloudflare R2/ }).click();
  await configDialog.locator('.cc-input.mono.has-prefix').fill(tableUri.replace(/^r2:\/\//, ''));
  await configDialog.getByLabel(/Public R2 HTTPS endpoint/).fill(endpoint!);
  await configDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(configDialog).toContainText(/source check passed/i, { timeout: 90_000 });
  await configDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  const recommended = reviewDialog.getByLabel('Use recommended organization');
  if (await recommended.isChecked()) await recommended.uncheck();
  await reviewDialog.getByLabel('Catalog alias').fill(alias);
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();
  await expect(page.locator('.conn-pill')).toContainText(alias, { timeout: 30_000 });
}

async function selectPersistedTable(page: Page, alias: string, tableName: string): Promise<void> {
  await expect(page.locator('.conn-pill')).toContainText(alias, { timeout: 30_000 });
  await page.locator('.conn-pill').click();
  const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
  const activate = panel.getByRole('button', { name: `Activate ${alias} default ${tableName}` });
  if (!(await activate.isVisible()))
    await panel.getByRole('button', { name: `Expand ${alias}` }).click();
  await activate.click();
}

async function runScalarQuery(
  page: Page,
  tableName: string,
  sql: string,
  expected: string,
): Promise<string> {
  await selectPersistedTableIfNeeded(page, tableName);
  await page.locator('.code-input').fill(sql);
  await page.locator('.btn.primary', { hasText: 'Run' }).click();
  await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 90_000 });
  const value = (await page.locator('table.grid tbody tr td').last().innerText()).trim();
  expect(value).toBe(expected);
  return value;
}

async function selectPersistedTableIfNeeded(page: Page, tableName: string): Promise<void> {
  if (await page.locator('.queryref-bar .qref', { hasText: tableName }).isVisible()) return;
  await expect(page.locator('.queryref-bar .qref')).toContainText(tableName);
}

function captureRuntimeErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text());
  });
  return errors;
}

async function installEvidenceCapture(page: Page): Promise<void> {
  await page.addInitScript((key) => {
    const captured: Array<Record<string, unknown>> = [];
    Object.defineProperty(window, key, { value: captured, configurable: true });
    const OriginalWorker = window.Worker;
    class InstrumentedWorker extends OriginalWorker {
      constructor(url: string | URL, options?: WorkerOptions) {
        super(url, options);
        this.addEventListener('message', (event: MessageEvent<Record<string, unknown>>) => {
          const data = event.data;
          if (!data || typeof data !== 'object') return;
          for (const field of ['range_read_metrics', 'owned_memory_metrics'] as const) {
            if (data[field] && typeof data[field] === 'object') {
              captured.push({ kind: field, value: data[field] });
            }
          }
          if (data.fallback && typeof data.fallback === 'object') {
            const fallback = data.fallback as Record<string, unknown>;
            const context = fallback.context as Record<string, unknown> | undefined;
            captured.push({ kind: 'fallback', request_id: context?.request_id });
          }
          if (data.success && typeof data.success === 'object') {
            const success = data.success as Record<string, unknown>;
            const response = success.response as Record<string, unknown> | undefined;
            captured.push({
              kind: 'success',
              request_id: success.request_id,
              executed_on: response?.executed_on,
              response_fallback_reason: response?.fallback_reason ?? null,
            });
          }
        });
      }
    }
    Object.defineProperty(window, 'Worker', { value: InstrumentedWorker, configurable: true });
  }, captureKey);
}

async function latestEvidence(page: Page): Promise<QueryEvidence> {
  await page.waitForFunction(
    (key) => {
      const records = (window as typeof window & Record<string, unknown>)[key];
      return Array.isArray(records) && records.some((record) => record.kind === 'success');
    },
    captureKey,
    { timeout: 10_000 },
  );
  const records = (await page.evaluate(
    (key) => (window as typeof window & Record<string, unknown>)[key],
    captureKey,
  )) as Array<Record<string, unknown>>;
  const success = records.filter((record) => record.kind === 'success').at(-1)!;
  const requestId = success.request_id;
  const forRequest = (record: Record<string, unknown>) => {
    const value = record.value as Record<string, unknown> | undefined;
    const context = value?.context as Record<string, unknown> | undefined;
    return context?.request_id === requestId;
  };
  const metrics = records
    .filter((record) => record.kind === 'range_read_metrics' && forRequest(record))
    .at(-1)?.value as Record<string, number>;
  const ownedMemory = records
    .filter((record) => record.kind === 'owned_memory_metrics' && forRequest(record))
    .at(-1)?.value as Record<string, unknown>;
  expect(metrics).toBeTruthy();
  expect(ownedMemory).toBeTruthy();
  return {
    metrics,
    owned_memory: ownedMemory,
    execution: {
      executed_on: String(success.executed_on),
      fallback_event_observed: records.some(
        (record) => record.kind === 'fallback' && record.request_id === requestId,
      ),
      response_fallback_reason: success.response_fallback_reason,
    },
  };
}

function assertSuccessfulBrowserEvidence(evidence: QueryEvidence): void {
  expect(evidence.execution).toEqual({
    executed_on: 'browser_wasm',
    fallback_event_observed: false,
    response_fallback_reason: null,
  });
  expect(evidence.metrics.bytes_fetched).toBeGreaterThan(0);
  expect(evidence.metrics.scan_data_range_reads).toBeGreaterThan(0);
  expect(evidence.metrics.rows_emitted).toBeGreaterThan(0);
  expect(evidence.metrics.arrow_ipc_bytes).toBeGreaterThan(0);
  expect(evidence.metrics.coordinator_peak_staged_bytes).toBeLessThanOrEqual(
    evidence.metrics.coordinator_staging_limit_bytes,
  );
  expect(evidence.metrics.cursor_peak_pending_encoded_bytes).toBeLessThanOrEqual(8 * 1024 * 1024);
  expect(evidence.metrics.cursor_peak_transport_chunk_bytes).toBeLessThanOrEqual(1024 * 1024);
}
