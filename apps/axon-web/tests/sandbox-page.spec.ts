import { expect, test } from '@playwright/test';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

test('executes SQL from a fresh editor without a manual snapshot step', async ({ page }) => {
  await page.goto('/sandbox.html');

  await expect(page.getByRole('heading', { name: 'Axon SQL' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Resolve Snapshot' })).toHaveCount(0);
  await expect(page.getByLabel('Data source')).toHaveValue('prod-like');
  await expect(page.getByTestId('query-status')).toHaveText('Ready');

  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished');
  await expect(page.getByTestId('status')).toHaveText(
    'Snapshot 3 resolved from checkpoint 2 + 1 replay commit',
  );
  await expect(page.getByTestId('query-executed-on')).toHaveText('browser_wasm');
  await expect(page.getByTestId('result-grid')).toContainText('row_count');
  await expect(page.getByTestId('result-grid')).toContainText('4');
});

test('maps a prod-like Delta fixture from log inputs to resolved active output', async ({
  page,
}) => {
  const parquetRangeHeaders: string[] = [];
  page.on('request', (request) => {
    const url = request.url();
    if (!url.includes('/fixtures/prod-like/table/') || !url.endsWith('.parquet')) {
      return;
    }

    const range = request.headers().range;
    if (range) {
      parquetRangeHeaders.push(range);
    }
  });

  await page.goto('/sandbox.html');

  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('status')).toHaveText(
    'Snapshot 3 resolved from checkpoint 2 + 1 replay commit',
  );
  await expect(page.getByTestId('fixture-name')).toContainText('Prod-like generated Delta table');
  await expect(page.getByTestId('snapshot-version')).toHaveText('3');
  await expect(page.getByTestId('checkpoint-version')).toHaveText('2');
  await expect(page.getByTestId('log-objects')).toContainText(
    '00000000000000000002.checkpoint.parquet',
  );
  await expect(page.getByTestId('log-objects')).toContainText('_last_checkpoint');
  await expect(page.getByTestId('commit-actions')).toContainText('remove');
  await expect(page.getByTestId('data-files')).toContainText('inactive');
  await expect(page.getByTestId('active-files')).toContainText('category=B');
  await expect(page.getByTestId('active-files')).toContainText('category=D');
  await expect(page.getByTestId('active-data-file-urls')).toContainText(
    '/fixtures/prod-like/table/category=B/',
  );
  await expect(page.getByTestId('active-data-file-urls')).toContainText(
    '/fixtures/prod-like/table/category=D/',
  );
  await expect(page.getByTestId('active-data-file-urls')).not.toContainText('category=A');
  await expect(page.getByTestId('active-data-file-urls')).not.toContainText('category=C');
  await expect(page.getByTestId('pruning-preflight')).toContainText('value >= 90');
  await expect(page.getByTestId('pruning-preflight')).toContainText('files_touched 1');
  await expect(page.getByTestId('pruning-preflight')).toContainText('files_skipped 1');
  await expect(page.getByTestId('pruning-preflight')).toContainText('category=B');
  await expect(page.getByTestId('pruning-preflight')).toContainText('skipped');
  await expect(page.getByTestId('pruning-preflight')).toContainText('category=D');
  await expect(page.getByTestId('parquet-preflight')).toContainText('category=B');
  await expect(page.getByTestId('parquet-preflight')).toContainText('category=D');
  await expect(page.getByTestId('parquet-preflight')).toContainText('rows 2');
  await expect(page.getByTestId('parquet-preflight')).toContainText('row groups 1');
  await expect(page.getByTestId('parquet-preflight')).toContainText('id: Int32 required');
  await expect(page.getByTestId('parquet-preflight')).toContainText('value: Int32 required');
  await expect(page.getByTestId('parquet-preflight')).toContainText('parquet stats id min 7');
  await expect(page.getByTestId('parquet-preflight')).toContainText('delta stats 2 rows');
  await expect(page.getByTestId('parquet-preflight')).toContainText('metadata memory');
  await expect(page.getByTestId('parquet-preflight')).toContainText('indexes column');
  await expect(page.getByTestId('parquet-preflight')).toContainText('compressed');
  await expect(page.getByTestId('parquet-preflight')).not.toContainText('category=A');
  await expect(page.getByTestId('parquet-preflight')).not.toContainText('category=C');
  await expect(page.getByTestId('input-output-map')).toContainText('checkpoint seed');
  await expect(page.getByTestId('input-output-map')).toContainText('replay commit 3');
  expect(parquetRangeHeaders).toEqual(expect.arrayContaining([expect.stringMatching(/^bytes=/)]));
});

test('runs prod-like SQL through the sandbox worker and renders query telemetry', async ({
  page,
}) => {
  await page.goto('/sandbox.html');

  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished');
  await expect(page.getByTestId('query-executed-on')).toHaveText('browser_wasm');
  await expect(page.getByTestId('query-fallback-reason')).toHaveCount(0);
  await expect(page.getByText(/fallback/i)).toHaveCount(0);
  await expect(page.getByTestId('query-row-count')).toHaveText('1');
  await expect(page.getByTestId('query-arrow-ipc-bytes')).toHaveText(/\d+ bytes/);
  await expect(page.getByTestId('query-metrics')).toContainText('files_touched');
  await expect(page.getByTestId('result-grid')).toContainText('row_count');
  await expect(page.getByTestId('result-grid')).toContainText('4');
  await expect(page.getByTestId('worker-event-log')).toContainText('open started');
  await expect(page.getByTestId('worker-event-log')).toContainText('query arrow_ipc_ready');
  await expect(page.getByTestId('worker-event-log')).toContainText('range_read_metrics');
  await expect(page.getByTestId('worker-event-log')).toContainText('cache_metrics');
  const workerEvents = await page.getByTestId('worker-event-log').locator('li').allTextContents();
  expect(workerEvents.filter((event) => event.startsWith('range_read_metrics'))).toHaveLength(1);

  await page.getByTestId('sql-editor').fill('DELETE FROM axon_table');
  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Error');
  await expect(page.getByTestId('query-error')).toContainText('invalid_request');
  await expect(page.getByTestId('query-error')).toContainText('browser_wasm');
  await expect(page.getByTestId('query-error')).not.toContainText('?');
  await expect(page.getByTestId('query-error')).not.toContainText('#');
  await expect(page.getByTestId('worker-event-log')).toContainText('terminal_error');
});

test('opens object-store tables through browser-local snapshot reconstruction without services', async ({
  page,
}) => {
  await page.goto('/sandbox.html');

  await page.getByLabel('Data source').selectOption('object-store');

  await expect(page.getByTestId('object-store-controls')).toBeVisible();
  await expect(page.getByLabel('Object store provider')).toHaveValue('s3');
  await expect(page.getByLabel('Object store table URI')).toHaveValue(
    's3://axon-fixtures/prod-like-events',
  );
  await expect(
    page.getByText(
      /secret key|access key|SAS|bearer token|service-account JSON|trusted resolver|BFF|service/i,
    ),
  ).toHaveCount(0);

  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished');
  await expect(page.getByTestId('table-uri')).toHaveText('s3://axon-fixtures/prod-like-events');
  await expect(page.getByTestId('browser-snapshot-provider')).toHaveText('s3');
  await expect(page.getByTestId('browser-snapshot-mode')).toHaveText('browser_wasm');
  await expect(page.getByTestId('browser-snapshot-uri')).toHaveText(
    's3://axon-fixtures/prod-like-events',
  );
  await expect(page.getByTestId('query-executed-on')).toHaveText('browser_wasm');
  await expect(page.getByTestId('result-grid')).toContainText('row_count');
  await expect(page.getByTestId('worker-event-log')).toContainText('open started');
  await expect(page.getByTestId('active-data-file-urls')).toContainText(
    '/fixtures/prod-like/table/category=B/',
  );
  await expect(page.getByText(/resolver|BFF|service/i)).toHaveCount(0);
});

test('loads a local Delta table directory and queries it in browser', async ({ page }) => {
  const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));

  await page.goto('/sandbox.html');

  await page.getByLabel('Data source').selectOption('local-files');
  await expect(page.getByTestId('local-table-controls')).toBeVisible();
  await page.getByLabel('Local Delta table directory').setInputFiles(tableDir);
  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished', { timeout: 30_000 });
  await expect(page.getByTestId('fixture-name')).toHaveText('Local Delta table');
  await expect(page.getByTestId('table-uri')).toContainText('browser-local://');
  await expect(page.getByTestId('browser-snapshot-provider')).toHaveText('local_files');
  await expect(page.getByTestId('browser-snapshot-mode')).toHaveText('browser_wasm');
  await expect(page.getByTestId('active-data-file-urls')).toContainText('blob:');
  await expect(page.getByTestId('active-data-file-urls')).toContainText('category=B');
  await expect(page.getByTestId('active-data-file-urls')).toContainText('category=D');
  await expect(page.getByTestId('query-executed-on')).toHaveText('browser_wasm');
  await expect(page.getByTestId('result-grid')).toContainText('row_count');
  await expect(page.getByTestId('result-grid')).toContainText('4');
  await expect(page.getByText(/resolver|BFF|service/i)).toHaveCount(0);
});

test('reopens an imported local Delta table after reload without reselecting files', async ({
  page,
}) => {
  const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));

  await page.goto('/sandbox.html');

  await page.getByLabel('Data source').selectOption('local-files');
  await page.getByLabel('Local Delta table directory').setInputFiles(tableDir);
  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.getByTestId('query-status')).toHaveText('Finished', { timeout: 30_000 });

  await page.reload();
  await page.getByLabel('Data source').selectOption('local-files');
  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished', { timeout: 30_000 });
  await expect(page.getByTestId('fixture-name')).toHaveText('Local Delta table');
  await expect(page.getByTestId('table-uri')).toContainText('browser-local://');
  await expect(page.getByTestId('browser-snapshot-provider')).toHaveText('local_files');
  await expect(page.getByTestId('result-grid')).toContainText('row_count');
  await expect(page.getByTestId('result-grid')).toContainText('4');
});

test('sandbox object-store source opens browser-built descriptors directly', () => {
  const source = readFileSync(new URL('../src/main.ts', import.meta.url), 'utf8');

  expect(source).not.toContain('mockResolveDeltaSnapshotDescriptor');
  expect(source).not.toContain('openDeltaLocation');
  expect(source).toContain('resolve_delta_snapshot_from_manifest');
  expect(source).toContain('openDeltaTable');
});

test('local Delta registry persists OPFS records without byte-copying every file', () => {
  const source = readFileSync(new URL('../src/main.ts', import.meta.url), 'utf8');

  expect(source).toContain('localDeltaMetadataRecords(table)');
  expect(source).toContain('localDeltaIndexedDbBlobRecords(table)');
  expect(source).not.toMatch(
    /const recordFiles = await Promise\.all\([\s\S]*bytes: await entry\.file\.arrayBuffer\(\)[\s\S]*if \(await tryWriteLocalDeltaTableToOpfs/,
  );
});

test('local Delta reload uses the active local table registry id', () => {
  const source = readFileSync(new URL('../src/main.ts', import.meta.url), 'utf8');

  expect(source).toContain('LOCAL_DELTA_ACTIVE_ID_KEY');
  expect(source).toContain('setActiveLocalDeltaRegistryId(id)');
  expect(source).toContain('loadActiveLocalDeltaTable()');
  expect(source).not.toContain('loadLatestLocalDeltaTable');
});

test('records honest UI supersession when cancelling a running sandbox query', async ({ page }) => {
  await page.goto('/sandbox.html');

  let releaseFirstQueryRead: (() => void) | undefined;
  const firstQueryReadRelease = new Promise<void>((resolve) => {
    releaseFirstQueryRead = resolve;
  });

  await page.route('**/fixtures/prod-like/table/**/*.parquet', async (route) => {
    if (route.request().url().includes('/_delta_log/')) {
      await route.continue();
      return;
    }
    await firstQueryReadRelease;
    await route.continue();
  });

  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.getByTestId('worker-event-log')).toContainText('instantiate finished');
  await page.getByRole('button', { name: 'Cancel' }).click();
  releaseFirstQueryRead?.();

  await expect(page.getByTestId('query-status')).toHaveText('Cancellation requested');
  await expect(page.getByTestId('query-fallback-reason')).toHaveCount(0);
  await expect(page.getByTestId('query-error')).not.toContainText(/fallback/i);
  await expect(page.getByTestId('worker-event-log')).not.toContainText(/fallback/i);
  await expect(page.getByTestId('worker-event-log')).toContainText('cancellation');
});

test('does not append stale worker events after cancelling and starting a new query', async ({
  page,
}) => {
  await page.goto('/sandbox.html');

  let delayedFirstQueryRead = false;
  let releaseFirstQueryRead: (() => void) | undefined;
  const firstQueryReadRelease = new Promise<void>((resolve) => {
    releaseFirstQueryRead = resolve;
  });

  await page.route('**/fixtures/prod-like/table/**/*.parquet', async (route) => {
    if (route.request().url().includes('/_delta_log/')) {
      await route.continue();
      return;
    }
    if (!delayedFirstQueryRead) {
      delayedFirstQueryRead = true;
      await firstQueryReadRelease;
    }
    await route.continue();
  });

  await page.getByRole('button', { name: 'Run' }).click();
  await expect(page.getByTestId('worker-event-log')).toContainText('instantiate finished');
  await page.getByRole('button', { name: 'Cancel' }).click();
  await page.getByRole('button', { name: 'Run' }).click();
  releaseFirstQueryRead?.();

  await expect(page.getByTestId('query-status')).toHaveText('Finished');
  const workerEvents = await page.getByTestId('worker-event-log').locator('li').allTextContents();
  expect(workerEvents.filter((event) => event === 'open finished')).toHaveLength(1);
  expect(workerEvents.filter((event) => event === 'query arrow_ipc_ready')).toHaveLength(1);
  expect(workerEvents.filter((event) => event === 'query finished')).toHaveLength(1);
});
