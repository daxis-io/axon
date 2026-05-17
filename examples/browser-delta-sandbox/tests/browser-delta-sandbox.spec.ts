import { expect, test } from '@playwright/test';

test('executes SQL from a fresh editor without a manual snapshot step', async ({ page }) => {
  await page.goto('/');

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

  await page.goto('/');

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
  await page.goto('/');

  await page.getByRole('button', { name: 'Run' }).click();

  await expect(page.getByTestId('query-status')).toHaveText('Finished');
  await expect(page.getByTestId('query-executed-on')).toHaveText('browser_wasm');
  await expect(page.getByTestId('query-fallback-reason')).toHaveText('-');
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

test('records honest UI supersession when cancelling a running sandbox query', async ({ page }) => {
  await page.goto('/');

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
  await expect(page.getByTestId('query-fallback-reason')).toHaveText('browser_runtime_constraint');
  await expect(page.getByTestId('worker-event-log')).toContainText('cancellation');
});

test('does not append stale worker events after cancelling and starting a new query', async ({
  page,
}) => {
  await page.goto('/');

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
