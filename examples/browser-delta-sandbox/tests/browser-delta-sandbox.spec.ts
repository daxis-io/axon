import { expect, test } from '@playwright/test';

test('resolves a Delta snapshot from same-origin browser HTTP log objects', async ({ page }) => {
  await page.goto('/');

  await expect(page.getByRole('button', { name: 'Resolve Prod-like Snapshot' })).toHaveCount(0);
  await expect(page.getByRole('radio', { name: 'Simple snapshot' })).toBeChecked();
  await page.getByRole('button', { name: 'Resolve Snapshot' }).click();

  await expect(page.getByTestId('status')).toHaveText('Snapshot 1 resolved');
  await expect(page.getByTestId('active-files')).toContainText('data/b.parquet');
  await expect(page.getByTestId('active-files')).toContainText('category=B');
});

test('maps a prod-like Delta fixture from log inputs to resolved active output', async ({ page }) => {
  await page.goto('/');

  await page.getByRole('radio', { name: 'Prod-like snapshot' }).check();
  await page.getByRole('button', { name: 'Resolve Snapshot' }).click();

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
  await expect(page.getByTestId('parquet-preflight')).not.toContainText('category=A');
  await expect(page.getByTestId('parquet-preflight')).not.toContainText('category=C');
  await expect(page.getByTestId('input-output-map')).toContainText('checkpoint seed');
  await expect(page.getByTestId('input-output-map')).toContainText('replay commit 3');
});
