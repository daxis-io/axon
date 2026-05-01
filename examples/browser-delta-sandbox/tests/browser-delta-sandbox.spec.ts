import { expect, test } from '@playwright/test';

test('resolves a Delta snapshot from same-origin browser HTTP log objects', async ({ page }) => {
  await page.goto('/');

  await page.getByRole('button', { name: 'Resolve Snapshot' }).click();

  await expect(page.getByTestId('status')).toHaveText('Snapshot 1 resolved');
  await expect(page.getByTestId('active-files')).toContainText('data/b.parquet');
  await expect(page.getByTestId('active-files')).toContainText('category=B');
});

test('maps a prod-like Delta fixture from log inputs to resolved active output', async ({ page }) => {
  await page.goto('/');

  await page.getByRole('button', { name: 'Resolve Prod-like Snapshot' }).click();

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
  await expect(page.getByTestId('input-output-map')).toContainText('checkpoint seed');
  await expect(page.getByTestId('input-output-map')).toContainText('replay commit 3');
});
