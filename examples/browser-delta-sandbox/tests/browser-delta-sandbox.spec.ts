import { expect, test } from '@playwright/test';

test('resolves a Delta snapshot from same-origin browser HTTP log objects', async ({ page }) => {
  await page.goto('/');

  await page.getByRole('button', { name: 'Resolve Snapshot' }).click();

  await expect(page.getByTestId('status')).toHaveText('Snapshot 1 resolved');
  await expect(page.getByTestId('active-files')).toContainText('data/b.parquet');
  await expect(page.getByTestId('active-files')).toContainText('category=B');
});
