import { expect, test } from '@playwright/test';

const APP_ORIGIN = new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5174').origin;

// Phase 1 smoke test: editor mounts, catalog populates, and a query returns rows.
// Lives under tests/ so it benefits from the same baseURL config as the sandbox
// suite, but is opt-in via grep so existing CI continues to target the sandbox.

test.describe('editor (Phase 1 smoke)', () => {
  test('loads, populates catalog, runs a query', async ({ page, context }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });
    page.on('pageerror', (err) => consoleErrors.push(err.message));
    await context.grantPermissions(['clipboard-read', 'clipboard-write'], {
      origin: APP_ORIGIN,
    });

    await page.goto('/');

    // Shell mounts.
    await expect(page.locator('.shell .brand-name')).toContainText('axon');
    await expect(page.getByText(/fallback/i)).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Native' })).toHaveCount(0);

    // Catalog resolves and lists axon_table (data comes from the prod-like fixture).
    await expect(page.locator('.sb-row.tbl')).toContainText('axon_table', { timeout: 15_000 });

    // Run the seeded count query.
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/rows/i, { timeout: 30_000 });

    // The results grid is populated.
    await expect(page.locator('table.grid tbody tr')).toHaveCount(1);

    // Query history is persisted as versioned browser metadata, not localStorage.
    await expect
      .poll(async () =>
        page.evaluate(async () => {
          const localKeys = Object.keys(localStorage).filter((key) =>
            key.startsWith('axon-editor.'),
          );
          const dbs = await indexedDB.databases();
          return {
            localKeys,
            hasMetadataDb: dbs.some((db) => db.name === 'axon-editor-metadata'),
          };
        }),
      )
      .toEqual({ localKeys: [], hasMetadataDb: true });

    // Result-grid actions operate on the visible result set.
    await page.locator('button[title="Copy results as CSV"]').click();
    await expect
      .poll(async () => page.evaluate(() => navigator.clipboard.readText()))
      .toContain('row_count');

    await page.locator('table.grid tbody td').nth(1).dblclick();
    await expect(page.locator('[role="dialog"][aria-label="Cell value"]')).toContainText(
      'row_count',
    );

    const downloadPromise = page.waitForEvent('download');
    await page.locator('button[title="Export results as CSV"]').click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toMatch(/^axon-query-results-.*\.csv$/);

    // Plan tab renders the explain string from the worker.
    await page.locator('.res-tab', { hasText: 'Plan' }).click();
    await expect(page.locator('.plan-tree')).toContainText('DataFusion physical plan', {
      timeout: 5_000,
    });

    await page.locator('.res-tab', { hasText: 'Snapshot' }).click();
    await expect(
      page.locator('.kpi', { has: page.locator('.l', { hasText: 'Active files' }) }).locator('.v'),
    ).not.toHaveText('0');

    await page.locator('.code-input').fill('SELECT * FROM missing_table');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText('error', { timeout: 30_000 });
    await page.locator('.res-tab', { hasText: 'Plan' }).click();
    await expect(page.locator('.plan-tree')).toHaveCount(0);

    expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toEqual([]);
  });
});
