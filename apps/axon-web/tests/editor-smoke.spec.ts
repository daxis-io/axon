import { expect, test } from '@playwright/test';
import { readFileSync } from 'node:fs';

const APP_ORIGIN = new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5174').origin;

// Phase 1 smoke test: editor mounts, catalog populates, and a query returns rows.
// Lives under tests/ so it benefits from the same baseURL config as the sandbox
// suite, but is opt-in via grep so existing CI continues to target the sandbox.

test.describe('editor (Phase 1 smoke)', () => {
  test('fallback environment gate accepts only the server mode', () => {
    const source = readFileSync(
      new URL('../src/services/server-fallback.ts', import.meta.url),
      'utf8',
    );

    expect(source).toContain("rawMode === 'server'");
    expect(source).not.toContain("rawMode === 'enabled'");
    expect(source).not.toContain("rawMode === 'true'");
  });

  test('routes between the workspace and connect page', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });
    page.on('pageerror', (err) => consoleErrors.push(err.message));

    await page.goto('/connect');
    await expect(page.getByRole('heading', { name: 'Connect a Delta source' })).toBeVisible();

    await page.getByRole('button', { name: /Back to workspace/ }).click();
    await expect(page.locator('.shell .brand-name')).toContainText('axon');

    await page.getByRole('button', { name: 'Connect' }).click();
    await expect(page).toHaveURL(/\/connect$/);
    await expect(page.getByRole('button', { name: 'Connect a source' })).toBeVisible();

    await page.getByRole('button', { name: 'local folder' }).click();
    await expect(page.getByRole('dialog', { name: 'Connect a local Delta folder' })).toBeVisible();

    expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toEqual([]);
  });

  test('connect source flows fail closed without browser-owned credentials', async ({ page }) => {
    await page.goto('/connect');

    await page.getByRole('button', { name: 'Connect a source' }).click();
    const dialog = page.getByRole('dialog', { name: 'Connect a Delta source' });

    await expect(dialog).not.toContainText(/all four sources support the same sql surface area/i);
    await expect(dialog.locator('.cc-source-card', { hasText: 'Object storage' })).toContainText(
      /Access\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-card', { hasText: 'Object storage' })).toContainText(
      /Snapshot\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-card', { hasText: 'Object storage' })).toContainText(
      /Query\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-card', { hasText: 'Delta Sharing' })).toContainText(
      /Snapshot\s*Browser materialized/i,
    );

    await dialog.locator('.cc-source-card', { hasText: 'Object storage' }).click();
    await dialog.getByRole('button', { name: /Continue/ }).click();

    const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
    await expect(configDialog).toContainText(/browser-local delta log access/i);
    await expect(configDialog).not.toContainText(/trusted delta snapshot descriptor resolver/i);
    await expect(configDialog).not.toContainText(/BFF/i);
    await expect(
      configDialog.getByText(
        /secret key|access key|SAS|bearer token|service-account JSON|encrypted/i,
      ),
    ).toHaveCount(0);

    await configDialog.getByRole('button', { name: 'Test connection' }).click();
    await expect(configDialog.getByText(/connection verified/i)).toHaveCount(0);
    await expect(configDialog).toContainText(/browser-local storage access not configured/i);
    await expect(configDialog.getByRole('button', { name: /Discover tables/ })).toBeDisabled();
  });

  test('loads selected connected catalog, populates table, runs a query', async ({
    page,
    context,
  }) => {
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

    // Catalog resolves from the selected connected catalog/table, not the legacy fixture name.
    await expect(page.locator('.conn-pill')).toContainText('sample-lake', { timeout: 15_000 });
    await expect(page.locator('.queryref-bar .qref')).toContainText('events');
    await expect(page.locator('.sb-row.tbl')).toContainText('events');
    await expect(page.locator('.queryref-bar .qref')).not.toContainText('axon_table');

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

    const connectState = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(connectState).toContain('sample-lake');
    expect(connectState).not.toMatch(
      /secret|access[_-]?key|bearer|token|service[_-]?account|client[_-]?secret|sas/i,
    );

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
