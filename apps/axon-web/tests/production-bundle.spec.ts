import { expect, test, type Request } from '@playwright/test';
import { catalogTableSqlPath } from '../src/editor/catalog-navigation.ts';
import { SAMPLE_QUERY_SOURCE_REF } from '../src/services/query-source.ts';

// Runs under playwright.production-bundle.config.ts: the built bundle, the production host's
// routing and headers, and no cross-origin isolation.
const CATEGORY_TOTALS_SQL =
  'SELECT category, COUNT(*) AS rows, SUM(value) AS total_value FROM events GROUP BY category ORDER BY category';

test('queries the bundled sample table through the built app under production headers', async ({
  page,
}) => {
  const pageErrors: string[] = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  const requests: Request[] = [];
  page.on('request', (request) => requests.push(request));

  // A canonical table route must load the app on a cold visit, as a reload or shared link does.
  const response = await page.goto(catalogTableSqlPath(SAMPLE_QUERY_SOURCE_REF));
  expect(response?.status()).toBe(200);
  expect(await page.evaluate(() => globalThis.crossOriginIsolated)).toBe(false);
  await expect(page.locator('.queryref-bar .qref')).toContainText('events', { timeout: 15_000 });

  await page.locator('.code-input').fill(CATEGORY_TOTALS_SQL);
  await page.locator('.btn.primary', { hasText: 'Run' }).click();
  await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 60_000 });

  // The bundled sample's latest snapshot (v3) holds ids 7-10: B 70, 80 and D 90, 100.
  const rows = page.locator('table.grid tbody tr');
  await expect(rows).toHaveCount(2);
  await expect(rows.nth(0).locator('td')).toContainText(['B', '2', '150']);
  await expect(rows.nth(1).locator('td')).toContainText(['D', '2', '190']);

  const urls = requests.map((request) => new URL(request.url()).pathname);
  expect(urls.some((path) => /^\/assets\/axon_web_wasm_bg-[\w-]+\.wasm$/.test(path))).toBe(true);
  expect(urls.some((path) => /^\/assets\/sandbox-query-child-worker-[\w-]+\.js$/.test(path))).toBe(
    true,
  );
  expect(urls.filter((path) => path.startsWith('/src/'))).toEqual([]);
  expect(pageErrors).toEqual([]);

  // A hashed asset the build did not emit must 404 rather than come back as the app shell.
  const missing = await page.request.get('/assets/sandbox-query-child-worker-missing.js');
  expect(missing.status()).toBe(404);
});
