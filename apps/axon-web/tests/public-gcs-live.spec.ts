import { expect, test } from '@playwright/test';

import {
  buildPublicDeltaLogManifest,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';

const liveTableUri = process.env.AXON_LIVE_PUBLIC_GCS_TABLE_URI;
const liveOrigin =
  process.env.AXON_LIVE_PUBLIC_GCS_ORIGIN ??
  new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173').origin;

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
  }) => {
    const tableName = liveTableUri!.split('/').filter(Boolean).at(-1) ?? 'public_table';

    await page.goto('/');
    await page.getByRole('button', { name: /^Connect$/ }).click();
    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-card', { hasText: 'Object storage' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

    const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
    await configDialog
      .locator('.cc-input.mono.has-prefix')
      .fill(liveTableUri!.replace(/^gs:\/\//, ''));
    await configDialog.getByRole('button', { name: 'Test connection' }).click();
    await expect(configDialog).toContainText(/source check passed/i, { timeout: 60_000 });
    await configDialog.getByRole('button', { name: /Discover tables/ }).click();

    const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
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
  });
});

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
