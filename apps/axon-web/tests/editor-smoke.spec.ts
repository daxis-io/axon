import { expect, test, type Page, type Route } from '@playwright/test';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { connectorFeaturesFromEnv } from '../src/services/connector-features.ts';

const APP_ORIGIN = new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5174').origin;
const LOCAL_DELTA_ACTIVE_ID_KEY = 'axon-local-delta-active-id';

type LocalDeltaFixtureFile = {
  relativePath: string;
  bytes: number[];
};

// Phase 1 smoke test: editor mounts, catalog populates, and a query returns rows.
// Lives under tests/ so it benefits from the same baseURL config as the sandbox
// suite, but is opt-in via grep so existing CI continues to target the sandbox.

test.describe('editor (Phase 1 smoke)', () => {
  test('BFF auth-service connector gate is explicitly opt-in', () => {
    expect(connectorFeaturesFromEnv({}).bffAuthServiceConnectors).toBe(false);
    expect(
      connectorFeaturesFromEnv({ VITE_AXON_BFF_AUTH_SERVICE_CONNECTORS: 'enabled' })
        .bffAuthServiceConnectors,
    ).toBe(true);
    expect(
      connectorFeaturesFromEnv({ VITE_AXON_BFF_AUTH_SERVICE_CONNECTORS: 'true' })
        .bffAuthServiceConnectors,
    ).toBe(false);
  });

  test('fallback environment gate accepts only the server mode', () => {
    const source = readFileSync(
      new URL('../src/services/server-fallback.ts', import.meta.url),
      'utf8',
    );

    expect(source).toContain("rawMode === 'server'");
    expect(source).not.toContain("rawMode === 'enabled'");
    expect(source).not.toContain("rawMode === 'true'");
  });

  test('production build declares only the root editor entrypoint', () => {
    const source = readFileSync(new URL('../vite.config.ts', import.meta.url), 'utf8');
    const deployDoc = readFileSync(
      new URL('../../../docs/program/browser-embedding-deployment.md', import.meta.url),
      'utf8',
    );

    expect(source).toContain("editor: resolve(__dirname, 'index.html')");
    expect(source).not.toContain("sandbox: resolve(__dirname, 'sandbox.html')");
    expect(source).toContain("=== '/sandbox.html'");
    expect(deployDoc).not.toContain('two Vite HTML entries');
    expect(deployDoc).not.toContain('leaving `/sandbox.html` as its own entry');
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
    await expect(page.getByRole('dialog', { name: 'Connect a Delta source' })).toBeVisible();
    await page.getByRole('button', { name: 'Close (Esc)' }).click();

    await page.goto('/connect');
    await expect(page.getByRole('button', { name: 'Connect a source' })).toBeVisible();
    await page.getByRole('button', { name: 'local folder' }).click();
    await expect(page.getByRole('dialog', { name: 'Connect a local Delta folder' })).toBeVisible();

    expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toEqual([]);
  });

  test('connect source flows stay browser-owned without private credentials', async ({ page }) => {
    await page.goto('/connect');

    await expect(page.getByRole('button', { name: 'Unity Catalog' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Delta Sharing' })).toBeDisabled();

    await page.getByRole('button', { name: 'Connect a source' }).click();
    const dialog = page.getByRole('dialog', { name: 'Connect a Delta source' });

    await expect(dialog).not.toContainText(/all four sources support the same sql surface area/i);
    await expect(dialog.locator('.cc-source-card', { hasText: 'Object storage' })).toContainText(
      /Access\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-card', { hasText: 'Object storage' })).toContainText(
      /public GCS/i,
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

    const unityCatalogCard = dialog.locator('.cc-source-card', { hasText: 'Unity Catalog' });
    const deltaSharingCard = dialog.locator('.cc-source-card', { hasText: 'Delta Sharing' });
    await expect(unityCatalogCard).toHaveAttribute('aria-disabled', 'true');
    await expect(deltaSharingCard).toHaveAttribute('aria-disabled', 'true');
    await expect(unityCatalogCard).toContainText(/coming soon/i);
    await expect(deltaSharingCard).toContainText(/coming soon/i);
    await unityCatalogCard.click();
    await expect(dialog.getByRole('button', { name: /Continue/ })).toBeDisabled();

    await dialog.locator('.cc-source-card', { hasText: 'Local files' }).click();
    await dialog.getByRole('button', { name: /Continue/ }).click();

    const localConfigDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
    await expect(localConfigDialog).toContainText(/Local Delta table directory/i);
    await expect(localConfigDialog).not.toContainText(/sandbox|not wired/i);
    await expect(localConfigDialog.getByText(/Delta log parsed/i)).toHaveCount(0);
    await expect(localConfigDialog.getByRole('button', { name: 'Test connection' })).toBeDisabled();
    await expect(localConfigDialog.getByRole('button', { name: /Discover tables/ })).toBeDisabled();
    await localConfigDialog.getByRole('button', { name: 'Back' }).click();

    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-card', { hasText: 'Object storage' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

    const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
    await expect(configDialog).toContainText(/browser-local delta log access/i);
    await expect(configDialog).not.toContainText(/trusted delta snapshot descriptor resolver/i);
    await expect(configDialog).not.toContainText(/BFF/i);
    await expect(configDialog.getByRole('button', { name: /AWS S3/ })).toBeDisabled();
    await expect(configDialog.getByRole('button', { name: /Azure ADLS Gen2/ })).toBeDisabled();
    await expect(configDialog.getByRole('button', { name: /Cloudflare R2/ })).toBeDisabled();
    await expect(
      configDialog.getByText(
        /secret key|access key|SAS|bearer token|service-account JSON|encrypted/i,
      ),
    ).toHaveCount(0);

    const gcsParquetPath = prodLikeParquetPath('category=A');
    const gcsParquetBytes = readFileSync(
      new URL(`../public/fixtures/prod-like/table/${gcsParquetPath}`, import.meta.url),
    );
    const gcsDataRequests: string[] = [];

    await page.route('https://storage.googleapis.com/acme-lake?*', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/xml',
        headers: { 'access-control-allow-origin': APP_ORIGIN },
        body: `<?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult>
            <IsTruncated>false</IsTruncated>
            <Contents>
              <Key>silver/_delta_log/00000000000000000000.json</Key>
            </Contents>
          </ListBucketResult>`,
      });
    });
    await page.route(
      'https://storage.googleapis.com/acme-lake/silver/_delta_log/00000000000000000000.json',
      async (route) => {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          headers: { 'access-control-allow-origin': APP_ORIGIN },
          body: [
            JSON.stringify({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }),
            JSON.stringify({
              metaData: {
                id: 'public-object-storage-test',
                format: { provider: 'parquet', options: {} },
                schemaString: JSON.stringify({
                  type: 'struct',
                  fields: [
                    { name: 'id', type: 'long', nullable: true, metadata: {} },
                    { name: 'category', type: 'string', nullable: true, metadata: {} },
                  ],
                }),
                partitionColumns: [],
                configuration: {},
              },
            }),
            JSON.stringify({
              add: {
                path: gcsParquetPath,
                partitionValues: {},
                size: gcsParquetBytes.length,
                modificationTime: 1779479201568,
                dataChange: true,
                stats: JSON.stringify({
                  numRecords: 4,
                  minValues: { id: 1, category: 'alpha' },
                  maxValues: { id: 4, category: 'gamma' },
                  nullCount: { id: 0, category: 0 },
                }),
              },
            }),
          ].join('\n'),
        });
      },
    );
    await page.route(
      `https://storage.googleapis.com/acme-lake/silver/category%3DA/*`,
      async (route) => {
        gcsDataRequests.push(route.request().headers().range ?? 'full');
        await fulfillRangeRequest(route, gcsParquetBytes, APP_ORIGIN);
      },
    );

    await configDialog.getByRole('button', { name: 'Test connection' }).click();
    await expect(configDialog).toContainText(/source check passed/i);
    await expect(configDialog).toContainText(/Delta log is browser-readable/i);
    expect(gcsDataRequests.length).toBeGreaterThan(0);
    await configDialog.getByRole('button', { name: /Discover tables/ }).click();

    const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
    await expect(reviewDialog).toContainText(/Detected 1 public Delta table/i);
    await expect(reviewDialog).toContainText(/silver/i);
    await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

    const persisted = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(persisted).toContain('gs://acme-lake/silver');
    expect(persisted).not.toContain('storage.googleapis.com');
    expect(persisted).not.toContain('X-Goog');
  });

  test('persisted BFF-backed catalogs are not active when the connector gate is off', async ({
    page,
  }) => {
    await page.goto('/connect');
    await page.evaluate(() => {
      localStorage.setItem(
        'axon.connect.catalogs.v1',
        JSON.stringify([
          {
            id: 'legacy-uc',
            alias: 'legacy-uc',
            kind: 'unity_catalog',
            storage: '/api/uc/read-access-plan',
            host: 'https://acme-prod.cloud.databricks.com',
            region: 'brokered',
            status: 'connected',
            connectedAt: 'old session',
            schemas: [
              {
                name: 'main',
                tables: [
                  {
                    name: 'orders',
                    snapshot: 42,
                    rows: 10,
                    files: 1,
                    size: '1 MB',
                    protocol: 'r2/w5',
                    manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
                  },
                ],
              },
            ],
          },
          {
            id: 'legacy-share',
            alias: 'legacy-share',
            kind: 'delta_share',
            storage: 'partner-profile',
            host: 'https://sharing.acme.io/delta-sharing',
            region: 'provider-vended',
            status: 'connected',
            connectedAt: 'old session',
            schemas: [
              {
                name: 'partner',
                tables: [
                  {
                    name: 'events',
                    snapshot: 7,
                    rows: 10,
                    files: 1,
                    size: '1 MB',
                    protocol: 'r2/w5',
                    manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
                  },
                ],
              },
            ],
          },
        ]),
      );
    });

    await page.reload();

    await expect(page.getByText('legacy-uc')).toHaveCount(0);
    await expect(page.getByText('legacy-share')).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Unity Catalog' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Delta Sharing' })).toBeDisabled();

    const persisted = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(persisted).toContain('legacy-uc');
    expect(persisted).toContain('legacy-share');
  });

  test('activates a selected connected table instead of always using the first catalog', async ({
    page,
  }) => {
    const catalogs = [
      {
        id: 'sample-lake-fixture',
        alias: 'sample-lake',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://axon-sample/prod-like-events',
        region: 'browser-local',
        status: 'connected',
        connectedAt: 'sample fixture',
        schemas: [
          {
            name: 'prod_like',
            tables: [
              {
                name: 'events',
                snapshot: 3,
                rows: 6,
                files: 1,
                size: 'fixture',
                protocol: 'r2/w5',
                manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
              },
            ],
          },
        ],
      },
      {
        id: 'second-lake-fixture',
        alias: 'second-lake',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://axon-second/prod-like-events',
        region: 'browser-local',
        status: 'connected',
        connectedAt: 'test fixture',
        schemas: [
          {
            name: 'prod_like',
            tables: [
              {
                name: 'events',
                snapshot: 3,
                rows: 6,
                files: 1,
                size: 'fixture',
                protocol: 'r2/w5',
                manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
              },
            ],
          },
        ],
      },
    ];
    await page.addInitScript((value) => {
      localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
    }, catalogs);

    await page.goto('/');
    await expect(page.locator('.conn-pill')).toContainText('sample-lake', { timeout: 15_000 });

    await page.locator('.conn-pill').click();
    await page.getByRole('button', { name: /Expand second-lake/ }).click();
    await page.getByRole('button', { name: /Activate second-lake prod_like events/ }).click();

    await expect(page.locator('.conn-pill')).toContainText('second-lake');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/rows/i, { timeout: 30_000 });
  });

  test('connects a local Delta folder from the root editor and queries it in browser WASM', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));

    const localRegistryId = await connectLocalDeltaFolder(page, tableDir, 'local-prod-like');

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 30_000,
    });
    await expect(page.locator('table.grid tbody tr')).toHaveCount(1);
    await expect(page.locator('table.grid')).toContainText('row_count');
    await expect(page.locator('table.grid')).toContainText('4');

    const connectState = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(connectState).toContain('local-prod-like');
    expect(connectState).toContain('localRegistryId');
    expect(connectState).not.toMatch(/bytes|ArrayBuffer|secret|bearer|token|client[_-]?secret/i);

    const registryRecord = await localDeltaRegistryRecord(page, localRegistryId);
    expect(registryRecord?.backend).toBe('metadata_only');
    expect(registryRecord?.paths).toContain('_delta_log/00000000000000000003.json');
    expect(registryRecord?.paths.some((path) => path.includes('category=A'))).toBe(false);
    expect(registryRecord?.paths.some((path) => path.includes('category=C'))).toBe(false);
    expect(registryRecord?.paths.some((path) => path.includes('category=B'))).toBe(true);
    expect(registryRecord?.paths.some((path) => path.includes('category=D'))).toBe(true);
    expect(
      registryRecord?.files.some((file) => file.path.endsWith('.parquet') && file.hasBytes),
    ).toBe(false);
  });

  test('queries the query-engine stress Delta table with DATE and timestamp columns in browser WASM', async ({
    page,
  }) => {
    const tableDir = process.env.AXON_STRESS_DELTA_PATH;
    if (!tableDir) {
      test.skip(
        true,
        'Set AXON_STRESS_DELTA_PATH=/Users/ethanurbanski/delta-tables/query-engine-stress-delta to run this local smoke.',
      );
      return;
    }

    await connectLocalDeltaFolder(page, tableDir, 'stress-local', {
      expectedTable: 'query_engine_stress_delta',
    });

    await page
      .locator('.code-input')
      .fill(
        "SELECT event_id, event_date, event_ts, region FROM query_engine_stress_delta WHERE region = 'us-east' AND event_date = DATE '2025-05-13' LIMIT 5",
      );
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 120_000,
    });
    await expect(page.locator('table.grid tbody tr')).toHaveCount(5, { timeout: 120_000 });
    await expect(page.locator('table.grid')).toContainText('event_date');
    await expect(page.locator('table.grid')).toContainText('2025-05-13');
    await expect(page.locator('table.grid')).toContainText('event_ts');
  });

  test('local Delta metadata registry does not copy active Parquet data files', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));
    await blockOpfsLocalDeltaRegistry(page);
    await failOnParquetArrayBuffer(page);

    const localRegistryId = await connectLocalDeltaFolder(page, tableDir, 'metadata-local');

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 30_000,
    });
    await expect(page.locator('table.grid')).toContainText('4');

    const registryRecord = await localDeltaRegistryRecord(page, localRegistryId);
    expect(registryRecord).toMatchObject({
      id: localRegistryId,
      backend: 'metadata_only',
    });
    expect(registryRecord?.paths.some((path) => path.endsWith('.parquet'))).toBe(true);
    expect(
      registryRecord?.files.some((file) => file.path.endsWith('.parquet') && file.hasBytes),
    ).toBe(false);

    const arrayBufferReads = await page.evaluate(
      () =>
        (window as Window & { __axonParquetArrayBufferReads?: string[] })
          .__axonParquetArrayBufferReads ?? [],
    );
    expect(arrayBufferReads).toEqual([]);
  });

  test('local Delta folder still queries for the current session when durable registry storage is unavailable', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));
    await blockDurableLocalDeltaRegistry(page);

    await connectLocalDeltaFolder(page, tableDir, 'session-local', { expectPersisted: false });

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 30_000,
    });
    await expect(page.locator('table.grid')).toContainText('row_count');
    await expect(page.locator('table.grid')).toContainText('4');

    const persisted = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(persisted).not.toContain('session-local');
  });

  test('unsupported local Delta features do not leave an active local registry id', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(
      new URL('./fixtures/unsupported-feature-table', import.meta.url),
    );

    await page.goto('/');
    await page.getByRole('button', { name: /^Connect$/ }).click();
    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-card', { hasText: 'Local files' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

    const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
    await localDialog.getByLabel('Local Delta table directory').setInputFiles(tableDir);
    await expect(localDialog).toContainText(/unsupported features: deletionVectors/i);

    const activeId = await page.evaluate(
      (key) => localStorage.getItem(key),
      LOCAL_DELTA_ACTIVE_ID_KEY,
    );
    expect(activeId).toBeNull();
  });

  test('disconnecting a local Delta catalog removes its local registry entry', async ({ page }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));
    const localRegistryId = await connectLocalDeltaFolder(page, tableDir, 'local-prod-like');

    await expect
      .poll(() => localDeltaRegistryRecord(page, localRegistryId))
      .toMatchObject({ id: localRegistryId });

    await page.locator('.conn-pill').click();
    const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
    await panel.locator('[title="Manage connection"]').first().click();
    await panel.getByRole('button', { name: /Disconnect catalog/ }).click();

    await expect(page.locator('.conn-pill')).toContainText('sample-lake');
    await expect
      .poll(async () => ({
        activeId: await page.evaluate(
          (key) => localStorage.getItem(key),
          LOCAL_DELTA_ACTIVE_ID_KEY,
        ),
        record: await localDeltaRegistryRecord(page, localRegistryId),
      }))
      .toEqual({ activeId: null, record: null });
  });

  test('reload keeps local Delta metadata but requires reselect before querying', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));

    await page.goto('/');
    await page.getByRole('button', { name: /^Connect$/ }).click();
    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-card', { hasText: 'Local files' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();
    const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
    await localDialog.getByLabel('Local Delta table directory').setInputFiles(tableDir);
    await localDialog.getByRole('button', { name: 'Test connection' }).click();
    await localDialog.getByRole('button', { name: /Discover tables/ }).click();
    const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
    await reviewDialog.getByLabel('Catalog alias').fill('local-prod-like');
    await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();
    await expect(page.locator('.conn-pill')).toContainText('local-prod-like', {
      timeout: 15_000,
    });

    await page.reload();
    await expect(page.locator('.conn-pill')).toContainText('local-prod-like', {
      timeout: 15_000,
    });
    await expect(page.locator('.queryref-bar .qref')).toContainText('axon_prod_like_fixture');

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/error/i, {
      timeout: 30_000,
    });
    await expect(page.locator('.results')).toContainText(/saved as metadata only/i);
    await expect(page.locator('.results')).toContainText(/Select the folder again/i);
  });

  test('persists File System Access directory handles across reload for local Delta catalogs', async ({
    page,
  }) => {
    const tableDir = fileURLToPath(new URL('../public/fixtures/prod-like/table', import.meta.url));
    const localRegistryId = await connectLocalDeltaDirectoryHandle(page, tableDir, 'handle-local');

    const registryRecord = await localDeltaRegistryRecord(page, localRegistryId);
    expect(registryRecord?.backend).toBe('directory_handle');
    expect(
      registryRecord?.files.some((file) => file.path.endsWith('.parquet') && file.hasBytes),
    ).toBe(false);

    await page.reload();
    await expect(page.locator('.conn-pill')).toContainText('handle-local', {
      timeout: 15_000,
    });

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 30_000,
    });
    await expect(page.locator('table.grid')).toContainText('row_count');
    await expect(page.locator('table.grid')).toContainText('4');
  });

  test('unexpected local Delta registry errors surface instead of using fallback catalog metadata', async ({
    page,
  }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });
    await page.addInitScript(() => {
      localStorage.setItem(
        'axon.connect.catalogs.v1',
        JSON.stringify([
          {
            id: 'broken-local',
            alias: 'broken-local',
            kind: 'local',
            storage: 'Local folder: broken',
            region: 'browser-local',
            status: 'connected',
            connectedAt: 'test fixture',
            schemas: [
              {
                name: 'default',
                tables: [
                  {
                    name: 'broken_local_table',
                    snapshot: 3,
                    rows: 4,
                    files: 2,
                    size: 'fixture',
                    protocol: 'r2/w5',
                    localRegistryId: 'broken-registry',
                  },
                ],
              },
            ],
          },
        ]),
      );
      const originalOpen = window.indexedDB.open.bind(window.indexedDB);
      Object.defineProperty(window.indexedDB, 'open', {
        configurable: true,
        value: (name: string, version?: number) => {
          if (name === 'axon-local-delta-registry') {
            throw new Error('registry boom');
          }
          return originalOpen(name, version);
        },
      });
    });

    await page.goto('/');

    await expect
      .poll(() => consoleErrors.join('\n'), { timeout: 15_000 })
      .toContain('failed to load catalog');
    expect(consoleErrors.join('\n')).toContain('registry boom');
  });

  test('cancelling File System Access directory picker leaves local connect dialog stable', async ({
    page,
  }) => {
    const browserErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') browserErrors.push(msg.text());
    });
    page.on('pageerror', (err) => browserErrors.push(err.message));
    await page.addInitScript(() => {
      Object.defineProperty(window, '__axonUnhandledRejections', {
        configurable: true,
        value: [],
        writable: true,
      });
      window.addEventListener('unhandledrejection', (event) => {
        const reason = event.reason;
        (
          window as Window & { __axonUnhandledRejections?: string[] }
        ).__axonUnhandledRejections?.push(
          reason instanceof Error ? `${reason.name}: ${reason.message}` : String(reason),
        );
      });
      Object.defineProperty(window, 'showDirectoryPicker', {
        configurable: true,
        value: async () => {
          throw new DOMException('The user aborted a request.', 'AbortError');
        },
      });
    });

    await page.goto('/');
    const localDialog = await openLocalDeltaConnectDialog(page);
    await localDialog.locator('.cc-drop').click();

    await expect(localDialog).toContainText(/Local Delta table directory/i);
    await expect(localDialog.getByText(/Delta log parsed/i)).toHaveCount(0);
    await expect
      .poll(() =>
        page.evaluate(
          () =>
            (window as Window & { __axonUnhandledRejections?: string[] })
              .__axonUnhandledRejections ?? [],
        ),
      )
      .toEqual([]);
    expect(browserErrors, `browser errors:\n${browserErrors.join('\n')}`).toEqual([]);
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

async function openLocalDeltaConnectDialog(page: Page) {
  await page.getByRole('button', { name: /^Connect$/ }).click();

  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-card', { hasText: 'Local files' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  return page.getByRole('dialog', { name: 'Connect a local Delta folder' });
}

async function connectLocalDeltaFolder(
  page: Page,
  tableDir: string,
  alias: string,
  options: { expectPersisted?: boolean; expectedTable?: string | RegExp } = {},
): Promise<string> {
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();

  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-card', { hasText: 'Local files' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
  await localDialog.getByLabel('Local Delta table directory').setInputFiles(tableDir);
  await expect(localDialog).toContainText(/Delta log parsed/i);
  await localDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(localDialog).toContainText(/source check passed/i);
  await localDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  await expect(reviewDialog).toContainText(/Detected 1 local Delta table/i);
  await reviewDialog.getByLabel('Catalog alias').fill(alias);
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

  await expect(page.locator('.conn-pill')).toContainText(alias, { timeout: 15_000 });
  await expect(page.locator('.queryref-bar .qref')).toContainText(
    options.expectedTable ?? 'axon_prod_like_fixture',
  );

  const localRegistryId = await page.evaluate((catalogAlias) => {
    const catalogs = JSON.parse(localStorage.getItem('axon.connect.catalogs.v1') ?? '[]') as Array<{
      alias: string;
      schemas: Array<{ tables: Array<{ localRegistryId?: string }> }>;
    }>;
    return catalogs
      .find((catalog) => catalog.alias === catalogAlias)
      ?.schemas.flatMap((schema) => schema.tables)
      .find((table) => table.localRegistryId)?.localRegistryId;
  }, alias);
  if (options.expectPersisted !== false) expect(localRegistryId).toBeTruthy();
  return localRegistryId ?? '';
}

async function connectLocalDeltaDirectoryHandle(
  page: Page,
  tableDir: string,
  alias: string,
): Promise<string> {
  await installDirectoryPickerFixture(page, localDeltaFixtureFiles(tableDir), 'opfs-prod-like');
  await page.goto('/');
  const localDialog = await openLocalDeltaConnectDialog(page);
  await localDialog.locator('.cc-drop').click();
  await expect(localDialog).toContainText(/Delta log parsed/i);
  await expect(localDialog).toContainText(/Reload: directory handle stored/i);
  await localDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(localDialog).toContainText(/source check passed/i);
  await localDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  await expect(reviewDialog).toContainText(/Detected 1 local Delta table/i);
  await reviewDialog.getByLabel('Catalog alias').fill(alias);
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

  await expect(page.locator('.conn-pill')).toContainText(alias, { timeout: 15_000 });
  await expect(page.locator('.queryref-bar .qref')).toContainText('axon_prod_like_fixture');

  const localRegistryId = await page.evaluate((catalogAlias) => {
    const catalogs = JSON.parse(localStorage.getItem('axon.connect.catalogs.v1') ?? '[]') as Array<{
      alias: string;
      schemas: Array<{ tables: Array<{ localRegistryId?: string }> }>;
    }>;
    return catalogs
      .find((catalog) => catalog.alias === catalogAlias)
      ?.schemas.flatMap((schema) => schema.tables)
      .find((table) => table.localRegistryId)?.localRegistryId;
  }, alias);
  expect(localRegistryId).toBeTruthy();
  return localRegistryId ?? '';
}

async function fulfillRangeRequest(route: Route, bytes: Buffer, origin: string): Promise<void> {
  const range = route.request().headers().range;
  if (!range) {
    await route.fulfill({
      status: 200,
      contentType: 'application/octet-stream',
      headers: {
        'access-control-allow-origin': origin,
        'access-control-expose-headers': 'Content-Length, Content-Range, Accept-Ranges, ETag',
        'accept-ranges': 'bytes',
        'content-length': String(bytes.length),
      },
      body: bytes,
    });
    return;
  }

  const bounded = /^bytes=(\d+)-(\d+)?$/.exec(range);
  const suffix = /^bytes=-(\d+)$/.exec(range);
  let start: number;
  let end: number;
  if (bounded) {
    start = Number(bounded[1]);
    end = bounded[2] === undefined ? bytes.length - 1 : Number(bounded[2]);
  } else if (suffix) {
    const length = Number(suffix[1]);
    start = Math.max(0, bytes.length - length);
    end = bytes.length - 1;
  } else {
    throw new Error(`unsupported test range header: ${range}`);
  }

  const body = bytes.subarray(start, end + 1);
  await route.fulfill({
    status: 206,
    contentType: 'application/octet-stream',
    headers: {
      'access-control-allow-origin': origin,
      'access-control-expose-headers': 'Content-Length, Content-Range, Accept-Ranges, ETag',
      'accept-ranges': 'bytes',
      'content-range': `bytes ${start}-${end}/${bytes.length}`,
      'content-length': String(body.length),
      etag: '"editor-smoke-public-gcs"',
    },
    body,
  });
}

async function installDirectoryPickerFixture(
  page: Page,
  files: LocalDeltaFixtureFile[],
  tableName: string,
): Promise<void> {
  await page.addInitScript(
    ({ records, rootName }) => {
      async function directoryFor(
        root: FileSystemDirectoryHandle,
        segments: string[],
      ): Promise<FileSystemDirectoryHandle> {
        let directory = root;
        for (const segment of segments) {
          directory = await directory.getDirectoryHandle(segment, { create: true });
        }
        return directory;
      }

      async function writeFixtureFile(
        root: FileSystemDirectoryHandle,
        relativePath: string,
        bytes: number[],
      ): Promise<void> {
        const segments = relativePath.split('/');
        const fileName = segments.pop();
        if (!fileName) throw new Error(`fixture path '${relativePath}' did not include a file`);
        const directory = await directoryFor(root, segments);
        const fileHandle = await directory.getFileHandle(fileName, { create: true });
        const writable = await fileHandle.createWritable();
        try {
          await writable.write(new Uint8Array(bytes));
        } finally {
          await writable.close();
        }
      }

      Object.defineProperty(window, 'showDirectoryPicker', {
        configurable: true,
        value: async () => {
          const storageRoot = await navigator.storage.getDirectory();
          const fixtureRoot = await storageRoot.getDirectoryHandle('axon-test-local-delta', {
            create: true,
          });
          try {
            await fixtureRoot.removeEntry(rootName, { recursive: true });
          } catch {
            // The test fixture is created lazily; no prior entry is expected on a clean context.
          }
          const tableRoot = await fixtureRoot.getDirectoryHandle(rootName, { create: true });
          for (const file of records) {
            await writeFixtureFile(tableRoot, file.relativePath, file.bytes);
          }
          return tableRoot;
        },
      });
    },
    { records: files, rootName: tableName },
  );
}

function localDeltaFixtureFiles(rootDir: string, prefix = ''): LocalDeltaFixtureFile[] {
  return readdirSync(join(rootDir, prefix), { withFileTypes: true }).flatMap((entry) => {
    const relativePath = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) return localDeltaFixtureFiles(rootDir, relativePath);
    if (!entry.isFile()) return [];
    return [
      {
        relativePath,
        bytes: [...readFileSync(join(rootDir, relativePath))],
      },
    ];
  });
}

function prodLikeParquetPath(categoryPath: string): string {
  const categoryDir = fileURLToPath(
    new URL(`../public/fixtures/prod-like/table/${categoryPath}/`, import.meta.url),
  );
  const fileName = readdirSync(categoryDir).find((name) => name.endsWith('.parquet'));
  if (!fileName) throw new Error(`no Parquet fixture found under ${categoryPath}`);
  return `${categoryPath}/${fileName}`;
}

async function blockDurableLocalDeltaRegistry(page: Page): Promise<void> {
  await page.addInitScript(() => {
    if (navigator.storage) {
      Object.defineProperty(navigator.storage, 'getDirectory', {
        configurable: true,
        value: undefined,
      });
    }
    Object.defineProperty(window.indexedDB, 'open', {
      configurable: true,
      value: () => {
        throw new DOMException('blocked by test', 'InvalidStateError');
      },
    });
  });
}

async function blockOpfsLocalDeltaRegistry(page: Page): Promise<void> {
  await page.addInitScript(() => {
    if (navigator.storage) {
      Object.defineProperty(navigator.storage, 'getDirectory', {
        configurable: true,
        value: undefined,
      });
    }
  });
}

async function failOnParquetArrayBuffer(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const originalArrayBuffer = File.prototype.arrayBuffer;
    Object.defineProperty(window, '__axonParquetArrayBufferReads', {
      configurable: true,
      value: [],
      writable: true,
    });
    File.prototype.arrayBuffer = function () {
      if (this.name.endsWith('.parquet')) {
        (
          window as unknown as { __axonParquetArrayBufferReads: string[] }
        ).__axonParquetArrayBufferReads.push(this.name);
        throw new Error(`unexpected Parquet data-file copy: ${this.name}`);
      }
      return originalArrayBuffer.call(this);
    };
  });
}

async function localDeltaRegistryRecord(
  page: Page,
  registryId: string,
): Promise<{
  id: string;
  backend: string;
  paths: string[];
  files: Array<{ path: string; hasBytes: boolean }>;
} | null> {
  return page.evaluate(async (id) => {
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open('axon-local-delta-registry', 1);
      request.onerror = () => reject(request.error ?? new Error('open failed'));
      request.onsuccess = () => resolve(request.result);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains('tables')) {
          db.createObjectStore('tables', { keyPath: 'id' });
        }
      };
    });
    return new Promise<{
      id: string;
      backend: string;
      paths: string[];
      files: Array<{ path: string; hasBytes: boolean }>;
    } | null>((resolve, reject) => {
      const tx = db.transaction('tables', 'readonly');
      const request = tx.objectStore('tables').get(id);
      request.onerror = () => reject(request.error ?? new Error('read failed'));
      request.onsuccess = () => {
        const record = request.result as
          | {
              id: string;
              backend: string;
              files: Array<{ relativePath: string; bytes?: ArrayBuffer }>;
            }
          | undefined;
        resolve(
          record
            ? {
                id: record.id,
                backend: record.backend,
                paths: record.files.map((file) => file.relativePath).sort(),
                files: record.files
                  .map((file) => ({
                    path: file.relativePath,
                    hasBytes: !!file.bytes,
                  }))
                  .sort((left, right) => left.path.localeCompare(right.path)),
              }
            : null,
        );
      };
    });
  }, registryId);
}
