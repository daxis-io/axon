import { expect, test, type Locator, type Page } from '@playwright/test';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  buildCatalogFromResult,
  catalogsAvailableForFeatures,
  loadConnectedCatalogs,
  localRegistryIdsForCatalogs,
  upsertConnectedCatalog,
} from '../src/editor/connect/store.ts';
import type { ConnectedCatalog, ConnectResult } from '../src/editor/connect/types.ts';
import {
  querySourceFromConnectedCatalogs,
  SAMPLE_QUERY_SOURCE,
} from '../src/services/query-source.ts';
import { connectorFeaturesFromEnv } from '../src/services/connector-features.ts';
import {
  QUERY_RESULT_PAGE_SIZE,
  queryResultPageRequest,
  queryResultPageRun,
  queryResultPageRunRequest,
  resultPageFromPreview,
  sameQueryResultPageRun,
} from '../src/services/query-pagination.ts';

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

  test('workspace topbar does not expose placeholder controls', () => {
    const source = readFileSync(new URL('../src/editor/App.tsx', import.meta.url), 'utf8');

    expect(source).not.toContain('Branch (Git-style versioning)');
    expect(source).not.toContain('Explain plan (Phase 2)');
    expect(source).not.toContain('title="Share"');
    expect(source).not.toContain('<IconBranch');
    expect(source).not.toContain('<IconShare');
    expect(source).not.toContain('<IconSparkle');
  });

  test('connect results default into a workspace Axon catalog with table source bindings', () => {
    const catalog = buildCatalogFromResult(
      connectResultFixture({
        alias: '',
        source: 'object_store',
        schemaName: 'default',
        tableName: 'orders',
      }),
    );

    expect(catalog.alias).toBe('workspace');
    expect(catalog.schemas[0]?.name).toBe('default');
    expect(catalog.schemas[0]?.tables[0]).toMatchObject({
      name: 'orders',
      source: {
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://acme-lake/silver',
        region: 'us-central1',
      },
    });
  });

  test('connected catalog store merges different sources into one Axon catalog', () => {
    const localCatalog = buildCatalogFromResult(
      connectResultFixture({
        alias: 'workspace',
        source: 'local',
        schemaName: 'default',
        tableName: 'orders_local',
      }),
    );
    const objectCatalog = buildCatalogFromResult(
      connectResultFixture({
        alias: 'workspace',
        source: 'object_store',
        schemaName: 'analytics',
        tableName: 'events',
      }),
    );

    const result = upsertConnectedCatalog([localCatalog], objectCatalog);

    expect(result.catalogs).toHaveLength(1);
    expect(result.catalogs[0].alias).toBe('workspace');
    expect(result.catalogs[0].schemas.map((schema) => schema.name)).toEqual([
      'default',
      'analytics',
    ]);
    expect(
      result.catalogs[0].schemas.flatMap((schema) => schema.tables.map((table) => table.name)),
    ).toEqual(['orders_local', 'events']);

    const source = querySourceFromConnectedCatalogs(result.catalogs, {
      catalogId: result.catalogs[0].id,
      schemaName: 'analytics',
      tableName: 'events',
    });
    expect(source).toMatchObject({
      catalogName: 'workspace',
      schemaName: 'analytics',
      tableName: 'events',
      storage: 'gs://acme-lake/silver',
      region: 'us-central1',
    });
  });

  test('connector feature gates filter tables inside mixed Axon catalogs', () => {
    const catalog = connectedCatalogFixture({
      id: 'workspace',
      alias: 'workspace',
      kind: 'unity_catalog',
      storage: '2 sources',
      region: 'mixed',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'local_orders',
              snapshot: 3,
              rows: 6,
              files: 1,
              size: 'fixture',
              protocol: 'r2/w5',
              localRegistryId: 'local-registry-orders',
              source: {
                id: 'source-local-orders',
                kind: 'local',
                storage: 'Local folder: local-orders',
                region: 'browser-local',
                canonicalKey: 'local||||default|local_orders',
                connectedAt: 'test fixture',
              },
            },
            {
              name: 'governed_orders',
              snapshot: 3,
              rows: 6,
              files: 1,
              size: 'fixture',
              protocol: 'r2/w5',
              manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
              source: {
                id: 'source-uc-orders',
                kind: 'unity_catalog',
                storage: '/api/uc/read-access-plan',
                region: 'brokered',
                canonicalKey:
                  'unity_catalog|||https://acme.cloud.databricks.com||default|governed_orders',
                connectedAt: 'test fixture',
              },
            },
          ],
        },
      ],
    });

    const filtered = catalogsAvailableForFeatures([catalog], { bffAuthServiceConnectors: false });

    expect(filtered).toHaveLength(1);
    expect(filtered[0].schemas[0]?.tables.map((table) => table.name)).toEqual(['local_orders']);
  });

  test('local registry cleanup includes local tables inside mixed Axon catalogs', () => {
    const catalog = connectedCatalogFixture({
      id: 'workspace',
      alias: 'workspace',
      kind: 'object_store',
      storage: '2 sources',
      region: 'mixed',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'local_orders',
              snapshot: 3,
              rows: 6,
              files: 1,
              size: 'fixture',
              protocol: 'r2/w5',
              localRegistryId: 'local-registry-orders',
              source: {
                id: 'source-local-orders',
                kind: 'local',
                storage: 'Local folder: local-orders',
                region: 'browser-local',
                canonicalKey: 'local||||default|local_orders',
                connectedAt: 'test fixture',
              },
            },
          ],
        },
      ],
    });

    expect(localRegistryIdsForCatalogs([catalog])).toEqual(['local-registry-orders']);
  });

  test('merged Axon catalogs keep incoming tables addressable by the final catalog id', () => {
    const existing = connectedCatalogFixture({
      id: 'legacy-workspace-id',
      alias: 'workspace',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'orders',
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
    });
    const incoming = buildCatalogFromResult(
      connectResultFixture({
        alias: 'workspace',
        source: 'object_store',
        schemaName: 'analytics',
        tableName: 'events',
      }),
    );

    const result = upsertConnectedCatalog([existing], incoming);
    const merged = result.catalogs[0];

    expect(merged.id).toBe('legacy-workspace-id');
    expect(merged.id).not.toBe(incoming.id);
    expect(
      querySourceFromConnectedCatalogs(result.catalogs, {
        catalogId: merged.id,
        schemaName: 'analytics',
        tableName: 'events',
      }),
    ).toMatchObject({
      catalogName: 'workspace',
      schemaName: 'analytics',
      tableName: 'events',
      storage: 'gs://acme-lake/silver',
    });
  });

  test('review step keeps recommended organization enabled while exposing a custom alias label', () => {
    const source = readFileSync(
      new URL('../src/editor/connect/ConnectModal.tsx', import.meta.url),
      'utf8',
    );

    expect(source).toContain('Use recommended organization');
    expect(source).toContain('Catalog alias');
  });

  test('connected catalog store keeps only the newest catalog for each source', () => {
    const original = connectedCatalogFixture({
      id: 'old-acme',
      alias: 'old-acme',
      storage: 'gs://acme-lake/silver/',
    });
    const other = connectedCatalogFixture({
      id: 'other-lake',
      alias: 'other-lake',
      storage: 'gs://other-lake/silver',
    });
    const updated = connectedCatalogFixture({
      id: 'new-acme',
      alias: 'new-acme',
      storage: ' gs://acme-lake/silver ',
    });
    const previousWindow = globalThis.window;
    const storage = new Map<string, string>([
      ['axon.connect.catalogs.v1', JSON.stringify([updated, original, other])],
    ]);

    Object.defineProperty(globalThis, 'window', {
      configurable: true,
      value: {
        localStorage: {
          getItem: (key: string) => storage.get(key) ?? null,
          setItem: (key: string, value: string) => storage.set(key, value),
        },
      },
    });

    try {
      expect(
        upsertConnectedCatalog([original, other], updated).catalogs.map((catalog) => catalog.id),
      ).toEqual(['new-acme', 'other-lake']);
      expect(loadConnectedCatalogs().map((catalog) => catalog.id)).toEqual([
        'new-acme',
        'other-lake',
      ]);
    } finally {
      if (previousWindow === undefined) {
        Reflect.deleteProperty(globalThis, 'window');
      } else {
        Object.defineProperty(globalThis, 'window', {
          configurable: true,
          value: previousWindow,
        });
      }
    }
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

  test('query result pagination requests a sentinel row and exposes the next offset', () => {
    const workerPage = queryResultPageRequest({ offset: 500, size: QUERY_RESULT_PAGE_SIZE });
    const previewRows = Array.from({ length: QUERY_RESULT_PAGE_SIZE + 1 }, (_, index) => [index]);

    const result = resultPageFromPreview(
      {
        columns: ['id'],
        rows: previewRows,
        row_count: previewRows.length,
        preview_row_limit: previewRows.length,
        truncated: false,
      },
      { offset: 0, size: QUERY_RESULT_PAGE_SIZE },
    );

    expect(workerPage).toEqual({ limit: QUERY_RESULT_PAGE_SIZE + 1, offset: 500 });
    expect(result.rows).toHaveLength(QUERY_RESULT_PAGE_SIZE);
    expect(result.row_count).toBe(QUERY_RESULT_PAGE_SIZE);
    expect(result.page).toMatchObject({
      has_more: true,
      next_offset: QUERY_RESULT_PAGE_SIZE,
      loaded_rows: QUERY_RESULT_PAGE_SIZE,
    });
  });

  test('query result page request rejects page sizes over the runtime cap', () => {
    expect(() => queryResultPageRequest({ offset: 0, size: QUERY_RESULT_PAGE_SIZE + 1 })).toThrow(
      /result page size .*maximum/i,
    );
  });

  test('query result page identity rejects loading more after SQL changes', () => {
    const original = queryResultPageRun(
      {
        sql: 'SELECT * FROM events',
        table_name: 'events',
        preferred_target: 'browser_wasm',
        page: { offset: 0, size: QUERY_RESULT_PAGE_SIZE },
      },
      SAMPLE_QUERY_SOURCE,
    );
    const edited = queryResultPageRun(
      {
        sql: 'SELECT id FROM events',
        table_name: 'events',
        preferred_target: 'browser_wasm',
      },
      SAMPLE_QUERY_SOURCE,
    );

    expect(sameQueryResultPageRun(original, edited)).toBe(false);
    expect(
      queryResultPageRunRequest(original, { offset: QUERY_RESULT_PAGE_SIZE, size: 250 }),
    ).toEqual({
      sql: 'SELECT * FROM events',
      table_name: 'events',
      preferred_target: 'browser_wasm',
      page: { offset: QUERY_RESULT_PAGE_SIZE, size: 250 },
    });
  });

  test('load-more control disables after editing SQL for the current result', async ({ page }) => {
    await page.addInitScript(() => {
      type Listener = EventListenerOrEventListenerObject;
      class FakeQueryWorker {
        private listeners = new Map<string, Set<Listener>>();

        addEventListener(type: string, listener: Listener): void {
          const listeners = this.listeners.get(type) ?? new Set<Listener>();
          listeners.add(listener);
          this.listeners.set(type, listeners);
        }

        removeEventListener(type: string, listener: Listener): void {
          this.listeners.get(type)?.delete(listener);
        }

        postMessage(command: unknown): void {
          const payload = command as {
            open_delta_table?: { request_id: string; name: string };
            sql?: {
              request_id: string;
              name: string;
              query?: { options?: { result_page?: { limit?: number; offset?: number } } };
            };
            dispose?: { request_id: string; name: string };
          };
          if (payload.open_delta_table) {
            this.emit({
              opened: {
                request_id: payload.open_delta_table.request_id,
                name: payload.open_delta_table.name,
              },
            });
            return;
          }
          if (payload.dispose) {
            this.emit({
              disposed: {
                request_id: payload.dispose.request_id,
                name: payload.dispose.name,
              },
            });
            return;
          }
          if (payload.sql) {
            const resultPage = payload.sql.query?.options?.result_page;
            const limit = resultPage?.limit ?? 501;
            const offset = resultPage?.offset ?? 0;
            const rows = Array.from({ length: limit }, (_, index) => [offset + index + 1]);
            this.emit({
              success: {
                request_id: payload.sql.request_id,
                response: {
                  executed_on: 'browser_wasm',
                  capabilities: { capabilities: {} },
                  metrics: {
                    bytes_fetched: 0,
                    duration_ms: 0,
                    files_touched: 0,
                    files_skipped: 0,
                    rows_emitted: rows.length,
                  },
                  explain: 'fake editor pagination plan',
                },
                result: {
                  format: 'stream',
                  content_type: 'application/vnd.apache.arrow.stream',
                  bytes: [],
                },
                preview: {
                  columns: ['id'],
                  rows,
                  row_count: rows.length,
                  preview_row_limit: limit,
                  truncated: false,
                },
              },
            });
          }
        }

        terminate(): void {
          this.listeners.clear();
        }

        private emit(data: unknown): void {
          const event = new MessageEvent('message', { data });
          queueMicrotask(() => {
            const listeners = this.listeners.get('message') ?? new Set<Listener>();
            for (const listener of listeners) {
              if (typeof listener === 'function') {
                listener.call(this, event);
              } else {
                listener.handleEvent(event);
              }
            }
          });
        }
      }

      Object.defineProperty(window, 'Worker', {
        configurable: true,
        value: FakeQueryWorker,
      });
    });

    await page.goto('/');

    await page.locator('.code-input').fill('SELECT id FROM axon_prod_like_fixture ORDER BY id');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    const loadNext = page.locator('button[title="Load next result batch"]');
    await expect(loadNext).toBeEnabled({ timeout: 15_000 });

    await page.locator('.code-input').fill('SELECT id FROM axon_prod_like_fixture WHERE id = 1');

    await expect(loadNext).toBeDisabled();
  });

  test('routes between the workspace and connect page', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      const text = msg.text();
      if (
        msg.type() === 'error' &&
        !/WebAssembly compilation aborted: Network error: Response body loading was aborted/i.test(
          text,
        )
      ) {
        consoleErrors.push(text);
      }
    });
    page.on('pageerror', (err) => consoleErrors.push(err.message));

    await page.goto('/connect');
    await expect(page.getByRole('heading', { name: 'Connect a Delta source' })).toBeVisible();

    await page.getByRole('button', { name: /Back to workspace/ }).click();
    await expect(page.locator('.shell .brand-name')).toContainText('axon');
    await expect(page.getByRole('button', { name: /^main$/ })).toHaveCount(0);
    await expect(page.getByRole('button', { name: 'Explain' })).toHaveCount(0);
    await expect(page.locator('button[title="Branch (Git-style versioning)"]')).toHaveCount(0);
    await expect(page.locator('button[title="Explain plan (Phase 2)"]')).toHaveCount(0);
    await expect(page.locator('button[title="Share"]')).toHaveCount(0);

    await page.getByRole('button', { name: 'Connect' }).click();
    await expect(page.getByRole('dialog', { name: 'Connect a Delta source' })).toBeVisible();
    await page.getByRole('button', { name: 'Close (Esc)' }).click();

    await page.goto('/connect');
    await expect(page.getByRole('button', { name: 'Connect a source' })).toBeVisible();
    await page.getByRole('button', { name: 'local folder' }).click();
    await expect(page.getByRole('dialog', { name: 'Connect a local Delta folder' })).toBeVisible();

    expect(consoleErrors, `console errors:\n${consoleErrors.join('\n')}`).toEqual([]);
  });

  test('connect source flows fail closed without browser-owned credentials', async ({ page }) => {
    await page.goto('/connect');

    await expect(page.getByRole('button', { name: 'Unity Catalog' })).toBeDisabled();
    await expect(page.getByRole('button', { name: 'Delta Sharing' })).toBeDisabled();

    await page.getByRole('button', { name: 'Connect a source' }).click();
    const dialog = page.getByRole('dialog', { name: 'Connect a Delta source' });

    await expect(dialog).not.toContainText(/all four sources support the same sql surface area/i);
    await expect(dialog.locator('.cc-source-row', { hasText: 'Object storage' })).toContainText(
      /Access\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-row', { hasText: 'Object storage' })).toContainText(
      /Snapshot\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-row', { hasText: 'Object storage' })).toContainText(
      /Query\s*Browser/i,
    );
    await expect(dialog.locator('.cc-source-row', { hasText: 'Delta Sharing' })).toContainText(
      /Snapshot\s*Browser materialized/i,
    );

    const unityCatalogCard = dialog.locator('.cc-source-row', { hasText: 'Unity Catalog' });
    const deltaSharingCard = dialog.locator('.cc-source-row', { hasText: 'Delta Sharing' });
    await expect(unityCatalogCard).toHaveAttribute('aria-disabled', 'true');
    await expect(deltaSharingCard).toHaveAttribute('aria-disabled', 'true');
    await expect(unityCatalogCard).toContainText(/coming soon/i);
    await expect(deltaSharingCard).toContainText(/coming soon/i);
    await expect(dialog.getByRole('button', { name: /Continue/ })).toBeDisabled();

    await dialog.locator('.cc-source-row', { hasText: 'Local files' }).click();
    await dialog.getByRole('button', { name: /Continue/ }).click();

    const localConfigDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
    await expect(localConfigDialog).toContainText(/Persistent folder access/i);
    await expect(localConfigDialog).not.toContainText(/sandbox|not wired/i);
    await expect(localConfigDialog.getByText(/Delta log parsed/i)).toHaveCount(0);
    await expect(localConfigDialog.getByRole('button', { name: 'Test connection' })).toBeDisabled();
    await expect(localConfigDialog.getByRole('button', { name: /Discover tables/ })).toBeDisabled();
    await localConfigDialog.getByRole('button', { name: 'Back' }).click();

    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

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

  test('local Delta connect prefers persistent browser folder access when supported', async ({
    page,
  }) => {
    await page.addInitScript(() => {
      Object.defineProperty(window, 'showDirectoryPicker', {
        configurable: true,
        value: async () => {
          throw new DOMException('The user aborted a request.', 'AbortError');
        },
      });
    });

    await page.goto('/');
    const localDialog = await openLocalDeltaConnectDialog(page);

    await expect(localDialog).toContainText(/Persistent folder access/i);
    await expect(localDialog).toContainText(/Refresh-ready/i);
    await expect(localDialog.getByLabel('One-session local Delta folder import')).toHaveCount(0);
  });

  test('local Delta file input is labeled as a one-session fallback', async ({ page }) => {
    await installUnavailableDirectoryPicker(page);

    await page.goto('/');
    const localDialog = await openLocalDeltaConnectDialog(page);

    await expect(localDialog).toContainText(/One-session folder import/i);
    await expect(localDialog.getByLabel('One-session local Delta folder import')).toBeVisible();
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

  test('shows all connected catalogs in the sidebar explorer', async ({ page }) => {
    const catalogs = [
      connectedCatalogFixture(),
      connectedCatalogFixture({
        id: 'second-lake-fixture',
        alias: 'second-lake',
        storage: 'gs://axon-second/prod-like-events',
        connectedAt: 'second fixture',
      }),
    ];
    await page.addInitScript((value) => {
      localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
    }, catalogs);

    await page.goto('/');
    await expect(page.locator('.conn-pill')).toContainText('sample-lake', { timeout: 15_000 });

    const sidebar = page.locator('.sidebar');
    await expect(sidebar.locator('.sb-section', { hasText: 'Connected catalogs' })).toContainText(
      '2',
    );
    await expect(sidebar.locator('.sb-row.db')).toContainText(['sample-lake', 'second-lake']);
    await expect(sidebar.locator('.sb-row.tbl', { hasText: 'events' })).toHaveCount(2);

    await sidebar.locator('.sb-row.tbl', { hasText: 'events' }).nth(1).click();
    await expect(page.locator('.conn-pill')).toContainText('second-lake');
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

    await installUnavailableDirectoryPicker(page);
    await page.goto('/');
    await page.getByRole('button', { name: /^Connect$/ }).click();
    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-row', { hasText: 'Local files' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();

    const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
    await localDialog.getByLabel('One-session local Delta folder import').setInputFiles(tableDir);
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

    await connectLocalDeltaFolder(page, tableDir, 'local-prod-like');

    await page.reload();
    await expect(page.locator('.conn-pill')).toContainText('local-prod-like', {
      timeout: 15_000,
    });
    await expect(page.locator('.queryref-bar .qref')).toContainText('axon_prod_like_fixture');
    await expect(page.locator('.queryref-bar')).toContainText(/Reselect folder/i);

    await page
      .locator('.code-input')
      .fill('SELECT COUNT(*) AS row_count FROM axon_prod_like_fixture');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.getByRole('dialog', { name: 'Connect a local Delta folder' })).toBeVisible();
    await expect(page.locator('.results')).not.toContainText(/saved as metadata only/i);
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

    await expect(localDialog).toContainText(/Persistent folder access/i);
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

function connectResultFixture({
  alias,
  source,
  schemaName,
  tableName,
}: {
  alias: string;
  source: ConnectResult['source'];
  schemaName: string;
  tableName: string;
}): ConnectResult {
  return {
    source,
    alias,
    form: {
      path: 'Local folder: local-orders',
      detected: null,
      localDelta:
        source === 'local'
          ? ({
              registryId: 'local-registry-orders',
              persistence: 'metadata_only_reselect',
              storageLabel: 'Local folder: local-orders',
              discovery: { summary: 'Detected 1 Delta table', schemas: [] },
            } as unknown as NonNullable<ConnectResult['form']['localDelta']>)
          : null,
      provider: 'gcs',
      uri: 'gs://acme-lake/silver',
      region: 'us-central1',
      endpoint: 'browser-local',
      uc_mode: 'databricks',
      uc_host: '',
      uc_bff_url: '',
      uc_session_label: '',
      uc_catalog: '',
      uc_schema_filter: '',
      ds_mode: 'profile',
      ds_profile_name: '',
      ds_endpoint: '',
      ds_share: '',
    },
    selection: { [schemaName]: 'all' },
    discovered: {
      summary: 'Detected 1 Delta table',
      schemas: [
        {
          name: schemaName,
          tableCount: 1,
          included: true,
          tables: [
            {
              name: tableName,
              snapshot: 3,
              rows: 6,
              files: 1,
              size: 'fixture',
              protocol: 'r2/w5',
              manifestUrl:
                source === 'object_store'
                  ? '/fixtures/prod-like/delta-log-manifest.json'
                  : undefined,
            },
          ],
        },
      ],
    },
  };
}

function connectedCatalogFixture(overrides: Partial<ConnectedCatalog> = {}): ConnectedCatalog {
  return {
    id: 'sample-lake-fixture',
    alias: 'sample-lake',
    kind: 'object_store',
    provider: 'gcs',
    storage: 'gs://axon-sample/prod-like-events',
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
    ...overrides,
  };
}

async function openLocalDeltaConnectDialog(page: Page) {
  await page.getByRole('button', { name: /^Connect$/ }).click();

  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Local files' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  return page.getByRole('dialog', { name: 'Connect a local Delta folder' });
}

async function connectLocalDeltaFolder(
  page: Page,
  tableDir: string,
  alias: string,
  options: { expectPersisted?: boolean; expectedTable?: string | RegExp } = {},
): Promise<string> {
  await installUnavailableDirectoryPicker(page);
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();

  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Local files' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
  await localDialog.getByLabel('One-session local Delta folder import').setInputFiles(tableDir);
  await expect(localDialog).toContainText(/Delta log parsed/i);
  await localDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(localDialog).toContainText(/source check passed/i);
  await localDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  await expect(reviewDialog).toContainText(/Detected 1 local Delta table/i);
  await setCustomCatalogAlias(reviewDialog, alias);
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

async function setCustomCatalogAlias(reviewDialog: Locator, alias: string): Promise<void> {
  const recommended = reviewDialog.getByLabel('Use recommended organization');
  if (await recommended.isChecked()) await recommended.uncheck();
  await reviewDialog.getByLabel('Catalog alias').fill(alias);
}

async function installUnavailableDirectoryPicker(page: Page): Promise<void> {
  await page.addInitScript(() => {
    Object.defineProperty(window, 'showDirectoryPicker', {
      configurable: true,
      value: undefined,
    });
  });
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
  await expect(localDialog).toContainText(/Reload: refresh-ready directory handle stored/i);
  await localDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(localDialog).toContainText(/source check passed/i);
  await localDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  await expect(reviewDialog).toContainText(/Detected 1 local Delta table/i);
  await setCustomCatalogAlias(reviewDialog, alias);
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
