import { create, toJson } from '@bufbuild/protobuf';
import {
  expect,
  test,
  type ConsoleMessage,
  type Locator,
  type Page,
  type Request,
  type Route,
} from '@playwright/test';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ColumnNodeSchema,
  DeltaProtocolFeatureSchema,
  TableMetadataSchema,
  TableType,
} from '../src/generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import { ExecutionTarget as ContractExecutionTarget } from '../src/generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  buildCatalogFromResult,
  catalogsAvailableForFeatures,
  loadConnectedCatalogs,
  localRegistryIdsForCatalogs,
  upsertConnectedCatalog,
} from '../src/editor/connect/store.ts';
import {
  catalogTablePath,
  catalogTableSqlPath,
  savedQueryPath,
} from '../src/editor/catalog-navigation.ts';
import type { ConnectedCatalog, ConnectResult } from '../src/editor/connect/types.ts';
import {
  resolveQuerySourceSelection,
  SAMPLE_QUERY_SOURCE,
  SAMPLE_QUERY_SOURCE_REF,
} from '../src/services/query-source.ts';
import { connectorFeaturesFromEnv } from '../src/services/connector-features.ts';
import { canonicalTableForSelection } from '../src/services/browser-read-resolution.ts';
import { createPublicObjectStorageCanonicalTable } from '../src/services/canonical-table-identity.ts';
import {
  QUERY_RESULT_PAGE_SIZE,
  browserQueryRequest,
  queryResultPageRequest,
  queryResultPageRun,
  queryResultPageRunRequest,
  resultPageFromPreview,
  sameQueryResultPageRun,
} from '../src/services/query-pagination.ts';

const APP_ORIGIN = new URL(process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5174').origin;

// Console noise that says nothing about the app under test.
const IGNORABLE_CONSOLE_ERRORS = [
  // Cancelling an in-flight WASM fetch during navigation.
  /WebAssembly compilation aborted: Network error: Response body loading was aborted/i,
  // Vercel Analytics is injected into production builds and served only by Vercel's edge, so it
  // 404s whenever a production build is exercised anywhere else.
  /_vercel\/insights/i,
];

// A failed subresource logs only "Failed to load resource: ... 404 ()", so the URL that identifies
// it lives in the message location rather than the text.
function isIgnorableConsoleError(message: ConsoleMessage): boolean {
  const text = message.text();
  const url = message.location().url;
  return IGNORABLE_CONSOLE_ERRORS.some((pattern) => pattern.test(text) || pattern.test(url));
}
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

  test('workspace settings surface replaces the floating Tweaks panel', () => {
    const source = readFileSync(new URL('../src/editor/App.tsx', import.meta.url), 'utf8');

    expect(source).not.toContain("from './tweaks/TweaksPanel.tsx'");
    expect(source).not.toContain('<TweaksPanel');
    expect(existsSync(new URL('../src/editor/tweaks/TweaksPanel.tsx', import.meta.url))).toBe(
      false,
    );
  });

  test('editor shell mounts Vercel Web Analytics once at the app root', () => {
    const source = readFileSync(new URL('../src/editor/main.tsx', import.meta.url), 'utf8');

    expect(source).toContain("import { Analytics } from '@vercel/analytics/react';");
    expect(source).toContain('<Analytics />');
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

  test('connected catalog store keeps same-alias generated connections distinct', () => {
    const first = connectedCatalogFixture({
      alias: 'workspace',
      storage: 'gs://first-lake/events',
    });
    const second = connectedCatalogFixture({
      alias: 'workspace',
      storage: 'gs://second-lake/events',
    });

    const result = upsertConnectedCatalog([first], second);

    expect(result.catalogs).toHaveLength(2);
    expect(result.catalogs.map((catalog) => catalog.alias)).toEqual(['workspace', 'workspace']);
    expect(result.catalogs.map((catalog) => catalog.id).sort()).toEqual([
      'axon-connection://public-gcs/first-lake',
      'axon-connection://public-gcs/second-lake',
    ]);
    const selected = second.schemas[0]!.tables[0]!.logicalTable!;
    const selection = resolveQuerySourceSelection(result.catalogs, selected);
    expect(selection).toMatchObject({
      kind: 'resource',
      source: {
        catalogName: 'workspace',
        schemaName: 'prod_like',
        tableName: 'events',
        storage: 'gs://second-lake/events',
      },
    });
  });

  test('connector feature gates filter disabled connections without mixing their tables', () => {
    const local = buildCatalogFromResult(
      connectResultFixture({
        alias: 'local',
        source: 'local',
        schemaName: 'default',
        tableName: 'local_orders',
      }),
    );
    const governed = buildCatalogFromResult(
      connectResultFixture({
        alias: 'governed',
        source: 'unity_catalog',
        schemaName: 'default',
        tableName: 'governed_orders',
      }),
    );

    const filtered = catalogsAvailableForFeatures([local, governed], {
      bffAuthServiceConnectors: false,
    });

    expect(filtered).toHaveLength(1);
    expect(filtered[0].schemas[0]?.tables.map((table) => table.name)).toEqual(['local_orders']);
  });

  test('local registry cleanup derives ids from the exact local connection', () => {
    const catalog = buildCatalogFromResult(
      connectResultFixture({
        alias: 'local',
        source: 'local',
        schemaName: 'default',
        tableName: 'local_orders',
      }),
    );

    expect(localRegistryIdsForCatalogs([catalog])).toEqual(['local-registry-orders']);
  });

  test('reconnecting one generated connection keeps incoming tables canonically addressable', () => {
    const existing = connectedCatalogFixture({
      alias: 'workspace',
      storage: 'gs://acme-lake/orders',
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
              uri: 'gs://acme-lake/orders',
            },
          ],
        },
      ],
    });
    const incoming = connectedCatalogFixture({
      alias: 'renamed workspace',
      storage: 'gs://acme-lake/events',
      schemas: [
        {
          name: 'analytics',
          tables: [
            {
              name: 'events',
              snapshot: 4,
              rows: 7,
              files: 2,
              size: 'fixture',
              protocol: 'r2/w5',
              uri: 'gs://acme-lake/events',
            },
          ],
        },
      ],
    });

    const result = upsertConnectedCatalog([existing], incoming);
    const merged = result.catalogs[0];

    expect(result.catalogs).toHaveLength(1);
    expect(merged.id).toBe('axon-connection://public-gcs/acme-lake');
    expect(merged.alias).toBe('renamed workspace');
    expect(
      resolveQuerySourceSelection(
        result.catalogs,
        merged.schemas
          .find((schema) => schema.name === 'analytics')!
          .tables.find((table) => table.name === 'events')!.logicalTable!,
      ),
    ).toMatchObject({
      kind: 'resource',
      source: {
        catalogName: 'renamed workspace',
        schemaName: 'analytics',
        tableName: 'events',
        storage: 'gs://acme-lake/events',
      },
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

  test('connected catalog store keeps the newest presentation for each canonical connection', () => {
    const original = connectedCatalogFixture({
      alias: 'old-acme',
      storage: 'gs://acme-lake/silver',
    });
    const other = connectedCatalogFixture({
      alias: 'other-lake',
      storage: 'gs://other-lake/silver',
    });
    const updated = connectedCatalogFixture({
      alias: 'new-acme',
      storage: 'gs://acme-lake/silver',
    });
    const previousLocalStorage = globalThis.localStorage;
    const storage = new Map<string, string>([
      ['axon.connect.catalogs.v1', JSON.stringify([updated, original, other])],
    ]);

    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => storage.set(key, value),
      },
    });

    try {
      expect(
        upsertConnectedCatalog([original, other], updated).catalogs.map((catalog) => catalog.id),
      ).toEqual([
        'axon-connection://public-gcs/acme-lake',
        'axon-connection://public-gcs/other-lake',
      ]);
      expect(loadConnectedCatalogs().map((catalog) => catalog.id)).toEqual([
        'axon-connection://public-gcs/acme-lake',
        'axon-connection://public-gcs/other-lake',
      ]);
    } finally {
      if (previousLocalStorage === undefined) {
        Reflect.deleteProperty(globalThis, 'localStorage');
      } else {
        Object.defineProperty(globalThis, 'localStorage', {
          configurable: true,
          value: previousLocalStorage,
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

    expect(workerPage).toMatchObject({
      limit: BigInt(QUERY_RESULT_PAGE_SIZE + 1),
      offset: 500n,
    });
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
    const selection = {
      kind: 'sample',
      ref: SAMPLE_QUERY_SOURCE_REF,
      source: SAMPLE_QUERY_SOURCE,
    } as const;
    const table = canonicalTableForSelection(selection);
    const original = queryResultPageRun(
      table,
      browserQueryRequest({
        sql: 'SELECT * FROM events',
        page: { offset: 0, size: QUERY_RESULT_PAGE_SIZE },
        preferredTarget: ContractExecutionTarget.BROWSER_WASM,
      }),
      selection,
    );
    const edited = queryResultPageRun(
      table,
      browserQueryRequest({
        sql: 'SELECT id FROM events',
        preferredTarget: ContractExecutionTarget.BROWSER_WASM,
      }),
      selection,
    );

    expect(sameQueryResultPageRun(original, edited)).toBe(false);
    expect(
      queryResultPageRunRequest(original, { offset: QUERY_RESULT_PAGE_SIZE, size: 250 }),
    ).toMatchObject({
      sql: 'SELECT * FROM events',
      preferredTarget: ContractExecutionTarget.BROWSER_WASM,
      options: {
        resultPage: { offset: BigInt(QUERY_RESULT_PAGE_SIZE), limit: 251n },
      },
    });
  });

  test('load-more control disables after editing SQL for the current result', async ({ page }) => {
    await installFakePaginationWorker(page);

    await page.goto('/');

    await page.locator('.code-input').fill('SELECT id FROM axon_prod_like_fixture ORDER BY id');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    const loadNext = page.locator('button[title="Load next result batch"]');
    await expect(loadNext).toBeEnabled({ timeout: 15_000 });

    await page.locator('.code-input').fill('SELECT id FROM axon_prod_like_fixture WHERE id = 1');

    await expect(loadNext).toBeDisabled();
  });

  test('scrolling near the loaded result bottom automatically loads the next batch', async ({
    page,
  }) => {
    await installFakePaginationWorker(page);
    await page.goto('/');

    await page.locator('.code-input').fill('SELECT id FROM axon_prod_like_fixture ORDER BY id');
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText('500 rows+', { timeout: 15_000 });

    await page.locator('.table-wrap').evaluate((node) => {
      node.scrollTop = node.scrollHeight;
      node.dispatchEvent(new Event('scroll', { bubbles: true }));
    });

    await expect(page.locator('.res-meta')).toContainText('1,000 rows+', { timeout: 15_000 });
  });

  test('routes between the workspace and connect page', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      const text = msg.text();
      if (msg.type() === 'error' && !isIgnorableConsoleError(msg)) {
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

  test('catalog routes support explorer navigation, reload, and browser history', async ({
    page,
  }) => {
    const catalogs = [
      connectedCatalogFixture(),
      connectedCatalogFixture({
        id: 'second-lake-fixture',
        alias: 'second-lake',
        storage: 'gs://axon-second/prod-like-events',
        connectedAt: 'second fixture',
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
                uri: 'gs://axon-second/prod-like-events',
              },
            ],
          },
        ],
      }),
    ];
    await page.addInitScript((value) => {
      localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
    }, catalogs);

    const secondTable = createPublicObjectStorageCanonicalTable({
      provider: 'gcs',
      connectionId: 'axon-connection://public-gcs/axon-second',
      normalizedTableUri: 'gs://axon-second/prod-like-events',
      tableName: 'events',
    });
    const secondTablePath = catalogTablePath(secondTable);
    const secondTableSqlPath = catalogTableSqlPath(secondTable);

    await page.goto('/catalogs');
    await expect(page.locator('.catalogs-title')).toContainText('Catalog Explorer');
    await expect(page.locator('.catalogs-stats')).toContainText('2 connections');
    await expect(page.getByRole('heading', { name: 'Select a table or view' })).toBeVisible();
    await expect(page.locator('.catalog-table-row.active')).toHaveCount(0);
    await page
      .locator('.catalog-block', { hasText: 'second-lake' })
      .locator('.catalog-table-row', { hasText: 'events' })
      .click();

    await expect(page).toHaveURL(new RegExp(`${secondTablePath}$`));
    await expect(
      page
        .locator('.catalog-block', { hasText: 'second-lake' })
        .locator('.catalog-table-row', { hasText: 'events' }),
    ).toHaveClass(/active/);

    await page.reload();
    await expect(
      page
        .locator('.catalog-block', { hasText: 'second-lake' })
        .locator('.catalog-table-row', { hasText: 'events' }),
    ).toHaveClass(/active/);

    await page.goBack();
    await expect(page.locator('.catalogs-title')).toContainText('Catalog Explorer');

    await page.goForward();
    await expect(page).toHaveURL(new RegExp(`${secondTablePath}$`));

    await page.goto(secondTableSqlPath);
    await expect(page.locator('.conn-pill')).toContainText('second-lake');
  });

  test('catalog explorer renders generated overview, columns, views, and logical SQL handoff', async ({
    page,
  }) => {
    const catalog = catalogExplorerFixture();
    await page.addInitScript(
      (value) => {
        localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
      },
      [catalog],
    );
    const table = catalog.schemas[0]!.tables.find(({ name }) => name === 'orders')!.logicalTable!;
    const view = catalog.schemas[0]!.tables.find(
      ({ name }) => name === 'weekly_orders',
    )!.logicalTable!;
    const metadataMissing = catalog.schemas[0]!.tables.find(
      ({ name }) => name === 'metadata_missing',
    )!.logicalTable!;
    const requests = trackRelevantRequests(page);

    await page.goto(catalogTablePath(table));

    await expect(page.getByRole('heading', { name: 'orders' })).toBeVisible();
    await expect(page.locator('.catalog-kind-badge')).toHaveText('Table');
    await expect(page.getByText('Generated order facts for Explorer QA.')).toBeVisible();
    await expect(page.locator('.catalog-overview')).toContainText('gs://axon-explorer/orders');
    await expect(page.locator('.catalog-overview')).toContainText('r2/w7');
    await expect(page.locator('.catalog-overview')).toContainText('deletionVectors');
    await expect(page.locator('.catalog-overview')).toContainText('order_date');
    const columns = page.getByRole('table', { name: 'Columns for orders' });
    await expect(columns.getByRole('row')).toHaveCount(3);
    await expect(
      columns.getByRole('row', { name: /order_id bigint No No primary key/i }),
    ).toBeVisible();
    await expect(columns.getByRole('row', { name: /order_date date No Yes/i })).toBeVisible();

    const beforeSqlHandoff = requests.length;
    await page.getByRole('button', { name: 'Open in SQL editor' }).click();
    await expect(page).toHaveURL(new RegExp(`${catalogTableSqlPath(table)}$`));
    await expect(page.locator('.conn-pill')).toContainText('explorer-lake');
    const handoffRequests = requests.slice(beforeSqlHandoff);
    expect(handoffRequests.filter((request) => request.resourceType === 'worker')).toEqual([]);
    expectRequestLogExcludes(handoffRequests, [
      '_delta_log',
      'sandbox-query-worker',
      'axon_web_wasm_bg.wasm',
    ]);

    await page.goto(catalogTablePath(view));
    await expect(page.getByRole('heading', { name: 'weekly_orders' })).toBeVisible();
    await expect(page.locator('.catalog-kind-badge')).toHaveText('View');
    await expect(page.getByText(/browseable but not queryable/i)).toBeVisible();
    await expect(page.getByRole('button', { name: 'Open in SQL editor' })).toHaveCount(0);

    await page.goto(catalogTablePath(metadataMissing));
    await expect(page.getByRole('heading', { name: 'Metadata unavailable' })).toBeVisible();
    await expect(page.getByText(/did not report generated metadata/i)).toBeVisible();
  });

  test('catalog explorer distinguishes an empty connection state', async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('axon.connect.catalogs.v1', '[]');
    });

    await page.goto('/catalogs');

    await expect(page.getByRole('heading', { name: 'No connections available' })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Connect a source' })).toBeVisible();
  });

  test('canonical SQL route fails closed when its exact connection is disconnected', async ({
    page,
  }) => {
    const catalog = connectedCatalogFixture({
      alias: 'disconnect-lake',
      storage: 'gs://axon-disconnect/events',
    });
    const table = catalog.schemas[0]!.tables[0]!.logicalTable!;
    await page.addInitScript(
      (value) => {
        localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
      },
      [catalog],
    );

    await page.goto(catalogTableSqlPath(table));
    await expect(page.locator('.conn-pill')).toContainText('disconnect-lake');
    await page.locator('.conn-pill').click();
    const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
    await panel.getByTitle('Manage connection').click();
    await panel.getByRole('button', { name: 'Disconnect catalog' }).click();

    await expect(page.getByRole('heading', { name: 'Table route unavailable' })).toBeVisible();
    await expect(page.getByText(/no longer connected/i)).toBeVisible();
    await expect
      .poll(() =>
        page.evaluate(() => JSON.parse(localStorage.getItem('axon.connect.catalogs.v1') ?? '[]')),
      )
      .toEqual([]);
  });

  test('invalid catalog table routes render a catalog recovery action', async ({ page }) => {
    await page.addInitScript(
      (value) => {
        localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
      },
      [connectedCatalogFixture()],
    );

    await page.goto('/catalog/sample-lake-fixture/prod_like/missing');

    await expect(
      page.getByRole('heading', { name: 'Legacy table route unavailable' }),
    ).toBeVisible();
    await page.getByRole('button', { name: 'View catalogs' }).click();
    await expect(page).toHaveURL(/\/catalogs$/);
  });

  test('saved query routes open saved tabs and report missing ids', async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem(
        'axon-editor.saved.v1',
        JSON.stringify([
          {
            id: 'saved-route-1',
            name: 'saved route',
            owner: 'you',
            edited: '10:30',
            target: 'browser_wasm',
            sql: 'SELECT 1 AS saved_route',
          },
        ]),
      );
    });

    await page.goto(savedQueryPath('saved-route-1'));

    await expect(page.locator('.qtab.active')).toContainText('saved route.sql');
    await expect(page.locator('.code-input')).toHaveValue('SELECT 1 AS saved_route');

    await page.goto('/saved/missing');
    await expect(page.getByRole('heading', { name: 'Saved query not found' })).toBeVisible();
  });

  test('lazy startup defers query runtime requests until the first workspace query', async ({
    page,
  }) => {
    const requests = trackRelevantRequests(page);

    await page.goto('/');
    await expect(page.locator('.shell .brand-name')).toContainText('axon');
    await expect(page.locator('.queryref-bar .qref')).toContainText('events');

    const initial = requests.slice();
    expectRequestLogExcludes(initial, [
      'axon_web_wasm_bg.wasm',
      'sandbox-query-worker',
      '/src/services/query.ts',
      '/src/services/local-delta.ts',
      '/src/wasm/',
    ]);
    expect(initial.filter((request) => request.resourceType === 'worker')).toEqual([]);

    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 30_000,
    });

    await expect
      .poll(() => ({
        queryRuntime: requests.some((request) => request.url.includes('/src/services/query.ts')),
        worker: requests.some(
          (request) =>
            request.resourceType === 'worker' || request.url.includes('sandbox-query-worker'),
        ),
        wasm: requests.some(
          (request) =>
            request.url.includes('/src/wasm/') || request.url.includes('axon_web_wasm_bg.wasm'),
        ),
      }))
      .toEqual({ queryRuntime: true, worker: true, wasm: true });
  });

  test('lazy startup keeps connect query and WASM runtimes deferred to validation actions', async ({
    page,
  }) => {
    const requests = trackRelevantRequests(page);
    await page.route('https://storage.googleapis.com/**', async (route) => {
      await route.fulfill({
        status: 404,
        contentType: 'application/xml',
        headers: { 'access-control-allow-origin': APP_ORIGIN },
        body: '<Error><Code>NoSuchBucket</Code></Error>',
      });
    });

    await page.goto('/connect');
    await expect(page.getByRole('heading', { name: 'Connect a Delta source' })).toBeVisible();

    const initial = requests.slice();
    expectRequestLogExcludes(initial, [
      '/src/editor/App.tsx',
      '/src/services/query.ts',
      'sandbox-query-worker',
      '/src/services/local-delta.ts',
      '/src/wasm/',
      'axon_web_wasm_bg.wasm',
    ]);
    expect(initial.filter((request) => request.resourceType === 'worker')).toEqual([]);

    await page.getByRole('button', { name: 'Connect a source' }).click();
    await expect(page.getByRole('dialog', { name: 'Connect a Delta source' })).toBeVisible();

    const afterModal = requests.slice(initial.length);
    expect(
      afterModal.some((request) => request.url.includes('/src/editor/connect/ConnectModal.tsx')),
    ).toBe(true);
    expectRequestLogExcludes(afterModal, [
      '/src/services/local-delta.ts',
      '/src/wasm/',
      'axon_web_wasm_bg.wasm',
    ]);

    const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
    await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
    await sourceDialog.getByRole('button', { name: /Continue/ }).click();
    const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
    await expect(configDialog).toBeVisible();

    const beforeValidationCount = requests.length;
    await configDialog.getByRole('button', { name: 'Test connection' }).click();
    await expect(configDialog).toContainText(
      /not configured|failed|NoSuchBucket|public GCS|public object storage/i,
    );

    const validationRequests = requests.slice(beforeValidationCount);
    expect(
      validationRequests.some(
        (request) =>
          request.url.includes('/src/wasm/') || request.url.includes('axon_web_wasm_bg.wasm'),
      ),
    ).toBe(true);
  });

  test('persists client appearance settings from the routed settings surface across reloads', async ({
    page,
  }) => {
    await page.goto('/');
    await expect(page.locator('.shell .brand-name')).toContainText('axon');

    await page.getByRole('button', { name: 'Open settings' }).click();
    await expect(page).toHaveURL(/\/settings$/);
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
    await page
      .getByRole('radiogroup', { name: 'Mode' })
      .getByRole('radio', { name: 'dark' })
      .click();
    await page
      .getByRole('radiogroup', { name: 'Accent' })
      .getByRole('radio', { name: '#0F9D74' })
      .click();
    await page
      .getByRole('radiogroup', { name: 'Density' })
      .getByRole('radio', { name: 'comfy' })
      .click();
    await page.getByRole('combobox', { name: 'UI font' }).selectOption('IBM Plex Sans');
    await page.getByRole('combobox', { name: 'Code font' }).selectOption('Fira Code');

    await expect
      .poll(() =>
        page.evaluate(() => ({
          theme: document.documentElement.getAttribute('data-theme'),
          density: document.documentElement.getAttribute('data-density'),
          accent: document.documentElement.style.getPropertyValue('--accent').trim(),
          uiFont: document.documentElement.style.getPropertyValue('--ui'),
          monoFont: document.documentElement.style.getPropertyValue('--mono'),
        })),
      )
      .toMatchObject({
        theme: 'dark',
        density: 'comfy',
        accent: '#0F9D74',
        uiFont: expect.stringContaining('IBM Plex Sans'),
        monoFont: expect.stringContaining('Fira Code'),
      });

    const persistedBeforeReload = await page.evaluate(() => {
      const raw = localStorage.getItem('axon.client-state.v1');
      const parsed = raw ? (JSON.parse(raw) as { state?: Record<string, unknown> }) : null;
      return {
        legacyTweaks: localStorage.getItem('axon-editor.tweaks.v1'),
        topLevelKeys: Object.keys(parsed?.state ?? {}).sort(),
        raw,
      };
    });
    expect(persistedBeforeReload.legacyTweaks).toBeNull();
    expect(persistedBeforeReload.topLevelKeys).toEqual(['layout', 'settings', 'tabs']);
    expect(persistedBeforeReload.raw).toContain('"theme":"dark"');
    expect(persistedBeforeReload.raw).toContain('"density":"comfy"');
    expect(persistedBeforeReload.raw).toContain('"accent":"#0F9D74"');
    expect(persistedBeforeReload.raw).toContain('"uiFont":"IBM Plex Sans"');
    expect(persistedBeforeReload.raw).toContain('"monoFont":"Fira Code"');

    await page.reload();
    await expect(page).toHaveURL(/\/settings$/);
    await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();

    await expect
      .poll(() =>
        page.evaluate(() => ({
          theme: document.documentElement.getAttribute('data-theme'),
          density: document.documentElement.getAttribute('data-density'),
          accent: document.documentElement.style.getPropertyValue('--accent').trim(),
          uiFont: document.documentElement.style.getPropertyValue('--ui'),
          monoFont: document.documentElement.style.getPropertyValue('--mono'),
        })),
      )
      .toMatchObject({
        theme: 'dark',
        density: 'comfy',
        accent: '#0F9D74',
        uiFont: expect.stringContaining('IBM Plex Sans'),
        monoFont: expect.stringContaining('Fira Code'),
      });

    await expect(
      page.getByRole('radiogroup', { name: 'Mode' }).getByRole('radio', { name: 'dark' }),
    ).toHaveAttribute('aria-checked', 'true');
    await expect(page.getByRole('combobox', { name: 'UI font' })).toHaveValue('IBM Plex Sans');
    await expect(page.getByRole('combobox', { name: 'Code font' })).toHaveValue('Fira Code');
  });

  test('connect source flows stay browser-owned without private credentials', async ({ page }) => {
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
      /public GCS or S3/i,
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
    const s3Provider = configDialog.getByRole('button', { name: /AWS S3/ });
    await expect(s3Provider).toBeEnabled();
    await s3Provider.click();
    await expect(configDialog.locator('.prefix')).toHaveText('s3://');
    await expect(configDialog.locator('select.cc-select')).toHaveValue('us-east-1');
    await expect(configDialog.locator('select.cc-select option[value=""]')).toHaveCount(0);
    await configDialog.getByRole('button', { name: /Google Cloud Storage/ }).click();
    await expect(configDialog.locator('.prefix')).toHaveText('gs://');
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
    await expect(reviewDialog).toContainText(/Detected 1 (?:public Delta|catalog) table/i);
    await expect(reviewDialog).toContainText(/silver/i);
    await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();

    const persisted = await page.evaluate(
      () => localStorage.getItem('axon.connect.catalogs.v1') ?? '',
    );
    expect(persisted).toContain('gs://acme-lake/silver');
    expect(persisted).not.toContain('storage.googleapis.com');
    expect(persisted).not.toContain('X-Goog');
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
      connectedCatalogFixture(),
      connectedCatalogFixture({
        alias: 'second-lake',
        storage: 'gs://axon-second/prod-like-events',
        connectedAt: 'test fixture',
      }),
    ];
    const secondTable = catalogs[1].schemas[0]!.tables[0]!.logicalTable!;
    const secondTablePath = catalogTableSqlPath(secondTable);
    await page.addInitScript((value) => {
      localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
    }, catalogs);

    await page.goto('/');
    await expect(page.locator('.conn-pill')).toContainText('Select table', { timeout: 15_000 });
    await expect(page.locator('.btn.primary', { hasText: 'Run' })).toBeDisabled();

    await activateConnectedTable(page, 'second-lake', 'prod_like', 'events');

    await expect(page).toHaveURL(new RegExp(`${secondTablePath}$`));
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
    await expect(page.locator('.conn-pill')).toContainText('Select table', { timeout: 15_000 });

    const sidebar = page.locator('.sidebar');
    await expect(sidebar.locator('.sb-section', { hasText: 'Connected catalogs' })).toContainText(
      '2',
    );
    await expect(sidebar.locator('.sb-row.db')).toContainText(['sample-lake', 'second-lake']);
    await expect(sidebar.locator('.sb-row.tbl', { hasText: 'events' })).toHaveCount(2);

    await sidebar.locator('.sb-row.tbl', { hasText: 'events' }).nth(1).click();
    await expect(page.locator('.conn-pill')).toContainText('second-lake');
  });

  test('workspace pickers navigate public object-store table roots without manifests', async ({
    page,
  }) => {
    const publicRoot = publicObjectStoreTableRootCatalogFixture();
    const publicRootPath = catalogTableSqlPath(
      createPublicObjectStorageCanonicalTable({
        provider: 'gcs',
        connectionId: 'axon-connection://public-gcs/axon-public',
        normalizedTableUri: 'gs://axon-public/direct-events',
        tableName: 'events',
      }),
    );
    const samplePath = catalogTableSqlPath(SAMPLE_QUERY_SOURCE_REF);
    await page.addInitScript(
      (value) => {
        localStorage.setItem('axon.connect.catalogs.v1', JSON.stringify(value));
      },
      [connectedCatalogFixture(), publicRoot],
    );

    await page.goto('/');
    await expect(page.locator('.conn-pill')).toContainText('Select table', { timeout: 15_000 });

    const sidebar = page.locator('.sidebar');
    const publicSidebarRow = sidebar.locator('.sb-row.tbl', { hasText: 'events' }).nth(1);
    await expect(publicSidebarRow).toHaveAttribute('aria-disabled', 'false');
    await publicSidebarRow.click();

    await expect(page).toHaveURL(new RegExp(`${publicRootPath}$`));
    await expect(page.locator('.conn-pill')).toContainText('public-root');

    await page.goto(samplePath);
    await expect(page.locator('.conn-pill')).toContainText('sample-lake', { timeout: 15_000 });

    await page.locator('.conn-pill').click();
    const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
    await panel.getByRole('button', { name: /Expand public-root/ }).click();
    const publicPanelRow = panel.getByRole('button', {
      name: /Activate public-root default events/,
    });
    await expect(publicPanelRow).toBeEnabled();
    await publicPanelRow.click();

    await expect(page).toHaveURL(new RegExp(`${publicRootPath}$`));
    await expect(page.locator('.conn-pill')).toContainText('public-root');
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
    expect(connectState).not.toMatch(
      /ArrayBuffer|signed[_-]?url|descriptor|bearer|token|client[_-]?secret|credential|grant|session[_-]?value/i,
    );

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
      parseTimeoutMs: 120_000,
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

  test('queries the complex Delta feature stress table in browser WASM', async ({ page }) => {
    const tableDir = process.env.AXON_COMPLEX_DELTA_PATH;
    if (!tableDir) {
      test.skip(
        true,
        'Set AXON_COMPLEX_DELTA_PATH=/Users/ethanurbanski/axon/.generated-delta/query-engine-complex-features-delta to run this local smoke.',
      );
      return;
    }

    await connectLocalDeltaFolder(page, tableDir, 'complex-local', {
      expectedTable: 'query_engine_complex_features_delta',
      parseTimeoutMs: 120_000,
    });

    await page
      .locator('.code-input')
      .fill(
        "SELECT event_id, event_date, event_ts, region, ingest_bucket, status FROM query_engine_complex_features_delta WHERE region = 'us-east' AND ingest_bucket = 1 LIMIT 10",
      );
    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, {
      timeout: 120_000,
    });
    await expect(page.locator('table.grid tbody tr')).toHaveCount(10, { timeout: 120_000 });
    await expect(page.locator('table.grid')).toContainText('event_date');
    await expect(page.locator('table.grid')).toContainText('event_ts');
    await expect(page.locator('table.grid')).toContainText('ingest_bucket');
    await expect(page.locator('table.grid')).toContainText('us-east');
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

    await expect(page.locator('.conn-pill')).toContainText('Select table');
    await expect(page.locator('.btn.primary', { hasText: 'Run' })).toBeDisabled();
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
    await expect(page.locator('.conn-pill')).toContainText('Select table', { timeout: 15_000 });
    await activateConnectedTable(page, 'local-prod-like', 'default', 'axon_prod_like_fixture');
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
    await expect(page.locator('.conn-pill')).toContainText('Select table', { timeout: 15_000 });
    await activateConnectedTable(page, 'handle-local', 'default', 'axon_prod_like_fixture');
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

  test('unexpected local Delta registry errors surface when querying instead of using fallback catalog metadata', async ({
    page,
  }) => {
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

    await page.locator('.btn.primary', { hasText: 'Run' }).click();

    await expect(page.locator('.res-meta')).toContainText(/rejected/i, { timeout: 15_000 });
    await expect(page.locator('.results')).toContainText('registry boom');
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
      const text = msg.text();
      if (msg.type() === 'error' && !isIgnorableConsoleError(msg)) consoleErrors.push(text);
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

    const connectState = await page.evaluate(() =>
      localStorage.getItem('axon.connect.catalogs.v1'),
    );
    expect(connectState).toBeNull();

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
    await expect(page.locator('.res-meta')).toContainText('failed', { timeout: 30_000 });
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
      objectStorage: null,
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
  const alias = overrides.alias ?? 'sample-lake';
  const storage = overrides.storage ?? 'gs://axon-sample/prod-like-events';
  const isSample = alias === 'sample-lake' && storage === 'gs://axon-sample/prod-like-events';
  const defaultTable = isSample
    ? SAMPLE_QUERY_SOURCE_REF
    : createPublicObjectStorageCanonicalTable({
        provider: 'gcs',
        connectionId: `axon-connection://public-gcs/${new URL(storage).hostname}`,
        normalizedTableUri: storage,
        tableName: 'events',
      });
  const schemas = (
    overrides.schemas ?? [
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
            manifestUrl: isSample ? '/fixtures/prod-like/delta-log-manifest.json' : undefined,
            uri: isSample ? undefined : storage,
          },
        ],
      },
    ]
  ).map((schema) => ({
    ...schema,
    tables: schema.tables.map((table) => {
      const logicalTable =
        table.logicalTable ??
        (isSample
          ? SAMPLE_QUERY_SOURCE_REF
          : createPublicObjectStorageCanonicalTable({
              provider: 'gcs',
              connectionId: defaultTable.resource!.connectionId,
              normalizedTableUri: table.uri ?? storage,
              tableName: table.name,
            }));
      return {
        ...table,
        logicalTable,
        catalogMetadataJson:
          table.catalogMetadataJson ??
          (isSample
            ? undefined
            : (toJson(
                TableMetadataSchema,
                create(TableMetadataSchema, {
                  table: logicalTable,
                  storageLocation: table.uri ?? storage,
                  latestSnapshotVersion: BigInt(table.snapshot ?? 0),
                  rowCount: BigInt(table.rows ?? 0),
                  fileCount: BigInt(table.files ?? 0),
                  minReaderVersion: 2,
                  minWriterVersion: 5,
                }),
              ) as Readonly<Record<string, unknown>>)),
      };
    }),
  }));
  return {
    kind: 'object_store',
    provider: 'gcs',
    region: 'browser-local',
    status: 'connected',
    connectedAt: 'test fixture',
    ...overrides,
    id: defaultTable.resource!.connectionId,
    catalogName: isSample ? 'sample-lake' : 'public-gcs',
    alias,
    storage,
    schemas,
  };
}

function catalogExplorerFixture(): ConnectedCatalog {
  const catalog = connectedCatalogFixture({
    alias: 'explorer-lake',
    storage: 'gs://axon-explorer/orders',
    schemas: [
      {
        name: 'analytics',
        tables: [
          {
            name: 'orders',
            snapshot: 14,
            rows: 1_250,
            files: 8,
            size: '4 KiB',
            protocol: 'r2/w7',
            uri: 'gs://axon-explorer/orders',
          },
        ],
      },
    ],
  });
  const schema = catalog.schemas[0]!;
  const orders = schema.tables[0]!;
  orders.logicalTable!.comment = 'Generated order facts for Explorer QA.';
  orders.catalogMetadataJson = toJson(
    TableMetadataSchema,
    create(TableMetadataSchema, {
      table: orders.logicalTable,
      columns: [
        create(ColumnNodeSchema, {
          name: 'order_id',
          type: 'bigint',
          nullable: false,
          comment: 'primary key',
        }),
        create(ColumnNodeSchema, {
          name: 'order_date',
          type: 'date',
          nullable: false,
        }),
      ],
      partitionColumns: ['order_date'],
      rowCount: 1_250n,
      sizeBytes: 4_096n,
      fileCount: 8n,
      latestSnapshotVersion: 14n,
      minReaderVersion: 2,
      minWriterVersion: 7,
      protocolFeatures: [
        create(DeltaProtocolFeatureSchema, {
          name: 'deletionVectors',
          reader: true,
          writer: true,
        }),
      ],
      storageLocation: 'gs://axon-explorer/orders',
    }),
  ) as Readonly<Record<string, unknown>>;

  const connectionId = orders.logicalTable!.resource!.connectionId;
  const view = createPublicObjectStorageCanonicalTable({
    provider: 'gcs',
    connectionId,
    normalizedTableUri: 'gs://axon-explorer/weekly-orders',
    tableName: 'weekly_orders',
  });
  view.tableType = TableType.VIEW;
  view.comment = 'A generated weekly view.';
  schema.tables.push({
    name: view.name,
    snapshot: 14,
    rows: 50,
    files: 0,
    size: 'logical',
    protocol: 'r2/w7',
    uri: 'gs://axon-explorer/weekly-orders',
    logicalTable: view,
    catalogMetadataJson: toJson(
      TableMetadataSchema,
      create(TableMetadataSchema, {
        table: view,
        storageLocation: 'gs://axon-explorer/weekly-orders',
      }),
    ) as Readonly<Record<string, unknown>>,
  });

  const metadataMissing = createPublicObjectStorageCanonicalTable({
    provider: 'gcs',
    connectionId,
    normalizedTableUri: 'gs://axon-explorer/metadata-missing',
    tableName: 'metadata_missing',
  });
  schema.tables.push({
    name: metadataMissing.name,
    snapshot: 0,
    rows: 0,
    files: 0,
    size: 'not reported',
    protocol: 'not reported',
    uri: 'gs://axon-explorer/metadata-missing',
    logicalTable: metadataMissing,
  });
  return catalog;
}

function publicObjectStoreTableRootCatalogFixture(): ConnectedCatalog {
  return connectedCatalogFixture({
    id: 'public-root-fixture',
    alias: 'public-root',
    storage: 'gs://axon-public/direct-events',
    connectedAt: 'public root fixture',
    schemas: [
      {
        name: 'default',
        tables: [
          {
            name: 'events',
            snapshot: 4,
            rows: 8,
            files: 1,
            size: 'fixture',
            protocol: 'r2/w5',
            uri: 'gs://axon-public/direct-events',
          },
        ],
      },
    ],
  });
}

async function activateConnectedTable(
  page: Page,
  catalogAlias: string,
  schemaName: string,
  tableName: string,
): Promise<void> {
  await page.locator('.conn-pill').click();
  const panel = page.getByRole('dialog', { name: 'Connected catalogs' });
  const expand = panel.getByRole('button', { name: `Expand ${catalogAlias}`, exact: true });
  if ((await expand.count()) > 0) await expand.click();
  await panel
    .getByRole('button', {
      name: `Activate ${catalogAlias} ${schemaName} ${tableName}`,
      exact: true,
    })
    .click();
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
  options: {
    expectPersisted?: boolean;
    expectedTable?: string | RegExp;
    parseTimeoutMs?: number;
  } = {},
): Promise<string> {
  await installUnavailableDirectoryPicker(page);
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();

  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Local files' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();

  const localDialog = page.getByRole('dialog', { name: 'Connect a local Delta folder' });
  await localDialog.getByLabel('One-session local Delta folder import').setInputFiles(tableDir);
  await expect(localDialog).toContainText(/Delta log parsed/i, {
    timeout: options.parseTimeoutMs,
  });
  await localDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(localDialog).toContainText(/source check passed/i);
  await localDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  await expect(reviewDialog).toContainText(/Detected 1 (?:local Delta|catalog) table/i);
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

async function installFakePaginationWorker(page: Page): Promise<void> {
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
  await expect(reviewDialog).toContainText(/Detected 1 (?:local Delta|catalog) table/i);
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

type RequestLogEntry = {
  url: string;
  resourceType: string;
};

function trackRelevantRequests(page: Page): RequestLogEntry[] {
  const requests: RequestLogEntry[] = [];
  page.on('request', (request) => {
    if (isIgnoredRequest(request)) return;
    requests.push({
      url: request.url(),
      resourceType: request.resourceType(),
    });
  });
  return requests;
}

function expectRequestLogExcludes(requests: RequestLogEntry[], forbidden: string[]): void {
  const urls = requests.map((request) => request.url);
  for (const pattern of forbidden) {
    expect(
      urls.filter((url) => url.includes(pattern)),
      `unexpected startup request matching ${pattern}:\n${urls.join('\n')}`,
    ).toEqual([]);
  }
}

function isIgnoredRequest(request: Request): boolean {
  const url = request.url();
  if (url.startsWith('chrome-extension://')) return true;
  if (url.includes('/@vite/') || url.includes('/@react-refresh')) return true;
  if (url.includes('/node_modules/')) return true;
  if (url.includes('__playwright')) return true;
  if (url.endsWith('/favicon.ico')) return true;
  if (url.endsWith('.css') || url.endsWith('.map')) return true;
  return false;
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
