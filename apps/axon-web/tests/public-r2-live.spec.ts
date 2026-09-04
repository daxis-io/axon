import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';

import { expect, test, type APIRequestContext, type Page, type TestInfo } from '@playwright/test';

import {
  verifyQualificationRuntimeBuildManifest,
  type BrowserRuntimeBuildManifest,
} from '../scripts/browser-runtime-build.ts';

import {
  parsePublicDeltaLogIndexV1,
  parsePublicObjectStorageTableRoot,
  publicObjectUrl,
} from '../src/services/object-storage.ts';
import {
  isIgnorablePublicR2ConsoleError,
  validatePublicR2BrowserQueryEvidence,
  validatePublicR2OnboardingCsv,
  validatePublicR2PerformanceCsv,
  validatePublicR2UnsatisfiedRangeObservation,
  type PublicR2BrowserQueryEvidence,
} from '../src/services/public-r2-qualification.ts';

const endpoint = process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT;
const onboardingTableUri = process.env.AXON_LIVE_PUBLIC_R2_ONBOARDING_TABLE_URI;
const performanceTableUri = process.env.AXON_LIVE_PUBLIC_R2_PERF_TABLE_URI;
const daxisCommit = process.env.AXON_LIVE_PUBLIC_R2_DAXIS_COMMIT;
const runtimeCommit = process.env.AXON_LIVE_PUBLIC_R2_RUNTIME_COMMIT;
const endpointClass = process.env.AXON_LIVE_PUBLIC_R2_ENDPOINT_CLASS;
const qualificationContractPath = process.env.AXON_LIVE_PUBLIC_R2_QUALIFICATION_CONTRACT;
const productionOrigin = new URL(
  process.env.AXON_LIVE_PUBLIC_R2_ORIGIN ?? 'https://axon.daxistech.io',
).origin;
const qualificationLocalOrigin = new URL(
  process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173',
).origin;
const captureKey = '__AXON_PUBLIC_R2_QUERY_EVIDENCE__';

type QualificationTable = {
  fixture_revision: string;
  table_uri: string;
  expected: {
    latest_version: number;
    row_count: number;
    active_file_count: number;
    active_data_bytes: number;
  };
  qualification: { columns: string[]; result_sha256: string };
  provenance_sha256: string;
  index_sha256: string;
};

type QualificationContract = {
  schema_version: 1;
  bucket: string;
  daxis_commit: string;
  source: { repository: string; commit: string };
  tables: QualificationTable[];
};

type LoadedQualificationContract = {
  contract: QualificationContract;
  sha256: string;
};

const missingLiveConfiguration = [
  endpoint,
  endpointClass,
  onboardingTableUri,
  performanceTableUri,
  daxisCommit,
  runtimeCommit,
  qualificationContractPath,
].some((value) => !value);

test.describe('public R2 live qualification', () => {
  test.skip(
    missingLiveConfiguration,
    'set the complete public R2 qualification contract and runtime identity to run live qualification',
  );

  test('proves CORS, strong validators, range semantics, and the well-known index', async ({
    request,
  }, testInfo) => {
    const runtimeBuild = await loadQualificationRuntimeBuild(request);
    const loaded = await loadQualificationContract();
    const publicArtifacts: Array<{
      fixture_revision: string;
      table_uri: string;
      provenance_sha256: string;
      index_sha256: string;
    }> = [];
    let onboardingDataObject: { relative_path: string; size_bytes: number } | undefined;
    let onboardingRoot: ReturnType<typeof parsePublicObjectStorageTableRoot> | undefined;
    let indexGetCount = 0;
    let logGetCount = 0;
    let provenanceGetCount = 0;

    for (const table of loaded.contract.tables) {
      const root = parsePublicObjectStorageTableRoot({
        provider: 'r2',
        tableUri: table.table_uri,
        endpoint,
      });
      const indexUrl = publicObjectUrl(root, '_axon/public-delta-log-index.json');
      const indexResponse = await request.get(indexUrl, {
        headers: { Origin: productionOrigin },
      });
      expect(indexResponse.status()).toBe(200);
      expect(indexResponse.url()).toBe(indexUrl);
      expectCors(indexResponse.headers(), productionOrigin);
      expectImmutableCache(indexResponse.headers());
      indexGetCount += 1;
      const indexBytes = await indexResponse.body();
      expect(sha256Hex(indexBytes)).toBe(table.index_sha256);
      const objects = parsePublicDeltaLogIndexV1(
        JSON.parse(indexBytes.toString('utf8')) as unknown,
        root,
      );
      expect(objects.length).toBeGreaterThan(0);

      const logObject = objects.find((object) => object.relative_path.endsWith('.json'))!;
      const logResponse = await request.get(logObject.url, {
        headers: { Origin: productionOrigin },
      });
      expect(logResponse.status()).toBe(200);
      expect(logResponse.url()).toBe(logObject.url);
      expectCors(logResponse.headers(), productionOrigin);
      expectImmutableCache(logResponse.headers());
      logGetCount += 1;

      const provenanceUrl = publicObjectUrl(root, '_axon/fixture-provenance.json');
      const provenanceResponse = await request.get(provenanceUrl, {
        headers: { Origin: productionOrigin },
      });
      expect(provenanceResponse.status()).toBe(200);
      expect(provenanceResponse.url()).toBe(provenanceUrl);
      expectCors(provenanceResponse.headers(), productionOrigin);
      expectImmutableCache(provenanceResponse.headers());
      provenanceGetCount += 1;
      const provenanceBytes = await provenanceResponse.body();
      expect(sha256Hex(provenanceBytes)).toBe(table.provenance_sha256);
      const provenance = JSON.parse(provenanceBytes.toString('utf8')) as {
        objects: Array<{ relative_path: string; size_bytes: number }>;
      };
      if (table.fixture_revision === 'onboarding-v1') {
        onboardingRoot = root;
        onboardingDataObject = provenance.objects.find(
          (object) =>
            object.relative_path.endsWith('.parquet') &&
            !object.relative_path.startsWith('_delta_log/'),
        );
      }
      publicArtifacts.push({
        fixture_revision: table.fixture_revision,
        table_uri: table.table_uri,
        provenance_sha256: sha256Hex(provenanceBytes),
        index_sha256: sha256Hex(indexBytes),
      });
    }

    expect(onboardingRoot).toBeTruthy();
    expect(onboardingDataObject).toBeTruthy();
    const dataUrl = publicObjectUrl(onboardingRoot!, onboardingDataObject!.relative_path);
    const head = await request.head(dataUrl, { headers: { Origin: productionOrigin } });
    expect(head.status()).toBe(200);
    expect(head.url()).toBe(dataUrl);
    expectCors(head.headers(), productionOrigin);
    expectImmutableCache(head.headers());
    const contentLength = Number(head.headers()['content-length']);
    expect(contentLength).toBe(onboardingDataObject!.size_bytes);
    expect(head.headers()['accept-ranges']).toBe('bytes');
    const etag = head.headers().etag;
    expect(etag).toMatch(/^".+"$/);

    const range = await request.get(dataUrl, {
      headers: { Origin: productionOrigin, Range: 'bytes=0-15' },
    });
    expect(range.status()).toBe(206);
    expect(range.url()).toBe(dataUrl);
    expectCors(range.headers(), productionOrigin);
    expectImmutableCache(range.headers());
    expect(range.headers()['content-range']).toBe(`bytes 0-15/${onboardingDataObject!.size_bytes}`);
    const rangeBodyPrefix = Buffer.from(await range.body())
      .subarray(0, 4)
      .toString('utf8');
    expect(rangeBodyPrefix).toBe('PAR1');

    const ifRange = await request.get(dataUrl, {
      headers: { Origin: productionOrigin, Range: 'bytes=0-15', 'If-Range': etag! },
    });
    expect(ifRange.status()).toBe(206);
    expect(ifRange.url()).toBe(dataUrl);
    expectCors(ifRange.headers(), productionOrigin);
    expectImmutableCache(ifRange.headers());
    expect(ifRange.headers().etag).toBe(etag);

    const unsatisfied = await request.get(dataUrl, {
      headers: { Origin: productionOrigin, Range: `bytes=${onboardingDataObject!.size_bytes}-` },
    });
    expect(unsatisfied.status()).toBe(416);
    expect(unsatisfied.url()).toBe(dataUrl);
    expectCors(unsatisfied.headers(), productionOrigin);
    const unsatisfiedObservation = {
      status: unsatisfied.status(),
      exact_url: unsatisfied.url() === dataUrl,
      content_range: unsatisfied.headers()['content-range'] ?? null,
    };
    validatePublicR2UnsatisfiedRangeObservation(
      unsatisfiedObservation,
      onboardingDataObject!.size_bytes,
    );

    const hostileOrigin = 'https://attacker.invalid';
    const hostileResponse = await request.get(
      publicObjectUrl(onboardingRoot!, '_axon/public-delta-log-index.json'),
      { headers: { Origin: hostileOrigin } },
    );
    expect(hostileResponse.headers()['access-control-allow-origin']).toBeUndefined();

    const localOriginResponse = await request.get(
      publicObjectUrl(onboardingRoot!, '_axon/public-delta-log-index.json'),
      { headers: { Origin: qualificationLocalOrigin } },
    );
    expect(localOriginResponse.status()).toBe(200);
    expectCors(localOriginResponse.headers(), qualificationLocalOrigin);

    const artifact = {
      schema_version: 1,
      qualification_contract_sha256: loaded.sha256,
      endpoint_class: endpointClass,
      endpoint_origin: new URL(endpoint!).origin,
      bucket: loaded.contract.bucket,
      daxis_commit: daxisCommit,
      axon_runtime_commit: runtimeCommit,
      runtime_build: runtimeBuild,
      cors_origins: {
        production: { origin: productionOrigin, allowed: true },
        qualification_local: { origin: qualificationLocalOrigin, allowed: true },
        hostile: { origin: hostileOrigin, allowed: false },
      },
      observed: {
        public_gets: {
          index_count: indexGetCount,
          log_count: logGetCount,
          provenance_count: provenanceGetCount,
          all_status_200: true,
          all_exact_url: true,
          all_immutable_cache: true,
        },
        object_head: {
          status: head.status(),
          exact_url: head.url() === dataUrl,
          content_length: contentLength,
          accept_ranges: head.headers()['accept-ranges'],
          strong_etag: /^".+"$/.test(etag ?? ''),
          immutable_cache: true,
        },
        bounded_range: {
          status: range.status(),
          exact_url: range.url() === dataUrl,
          content_range: range.headers()['content-range'],
          body_prefix: rangeBodyPrefix,
          immutable_cache: true,
        },
        if_range: {
          status: ifRange.status(),
          exact_url: ifRange.url() === dataUrl,
          etag_preserved: ifRange.headers().etag === etag,
        },
        unsatisfied_range: {
          ...unsatisfiedObservation,
        },
      },
      public_artifacts: publicArtifacts,
    };
    await writeQualificationArtifact(testInfo, 'public-r2-http-contract.json', artifact);
  });

  test('queries the exact onboarding snapshot in browser WASM', async ({
    page,
    request,
  }, testInfo) => {
    const runtimeBuild = await loadQualificationRuntimeBuild(request);
    const loaded = await loadQualificationContract();
    const onboardingContract = qualificationTable(loaded.contract, 'onboarding-v1');
    const runtimeErrors = captureRuntimeErrors(page);
    await connectPublicR2Table(page, onboardingTableUri!, 'live-r2-onboarding');
    const tableName = tableNameFromUri(onboardingTableUri!);
    await runScalarQuery(page, tableName, `SELECT COUNT(*) AS row_count FROM "${tableName}"`, '4');

    expect(onboardingContract.qualification.columns).toEqual(['id', 'category', 'value']);
    await page
      .locator('.code-input')
      .fill(
        `SELECT ${onboardingContract.qualification.columns.join(', ')} FROM "${tableName}" ORDER BY id`,
      );
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 90_000 });
    await expect(page.locator('.res-meta')).toContainText('4 rows');
    const downloadPromise = page.waitForEvent('download');
    await page.locator('button[title="Export results as CSV"]').click();
    const download = await downloadPromise;
    const downloadPath = await download.path();
    expect(downloadPath).not.toBeNull();
    const result = await validatePublicR2OnboardingCsv(await readFile(downloadPath!, 'utf8'));
    expect(result.result_sha256).toBe(onboardingContract.qualification.result_sha256);
    expect(result.columns).toEqual(onboardingContract.qualification.columns);
    expect(runtimeErrors).toEqual([]);

    const observedSnapshot = await observedSnapshotMetadata(page, onboardingContract);
    await writeQualificationArtifact(testInfo, 'public-r2-onboarding-qualification.json', {
      schema_version: 1,
      qualification_contract_sha256: loaded.sha256,
      endpoint_class: endpointClass,
      endpoint_origin: new URL(endpoint!).origin,
      bucket: loaded.contract.bucket,
      daxis_commit: daxisCommit,
      axon_runtime_commit: runtimeCommit,
      runtime_build: runtimeBuild,
      fixture_revision: onboardingContract.fixture_revision,
      table_uri: onboardingContract.table_uri,
      result,
      observed: { snapshot: observedSnapshot, columns: result.columns },
    });
  });

  test('runs three fresh-runtime counts and the performance query without fallback', async ({
    page,
    request,
    browser,
    browserName,
  }, testInfo) => {
    testInfo.setTimeout(300_000);
    const runtimeBuild = await loadQualificationRuntimeBuild(request);
    const loaded = await loadQualificationContract();
    const performanceContract = qualificationTable(loaded.contract, 's3-browser-perf-v1');
    const tableName = tableNameFromUri(performanceTableUri!);
    const runs: Array<{
      run: number;
      scalar_result: string;
      evidence: PublicR2BrowserQueryEvidence;
    }> = [];

    for (let run = 1; run <= 3; run += 1) {
      const context = await browser.newContext({
        baseURL: process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173',
        ignoreHTTPSErrors: true,
      });
      const freshPage = await context.newPage();
      const freshRuntimeErrors = captureRuntimeErrors(freshPage);
      try {
        await installEvidenceCapture(freshPage);
        await connectPublicR2Table(freshPage, performanceTableUri!, 'live-r2-performance');
        const scalar = await runScalarQuery(
          freshPage,
          tableName,
          `SELECT COUNT(*) AS row_count FROM "${tableName}"`,
          '1048576',
        );
        const evidence = await latestEvidence(freshPage);
        validatePublicR2BrowserQueryEvidence(evidence, 'metadata-only-allowed');
        expect(freshRuntimeErrors).toEqual([]);
        runs.push({ run, scalar_result: scalar, evidence });
      } finally {
        await context.close();
      }
    }

    await installEvidenceCapture(page);
    const runtimeErrors = captureRuntimeErrors(page);
    await connectPublicR2Table(page, performanceTableUri!, 'live-r2-performance');
    const observedSnapshot = await observedSnapshotMetadata(page, performanceContract);
    expect(performanceContract.qualification.columns).toEqual([
      'event_id',
      'event_ts',
      'region',
      'customer_id',
      'amount',
      'status',
    ]);
    await page.locator('.code-input').fill(`
SELECT ${performanceContract.qualification.columns.join(', ')}
FROM "${tableName}"
WHERE amount > 100 AND status IN ('paid', 'shipped')
ORDER BY event_ts, event_id
LIMIT 1000
`);
    await page.locator('.btn.primary', { hasText: 'Run' }).click();
    await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 90_000 });
    await expect(page.locator('table.grid')).toContainText('event_id');
    const filteredEvidence = await latestEvidence(page);
    validatePublicR2BrowserQueryEvidence(filteredEvidence);
    await loadAllQueryRows(page);
    const downloadPromise = page.waitForEvent('download');
    await page.locator('button[title="Export results as CSV"]').click();
    const download = await downloadPromise;
    const downloadPath = await download.path();
    expect(downloadPath).not.toBeNull();
    const filteredResult = await validatePublicR2PerformanceCsv(
      await readFile(downloadPath!, 'utf8'),
    );
    expect(filteredResult.columns).toEqual(performanceContract.qualification.columns);
    expect(runtimeErrors).toEqual([]);

    const artifact = {
      schema_version: 1,
      qualification_contract_sha256: loaded.sha256,
      endpoint_class: endpointClass,
      endpoint_origin: new URL(endpoint!).origin,
      bucket: loaded.contract.bucket,
      daxis_commit: daxisCommit,
      axon_runtime_commit: runtimeCommit,
      runtime_build: runtimeBuild,
      fixture_revision: performanceContract.fixture_revision,
      table_uri: performanceContract.table_uri,
      browser_name: browserName,
      browser_version: browser.version(),
      runs,
      filtered_result: filteredResult,
      observed: { snapshot: observedSnapshot, columns: filteredResult.columns },
      filtered_query: filteredEvidence,
    };
    expect(filteredResult.result_sha256).toBe(performanceContract.qualification.result_sha256);
    await writeQualificationArtifact(testInfo, 'public-r2-live-qualification.json', artifact);
  });
});

async function loadAllQueryRows(page: Page): Promise<void> {
  for (let pageNumber = 0; pageNumber < 10; pageNumber += 1) {
    const loadMore = page.getByRole('button', { name: 'Load more' });
    if (!(await loadMore.isVisible())) break;
    const previous = loadedResultRows(await page.locator('.res-meta').innerText());
    await loadMore.click();
    await expect
      .poll(async () => loadedResultRows(await page.locator('.res-meta').innerText()))
      .toBeGreaterThan(previous);
  }
  await expect(page.getByRole('button', { name: 'Load more' })).not.toBeVisible();
  await expect(page.locator('.res-meta')).toContainText('1,000 rows');
}

function loadedResultRows(metadata: string): number {
  const match = metadata.match(/([\d,]+) rows/);
  if (!match) return 0;
  return Number(match[1]!.replaceAll(',', ''));
}

function expectCors(headers: Record<string, string>, origin: string): void {
  expect(headers['access-control-allow-origin']).toBe(origin);
}

async function loadQualificationRuntimeBuild(
  request: APIRequestContext,
): Promise<BrowserRuntimeBuildManifest> {
  const manifestUrl = new URL(
    '/axon-runtime-build.json',
    process.env.PLAYWRIGHT_BASE_URL ?? 'https://127.0.0.1:5173',
  ).toString();
  const response = await request.get(manifestUrl);
  expect(response.status()).toBe(200);
  expect(response.url()).toBe(manifestUrl);
  const value = (await response.json()) as unknown;
  verifyQualificationRuntimeBuildManifest(value, runtimeCommit!);
  return value;
}

async function observedSnapshotMetadata(
  page: Page,
  tableContract: QualificationTable,
): Promise<QualificationTable['expected']> {
  const persisted = JSON.parse(
    await page.evaluate(() => localStorage.getItem('axon.connect.catalogs.v1') ?? '[]'),
  ) as Array<{ schemas?: Array<{ tables?: Array<Record<string, unknown>> }> }>;
  const table = persisted
    .flatMap((catalog) => catalog.schemas ?? [])
    .flatMap((schema) => schema.tables ?? [])
    .find((candidate) => candidate.uri === tableContract.table_uri);
  expect(table, `missing persisted metadata for ${tableContract.fixture_revision}`).toBeTruthy();
  const metadata = table!.catalogMetadataJson;
  requireClosedSubset(metadata, ['latestSnapshotVersion', 'rowCount', 'fileCount', 'sizeBytes']);
  const snapshot = {
    latest_version: observedInteger(metadata.latestSnapshotVersion, 'latest snapshot version'),
    row_count: observedInteger(metadata.rowCount, 'row count'),
    active_file_count: observedInteger(metadata.fileCount, 'active file count'),
    active_data_bytes: observedInteger(metadata.sizeBytes, 'active data bytes'),
  };
  expect(table).toMatchObject({
    snapshot: snapshot.latest_version,
    rows: snapshot.row_count,
    files: snapshot.active_file_count,
  });
  expect(snapshot).toEqual(tableContract.expected);
  return snapshot;
}

function requireClosedSubset(
  value: unknown,
  keys: string[],
): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('persisted table metadata must be an object');
  }
  for (const key of keys) expect(value).toHaveProperty(key);
}

function observedInteger(value: unknown, label: string): number {
  const number = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  if (typeof number !== 'number' || !Number.isSafeInteger(number) || number < 0) {
    throw new Error(`persisted ${label} was not a browser-safe integer`);
  }
  return number;
}

function expectImmutableCache(headers: Record<string, string>): void {
  const directives = (headers['cache-control'] ?? '')
    .split(',')
    .map((directive) => directive.trim().toLowerCase());
  expect(directives).toEqual(expect.arrayContaining(['public', 'max-age=31536000', 'immutable']));
}

async function loadQualificationContract(): Promise<LoadedQualificationContract> {
  const bytes = await readFile(qualificationContractPath!);
  const value = JSON.parse(bytes.toString('utf8')) as unknown;
  requireClosedObject(
    value,
    ['bucket', 'daxis_commit', 'schema_version', 'source', 'tables'],
    'qualification contract',
  );
  expect(value.schema_version).toBe(1);
  expect(value.bucket).toBe('axon-public-data');
  expect(value.daxis_commit).toBe(daxisCommit);
  expect(value.daxis_commit).toMatch(/^[0-9a-f]{40}$/);
  expect(runtimeCommit).toMatch(/^[0-9a-f]{40}$/);
  requireClosedObject(value.source, ['commit', 'repository'], 'qualification contract source');
  expect(value.source.repository).toBe('daxis-io/axon');
  expect(value.source.commit).toMatch(/^[0-9a-f]{40}$/);
  expect(Array.isArray(value.tables)).toBe(true);
  expect(value.tables).toHaveLength(2);

  const seen = new Set<string>();
  for (const table of value.tables as unknown[]) {
    requireClosedObject(
      table,
      [
        'expected',
        'fixture_revision',
        'index_sha256',
        'provenance_sha256',
        'qualification',
        'table_uri',
      ],
      'qualification contract table',
    );
    expect(table.fixture_revision).toMatch(/^[a-z0-9][a-z0-9-]+$/);
    expect(seen.has(String(table.fixture_revision))).toBe(false);
    seen.add(String(table.fixture_revision));
    expect(table.table_uri).toBe(
      `r2://${value.bucket}/fixtures/${String(table.fixture_revision)}/table`,
    );
    expect(table.provenance_sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(table.index_sha256).toMatch(/^[0-9a-f]{64}$/);
    requireClosedObject(
      table.expected,
      ['active_data_bytes', 'active_file_count', 'latest_version', 'row_count'],
      'qualification expected snapshot',
    );
    expect(Object.values(table.expected).every(Number.isSafeInteger)).toBe(true);
    const qualification = table.qualification;
    requireClosedObject(qualification, ['columns', 'result_sha256'], 'qualification result');
    expect(Array.isArray(qualification.columns)).toBe(true);
    expect((qualification.columns as unknown[]).length).toBeGreaterThan(0);
    expect(qualification.result_sha256).toMatch(/^[0-9a-f]{64}$/);
  }

  const contract = value as unknown as QualificationContract;
  expect(qualificationTable(contract, 'onboarding-v1').table_uri).toBe(onboardingTableUri);
  expect(qualificationTable(contract, 's3-browser-perf-v1').table_uri).toBe(performanceTableUri);
  const endpointUrl = new URL(endpoint!);
  expect(endpointUrl.origin).toBe(endpoint!.replace(/\/$/, ''));
  expect(endpointUrl.protocol).toBe('https:');
  expect(endpointUrl.username).toBe('');
  expect(endpointUrl.password).toBe('');
  expect(endpointUrl.search).toBe('');
  expect(endpointUrl.hash).toBe('');
  if (endpointClass === 'r2.dev') expect(endpointUrl.hostname).toMatch(/\.r2\.dev$/);
  else {
    expect(endpointClass).toBe('custom-domain');
    expect(endpointUrl.origin).toBe('https://data.axon.daxistech.io');
  }
  return { contract, sha256: sha256Hex(bytes) };
}

function qualificationTable(
  contract: QualificationContract,
  fixtureRevision: string,
): QualificationTable {
  const table = contract.tables.find((candidate) => candidate.fixture_revision === fixtureRevision);
  expect(table, `missing qualification contract table ${fixtureRevision}`).toBeTruthy();
  return table!;
}

function requireClosedObject(
  value: unknown,
  keys: string[],
  context: string,
): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${context} must be an object`);
  }
  expect(Object.keys(value).sort(), `${context} must use the closed schema`).toEqual(
    [...keys].sort(),
  );
}

function sha256Hex(value: Uint8Array): string {
  return createHash('sha256').update(value).digest('hex');
}

async function writeQualificationArtifact(
  testInfo: TestInfo,
  name: string,
  value: Record<string, unknown>,
): Promise<void> {
  const serialized = JSON.stringify(value, null, 2);
  expect(serialized).not.toMatch(
    /x-amz-(?:credential|signature|security-token)|aws_(?:access_key_id|secret_access_key|session_token)|access[_-]?token|bearer|private[_-]?key/i,
  );
  const artifactPath = testInfo.outputPath(name);
  await writeFile(artifactPath, `${serialized}\n`, 'utf8');
  await testInfo.attach(name.replace(/\.json$/, ''), {
    path: artifactPath,
    contentType: 'application/json',
  });
}

function tableNameFromUri(uri: string): string {
  return uri.split('/').filter(Boolean).at(-1) ?? 'table';
}

async function connectPublicR2Table(page: Page, tableUri: string, alias: string): Promise<void> {
  await page.goto('/');
  await page.getByRole('button', { name: /^Connect$/ }).click();
  const sourceDialog = page.getByRole('dialog', { name: 'Connect a Delta source' });
  await sourceDialog.locator('.cc-source-row', { hasText: 'Object storage' }).click();
  await sourceDialog.getByRole('button', { name: /Continue/ }).click();
  const configDialog = page.getByRole('dialog', { name: 'Connect to object storage' });
  await configDialog.getByRole('button', { name: /Cloudflare R2/ }).click();
  await configDialog.locator('.cc-input.mono.has-prefix').fill(tableUri.replace(/^r2:\/\//, ''));
  await configDialog.getByLabel(/Public R2 HTTPS endpoint/).fill(endpoint!);
  await configDialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(configDialog).toContainText(/source check passed/i, { timeout: 90_000 });
  await configDialog.getByRole('button', { name: /Discover tables/ }).click();
  const reviewDialog = page.getByRole('dialog', { name: 'Review & name catalog' });
  const recommended = reviewDialog.getByLabel('Use recommended organization');
  if (await recommended.isChecked()) await recommended.uncheck();
  await reviewDialog.getByLabel('Catalog alias').fill(alias);
  await reviewDialog.getByRole('button', { name: /Connect catalog/ }).click();
  await expect(page.locator('.conn-pill')).toContainText(alias, { timeout: 30_000 });
}

async function runScalarQuery(
  page: Page,
  tableName: string,
  sql: string,
  expected: string,
): Promise<string> {
  await selectPersistedTableIfNeeded(page, tableName);
  await page.locator('.code-input').fill(sql);
  await page.locator('.btn.primary', { hasText: 'Run' }).click();
  await expect(page.locator('.res-meta')).toContainText(/browser · wasm/i, { timeout: 90_000 });
  const value = (await page.locator('table.grid tbody tr td').last().innerText()).trim();
  expect(value).toBe(expected);
  return value;
}

async function selectPersistedTableIfNeeded(page: Page, tableName: string): Promise<void> {
  if (await page.locator('.queryref-bar .qref', { hasText: tableName }).isVisible()) return;
  await expect(page.locator('.queryref-bar .qref')).toContainText(tableName);
}

function captureRuntimeErrors(page: Page): string[] {
  const errors: string[] = [];
  page.on('pageerror', (error) => errors.push(error.message));
  page.on('console', (message) => {
    if (
      message.type() === 'error' &&
      !isIgnorablePublicR2ConsoleError(message.location().url, qualificationLocalOrigin)
    ) {
      errors.push(message.text());
    }
  });
  return errors;
}

async function installEvidenceCapture(page: Page): Promise<void> {
  await page.addInitScript((key) => {
    const captured: Array<Record<string, unknown>> = [];
    Object.defineProperty(window, key, { value: captured, configurable: true });
    const OriginalWorker = window.Worker;
    class InstrumentedWorker extends OriginalWorker {
      constructor(url: string | URL, options?: WorkerOptions) {
        super(url, options);
        this.addEventListener('message', (event: MessageEvent<Record<string, unknown>>) => {
          const data = event.data;
          if (!data || typeof data !== 'object') return;
          for (const field of ['range_read_metrics', 'owned_memory_metrics'] as const) {
            if (data[field] && typeof data[field] === 'object') {
              captured.push({ kind: field, value: data[field] });
            }
          }
          if (data.fallback && typeof data.fallback === 'object') {
            const fallback = data.fallback as Record<string, unknown>;
            const context = fallback.context as Record<string, unknown> | undefined;
            captured.push({ kind: 'fallback', request_id: context?.request_id });
          }
          if (data.success && typeof data.success === 'object') {
            const success = data.success as Record<string, unknown>;
            const response = success.response as Record<string, unknown> | undefined;
            captured.push({
              kind: 'success',
              request_id: success.request_id,
              executed_on: response?.executed_on,
              response_fallback_reason: response?.fallback_reason ?? null,
            });
          }
        });
      }
    }
    Object.defineProperty(window, 'Worker', { value: InstrumentedWorker, configurable: true });
  }, captureKey);
}

async function latestEvidence(page: Page): Promise<PublicR2BrowserQueryEvidence> {
  await page.waitForFunction(
    (key) => {
      const records = (window as typeof window & Record<string, unknown>)[key];
      return Array.isArray(records) && records.some((record) => record.kind === 'success');
    },
    captureKey,
    { timeout: 10_000 },
  );
  const records = (await page.evaluate(
    (key) => (window as typeof window & Record<string, unknown>)[key],
    captureKey,
  )) as Array<Record<string, unknown>>;
  const success = records.filter((record) => record.kind === 'success').at(-1)!;
  const requestId = success.request_id;
  const forRequest = (record: Record<string, unknown>) => {
    const value = record.value as Record<string, unknown> | undefined;
    const context = value?.context as Record<string, unknown> | undefined;
    return context?.request_id === requestId;
  };
  const rawMetrics = records
    .filter((record) => record.kind === 'range_read_metrics' && forRequest(record))
    .at(-1)?.value as Record<string, number>;
  const rawOwnedMemory = records
    .filter((record) => record.kind === 'owned_memory_metrics' && forRequest(record))
    .at(-1)?.value as Record<string, unknown>;
  expect(rawMetrics).toBeTruthy();
  expect(rawOwnedMemory).toBeTruthy();
  const rawCoordinator = rawOwnedMemory.coordinator as Record<string, unknown> | undefined;
  const rawDatafusion = rawOwnedMemory.datafusion as Record<string, unknown> | undefined;
  expect(rawCoordinator).toBeTruthy();
  expect(rawDatafusion).toBeTruthy();
  return {
    metrics: {
      bytes_fetched: evidenceInteger(rawMetrics.bytes_fetched, 'bytes_fetched'),
      scan_data_range_reads: evidenceInteger(
        rawMetrics.scan_data_range_reads,
        'scan_data_range_reads',
      ),
      rows_emitted: evidenceInteger(rawMetrics.rows_emitted, 'rows_emitted'),
      arrow_ipc_bytes: evidenceInteger(rawMetrics.arrow_ipc_bytes, 'arrow_ipc_bytes'),
      coordinator_peak_staged_bytes: evidenceInteger(
        rawMetrics.coordinator_peak_staged_bytes,
        'coordinator_peak_staged_bytes',
      ),
      coordinator_staging_limit_bytes: evidenceInteger(
        rawMetrics.coordinator_staging_limit_bytes,
        'coordinator_staging_limit_bytes',
      ),
      cursor_peak_pending_encoded_bytes: evidenceInteger(
        rawMetrics.cursor_peak_pending_encoded_bytes,
        'cursor_peak_pending_encoded_bytes',
      ),
      cursor_peak_transport_chunk_bytes: evidenceInteger(
        rawMetrics.cursor_peak_transport_chunk_bytes,
        'cursor_peak_transport_chunk_bytes',
      ),
    },
    owned_memory: {
      coordinator: {
        limit_bytes: evidenceInteger(rawCoordinator!.limit_bytes, 'coordinator.limit_bytes'),
        reserved_bytes: evidenceInteger(
          rawCoordinator!.reserved_bytes,
          'coordinator.reserved_bytes',
        ),
        staged_bytes: evidenceInteger(rawCoordinator!.staged_bytes, 'coordinator.staged_bytes'),
        peak_reserved_bytes: evidenceInteger(
          rawCoordinator!.peak_reserved_bytes,
          'coordinator.peak_reserved_bytes',
        ),
        peak_staged_bytes: evidenceInteger(
          rawCoordinator!.peak_staged_bytes,
          'coordinator.peak_staged_bytes',
        ),
      },
      datafusion: {
        limit_bytes: evidenceInteger(rawDatafusion!.limit_bytes, 'datafusion.limit_bytes'),
        reserved_bytes: evidenceInteger(rawDatafusion!.reserved_bytes, 'datafusion.reserved_bytes'),
        peak_bytes: evidenceInteger(rawDatafusion!.peak_bytes, 'datafusion.peak_bytes'),
      },
    },
    execution: {
      executed_on: String(success.executed_on),
      fallback_event_observed: records.some(
        (record) => record.kind === 'fallback' && record.request_id === requestId,
      ),
      response_fallback_reason: success.response_fallback_reason,
    },
  };
}

function evidenceInteger(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`public R2 qualification metric '${field}' was not a safe integer`);
  }
  return value;
}
