import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { expect, test } from '@playwright/test';

import type {
  AxonBrowserClient,
  BrowserHttpFileDescriptor,
  BrowserHttpParquetDatasetDescriptor,
  BrowserWorkerResultPreviewCell,
} from '../src/axon-browser-sdk.ts';
import type { AggregateRow } from './worker-pool-benchmark-analysis.ts';

type ManifestFile = {
  relative_path: string;
  url_path: string;
  size_bytes: number;
  row_count: number;
  row_group_count: number;
  partition_values: Record<string, string>;
};

type FixtureManifest = {
  fixture_revision: string;
  actual_active_data_bytes: number;
  active_file_count: number;
  active_data_files: ManifestFile[];
  row_groups_per_file: number;
};

type ChildObservation = {
  aggregate: AggregateRow;
  arrow_ipc_bytes: number;
  executed_on: string;
  fallback_reason?: unknown;
  files: string[];
};

const WEB_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EXPECTED_AGGREGATE: AggregateRow = {
  row_count: 1_048_576,
  quantity_sum: 5_239_848,
  mobile_rows: 713_690,
};
const QUERY = `SELECT
  COUNT(*) AS row_count,
  SUM(quantity) AS quantity_sum,
  SUM(CASE WHEN is_mobile THEN 1 ELSE 0 END) AS mobile_rows
FROM bench`;

test('current-main coordinators preserve exact two-worker fanout compatibility', async ({
  browser,
  browserName,
  page,
}, testInfo) => {
  testInfo.setTimeout(120_000);
  await page.goto('/');

  const observation = await page.evaluate(
    async ({
      query,
    }): Promise<{
      fixture_revision: string;
      file_count: number;
      single: ChildObservation;
      pooled_children: ChildObservation[];
      pooled_aggregate: AggregateRow;
      p2_shards: string[][];
    }> => {
      const sdk = (await import(
        new URL('/src/axon-browser-sdk.ts', location.href).href
      )) as typeof import('../src/axon-browser-sdk.ts');
      const manifestResponse = await fetch('/fixtures/s3-perf/s3-perf-fixture-manifest.json');
      if (!manifestResponse.ok) {
        throw new Error(`fixture manifest fetch failed with HTTP ${manifestResponse.status}`);
      }
      const manifest = (await manifestResponse.json()) as FixtureManifest;
      if (
        manifest.fixture_revision !== 's3-browser-perf-v1' ||
        manifest.active_file_count !== 8 ||
        manifest.active_data_files.length !== 8 ||
        manifest.actual_active_data_bytes !== 82_057_700 ||
        manifest.row_groups_per_file !== 4
      ) {
        throw new Error('fixture manifest does not match the pinned worker-pool identity');
      }
      const allFiles = manifest.active_data_files;
      const shards = [
        allFiles.filter((_, index) => index % 2 === 0),
        allFiles.filter((_, index) => index % 2 === 1),
      ];
      const shardPaths = shards.map((shard) => shard.map(({ relative_path }) => relative_path));
      if (
        shards.some(({ length }) => length !== 4) ||
        new Set(shardPaths.flat()).size !== allFiles.length
      ) {
        throw new Error('P2 shards did not cover every active fixture file exactly once');
      }

      const runTarget = async (
        targetShards: ManifestFile[][],
        variant: string,
      ): Promise<ChildObservation[]> => {
        const clients: AxonBrowserClient[] = [];
        try {
          for (const [childIndex, files] of targetShards.entries()) {
            const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
              type: 'module',
              name: `worker-pool-compat-${variant}-${childIndex}`,
            });
            const client = sdk.createAxonBrowserClient({ worker });
            clients.push(client);
            await client.openParquetDataset('bench', descriptor(files, variant), {
              requestId: `open-${variant}-${childIndex}`,
            });
          }
          return await Promise.all(
            clients.map(async (client, childIndex): Promise<ChildObservation> => {
              const result = await client.query('bench', query, {
                requestId: `query-${variant}-${childIndex}`,
                preferredTarget: 'browser_wasm',
                queryOptions: { collect_metrics: true },
              });
              const preview = result.preview;
              if (preview === undefined || preview.rows.length !== 1 || preview.row_count !== 1) {
                throw new Error('worker-pool aggregate query did not return one preview row');
              }
              return {
                aggregate: parseAggregate(
                  preview.columns,
                  preview.rows[0] as BrowserWorkerResultPreviewCell[],
                ),
                arrow_ipc_bytes: result.result.byte_length,
                executed_on: result.response.executed_on,
                fallback_reason: result.response.fallback_reason ?? result.fallbackReason,
                files: targetShards[childIndex]?.map(({ relative_path }) => relative_path) ?? [],
              };
            }),
          );
        } finally {
          for (const client of clients) client.terminate();
        }
      };

      const descriptor = (
        files: ManifestFile[],
        variant: string,
      ): BrowserHttpParquetDatasetDescriptor => {
        const tableUrl = new URL('/fixtures/s3-perf/table', location.href);
        tableUrl.searchParams.set('axon_worker_pool_compat', variant);
        return {
          table_uri: tableUrl.href,
          partition_column_types: {
            event_date: 'string',
            region: 'string',
          },
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          files: files.map((file): BrowserHttpFileDescriptor => {
            const url = new URL(file.url_path, location.href);
            url.searchParams.set('axon_worker_pool_compat', variant);
            return {
              path: file.relative_path,
              url: url.href,
              size_bytes: file.size_bytes,
              partition_values: file.partition_values,
            };
          }),
        };
      };

      const parseAggregate = (
        columns: string[],
        row: BrowserWorkerResultPreviewCell[],
      ): AggregateRow => {
        const indexes = Object.fromEntries(columns.map((column, index) => [column, index]));
        if (
          columns.length !== 3 ||
          row.length !== 3 ||
          !['row_count', 'quantity_sum', 'mobile_rows'].every((column) => column in indexes)
        ) {
          throw new Error(`unexpected aggregate schema: ${columns.join(', ')}`);
        }
        return {
          row_count: parseInteger(row[indexes.row_count as number], 'row_count'),
          quantity_sum: parseInteger(row[indexes.quantity_sum as number], 'quantity_sum'),
          mobile_rows: parseInteger(row[indexes.mobile_rows as number], 'mobile_rows'),
        };
      };

      const parseInteger = (value: BrowserWorkerResultPreviewCell, label: string): number => {
        if (typeof value === 'number' && Number.isSafeInteger(value)) return value;
        if (typeof value !== 'string' || !/^[+-]?\d+$/.test(value)) {
          throw new Error(`${label} was not an exact integer`);
        }
        const integer = BigInt(value);
        if (
          integer < BigInt(Number.MIN_SAFE_INTEGER) ||
          integer > BigInt(Number.MAX_SAFE_INTEGER)
        ) {
          throw new Error(`${label} exceeded the safe integer range`);
        }
        return Number(integer);
      };

      const merge = (rows: AggregateRow[]): AggregateRow =>
        rows.reduce(
          (total, row) => ({
            row_count: total.row_count + row.row_count,
            quantity_sum: total.quantity_sum + row.quantity_sum,
            mobile_rows: total.mobile_rows + row.mobile_rows,
          }),
          { row_count: 0, quantity_sum: 0, mobile_rows: 0 },
        );

      const [single] = await runTarget([allFiles], 'single');
      if (single === undefined) throw new Error('single-worker compatibility result is missing');
      const pooledChildren = await runTarget(shards, 'p2');
      return {
        fixture_revision: manifest.fixture_revision,
        file_count: allFiles.length,
        single,
        pooled_children: pooledChildren,
        pooled_aggregate: merge(pooledChildren.map(({ aggregate }) => aggregate)),
        p2_shards: shardPaths,
      };
    },
    { query: QUERY },
  );

  expect(observation.fixture_revision).toBe('s3-browser-perf-v1');
  expect(observation.file_count).toBe(8);
  expect(observation.single.aggregate).toEqual(EXPECTED_AGGREGATE);
  expect(observation.pooled_aggregate).toEqual(EXPECTED_AGGREGATE);
  expect(observation.pooled_children).toHaveLength(2);
  expect(observation.p2_shards.map(({ length }) => length)).toEqual([4, 4]);
  expect(new Set(observation.p2_shards.flat()).size).toBe(8);
  for (const child of [observation.single, ...observation.pooled_children]) {
    expect(child.executed_on).toBe('browser_wasm');
    expect(child.fallback_reason).toBeUndefined();
    expect(child.arrow_ipc_bytes).toBeGreaterThan(0);
  }

  const artifact = {
    schema_version: 1,
    recorded_at: new Date().toISOString(),
    source_sha: execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: resolve(WEB_ROOT, '../..'),
      encoding: 'utf8',
    }).trim(),
    wasm_sha256: createHash('sha256')
      .update(await readFile(resolve(WEB_ROOT, 'src/wasm/axon_web_wasm_bg.wasm')))
      .digest('hex'),
    browser: { name: browserName, version: browser.version() },
    decision_scope: {
      production_worker_pool: 'inconclusive',
      wcrpc: 'not_justified',
      compatibility: 'pass',
    },
    observation,
  };
  const artifactPath = testInfo.outputPath('worker-pool-current-main-compatibility.json');
  await writeFile(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`, 'utf8');
  await testInfo.attach('worker-pool-current-main-compatibility', {
    path: artifactPath,
    contentType: 'application/json',
  });
});
