import { expect, test } from '@playwright/test';

import {
  querySourceFromConnectedCatalogs,
  sameQuerySource,
  type QueryCatalogCandidate,
} from '../src/services/query-source.ts';
import { loadCatalog, snapshotCatalog } from '../src/services/catalog.ts';
import {
  buildCatalogFromResult,
  loadConnectedCatalogs,
  saveConnectedCatalogs,
} from '../src/editor/connect/store.ts';
import type { ConnectedCatalog } from '../src/editor/connect/types.ts';
import {
  markSessionSetupMetricsEmitted,
  pendingSessionSetupMetrics,
  queryMetricsFromRangeReadMetricsEvent,
  type SessionSetupMetrics,
} from '../src/services/query.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';

class MemoryStorage implements Pick<Storage, 'getItem' | 'setItem' | 'removeItem'> {
  private readonly records = new Map<string, string>();

  getItem(key: string): string | null {
    return this.records.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.records.set(key, value);
  }

  removeItem(key: string): void {
    this.records.delete(key);
  }
}

test.describe('query source', () => {
  test('selects public GCS object-store table roots without a manifest', () => {
    const catalogs = [
      {
        id: 'public-gcs',
        alias: 'public-gcs',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://bucket/table',
        region: 'us-central1',
        schemas: [
          {
            name: 'default',
            tables: [
              {
                name: 'orders',
                uri: 'gs://bucket/table',
                snapshot: 0,
                rows: 4,
                files: 1,
                size: '1 KB',
                protocol: 'r1/w2',
              },
            ],
          },
        ],
      },
    ] as QueryCatalogCandidate[];

    expect(querySourceFromConnectedCatalogs(catalogs)).toEqual({
      kind: 'object_store_table_root',
      provider: 'gcs',
      catalogName: 'public-gcs',
      schemaName: 'default',
      tableName: 'orders',
      tableUri: 'gs://bucket/table',
      storage: 'gs://bucket/table',
      region: 'us-central1',
      snapshot: 0,
      rows: 4,
      files: 1,
      size: '1 KB',
      protocol: 'r1/w2',
    });
  });

  test('selects public S3 object-store table roots without a manifest', () => {
    const catalogs = [
      {
        id: 'public-s3',
        alias: 'public-s3',
        kind: 'object_store',
        provider: 's3',
        storage: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
        region: 'us-east-2',
        schemas: [
          {
            name: 'default',
            tables: [
              {
                name: 'orders',
                uri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
                snapshot: 0,
                rows: 4,
                files: 1,
                size: '1 KB',
                protocol: 'r1/w2',
              },
            ],
          },
        ],
      },
    ] as QueryCatalogCandidate[];

    expect(querySourceFromConnectedCatalogs(catalogs)).toEqual({
      kind: 'object_store_table_root',
      provider: 's3',
      catalogName: 'public-s3',
      schemaName: 'default',
      tableName: 'orders',
      tableUri: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
      storage: 's3://axon-public-s3-fixture-452456948477/fixtures/prod-like/table',
      region: 'us-east-2',
      snapshot: 0,
      rows: 4,
      files: 1,
      size: '1 KB',
      protocol: 'r1/w2',
    });
  });

  test('rejects public S3 connect results without an explicit bucket region', () => {
    expect(() =>
      buildCatalogFromResult({
        source: 'object_store',
        alias: 'public-s3',
        form: {
          path: '',
          detected: null,
          localDelta: null,
          provider: 's3',
          uri: 's3://bucket/table',
          region: '',
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
        selection: { default: 'all' },
        discovered: {
          summary: 'Detected 1 public Delta table',
          schemas: [
            {
              name: 'default',
              tableCount: 1,
              included: true,
              tables: [
                {
                  name: 'orders',
                  uri: 's3://bucket/table',
                  snapshot: 0,
                  rows: 4,
                  files: 1,
                  size: '1 KB',
                  protocol: 'r1/w2',
                },
              ],
            },
          ],
        },
      }),
    ).toThrow(/S3.*region/i);
  });

  test('carries public descriptor setup metrics from connected catalogs into query sources', () => {
    const catalog = buildCatalogFromResult({
      source: 'object_store',
      alias: 'public-gcs',
      form: {
        path: '',
        detected: null,
        localDelta: null,
        provider: 'gcs',
        uri: 'gs://bucket/table',
        region: 'us-central1',
        endpoint: '',
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
      selection: { default: 'all' },
      discovered: {
        summary: 'Detected 1 public Delta table',
        schemas: [
          {
            name: 'default',
            tableCount: 1,
            included: true,
            tables: [
              {
                name: 'orders',
                uri: 'gs://bucket/table',
                snapshot: 0,
                rows: 4,
                files: 1,
                size: '1 KB',
                protocol: 'r1/w2',
                descriptorResolutionMetrics: {
                  descriptor_resolution_count: 1,
                  delta_log_manifest_list_count: 1,
                  delta_log_manifest_list_duration_ms: 5,
                  snapshot_resolve_count: 1,
                  snapshot_resolve_duration_ms: 7,
                },
              },
            ],
          },
        ],
      },
    });

    expect(querySourceFromConnectedCatalogs([catalog])).toMatchObject({
      kind: 'object_store_table_root',
      descriptorResolutionMetrics: {
        descriptor_resolution_count: 1,
        delta_log_manifest_list_count: 1,
        delta_log_manifest_list_duration_ms: 5,
        snapshot_resolve_count: 1,
        snapshot_resolve_duration_ms: 7,
      },
    });
  });

  test('compares public object-store table roots by provider and table URI', () => {
    const left = {
      kind: 'object_store_table_root' as const,
      provider: 'gcs' as const,
      catalogName: 'lake',
      schemaName: 'default',
      tableName: 'orders',
      tableUri: 'gs://bucket/table',
      storage: 'gs://bucket/table',
      region: 'us-central1',
    };

    expect(sameQuerySource(left, { ...left })).toBe(true);
    expect(sameQuerySource(left, { ...left, tableUri: 'gs://bucket/other' })).toBe(false);
    expect(
      sameQuerySource(left, {
        ...left,
        provider: 's3',
        tableUri: 's3://bucket/table',
        storage: 's3://bucket/table',
        region: 'us-east-2',
      }),
    ).toBe(false);
  });

  test('pre-bootstrap GCS catalog exposes only summary metadata from the query source', async () => {
    const catalogs = [
      {
        id: 'public-gcs',
        alias: 'public-gcs',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://bucket/table',
        region: 'us-central1',
        schemas: [
          {
            name: 'default',
            tables: [
              {
                name: 'orders',
                uri: 'gs://bucket/table',
                snapshot: 12,
                rows: 42,
                files: 7,
                size: '3 KB',
                protocol: 'r3/w7',
              },
            ],
          },
        ],
      },
    ] as QueryCatalogCandidate[];
    const source = querySourceFromConnectedCatalogs(catalogs);

    expect(snapshotCatalog(source)).toEqual({
      name: 'public-gcs',
      region: 'us-central1',
      storage: 'gs://bucket/table',
      tables: [
        {
          name: 'orders',
          uri: 'gs://bucket/table',
          kind: 'delta',
          snapshot: 12,
          size_bytes: 3 * 1024,
          row_count: 42,
          file_count: 7,
          row_group_count: 0,
          partition_columns: [],
          protocol: { minReaderVersion: 3, minWriterVersion: 7, features: [] },
          columns: [],
        },
      ],
    });
    await expect(loadCatalog(source)).resolves.toEqual(snapshotCatalog(source));
  });

  test('pre-bootstrap S3 catalog exposes only summary metadata from the query source', async () => {
    const catalogs = [
      {
        id: 'public-s3',
        alias: 'public-s3',
        kind: 'object_store',
        provider: 's3',
        storage: 's3://bucket/table',
        region: 'us-east-2',
        schemas: [
          {
            name: 'default',
            tables: [
              {
                name: 'orders',
                uri: 's3://bucket/table',
                snapshot: 12,
                rows: 42,
                files: 7,
                size: '3 KB',
                protocol: 'r3/w7',
              },
            ],
          },
        ],
      },
    ] as QueryCatalogCandidate[];
    const source = querySourceFromConnectedCatalogs(catalogs);

    expect(snapshotCatalog(source)).toEqual({
      name: 'public-s3',
      region: 'us-east-2',
      storage: 's3://bucket/table',
      tables: [
        {
          name: 'orders',
          uri: 's3://bucket/table',
          kind: 'delta',
          snapshot: 12,
          size_bytes: 3 * 1024,
          row_count: 42,
          file_count: 7,
          row_group_count: 0,
          partition_columns: [],
          protocol: { minReaderVersion: 3, minWriterVersion: 7, features: [] },
          columns: [],
        },
      ],
    });
    await expect(loadCatalog(source)).resolves.toEqual(snapshotCatalog(source));
  });

  test('pre-bootstrap manifest catalog preserves connected summary metadata', async () => {
    const catalogs = [
      {
        id: 'sample-lake',
        alias: 'sample-lake',
        kind: 'object_store',
        provider: 'gcs',
        storage: 'gs://axon-sample/prod-like-events',
        region: 'browser-local',
        schemas: [
          {
            name: 'prod_like',
            tables: [
              {
                name: 'events',
                manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
                snapshot: 3,
                rows: 6,
                files: 1,
                size: '2 KB',
                protocol: 'r2/w5',
              },
            ],
          },
        ],
      },
    ] as QueryCatalogCandidate[];
    const source = querySourceFromConnectedCatalogs(catalogs);

    expect(source.kind).toBe('manifest');
    expect(snapshotCatalog(source)).toEqual({
      name: 'sample-lake',
      region: 'browser-local',
      storage: 'gs://axon-sample/prod-like-events',
      tables: [
        {
          name: 'events',
          uri: 'gs://axon-sample/prod-like-events',
          kind: 'delta',
          snapshot: 3,
          size_bytes: 2 * 1024,
          row_count: 6,
          file_count: 1,
          row_group_count: 0,
          partition_columns: [],
          protocol: { minReaderVersion: 2, minWriterVersion: 5, features: [] },
          columns: [],
        },
      ],
    });
  });

  test('durable connected catalog JSON excludes descriptors, read plans, and signed URLs', () => {
    const storage = new MemoryStorage();
    Object.defineProperty(globalThis, 'localStorage', {
      configurable: true,
      value: storage,
    });

    const catalog = {
      id: 'public-gcs',
      alias: 'public-gcs',
      kind: 'object_store',
      provider: 'gcs',
      storage: 'gs://bucket/table',
      region: 'us-central1',
      status: 'connected',
      connectedAt: 'now',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              id: 'default.orders',
              name: 'orders',
              snapshot: 12,
              rows: 42,
              files: 7,
              size: '3 KB',
              protocol: 'r3/w7',
              uri: 'gs://bucket/table',
              descriptor: {
                active_files: [
                  {
                    path: 'part-000.parquet',
                    url: 'https://storage.googleapis.com/bucket/table/part-000.parquet?X-Goog-Signature=secret',
                  },
                ],
              },
              active_files: [{ path: 'part-000.parquet' }],
              read_access_plan: {
                signedUrls: [
                  'https://storage.googleapis.com/bucket/table/part-000.parquet?X-Goog-Signature=secret',
                ],
              },
              openable_descriptor: { url: 'https://example.invalid/signed?token=secret' },
              source: {
                id: 'source-orders',
                kind: 'object_store',
                provider: 'gcs',
                storage: 'gs://bucket/table',
                region: 'us-central1',
                canonicalKey: 'object_store|gcs|gs://bucket/table|||default|orders',
                connectedAt: 'now',
                signedUrl: 'https://example.invalid/signed?token=secret',
              },
            },
          ],
        },
      ],
    } as unknown as ConnectedCatalog;

    saveConnectedCatalogs([catalog]);

    const raw = storage.getItem(STORAGE_KEY) ?? '';
    expect(raw).toContain('gs://bucket/table');
    expect(raw).not.toMatch(
      /X-Goog-Signature|signedUrls|signedUrl|active_files|read_access_plan|openable_descriptor|descriptor|https:\/\/storage\.googleapis\.com/i,
    );
    expect(loadConnectedCatalogs()).toHaveLength(1);
  });

  test('keeps query setup metrics pending until a query marks them emitted', () => {
    const setupMetrics: SessionSetupMetrics = {
      descriptor_resolution_count: 1,
      delta_log_manifest_list_count: 1,
      delta_log_manifest_list_duration_ms: 5,
      snapshot_resolve_count: 1,
      snapshot_resolve_duration_ms: 7,
    };
    const state = {
      setupMetrics,
      setupMetricsEmitted: false,
    };

    expect(pendingSessionSetupMetrics(state)).toEqual(setupMetrics);
    expect(state.setupMetricsEmitted).toBe(false);

    markSessionSetupMetricsEmitted(state);

    expect(pendingSessionSetupMetrics(state)).toBeUndefined();
    expect(state.setupMetricsEmitted).toBe(true);
  });

  test('projects prebootstrap pruning counters from worker range metrics', () => {
    const metrics = queryMetricsFromRangeReadMetricsEvent(
      {
        context: {
          phase: 'query',
          request_id: 'editor-query-1',
          query_id: 'editor-query-1',
          table_name: 'events',
        },
        bytes_fetched: 12_288,
        files_touched: 3,
        files_skipped: 0,
        prebootstrap_fail_open_count: 1,
        prebootstrap_files_pruned: 0,
        footer_reads_avoided: 0,
        prebootstrap_candidate_files: 3,
        row_groups_touched: 4,
        row_groups_skipped: 0,
        footer_reads: 3,
        descriptor_cache_hit: 1,
        session_reuse_count: 2,
        opened_table_reuse_count: 3,
        identity_refresh_count: 4,
        access_envelope_refresh_count: 5,
        rows_emitted: 0,
        access_mode: 'browser_safe_http',
      } as Parameters<typeof queryMetricsFromRangeReadMetricsEvent>[0],
      42,
      undefined,
    );

    expect(metrics).toMatchObject({
      bytes_fetched: 12_288,
      duration_ms: 42,
      files_touched: 3,
      files_skipped: 0,
      prebootstrap_fail_open_count: 1,
      prebootstrap_files_pruned: 0,
      footer_reads_avoided: 0,
      prebootstrap_candidate_files: 3,
      row_groups_touched: 4,
      row_groups_skipped: 0,
      footer_reads: 3,
      descriptor_cache_hit: 1,
      session_reuse_count: 2,
      opened_table_reuse_count: 3,
      identity_refresh_count: 4,
      access_envelope_refresh_count: 5,
      rows_emitted: 0,
      access_mode: 'browser_safe_http',
    });
  });
});
