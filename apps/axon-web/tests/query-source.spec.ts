import { expect, test } from '@playwright/test';

import {
  querySourceFromConnectedCatalogs,
  sameQuerySource,
  type QueryCatalogCandidate,
} from '../src/services/query-source.ts';
import { buildCatalogFromResult } from '../src/editor/connect/store.ts';
import {
  markSessionSetupMetricsEmitted,
  pendingSessionSetupMetrics,
  type SessionSetupMetrics,
} from '../src/services/query.ts';

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
});
