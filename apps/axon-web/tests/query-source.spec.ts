import { expect, test } from '@playwright/test';

import {
  querySourceFromConnectedCatalogs,
  sameQuerySource,
  type QueryCatalogCandidate,
} from '../src/services/query-source.ts';

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
});
