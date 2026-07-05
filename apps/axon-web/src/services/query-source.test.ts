import { describe, expect, it } from 'vitest';
import type { QueryCatalogCandidate } from './query-source.ts';
import { querySourcesForCatalog } from './query-source.ts';

describe('querySourcesForCatalog', () => {
  it('extracts queryable table sources from a connected catalog', () => {
    const catalog: QueryCatalogCandidate = {
      id: 'workspace',
      alias: 'Workspace',
      kind: 'object_store',
      provider: 'gcs',
      storage: 'gs://workspace',
      region: 'us',
      schemas: [
        {
          name: 'default',
          tables: [
            {
              name: 'manifested',
              manifestUrl: '/manifests/manifested.json',
              source: {
                storage: 'gs://workspace/manifested',
                region: 'us',
              },
            },
            {
              name: 'rooted',
              uri: 'gs://workspace/rooted',
            },
            {
              name: 'local',
              localRegistryId: 'local-registry',
              source: {
                storage: 'browser-cache://local',
                region: 'browser-local',
              },
            },
            {
              name: 'not-queryable',
              uri: 's3://wrong-provider/not-queryable',
            },
          ],
        },
      ],
    };

    const sources = querySourcesForCatalog(catalog);

    expect(sources).toHaveLength(3);
    expect(sources).toEqual([
      expect.objectContaining({
        kind: 'manifest',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'manifested',
        manifestUrl: '/manifests/manifested.json',
        storage: 'gs://workspace/manifested',
        region: 'us',
      }),
      expect.objectContaining({
        kind: 'object_store_table_root',
        provider: 'gcs',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'rooted',
        tableUri: 'gs://workspace/rooted',
        storage: 'gs://workspace/rooted',
        region: 'us',
      }),
      expect.objectContaining({
        kind: 'local_delta',
        catalogName: 'Workspace',
        schemaName: 'default',
        tableName: 'local',
        localRegistryId: 'local-registry',
        storage: 'browser-cache://local',
        region: 'browser-local',
      }),
    ]);
  });
});
