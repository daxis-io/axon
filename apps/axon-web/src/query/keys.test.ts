import { describe, expect, it } from 'vitest';
import { queryKeys, querySourceIdentity } from './keys';
import type { QueryTableSource } from '../services/query-source.ts';

describe('queryKeys', () => {
  const manifestSource: QueryTableSource = {
    kind: 'manifest',
    catalogName: 'catalog-a',
    schemaName: 'schema-a',
    tableName: 'table-a',
    manifestUrl: '/manifest-a.json',
    storage: 'gs://bucket/table-a',
    region: 'us',
    snapshot: 1,
    rows: 100,
  };

  it('builds stable source identities from the same fields used to compare sources', () => {
    expect(
      querySourceIdentity({
        ...manifestSource,
        storage: 'gs://changed-summary-field',
        region: 'changed-region',
        snapshot: 2,
        rows: 200,
      }),
    ).toEqual(querySourceIdentity(manifestSource));

    expect(
      querySourceIdentity({
        kind: 'local_delta',
        catalogName: 'local',
        schemaName: 'main',
        tableName: 'events',
        localRegistryId: 'registry-1',
        storage: 'local folder',
        region: 'browser-local',
      }),
    ).toEqual(['local_delta', 'local', 'main', 'events', 'registry-1']);

    expect(
      querySourceIdentity({
        kind: 'object_store_table_root',
        provider: 'gcs',
        catalogName: 'public',
        schemaName: 'main',
        tableName: 'events',
        tableUri: 'gs://bucket/events',
        storage: 'gs://bucket/events',
        region: 'us',
      }),
    ).toEqual(['object_store_table_root', 'gcs', 'gs://bucket/events']);
  });

  it('builds catalog keys under stable source identity prefixes', () => {
    const sourceKey = ['catalog', 'source', querySourceIdentity(manifestSource)] as const;

    expect(queryKeys.catalog.root()).toEqual(['catalog']);
    expect(queryKeys.catalog.source(manifestSource)).toEqual(sourceKey);
    expect(queryKeys.catalog.tableDerived(manifestSource)).toEqual([...sourceKey, 'table-derived']);
    expect(queryKeys.catalog.commits(manifestSource)).toEqual([...sourceKey, 'commits']);
  });

  it('builds local workspace keys without product data fetch behavior', () => {
    expect(queryKeys.local.root()).toEqual(['local']);
    expect(queryKeys.local.history()).toEqual(['local', 'history']);
    expect(queryKeys.local.saved()).toEqual(['local', 'saved']);
  });
});
