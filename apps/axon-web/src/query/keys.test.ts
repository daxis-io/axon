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

  it('includes every execution-relevant source field while ignoring display summaries', () => {
    expect(
      querySourceIdentity({
        ...manifestSource,
        rows: 200,
      }),
    ).toEqual(querySourceIdentity(manifestSource));
    expect(
      querySourceIdentity({
        ...manifestSource,
        storage: 'gs://changed-source',
        region: 'changed-region',
        snapshot: 2,
      }),
    ).not.toEqual(querySourceIdentity(manifestSource));

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
    ).toEqual([
      'local_delta',
      'local',
      'main',
      'events',
      'registry-1',
      'local folder',
      'browser-local',
      null,
    ]);

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
    ).toEqual([
      'object_store_table_root',
      'gcs',
      'public',
      'main',
      'events',
      'gs://bucket/events',
      'gs://bucket/events',
      'us',
      null,
    ]);

    expect(
      querySourceIdentity({
        kind: 'object_store_table_root',
        provider: 's3',
        catalogName: 'public',
        schemaName: 'main',
        tableName: 'events',
        tableUri: 's3://bucket/events',
        storage: 's3://bucket/events',
        region: 'us-east-2',
      }),
    ).toEqual([
      'object_store_table_root',
      's3',
      'public',
      'main',
      'events',
      's3://bucket/events',
      's3://bucket/events',
      'us-east-2',
      null,
    ]);
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
