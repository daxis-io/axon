import { describe, expect, it } from 'vitest';
import { queryKeys } from './keys';
import { SAMPLE_QUERY_SOURCE, type QueryTableSource } from '../services/query-source.ts';

describe('queryKeys', () => {
  const manifestSource: QueryTableSource = {
    ...SAMPLE_QUERY_SOURCE,
    snapshot: 1,
    rows: 100,
  };

  it('builds explicit fixture keys under a fixed internal authority', () => {
    const connectionKey = [
      'catalog',
      'provider',
      'axon.fixture/v1',
      'connection',
      'axon-connection://fixture/sample-lake',
      'authority',
      'fixture',
    ] as const;
    const tableKey = [
      ...connectionKey,
      'resource',
      'table',
      'canonicalLocator',
      'axon-fixture://sample-lake/prod_like/events',
      'snapshot',
      1,
    ] as const;

    expect(queryKeys.catalog.root()).toEqual(['catalog']);
    expect(queryKeys.catalog.connection(manifestSource)).toEqual(connectionKey);
    expect(queryKeys.catalog.table(manifestSource)).toEqual(tableKey);
    expect(queryKeys.catalog.tableDerived(manifestSource)).toEqual([...tableKey, 'table-derived']);
    expect(queryKeys.catalog.commits(manifestSource)).toEqual([...tableKey, 'commits']);
  });

  it('keys local and public sources by canonical identity, not aliases or display summaries', () => {
    const local: QueryTableSource = {
      kind: 'local_delta',
      catalogName: 'local alias',
      schemaName: 'main',
      tableName: 'events',
      localRegistryId: 'registry-1',
      storage: 'local folder',
      region: 'browser-local',
      snapshot: 0,
    };
    const publicGcs: QueryTableSource = {
      kind: 'object_store_table_root',
      provider: 'gcs',
      catalogName: 'public alias',
      schemaName: 'main',
      tableName: 'events',
      tableUri: 'gs://bucket/events',
      storage: 'gs://bucket/events',
      region: 'us',
    };

    expect(queryKeys.catalog.connection(local)).toEqual([
      'catalog',
      'provider',
      'axon.local-delta/v1',
      'connection',
      'axon-connection://local-delta/registry-1',
      'authority',
      'non-session',
    ]);
    expect(queryKeys.catalog.table(local)).toEqual([
      ...queryKeys.catalog.connection(local),
      'resource',
      'table',
      'providerObjectId',
      'registry-1',
      'snapshot',
      0,
    ]);
    expect(queryKeys.catalog.table(publicGcs)).toEqual([
      'catalog',
      'provider',
      'axon.public-gcs/v1',
      'connection',
      'axon-connection://public-gcs/bucket',
      'authority',
      'non-session',
      'resource',
      'table',
      'canonicalLocator',
      'gs://bucket/events',
      'snapshot',
      null,
    ]);

    expect(
      queryKeys.catalog.table({
        ...publicGcs,
        catalogName: 'renamed',
        schemaName: 'renamed',
        tableName: 'renamed',
        region: 'renamed',
        rows: 999,
      }),
    ).toEqual(queryKeys.catalog.table(publicGcs));
    expect(queryKeys.catalog.table({ ...local, snapshot: undefined })).not.toEqual(
      queryKeys.catalog.table(local),
    );
  });

  it('builds local workspace keys without product data fetch behavior', () => {
    expect(queryKeys.local.root()).toEqual(['local']);
    expect(queryKeys.local.history()).toEqual(['local', 'history']);
    expect(queryKeys.local.saved()).toEqual(['local', 'saved']);
  });
});
