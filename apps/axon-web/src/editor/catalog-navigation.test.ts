import { describe, expect, it } from 'vitest';
import {
  catalogTablePath,
  catalogTableRefFromParams,
  resolveCatalogTableRoute,
  savedQueryPath,
} from './catalog-navigation.ts';
import type { QueryCatalogCandidate } from '../services/query-source.ts';

function connectedCatalogs(): QueryCatalogCandidate[] {
  return [
    {
      id: 'cat/local 1',
      alias: 'local lake',
      storage: '/tmp/lake',
      region: 'browser-local',
      kind: 'local',
      schemas: [
        {
          name: 'sales ops',
          tables: [
            {
              name: 'orders/returns',
              manifestUrl: '/fixtures/orders-manifest.json',
              source: {
                storage: 'gs://unit-test/orders',
                region: 'us-test1',
              },
              snapshot: 9,
              rows: 42,
            },
          ],
        },
      ],
    },
  ];
}

describe('catalog navigation helpers', () => {
  it('encodes catalog table route segments without losing slashes or spaces', () => {
    expect(
      catalogTablePath({
        catalogId: 'cat/local 1',
        schemaName: 'sales ops',
        tableName: 'orders/returns',
      }),
    ).toBe('/catalog/cat%2Flocal%201/sales%20ops/orders%2Freturns');
  });

  it('encodes saved query ids as route segments', () => {
    expect(savedQueryPath('saved/query 42')).toBe('/saved/saved%2Fquery%2042');
  });

  it('rejects incomplete table params before touching catalog state', () => {
    expect(
      catalogTableRefFromParams({
        catalogId: 'cat/local 1',
        schemaName: '',
        tableName: 'orders/returns',
      }),
    ).toBeUndefined();
  });

  it('resolves valid table params to a connected table source without sample fallback', () => {
    expect(
      resolveCatalogTableRoute(connectedCatalogs(), {
        catalogId: 'cat/local 1',
        schemaName: 'sales ops',
        tableName: 'orders/returns',
      }),
    ).toEqual({
      status: 'valid',
      ref: {
        catalogId: 'cat/local 1',
        schemaName: 'sales ops',
        tableName: 'orders/returns',
      },
      source: {
        kind: 'manifest',
        catalogName: 'local lake',
        schemaName: 'sales ops',
        tableName: 'orders/returns',
        manifestUrl: '/fixtures/orders-manifest.json',
        storage: 'gs://unit-test/orders',
        region: 'us-test1',
        snapshot: 9,
        rows: 42,
        files: undefined,
        size: undefined,
        protocol: undefined,
      },
    });
  });

  it('reports missing table params as invalid instead of falling back to the sample table', () => {
    expect(
      resolveCatalogTableRoute(connectedCatalogs(), {
        catalogId: 'cat/local 1',
        schemaName: 'sales ops',
        tableName: 'missing',
      }),
    ).toEqual({
      status: 'invalid',
      reason: 'table_not_found',
      ref: {
        catalogId: 'cat/local 1',
        schemaName: 'sales ops',
        tableName: 'missing',
      },
    });
  });
});
