export type QueryTableSource = {
  catalogName: string;
  schemaName: string;
  tableName: string;
  manifestUrl: string;
  storage: string;
  region: string;
};

export type QueryCatalogCandidate = {
  alias: string;
  storage: string;
  region?: string;
  schemas: Array<{
    name: string;
    tables: Array<{
      name: string;
      manifestUrl?: string;
    }>;
  }>;
};

export const SAMPLE_QUERY_SOURCE: QueryTableSource = {
  catalogName: 'sample-lake',
  schemaName: 'prod_like',
  tableName: 'events',
  manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
  storage: 'gs://axon-sample/prod-like-events',
  region: 'browser-local',
};

export function querySourceFromConnectedCatalogs(
  catalogs: QueryCatalogCandidate[],
): QueryTableSource {
  for (const catalog of catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find((candidate) => candidate.manifestUrl);
      if (!table?.manifestUrl) continue;
      return {
        catalogName: catalog.alias,
        schemaName: schema.name,
        tableName: table.name,
        manifestUrl: table.manifestUrl,
        storage: catalog.storage,
        region: catalog.region || SAMPLE_QUERY_SOURCE.region,
      };
    }
  }
  return SAMPLE_QUERY_SOURCE;
}

export function sameQuerySource(a: QueryTableSource, b: QueryTableSource): boolean {
  return (
    a.catalogName === b.catalogName &&
    a.schemaName === b.schemaName &&
    a.tableName === b.tableName &&
    a.manifestUrl === b.manifestUrl
  );
}
