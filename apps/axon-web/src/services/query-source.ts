export type QueryTableSource = {
  catalogName: string;
  schemaName: string;
  tableName: string;
  manifestUrl: string;
  storage: string;
  region: string;
};

export type QueryCatalogCandidate = {
  id: string;
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

export type ActiveConnectedTableRef = {
  catalogId: string;
  schemaName: string;
  tableName: string;
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
  activeTable?: ActiveConnectedTableRef,
): QueryTableSource {
  const active = activeTable ? querySourceForTableRef(catalogs, activeTable) : undefined;
  if (active) return active;

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

export function firstQueryableTableRef(
  catalogs: QueryCatalogCandidate[],
): ActiveConnectedTableRef | undefined {
  for (const catalog of catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find((candidate) => candidate.manifestUrl);
      if (!table?.manifestUrl) continue;
      return {
        catalogId: catalog.id,
        schemaName: schema.name,
        tableName: table.name,
      };
    }
  }
  return undefined;
}

export function resolveActiveTableRef(
  catalogs: QueryCatalogCandidate[],
  activeTable?: ActiveConnectedTableRef,
): ActiveConnectedTableRef | undefined {
  if (activeTable && querySourceForTableRef(catalogs, activeTable)) {
    return activeTable;
  }
  return firstQueryableTableRef(catalogs);
}

export function sameQuerySource(a: QueryTableSource, b: QueryTableSource): boolean {
  return (
    a.catalogName === b.catalogName &&
    a.schemaName === b.schemaName &&
    a.tableName === b.tableName &&
    a.manifestUrl === b.manifestUrl
  );
}

function querySourceForTableRef(
  catalogs: QueryCatalogCandidate[],
  activeTable: ActiveConnectedTableRef,
): QueryTableSource | undefined {
  const catalog = catalogs.find((candidate) => candidate.id === activeTable.catalogId);
  const schema = catalog?.schemas.find((candidate) => candidate.name === activeTable.schemaName);
  const table = schema?.tables.find((candidate) => candidate.name === activeTable.tableName);
  if (!catalog || !schema || !table?.manifestUrl) return undefined;
  return {
    catalogName: catalog.alias,
    schemaName: schema.name,
    tableName: table.name,
    manifestUrl: table.manifestUrl,
    storage: catalog.storage,
    region: catalog.region || SAMPLE_QUERY_SOURCE.region,
  };
}
