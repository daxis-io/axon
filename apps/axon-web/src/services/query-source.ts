export type ManifestQueryTableSource = {
  kind: 'manifest';
  catalogName: string;
  schemaName: string;
  tableName: string;
  manifestUrl: string;
  storage: string;
  region: string;
};

export type LocalDeltaQueryTableSource = {
  kind: 'local_delta';
  catalogName: string;
  schemaName: string;
  tableName: string;
  localRegistryId: string;
  storage: string;
  region: string;
};

export type QueryTableSource = ManifestQueryTableSource | LocalDeltaQueryTableSource;

export type QueryCatalogCandidate = {
  id: string;
  alias: string;
  storage: string;
  region?: string;
  kind?: string;
  schemas: Array<{
    name: string;
    tables: Array<{
      name: string;
      manifestUrl?: string;
      localRegistryId?: string;
    }>;
  }>;
};

export type ActiveConnectedTableRef = {
  catalogId: string;
  schemaName: string;
  tableName: string;
};

export const SAMPLE_QUERY_SOURCE: ManifestQueryTableSource = {
  kind: 'manifest',
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
      const table = schema.tables.find((candidate) => isQueryableTable(candidate));
      if (!table) continue;
      return querySourceForTable(catalog, schema.name, table);
    }
  }
  return SAMPLE_QUERY_SOURCE;
}

export function firstQueryableTableRef(
  catalogs: QueryCatalogCandidate[],
): ActiveConnectedTableRef | undefined {
  for (const catalog of catalogs) {
    for (const schema of catalog.schemas) {
      const table = schema.tables.find((candidate) => isQueryableTable(candidate));
      if (!table) continue;
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
  if (a.kind !== b.kind) return false;
  if (a.kind === 'manifest' && b.kind === 'manifest') {
    return (
      a.catalogName === b.catalogName &&
      a.schemaName === b.schemaName &&
      a.tableName === b.tableName &&
      a.manifestUrl === b.manifestUrl
    );
  }
  if (a.kind === 'local_delta' && b.kind === 'local_delta') {
    return (
      a.catalogName === b.catalogName &&
      a.schemaName === b.schemaName &&
      a.tableName === b.tableName &&
      a.localRegistryId === b.localRegistryId
    );
  }
  return false;
}

function querySourceForTableRef(
  catalogs: QueryCatalogCandidate[],
  activeTable: ActiveConnectedTableRef,
): QueryTableSource | undefined {
  const catalog = catalogs.find((candidate) => candidate.id === activeTable.catalogId);
  const schema = catalog?.schemas.find((candidate) => candidate.name === activeTable.schemaName);
  const table = schema?.tables.find((candidate) => candidate.name === activeTable.tableName);
  if (!catalog || !schema || !table || !isQueryableTable(table)) return undefined;
  return querySourceForTable(catalog, schema.name, table);
}

function querySourceForTable(
  catalog: QueryCatalogCandidate,
  schemaName: string,
  table: QueryCatalogCandidate['schemas'][number]['tables'][number],
): QueryTableSource {
  if (table.localRegistryId) {
    return {
      kind: 'local_delta',
      catalogName: catalog.alias,
      schemaName,
      tableName: table.name,
      localRegistryId: table.localRegistryId,
      storage: catalog.storage,
      region: catalog.region || 'browser-local',
    };
  }

  if (table.manifestUrl) {
    return {
      kind: 'manifest',
      catalogName: catalog.alias,
      schemaName,
      tableName: table.name,
      manifestUrl: table.manifestUrl,
      storage: catalog.storage,
      region: catalog.region || SAMPLE_QUERY_SOURCE.region,
    };
  }

  return SAMPLE_QUERY_SOURCE;
}

function isQueryableTable(
  table: QueryCatalogCandidate['schemas'][number]['tables'][number],
): boolean {
  return !!table.manifestUrl || !!table.localRegistryId;
}
