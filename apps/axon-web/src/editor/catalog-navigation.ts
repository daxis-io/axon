import {
  querySourceForConnectedTableRef,
  type ActiveConnectedTableRef,
  type QueryCatalogCandidate,
  type QueryTableSource,
} from '../services/query-source.ts';

export type CatalogTableRouteParams = {
  catalogId: string;
  schemaName: string;
  tableName: string;
};

export type CatalogTableHref = `/catalog/${string}/${string}/${string}`;
export type SavedQueryHref = `/saved/${string}`;

export type CatalogTableRouteResolution =
  | {
      status: 'valid';
      ref: ActiveConnectedTableRef;
      source: QueryTableSource;
    }
  | {
      status: 'invalid';
      reason: 'missing_params' | 'table_not_found';
      ref?: ActiveConnectedTableRef;
    };

export type CatalogExplorerTable = {
  key: string;
  name: string;
  active: boolean;
  queryable: boolean;
  path?: CatalogTableHref;
  snapshot?: number;
  rows?: number;
  files?: number;
  size?: string;
  storage: string;
  region: string;
};

export type CatalogExplorerSchema = {
  name: string;
  tableCount: number;
  tables: CatalogExplorerTable[];
};

export type CatalogExplorerCatalog = {
  id: string;
  alias: string;
  storage: string;
  region?: string;
  kind?: string;
  schemas: CatalogExplorerSchema[];
};

export type CatalogExplorerModel = {
  status: 'empty' | 'ready';
  catalogCount: number;
  schemaCount: number;
  tableCount: number;
  queryableTableCount: number;
  catalogs: CatalogExplorerCatalog[];
};

export function catalogTablePath(ref: ActiveConnectedTableRef): CatalogTableHref {
  return `/catalog/${encodePathSegment(ref.catalogId)}/${encodePathSegment(
    ref.schemaName,
  )}/${encodePathSegment(ref.tableName)}`;
}

export function savedQueryPath(id: string): SavedQueryHref {
  return `/saved/${encodePathSegment(id)}`;
}

export function catalogTableRefFromParams(
  params: CatalogTableRouteParams,
): ActiveConnectedTableRef | undefined {
  if (!nonEmptySegment(params.catalogId)) return undefined;
  if (!nonEmptySegment(params.schemaName)) return undefined;
  if (!nonEmptySegment(params.tableName)) return undefined;

  return {
    catalogId: params.catalogId,
    schemaName: params.schemaName,
    tableName: params.tableName,
  };
}

export function resolveCatalogTableRoute(
  catalogs: QueryCatalogCandidate[],
  params: CatalogTableRouteParams,
): CatalogTableRouteResolution {
  const ref = catalogTableRefFromParams(params);
  if (!ref) {
    return { status: 'invalid', reason: 'missing_params' };
  }

  const source = querySourceForConnectedTableRef(catalogs, ref);
  if (!source) {
    return { status: 'invalid', reason: 'table_not_found', ref };
  }

  return { status: 'valid', ref, source };
}

export function isQueryableCatalogTable(
  catalogs: QueryCatalogCandidate[],
  ref: ActiveConnectedTableRef,
): boolean {
  return querySourceForConnectedTableRef(catalogs, ref) !== undefined;
}

export function tableRefForRouteSelection(
  resolution: CatalogTableRouteResolution,
  current?: ActiveConnectedTableRef,
): ActiveConnectedTableRef | undefined {
  if (resolution.status !== 'valid') {
    return undefined;
  }

  if (
    current?.catalogId === resolution.ref.catalogId &&
    current.schemaName === resolution.ref.schemaName &&
    current.tableName === resolution.ref.tableName
  ) {
    return undefined;
  }

  return resolution.ref;
}

export function catalogExplorerModel(
  catalogs: QueryCatalogCandidate[],
  activeTable?: ActiveConnectedTableRef,
): CatalogExplorerModel {
  let schemaCount = 0;
  let tableCount = 0;
  let queryableTableCount = 0;

  const explorerCatalogs = catalogs.map((catalog) => {
    const schemas = catalog.schemas.map((schema) => {
      schemaCount += 1;
      tableCount += schema.tables.length;

      const tables = schema.tables.map((table) => {
        const ref = {
          catalogId: catalog.id,
          schemaName: schema.name,
          tableName: table.name,
        };
        const queryable = isQueryableCatalogTable(catalogs, ref);
        if (queryable) queryableTableCount += 1;

        return {
          key: `${catalog.id}/${schema.name}/${table.name}`,
          name: table.name,
          active:
            activeTable?.catalogId === catalog.id &&
            activeTable.schemaName === schema.name &&
            activeTable.tableName === table.name,
          path: queryable ? catalogTablePath(ref) : undefined,
          queryable,
          snapshot: table.snapshot,
          rows: table.rows,
          files: table.files,
          size: table.size,
          storage: table.source?.storage ?? table.uri ?? catalog.storage,
          region: table.source?.region ?? catalog.region ?? 'browser-local',
        };
      });

      return {
        name: schema.name,
        tableCount: schema.tables.length,
        tables,
      };
    });

    return {
      id: catalog.id,
      alias: catalog.alias,
      storage: catalog.storage,
      region: catalog.region,
      kind: catalog.kind,
      schemas,
    };
  });

  return {
    status: catalogs.length === 0 ? 'empty' : 'ready',
    catalogCount: catalogs.length,
    schemaCount,
    tableCount,
    queryableTableCount,
    catalogs: explorerCatalogs,
  };
}

function encodePathSegment(segment: string): string {
  return encodeURIComponent(segment);
}

function nonEmptySegment(segment: string): boolean {
  return segment.trim().length > 0;
}
