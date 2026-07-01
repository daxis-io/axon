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

export function catalogTablePath(ref: ActiveConnectedTableRef): string {
  return `/catalog/${encodePathSegment(ref.catalogId)}/${encodePathSegment(
    ref.schemaName,
  )}/${encodePathSegment(ref.tableName)}`;
}

export function savedQueryPath(id: string): string {
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

function encodePathSegment(segment: string): string {
  return encodeURIComponent(segment);
}

function nonEmptySegment(segment: string): boolean {
  return segment.trim().length > 0;
}
