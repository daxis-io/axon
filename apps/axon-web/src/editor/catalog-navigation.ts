import { create, fromJson, type JsonValue } from '@bufbuild/protobuf';
import {
  TableMetadataSchema,
  TableType,
  type TableMetadata,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  CanonicalResourceRefSchema,
  ResourceKind,
  type CanonicalResourceRef,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  canonicalTableIdentityKey,
  sameCanonicalTableIdentity,
  validatedCanonicalResourceRef,
} from '../services/canonical-table-identity.ts';
import {
  connectedTableLocationsForResource,
  querySourceForConnectedTableRef,
  type ActiveConnectedTableRef,
  type QueryCatalogCandidate,
  type QueryTableSource,
} from '../services/query-source.ts';

export type CatalogTableIdentityArm = 'provider-object-id' | 'canonical-locator';

export type CatalogTableRouteParams = {
  connectionId: string;
  providerNamespace: string;
  identityArm: string;
  identityValue: string;
};

export type LegacyCatalogTableRouteParams = {
  catalogId: string;
  schemaName: string;
  tableName: string;
};

export type CatalogTableHref =
  `/catalog/${string}/table/${string}/${CatalogTableIdentityArm}/${string}`;
export type CatalogTableSqlHref = `${CatalogTableHref}/sql`;
export type LegacyCatalogTableHref = `/catalog/${string}/${string}/${string}`;
export type SavedQueryHref = `/saved/${string}`;

export type CatalogTableRouteResolution =
  | {
      status: 'valid';
      ref: ActiveConnectedTableRef;
      source?: QueryTableSource;
    }
  | {
      status: 'invalid';
      reason:
        | 'malformed_route'
        | 'disconnected_connection'
        | 'stale_resource'
        | 'ambiguous_resource'
        | 'non_queryable';
      resource?: CanonicalResourceRef;
    };

export type LegacyCatalogTableRouteResolution =
  | { status: 'valid'; ref: ActiveConnectedTableRef; redirect: CatalogTableSqlHref }
  | {
      status: 'invalid';
      reason: 'malformed_route' | 'legacy_table_not_found' | 'ambiguous_legacy_route';
    };

export type CatalogExplorerTable = {
  key: string;
  name: string;
  tableKind: string;
  ref?: ActiveConnectedTableRef;
  active: boolean;
  queryable: boolean;
  path?: CatalogTableHref;
  sqlPath?: CatalogTableSqlHref;
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
  catalogName?: string;
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

export type CatalogExplorerTableDetail =
  | { status: 'no_selection' }
  | { status: 'unavailable'; reason: 'stale' }
  | {
      status: 'metadata_unavailable';
      reason: 'missing' | 'invalid';
      ref: ActiveConnectedTableRef;
      connectionAlias: string;
      schemaName: string;
      tableName: string;
      tableKind: string;
      queryable: boolean;
    }
  | {
      status: 'ready';
      ref: ActiveConnectedTableRef;
      connectionAlias: string;
      catalogName: string;
      schemaName: string;
      tableName: string;
      tableKind: string;
      comment?: string;
      queryable: boolean;
      sqlPath?: CatalogTableSqlHref;
      overview: {
        storageLocation: string;
        snapshot?: number;
        rows?: number;
        files?: number;
        sizeBytes?: number;
        protocol: string;
        features: string[];
        partitions: string[];
      };
      columnsStatus: 'ready' | 'empty';
      columns: Array<{
        name: string;
        type: string;
        nullable: boolean;
        comment?: string;
        partition: boolean;
      }>;
    };

export function catalogTablePath(ref: ActiveConnectedTableRef): CatalogTableHref {
  const resource = validatedTableResource(ref);
  return `/catalog/${encodePathSegment(resource.connectionId)}/table/${encodePathSegment(
    resource.providerNamespace,
  )}/${routeIdentityArm(resource)}/${encodePathSegment(resource.identity.value ?? '')}`;
}

export function catalogTableSqlPath(ref: ActiveConnectedTableRef): CatalogTableSqlHref {
  return `${catalogTablePath(ref)}/sql`;
}

export function legacyCatalogTablePath(
  params: LegacyCatalogTableRouteParams,
): LegacyCatalogTableHref {
  return `/catalog/${encodePathSegment(params.catalogId)}/${encodePathSegment(
    params.schemaName,
  )}/${encodePathSegment(params.tableName)}`;
}

export function savedQueryPath(id: string): SavedQueryHref {
  return `/saved/${encodePathSegment(id)}`;
}

export function catalogTableResourceFromParams(
  params: CatalogTableRouteParams,
): CanonicalResourceRef | undefined {
  if (!nonEmptySegment(params.connectionId)) return undefined;
  if (!nonEmptySegment(params.providerNamespace)) return undefined;
  if (!nonEmptySegment(params.identityValue)) return undefined;
  const identityCase =
    params.identityArm === 'provider-object-id'
      ? 'providerObjectId'
      : params.identityArm === 'canonical-locator'
        ? 'canonicalLocator'
        : undefined;
  if (!identityCase) return undefined;

  try {
    return validatedCanonicalResourceRef(
      create(CanonicalResourceRefSchema, {
        connectionId: params.connectionId,
        providerNamespace: params.providerNamespace,
        kind: ResourceKind.TABLE,
        identity: { case: identityCase, value: params.identityValue },
      }),
    );
  } catch {
    return undefined;
  }
}

export function resolveCatalogTableRoute(
  catalogs: QueryCatalogCandidate[],
  params: CatalogTableRouteParams,
  options: { requireQueryable?: boolean } = {},
): CatalogTableRouteResolution {
  const resource = catalogTableResourceFromParams(params);
  if (!resource) return { status: 'invalid', reason: 'malformed_route' };
  if (!catalogs.some((catalog) => catalog.id === resource.connectionId)) {
    return { status: 'invalid', reason: 'disconnected_connection', resource };
  }

  const locations = connectedTableLocationsForResource(catalogs, resource);
  if (locations.length === 0) {
    return { status: 'invalid', reason: 'stale_resource', resource };
  }
  if (locations.length > 1) {
    return { status: 'invalid', reason: 'ambiguous_resource', resource };
  }
  const ref = locations[0]!.table.logicalTable!;
  const source = querySourceForConnectedTableRef(catalogs, ref);
  if (options.requireQueryable && !source) {
    return { status: 'invalid', reason: 'non_queryable', resource };
  }
  return { status: 'valid', ref, source };
}

export function resolveLegacyCatalogTableRoute(
  catalogs: QueryCatalogCandidate[],
  params: LegacyCatalogTableRouteParams,
): LegacyCatalogTableRouteResolution {
  if (
    !nonEmptySegment(params.catalogId) ||
    !nonEmptySegment(params.schemaName) ||
    !nonEmptySegment(params.tableName)
  ) {
    return { status: 'invalid', reason: 'malformed_route' };
  }

  const matches = catalogs.flatMap((catalog) => {
    if (
      catalog.id !== params.catalogId &&
      legacyCatalogIdForAlias(catalog.alias) !== params.catalogId
    ) {
      return [];
    }
    return catalog.schemas.flatMap((schema) => {
      if (schema.name !== params.schemaName) return [];
      return schema.tables
        .filter((table) => table.name === params.tableName && table.logicalTable)
        .map((table) => table.logicalTable!);
    });
  });
  if (matches.length === 0) {
    return { status: 'invalid', reason: 'legacy_table_not_found' };
  }
  if (matches.length > 1) {
    return { status: 'invalid', reason: 'ambiguous_legacy_route' };
  }
  const ref = matches[0]!;
  if (!querySourceForConnectedTableRef(catalogs, ref)) {
    return { status: 'invalid', reason: 'legacy_table_not_found' };
  }
  return { status: 'valid', ref, redirect: catalogTableSqlPath(ref) };
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
  if (resolution.status !== 'valid') return undefined;
  if (current && sameCanonicalTableIdentity(current, resolution.ref)) return undefined;
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
        const ref = table.logicalTable;
        const queryable = !!ref && isQueryableCatalogTable(catalogs, ref);
        if (queryable) queryableTableCount += 1;

        return {
          key: ref ? canonicalTableIdentityKey(ref) : `${catalog.id}/${schema.name}/${table.name}`,
          name: table.name,
          tableKind: ref ? tableKindLabel(ref.tableType) : 'Unspecified',
          ref,
          active: !!ref && !!activeTable && sameCanonicalTableIdentity(activeTable, ref),
          path: ref ? catalogTablePath(ref) : undefined,
          sqlPath: queryable && ref ? catalogTableSqlPath(ref) : undefined,
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
      catalogName: catalog.catalogName,
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

export function catalogExplorerTableDetail(
  catalogs: QueryCatalogCandidate[],
  activeTable?: ActiveConnectedTableRef,
): CatalogExplorerTableDetail {
  if (!activeTable) return { status: 'no_selection' };
  const resource = activeTable.resource;
  if (!resource) return { status: 'unavailable', reason: 'stale' };
  const locations = connectedTableLocationsForResource(catalogs, resource);
  if (locations.length !== 1) return { status: 'unavailable', reason: 'stale' };
  const { catalog, schema, table } = locations[0]!;
  const ref = table.logicalTable!;
  const tableKind = tableKindLabel(ref.tableType);
  const queryable =
    ref.tableType === TableType.TABLE &&
    querySourceForConnectedTableRef(catalogs, ref) !== undefined;
  const identity = {
    ref,
    connectionAlias: catalog.alias,
    schemaName: schema.name,
    tableName: table.name,
    tableKind,
    queryable,
  };
  if (!table.catalogMetadataJson) {
    return { status: 'metadata_unavailable', reason: 'missing', ...identity };
  }

  let metadata: TableMetadata;
  try {
    metadata = fromJson(TableMetadataSchema, table.catalogMetadataJson as JsonValue);
    if (!metadata.table || !sameCanonicalTableIdentity(metadata.table, ref)) {
      throw new Error('metadata table identity did not match the selected resource');
    }
  } catch {
    return { status: 'metadata_unavailable', reason: 'invalid', ...identity };
  }

  try {
    const partitionNames = new Set(metadata.partitionColumns);
    const columns = metadata.columns.map((column) => ({
      name: column.name,
      type: column.type,
      nullable: column.nullable,
      comment: column.comment.trim() || undefined,
      partition: partitionNames.has(column.name),
    }));
    return {
      status: 'ready',
      ...identity,
      catalogName: catalog.catalogName?.trim() || catalog.alias,
      comment: ref.comment.trim() || undefined,
      sqlPath: queryable ? catalogTableSqlPath(ref) : undefined,
      overview: {
        storageLocation:
          metadata.storageLocation || table.source?.storage || table.uri || catalog.storage,
        snapshot: browserSafeOptionalInteger(metadata.latestSnapshotVersion),
        rows: browserSafeOptionalInteger(metadata.rowCount),
        files: browserSafeOptionalInteger(metadata.fileCount),
        sizeBytes: browserSafeOptionalInteger(metadata.sizeBytes),
        protocol:
          metadata.minReaderVersion !== undefined && metadata.minWriterVersion !== undefined
            ? `r${metadata.minReaderVersion}/w${metadata.minWriterVersion}`
            : 'not reported',
        features: metadata.protocolFeatures.map((feature) => feature.name),
        partitions: [...metadata.partitionColumns],
      },
      columnsStatus: columns.length === 0 ? 'empty' : 'ready',
      columns,
    };
  } catch {
    return { status: 'metadata_unavailable', reason: 'invalid', ...identity };
  }
}

function validatedTableResource(ref: ActiveConnectedTableRef): CanonicalResourceRef {
  if (!ref.resource) throw new Error('canonical table route requires a resource identity');
  return validatedCanonicalResourceRef(ref.resource);
}

function routeIdentityArm(resource: CanonicalResourceRef): CatalogTableIdentityArm {
  if (resource.identity.case === 'providerObjectId') return 'provider-object-id';
  if (resource.identity.case === 'canonicalLocator') return 'canonical-locator';
  throw new Error('canonical table route requires a supported identity arm');
}

export function tableKindLabel(tableType: TableType): string {
  switch (tableType) {
    case TableType.TABLE:
      return 'Table';
    case TableType.VIEW:
      return 'View';
    case TableType.MATERIALIZED_VIEW:
      return 'Materialized view';
    case TableType.STREAMING_TABLE:
      return 'Streaming table';
    case TableType.UNSPECIFIED:
      return 'Unspecified';
  }
}

function browserSafeOptionalInteger(value: bigint | undefined): number | undefined {
  if (value === undefined) return undefined;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new Error('generated catalog metadata integer was outside the browser-safe range');
  }
  return number;
}

function legacyCatalogIdForAlias(alias: string): string {
  const slug =
    alias
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 48) || 'default';
  return `catalog-${slug}`;
}

function encodePathSegment(segment: string): string {
  return encodeURIComponent(segment);
}

function nonEmptySegment(segment: string): boolean {
  return segment.trim().length > 0;
}
