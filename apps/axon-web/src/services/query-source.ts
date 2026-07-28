import { clone } from '@bufbuild/protobuf';
import {
  TableNodeSchema,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import type { CanonicalResourceRef } from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  canonicalResourceIdentityKey,
  canonicalTableForQuerySource,
  createSampleFixtureCanonicalTable,
  sameCanonicalTableIdentity,
} from './canonical-table-identity.ts';
import type {
  PublicObjectStorageDescriptorResolutionMetrics,
  PublicObjectStorageProvider,
} from './object-storage.ts';

export type ManifestQueryTableSource = {
  kind: 'manifest';
  catalogName: string;
  schemaName: string;
  tableName: string;
  manifestUrl: string;
  storage: string;
  region: string;
  snapshot?: number;
  rows?: number;
  files?: number;
  size?: string;
  protocol?: string;
};

export type LocalDeltaQueryTableSource = {
  kind: 'local_delta';
  catalogName: string;
  schemaName: string;
  tableName: string;
  localRegistryId: string;
  storage: string;
  region: string;
  snapshot?: number;
  rows?: number;
  files?: number;
  size?: string;
  protocol?: string;
  catalogMetadataJson?: Readonly<Record<string, unknown>>;
};

export type ObjectStoreTableRootQueryTableSource = {
  kind: 'object_store_table_root';
  provider: PublicObjectStorageProvider;
  catalogName: string;
  schemaName: string;
  tableName: string;
  tableUri: string;
  storage: string;
  region: string;
  snapshot?: number;
  rows?: number;
  files?: number;
  size?: string;
  protocol?: string;
  catalogMetadataJson?: Readonly<Record<string, unknown>>;
  descriptorResolutionMetrics?: PublicObjectStorageDescriptorResolutionMetrics;
};

export type QueryTableSource =
  | ManifestQueryTableSource
  | LocalDeltaQueryTableSource
  | ObjectStoreTableRootQueryTableSource;

export type QuerySourceIdentity =
  | readonly ['manifest', string, string, string, string, string, string, number | null]
  | readonly ['local_delta', string, number | null]
  | readonly [
      'object_store_table_root',
      PublicObjectStorageProvider,
      string,
      string,
      number | null,
    ];

export function querySourceIdentity(source: QueryTableSource): QuerySourceIdentity {
  if (source.kind === 'manifest') {
    return [
      'manifest',
      source.catalogName,
      source.schemaName,
      source.tableName,
      source.manifestUrl,
      source.storage,
      source.region,
      source.snapshot ?? null,
    ] as const;
  }

  if (source.kind === 'local_delta') {
    return ['local_delta', source.localRegistryId, source.snapshot ?? null] as const;
  }

  return [
    'object_store_table_root',
    source.provider,
    source.tableUri,
    source.region,
    source.snapshot ?? null,
  ] as const;
}

export type QueryCatalogCandidate = {
  id: string;
  alias: string;
  storage: string;
  region?: string;
  kind?: string;
  provider?: string;
  schemas: Array<{
    name: string;
    tables: Array<{
      name: string;
      snapshot?: number;
      rows?: number;
      files?: number;
      size?: string;
      protocol?: string;
      manifestUrl?: string;
      localRegistryId?: string;
      catalogMetadataJson?: Readonly<Record<string, unknown>>;
      logicalTable?: TableNode;
      source?: {
        storage: string;
        region: string;
      };
      uri?: string;
      descriptorResolutionMetrics?: PublicObjectStorageDescriptorResolutionMetrics;
    }>;
  }>;
};

export type ActiveConnectedTableRef = TableNode;

export type QuerySourceSelection =
  | { kind: 'resource'; ref: ActiveConnectedTableRef; source: QueryTableSource }
  | { kind: 'sample'; ref: ActiveConnectedTableRef; source: QueryTableSource }
  | {
      kind: 'unavailable';
      reason: 'missing' | 'empty' | 'stale' | 'unqueryable';
      ref?: ActiveConnectedTableRef;
    };

export type AvailableQuerySourceSelection = Extract<
  QuerySourceSelection,
  { kind: 'resource' | 'sample' }
>;

export const SAMPLE_QUERY_SOURCE: ManifestQueryTableSource = {
  kind: 'manifest',
  catalogName: 'sample-lake',
  schemaName: 'prod_like',
  tableName: 'events',
  manifestUrl: '/fixtures/prod-like/delta-log-manifest.json',
  storage: 'gs://axon-sample/prod-like-events',
  region: 'browser-local',
};

export const SAMPLE_QUERY_SOURCE_REF: Readonly<ActiveConnectedTableRef> = Object.freeze(
  createSampleFixtureCanonicalTable(SAMPLE_QUERY_SOURCE.tableName),
);

export function isExplicitSampleFixtureSelection(
  selection: AvailableQuerySourceSelection,
): boolean {
  return (
    selection.kind === 'sample' &&
    sameConnectedTableRef(selection.ref, SAMPLE_QUERY_SOURCE_REF) &&
    isSampleFixtureSource(selection.source)
  );
}

export function resolveQuerySourceSelection(
  catalogs: QueryCatalogCandidate[],
  selectedRef?: ActiveConnectedTableRef,
): QuerySourceSelection {
  if (!selectedRef) {
    return {
      kind: 'unavailable',
      reason: countTables(catalogs) === 0 ? 'empty' : 'missing',
    };
  }

  const location = connectedTableLocationForRef(catalogs, selectedRef);
  if (!location) {
    return { kind: 'unavailable', reason: 'stale', ref: selectedRef };
  }

  const source = querySourceForTable(location.catalog, location.schema.name, location.table);
  if (!source) {
    return { kind: 'unavailable', reason: 'unqueryable', ref: selectedRef };
  }
  if (
    source.kind !== 'manifest' &&
    isGeneratedQuerySourceNamespace(selectedRef.resource?.providerNamespace) &&
    !sameCanonicalTableIdentity(canonicalTableForQuerySource(source), selectedRef)
  ) {
    return { kind: 'unavailable', reason: 'unqueryable', ref: selectedRef };
  }

  return {
    kind:
      sameConnectedTableRef(selectedRef, SAMPLE_QUERY_SOURCE_REF) && isSampleFixtureSource(source)
        ? 'sample'
        : 'resource',
    ref: selectedRef,
    source,
  };
}

function isGeneratedQuerySourceNamespace(namespace: string | undefined): boolean {
  return (
    namespace === 'axon.local-delta/v1' ||
    namespace === 'axon.public-gcs/v1' ||
    namespace === 'axon.public-s3/v1'
  );
}

function isSampleFixtureSource(source: QueryTableSource): boolean {
  return (
    source.kind === 'manifest' &&
    source.catalogName === SAMPLE_QUERY_SOURCE.catalogName &&
    source.schemaName === SAMPLE_QUERY_SOURCE.schemaName &&
    source.tableName === SAMPLE_QUERY_SOURCE.tableName &&
    source.manifestUrl === SAMPLE_QUERY_SOURCE.manifestUrl &&
    source.storage === SAMPLE_QUERY_SOURCE.storage &&
    source.region === SAMPLE_QUERY_SOURCE.region
  );
}

function sameConnectedTableRef(
  left: ActiveConnectedTableRef,
  right: ActiveConnectedTableRef,
): boolean {
  return sameCanonicalTableIdentity(left, right);
}

export function sameAvailableQuerySourceSelection(
  left: AvailableQuerySourceSelection,
  right: AvailableQuerySourceSelection,
): boolean {
  return (
    left.kind === right.kind &&
    sameConnectedTableRef(left.ref, right.ref) &&
    sameQuerySource(left.source, right.source)
  );
}

export function soleQueryableTableRef(
  catalogs: QueryCatalogCandidate[],
): ActiveConnectedTableRef | undefined {
  let selected: ActiveConnectedTableRef | undefined;
  for (const catalog of catalogs) {
    for (const schema of catalog.schemas) {
      for (const table of schema.tables) {
        if (!table.logicalTable || !isQueryableTable(catalog, table)) continue;
        if (selected) return undefined;
        selected = clone(TableNodeSchema, table.logicalTable);
      }
    }
  }
  return selected;
}

export function sameQuerySource(a: QueryTableSource, b: QueryTableSource): boolean {
  const left = querySourceIdentity(a);
  const right = querySourceIdentity(b);
  return (
    left.length === right.length && left.every((value, index) => Object.is(value, right[index]))
  );
}

export function querySourceForConnectedTableRef(
  catalogs: QueryCatalogCandidate[],
  activeTable: ActiveConnectedTableRef,
): QueryTableSource | undefined {
  const location = connectedTableLocationForRef(catalogs, activeTable);
  if (!location) return undefined;
  return querySourceForTable(location.catalog, location.schema.name, location.table);
}

export type ConnectedTableLocation = Readonly<{
  catalog: QueryCatalogCandidate;
  schema: QueryCatalogCandidate['schemas'][number];
  table: QueryCatalogCandidate['schemas'][number]['tables'][number];
}>;

export function connectedTableLocationForRef(
  catalogs: QueryCatalogCandidate[],
  activeTable: ActiveConnectedTableRef,
): ConnectedTableLocation | undefined {
  if (!activeTable.resource) return undefined;
  const matches = connectedTableLocationsForResource(catalogs, activeTable.resource);
  return matches.length === 1 ? matches[0] : undefined;
}

export function connectedTableLocationsForResource(
  catalogs: QueryCatalogCandidate[],
  resource: CanonicalResourceRef,
): ConnectedTableLocation[] {
  let identity: string;
  try {
    identity = canonicalResourceIdentityKey(resource);
  } catch {
    return [];
  }
  const matches: ConnectedTableLocation[] = [];
  for (const catalog of catalogs) {
    if (catalog.id !== resource.connectionId) continue;
    for (const schema of catalog.schemas) {
      for (const table of schema.tables) {
        if (!table.logicalTable?.resource) continue;
        try {
          if (canonicalResourceIdentityKey(table.logicalTable.resource) !== identity) continue;
        } catch {
          continue;
        }
        matches.push({ catalog, schema, table });
      }
    }
  }
  return matches;
}

export function querySourcesForCatalog(catalog: QueryCatalogCandidate): QueryTableSource[] {
  const sources: QueryTableSource[] = [];
  for (const schema of catalog.schemas) {
    for (const table of schema.tables) {
      const source = querySourceForTable(catalog, schema.name, table);
      if (source) sources.push(source);
    }
  }
  return sources;
}

function querySourceForTable(
  catalog: QueryCatalogCandidate,
  schemaName: string,
  table: QueryCatalogCandidate['schemas'][number]['tables'][number],
): QueryTableSource | undefined {
  if (table.localRegistryId) {
    return {
      kind: 'local_delta',
      catalogName: catalog.alias,
      schemaName,
      tableName: table.name,
      localRegistryId: table.localRegistryId,
      storage: table.source?.storage ?? catalog.storage,
      region: table.source?.region ?? catalog.region ?? 'browser-local',
      snapshot: table.snapshot,
      rows: table.rows,
      files: table.files,
      size: table.size,
      protocol: table.protocol,
      catalogMetadataJson: table.catalogMetadataJson,
    };
  }

  if (table.manifestUrl) {
    return {
      kind: 'manifest',
      catalogName: catalog.alias,
      schemaName,
      tableName: table.name,
      manifestUrl: table.manifestUrl,
      storage: table.source?.storage ?? catalog.storage,
      region: table.source?.region ?? catalog.region ?? 'browser-local',
      snapshot: table.snapshot,
      rows: table.rows,
      files: table.files,
      size: table.size,
      protocol: table.protocol,
    };
  }

  const tableRoot = publicObjectStoreTableRoot(catalog, table);
  if (tableRoot) {
    return {
      kind: 'object_store_table_root',
      provider: tableRoot.provider,
      catalogName: catalog.alias,
      schemaName,
      tableName: table.name,
      tableUri: tableRoot.tableUri,
      storage: tableRoot.tableUri,
      region: table.source?.region ?? catalog.region ?? 'browser-local',
      snapshot: table.snapshot,
      rows: table.rows,
      files: table.files,
      size: table.size,
      protocol: table.protocol,
      catalogMetadataJson: table.catalogMetadataJson,
      descriptorResolutionMetrics: table.descriptorResolutionMetrics,
    };
  }

  return undefined;
}

function isQueryableTable(
  catalog: QueryCatalogCandidate,
  table: QueryCatalogCandidate['schemas'][number]['tables'][number],
): boolean {
  return (
    !!table.manifestUrl || !!table.localRegistryId || !!publicObjectStoreTableRoot(catalog, table)
  );
}

function publicObjectStoreTableRoot(
  catalog: QueryCatalogCandidate,
  table: QueryCatalogCandidate['schemas'][number]['tables'][number],
): { provider: PublicObjectStorageProvider; tableUri: string } | undefined {
  if (catalog.kind !== 'object_store') return undefined;
  const provider = publicObjectStorageProvider(catalog.provider);
  if (!provider) return undefined;
  const tableUri = table.uri ?? table.source?.storage ?? catalog.storage;
  const expectedScheme = provider === 's3' ? 's3://' : 'gs://';
  return tableUri.startsWith(expectedScheme) ? { provider, tableUri } : undefined;
}

function publicObjectStorageProvider(
  provider: string | undefined,
): PublicObjectStorageProvider | undefined {
  return provider === 'gcs' || provider === 's3' ? provider : undefined;
}

function countTables(catalogs: QueryCatalogCandidate[]): number {
  return catalogs.reduce(
    (catalogTotal, catalog) =>
      catalogTotal +
      catalog.schemas.reduce((schemaTotal, schema) => schemaTotal + schema.tables.length, 0),
    0,
  );
}
