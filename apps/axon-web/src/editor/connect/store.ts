// Persistence for connected catalogs from the Connect Catalog workflow.
// Stored in localStorage so non-sensitive catalog metadata survives reloads.

import { clone, equals, toJson } from '@bufbuild/protobuf';
import {
  TableMetadataSchema,
  TableNodeSchema,
  TableType,
  type TableNode,
} from '../../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import type { CatalogDiscoverySnapshot } from '../../services/catalog-provider.ts';
import {
  availabilityForSource,
  type DiscoveryPayload,
  type ObjectStoreProviderId,
} from './data.ts';
import type { ConnectResult, ConnectedCatalog, ConnectedCatalogSchema } from './types.ts';
import { createLocalStorageKeyValueStore } from '../../persistence/key-value.ts';
import { validatePersistedCatalogMetadata } from '../../services/catalog.ts';
import {
  canonicalTableFromMetadataJson,
  canonicalTableIdentityKey,
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
  LOCAL_DELTA_PROVIDER_NAMESPACE,
} from '../../services/canonical-table-identity.ts';
import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';
import {
  parsePublicObjectStorageTableRoot,
  publicObjectStorageConnectionId,
  type PublicObjectStorageProvider,
} from '../../services/object-storage.ts';
import {
  SAMPLE_QUERY_SOURCE,
  SAMPLE_QUERY_SOURCE_REF,
  querySourceForConnectedTableRef,
} from '../../services/query-source.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';
const LEGACY_SAMPLE_CATALOG_ID = 'sample-lake-fixture';
export const DEFAULT_AXON_CATALOG_ALIAS = 'workspace';
export const DEFAULT_AXON_SCHEMA_NAME = 'default';

export type ConnectedCatalogUpsertResult = {
  catalogs: ConnectedCatalog[];
  replaced: ConnectedCatalog[];
};

export const SAMPLE_CONNECTED_CATALOG: ConnectedCatalog = {
  id: SAMPLE_QUERY_SOURCE_REF.resource!.connectionId,
  catalogName: SAMPLE_QUERY_SOURCE.catalogName,
  alias: SAMPLE_QUERY_SOURCE.catalogName,
  kind: 'object_store',
  provider: 'gcs',
  storage: SAMPLE_QUERY_SOURCE.storage,
  region: SAMPLE_QUERY_SOURCE.region,
  status: 'connected',
  connectedAt: 'sample fixture',
  schemas: [
    {
      name: SAMPLE_QUERY_SOURCE.schemaName,
      tables: [
        {
          id: `${SAMPLE_QUERY_SOURCE.schemaName}.${SAMPLE_QUERY_SOURCE.tableName}`,
          name: SAMPLE_QUERY_SOURCE.tableName,
          snapshot: 3,
          rows: 6,
          files: 1,
          size: 'fixture',
          protocol: 'r2/w5',
          manifestUrl: SAMPLE_QUERY_SOURCE.manifestUrl,
          logicalTable: clone(TableNodeSchema, SAMPLE_QUERY_SOURCE_REF),
          source: {
            id: 'source-sample-lake-fixture',
            kind: 'object_store',
            provider: 'gcs',
            storage: SAMPLE_QUERY_SOURCE.storage,
            region: SAMPLE_QUERY_SOURCE.region,
            canonicalKey: tableSourceKeyFromParts({
              kind: 'object_store',
              provider: 'gcs',
              storage: SAMPLE_QUERY_SOURCE.storage,
              schemaName: SAMPLE_QUERY_SOURCE.schemaName,
              tableName: SAMPLE_QUERY_SOURCE.tableName,
            }),
            connectedAt: 'sample fixture',
          },
        },
      ],
    },
  ],
};

const connectedCatalogStore = createLocalStorageKeyValueStore<ConnectedCatalog>({
  storageKey: STORAGE_KEY,
  fallback: () => [SAMPLE_CONNECTED_CATALOG],
  invalidFallback: () => [],
  afterRead: (catalogs) =>
    validateConnectedCatalogMetadata(
      dedupeConnectedCatalogs(migrateConnectedCatalogs(catalogs, { rejectInvalid: true })),
    ),
  beforeWrite: (catalogs) =>
    validateConnectedCatalogMetadata(
      dedupeConnectedCatalogs(
        migrateConnectedCatalogs(durableConnectedCatalogs(catalogs), { rejectInvalid: true }),
      ),
    ),
});

export function loadConnectedCatalogs(): ConnectedCatalog[] {
  return connectedCatalogStore.getAll();
}

export function catalogsAvailableForFeatures(
  catalogs: ConnectedCatalog[],
  connectorFeatures: ConnectorFeatureFlags,
): ConnectedCatalog[] {
  return catalogs
    .map((catalog) => {
      const schemas = catalog.schemas
        .map((schema) => ({
          ...schema,
          tables: schema.tables.filter(
            (table) =>
              availabilityForSource(table.source?.kind ?? catalog.kind, connectorFeatures).enabled,
          ),
        }))
        .filter((schema) => schema.tables.length > 0);
      return schemas.length > 0 ? summarizeCatalog({ ...catalog, schemas }) : null;
    })
    .filter((catalog): catalog is ConnectedCatalog => catalog !== null);
}

export function saveConnectedCatalogs(catalogs: ConnectedCatalog[]): void {
  connectedCatalogStore.replaceAll(catalogs);
}

export function upsertConnectedCatalog(
  catalogs: ConnectedCatalog[],
  catalog: ConnectedCatalog,
): ConnectedCatalogUpsertResult {
  const targetCatalogKey = catalog.id;
  const replaced: ConnectedCatalog[] = [];
  const retained: ConnectedCatalog[] = [];
  let merged = catalog;

  for (const existing of catalogs) {
    if (existing.id === targetCatalogKey) {
      const merge = mergeCatalogs(existing, merged);
      merged = merge.catalog;
      if (merge.removed) replaced.push(merge.removed);
      continue;
    }

    retained.push(existing);
  }

  return { catalogs: [summarizeCatalog(merged), ...retained.map(summarizeCatalog)], replaced };
}

export function localRegistryIdsForCatalogs(catalogs: ConnectedCatalog[]): string[] {
  return catalogs.flatMap((catalog) =>
    catalog.schemas.flatMap((schema) =>
      schema.tables.flatMap((table) => table.localRegistryId ?? []),
    ),
  );
}

export function buildCatalogFromResult(result: ConnectResult): ConnectedCatalog {
  const { source, form, alias, selection } = result;
  const disc = result.catalogDiscovery
    ? discoveryPayloadFromCatalogDiscovery(result.catalogDiscovery)
    : result.discovered;
  const catalogMetadataJson = result.catalogDiscovery
    ? generatedMetadataJson(result.catalogDiscovery.metadata)
    : undefined;
  const catalogAlias = displayCatalogAlias(alias) || DEFAULT_AXON_CATALOG_ALIAS;
  const storage = storageForResult(result);
  const host =
    source === 'unity_catalog'
      ? form.uc_host
      : source === 'delta_share'
        ? form.ds_endpoint
        : undefined;
  const path = source === 'local' ? form.path : undefined;
  const region = regionForResult(result);
  const connectedAt = 'just now';
  const schemas = disc.schemas
    .map((s) => {
      const schemaName = s.name || DEFAULT_AXON_SCHEMA_NAME;
      const sel = selection[s.name] ?? (s.included ? 'all' : 'none');
      if (sel === 'none') return null;
      let tables = s.tables;
      if (sel !== 'all') {
        if ('except' in sel) tables = s.tables.filter((t) => !sel.except.includes(t.name));
        else tables = s.tables.filter((t) => sel.only.includes(t.name));
      }
      return {
        name: schemaName,
        tables: tables.map((t) => ({
          id: result.catalogDiscovery
            ? canonicalTableIdentityKey(result.catalogDiscovery.table)
            : `${schemaName}.${t.name}`,
          name: t.name,
          snapshot: t.snapshot,
          rows: t.rows,
          files: t.files,
          size: t.size,
          protocol: t.protocol,
          features: t.features,
          uri: source === 'object_store' ? (t.uri ?? form.uri) : t.name,
          manifestUrl: t.manifestUrl,
          descriptorResolutionMetrics:
            t.descriptorResolutionMetrics ??
            (source === 'object_store'
              ? form.objectStorage?.descriptorResolutionMetrics
              : undefined),
          localRegistryId: source === 'local' ? form.localDelta?.registryId : undefined,
          localPersistence: source === 'local' ? form.localDelta?.persistence : undefined,
          logicalTable: result.catalogDiscovery
            ? clone(TableNodeSchema, result.catalogDiscovery.table)
            : undefined,
          catalogMetadataJson,
          source: {
            id: sourceBindingId(source, storage, schemaName, t.name),
            kind: source,
            provider:
              source === 'object_store' ? (form.provider as ObjectStoreProviderId) : undefined,
            storage,
            host,
            path,
            region,
            canonicalKey: tableSourceKeyFromParts({
              kind: source,
              provider:
                source === 'object_store' ? (form.provider as ObjectStoreProviderId) : undefined,
              storage,
              host,
              path,
              schemaName,
              tableName: t.name,
            }),
            connectedAt,
          },
        })),
      };
    })
    .filter((s): s is NonNullable<typeof s> => s != null);

  return summarizeCatalog({
    id: result.catalogDiscovery?.catalog.connectionId ?? catalogIdForAlias(catalogAlias),
    catalogName: result.catalogDiscovery?.catalog.name ?? catalogAlias,
    alias: catalogAlias,
    kind: source,
    provider: source === 'object_store' ? (form.provider as ObjectStoreProviderId) : undefined,
    storage,
    host,
    path,
    region,
    status: 'connected',
    connectedAt,
    schemas,
  });
}

function mergeCatalogs(
  existing: ConnectedCatalog,
  incoming: ConnectedCatalog,
): { catalog: ConnectedCatalog; removed?: ConnectedCatalog } {
  const schemas = existing.schemas.map((schema) => ({
    ...schema,
    tables: [...schema.tables],
  }));
  const removedSchemas: ConnectedCatalog['schemas'] = [];

  for (const incomingSchema of incoming.schemas) {
    let targetSchema = schemas.find((schema) => schema.name === incomingSchema.name);
    if (!targetSchema) {
      targetSchema = { ...incomingSchema, tables: [] };
      schemas.push(targetSchema);
    }

    for (const incomingTable of incomingSchema.tables) {
      const incomingSourceKey = tableSourceKey(incomingTable, incoming);
      for (const schema of schemas) {
        const removedTables = schema.tables.filter(
          (existingTable) => tableSourceKey(existingTable, existing) === incomingSourceKey,
        );
        if (removedTables.length > 0) {
          const removedSchema = removedSchemas.find((candidate) => candidate.name === schema.name);
          if (removedSchema) {
            removedSchema.tables.push(...removedTables);
          } else {
            removedSchemas.push({
              ...schema,
              tables: removedTables,
            });
          }
        }
        schema.tables = schema.tables.filter(
          (existingTable) => tableSourceKey(existingTable, existing) !== incomingSourceKey,
        );
      }
      targetSchema.tables.push(incomingTable);
    }
  }

  const catalog = summarizeCatalog({
    ...incoming,
    id: incoming.id,
    alias: incoming.alias,
    connectedAt: existing.connectedAt || incoming.connectedAt,
    schemas: schemas.filter((schema) => schema.tables.length > 0),
  });
  const removed =
    removedSchemas.length > 0
      ? summarizeCatalog({
          ...existing,
          schemas: removedSchemas,
        })
      : undefined;

  return { catalog, removed };
}

function dedupeConnectedCatalogs(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  let deduped: ConnectedCatalog[] = [];
  for (let i = catalogs.length - 1; i >= 0; i -= 1) {
    deduped = upsertConnectedCatalog(deduped, catalogs[i]).catalogs;
  }
  return deduped;
}

function migrateConnectedCatalogs(
  catalogs: ConnectedCatalog[],
  options: { rejectInvalid?: boolean } = {},
): ConnectedCatalog[] {
  const migrated: ConnectedCatalog[] = [];
  for (const catalog of catalogs) {
    const legacySample = migrateLegacyExplicitSampleCatalogClaim(catalog, options);
    if (legacySample) {
      migrated.push(legacySample);
      continue;
    }
    validateExplicitSampleCatalogClaim(catalog, options);
    for (const schema of catalog.schemas ?? []) {
      for (const table of schema.tables ?? []) {
        const logicalTable = logicalTableForPersistedTable(table, catalog);
        const connectionId = logicalTable?.resource?.connectionId;
        if (!logicalTable || !connectionId) {
          if (options.rejectInvalid) {
            throw new Error('connected catalog table omitted a valid logical identity');
          }
          continue;
        }
        const envelope = migratedCatalogEnvelope(catalog, table, logicalTable);
        migrated.push({
          ...catalog,
          ...envelope,
          id: connectionId,
          catalogName: catalogNameForLogicalTable(logicalTable, catalog),
          schemas: [
            {
              ...schema,
              tables: [
                {
                  ...table,
                  id: canonicalTableIdentityKey(logicalTable),
                  logicalTable,
                },
              ],
            },
          ],
        });
      }
    }
  }
  return migrated;
}

function logicalTableForPersistedTable(
  table: ConnectedCatalogSchema['tables'][number],
  catalog: ConnectedCatalog,
): TableNode | undefined {
  try {
    if (table.catalogMetadataJson) {
      return canonicalTableFromMetadataJson(table.catalogMetadataJson);
    }
    if (table.logicalTable) {
      canonicalTableIdentityKey(table.logicalTable);
      return clone(TableNodeSchema, table.logicalTable);
    }
    if (table.localRegistryId) {
      return createLocalDeltaCanonicalTable({
        registryId: table.localRegistryId,
        tableName: table.name,
      });
    }
    const provider = publicObjectStorageProvider(table.source?.provider ?? catalog.provider);
    if (!provider) return undefined;
    const tableUri = table.uri ?? table.source?.storage ?? catalog.storage;
    const root = parsePublicObjectStorageTableRoot({
      provider,
      tableUri,
      region: table.source?.region ?? catalog.region,
    });
    const connectionId = publicObjectStorageConnectionId(root);
    if (provider === 'gcs') {
      return createPublicObjectStorageCanonicalTable({
        provider,
        connectionId,
        normalizedTableUri: root.tableUri,
        tableName: table.name,
      });
    }
    if (root.provider !== 's3') {
      throw new Error('public S3 identity resolved as a non-S3 table root');
    }
    return createPublicObjectStorageCanonicalTable({
      provider,
      connectionId,
      normalizedTableUri: root.tableUri,
      tableName: table.name,
      region: root.region,
    });
  } catch {
    return undefined;
  }
}

function migratedCatalogEnvelope(
  catalog: ConnectedCatalog,
  table: ConnectedCatalogSchema['tables'][number],
  logicalTable: TableNode,
): Pick<ConnectedCatalog, 'kind' | 'provider' | 'storage' | 'host' | 'path' | 'region'> {
  const namespace = logicalTable.resource?.providerNamespace;
  if (namespace === LOCAL_DELTA_PROVIDER_NAMESPACE) {
    return {
      kind: 'local',
      provider: undefined,
      storage: table.source?.storage ?? catalog.storage,
      host: undefined,
      path: table.source?.path ?? catalog.path,
      region: table.source?.region ?? 'browser-local',
    };
  }
  const provider = publicObjectStorageProviderForNamespace(namespace);
  if (provider) {
    const canonicalLocator =
      logicalTable.resource?.identity.case === 'canonicalLocator'
        ? logicalTable.resource.identity.value
        : undefined;
    return {
      kind: 'object_store',
      provider,
      storage: canonicalLocator ?? table.source?.storage ?? table.uri ?? catalog.storage,
      host: undefined,
      path: undefined,
      region: table.source?.region ?? catalog.region,
    };
  }
  return {
    kind: catalog.kind,
    provider: catalog.provider,
    storage: catalog.storage,
    host: catalog.host,
    path: catalog.path,
    region: catalog.region,
  };
}

function catalogNameForLogicalTable(logicalTable: TableNode, catalog: ConnectedCatalog): string {
  const namespace = logicalTable.resource?.providerNamespace;
  if (namespace === LOCAL_DELTA_PROVIDER_NAMESPACE) return 'local-delta';
  const provider = publicObjectStorageProviderForNamespace(namespace);
  if (provider) return `public-${provider}`;
  return catalog.catalogName?.trim() || catalog.alias;
}

function publicObjectStorageProviderForNamespace(
  namespace: string | undefined,
): PublicObjectStorageProvider | undefined {
  if (namespace === 'axon.public-gcs/v1') return 'gcs';
  if (namespace === 'axon.public-s3/v1') return 's3';
  return undefined;
}

function publicObjectStorageProvider(
  provider: ObjectStoreProviderId | undefined,
): PublicObjectStorageProvider | undefined {
  return provider === 'gcs' || provider === 's3' ? provider : undefined;
}

function migrateLegacyExplicitSampleCatalogClaim(
  catalog: ConnectedCatalog,
  options: { rejectInvalid?: boolean },
): ConnectedCatalog | undefined {
  if (catalog.id !== LEGACY_SAMPLE_CATALOG_ID) return undefined;
  if (isExactLegacyExplicitSampleCatalog(catalog)) {
    return cloneExplicitSampleConnectedCatalog();
  }
  if (options.rejectInvalid) {
    throw new Error('persisted legacy sample catalog did not match the explicit sample fixture');
  }
  return undefined;
}

function isExactLegacyExplicitSampleCatalog(catalog: ConnectedCatalog): boolean {
  const schema = catalog.schemas?.[0];
  const table = schema?.tables?.[0];
  const source = table?.source;
  const sampleTable = SAMPLE_CONNECTED_CATALOG.schemas[0]!.tables[0]!;
  const sampleSource = sampleTable.source!;
  return (
    hasExactOwnKeys(catalog, [
      'alias',
      'connectedAt',
      'id',
      'kind',
      'provider',
      'region',
      'schemas',
      'status',
      'storage',
    ]) &&
    catalog.alias === SAMPLE_CONNECTED_CATALOG.alias &&
    catalog.kind === SAMPLE_CONNECTED_CATALOG.kind &&
    catalog.provider === SAMPLE_CONNECTED_CATALOG.provider &&
    catalog.storage === SAMPLE_CONNECTED_CATALOG.storage &&
    catalog.region === SAMPLE_CONNECTED_CATALOG.region &&
    catalog.status === SAMPLE_CONNECTED_CATALOG.status &&
    catalog.connectedAt === SAMPLE_CONNECTED_CATALOG.connectedAt &&
    catalog.schemas.length === 1 &&
    hasExactOwnKeys(schema, ['name', 'tables']) &&
    schema?.name === SAMPLE_QUERY_SOURCE.schemaName &&
    schema.tables.length === 1 &&
    hasExactOwnKeys(table, [
      'files',
      'id',
      'manifestUrl',
      'name',
      'protocol',
      'rows',
      'size',
      'snapshot',
      'source',
    ]) &&
    table?.id === sampleTable.id &&
    table.name === sampleTable.name &&
    table.snapshot === sampleTable.snapshot &&
    table.rows === sampleTable.rows &&
    table.files === sampleTable.files &&
    table.size === sampleTable.size &&
    table.protocol === sampleTable.protocol &&
    table.manifestUrl === sampleTable.manifestUrl &&
    hasExactOwnKeys(source, [
      'canonicalKey',
      'connectedAt',
      'id',
      'kind',
      'provider',
      'region',
      'storage',
    ]) &&
    source?.id === sampleSource.id &&
    source.kind === sampleSource.kind &&
    source.provider === sampleSource.provider &&
    source.storage === sampleSource.storage &&
    source.region === sampleSource.region &&
    source.canonicalKey === sampleSource.canonicalKey &&
    source.connectedAt === sampleSource.connectedAt
  );
}

function cloneExplicitSampleConnectedCatalog(): ConnectedCatalog {
  const schema = SAMPLE_CONNECTED_CATALOG.schemas[0]!;
  const table = schema.tables[0]!;
  return {
    ...SAMPLE_CONNECTED_CATALOG,
    schemas: [
      {
        ...schema,
        tables: [
          {
            ...table,
            logicalTable: table.logicalTable
              ? clone(TableNodeSchema, table.logicalTable)
              : undefined,
            source: table.source ? { ...table.source } : undefined,
          },
        ],
      },
    ],
  };
}

function hasExactOwnKeys(value: object | null | undefined, expected: readonly string[]): boolean {
  if (!value) return false;
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}

function validateExplicitSampleCatalogClaim(
  catalog: ConnectedCatalog,
  options: { rejectInvalid?: boolean },
): void {
  if (catalog.id !== SAMPLE_QUERY_SOURCE_REF.resource?.connectionId) return;
  const schema = catalog.schemas?.[0];
  const table = schema?.tables?.[0];
  const valid =
    catalog.schemas.length === 1 &&
    schema?.name === SAMPLE_QUERY_SOURCE.schemaName &&
    schema.tables.length === 1 &&
    table?.name === SAMPLE_QUERY_SOURCE.tableName &&
    table.manifestUrl === SAMPLE_QUERY_SOURCE.manifestUrl &&
    table.logicalTable !== undefined &&
    equals(TableNodeSchema, table.logicalTable, SAMPLE_QUERY_SOURCE_REF);
  if (valid || !options.rejectInvalid) return;
  throw new Error('persisted sample catalog did not match the explicit sample fixture');
}

function validateConnectedCatalogMetadata(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  for (const catalog of catalogs) {
    const schemaNames = new Set<string>();
    const tableIdentities = new Set<string>();
    for (const schema of catalog.schemas) {
      if (schemaNames.has(schema.name)) {
        throw new Error('persisted catalog contains a duplicate schema identity');
      }
      schemaNames.add(schema.name);
      for (const table of schema.tables) {
        const identity = table.logicalTable
          ? canonicalTableIdentityKey(table.logicalTable)
          : tableSourceKey(table, catalog);
        if (tableIdentities.has(identity)) {
          throw new Error('persisted catalog contains a duplicate table identity');
        }
        tableIdentities.add(identity);
        if (!table.catalogMetadataJson) continue;
        if (table.logicalTable?.tableType !== TableType.TABLE) continue;
        const source = table.logicalTable
          ? querySourceForConnectedTableRef([catalog], table.logicalTable)
          : undefined;
        if (!source || source.kind === 'manifest') {
          throw new Error('persisted generated metadata source is invalid');
        }
        validatePersistedCatalogMetadata(source);
      }
    }
  }
  return catalogs;
}

function durableConnectedCatalogs(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  return catalogs
    .map((catalog) => {
      const schemas = catalog.schemas
        .map((schema) => ({
          name: schema.name,
          tables: schema.tables
            .filter((table) => table.localPersistence !== 'session_handles')
            .map(durableConnectedTable),
        }))
        .filter((schema) => schema.tables.length > 0);
      return schemas.length > 0
        ? summarizeCatalog(durableConnectedCatalog(catalog, schemas))
        : null;
    })
    .filter((catalog): catalog is ConnectedCatalog => catalog !== null);
}

function durableConnectedCatalog(
  catalog: ConnectedCatalog,
  schemas: ConnectedCatalogSchema[],
): ConnectedCatalog {
  return {
    id: catalog.id,
    catalogName: catalog.catalogName,
    alias: catalog.alias,
    kind: catalog.kind,
    provider: catalog.provider,
    storage: catalog.storage,
    host: catalog.host,
    path: catalog.path,
    region: catalog.region,
    status: catalog.status,
    connectedAt: catalog.connectedAt,
    schemas,
  };
}

function durableConnectedTable(
  table: ConnectedCatalogSchema['tables'][number],
): ConnectedCatalogSchema['tables'][number] {
  return {
    id: table.id,
    name: table.name,
    snapshot: table.snapshot,
    rows: table.rows,
    files: table.files,
    size: table.size,
    protocol: table.protocol,
    features: table.features,
    uri: table.uri,
    manifestUrl: table.manifestUrl,
    localRegistryId: table.localRegistryId,
    localPersistence: table.localPersistence,
    logicalTable: table.logicalTable,
    catalogMetadataJson: table.catalogMetadataJson,
    descriptorResolutionMetrics: table.descriptorResolutionMetrics,
    source: table.source
      ? {
          id: table.source.id,
          kind: table.source.kind,
          provider: table.source.provider,
          storage: table.source.storage,
          host: table.source.host,
          path: table.source.path,
          region: table.source.region,
          canonicalKey: table.source.canonicalKey,
          connectedAt: table.source.connectedAt,
        }
      : undefined,
  };
}

function generatedMetadataJson(
  metadata: CatalogDiscoverySnapshot['metadata'],
): Readonly<Record<string, unknown>> {
  const json = toJson(TableMetadataSchema, metadata);
  if (typeof json !== 'object' || json === null || Array.isArray(json)) {
    throw new Error('Catalog metadata did not encode as a JSON object.');
  }
  return json;
}

export function discoveryPayloadFromCatalogDiscovery(
  discovery: CatalogDiscoverySnapshot,
): DiscoveryPayload {
  const { metadata } = discovery;
  const table = metadata.table;
  if (!table?.resource) {
    throw new Error('Catalog provider metadata omitted canonical table identity.');
  }
  const schemaName = discovery.schema.name;
  const snapshot = browserSafeGeneratedInteger(metadata.latestSnapshotVersion, 'snapshot version');
  const rows = browserSafeGeneratedInteger(metadata.rowCount, 'row count');
  const files = browserSafeGeneratedInteger(metadata.fileCount, 'file count');
  const sizeBytes = browserSafeGeneratedInteger(metadata.sizeBytes, 'size bytes');
  const protocol =
    metadata.minReaderVersion !== undefined && metadata.minWriterVersion !== undefined
      ? `r${metadata.minReaderVersion}/w${metadata.minWriterVersion}`
      : 'json-log';

  return {
    summary: 'Detected 1 catalog table',
    schemas: [
      {
        name: schemaName,
        tableCount: 1,
        included: true,
        tables: [
          {
            name: table.name,
            snapshot,
            rows,
            files,
            size: formatBytes(sizeBytes),
            protocol,
            features: metadata.protocolFeatures.map((feature) => feature.name),
            uri: metadata.storageLocation,
            columns: metadata.columns.map((column) => ({
              name: column.name,
              type: column.type,
              part: metadata.partitionColumns.includes(column.name) || undefined,
            })),
          },
        ],
      },
    ],
  };
}

function browserSafeGeneratedInteger(value: bigint | undefined, label: string): number {
  if (value === undefined) return 0;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new Error(`Catalog ${label} was outside the browser-safe range.`);
  }
  return number;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} ${bytes === 1 ? 'byte' : 'bytes'}`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = -1;
  do {
    value /= 1024;
    unit += 1;
  } while (value >= 1024 && unit < units.length - 1);
  return `${value.toFixed(1)} ${units[unit]}`;
}

function summarizeCatalog(catalog: ConnectedCatalog): ConnectedCatalog {
  const sources = catalog.schemas.flatMap((schema) => schema.tables.map((table) => table.source));
  const storages = uniqueNonEmpty(sources.map((source) => source?.storage ?? catalog.storage));
  const regions = uniqueNonEmpty(sources.map((source) => source?.region ?? catalog.region));
  return {
    ...catalog,
    storage: storages.length <= 1 ? storages[0] || catalog.storage : `${storages.length} sources`,
    region: regions.length <= 1 ? regions[0] || catalog.region : 'mixed',
  };
}

function tableSourceKey(
  table: ConnectedCatalogSchema['tables'][number],
  catalog: ConnectedCatalog,
): string {
  return table.logicalTable
    ? canonicalTableIdentityKey(table.logicalTable)
    : (table.source?.canonicalKey ?? legacyTableSourceKey(table, catalog));
}

function legacyTableSourceKey(
  table: ConnectedCatalogSchema['tables'][number],
  catalog: ConnectedCatalog,
): string {
  return tableSourceKeyFromParts({
    kind: catalog.kind,
    provider: catalog.provider,
    storage: catalog.storage,
    host: catalog.host,
    path: catalog.path,
    schemaName: '',
    tableName: table.name,
  });
}

function tableSourceKeyFromParts({
  kind,
  provider,
  storage,
  host,
  path,
  schemaName,
  tableName,
}: {
  kind: ConnectResult['source'];
  provider?: ObjectStoreProviderId;
  storage: string;
  host?: string;
  path?: string;
  schemaName: string;
  tableName: string;
}): string {
  return [
    kind,
    provider ?? '',
    normalizeCatalogLocator(storage),
    normalizeCatalogLocator(host),
    normalizeCatalogLocator(path),
    normalizeCatalogAlias(schemaName),
    normalizeCatalogAlias(tableName),
  ].join('|');
}

function sourceBindingId(
  kind: ConnectResult['source'],
  storage: string,
  schemaName: string,
  tableName: string,
): string {
  return `source-${slug([kind, storage, schemaName, tableName].join('-'))}`;
}

function catalogIdForAlias(alias: string): string {
  return `catalog-${slug(alias)}`;
}

function normalizeCatalogAlias(alias: string | undefined): string {
  return (alias ?? '').trim().toLowerCase();
}

function displayCatalogAlias(alias: string | undefined): string {
  return (alias ?? '').trim();
}

function normalizeCatalogLocator(locator: string | undefined): string {
  return (locator ?? '').trim().replace(/\/+$/, '');
}

function uniqueNonEmpty(values: Array<string | undefined>): string[] {
  return Array.from(new Set(values.map((value) => value?.trim()).filter(Boolean) as string[]));
}

function slug(value: string): string {
  const slugged = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 48);
  return slugged || 'default';
}

function storageForResult(result: ConnectResult): string {
  const { source, form } = result;
  if (source === 'local') return form.localDelta?.storageLabel ?? form.path;
  if (source === 'object_store') return form.uri;
  if (source === 'delta_share') {
    return form.ds_mode === 'profile' ? form.ds_profile_name : form.ds_endpoint;
  }
  return form.uc_bff_url || form.uc_host;
}

function regionForResult(result: ConnectResult): string {
  const { source, form } = result;
  if (source === 'object_store') {
    const region = form.region.trim();
    if (form.provider === 's3' && !region) {
      throw new Error('Public S3 object storage requires an AWS region.');
    }
    return region || 'auto';
  }
  if (source === 'unity_catalog') return 'brokered';
  if (source === 'delta_share') return 'provider-vended';
  return 'browser-local';
}
