// Persistence for connected catalogs from the Connect Catalog workflow.
// Stored in localStorage so non-sensitive catalog metadata survives reloads.

import { clone, toJson } from '@bufbuild/protobuf';
import {
  TableMetadataSchema,
  TableNodeSchema,
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
} from '../../services/canonical-table-identity.ts';
import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';
import {
  SAMPLE_QUERY_SOURCE,
  SAMPLE_QUERY_SOURCE_REF,
  querySourceForConnectedTableRef,
} from '../../services/query-source.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';
export const DEFAULT_AXON_CATALOG_ALIAS = 'workspace';
export const DEFAULT_AXON_SCHEMA_NAME = 'default';

export type ConnectedCatalogUpsertResult = {
  catalogs: ConnectedCatalog[];
  replaced: ConnectedCatalog[];
};

export const SAMPLE_CONNECTED_CATALOG: ConnectedCatalog = {
  id: SAMPLE_QUERY_SOURCE_REF.catalogId,
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
    validateConnectedCatalogMetadata(dedupeConnectedCatalogs(migrateConnectedCatalogs(catalogs))),
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

  for (const incomingSchema of incoming.schemas) {
    const targetSchema = schemas.find((schema) => schema.name === incomingSchema.name);
    if (!targetSchema) {
      schemas.push({ ...incomingSchema, tables: [...incomingSchema.tables] });
      continue;
    }

    for (const incomingTable of incomingSchema.tables) {
      const incomingSourceKey = tableSourceKey(incomingTable, incoming);
      targetSchema.tables = targetSchema.tables.filter((existingTable) => {
        const sameSource = tableSourceKey(existingTable, existing) === incomingSourceKey;
        if (sameSource) {
          return false;
        }
        return true;
      });
      targetSchema.tables.push(incomingTable);
    }
  }

  const catalog = summarizeCatalog({
    ...incoming,
    id: incoming.id,
    alias: incoming.alias,
    connectedAt: existing.connectedAt || incoming.connectedAt,
    schemas,
  });

  return { catalog };
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
    if (isExplicitSampleCatalog(catalog)) {
      migrated.push({ ...catalog, catalogName: SAMPLE_QUERY_SOURCE.catalogName });
      continue;
    }
    for (const schema of catalog.schemas ?? []) {
      for (const table of schema.tables ?? []) {
        const logicalTable = logicalTableForPersistedTable(table);
        const connectionId = logicalTable?.resource?.connectionId;
        if (!logicalTable || !connectionId) {
          if (options.rejectInvalid) {
            throw new Error('connected catalog table omitted a valid logical identity');
          }
          continue;
        }
        migrated.push({
          ...catalog,
          id: connectionId,
          catalogName: catalog.catalogName?.trim() || catalogNameFor(catalog),
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
): TableNode | undefined {
  try {
    if (table.catalogMetadataJson) {
      return canonicalTableFromMetadataJson(table.catalogMetadataJson);
    }
    if (table.logicalTable) {
      canonicalTableIdentityKey(table.logicalTable);
      return clone(TableNodeSchema, table.logicalTable);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function catalogNameFor(catalog: ConnectedCatalog): string {
  if (catalog.kind === 'local') return 'local-delta';
  if (
    catalog.kind === 'object_store' &&
    (catalog.provider === 'gcs' || catalog.provider === 's3')
  ) {
    return `public-${catalog.provider}`;
  }
  return catalog.alias;
}

function isExplicitSampleCatalog(catalog: ConnectedCatalog): boolean {
  return (
    catalog.id === SAMPLE_QUERY_SOURCE_REF.catalogId &&
    catalog.schemas.some(
      (schema) =>
        schema.name === SAMPLE_QUERY_SOURCE_REF.schemaName &&
        schema.tables.some(
          (table) =>
            table.name === SAMPLE_QUERY_SOURCE_REF.tableName &&
            table.manifestUrl === SAMPLE_QUERY_SOURCE.manifestUrl,
        ),
    )
  );
}

function validateConnectedCatalogMetadata(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  for (const catalog of catalogs) {
    const schemaNames = new Set<string>();
    for (const schema of catalog.schemas) {
      if (schemaNames.has(schema.name)) {
        throw new Error('persisted catalog contains a duplicate schema identity');
      }
      schemaNames.add(schema.name);
      const tableIdentities = new Set<string>();
      for (const table of schema.tables) {
        const identity = table.logicalTable
          ? canonicalTableIdentityKey(table.logicalTable)
          : tableSourceKey(table, catalog);
        if (tableIdentities.has(identity)) {
          throw new Error('persisted catalog contains a duplicate table identity');
        }
        tableIdentities.add(identity);
        if (!table.catalogMetadataJson) continue;
        const source = querySourceForConnectedTableRef([catalog], {
          catalogId: catalog.id,
          schemaName: schema.name,
          tableName: table.name,
        });
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
