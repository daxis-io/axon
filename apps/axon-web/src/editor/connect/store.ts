// Persistence for connected catalogs from the Connect Catalog workflow.
// Stored in localStorage so non-sensitive catalog metadata survives reloads.

import { availabilityForSource, type ObjectStoreProviderId } from './data.ts';
import type { ConnectResult, ConnectedCatalog, ConnectedCatalogSchema } from './types.ts';
import { createLocalStorageKeyValueStore } from '../../persistence/key-value.ts';
import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';
import { SAMPLE_QUERY_SOURCE } from '../../services/query-source.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';
export const DEFAULT_AXON_CATALOG_ALIAS = 'workspace';
export const DEFAULT_AXON_SCHEMA_NAME = 'default';

export type ConnectedCatalogUpsertResult = {
  catalogs: ConnectedCatalog[];
  replaced: ConnectedCatalog[];
};

export const SAMPLE_CONNECTED_CATALOG: ConnectedCatalog = {
  id: 'sample-lake-fixture',
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
  afterRead: dedupeConnectedCatalogs,
  beforeWrite: (catalogs) => dedupeConnectedCatalogs(durableConnectedCatalogs(catalogs)),
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
  const incomingKeys = tableSourceKeys(catalog);
  const targetCatalogKey = catalogAliasKey(catalog);
  const replaced: ConnectedCatalog[] = [];
  const retained: ConnectedCatalog[] = [];
  let merged = catalog;

  for (const existing of catalogs) {
    if (catalogAliasKey(existing) === targetCatalogKey) {
      const merge = mergeCatalogs(existing, merged);
      merged = merge.catalog;
      if (merge.removed) replaced.push(merge.removed);
      continue;
    }

    const pruned = removeTablesBySourceKeys(existing, incomingKeys);
    if (pruned.removed) replaced.push(pruned.removed);
    if (pruned.catalog) retained.push(pruned.catalog);
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
  const disc = result.discovered;
  const catalogAlias = normalizeCatalogAlias(alias) || DEFAULT_AXON_CATALOG_ALIAS;
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
          id: `${schemaName}.${t.name}`,
          name: t.name,
          snapshot: t.snapshot,
          rows: t.rows,
          files: t.files,
          size: t.size,
          protocol: t.protocol,
          features: t.features,
          uri: source === 'object_store' ? (t.uri ?? form.uri) : t.name,
          manifestUrl: t.manifestUrl,
          descriptorResolutionMetrics: t.descriptorResolutionMetrics,
          localRegistryId: source === 'local' ? form.localDelta?.registryId : undefined,
          localPersistence: source === 'local' ? form.localDelta?.persistence : undefined,
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
    id: catalogIdForAlias(catalogAlias),
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
  const removedSchemas: ConnectedCatalogSchema[] = [];
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

    const removedTables: typeof targetSchema.tables = [];
    for (const incomingTable of incomingSchema.tables) {
      const incomingSourceKey = tableSourceKey(incomingTable, incoming);
      targetSchema.tables = targetSchema.tables.filter((existingTable) => {
        const samePath = existingTable.name === incomingTable.name;
        const sameSource = tableSourceKey(existingTable, existing) === incomingSourceKey;
        if (samePath || sameSource) {
          removedTables.push(existingTable);
          return false;
        }
        return true;
      });
      targetSchema.tables.push(incomingTable);
    }
    if (removedTables.length > 0) {
      removedSchemas.push({ name: targetSchema.name, tables: removedTables });
    }
  }

  const catalog = summarizeCatalog({
    ...incoming,
    id: existing.id,
    alias: existing.alias,
    connectedAt: existing.connectedAt || incoming.connectedAt,
    schemas,
  });

  return {
    catalog,
    removed: removedSchemas.length > 0 ? { ...existing, schemas: removedSchemas } : undefined,
  };
}

function removeTablesBySourceKeys(
  catalog: ConnectedCatalog,
  sourceKeys: Set<string>,
): { catalog?: ConnectedCatalog; removed?: ConnectedCatalog } {
  if (sourceKeys.size === 0) return { catalog };

  const keptSchemas: ConnectedCatalogSchema[] = [];
  const removedSchemas: ConnectedCatalogSchema[] = [];

  for (const schema of catalog.schemas) {
    const keptTables = [];
    const removedTables = [];
    for (const table of schema.tables) {
      if (sourceKeys.has(tableSourceKey(table, catalog))) removedTables.push(table);
      else keptTables.push(table);
    }
    if (keptTables.length > 0) keptSchemas.push({ ...schema, tables: keptTables });
    if (removedTables.length > 0) removedSchemas.push({ ...schema, tables: removedTables });
  }

  return {
    catalog: keptSchemas.length > 0 ? { ...catalog, schemas: keptSchemas } : undefined,
    removed: removedSchemas.length > 0 ? { ...catalog, schemas: removedSchemas } : undefined,
  };
}

function dedupeConnectedCatalogs(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  let deduped: ConnectedCatalog[] = [];
  for (let i = catalogs.length - 1; i >= 0; i -= 1) {
    deduped = upsertConnectedCatalog(deduped, catalogs[i]).catalogs;
  }
  return deduped;
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

function tableSourceKeys(catalog: ConnectedCatalog): Set<string> {
  return new Set(
    catalog.schemas.flatMap((schema) =>
      schema.tables.map((table) => tableSourceKey(table, catalog)),
    ),
  );
}

function tableSourceKey(
  table: ConnectedCatalogSchema['tables'][number],
  catalog: ConnectedCatalog,
): string {
  return table.source?.canonicalKey ?? legacyTableSourceKey(table, catalog);
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

function catalogAliasKey(catalog: ConnectedCatalog): string {
  return normalizeCatalogAlias(catalog.alias);
}

function normalizeCatalogAlias(alias: string | undefined): string {
  return (alias ?? '').trim().toLowerCase();
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
