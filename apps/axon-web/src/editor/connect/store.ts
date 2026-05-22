// Persistence for connected catalogs from the Connect Catalog workflow.
// Stored in localStorage so non-sensitive catalog metadata survives reloads.

import { availabilityForSource, type ObjectStoreProviderId } from './data.ts';
import type { ConnectResult, ConnectedCatalog, SchemaSelection } from './types.ts';
import type { ConnectorFeatureFlags } from '../../services/connector-features.ts';
import { SAMPLE_QUERY_SOURCE } from '../../services/query-source.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';

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
          name: SAMPLE_QUERY_SOURCE.tableName,
          snapshot: 3,
          rows: 6,
          files: 1,
          size: 'fixture',
          protocol: 'r2/w5',
          manifestUrl: SAMPLE_QUERY_SOURCE.manifestUrl,
        },
      ],
    },
  ],
};

export function loadConnectedCatalogs(): ConnectedCatalog[] {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [SAMPLE_CONNECTED_CATALOG];
    const parsed = JSON.parse(raw) as ConnectedCatalog[];
    if (!Array.isArray(parsed)) return [SAMPLE_CONNECTED_CATALOG];
    return parsed;
  } catch {
    return [SAMPLE_CONNECTED_CATALOG];
  }
}

export function catalogsAvailableForFeatures(
  catalogs: ConnectedCatalog[],
  connectorFeatures: ConnectorFeatureFlags,
): ConnectedCatalog[] {
  return catalogs.filter(
    (catalog) => availabilityForSource(catalog.kind, connectorFeatures).enabled,
  );
}

export function saveConnectedCatalogs(catalogs: ConnectedCatalog[]): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(durableConnectedCatalogs(catalogs)));
  } catch {
    // ignore quota / disabled storage
  }
}

export function buildCatalogFromResult(result: ConnectResult): ConnectedCatalog {
  const { source, form, alias, selection } = result;
  const disc = result.discovered;
  const schemas = disc.schemas
    .map((s) => {
      const sel: SchemaSelection = selection[s.name] ?? (s.included ? 'all' : 'none');
      if (sel === 'none') return null;
      let tables = s.tables;
      if (sel !== 'all') {
        if ('except' in sel) tables = s.tables.filter((t) => !sel.except.includes(t.name));
        else tables = s.tables.filter((t) => sel.only.includes(t.name));
      }
      return {
        name: s.name,
        tables: tables.map((t) => ({
          name: t.name,
          snapshot: t.snapshot,
          rows: t.rows,
          files: t.files,
          size: t.size,
          protocol: t.protocol,
          features: t.features,
          uri: source === 'object_store' ? (t.uri ?? form.uri) : t.name,
          manifestUrl: t.manifestUrl,
          localRegistryId: source === 'local' ? form.localDelta?.registryId : undefined,
          localPersistence: source === 'local' ? form.localDelta?.persistence : undefined,
        })),
      };
    })
    .filter((s): s is NonNullable<typeof s> => s != null);

  const id = (alias || source) + '-' + Math.random().toString(36).slice(2, 6);

  const storage =
    source === 'local'
      ? (form.localDelta?.storageLabel ?? form.path)
      : source === 'object_store'
        ? form.uri
        : source === 'delta_share'
          ? form.ds_mode === 'profile'
            ? form.ds_profile_name
            : form.ds_endpoint
          : form.uc_bff_url || form.uc_host;

  return {
    id,
    alias: alias || defaultAlias(source),
    kind: source,
    provider: source === 'object_store' ? (form.provider as ObjectStoreProviderId) : undefined,
    storage,
    host:
      source === 'unity_catalog'
        ? form.uc_host
        : source === 'delta_share'
          ? form.ds_endpoint
          : undefined,
    path: source === 'local' ? form.path : undefined,
    region:
      source === 'object_store'
        ? form.region || 'auto'
        : source === 'unity_catalog'
          ? 'brokered'
          : source === 'delta_share'
            ? 'provider-vended'
            : '—',
    status: 'connected',
    connectedAt: 'just now',
    schemas,
  };
}

function durableConnectedCatalogs(catalogs: ConnectedCatalog[]): ConnectedCatalog[] {
  return catalogs
    .map((catalog) => {
      if (catalog.kind !== 'local') return catalog;
      const schemas = catalog.schemas
        .map((schema) => ({
          ...schema,
          tables: schema.tables.filter((table) => table.localPersistence !== 'session_handles'),
        }))
        .filter((schema) => schema.tables.length > 0);
      return schemas.length > 0 ? { ...catalog, schemas } : null;
    })
    .filter((catalog): catalog is ConnectedCatalog => catalog !== null);
}

function defaultAlias(s: ConnectResult['source']) {
  return {
    local: 'local-files',
    object_store: 'object-store',
    unity_catalog: 'unity-catalog',
    delta_share: 'delta-share',
  }[s];
}
