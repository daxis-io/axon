// Persistence for connected catalogs from the Connect Catalog workflow.
// Stored in localStorage so the demo state survives reloads.

import type { ObjectStoreProviderId } from './data.ts';
import { DISCOVERED } from './data.ts';
import type { ConnectResult, ConnectedCatalog, SchemaSelection } from './types.ts';

const STORAGE_KEY = 'axon.connect.catalogs.v1';

export function loadConnectedCatalogs(): ConnectedCatalog[] {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as ConnectedCatalog[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function saveConnectedCatalogs(catalogs: ConnectedCatalog[]): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(catalogs));
  } catch {
    // ignore quota / disabled storage
  }
}

export function buildCatalogFromResult(result: ConnectResult): ConnectedCatalog {
  const { source, form, alias, selection } = result;
  const disc = DISCOVERED[source];
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
        })),
      };
    })
    .filter((s): s is NonNullable<typeof s> => s != null);

  const id = (alias || source) + '-' + Math.random().toString(36).slice(2, 6);

  const storage =
    source === 'local'
      ? form.path
      : source === 'object_store'
        ? form.uri
        : source === 'delta_share'
          ? form.ds_mode === 'profile'
            ? form.ds_profile_name
            : form.ds_endpoint
          : form.uc_host;

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
          ? 'auto'
          : source === 'delta_share'
            ? 'provider-vended'
            : '—',
    status: 'connected',
    connectedAt: 'just now',
    schemas,
  };
}

function defaultAlias(s: ConnectResult['source']) {
  return {
    local: 'local-files',
    object_store: 'object-store',
    unity_catalog: 'unity-catalog',
    delta_share: 'delta-share',
  }[s];
}
