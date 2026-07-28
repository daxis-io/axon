import {
  LOCAL_DELTA_PROVIDER_NAMESPACE,
  localDeltaConnectionId,
} from '../services/canonical-table-identity.ts';
import {
  parsePublicObjectStorageTableRoot,
  publicObjectStorageConnectionId,
} from '../services/object-storage.ts';
import {
  SAMPLE_QUERY_SOURCE,
  type QuerySourceIdentity,
  type QuerySourceSelection,
  type QueryTableSource,
  querySourceIdentity,
} from '../services/query-source.ts';

export { querySourceIdentity };
export type { QuerySourceIdentity };

function catalogRootKey() {
  return ['catalog'] as const;
}

type CatalogCanonicalIdentity = Readonly<{
  providerNamespace: string;
  connectionId: string;
  authority: 'fixture' | 'non-session';
  identityArm: 'canonicalLocator' | 'providerObjectId';
  identityValue: string;
}>;

function catalogCanonicalIdentity(source: QueryTableSource): CatalogCanonicalIdentity {
  if (source.kind === 'manifest') {
    if (
      source.catalogName !== SAMPLE_QUERY_SOURCE.catalogName ||
      source.schemaName !== SAMPLE_QUERY_SOURCE.schemaName ||
      source.tableName !== SAMPLE_QUERY_SOURCE.tableName ||
      source.manifestUrl !== SAMPLE_QUERY_SOURCE.manifestUrl ||
      source.storage !== SAMPLE_QUERY_SOURCE.storage ||
      source.region !== SAMPLE_QUERY_SOURCE.region
    ) {
      throw new Error('manifest catalog source is not the explicit sample fixture');
    }
    return {
      providerNamespace: 'axon.fixture/v1',
      connectionId: 'axon-connection://fixture/sample-lake',
      authority: 'fixture',
      identityArm: 'canonicalLocator',
      identityValue: 'axon-fixture://sample-lake/prod_like/events',
    };
  }

  if (source.kind === 'local_delta') {
    return {
      providerNamespace: LOCAL_DELTA_PROVIDER_NAMESPACE,
      connectionId: localDeltaConnectionId(source.localRegistryId),
      authority: 'non-session',
      identityArm: 'providerObjectId',
      identityValue: source.localRegistryId,
    };
  }

  const root = parsePublicObjectStorageTableRoot({
    provider: source.provider,
    tableUri: source.tableUri,
    region: source.region,
  });
  return {
    providerNamespace: `axon.public-${source.provider}/v1`,
    connectionId: publicObjectStorageConnectionId(root),
    authority: 'non-session',
    identityArm: 'canonicalLocator',
    identityValue: root.tableUri,
  };
}

function catalogConnectionKey(source: QueryTableSource) {
  const identity = catalogCanonicalIdentity(source);
  return [
    ...catalogRootKey(),
    'provider',
    identity.providerNamespace,
    'connection',
    identity.connectionId,
    'authority',
    identity.authority,
  ] as const;
}

function catalogTableKey(source: QueryTableSource) {
  const identity = catalogCanonicalIdentity(source);
  return [
    ...catalogConnectionKey(source),
    'resource',
    'table',
    identity.identityArm,
    identity.identityValue,
    'snapshot',
    source.snapshot ?? null,
  ] as const;
}

export function sameCatalogConnection(left: QueryTableSource, right: QueryTableSource): boolean {
  const leftKey = catalogConnectionKey(left);
  const rightKey = catalogConnectionKey(right);
  return leftKey.every((value, index) => Object.is(value, rightKey[index]));
}

export const queryKeys = {
  catalog: {
    root: catalogRootKey,
    connection: catalogConnectionKey,
    source: catalogConnectionKey,
    table: catalogTableKey,
    tableDerived: (source: QueryTableSource) =>
      [...catalogTableKey(source), 'table-derived'] as const,
    commits: (source: QueryTableSource) => [...catalogTableKey(source), 'commits'] as const,
    unavailable: (
      selection: Extract<QuerySourceSelection, { kind: 'unavailable' }>,
      resource: 'catalog' | 'commits',
    ) =>
      [
        ...catalogRootKey(),
        'unavailable',
        resource,
        selection.reason,
        selection.ref?.resource?.connectionId ?? null,
        selection.ref?.resource?.providerNamespace ?? null,
        selection.ref?.resource?.identity.value ?? null,
      ] as const,
  },
  local: {
    root: () => ['local'] as const,
    history: () => [...queryKeys.local.root(), 'history'] as const,
    saved: () => [...queryKeys.local.root(), 'saved'] as const,
  },
} as const;
