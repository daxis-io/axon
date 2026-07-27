import { clone, create, fromJson, type JsonValue } from '@bufbuild/protobuf';
import {
  TableMetadataSchema,
  TableNodeSchema,
  TableType,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import {
  CanonicalResourceRefSchema,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';

export const LOCAL_DELTA_PROVIDER_NAMESPACE = 'axon.local-delta/v1';

export type LocalDeltaCanonicalTableInput = Readonly<{
  registryId: string;
  tableName: string;
}>;

type PublicObjectStorageCanonicalTableBase = Readonly<{
  connectionId: string;
  normalizedTableUri: string;
  tableName: string;
}>;

export type PublicObjectStorageCanonicalTableInput =
  | (PublicObjectStorageCanonicalTableBase & Readonly<{ provider: 'gcs'; region?: never }>)
  | (PublicObjectStorageCanonicalTableBase & Readonly<{ provider: 's3'; region: string }>);

export function localDeltaConnectionId(registryId: string): string {
  const opaqueId = requiredIdentityPart(registryId, 'local registry ID');
  return `axon-connection://local-delta/${encodeURIComponent(opaqueId)}`;
}

export function createLocalDeltaCanonicalTable(input: LocalDeltaCanonicalTableInput): TableNode {
  const registryId = requiredIdentityPart(input.registryId, 'local registry ID');
  const tableName = requiredIdentityPart(input.tableName, 'table name');
  return create(TableNodeSchema, {
    resource: create(CanonicalResourceRefSchema, {
      connectionId: localDeltaConnectionId(registryId),
      providerNamespace: LOCAL_DELTA_PROVIDER_NAMESPACE,
      kind: ResourceKind.TABLE,
      identity: {
        case: 'providerObjectId',
        value: registryId,
      },
    }),
    tableType: TableType.TABLE,
    name: tableName,
  });
}

export function createPublicObjectStorageCanonicalTable(
  input: PublicObjectStorageCanonicalTableInput,
): TableNode {
  const tableName = requiredIdentityPart(input.tableName, 'table name');
  const connectionId = requiredIdentityPart(input.connectionId, 'connection ID');
  if (!connectionId.startsWith(`axon-connection://public-${input.provider}/`)) {
    throw new Error('public connection ID did not match the provider');
  }
  const normalizedTableUri = normalizedPublicTableUri(input.provider, input.normalizedTableUri);
  const parsed = new URL(normalizedTableUri);
  const expectedConnectionId =
    input.provider === 'gcs'
      ? `axon-connection://public-gcs/${encodeURIComponent(parsed.hostname)}`
      : `axon-connection://public-s3/${encodeURIComponent(
          normalizedS3Region(input.region),
        )}/${encodeURIComponent(parsed.hostname)}`;
  if (connectionId !== expectedConnectionId) {
    throw new Error('public connection ID did not match the normalized table root');
  }
  return create(TableNodeSchema, {
    resource: create(CanonicalResourceRefSchema, {
      connectionId,
      providerNamespace: `axon.public-${input.provider}/v1`,
      kind: ResourceKind.TABLE,
      identity: {
        case: 'canonicalLocator',
        value: normalizedTableUri,
      },
    }),
    tableType: TableType.TABLE,
    name: tableName,
  });
}

export function canonicalTableFromMetadataJson(
  metadataJson: Readonly<Record<string, unknown>>,
): TableNode {
  const metadata = fromJson(TableMetadataSchema, metadataJson as JsonValue);
  if (!metadata.table) {
    throw new Error('catalog metadata omitted canonical table identity');
  }
  return validatedCanonicalTable(metadata.table);
}

export function validatedCanonicalTable(table: TableNode): TableNode {
  const resource = table.resource;
  if (
    !resource ||
    !resource.connectionId.trim() ||
    !resource.providerNamespace.trim() ||
    resource.kind !== ResourceKind.TABLE ||
    (resource.identity.case !== 'providerObjectId' &&
      resource.identity.case !== 'canonicalLocator') ||
    !resource.identity.value.trim() ||
    !table.name.trim()
  ) {
    throw new Error('canonical table identity is invalid');
  }
  return clone(TableNodeSchema, table);
}

export function canonicalTableIdentityKey(table: TableNode): string {
  const validated = validatedCanonicalTable(table);
  const resource = validated.resource!;
  return JSON.stringify([
    resource.connectionId,
    resource.providerNamespace,
    resource.kind,
    resource.identity.case,
    resource.identity.value,
  ]);
}

function normalizedS3Region(region: string | undefined): string {
  const normalized = region?.trim().toLowerCase();
  if (!normalized || !/^[a-z]{2}(?:-[a-z]+)+-\d+$/.test(normalized)) {
    throw new Error('public S3 region is invalid');
  }
  return normalized;
}

function requiredIdentityPart(value: string, label: string): string {
  if (value.length === 0 || value.trim().length === 0) {
    throw new Error(`${label} must be non-empty`);
  }
  return value;
}

function normalizedPublicTableUri(provider: 'gcs' | 's3', value: string): string {
  const uri = requiredIdentityPart(value, 'public table URI');
  const parsed = new URL(uri);
  const scheme = provider === 'gcs' ? 'gs' : 's3';
  const path = parsed.pathname.replace(/^\/+|\/+$/g, '');
  if (
    parsed.protocol !== `${scheme}:` ||
    !parsed.hostname ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash ||
    !path ||
    uri !== `${scheme}://${parsed.hostname}/${path}`
  ) {
    throw new Error('public table URI must be normalized and capability-free');
  }
  return uri;
}
