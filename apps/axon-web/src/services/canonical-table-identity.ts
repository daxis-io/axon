import { create } from '@bufbuild/protobuf';
import {
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

export type PublicObjectStorageCanonicalTableInput = Readonly<{
  provider: 'gcs' | 's3';
  connectionId: string;
  normalizedTableUri: string;
  tableName: string;
}>;

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
