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

function requiredIdentityPart(value: string, label: string): string {
  if (value.length === 0 || value.trim().length === 0) {
    throw new Error(`${label} must be non-empty`);
  }
  return value;
}
