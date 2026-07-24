import { create, equals } from '@bufbuild/protobuf';
import {
  CanonicalResourceRefSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
  ResourceKind,
  type CanonicalResourceRef,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserAccessClass,
  BrowserReadDescriptorSchema,
  ReadDeniedSchema,
  ReadResolutionSchema,
  ReadResolutionReason,
  ResolvedBrowserReadSchema,
  type BrowserHttpSnapshotDescriptor,
  type ReadResolution,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  TableNodeSchema,
  TableType,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import type { Timestamp } from '@bufbuild/protobuf/wkt';
import type { AvailableQuerySourceSelection } from './query-source.ts';
import {
  loadLocalDeltaRuntime as loadDefaultLocalDeltaRuntime,
  LocalDeltaError,
} from './local-delta.ts';
import {
  parsePublicObjectStorageTableRoot,
  publicObjectStorageConnectionId,
  PublicObjectStorageError,
  type PublicObjectStorageProvider,
} from './object-storage.ts';

export interface DataAccessResolver {
  resolve(
    resource: CanonicalResourceRef,
    context: {
      executionId: string;
      deadline: Timestamp;
      snapshotVersion?: number;
      signal: AbortSignal;
    },
  ): Promise<ReadResolution>;
}

type LocalDeltaResolutionRuntime = Readonly<{
  registryId: string;
  schemaName: string;
  tableName: string;
  descriptor: BrowserHttpSnapshotDescriptor;
}>;

export type BrowserReadResolutionDependencies = Readonly<{
  loadLocalDeltaRuntime?: (
    registryId: string,
    options: {
      schemaName: string;
      tableName: string;
      snapshotVersion?: number;
    },
  ) => Promise<LocalDeltaResolutionRuntime>;
  loadPublicObjectStorageDescriptor?: (input: {
    provider: PublicObjectStorageProvider;
    tableUri: string;
    region?: string;
    snapshotVersion?: number;
    expectedSnapshotVersion?: number;
    signal: AbortSignal;
  }) => Promise<BrowserHttpSnapshotDescriptor>;
}>;

export function canonicalTableForSelection(selection: AvailableQuerySourceSelection): TableNode {
  return create(TableNodeSchema, {
    resource: canonicalResourceForSelection(selection),
    tableType: TableType.TABLE,
    name: selection.source.tableName,
  });
}

export function dataAccessResolverForSelection(
  selection: AvailableQuerySourceSelection,
  dependencies: BrowserReadResolutionDependencies = {},
): DataAccessResolver {
  switch (selection.source.kind) {
    case 'local_delta':
      return localDeltaResolver(selection, dependencies);
    case 'object_store_table_root':
      return publicObjectStorageResolver(selection, dependencies);
    case 'manifest':
      throw new TypeError(
        `query source '${selection.source.kind}' does not have a Slice 2 resolver yet`,
      );
  }
}

function canonicalResourceForSelection(
  selection: AvailableQuerySourceSelection,
): CanonicalResourceRef {
  const source = selection.source;
  switch (source.kind) {
    case 'local_delta':
      return create(CanonicalResourceRefSchema, {
        connectionId: `axon-connection://local-delta/${encodeURIComponent(source.localRegistryId)}`,
        providerNamespace: 'axon.local-delta/v1',
        kind: ResourceKind.TABLE,
        identity: {
          case: 'providerObjectId',
          value: source.localRegistryId,
        },
      });
    case 'object_store_table_root': {
      const root = parsePublicObjectStorageTableRoot({
        provider: source.provider,
        tableUri: source.tableUri,
        region: source.region,
      });
      return create(CanonicalResourceRefSchema, {
        connectionId: publicObjectStorageConnectionId(root),
        providerNamespace: `axon.public-${root.provider}/v1`,
        kind: ResourceKind.TABLE,
        identity: {
          case: 'canonicalLocator',
          value: root.tableUri,
        },
      });
    }
    case 'manifest':
      throw new TypeError(
        `query source '${source.kind}' does not have a Slice 2 canonicalizer yet`,
      );
  }
}

function publicObjectStorageResolver(
  selection: AvailableQuerySourceSelection,
  dependencies: BrowserReadResolutionDependencies,
): DataAccessResolver {
  if (selection.source.kind !== 'object_store_table_root') {
    throw new TypeError('public object-storage resolver requires a public table-root source');
  }
  const source = selection.source;
  const root = parsePublicObjectStorageTableRoot({
    provider: source.provider,
    tableUri: source.tableUri,
    region: source.region,
  });
  const expectedResource = canonicalResourceForSelection(selection);

  return {
    async resolve(resource, context) {
      if (!equals(CanonicalResourceRefSchema, resource, expectedResource)) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'canonical resource did not match the exact selected public table root',
          context.executionId,
        );
      }
      if (context.signal.aborted) {
        throw new DOMException('cancelled', 'AbortError');
      }
      if (!dependencies.loadPublicObjectStorageDescriptor) {
        return providerErrorResolution(
          ProviderErrorCode.UNAVAILABLE,
          'public object-storage descriptor loader is unavailable',
          context.executionId,
        );
      }

      let descriptor: BrowserHttpSnapshotDescriptor;
      try {
        descriptor = await dependencies.loadPublicObjectStorageDescriptor({
          provider: root.provider,
          tableUri: root.tableUri,
          region: root.region,
          snapshotVersion: context.snapshotVersion,
          expectedSnapshotVersion: context.snapshotVersion ?? source.snapshot,
          signal: context.signal,
        });
      } catch (error) {
        if (isAbortError(error) || context.signal.aborted) throw error;
        return providerErrorResolution(
          error instanceof PublicObjectStorageError &&
            (error.code === 'invalid_public_object_path' ||
              error.code === 'invalid_public_object_storage_uri')
            ? ProviderErrorCode.INVALID
            : ProviderErrorCode.UNAVAILABLE,
          error instanceof PublicObjectStorageError
            ? error.message
            : 'Public object-storage resolution was unavailable.',
          context.executionId,
        );
      }
      if (context.signal.aborted) {
        throw new DOMException('cancelled', 'AbortError');
      }
      if (descriptor.tableUri !== root.tableUri) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'public descriptor identity did not match the canonical table root',
          context.executionId,
        );
      }
      const expectedSnapshotVersion = context.snapshotVersion ?? source.snapshot;
      if (
        expectedSnapshotVersion !== undefined &&
        descriptor.snapshotVersion !== BigInt(expectedSnapshotVersion)
      ) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'public descriptor snapshot did not match the requested snapshot',
          context.executionId,
        );
      }

      return create(ReadResolutionSchema, {
        outcome: {
          case: 'browserRead',
          value: create(ResolvedBrowserReadSchema, {
            resource,
            descriptor: create(BrowserReadDescriptorSchema, {
              descriptor: {
                case: 'snapshot',
                value: descriptor,
              },
            }),
            accessClass: BrowserAccessClass.PUBLIC,
            correlationId: context.executionId,
            provenance: {
              resolverId: `axon.public-${root.provider}/v1`,
              resolutionId: `${context.executionId}:public-${root.provider}`,
            },
          }),
        },
      });
    },
  };
}

function localDeltaResolver(
  selection: AvailableQuerySourceSelection,
  dependencies: BrowserReadResolutionDependencies,
): DataAccessResolver {
  if (selection.source.kind !== 'local_delta') {
    throw new TypeError('local Delta resolver requires a local Delta source');
  }
  const source = selection.source;
  const expectedResource = canonicalResourceForSelection(selection);

  return {
    async resolve(resource, context) {
      if (!equals(CanonicalResourceRefSchema, resource, expectedResource)) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'canonical resource did not match the exact selected local Delta table',
          context.executionId,
        );
      }
      if (context.signal.aborted) {
        throw new DOMException('cancelled', 'AbortError');
      }
      let runtime: LocalDeltaResolutionRuntime;
      try {
        runtime = await (dependencies.loadLocalDeltaRuntime ?? loadDefaultLocalDeltaRuntime)(
          source.localRegistryId,
          {
            schemaName: source.schemaName,
            tableName: source.tableName,
            snapshotVersion: context.snapshotVersion ?? source.snapshot,
          },
        );
      } catch (error) {
        if (isAbortError(error) || context.signal.aborted) throw error;
        if (error instanceof LocalDeltaError && error.code === 'registry_unavailable') {
          return create(ReadResolutionSchema, {
            outcome: {
              case: 'denied',
              value: create(ReadDeniedSchema, {
                resource,
                reason: ReadResolutionReason.SESSION_REQUIRED,
                message: error.message,
              }),
            },
          });
        }
        return providerErrorResolution(
          error instanceof LocalDeltaError
            ? ProviderErrorCode.INVALID
            : ProviderErrorCode.UNAVAILABLE,
          error instanceof LocalDeltaError
            ? error.message
            : 'Local Delta resolution was unavailable.',
          context.executionId,
        );
      }
      if (context.signal.aborted) {
        throw new DOMException('cancelled', 'AbortError');
      }
      if (
        runtime.registryId !== source.localRegistryId ||
        runtime.schemaName !== source.schemaName ||
        runtime.tableName !== source.tableName
      ) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'local Delta runtime identity changed during resolution',
          context.executionId,
        );
      }

      return create(ReadResolutionSchema, {
        outcome: {
          case: 'browserRead',
          value: create(ResolvedBrowserReadSchema, {
            resource,
            descriptor: create(BrowserReadDescriptorSchema, {
              descriptor: {
                case: 'snapshot',
                value: runtime.descriptor,
              },
            }),
            accessClass: BrowserAccessClass.LOCAL_HANDLE,
            notAfter: context.deadline,
            correlationId: context.executionId,
            provenance: {
              resolverId: 'axon.local-delta/v1',
              resolutionId: `${context.executionId}:local-delta`,
            },
          }),
        },
      });
    },
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function providerErrorResolution(
  code: ProviderErrorCode,
  message: string,
  correlationId: string,
): ReadResolution {
  return create(ReadResolutionSchema, {
    outcome: {
      case: 'error',
      value: create(ProviderErrorSchema, {
        code,
        message,
        correlationId,
      }),
    },
  });
}
