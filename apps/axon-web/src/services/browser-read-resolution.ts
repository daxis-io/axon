import { clone, create, equals } from '@bufbuild/protobuf';
import {
  CanonicalResourceRefSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
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
  type ResolvedBrowserRead,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  TableNodeSchema,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import { ExecutionRejectionReason } from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import type { Timestamp } from '@bufbuild/protobuf/wkt';
import {
  isExplicitSampleFixtureSelection,
  type AvailableQuerySourceSelection,
} from './query-source.ts';
import {
  loadLocalDeltaRuntime as loadDefaultLocalDeltaRuntime,
  LocalDeltaError,
} from './local-delta.ts';
import {
  canonicalTableForQuerySource,
  createSampleFixtureCanonicalTable,
  SAMPLE_FIXTURE_PROVIDER_NAMESPACE,
  sameCanonicalTableIdentity,
} from './canonical-table-identity.ts';
import {
  parsePublicObjectStorageTableRoot,
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

type BrowserReadResolutionFailureCode =
  | 'access_denied'
  | 'unsupported_feature'
  | 'execution_failed';

export class BrowserReadResolutionFailure extends Error {
  constructor(
    message: string,
    readonly code: BrowserReadResolutionFailureCode,
    readonly rejectionReason: ExecutionRejectionReason = defaultRejectionReason(code),
  ) {
    super(message);
    this.name = 'BrowserReadResolutionFailure';
  }
}

function defaultRejectionReason(code: BrowserReadResolutionFailureCode): ExecutionRejectionReason {
  switch (code) {
    case 'access_denied':
      return ExecutionRejectionReason.ACCESS_DENIED;
    case 'unsupported_feature':
      return ExecutionRejectionReason.UNSUPPORTED;
    case 'execution_failed':
      return ExecutionRejectionReason.UNAVAILABLE;
  }
}

export function requireBrowserReadResolution(resolution: ReadResolution): ResolvedBrowserRead {
  switch (resolution.outcome.case) {
    case 'browserRead':
      return resolution.outcome.value;
    case 'remoteRequired':
      throw new BrowserReadResolutionFailure(
        resolution.outcome.value.message || 'Browser execution requires remote enforcement.',
        'unsupported_feature',
        ExecutionRejectionReason.UNSUPPORTED,
      );
    case 'denied':
      throw new BrowserReadResolutionFailure(
        resolution.outcome.value.message || 'Browser read access was denied.',
        'access_denied',
        ExecutionRejectionReason.ACCESS_DENIED,
      );
    case 'error': {
      const providerError = resolution.outcome.value;
      if (providerError.code === ProviderErrorCode.BLOCKED) {
        throw new BrowserReadResolutionFailure(
          providerError.message || 'Browser read resolution was blocked.',
          'access_denied',
          ExecutionRejectionReason.ACCESS_DENIED,
        );
      }
      throw new BrowserReadResolutionFailure(
        providerError.message || 'Browser read resolution failed.',
        'execution_failed',
        providerError.code === ProviderErrorCode.INVALID
          ? ExecutionRejectionReason.INVALID_REQUEST
          : ExecutionRejectionReason.UNAVAILABLE,
      );
    }
    case undefined:
      throw new BrowserReadResolutionFailure(
        'Data access resolver returned no outcome.',
        'execution_failed',
        ExecutionRejectionReason.INVALID_REQUEST,
      );
  }
}

type LocalDeltaResolutionRuntime = Readonly<{
  registryId: string;
  schemaName: string;
  tableName: string;
  descriptor: BrowserHttpSnapshotDescriptor;
}>;

type BrowserReadResolutionDependencies = Readonly<{
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
  loadSampleFixtureDescriptor?: (input: {
    snapshotVersion?: number;
    signal: AbortSignal;
  }) => Promise<BrowserHttpSnapshotDescriptor>;
}>;

const SAMPLE_FIXTURE_TABLE_URI = 'gs://axon-sandbox/prod-like-events';

export function canonicalTableForSelection(selection: AvailableQuerySourceSelection): TableNode {
  const projected = projectedCanonicalTableForSelection(selection);
  if (!sameCanonicalTableIdentity(projected, selection.ref)) {
    throw new Error('logical table selection did not match its query source identity');
  }
  return clone(TableNodeSchema, selection.ref);
}

function projectedCanonicalTableForSelection(selection: AvailableQuerySourceSelection): TableNode {
  if (selection.source.kind !== 'manifest') {
    return canonicalTableForQuerySource(selection.source);
  }
  assertExactSampleFixtureSelection(selection);
  return createSampleFixtureCanonicalTable(selection.source.tableName);
}

export function dataAccessResolverForSelection(
  selection: AvailableQuerySourceSelection,
  dependencies: BrowserReadResolutionDependencies = {},
): DataAccessResolver {
  if (selection.source.kind === 'manifest') {
    assertExactSampleFixtureSelection(selection);
    return sampleFixtureResolver(selection, dependencies);
  }
  switch (selection.source.kind) {
    case 'local_delta':
      return localDeltaResolver(selection, dependencies);
    case 'object_store_table_root':
      return publicObjectStorageResolver(selection, dependencies);
  }
}

function canonicalResourceForSelection(
  selection: AvailableQuerySourceSelection,
): CanonicalResourceRef {
  return canonicalTableForSelection(selection).resource!;
}

function sampleFixtureResolver(
  selection: AvailableQuerySourceSelection,
  dependencies: BrowserReadResolutionDependencies,
): DataAccessResolver {
  assertExactSampleFixtureSelection(selection);
  const expectedResource = canonicalResourceForSelection(selection);
  return {
    async resolve(resource, context) {
      if (!equals(CanonicalResourceRefSchema, resource, expectedResource)) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'canonical resource did not match the explicit sample fixture',
          context.executionId,
        );
      }
      if (context.signal.aborted) {
        throw new DOMException('cancelled', 'AbortError');
      }
      if (!dependencies.loadSampleFixtureDescriptor) {
        return providerErrorResolution(
          ProviderErrorCode.UNAVAILABLE,
          'sample fixture descriptor loader is unavailable',
          context.executionId,
        );
      }

      let descriptor: BrowserHttpSnapshotDescriptor;
      try {
        descriptor = await dependencies.loadSampleFixtureDescriptor({
          snapshotVersion: context.snapshotVersion,
          signal: context.signal,
        });
      } catch (error) {
        if (isAbortError(error) || context.signal.aborted) throw error;
        return providerErrorResolution(
          ProviderErrorCode.UNAVAILABLE,
          error instanceof Error ? error.message : 'Sample fixture resolution was unavailable.',
          context.executionId,
        );
      }
      if (
        descriptor.tableUri !== SAMPLE_FIXTURE_TABLE_URI ||
        (context.snapshotVersion !== undefined &&
          descriptor.snapshotVersion !== BigInt(context.snapshotVersion))
      ) {
        return providerErrorResolution(
          ProviderErrorCode.INVALID,
          'sample fixture descriptor identity did not match the explicit fixture',
          context.executionId,
        );
      }

      return create(ReadResolutionSchema, {
        outcome: {
          case: 'browserRead',
          value: create(ResolvedBrowserReadSchema, {
            resource,
            descriptor: create(BrowserReadDescriptorSchema, {
              descriptor: { case: 'snapshot', value: descriptor },
            }),
            accessClass: BrowserAccessClass.PUBLIC,
            correlationId: context.executionId,
            provenance: {
              resolverId: SAMPLE_FIXTURE_PROVIDER_NAMESPACE,
              resolutionId: `${context.executionId}:sample-fixture`,
            },
          }),
        },
      });
    },
  };
}

function assertExactSampleFixtureSelection(selection: AvailableQuerySourceSelection): void {
  if (!isExplicitSampleFixtureSelection(selection)) {
    throw new TypeError('manifest execution is limited to the explicit sample fixture selection');
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
          error instanceof Error ? error.message : 'Local Delta resolution was unavailable.',
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
