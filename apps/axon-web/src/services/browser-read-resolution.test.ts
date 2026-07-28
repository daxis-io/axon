import { create } from '@bufbuild/protobuf';
import { timestampFromMs, timestampMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it, vi } from 'vitest';
import {
  CanonicalResourceRefSchema,
  ProviderErrorCode,
  ProviderErrorSchema,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserAccessClass,
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  PartitionColumnType,
  ReadDeniedSchema,
  ReadResolutionSchema,
  ReadResolutionReason,
  RemoteRequiredSchema,
  ResolvedBrowserReadSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import { ExecutionRejectionReason } from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  SAMPLE_QUERY_SOURCE,
  SAMPLE_QUERY_SOURCE_REF,
  type AvailableQuerySourceSelection,
} from './query-source.ts';
import { LocalDeltaError } from './local-delta.ts';
import {
  createLocalDeltaCanonicalTable,
  createPublicObjectStorageCanonicalTable,
} from './canonical-table-identity.ts';
import {
  BrowserReadResolutionFailure,
  canonicalTableForSelection,
  dataAccessResolverForSelection,
  requireBrowserReadResolution,
} from './browser-read-resolution.ts';

const localSelection: AvailableQuerySourceSelection = {
  kind: 'resource',
  ref: createLocalDeltaCanonicalTable({
    registryId: 'local events/2026',
    tableName: 'events',
  }),
  source: {
    kind: 'local_delta',
    catalogName: 'Local events',
    schemaName: 'default',
    tableName: 'events',
    localRegistryId: 'local events/2026',
    storage: 'browser-cache://local',
    region: 'browser-local',
    snapshot: 12,
  },
};

const publicGcsSelection: AvailableQuerySourceSelection = {
  kind: 'resource',
  ref: createPublicObjectStorageCanonicalTable({
    provider: 'gcs',
    connectionId: 'axon-connection://public-gcs/Public-Bucket',
    normalizedTableUri: 'gs://Public-Bucket/events/table',
    tableName: 'events',
  }),
  source: {
    kind: 'object_store_table_root',
    provider: 'gcs',
    catalogName: 'Public GCS',
    schemaName: 'default',
    tableName: 'events',
    tableUri: ' gs://Public-Bucket/events/table/ ',
    storage: 'gs://Public-Bucket/events/table',
    region: 'global',
    snapshot: 12,
  },
};

const publicS3Selection: AvailableQuerySourceSelection = {
  kind: 'resource',
  ref: createPublicObjectStorageCanonicalTable({
    provider: 's3',
    connectionId: 'axon-connection://public-s3/us-east-2/public-bucket',
    normalizedTableUri: 's3://public-bucket/events/table',
    tableName: 'events',
    region: 'us-east-2',
  }),
  source: {
    kind: 'object_store_table_root',
    provider: 's3',
    catalogName: 'Public S3',
    schemaName: 'default',
    tableName: 'events',
    tableUri: 's3://public-bucket/events/table/',
    storage: 's3://public-bucket/events/table',
    region: ' US-EAST-2 ',
    snapshot: 14,
  },
};

describe('browser read canonical identity', () => {
  it.each([
    ['local Delta', localSelection],
    ['public GCS', publicGcsSelection],
    ['public S3', publicS3Selection],
  ] as const)(
    'keeps the Explorer %s TableNode unchanged through the E9 handoff',
    (_label, selection) => {
      expect(canonicalTableForSelection(selection)).toEqual(selection.ref);
    },
  );

  it.each([
    ['access_denied', ExecutionRejectionReason.ACCESS_DENIED],
    ['unsupported_feature', ExecutionRejectionReason.UNSUPPORTED],
    ['execution_failed', ExecutionRejectionReason.UNAVAILABLE],
  ] as const)(
    'maps %s resolver failures to a distinct generated admission reason',
    (code, expected) => {
      const error = new BrowserReadResolutionFailure('resolution failed', code);

      expect(error.rejectionReason).toBe(expected);
    },
  );

  it('treats invalid provider envelopes as invalid generated requests', () => {
    const error = new BrowserReadResolutionFailure(
      'invalid envelope',
      'execution_failed',
      ExecutionRejectionReason.INVALID_REQUEST,
    );

    expect(error.rejectionReason).toBe(ExecutionRejectionReason.INVALID_REQUEST);
  });

  it('closes every generated read-resolution arm before execution admission', () => {
    const resource = canonicalTableForSelection(localSelection).resource!;
    const browserRead = create(ResolvedBrowserReadSchema, {
      resource,
      accessClass: BrowserAccessClass.LOCAL_HANDLE,
      correlationId: 'execution-local',
    });

    expect(
      requireBrowserReadResolution(
        create(ReadResolutionSchema, {
          outcome: { case: 'browserRead', value: browserRead },
        }),
      ),
    ).toBe(browserRead);

    const cases = [
      {
        resolution: create(ReadResolutionSchema, {
          outcome: {
            case: 'remoteRequired',
            value: create(RemoteRequiredSchema, {
              resource,
              reason: ReadResolutionReason.POLICY_ENFORCEMENT_REQUIRED,
              message: 'remote enforcement is required',
            }),
          },
        }),
        code: 'unsupported_feature',
        reason: ExecutionRejectionReason.UNSUPPORTED,
      },
      {
        resolution: create(ReadResolutionSchema, {
          outcome: {
            case: 'denied',
            value: create(ReadDeniedSchema, {
              resource,
              reason: ReadResolutionReason.ACCESS_DENIED,
              message: 'read access was denied',
            }),
          },
        }),
        code: 'access_denied',
        reason: ExecutionRejectionReason.ACCESS_DENIED,
      },
      {
        resolution: create(ReadResolutionSchema, {
          outcome: {
            case: 'error',
            value: create(ProviderErrorSchema, {
              code: ProviderErrorCode.INVALID,
              message: 'provider returned an invalid envelope',
              correlationId: 'execution-local',
            }),
          },
        }),
        code: 'execution_failed',
        reason: ExecutionRejectionReason.INVALID_REQUEST,
      },
      {
        resolution: create(ReadResolutionSchema),
        code: 'execution_failed',
        reason: ExecutionRejectionReason.INVALID_REQUEST,
      },
    ] as const;

    for (const expected of cases) {
      try {
        requireBrowserReadResolution(expected.resolution);
        throw new Error('expected the non-browser resolution to fail');
      } catch (error) {
        expect(error).toBeInstanceOf(BrowserReadResolutionFailure);
        expect(error).toMatchObject({
          code: expected.code,
          rejectionReason: expected.reason,
        });
      }
    }
  });

  it('maps an exact local Delta selection to one generated table resource', async () => {
    expect(canonicalTableForSelection).toBeTypeOf('function');
    expect(canonicalTableForSelection(localSelection)).toMatchObject({
      name: 'events',
      resource: {
        connectionId: 'axon-connection://local-delta/local%20events%2F2026',
        providerNamespace: 'axon.local-delta/v1',
        kind: ResourceKind.TABLE,
        identity: {
          case: 'providerObjectId',
          value: 'local events/2026',
        },
      },
    });
  });

  it('revalidates local access and mints a fresh deadline-bound envelope per execution', async () => {
    const descriptor = localDescriptor();
    const loadLocalDeltaRuntime = vi.fn(async () => ({
      descriptor,
      registryId: 'local events/2026',
      tableName: 'events',
      schemaName: 'default',
    }));
    const resolver = dataAccessResolverForSelection(localSelection, {
      loadLocalDeltaRuntime,
    });
    const resource = canonicalTableForSelection(localSelection).resource!;
    const firstDeadline = timestampFromMs(1_800_000_120_000);
    const secondDeadline = timestampFromMs(1_800_000_240_000);

    const first = await resolver.resolve(resource, {
      executionId: 'execution-local-1',
      deadline: firstDeadline,
      snapshotVersion: 12,
      signal: new AbortController().signal,
    });
    const second = await resolver.resolve(resource, {
      executionId: 'execution-local-2',
      deadline: secondDeadline,
      snapshotVersion: 12,
      signal: new AbortController().signal,
    });

    expect(loadLocalDeltaRuntime).toHaveBeenCalledTimes(2);
    expect(loadLocalDeltaRuntime).toHaveBeenNthCalledWith(1, 'local events/2026', {
      schemaName: 'default',
      tableName: 'events',
      snapshotVersion: 12,
    });
    expect(first.outcome.case).toBe('browserRead');
    expect(second.outcome.case).toBe('browserRead');
    if (first.outcome.case !== 'browserRead' || second.outcome.case !== 'browserRead') {
      throw new Error('local resolver did not return browser-read outcomes');
    }
    expect(first.outcome.value).toMatchObject({
      resource,
      descriptor: {
        descriptor: {
          case: 'snapshot',
          value: descriptor,
        },
      },
      accessClass: BrowserAccessClass.LOCAL_HANDLE,
      correlationId: 'execution-local-1',
      provenance: {
        resolverId: 'axon.local-delta/v1',
        resolutionId: 'execution-local-1:local-delta',
      },
    });
    expect(timestampMs(first.outcome.value.notAfter!)).toBe(1_800_000_120_000);
    expect(timestampMs(second.outcome.value.notAfter!)).toBe(1_800_000_240_000);
  });

  it('rejects a canonical-resource mismatch without touching the retained local handle', async () => {
    const loadLocalDeltaRuntime = vi.fn(async () => ({
      descriptor: localDescriptor(),
      registryId: 'local events/2026',
      tableName: 'events',
      schemaName: 'default',
    }));
    const resolver = dataAccessResolverForSelection(localSelection, {
      loadLocalDeltaRuntime,
    });
    const expected = canonicalTableForSelection(localSelection).resource!;
    const mismatched = create(CanonicalResourceRefSchema, {
      ...expected,
      identity: {
        case: 'providerObjectId',
        value: 'another-registry',
      },
    });

    const resolution = await resolver.resolve(mismatched, {
      executionId: 'execution-local-mismatch',
      deadline: timestampFromMs(1_800_000_120_000),
      signal: new AbortController().signal,
    });

    expect(resolution.outcome).toMatchObject({
      case: 'error',
      value: {
        code: ProviderErrorCode.INVALID,
        correlationId: 'execution-local-mismatch',
        message: expect.stringMatching(/canonical resource/i),
      },
    });
    expect(loadLocalDeltaRuntime).not.toHaveBeenCalled();
  });

  it('returns denied for a revoked handle and allows a later execution to regrant access', async () => {
    const descriptor = localDescriptor();
    const loadLocalDeltaRuntime = vi
      .fn()
      .mockRejectedValueOnce(
        new LocalDeltaError('registry_unavailable', 'Select the folder again.'),
      )
      .mockResolvedValueOnce({
        descriptor,
        registryId: 'local events/2026',
        tableName: 'events',
        schemaName: 'default',
      });
    const resolver = dataAccessResolverForSelection(localSelection, {
      loadLocalDeltaRuntime,
    });
    const resource = canonicalTableForSelection(localSelection).resource!;

    const denied = await resolver.resolve(resource, {
      executionId: 'execution-local-revoked',
      deadline: timestampFromMs(1_800_000_120_000),
      signal: new AbortController().signal,
    });
    const regranted = await resolver.resolve(resource, {
      executionId: 'execution-local-regranted',
      deadline: timestampFromMs(1_800_000_240_000),
      signal: new AbortController().signal,
    });

    expect(denied.outcome).toMatchObject({
      case: 'denied',
      value: {
        resource,
        reason: ReadResolutionReason.SESSION_REQUIRED,
        message: 'Select the folder again.',
      },
    });
    expect(regranted.outcome.case).toBe('browserRead');
    expect(loadLocalDeltaRuntime).toHaveBeenCalledTimes(2);
  });

  it('keeps invalid local snapshot failures distinct as provider errors', async () => {
    const loadLocalDeltaRuntime = vi.fn(async () => {
      throw new LocalDeltaError('invalid_delta_log', 'Invalid Delta snapshot.');
    });
    const resolver = dataAccessResolverForSelection(localSelection, {
      loadLocalDeltaRuntime,
    });
    const resource = canonicalTableForSelection(localSelection).resource!;

    const resolution = await resolver.resolve(resource, {
      executionId: 'execution-local-invalid',
      deadline: timestampFromMs(1_800_000_120_000),
      signal: new AbortController().signal,
    });

    expect(resolution.outcome).toMatchObject({
      case: 'error',
      value: {
        code: ProviderErrorCode.INVALID,
        correlationId: 'execution-local-invalid',
        message: 'Invalid Delta snapshot.',
      },
    });
  });

  it('preserves unexpected local provider diagnostics without opening a fallback source', async () => {
    const resolver = dataAccessResolverForSelection(localSelection, {
      loadLocalDeltaRuntime: vi.fn(async () => {
        throw new Error('registry boom');
      }),
    });

    const resolution = await resolver.resolve(
      canonicalTableForSelection(localSelection).resource!,
      {
        executionId: 'execution-local-provider-error',
        deadline: timestampFromMs(1_800_000_120_000),
        signal: new AbortController().signal,
      },
    );

    expect(resolution.outcome).toMatchObject({
      case: 'error',
      value: {
        code: ProviderErrorCode.UNAVAILABLE,
        correlationId: 'execution-local-provider-error',
        message: 'registry boom',
      },
    });
  });

  it.each([
    [
      publicGcsSelection,
      {
        connectionId: 'axon-connection://public-gcs/Public-Bucket',
        providerNamespace: 'axon.public-gcs/v1',
        canonicalLocator: 'gs://Public-Bucket/events/table',
      },
    ],
    [
      publicS3Selection,
      {
        connectionId: 'axon-connection://public-s3/us-east-2/public-bucket',
        providerNamespace: 'axon.public-s3/v1',
        canonicalLocator: 's3://public-bucket/events/table',
      },
    ],
  ])('maps a normalized public table root to one canonical resource', (selection, expected) => {
    expect(canonicalTableForSelection(selection)).toMatchObject({
      name: 'events',
      resource: {
        connectionId: expected.connectionId,
        providerNamespace: expected.providerNamespace,
        kind: ResourceKind.TABLE,
        identity: {
          case: 'canonicalLocator',
          value: expected.canonicalLocator,
        },
      },
    });
  });

  it.each([publicGcsSelection, publicS3Selection])(
    'resolves a requested public snapshot into a fresh non-expiring PUBLIC envelope',
    async (selection) => {
      const source = selection.source;
      if (source.kind !== 'object_store_table_root') {
        throw new Error('public resolver fixture must be an object-store table root');
      }
      const tableUri =
        source.provider === 'gcs'
          ? 'gs://Public-Bucket/events/table'
          : 's3://public-bucket/events/table';
      const descriptor = localDescriptor(tableUri, source.snapshot);
      const loadPublicObjectStorageDescriptor = vi.fn(async () => descriptor);
      const resolver = dataAccessResolverForSelection(selection, {
        loadPublicObjectStorageDescriptor,
      });
      const table = canonicalTableForSelection(selection);

      const resolution = await resolver.resolve(table.resource!, {
        executionId: `execution-${source.provider}`,
        deadline: timestampFromMs(1_800_000_120_000),
        snapshotVersion: source.snapshot,
        signal: new AbortController().signal,
      });

      expect(loadPublicObjectStorageDescriptor).toHaveBeenCalledWith({
        provider: source.provider,
        tableUri,
        region: source.provider === 's3' ? 'us-east-2' : undefined,
        snapshotVersion: source.snapshot,
        expectedSnapshotVersion: source.snapshot,
        signal: expect.objectContaining({ aborted: false }),
      });
      expect(resolution.outcome.case).toBe('browserRead');
      if (resolution.outcome.case !== 'browserRead') return;
      expect(resolution.outcome.value).toMatchObject({
        resource: table.resource,
        descriptor: {
          descriptor: {
            case: 'snapshot',
            value: descriptor,
          },
        },
        accessClass: BrowserAccessClass.PUBLIC,
        correlationId: `execution-${source.provider}`,
        provenance: {
          resolverId: `axon.public-${source.provider}/v1`,
          resolutionId: `execution-${source.provider}:public-${source.provider}`,
        },
      });
      expect(resolution.outcome.value.notAfter).toBeUndefined();
    },
  );

  it('maps only the exact sample selection to its fixed fixture identity and PUBLIC envelope', async () => {
    const selection: AvailableQuerySourceSelection = {
      kind: 'sample',
      ref: SAMPLE_QUERY_SOURCE_REF,
      source: SAMPLE_QUERY_SOURCE,
    };
    const descriptor = localDescriptor('gs://axon-sandbox/prod-like-events', 3);
    const loadSampleFixtureDescriptor = vi.fn(async () => descriptor);
    const table = canonicalTableForSelection(selection);

    expect(table).toMatchObject({
      name: 'events',
      resource: {
        connectionId: 'axon-connection://sample-fixture/sample-lake',
        providerNamespace: 'axon.sample-fixture/v1',
        kind: ResourceKind.TABLE,
        identity: {
          case: 'canonicalLocator',
          value: 'axon-fixture://sample-lake/prod_like/events',
        },
      },
    });

    const resolution = await dataAccessResolverForSelection(selection, {
      loadSampleFixtureDescriptor,
    }).resolve(table.resource!, {
      executionId: 'execution-sample',
      deadline: timestampFromMs(1_800_000_120_000),
      snapshotVersion: 3,
      signal: new AbortController().signal,
    });

    expect(loadSampleFixtureDescriptor).toHaveBeenCalledWith({
      snapshotVersion: 3,
      signal: expect.objectContaining({ aborted: false }),
    });
    expect(resolution.outcome).toMatchObject({
      case: 'browserRead',
      value: {
        resource: table.resource,
        accessClass: BrowserAccessClass.PUBLIC,
        correlationId: 'execution-sample',
        provenance: {
          resolverId: 'axon.sample-fixture/v1',
          resolutionId: 'execution-sample:sample-fixture',
        },
      },
    });
    if (resolution.outcome.case === 'browserRead') {
      expect(resolution.outcome.value.notAfter).toBeUndefined();
    }
  });

  it('does not turn an arbitrary manifest source into the sample fixture provider', () => {
    const impostor: AvailableQuerySourceSelection = {
      kind: 'resource',
      ref: SAMPLE_QUERY_SOURCE_REF,
      source: SAMPLE_QUERY_SOURCE,
    };

    expect(() => canonicalTableForSelection(impostor)).toThrow(/explicit sample fixture/i);
    expect(() => dataAccessResolverForSelection(impostor)).toThrow(/explicit sample fixture/i);
  });
});

function localDescriptor(tableUri = 'browser-local://delta-table/events', snapshotVersion = 12) {
  return create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri,
    snapshotVersion: BigInt(snapshotVersion),
    partitionColumnTypes: {
      day: PartitionColumnType.STRING,
    },
    activeFiles: [
      create(BrowserHttpFileDescriptorSchema, {
        path: 'day=2026-07-23/part-000.parquet',
        url: 'blob:part-000',
        sizeBytes: 128n,
        partitionValues: {},
      }),
    ],
  });
}
