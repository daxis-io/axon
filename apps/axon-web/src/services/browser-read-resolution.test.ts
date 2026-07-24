import { create } from '@bufbuild/protobuf';
import { timestampFromMs, timestampMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it, vi } from 'vitest';
import {
  CanonicalResourceRefSchema,
  ProviderErrorCode,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserAccessClass,
  BrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema,
  PartitionColumnType,
  ReadResolutionReason,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import type { AvailableQuerySourceSelection } from './query-source.ts';
import { LocalDeltaError } from './local-delta.ts';
import {
  canonicalTableForSelection,
  dataAccessResolverForSelection,
} from './browser-read-resolution.ts';

const localSelection: AvailableQuerySourceSelection = {
  kind: 'resource',
  ref: {
    catalogId: 'local-catalog',
    schemaName: 'default',
    tableName: 'events',
  },
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

describe('browser read canonical identity', () => {
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
});

function localDescriptor() {
  return create(BrowserHttpSnapshotDescriptorSchema, {
    tableUri: 'browser-local://delta-table/events',
    snapshotVersion: 12n,
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
