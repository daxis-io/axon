import { create } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it, vi } from 'vitest';
import { BrowserHttpSnapshotDescriptorSchema } from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  CancelRequestSchema,
  CancelResponseSchema,
  ExecuteRequestSchema,
  ExecuteResponseSchema,
  ExecutionLifecycleState,
  ExecutionTarget,
  QueryRequestSchema,
  type ExecuteRequest,
  type ExecuteResponse,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  canonicalTableForSelection,
  dataAccessResolverForSelection,
} from './browser-read-resolution.ts';
import {
  BrowserExecutionValidationError,
  createValidatedBrowserExecutionProvider,
} from './browser-execution-provider.ts';
import type { AvailableQuerySourceSelection } from './query-source.ts';

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
    localRegistryId: 'local-events',
    storage: 'browser-cache://local',
    region: 'browser-local',
    snapshot: 12,
  },
};

describe('validated browser execution provider', () => {
  it('passes the exact generated browser binding to the executor after validation', async () => {
    const table = canonicalTableForSelection(localSelection);
    const request = await localExecuteRequest();
    const response = create(ExecuteResponseSchema);
    const executeValidated = vi.fn(() => responses(response));
    const provider = createValidatedBrowserExecutionProvider(
      {
        execute: executeValidated,
        cancel: (cancelRequest) =>
          create(CancelResponseSchema, {
            executionId: cancelRequest.executionId,
            state: ExecutionLifecycleState.CANCEL_REQUESTED,
          }),
      },
      {
        now: () => 1_800_000_000_000,
        isCurrentLocalObjectUrl: (_registryId, url) => url === 'blob:part-000',
      },
    );

    await expect(collect(provider.execute({ table, request }))).resolves.toEqual([response]);
    expect(executeValidated).toHaveBeenCalledWith({ table, request });
    expect(
      executeValidated.mock.calls[0]?.[0].request.binding.case === 'browserRead' &&
        executeValidated.mock.calls[0][0].request.binding.value.descriptor?.descriptor.value,
    ).toBe(
      request.binding.case === 'browserRead' && request.binding.value.descriptor?.descriptor.value,
    );
  });

  it.each([
    [
      'missing binding',
      (request: ExecuteRequest) => {
        request.binding = { case: undefined };
      },
    ],
    [
      'logical-resource binding',
      (request: ExecuteRequest) => {
        if (request.binding.case !== 'browserRead' || !request.binding.value.resource) return;
        request.binding = { case: 'logicalResource', value: request.binding.value.resource };
      },
    ],
    [
      'resource mismatch',
      (request: ExecuteRequest) => {
        if (request.binding.case !== 'browserRead' || !request.binding.value.resource) return;
        request.binding.value.resource.connectionId = 'axon-connection://local-delta/other';
      },
    ],
    [
      'expired lifetime',
      (request: ExecuteRequest) => {
        if (request.binding.case !== 'browserRead') return;
        request.binding.value.notAfter = timestampFromMs(1_799_999_999_999);
      },
    ],
    [
      'stale Blob URL',
      (request: ExecuteRequest) => {
        if (
          request.binding.case !== 'browserRead' ||
          request.binding.value.descriptor?.descriptor.case !== 'snapshot'
        ) {
          return;
        }
        request.binding.value.descriptor.descriptor.value.activeFiles[0]!.url =
          'blob:revoked-part-000';
      },
    ],
  ])('rejects %s before the executor can open anything', async (_label, mutate) => {
    const table = canonicalTableForSelection(localSelection);
    const request = await localExecuteRequest();
    mutate(request);
    const execute = vi.fn(() => responses(create(ExecuteResponseSchema)));
    const provider = createValidatedBrowserExecutionProvider(
      {
        execute,
        cancel: () =>
          create(CancelResponseSchema, {
            executionId: request.executionId,
            state: ExecutionLifecycleState.CANCEL_REQUESTED,
          }),
      },
      {
        now: () => 1_800_000_000_000,
        isCurrentLocalObjectUrl: (_registryId, url) => url === 'blob:part-000',
      },
    );

    await expect(collect(provider.execute({ table, request }))).rejects.toBeInstanceOf(
      BrowserExecutionValidationError,
    );
    expect(execute).not.toHaveBeenCalled();
  });

  it('delegates generated cancellation without translating its authority', async () => {
    const cancel = vi.fn((request) =>
      create(CancelResponseSchema, {
        executionId: request.executionId,
        state: ExecutionLifecycleState.CANCEL_REQUESTED,
      }),
    );
    const provider = createValidatedBrowserExecutionProvider({
      execute: () => responses(create(ExecuteResponseSchema)),
      cancel,
    });
    const request = create(CancelRequestSchema, { executionId: 'execution-local-1' });

    expect(provider.cancel(request)).toEqual(
      create(CancelResponseSchema, {
        executionId: 'execution-local-1',
        state: ExecutionLifecycleState.CANCEL_REQUESTED,
      }),
    );
    expect(cancel).toHaveBeenCalledWith(request);
  });
});

async function localExecuteRequest() {
  const table = canonicalTableForSelection(localSelection);
  const deadline = timestampFromMs(1_800_000_120_000);
  const resolution = await dataAccessResolverForSelection(localSelection, {
    loadLocalDeltaRuntime: async () => ({
      registryId: 'local-events',
      schemaName: 'default',
      tableName: 'events',
      descriptor: create(BrowserHttpSnapshotDescriptorSchema, {
        tableUri: 'browser-local://delta-table/events',
        snapshotVersion: 12n,
        activeFiles: [
          {
            path: 'part-000.parquet',
            url: 'blob:part-000',
            sizeBytes: 128n,
            partitionValues: {},
          },
        ],
      }),
    }),
  }).resolve(table.resource!, {
    executionId: 'execution-local-1',
    deadline,
    snapshotVersion: 12,
    signal: new AbortController().signal,
  });
  if (resolution.outcome.case !== 'browserRead') {
    throw new Error(`unexpected resolution ${resolution.outcome.case}`);
  }
  return create(ExecuteRequestSchema, {
    executionId: 'execution-local-1',
    binding: {
      case: 'browserRead',
      value: resolution.outcome.value,
    },
    query: create(QueryRequestSchema, {
      sql: 'select * from events',
      preferredTarget: ExecutionTarget.BROWSER_WASM,
    }),
    deadline,
  });
}

async function* responses(response: ExecuteResponse): AsyncIterable<ExecuteResponse> {
  yield response;
}

async function collect(values: AsyncIterable<ExecuteResponse>): Promise<ExecuteResponse[]> {
  const collected: ExecuteResponse[] = [];
  for await (const value of values) collected.push(value);
  return collected;
}
