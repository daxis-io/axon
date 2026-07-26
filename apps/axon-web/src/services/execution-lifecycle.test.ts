import { clone, create } from '@bufbuild/protobuf';
import { timestampFromMs, timestampMs } from '@bufbuild/protobuf/wkt';
import { describe, expect, it, vi } from 'vitest';
import {
  CanonicalResourceRefSchema,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserAccessClass,
  ResolvedBrowserReadSchema,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ExecuteRequestSchema,
  ExecutionLifecycleState,
  ExecutionRejectionReason,
  ExecutionTarget,
  QueryExecutionOptionsSchema,
  QueryRequestSchema,
  QueryRuntimeLimitsSchema,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  ExecutionLifecycle,
  cancelExecutionRequest,
  createExecutionController,
  executionCancelSpanId,
  executionOpenSpanId,
  executionRequestId,
} from './execution-lifecycle.ts';

const resource = create(CanonicalResourceRefSchema, {
  connectionId: 'axon-connection://sample-fixture/sample-lake',
  providerNamespace: 'axon.sample-fixture/v1',
  kind: ResourceKind.TABLE,
  identity: {
    case: 'canonicalLocator',
    value: 'axon-fixture://sample-lake/prod_like/events',
  },
});

function executeRequest(
  overrides: {
    executionId?: string;
    deadlineAt?: number;
    sql?: string;
    target?: ExecutionTarget;
    maxResultRows?: bigint;
    maxArrowIpcBytes?: bigint;
    maxPreviewStringBytes?: bigint;
    maxScanBytes?: bigint;
  } = {},
) {
  const executionId = overrides.executionId ?? 'execution-001';
  return create(ExecuteRequestSchema, {
    executionId,
    binding: {
      case: 'browserRead',
      value: create(ResolvedBrowserReadSchema, {
        resource,
        accessClass: BrowserAccessClass.PUBLIC,
        correlationId: executionId,
      }),
    },
    query: create(QueryRequestSchema, {
      sql: overrides.sql ?? 'select * from events',
      preferredTarget: overrides.target ?? ExecutionTarget.BROWSER_WASM,
      options: create(QueryExecutionOptionsSchema, {
        runtimeLimits: create(QueryRuntimeLimitsSchema, {
          maxResultRows: overrides.maxResultRows ?? 501n,
          maxArrowIpcBytes: overrides.maxArrowIpcBytes ?? 8n * 1024n * 1024n,
          maxPreviewStringBytes: overrides.maxPreviewStringBytes ?? 256n * 1024n,
          maxScanBytes: overrides.maxScanBytes ?? 64n * 1024n * 1024n,
        }),
      }),
    }),
    deadline: timestampFromMs(overrides.deadlineAt ?? 121_000),
  });
}

function controllerFixture(maxRecords = 8) {
  let now = 1_000;
  let nextTimer = 0;
  const scheduled = new Map<number, { callback: () => void; delay: number }>();
  const lifecycle = new ExecutionLifecycle({ maxRecords });
  const controller = createExecutionController({
    lifecycle,
    idFactory: () => 'execution-001',
    now: () => now,
    setTimer: (callback, delay) => {
      const handle = ++nextTimer;
      scheduled.set(handle, { callback, delay });
      return handle;
    },
    clearTimer: (handle) => {
      scheduled.delete(handle as number);
    },
  });
  return {
    controller,
    lifecycle,
    scheduled,
    setNow(value: number) {
      now = value;
    },
  };
}

function prepareExecution() {
  const fixture = controllerFixture();
  const prepared = fixture.controller.prepare({ timeoutMs: 120_000 });
  if (prepared.kind === 'rejected') throw new Error('expected created execution');
  return { ...fixture, execution: prepared.execution };
}

describe('generated execution admission', () => {
  it('reserves a caller-created identity and generated absolute deadline before resolution', () => {
    const { controller, lifecycle, scheduled } = controllerFixture();

    const prepared = controller.prepare({ timeoutMs: 120_000 });

    expect(prepared).toMatchObject({
      kind: 'created',
      execution: { executionId: 'execution-001' },
      snapshot: {
        state: 'created',
        executionId: 'execution-001',
        request: undefined,
      },
    });
    if (prepared.kind === 'rejected') throw new Error('expected reservation');
    expect(timestampMs(prepared.execution.deadline)).toBe(121_000);
    expect(lifecycle.getSnapshot('execution-001')).toMatchObject({ state: 'created' });
    expect([...scheduled.values()][0]?.delay).toBe(120_000);
  });

  it('stores the exact generated request and replays it without relaunching', () => {
    const { controller, execution } = prepareExecution();
    const request = executeRequest({
      executionId: execution.executionId,
      deadlineAt: timestampMs(execution.deadline),
    });

    expect(controller.admit(request).admission.outcome).toMatchObject({
      case: 'accepted',
      value: {
        executionId: 'execution-001',
        state: ExecutionLifecycleState.RUNNING,
        launch: true,
      },
    });
    expect(controller.admit(clone(ExecuteRequestSchema, request)).admission.outcome).toMatchObject({
      case: 'accepted',
      value: { launch: false, state: ExecutionLifecycleState.RUNNING },
    });
  });

  it.each([
    [
      'resource',
      (request: ReturnType<typeof executeRequest>) => {
        if (request.binding.case === 'browserRead' && request.binding.value.resource) {
          request.binding.value.resource.connectionId = 'axon-connection://sample-fixture/other';
        }
      },
    ],
    [
      'SQL',
      (request: ReturnType<typeof executeRequest>) => {
        request.query!.sql = 'select 2';
      },
    ],
    [
      'target',
      (request: ReturnType<typeof executeRequest>) => {
        request.query!.preferredTarget = ExecutionTarget.NATIVE;
      },
    ],
    [
      'deadline',
      (request: ReturnType<typeof executeRequest>) => {
        request.deadline = timestampFromMs(121_001);
      },
    ],
    [
      'row budget',
      (request: ReturnType<typeof executeRequest>) => {
        request.query!.options!.runtimeLimits!.maxResultRows = 500n;
      },
    ],
  ])('rejects execution-ID reuse when the exact %s differs', (_label, mutate) => {
    const { controller, execution } = prepareExecution();
    const original = executeRequest({
      executionId: execution.executionId,
      deadlineAt: timestampMs(execution.deadline),
    });
    controller.admit(original);
    const changed = clone(ExecuteRequestSchema, original);
    mutate(changed);

    expect(controller.admit(changed).admission.outcome).toMatchObject({
      case: 'rejected',
      value: { reason: ExecutionRejectionReason.EXECUTION_ID_REUSE },
    });
    expect(controller.lifecycle.getSnapshot(execution.executionId)?.request).toEqual(original);
  });

  it('keeps caller mutation from changing the recorded immutable request', () => {
    const { controller, execution } = prepareExecution();
    const request = executeRequest({
      executionId: execution.executionId,
      deadlineAt: timestampMs(execution.deadline),
    });
    controller.admit(request);
    request.query!.sql = 'mutated after admission';

    expect(controller.lifecycle.getSnapshot(execution.executionId)?.request?.query?.sql).toBe(
      'select * from events',
    );
  });

  it.each([
    ['native target', { target: ExecutionTarget.NATIVE }],
    ['unsafe rows', { maxResultRows: 502n }],
    ['unsafe Arrow bytes', { maxArrowIpcBytes: 8n * 1024n * 1024n + 1n }],
    ['unsafe preview bytes', { maxPreviewStringBytes: 256n * 1024n + 1n }],
    ['unsafe scan bytes', { maxScanBytes: BigInt(Number.MAX_SAFE_INTEGER) + 1n }],
  ])('rejects %s before launch', (_label, overrides) => {
    const { controller, execution } = prepareExecution();
    const admission = controller.admit(
      executeRequest({
        executionId: execution.executionId,
        deadlineAt: timestampMs(execution.deadline),
        ...overrides,
      }),
    );

    expect(admission.admission.outcome.case).toBe('rejected');
    expect(admission.snapshot).toMatchObject({ state: 'rejected', admitted: false });
  });

  it('rejects the reserved request when its absolute deadline expires before admission', () => {
    const { controller, execution, setNow } = prepareExecution();
    setNow(timestampMs(execution.deadline));

    const admission = controller.admit(
      executeRequest({
        executionId: execution.executionId,
        deadlineAt: timestampMs(execution.deadline),
      }),
    );

    expect(admission.admission.outcome).toMatchObject({
      case: 'rejected',
      value: { reason: ExecutionRejectionReason.DEADLINE_EXPIRED },
    });
    expect(admission.snapshot).toMatchObject({ state: 'rejected', admitted: false });
  });
});

describe('cancellation, deadlines, and terminal authority', () => {
  it('records cancel-before-admit as a tombstone and rejects the later resolved request', () => {
    const { controller, execution } = prepareExecution();
    const cancel = vi.fn();
    controller.attachCancellation(execution.executionId, cancel);

    expect(controller.cancel(cancelExecutionRequest(execution.executionId))).toMatchObject({
      executionId: execution.executionId,
      state: ExecutionLifecycleState.REJECTED,
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(
      controller.admit(
        executeRequest({
          executionId: execution.executionId,
          deadlineAt: timestampMs(execution.deadline),
        }),
      ).admission.outcome,
    ).toMatchObject({
      case: 'rejected',
      value: { reason: ExecutionRejectionReason.CANCELLED },
    });
  });

  it('requests running cancellation once and records the generated response state', () => {
    const { controller, execution } = prepareExecution();
    const cancel = vi.fn();
    controller.attachCancellation(execution.executionId, cancel);
    controller.admit(
      executeRequest({
        executionId: execution.executionId,
        deadlineAt: timestampMs(execution.deadline),
      }),
    );

    expect(controller.cancel(cancelExecutionRequest(execution.executionId))).toMatchObject({
      state: ExecutionLifecycleState.CANCEL_REQUESTED,
    });
    expect(controller.cancel(cancelExecutionRequest(execution.executionId))).toMatchObject({
      state: ExecutionLifecycleState.CANCEL_REQUESTED,
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('lets the whole-lifecycle deadline win and emits one generated terminal frame', () => {
    const { controller, execution, scheduled, setNow } = prepareExecution();
    const cancel = vi.fn();
    const deliveries: unknown[] = [];
    controller.attachCancellation(execution.executionId, cancel);
    controller.subscribe(execution.executionId, (delivery) => deliveries.push(delivery));
    controller.admit(
      executeRequest({
        executionId: execution.executionId,
        deadlineAt: timestampMs(execution.deadline),
      }),
    );

    setNow(121_000);
    [...scheduled.values()][0]!.callback();

    expect(controller.lifecycle.getSnapshot(execution.executionId)).toMatchObject({
      state: 'failed',
      terminalReason: 'deadline',
      terminalFrame: {
        executionId: execution.executionId,
        sequence: 1n,
        state: { outcome: { case: 'failed' } },
      },
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(deliveries).toHaveLength(1);
  });

  it('makes the first terminal transition authoritative', () => {
    const { controller, execution } = prepareExecution();
    const listener = vi.fn();
    controller.subscribe(execution.executionId, listener);
    controller.admit(
      executeRequest({
        executionId: execution.executionId,
        deadlineAt: timestampMs(execution.deadline),
      }),
    );

    expect(controller.complete(execution.executionId)).toMatchObject({
      kind: 'transitioned',
      delivered: true,
    });
    controller.fail(execution.executionId, 'worker_error');
    controller.confirmCancelled(execution.executionId);

    expect(listener).toHaveBeenCalledTimes(1);
    expect(controller.lifecycle.getSnapshot(execution.executionId)?.invariantViolations).toEqual([
      'late failed after completed',
      'late cancelled after completed',
    ]);
  });

  it('bounds retained records, listeners, and invariant diagnostics', () => {
    const lifecycle = new ExecutionLifecycle({ maxRecords: 1 });
    const deadline = timestampFromMs(2_000);
    expect(lifecycle.reserve('execution-1', deadline).kind).toBe('created');
    expect(lifecycle.reserve('execution-2', deadline)).toEqual({
      kind: 'rejected',
      reason: ExecutionRejectionReason.CAPACITY,
    });

    const listeners = Array.from({ length: 40 }, () => vi.fn());
    for (const listener of listeners) lifecycle.subscribe('execution-1', listener);
    const request = executeRequest({ executionId: 'execution-1', deadlineAt: 2_000 });
    lifecycle.admit(request, 1_000);
    lifecycle.complete('execution-1');
    expect(listeners.filter((listener) => listener.mock.calls.length > 0)).toHaveLength(16);
    for (let index = 0; index < 100; index += 1) {
      lifecycle.publishFrame('execution-1', { index });
    }
    expect(lifecycle.getSnapshot('execution-1')?.invariantViolations).toHaveLength(32);
  });
});

describe('execution span identity', () => {
  it('uses the execution ID for SQL and deterministic child IDs for open and cancel', () => {
    expect(executionRequestId('execution-1')).toBe('execution-1');
    expect(executionOpenSpanId('execution-1', 2)).toBe('execution-1:open:2');
    expect(executionCancelSpanId('execution-1', 3)).toBe('execution-1:cancel:3');
  });
});
