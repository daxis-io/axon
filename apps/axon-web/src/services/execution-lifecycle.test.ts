import { describe, expect, it, vi } from 'vitest';
import {
  ExecutionLifecycle,
  createExecutionController,
  executionCancelSpanId,
  executionOpenSpanId,
  executionRequestId,
  type ExecutionAdmissionInput,
  type ExecutionBudgets,
} from './execution-lifecycle.ts';
import { SAMPLE_QUERY_SOURCE, SAMPLE_QUERY_SOURCE_REF } from './query-source.ts';

const budgets: ExecutionBudgets = {
  maxResultRows: 501,
  maxArrowIpcBytes: 8 * 1024 * 1024,
  maxPreviewStringBytes: 256 * 1024,
  maxScanBytes: 64 * 1024 * 1024,
};

const sampleSelection = {
  kind: 'sample',
  ref: SAMPLE_QUERY_SOURCE_REF,
  source: SAMPLE_QUERY_SOURCE,
} as const;

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

function prepareInput() {
  const fixture = controllerFixture();
  const prepared = fixture.controller.prepare({
    selection: sampleSelection,
    sql: 'select * from events',
    target: 'browser_wasm',
    timeoutMs: 120_000,
    budgets,
  });
  if (prepared.kind !== 'created') throw new Error('expected created execution');
  return { ...fixture, input: prepared.input };
}

describe('execution lifecycle admission', () => {
  it('creates a deterministic ID and absolute deadline before admission', () => {
    const { controller, lifecycle } = controllerFixture();

    const prepared = controller.prepare({
      selection: sampleSelection,
      sql: 'select * from events',
      target: 'browser_wasm',
      timeoutMs: 120_000,
      budgets,
    });

    expect(prepared).toMatchObject({
      kind: 'created',
      input: {
        executionId: 'execution-001',
        sql: 'select * from events',
        target: 'browser_wasm',
        deadlineAt: 121_000,
        budgets,
        sourceIdentity: {
          kind: 'sample',
          ref: SAMPLE_QUERY_SOURCE_REF,
          source: [
            'manifest',
            'sample-lake',
            'prod_like',
            'events',
            '/fixtures/prod-like/delta-log-manifest.json',
            'gs://axon-sample/prod-like-events',
            'browser-local',
            null,
          ],
          snapshotVersion: null,
        },
      },
      snapshot: { state: 'created', executionId: 'execution-001' },
    });
    expect(lifecycle.getSnapshot('execution-001')).toMatchObject({ state: 'created' });
  });

  it('admits once and replays an identical admission without launching again', () => {
    const { controller, lifecycle, input } = prepareInput();

    expect(controller.admit(input)).toMatchObject({
      kind: 'accepted',
      launch: true,
      snapshot: { state: 'running' },
    });
    expect(lifecycle.admit({ ...input, budgets: { ...input.budgets } })).toMatchObject({
      kind: 'accepted',
      launch: false,
      snapshot: { state: 'running' },
    });
  });

  it.each([
    [
      'selected ref',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        sourceIdentity: {
          ...input.sourceIdentity,
          ref: { ...input.sourceIdentity.ref, catalogId: 'other-catalog' },
        },
      }),
    ],
    [
      'selected snapshot',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        sourceIdentity: {
          ...input.sourceIdentity,
          snapshotVersion: 12,
        },
      }),
    ],
    [
      'source version',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        sourceIdentity: {
          ...input.sourceIdentity,
          source: [
            'manifest',
            'sample-lake',
            'prod_like',
            'events',
            '/fixtures/prod-like/delta-log-manifest.json',
            'gs://axon-sample/prod-like-events',
            'browser-local',
            11,
          ] as const,
        },
      }),
    ],
    ['sql', (input: ExecutionAdmissionInput) => ({ ...input, sql: 'select 2' })],
    ['target', (input: ExecutionAdmissionInput) => ({ ...input, target: 'native' as const })],
    [
      'deadline',
      (input: ExecutionAdmissionInput) => ({ ...input, deadlineAt: input.deadlineAt + 1 }),
    ],
    [
      'row budget',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        budgets: { ...input.budgets, maxResultRows: 500 },
      }),
    ],
    [
      'Arrow budget',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        budgets: { ...input.budgets, maxArrowIpcBytes: input.budgets.maxArrowIpcBytes - 1 },
      }),
    ],
    [
      'preview budget',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        budgets: {
          ...input.budgets,
          maxPreviewStringBytes: input.budgets.maxPreviewStringBytes - 1,
        },
      }),
    ],
    [
      'scan budget',
      (input: ExecutionAdmissionInput) => ({
        ...input,
        budgets: { ...input.budgets, maxScanBytes: (input.budgets.maxScanBytes ?? 1) - 1 },
      }),
    ],
  ])('rejects mismatched ID reuse for %s without changing the original', (_label, mutate) => {
    const { controller, lifecycle, input } = prepareInput();
    controller.admit(input);
    const before = lifecycle.getSnapshot(input.executionId);

    expect(controller.admit(mutate(input))).toMatchObject({
      kind: 'id_reuse',
      launch: false,
      snapshot: before,
    });
    expect(lifecycle.getSnapshot(input.executionId)).toEqual(before);
  });

  it('bounds retained records and rejects new IDs when no record is sweepable', () => {
    let sequence = 0;
    const lifecycle = new ExecutionLifecycle({ maxRecords: 1 });
    const controller = createExecutionController({
      lifecycle,
      idFactory: () => `execution-${++sequence}`,
      now: () => 100,
    });
    const request = {
      selection: sampleSelection,
      sql: 'select 1',
      target: 'browser_wasm' as const,
      timeoutMs: 1_000,
      budgets,
    };

    expect(controller.prepare(request).kind).toBe('created');
    expect(controller.prepare(request)).toEqual({ kind: 'rejected', reason: 'capacity' });
    expect(lifecycle.recordCount).toBe(1);
  });

  it('rejects mismatched expired reuse before deadline processing can mutate the original', () => {
    const { controller, lifecycle, input, setNow } = prepareInput();
    controller.admit(input);
    const before = lifecycle.getSnapshot(input.executionId);
    setNow(input.deadlineAt);

    expect(controller.admit({ ...input, sql: 'select 2' })).toMatchObject({
      kind: 'id_reuse',
      launch: false,
      snapshot: before,
    });
    expect(lifecycle.getSnapshot(input.executionId)).toEqual(before);
  });

  it('distinguishes non-finite immutable inputs without JSON null collisions', () => {
    const { input } = prepareInput();
    const lifecycle = new ExecutionLifecycle();
    const controller = createExecutionController({ lifecycle, now: () => 1_000 });
    const invalid = { ...input, deadlineAt: Number.NaN };
    expect(controller.admit(invalid)).toMatchObject({
      kind: 'rejected',
      reason: 'invalid_deadline',
    });
    expect(controller.admit({ ...invalid, deadlineAt: Number.POSITIVE_INFINITY })).toMatchObject({
      kind: 'id_reuse',
      snapshot: { state: 'rejected', rejectionReason: 'invalid_deadline' },
    });
    expect(lifecycle.getSnapshot(input.executionId)).toMatchObject({
      state: 'rejected',
      input: { deadlineAt: Number.NaN },
    });
  });

  it('maps the domain execution ID to query correlation and internal spans explicitly', () => {
    expect(executionRequestId('execution-001')).toBe('execution-001');
    expect(executionOpenSpanId('execution-001', 2)).toBe('execution-001:open:2');
    expect(executionCancelSpanId('execution-001', 3)).toBe('execution-001:cancel:3');
  });
});

describe('execution lifecycle cancellation and terminal delivery', () => {
  it('turns cancel-before-admit into a replayable rejected tombstone', () => {
    const { controller, lifecycle, input } = prepareInput();
    const listener = vi.fn();
    controller.subscribe(input.executionId, listener);

    expect(controller.requestCancellation(input.executionId)).toMatchObject({
      kind: 'cancelled_before_admit',
      snapshot: { state: 'rejected', rejectionReason: 'cancelled', admitted: false },
    });
    expect(controller.admit(input)).toMatchObject({
      kind: 'rejected',
      launch: false,
      reason: 'cancelled',
    });
    expect(lifecycle.admit({ ...input })).toMatchObject({
      kind: 'rejected',
      launch: false,
      reason: 'cancelled',
    });
    expect(listener).not.toHaveBeenCalled();
  });

  it('invokes the registered cancellation handle once and confirms one terminal', () => {
    const { controller, input } = prepareInput();
    const cancel = vi.fn();
    const deliveries: unknown[] = [];
    controller.attachCancellation(input.executionId, cancel);
    controller.subscribe(input.executionId, (delivery) => deliveries.push(delivery));
    controller.admit(input);

    expect(controller.requestCancellation(input.executionId)).toMatchObject({
      kind: 'cancel_requested',
      snapshot: { state: 'cancel_requested' },
    });
    expect(controller.requestCancellation(input.executionId)).toMatchObject({
      kind: 'recorded',
      snapshot: { state: 'cancel_requested' },
    });
    expect(cancel).toHaveBeenCalledTimes(1);

    expect(controller.confirmCancelled(input.executionId, { code: 'cancelled' })).toMatchObject({
      kind: 'transitioned',
      delivered: true,
      snapshot: { state: 'cancelled' },
    });
    expect(controller.confirmCancelled(input.executionId, { code: 'cancelled' })).toMatchObject({
      kind: 'recorded',
      delivered: false,
      snapshot: { state: 'cancelled' },
    });
    expect(controller.lifecycle.getSnapshot(input.executionId)?.invariantViolations).toContain(
      'late cancelled after cancelled',
    );
    expect(deliveries).toEqual([
      {
        kind: 'terminal',
        sequence: 1,
        state: 'cancelled',
        payload: { code: 'cancelled' },
      },
    ]);
  });

  it('replays accepted state after an admission response is lost without relaunching', () => {
    const { controller, input } = prepareInput();
    const deliveries: unknown[] = [];
    controller.subscribe(input.executionId, (delivery) => deliveries.push(delivery));
    controller.admit(input);
    controller.complete(input.executionId, { rows: 1 });

    expect(controller.admit({ ...input })).toMatchObject({
      kind: 'accepted',
      launch: false,
      snapshot: { state: 'completed' },
    });
    expect(controller.complete(input.executionId, { rows: 1 })).toMatchObject({
      kind: 'recorded',
      delivered: false,
    });
    expect(deliveries).toHaveLength(1);
  });

  it('ignores post-terminal cancellation without invoking a handle', () => {
    const { controller, lifecycle, input } = prepareInput();
    const cancel = vi.fn();
    controller.attachCancellation(input.executionId, cancel);
    controller.admit(input);
    controller.complete(input.executionId, { rows: 1 });

    expect(controller.requestCancellation(input.executionId)).toMatchObject({
      kind: 'recorded',
      snapshot: { state: 'completed' },
    });
    expect(cancel).not.toHaveBeenCalled();
    expect(lifecycle.getSnapshot(input.executionId)?.invariantViolations).toContain(
      'cancellation requested after completed',
    );
  });

  it('lets a worker failure win and suppresses late cancellation and result delivery', () => {
    const { controller, lifecycle, input } = prepareInput();
    const deliveries: unknown[] = [];
    controller.subscribe(input.executionId, (delivery) => deliveries.push(delivery));
    controller.admit(input);

    expect(controller.publishFrame(input.executionId, { kind: 'progress' })).toMatchObject({
      kind: 'published',
      sequence: 1,
    });
    expect(controller.fail(input.executionId, 'worker_error', { code: 'E_WORKER' })).toMatchObject({
      kind: 'transitioned',
      delivered: true,
      snapshot: { state: 'failed' },
    });
    expect(controller.confirmCancelled(input.executionId)).toMatchObject({
      kind: 'recorded',
      delivered: false,
    });
    expect(controller.complete(input.executionId, { rows: 1 })).toMatchObject({
      kind: 'recorded',
      delivered: false,
    });
    expect(controller.publishFrame(input.executionId, { kind: 'metrics' })).toMatchObject({
      kind: 'recorded',
    });
    expect(deliveries).toEqual([
      { kind: 'frame', sequence: 1, payload: { kind: 'progress' } },
      {
        kind: 'terminal',
        sequence: 2,
        state: 'failed',
        reason: 'worker_error',
        payload: { code: 'E_WORKER' },
      },
    ]);
    expect(lifecycle.getSnapshot(input.executionId)?.invariantViolations).toEqual([
      'late cancelled after failed',
      'late completed after failed',
      'frame published after failed',
    ]);
  });

  it.each([
    [
      'completion',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.complete(id),
    ],
    [
      'failure',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.fail(id, 'worker_error'),
    ],
    [
      'cancellation',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.confirmCancelled(id),
    ],
  ])('makes the first %s terminal transition authoritative', (_label, terminate) => {
    const { controller, input } = prepareInput();
    const listener = vi.fn();
    controller.subscribe(input.executionId, listener);
    controller.admit(input);

    expect(terminate(controller, input.executionId)).toMatchObject({
      kind: 'transitioned',
      delivered: true,
    });
    controller.complete(input.executionId);
    controller.fail(input.executionId, 'worker_error');
    controller.confirmCancelled(input.executionId);

    expect(listener).toHaveBeenCalledTimes(1);
  });

  it.each([
    [
      'completed',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.complete(id),
    ],
    [
      'failed',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.fail(id, 'worker_error'),
    ],
    [
      'cancelled',
      (controller: ReturnType<typeof createExecutionController>, id: string) =>
        controller.confirmCancelled(id),
    ],
  ])('records duplicate %s delivery as an invariant violation', (state, terminate) => {
    const { controller, lifecycle, input } = prepareInput();
    controller.admit(input);
    terminate(controller, input.executionId);

    expect(terminate(controller, input.executionId)).toMatchObject({
      kind: 'recorded',
      delivered: false,
    });
    expect(lifecycle.getSnapshot(input.executionId)?.invariantViolations).toContain(
      `late ${state} after ${state}`,
    );
  });
});

describe('execution lifecycle deadlines and admission bounds', () => {
  it.each([
    ['non-finite rows', { ...budgets, maxResultRows: Number.POSITIVE_INFINITY }],
    ['fractional rows', { ...budgets, maxResultRows: 1.5 }],
    ['zero rows', { ...budgets, maxResultRows: 0 }],
    ['unsafe rows', { ...budgets, maxResultRows: 502 }],
    ['unsafe Arrow bytes', { ...budgets, maxArrowIpcBytes: 8 * 1024 * 1024 + 1 }],
    ['unsafe preview bytes', { ...budgets, maxPreviewStringBytes: 256 * 1024 + 1 }],
    ['unsafe scan bytes', { ...budgets, maxScanBytes: Number.MAX_SAFE_INTEGER + 1 }],
  ])('rejects %s before admission without an execution stream', (_label, invalidBudgets) => {
    const { controller } = controllerFixture();
    const listener = vi.fn();
    const prepared = controller.prepare({
      selection: sampleSelection,
      sql: 'select * from events',
      target: 'browser_wasm',
      timeoutMs: 120_000,
      budgets: invalidBudgets,
    });
    if (prepared.kind === 'rejected') throw new Error('expected a recorded admission');
    controller.subscribe(prepared.input.executionId, listener);

    expect(controller.admit(prepared.input)).toMatchObject({
      kind: 'rejected',
      launch: false,
      snapshot: { state: 'rejected', admitted: false },
    });
    expect(listener).not.toHaveBeenCalled();
  });

  it('rejects an expired absolute deadline before admission', () => {
    const { controller } = controllerFixture();
    const prepared = controller.prepare({
      selection: sampleSelection,
      sql: 'select * from events',
      target: 'browser_wasm',
      timeoutMs: 0,
      budgets,
    });
    if (prepared.kind === 'rejected') throw new Error('expected a recorded admission');

    expect(controller.admit(prepared.input)).toMatchObject({
      kind: 'rejected',
      launch: false,
      reason: 'deadline_expired',
      snapshot: { state: 'rejected', admitted: false },
    });
  });

  it.each([
    ['non-finite', Number.NaN],
    ['fractional', 1.5],
  ])('rejects a %s absolute deadline before admission', (_label, timeoutMs) => {
    const { controller } = controllerFixture();
    const prepared = controller.prepare({
      selection: sampleSelection,
      sql: 'select * from events',
      target: 'browser_wasm',
      timeoutMs,
      budgets,
    });
    if (prepared.kind === 'rejected') throw new Error('expected a recorded admission');

    expect(controller.admit(prepared.input)).toMatchObject({
      kind: 'rejected',
      launch: false,
      reason: 'invalid_deadline',
      snapshot: { state: 'rejected', admitted: false },
    });
  });

  it('uses one whole-lifecycle timer and fails deadline before aborting local work', () => {
    const { controller, input, scheduled, setNow } = prepareInput();
    const cancel = vi.fn();
    const deliveries: unknown[] = [];
    controller.attachCancellation(input.executionId, cancel);
    controller.subscribe(input.executionId, (delivery) => deliveries.push(delivery));
    controller.admit(input);

    expect(scheduled.size).toBe(1);
    const timer = [...scheduled.values()][0];
    expect(timer.delay).toBe(120_000);
    setNow(input.deadlineAt);
    timer.callback();

    expect(controller.lifecycle.getSnapshot(input.executionId)).toMatchObject({
      state: 'failed',
      terminalReason: 'deadline',
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(deliveries).toEqual([
      {
        kind: 'terminal',
        sequence: 1,
        state: 'failed',
        reason: 'deadline',
      },
    ]);
    expect(controller.confirmCancelled(input.executionId)).toMatchObject({
      kind: 'recorded',
      delivered: false,
    });
  });

  it('retains terminal records through their deadline and sweeps them afterward', () => {
    let now = 100;
    let sequence = 0;
    const scheduled: Array<() => void> = [];
    const lifecycle = new ExecutionLifecycle({ maxRecords: 1 });
    const controller = createExecutionController({
      lifecycle,
      idFactory: () => `execution-${++sequence}`,
      now: () => now,
      setTimer: (callback) => {
        scheduled.push(callback);
        return callback;
      },
      clearTimer: vi.fn(),
    });
    const request = {
      selection: sampleSelection,
      sql: 'select 1',
      target: 'browser_wasm' as const,
      timeoutMs: 10,
      budgets,
    };
    const first = controller.prepare(request);
    if (first.kind === 'rejected') throw new Error('expected first admission');
    controller.admit(first.input);
    controller.complete(first.input.executionId);

    expect(controller.prepare(request)).toEqual({ kind: 'rejected', reason: 'capacity' });
    now = 110;
    scheduled[0]?.();
    expect(controller.prepare(request)).toMatchObject({ kind: 'created' });
    expect(lifecycle.recordCount).toBe(1);
  });

  it('bounds listeners and invariant diagnostics per retained execution', () => {
    const { controller, lifecycle, input } = prepareInput();
    const listeners = Array.from({ length: 40 }, () => vi.fn());
    for (const listener of listeners) controller.subscribe(input.executionId, listener);
    controller.admit(input);
    controller.complete(input.executionId);

    expect(listeners.filter((listener) => listener.mock.calls.length > 0)).toHaveLength(16);
    for (let index = 0; index < 100; index += 1) {
      controller.publishFrame(input.executionId, { index });
    }
    expect(lifecycle.getSnapshot(input.executionId)?.invariantViolations).toHaveLength(32);
  });
});
