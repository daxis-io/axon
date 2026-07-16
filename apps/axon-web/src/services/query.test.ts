import { describe, expect, expectTypeOf, it, vi } from 'vitest';
import {
  AxonWorkerError,
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
  queryCommand,
  type QueryErrorCode,
  type QueryRequest,
} from '../axon-browser-sdk.ts';
import type { ExecutionAdmissionInput } from './execution-lifecycle.ts';
import {
  queryClientOptionsForAdmission,
  queryExecutionOptionsForAdmission,
  queryFailureOutcome,
  queryFailureInvalidatesSession,
  runCancelableQueryStages,
} from './query.ts';
import { withValidatedArrowIpcOutput } from './worker-query-bounds.ts';

const admission: ExecutionAdmissionInput = {
  executionId: 'execution-1',
  sourceIdentity: {
    kind: 'sample',
    ref: { catalogId: 'sample', schemaName: 'main', tableName: 'events' },
    source: [
      'manifest',
      'sample',
      'main',
      'events',
      '/fixture.json',
      'gs://sample/events',
      'browser-local',
      null,
    ],
    snapshotVersion: null,
  },
  sql: 'select * from events',
  target: 'browser_wasm',
  deadlineAt: 121_000,
  budgets: {
    maxResultRows: BROWSER_SAFE_RESULT_ROW_LIMIT,
    maxArrowIpcBytes: BROWSER_SAFE_ARROW_IPC_BYTES,
    maxPreviewStringBytes: BROWSER_SAFE_PREVIEW_STRING_BYTES,
    maxScanBytes: 64 * 1024 * 1024,
  },
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe('cancelable query stages', () => {
  it('does no work when the authoritative lifecycle deadline already won', async () => {
    const deadlineController = new AbortController();
    deadlineController.abort();
    const getSession = vi.fn(async () => ({ id: 'session' }));
    const openTable = vi.fn(async () => undefined);
    const execute = vi.fn(async () => 'result');

    await expect(
      runCancelableQueryStages({
        signal: new AbortController().signal,
        deadlineSignal: deadlineController.signal,
        getSession,
        openTable,
        execute,
        cancelQuery: vi.fn(),
      }),
    ).rejects.toThrow('execution deadline expired');
    expect(getSession).not.toHaveBeenCalled();
    expect(openTable).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });

  it('does no session, open, or SQL work when cancellation wins before bootstrap', async () => {
    const controller = new AbortController();
    controller.abort();
    const getSession = vi.fn(async () => ({ id: 'session' }));
    const openTable = vi.fn(async () => undefined);
    const execute = vi.fn(async () => 'result');
    const cancelQuery = vi.fn();

    await expect(
      runCancelableQueryStages({
        signal: controller.signal,
        getSession,
        openTable,
        execute,
        cancelQuery,
      }),
    ).rejects.toMatchObject({ name: 'AbortError' });
    expect(getSession).not.toHaveBeenCalled();
    expect(openTable).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
    expect(cancelQuery).not.toHaveBeenCalled();
  });

  it('abandons a local session wait without opening a table or launching SQL', async () => {
    const controller = new AbortController();
    const session = deferred<{ id: string }>();
    const openTable = vi.fn(async () => undefined);
    const execute = vi.fn(async () => 'result');
    const cancelQuery = vi.fn();
    const pending = runCancelableQueryStages({
      signal: controller.signal,
      getSession: () => session.promise,
      openTable,
      execute,
      cancelQuery,
    });

    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(openTable).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
    expect(cancelQuery).not.toHaveBeenCalled();
    session.resolve({ id: 'session' });
  });

  it('does not launch SQL when cancellation wins while opening the table', async () => {
    const controller = new AbortController();
    const opened = deferred<void>();
    const session = { id: 'session' };
    const openTable = vi.fn(() => opened.promise);
    const execute = vi.fn(async () => 'result');
    const cancelQuery = vi.fn();
    const pending = runCancelableQueryStages({
      signal: controller.signal,
      getSession: async () => session,
      openTable,
      execute,
      cancelQuery,
    });
    await vi.waitFor(() => expect(openTable).toHaveBeenCalledTimes(1));

    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(execute).not.toHaveBeenCalled();
    expect(cancelQuery).not.toHaveBeenCalled();
    opened.resolve();
  });

  it('waits for worker confirmation after targeting a running SQL execution once', async () => {
    const controller = new AbortController();
    const query = deferred<string>();
    const session = { id: 'session' };
    const execute = vi.fn(() => query.promise);
    const cancelQuery = vi.fn();
    const pending = runCancelableQueryStages({
      signal: controller.signal,
      getSession: async () => session,
      openTable: async () => undefined,
      execute,
      cancelQuery,
    });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
    let settled = false;
    void pending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );

    controller.abort();
    await Promise.resolve();
    expect(settled).toBe(false);
    expect(cancelQuery).toHaveBeenCalledTimes(1);
    expect(cancelQuery).toHaveBeenCalledWith(session);
    const confirmation = new Error('worker cancellation confirmed');
    query.reject(confirmation);
    await expect(pending).rejects.toBe(confirmation);
  });

  it('lets worker completion win when cancellation loses the running SQL race', async () => {
    const controller = new AbortController();
    const query = deferred<string>();
    const execute = vi.fn(() => query.promise);
    const cancelQuery = vi.fn();
    const pending = runCancelableQueryStages({
      signal: controller.signal,
      getSession: async () => ({ id: 'session' }),
      openTable: async () => undefined,
      execute,
      cancelQuery,
    });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledTimes(1));

    controller.abort();
    query.resolve('completed');

    await expect(pending).resolves.toMatchObject({ result: 'completed' });
    expect(cancelQuery).toHaveBeenCalledTimes(1);
  });

  it('releases a cancelled SQL wait when the authoritative lifecycle deadline wins', async () => {
    const controller = new AbortController();
    const deadlineController = new AbortController();
    const query = deferred<string>();
    const execute = vi.fn(() => query.promise);
    const cancelQuery = vi.fn();
    const pending = runCancelableQueryStages({
      signal: controller.signal,
      deadlineSignal: deadlineController.signal,
      getSession: async () => ({ id: 'session' }),
      openTable: async () => undefined,
      execute,
      cancelQuery,
    });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledTimes(1));

    controller.abort();
    await Promise.resolve();
    deadlineController.abort();

    await expect(pending).rejects.toThrow('execution deadline expired');
    expect(cancelQuery).toHaveBeenCalledTimes(1);
  }, 2_000);

  it('runs session, open, and SQL stages once on success', async () => {
    const session = { id: 'session' };
    const calls: string[] = [];

    await expect(
      runCancelableQueryStages({
        signal: new AbortController().signal,
        getSession: async () => {
          calls.push('session');
          return session;
        },
        openTable: async () => {
          calls.push('open');
        },
        execute: async () => {
          calls.push('query');
          return 'result';
        },
        cancelQuery: vi.fn(),
      }),
    ).resolves.toEqual({ session, result: 'result' });
    expect(calls).toEqual(['session', 'open', 'query']);
  });

  it('recalculates remaining deadline time before every execution stage', async () => {
    const remaining = [119_900, 119_400, 118_800];
    const observed: number[] = [];

    await runCancelableQueryStages({
      signal: new AbortController().signal,
      remainingTime: () => remaining.shift() ?? 0,
      getSession: async (remainingMs) => {
        observed.push(remainingMs);
        return { id: 'session' };
      },
      openTable: async (_session, remainingMs) => {
        observed.push(remainingMs);
      },
      execute: async (_session, remainingMs) => {
        observed.push(remainingMs);
        return 'result';
      },
      cancelQuery: vi.fn(),
    });

    expect(observed).toEqual([119_900, 119_400, 118_800]);
  });

  it('does not retry failed accepted SQL work', async () => {
    const execute = vi.fn(async () => {
      throw new Error('worker failed');
    });

    await expect(
      runCancelableQueryStages({
        signal: new AbortController().signal,
        getSession: async () => ({ id: 'session' }),
        openTable: async () => undefined,
        execute,
        cancelQuery: vi.fn(),
      }),
    ).rejects.toThrow('worker failed');
    expect(execute).toHaveBeenCalledTimes(1);
  });
});

describe('query failure classification', () => {
  it('keeps lifecycle deadlines out of the worker query-error contract', () => {
    expectTypeOf<'deadline'>().not.toMatchTypeOf<QueryErrorCode>();
  });

  it('treats only local pre-SQL aborts and worker cancellation errors as cancelled', () => {
    expect(
      queryFailureOutcome(new DOMException('cancelled', 'AbortError'), 12, 'browser_wasm'),
    ).toMatchObject({ code: 'cancelled', message: 'Query cancelled' });

    const cancellation = new AxonWorkerError({
      request_id: 'execution-1',
      error: {
        code: 'execution_failed',
        message: 'experimental browser DataFusion query cancelled during execution',
        target: 'browser_wasm',
      },
    });
    expect(queryFailureOutcome(cancellation, 13, 'browser_wasm')).toMatchObject({
      code: 'cancelled',
      message: 'Query cancelled',
    });

    const workerFailure = new AxonWorkerError({
      request_id: 'execution-1',
      error: {
        code: 'execution_failed',
        message: 'worker failed after cancellation was requested',
        target: 'browser_wasm',
      },
    });
    expect(queryFailureOutcome(workerFailure, 14, 'browser_wasm')).toMatchObject({
      code: 'execution_failed',
      message: 'worker failed after cancellation was requested',
    });
  });

  it('invalidates the cached session after worker replacement or authoritative deadline cleanup', async () => {
    const deadlineController = new AbortController();
    deadlineController.abort();
    let deadlineFailure: unknown;
    try {
      await runCancelableQueryStages({
        signal: new AbortController().signal,
        deadlineSignal: deadlineController.signal,
        getSession: async () => ({ id: 'session' }),
        openTable: async () => undefined,
        execute: async () => 'result',
        cancelQuery: vi.fn(),
      });
    } catch (error) {
      deadlineFailure = error;
    }

    const invalidatedWorker = new AxonWorkerError({
      request_id: 'execution-invalidated',
      error: {
        code: 'execution_failed',
        message: 'browser query session invalidated: child worker crashed',
        target: 'browser_wasm',
      },
    });
    const ordinaryWorkerFailure = new AxonWorkerError({
      request_id: 'execution-failed',
      error: {
        code: 'execution_failed',
        message: 'query planning failed',
        target: 'browser_wasm',
      },
    });

    expect(queryFailureInvalidatesSession(deadlineFailure)).toBe(true);
    expect(queryFailureInvalidatesSession(invalidatedWorker)).toBe(true);
    expect(queryFailureInvalidatesSession(ordinaryWorkerFailure)).toBe(false);
  });
});

describe('admitted editor query bounds and delivery', () => {
  it('carries every admitted budget and requests single-buffer delivery', () => {
    expect(queryExecutionOptionsForAdmission(admission, { limit: 501, offset: 0 })).toEqual({
      collect_metrics: true,
      include_explain: true,
      result_page: { limit: 501, offset: 0 },
      runtime_limits: {
        max_result_rows: 501,
        max_arrow_ipc_bytes: 8 * 1024 * 1024,
        max_preview_string_bytes: 256 * 1024,
        max_scan_bytes: 64 * 1024 * 1024,
      },
    });
    expect(queryClientOptionsForAdmission(admission)).toEqual({
      requestId: 'execution-1',
      delivery: 'single_buffer',
    });
  });

  it('keeps generic SDK delivery chunked unless a caller explicitly selects single-buffer', () => {
    const request: QueryRequest = {
      table_uri: 'https://example.test/table',
      sql: 'select 1',
      preferred_target: 'browser_wasm',
    };

    expect(queryCommand('generic', 'events', request)).toMatchObject({
      sql: { delivery: 'chunked_buffers' },
    });
    expect(queryCommand('editor', 'events', request, 'single_buffer')).toMatchObject({
      sql: { delivery: 'single_buffer' },
    });
  });

  it('publishes no Arrow output when actual bytes exceed the admitted maximum', () => {
    const publish = vi.fn();
    let failure: unknown;

    try {
      withValidatedArrowIpcOutput(new Uint8Array(9), 9, 8, publish);
    } catch (error) {
      failure = error;
    }
    expect(failure).toMatchObject({
      code: 'execution_failed',
      target: 'browser_wasm',
      message: expect.stringMatching(/max_arrow_ipc_bytes.*actual 9.*limit 8/),
    });
    expect(publish).not.toHaveBeenCalled();
  });

  it('publishes only after actual and reported Arrow byte lengths agree', () => {
    const publish = vi.fn(({ byteLength }: { byteLength: number }) => byteLength);

    expect(withValidatedArrowIpcOutput(new Uint8Array(4), 4, 8, publish)).toBe(4);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(() => withValidatedArrowIpcOutput(new Uint8Array(4), 3, 8, publish)).toThrow(
      /reported Arrow IPC byte length 3 did not match actual 4/,
    );
    expect(publish).toHaveBeenCalledTimes(1);
  });
});
