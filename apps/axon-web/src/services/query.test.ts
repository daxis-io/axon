import { describe, expect, it, vi } from 'vitest';
import { runCancelableQueryStages } from './query.ts';

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

  it('targets the running SQL execution once when cancellation wins during SQL', async () => {
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

    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(cancelQuery).toHaveBeenCalledTimes(1);
    expect(cancelQuery).toHaveBeenCalledWith(session);
    query.resolve('late result');
  });

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
});
