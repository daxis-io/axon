import { expect, test } from '@playwright/test';

import {
  AxonSdkError,
  AxonWorkerError,
  createAxonBrowserClient,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerCommand,
  type QueryResponse,
  type WireBrowserWorkerMessageEnvelope,
} from '../src/axon-browser-sdk';

const snapshot: BrowserHttpSnapshotDescriptor = {
  table_uri: 'gs://axon-fixtures/partitioned-table',
  snapshot_version: 7,
  partition_column_types: {},
  browser_compatibility: { capabilities: {} },
  required_capabilities: { capabilities: {} },
  active_files: [
    {
      path: 'part-000.parquet',
      url: 'https://example.invalid/part-000.parquet',
      size_bytes: 128,
      partition_values: {},
    },
  ],
};

class FakeWorker implements Pick<
  Worker,
  'addEventListener' | 'removeEventListener' | 'postMessage' | 'terminate'
> {
  readonly commands: BrowserWorkerCommand[] = [];
  terminated = false;
  private readonly listeners = new Map<string, Set<EventListenerOrEventListenerObject>>();

  addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    const listeners = this.listeners.get(type) ?? new Set<EventListenerOrEventListenerObject>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    this.listeners.get(type)?.delete(listener);
  }

  postMessage(message: BrowserWorkerCommand): void {
    this.commands.push(message);
  }

  terminate(): void {
    this.terminated = true;
  }

  emitMessage(data: WireBrowserWorkerMessageEnvelope): void {
    this.emit('message', { data });
  }

  emitError(message: string): void {
    this.emit('error', { message });
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      if (typeof listener === 'function') {
        listener(event as Event);
      } else {
        listener.handleEvent(event as Event);
      }
    }
  }
}

test('rejects a duplicate active request id without orphaning the first request', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const first = client.openDeltaTable('events', snapshot, { requestId: 'req-duplicate' });
  const second = client.dispose('events', { requestId: 'req-duplicate' });

  await expect(settlement(second)).resolves.toMatchObject({
    status: 'rejected',
    message: "duplicate active request id 'req-duplicate'",
  });

  expect(worker.commands).toHaveLength(1);
  expect(worker.commands[0]).toHaveProperty('open_delta_table');

  worker.emitMessage({ opened: { request_id: 'req-duplicate', name: 'events' } });

  await expect(first).resolves.toEqual({ request_id: 'req-duplicate', name: 'events' });
});

test('normalizes Arrow IPC bytes and exposes success fallback reasons', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT COUNT(*) AS row_count FROM events',
      preferred_target: 'browser_wasm',
      options: {
        include_explain: false,
        collect_metrics: true,
      },
    },
    { requestId: 'req-query' },
  );

  worker.emitMessage({
    success: {
      request_id: 'req-query',
      response: queryResponse({
        executed_on: 'native',
        fallback_reason: {
          capability_gate: {
            capability: 'multi_partition_execution',
            required_state: 'native_only',
          },
        },
      }),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
    },
  });

  const result = await resultPromise;
  expect(result.result.bytes).toBeInstanceOf(Uint8Array);
  expect([...result.result.bytes]).toEqual([1, 2, 3, 4]);
  expect(result.fallbackReason).toEqual({
    capability_gate: {
      capability: 'multi_partition_execution',
      required_state: 'native_only',
    },
  });
});

test('routes worker runtime events without settling the active request', async () => {
  const worker = new FakeWorker();
  const events: BrowserWorkerEventEnvelope[] = [];
  const client = createAxonBrowserClient({
    worker: worker as unknown as Worker,
    onEvent: (event) => events.push(event),
  });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT COUNT(*) AS row_count FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-events' },
  );

  worker.emitMessage({
    progress: {
      context: {
        phase: 'query',
        request_id: 'req-query-events',
        query_id: 'req-query-events',
        table_name: 'events',
      },
      stage: 'started',
    },
  });

  await expect(settlement(resultPromise)).resolves.toEqual({ status: 'pending' });
  expect(events).toEqual([
    {
      progress: {
        context: {
          phase: 'query',
          request_id: 'req-query-events',
          query_id: 'req-query-events',
          table_name: 'events',
        },
        stage: 'started',
      },
    },
  ]);

  worker.emitMessage({
    success: {
      request_id: 'req-query-events',
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
    },
  });

  await expect(resultPromise).resolves.toMatchObject({
    request_id: 'req-query-events',
  });
});

test('preserves worker error fallback reasons', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.dispose('missing', { requestId: 'req-error' });

  worker.emitMessage({
    error: {
      request_id: 'req-error',
      error: {
        code: 'fallback_required',
        message: 'browser worker lost connectivity',
        target: 'browser_wasm',
        fallback_reason: 'network_failure',
      },
    },
  });

  await expect(resultPromise).rejects.toMatchObject({
    name: 'AxonWorkerError',
    fallbackReason: 'network_failure',
  } satisfies Partial<AxonWorkerError>);
});

test('rejects pending requests when the worker emits an error event', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.openDeltaTable('events', snapshot, { requestId: 'req-open' });

  worker.emitError('worker boot failed');

  await expect(resultPromise).rejects.toThrow(AxonSdkError);
  await expect(resultPromise).rejects.toThrow('worker boot failed');
});

test('terminate rejects pending requests and terminates the worker', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.openDeltaTable('events', snapshot, { requestId: 'req-open' });

  client.terminate();

  await expect(resultPromise).rejects.toThrow(AxonSdkError);
  await expect(resultPromise).rejects.toThrow('Axon browser client was terminated');
  expect(worker.terminated).toBe(true);
});

function queryResponse(overrides: Partial<QueryResponse> = {}): QueryResponse {
  return {
    executed_on: 'browser_wasm',
    capabilities: { capabilities: {} },
    metrics: {
      bytes_fetched: 128,
      duration_ms: 5,
      files_touched: 1,
      files_skipped: 0,
    },
    ...overrides,
  };
}

async function settlement(
  promise: Promise<unknown>,
): Promise<
  { status: 'pending' } | { status: 'resolved' } | { status: 'rejected'; message: string }
> {
  return Promise.race([
    promise.then(
      () => ({ status: 'resolved' as const }),
      (error) => ({
        status: 'rejected' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    ),
    new Promise<{ status: 'pending' }>((resolve) =>
      setTimeout(() => resolve({ status: 'pending' }), 0),
    ),
  ]);
}
