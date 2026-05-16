import { expect, test } from '@playwright/test';

import {
  AxonProtocolError,
  AxonSdkError,
  AxonWorkerError,
  createAxonBrowserClient,
  getPlatformFeatures,
  redactUrlSecrets,
  selectBundle,
  type BrowserBundleManifest,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerCommand,
  type PlatformFeatureScope,
  type PlatformFeatures,
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

  emitRawMessage(data: unknown): void {
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

const baselineOnlyFeatures: PlatformFeatures = {
  crossOriginIsolated: false,
  wasmSIMD: false,
  wasmThreads: false,
  bigInt64Array: false,
};

const simdOnlyFeatures: PlatformFeatures = {
  crossOriginIsolated: false,
  wasmSIMD: true,
  wasmThreads: false,
  bigInt64Array: true,
};

const threadedSimdFeatures: PlatformFeatures = {
  crossOriginIsolated: true,
  wasmSIMD: true,
  wasmThreads: true,
  bigInt64Array: true,
};

const manifest: BrowserBundleManifest = {
  bundles: [
    {
      id: 'baseline',
      tier: 'baseline',
      workerUrl: '/workers/axon-browser-baseline.js',
    },
    {
      id: 'simd',
      tier: 'simd',
      workerUrl: '/workers/axon-browser-simd.js',
      requiredFeatures: {
        wasmSIMD: true,
      },
    },
    {
      id: 'threaded',
      tier: 'threaded',
      workerUrl: '/workers/axon-browser-threaded.js',
      requiredFeatures: {
        crossOriginIsolated: true,
        wasmThreads: true,
      },
    },
    {
      id: 'simd-threaded',
      tier: 'simd_threaded',
      workerUrl: '/workers/axon-browser-simd-threaded.js',
      requiredFeatures: {
        crossOriginIsolated: true,
        wasmSIMD: true,
        wasmThreads: true,
      },
    },
  ],
};

test('selectBundle keeps the single-threaded baseline when optional features are unavailable', () => {
  const selected = selectBundle(manifest, baselineOnlyFeatures);

  expect(selected.bundle.id).toBe('baseline');
  expect(selected.features).toEqual(baselineOnlyFeatures);
});

test('selectBundle prefers a SIMD bundle without requiring cross-origin isolation', () => {
  const selected = selectBundle(manifest, simdOnlyFeatures);

  expect(selected.bundle.id).toBe('simd');
});

test('selectBundle prefers the threaded SIMD tier only when isolation and threads are available', () => {
  const selected = selectBundle(manifest, threadedSimdFeatures);

  expect(selected.bundle.id).toBe('simd-threaded');
});

test('selectBundle honors BigInt64Array requirements declared by a bundle', () => {
  const bigintManifest: BrowserBundleManifest = {
    bundles: [
      ...manifest.bundles,
      {
        id: 'simd-bigint64',
        tier: 'simd',
        workerUrl: '/workers/axon-browser-simd-bigint64.js',
        requiredFeatures: {
          wasmSIMD: true,
          bigInt64Array: true,
        },
      },
    ],
  };

  expect(
    selectBundle(bigintManifest, { ...simdOnlyFeatures, bigInt64Array: false }).bundle.id,
  ).toBe('simd');
  expect(selectBundle(bigintManifest, simdOnlyFeatures).bundle.id).toBe('simd-bigint64');
});

test('selectBundle does not choose future bundles before their artifacts are shipped', () => {
  const futureManifest: BrowserBundleManifest = {
    bundles: [
      ...manifest.bundles,
      {
        id: 'future-simd-threaded',
        tier: 'simd_threaded',
        status: 'future',
        workerUrl: '/workers/axon-browser-future-simd-threaded.js',
        requiredFeatures: {
          crossOriginIsolated: true,
          wasmSIMD: true,
          wasmThreads: true,
        },
      },
    ],
  };

  expect(selectBundle(futureManifest, threadedSimdFeatures).bundle.id).toBe('simd-threaded');
});

test('selectBundle rejects unknown bundle statuses from dynamic manifests', () => {
  const invalidManifest: BrowserBundleManifest = {
    bundles: [
      {
        id: 'unknown-status',
        tier: 'simd',
        status: 'experimental' as never,
        workerUrl: '/workers/axon-browser-experimental.js',
        requiredFeatures: {
          wasmSIMD: true,
        },
      },
      manifest.bundles[0],
    ],
  };

  expect(() => selectBundle(invalidManifest, simdOnlyFeatures)).toThrow(AxonSdkError);
});

test('createAxonBrowserClient constructs a worker from the selected bundle', () => {
  const workerGlobal = globalThis as typeof globalThis & { Worker?: typeof Worker };
  const previousWorker = workerGlobal.Worker;
  const createdWorkers: { url: string | URL; options?: WorkerOptions }[] = [];

  class ConstructedWorker extends FakeWorker {
    constructor(url: string | URL, options?: WorkerOptions) {
      super();
      createdWorkers.push({ url, options });
    }
  }

  workerGlobal.Worker = ConstructedWorker as unknown as typeof Worker;

  try {
    const client = createAxonBrowserClient({
      bundleManifest: manifest,
      platformFeatures: simdOnlyFeatures,
      workerOptions: {
        name: 'axon-worker',
      },
    });

    client.terminate();

    expect(createdWorkers).toEqual([
      {
        url: '/workers/axon-browser-simd.js',
        options: {
          type: 'module',
          name: 'axon-worker',
        },
      },
    ]);
  } finally {
    if (previousWorker) {
      workerGlobal.Worker = previousWorker;
    } else {
      Reflect.deleteProperty(workerGlobal, 'Worker');
    }
  }
});

test('getPlatformFeatures tracks browser isolation, SIMD, threads, and BigInt64Array', () => {
  class FakeSharedArrayBuffer {}
  class FakeMemory {
    readonly buffer = new FakeSharedArrayBuffer();
  }

  const scope = {
    crossOriginIsolated: true,
    WebAssembly: {
      validate: () => true,
      Memory: FakeMemory,
    },
    BigInt64Array: class FakeBigInt64Array {},
    SharedArrayBuffer: FakeSharedArrayBuffer,
    Atomics: {},
  } as unknown as PlatformFeatureScope;

  expect(getPlatformFeatures(scope)).toEqual({
    crossOriginIsolated: true,
    wasmSIMD: true,
    wasmThreads: true,
    bigInt64Array: true,
  });
  expect(getPlatformFeatures({ ...scope, crossOriginIsolated: false }).wasmThreads).toBe(false);
});

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

test('preserves optional bounded result previews on success envelopes', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT COUNT(*) AS row_count FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-preview' },
  );

  worker.emitMessage({
    success: {
      request_id: 'req-query-preview',
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
      preview: {
        columns: ['row_count'],
        rows: [[4]],
        row_count: 1,
        preview_row_limit: 100,
        truncated: false,
      },
    },
  });

  await expect(resultPromise).resolves.toMatchObject({
    preview: {
      columns: ['row_count'],
      rows: [[4]],
      row_count: 1,
      preview_row_limit: 100,
      truncated: false,
    },
  });
});

test('rejects malformed result previews on success envelopes', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT COUNT(*) AS row_count FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-preview-invalid' },
  );

  worker.emitMessage({
    success: {
      request_id: 'req-query-preview-invalid',
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
      preview: {
        columns: ['row_count'],
        rows: [['4']],
        row_count: Number.NaN,
        preview_row_limit: 100,
        truncated: false,
      },
    },
  });

  await expect(resultPromise).rejects.toThrow(AxonProtocolError);
  await expect(resultPromise).rejects.toThrow('success.preview.row_count');
});

test('redacts URL credentials, query strings, and fragments from diagnostics', () => {
  expect(
    redactUrlSecrets(
      'failed to fetch https://user:secret@example.invalid/table/file.parquet?X-Amz-Signature=abc#footer',
    ),
  ).toBe('failed to fetch https://example.invalid/table/file.parquet');
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

test('rejects malformed worker runtime events without routing them to the event handler', async () => {
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
    { requestId: 'req-query-malformed-event' },
  );

  worker.emitRawMessage({
    progress: {
      context: {
        phase: 'query',
        request_id: 'req-query-malformed-event',
        query_id: 'req-query-malformed-event',
        table_name: 'events',
      },
    },
  });

  await expect(resultPromise).rejects.toThrow(AxonProtocolError);
  await expect(resultPromise).rejects.toThrow('progress.stage');
  expect(events).toEqual([]);
});

test('rejects worker messages with multiple envelope tags', async () => {
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
    { requestId: 'req-query-mixed-envelope' },
  );

  worker.emitRawMessage({
    progress: {
      context: {
        phase: 'query',
        request_id: 'req-query-mixed-envelope',
        query_id: 'req-query-mixed-envelope',
        table_name: 'events',
      },
      stage: 'started',
    },
    success: {
      request_id: 'req-query-mixed-envelope',
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
    },
  });

  await expect(resultPromise).rejects.toThrow(AxonProtocolError);
  await expect(resultPromise).rejects.toThrow('multiple Axon envelope tags');
  expect(events).toEqual([]);
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
