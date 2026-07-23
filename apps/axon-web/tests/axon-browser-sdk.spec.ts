import { expect, test } from '@playwright/test';

import * as browserSdk from '../src/axon-browser-sdk';
import { openDaxisResolvedDeltaTable } from '../examples/daxis-descriptor-resolver';
import {
  DaxisReadAccessPlanError,
  openDaxisReadAccessPlanTable,
} from '../examples/daxis-read-access-plan';
import {
  DaxisObjectGrantError,
  createDaxisObjectGrantClient,
  createDaxisObjectGrantAdapter,
} from '../examples/daxis-object-grant-adapter';
import {
  DaxisHeadlessQueryError,
  executeDaxisApprovedAxonRead,
  validateDaxisApprovedAxonReadDescriptor,
  type DaxisApprovedAxonReadDescriptor,
} from '../examples/daxis-headless-query';
import {
  AxonProtocolError,
  AxonSdkError,
  AxonWorkerError,
  DeltaSharingError,
  DeltaLocationResolverError,
  createDeltaSharingClient,
  createAxonBrowserClient,
  getPlatformFeatures,
  parseReadAccessPlan,
  redactUrlSecrets,
  selectBundle,
  snapshotFromBrokeredDeltaReadPlan,
  validateDeltaLocationResolveResponse,
  type BrowserBundleManifest,
  type BrowserHttpParquetDatasetDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerCommand,
  type BrokeredDeltaReadAccessPlan,
  type DeltaSharingFetch,
  type DeltaSharingReadPlan,
  type DeltaSharingSession,
  type DeltaLocationResolveRequest,
  type DeltaLocationResolveResponse,
  type ResolvedSnapshotDescriptor,
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

const parquetDataset: BrowserHttpParquetDatasetDescriptor = {
  table_uri: 'https://example.invalid/datasets/events',
  partition_column_types: {},
  browser_compatibility: { capabilities: {} },
  required_capabilities: { capabilities: {} },
  files: [
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

function failingDeltaSharingSession(error: Error): DeltaSharingSession {
  return {
    profile: {
      kind: 'delta_sharing',
      profileSource: 'uploaded_profile',
      endpoint: 'https://sharing.example.test',
      authMode: 'bearer',
    },
    listShares: async () => ({ items: [] }),
    listSchemas: async () => ({ items: [] }),
    listTables: async () => ({ items: [] }),
    listAllTables: async () => ({ items: [] }),
    getTableVersion: async () => 0,
    getTableMetadata: async (table) => ({ table, rawActions: [] }),
    resolveTable: async () => {
      throw error;
    },
  };
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

test('default bundle manifest points baseline at the Daxis-facing DataFusion worker artifact', () => {
  const selected = selectBundle(browserSdk.AXON_BROWSER_BUNDLE_MANIFEST, baselineOnlyFeatures);

  expect(selected.bundle.id).toBe('baseline');
  expect(selected.bundle.workerUrl).toBe('/workers/axon-web-worker.js');
  expect(selected.bundle.wasmUrl).toBe('/workers/axon_web_wasm_bg.wasm');
  expect(String(selected.bundle.workerUrl)).not.toContain('browser-engine-worker');
  expect(String(selected.bundle.wasmUrl)).not.toContain('browser_engine_worker');
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

test('createAxonBrowserClient with workerUrl does not construct a Worker at creation', () => {
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
      workerUrl: '/workers/axon-browser-baseline.js',
      workerOptions: {
        name: 'axon-worker',
      },
    });

    expect(createdWorkers).toEqual([]);

    client.terminate();

    expect(createdWorkers).toEqual([]);
  } finally {
    if (previousWorker) {
      workerGlobal.Worker = previousWorker;
    } else {
      Reflect.deleteProperty(workerGlobal, 'Worker');
    }
  }
});

test('createAxonBrowserClient with a worker factory does not call the factory at creation', () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });

  expect(calls).toBe(0);

  client.terminate();

  expect(calls).toBe(0);
});

test('createAxonBrowserClient preserves concrete Worker eager behavior', () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  client.cancelQuery('query-daxis-timeout', { requestId: 'cancel-daxis-timeout' });

  expect(worker.commands).toEqual([
    {
      cancel: {
        request_id: 'cancel-daxis-timeout',
        query_id: 'query-daxis-timeout',
      },
    },
  ]);
});

test('first worker-backed command constructs a lazy worker exactly once', async () => {
  const worker = new FakeWorker();
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return worker as unknown as Worker;
    },
  });

  const opened = client.openDeltaTable('events', snapshot, { requestId: 'req-lazy-open' });

  expect(calls).toBe(1);
  expect(worker.commands).toEqual([
    {
      open_delta_table: {
        request_id: 'req-lazy-open',
        name: 'events',
        snapshot,
      },
    },
  ]);

  worker.emitMessage({ opened: { request_id: 'req-lazy-open', name: 'events' } });

  await expect(opened).resolves.toEqual({ request_id: 'req-lazy-open', name: 'events' });
  expect(calls).toBe(1);
});

test('concurrent first worker-backed commands share one lazy worker-backed client', async () => {
  const worker = new FakeWorker();
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return worker as unknown as Worker;
    },
  });

  const opened = client.openDeltaTable('events', snapshot, { requestId: 'req-lazy-open' });
  const disposed = client.dispose('events', { requestId: 'req-lazy-dispose' });

  expect(calls).toBe(1);
  expect(worker.commands.map((command) => Object.keys(command)[0])).toEqual([
    'open_delta_table',
    'dispose',
  ]);

  worker.emitMessage({ opened: { request_id: 'req-lazy-open', name: 'events' } });
  worker.emitMessage({ disposed: { request_id: 'req-lazy-dispose', name: 'events' } });

  await expect(opened).resolves.toEqual({ request_id: 'req-lazy-open', name: 'events' });
  await expect(disposed).resolves.toEqual({ request_id: 'req-lazy-dispose', name: 'events' });
  expect(calls).toBe(1);
});

test('failed lazy worker initialization can be retried', async () => {
  const worker = new FakeWorker();
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      if (calls === 1) {
        throw new Error('worker unavailable');
      }
      return worker as unknown as Worker;
    },
  });

  await expect(
    client.openDeltaTable('events', snapshot, { requestId: 'req-lazy-fails' }),
  ).rejects.toThrow('worker unavailable');

  const opened = client.openDeltaTable('events', snapshot, { requestId: 'req-lazy-retry' });
  worker.emitMessage({ opened: { request_id: 'req-lazy-retry', name: 'events' } });

  await expect(opened).resolves.toEqual({ request_id: 'req-lazy-retry', name: 'events' });
  expect(calls).toBe(2);
});

test('terminate before first lazy use does not construct a worker', () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });

  client.terminate();

  expect(calls).toBe(0);
});

test('cancelQuery before first lazy use does not construct a worker', () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });

  client.cancelQuery('query-before-worker', { requestId: 'cancel-before-worker' });

  expect(calls).toBe(0);
});

test('lazy bundle-backed client constructs a Worker from the selected bundle on first command', async () => {
  const workerGlobal = globalThis as typeof globalThis & { Worker?: typeof Worker };
  const previousWorker = workerGlobal.Worker;
  const createdWorkers: { url: string | URL; options?: WorkerOptions }[] = [];
  const workers: ConstructedWorker[] = [];

  class ConstructedWorker extends FakeWorker {
    constructor(url: string | URL, options?: WorkerOptions) {
      super();
      createdWorkers.push({ url, options });
      workers.push(this);
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

    expect(createdWorkers).toEqual([]);

    const opened = client.openDeltaTable('events', snapshot, { requestId: 'req-bundle-open' });

    expect(createdWorkers).toEqual([
      {
        url: '/workers/axon-browser-simd.js',
        options: {
          type: 'module',
          name: 'axon-worker',
        },
      },
    ]);

    workers[0].emitMessage({ opened: { request_id: 'req-bundle-open', name: 'events' } });
    await expect(opened).resolves.toEqual({ request_id: 'req-bundle-open', name: 'events' });
  } finally {
    if (previousWorker) {
      workerGlobal.Worker = previousWorker;
    } else {
      Reflect.deleteProperty(workerGlobal, 'Worker');
    }
  }
});

test('lazy openDeltaLocation resolver failures do not construct a worker', async () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 'gcs',
      tableUri: 'gs://bucket/table',
      credentialProfile: { id: 'browser-profile' },
      resolveDeltaLocation: async () => {
        throw new DeltaLocationResolverError('storage_auth_failed', 'storage broker denied access');
      },
    }),
  ).rejects.toThrow(DeltaLocationResolverError);

  expect(calls).toBe(0);
});

test('lazy openDeltaShare resolution failures do not construct a worker', async () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });
  const session = failingDeltaSharingSession(
    new DeltaSharingError('unsupported_access_mode', 'directory access requires a resolver'),
  );

  await expect(
    client.openDeltaShare('shared_events', {
      session,
      table: { share: 'main', schema: 'analytics', table: 'events' },
    }),
  ).rejects.toThrow(DeltaSharingError);

  expect(calls).toBe(0);
});

test('lazy openUnityCatalogTable non-browser plans do not construct a worker', async () => {
  let calls = 0;
  const client = createAxonBrowserClient({
    worker: () => {
      calls += 1;
      return new FakeWorker() as unknown as Worker;
    },
  });

  await expect(
    client.openUnityCatalogTable('payments', {
      fullName: 'main.secure.payments',
      resolveReadAccessPlan: async () => ({
        plan_type: 'sql_fallback_required',
        tableId: 'tbl-governed',
        fullName: 'main.secure.payments',
        reason: 'row_filter',
        message: 'governed table requires service-side SQL execution',
        statementEndpoint: 'https://dbc.example.com/api/2.0/sql/statements',
        warehouseRequired: true,
      }),
    }),
  ).resolves.toMatchObject({
    status: 'sql_fallback_required',
    reason: 'row_filter',
  });

  await expect(
    client.openUnityCatalogTable('denied', {
      fullName: 'main.secure.denied',
      resolveReadAccessPlan: async () => ({
        plan_type: 'blocked',
        tableId: 'tbl-blocked',
        fullName: 'main.secure.denied',
        reason: 'unknown_policy_state',
        message: 'policy state is not safe for browser execution',
      }),
    }),
  ).resolves.toMatchObject({
    status: 'blocked',
    reason: 'unknown_policy_state',
  });

  expect(calls).toBe(0);
});

test('does not export cancelCommand as a request-response SDK helper', () => {
  expect(Object.hasOwn(browserSdk, 'cancelCommand')).toBe(false);
});

test('cancelQuery sends a fire-and-forget worker cancellation without pending request state', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  client.cancelQuery('query-daxis-timeout', { requestId: 'cancel-daxis-timeout' });

  expect(worker.commands).toEqual([
    {
      cancel: {
        request_id: 'cancel-daxis-timeout',
        query_id: 'query-daxis-timeout',
      },
    },
  ]);

  const opened = client.openDeltaTable('events', snapshot, { requestId: 'cancel-daxis-timeout' });
  worker.emitMessage({ opened: { request_id: 'cancel-daxis-timeout', name: 'events' } });

  await expect(opened).resolves.toEqual({
    request_id: 'cancel-daxis-timeout',
    name: 'events',
  });
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

test('parseReadAccessPlan accepts fake-BFF contract variants and rejects raw secrets', () => {
  const brokered = parseReadAccessPlan(brokeredDeltaPlan());
  const sharing = parseReadAccessPlan(deltaSharingAccessPlan());
  const fallback = parseReadAccessPlan({
    plan_type: 'sql_fallback_required',
    tableId: 'tbl-governed',
    fullName: 'main.secure.payments',
    reason: 'row_filter',
    message: 'governed table requires service-side SQL execution',
    statementEndpoint: 'https://dbc.example.com/api/2.0/sql/statements',
    warehouseRequired: true,
  });
  const blocked = parseReadAccessPlan({
    plan_type: 'blocked',
    tableId: 'tbl-blocked',
    fullName: 'main.secure.denied',
    reason: 'unknown_policy_state',
    message: 'policy state is not safe for browser execution',
  });

  expect(brokered.plan_type).toBe('brokered_delta');
  expect(sharing.plan_type).toBe('delta_sharing');
  expect(fallback.plan_type).toBe('sql_fallback_required');
  expect(blocked.plan_type).toBe('blocked');

  expect(() =>
    parseReadAccessPlan({
      ...brokeredDeltaPlan(),
      databricks_bearer_token: 'dapi-secret-token',
    }),
  ).toThrow(AxonProtocolError);
  expect(() =>
    parseReadAccessPlan({
      plan_type: 'uc_oss_connection',
      tableId: 'tbl-oss',
    }),
  ).toThrow(AxonProtocolError);
});

test('snapshotFromBrokeredDeltaReadPlan adapts object-grant plans into browser descriptors', () => {
  const descriptor = snapshotFromBrokeredDeltaReadPlan(brokeredDeltaPlan(), brokeredSnapshot(), {
    'part-000.parquet': 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
  });

  expect(descriptor).toEqual({
    table_uri: 's3://prod-bucket/tables/orders',
    snapshot_version: 12,
    partition_column_types: { region: 'string' },
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: [
      {
        path: 'part-000.parquet',
        url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
        size_bytes: 128,
        partition_values: { region: 'west' },
      },
    ],
  });
});

test('openUnityCatalogTable opens brokered Delta plans through the existing worker descriptor path', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const openedPromise = client.openUnityCatalogTable('orders', {
    fullName: 'main.sales.orders',
    session: { id: 'browser-session-123' },
    resolveReadAccessPlan: async (request) => {
      expect(request).toEqual({
        fullName: 'main.sales.orders',
        session: { id: 'browser-session-123' },
      });
      return brokeredDeltaPlan();
    },
    brokeredDelta: {
      resolveSnapshot: async () => brokeredSnapshot(),
      batchSign: async (_plan, paths) => ({
        signedUrls: paths.map((path) => ({
          path,
          url: `https://storage.example.test/${path}?X-Amz-Signature=abc`,
          expiresAtEpochMs: 4_102_444_800_000,
        })),
      }),
    },
    requestId: 'req-uc-brokered',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  expect(worker.commands[0]).toMatchObject({
    open_delta_table: {
      request_id: 'req-uc-brokered',
      name: 'orders',
      snapshot: {
        table_uri: 's3://prod-bucket/tables/orders',
        active_files: [
          {
            path: 'part-000.parquet',
            url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
          },
        ],
      },
    },
  });
  expect(JSON.stringify(worker.commands[0])).not.toContain('grant-456');
  expect(JSON.stringify(worker.commands[0])).not.toContain('browser-session-123');

  worker.emitMessage({ opened: { request_id: 'req-uc-brokered', name: 'orders' } });
  await expect(openedPromise).resolves.toMatchObject({
    status: 'opened',
    planType: 'brokered_delta',
    request_id: 'req-uc-brokered',
    name: 'orders',
  });
});

test('openUnityCatalogTable opens delta_sharing plans without browser-owned UC credentials', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const openedPromise = client.openUnityCatalogTable('shared_events', {
    fullName: 'main.analytics.events',
    resolveReadAccessPlan: async () => deltaSharingAccessPlan(),
    requestId: 'req-uc-sharing',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  expect(worker.commands[0]).toMatchObject({
    open_delta_table: {
      request_id: 'req-uc-sharing',
      name: 'shared_events',
      snapshot: {
        table_uri: 'delta-sharing://sharing.example.test/main.analytics.events',
        snapshot_version: 0,
        active_files: [
          {
            path: 'part-000.parquet',
            url: 'https://storage.example.test/share/part-000.parquet?X-Amz-Signature=abc',
          },
        ],
      },
    },
  });
  expect(JSON.stringify(worker.commands[0])).not.toContain('Authorization');
  expect(JSON.stringify(worker.commands[0])).not.toContain('databricks_bearer_token');

  worker.emitMessage({ opened: { request_id: 'req-uc-sharing', name: 'shared_events' } });
  await expect(openedPromise).resolves.toMatchObject({
    status: 'opened',
    planType: 'delta_sharing',
  });
});

test('openUnityCatalogTable rejects expired read-access plans before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openUnityCatalogTable('orders', {
      fullName: 'main.sales.orders',
      resolveReadAccessPlan: async () => ({
        ...brokeredDeltaPlan(),
        expiresAtEpochMs: 1,
      }),
      brokeredDelta: {
        resolveSnapshot: async () => brokeredSnapshot(),
        batchSign: async () => ({
          signedUrls: [
            {
              path: 'part-000.parquet',
              url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
              expiresAtEpochMs: 4_102_444_800_000,
            },
          ],
        }),
      },
    }),
  ).rejects.toThrow(AxonProtocolError);

  await expect(
    client.openUnityCatalogTable('shared_events', {
      fullName: 'main.analytics.events',
      resolveReadAccessPlan: async () => ({
        ...deltaSharingAccessPlan(),
        expiresAtEpochMs: 1,
      }),
    }),
  ).rejects.toThrow(AxonProtocolError);

  expect(worker.commands).toHaveLength(0);
});

test('openUnityCatalogTable rejects expired brokered signed URLs before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openUnityCatalogTable('orders', {
      fullName: 'main.sales.orders',
      resolveReadAccessPlan: async () => brokeredDeltaPlan(),
      brokeredDelta: {
        resolveSnapshot: async () => brokeredSnapshot(),
        batchSign: async () => ({
          signedUrls: [
            {
              path: 'part-000.parquet',
              url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
              expiresAtEpochMs: 1,
            },
          ],
        }),
      },
    }),
  ).rejects.toThrow(AxonProtocolError);

  expect(worker.commands).toHaveLength(0);
});

test('openUnityCatalogTable keeps SQL fallback explicit and blocked plans fail closed', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openUnityCatalogTable('payments', {
      fullName: 'main.secure.payments',
      resolveReadAccessPlan: async () => ({
        plan_type: 'sql_fallback_required',
        tableId: 'tbl-governed',
        fullName: 'main.secure.payments',
        reason: 'row_filter',
        message: 'governed table requires service-side SQL execution',
        statementEndpoint: 'https://dbc.example.com/api/2.0/sql/statements',
        warehouseRequired: true,
      }),
    }),
  ).resolves.toMatchObject({
    status: 'sql_fallback_required',
    reason: 'row_filter',
  });

  const routed = await client.openUnityCatalogTable('payments', {
    fullName: 'main.secure.payments',
    serverFallbackEnabled: true,
    resolveReadAccessPlan: async () => ({
      plan_type: 'sql_fallback_required',
      tableId: 'tbl-governed',
      fullName: 'main.secure.payments',
      reason: 'row_filter',
      message: 'governed table requires service-side SQL execution',
      statementEndpoint: 'https://dbc.example.com/api/2.0/sql/statements',
      warehouseRequired: true,
    }),
    executeSqlFallback: async (plan) => ({ routedTo: plan.statementEndpoint }),
  });
  expect(routed).toMatchObject({
    status: 'sql_fallback_routed',
    reason: 'row_filter',
  });

  await expect(
    client.openUnityCatalogTable('denied', {
      fullName: 'main.secure.denied',
      resolveReadAccessPlan: async () => ({
        plan_type: 'blocked',
        tableId: 'tbl-blocked',
        fullName: 'main.secure.denied',
        reason: 'unknown_policy_state',
        message: 'policy state is not safe for browser execution',
      }),
    }),
  ).resolves.toMatchObject({
    status: 'blocked',
    reason: 'unknown_policy_state',
  });
  expect(worker.commands).toHaveLength(0);
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

test('openParquetDataset opens standard Parquet files without Delta snapshot metadata', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const openedPromise = client.openParquetDataset('events', parquetDataset, {
    requestId: 'req-open-parquet',
  });

  expect(worker.commands).toEqual([
    {
      open_parquet_dataset: {
        request_id: 'req-open-parquet',
        name: 'events',
        dataset: parquetDataset,
      },
    },
  ]);

  worker.emitMessage({ opened: { request_id: 'req-open-parquet', name: 'events' } });
  await expect(openedPromise).resolves.toEqual({
    request_id: 'req-open-parquet',
    name: 'events',
  });

  const resultPromise = client.query('events', 'SELECT id FROM events', {
    requestId: 'req-query-parquet',
  });
  const queryCommand = worker.commands[1];
  expect(queryCommand).toHaveProperty('sql');
  if (!('sql' in queryCommand)) {
    throw new Error('expected sql command');
  }
  expect(queryCommand.sql.query.table_uri).toBe(parquetDataset.table_uri);
  expect(queryCommand.sql.query).not.toHaveProperty('snapshot_version');

  worker.emitMessage({
    success: {
      request_id: 'req-query-parquet',
      response: queryResponse({ executed_on: 'browser_wasm' }),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2, 3, 4],
      },
    },
  });
  await expect(resultPromise).resolves.toMatchObject({
    response: {
      executed_on: 'browser_wasm',
    },
  });
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

test('query sends chunked delivery with browser-safe defaults', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-defaults' },
  );

  await Promise.resolve();
  expect(worker.commands).toHaveLength(1);
  const command = worker.commands[0];
  expect(command).toHaveProperty('sql');
  if (!('sql' in command)) {
    throw new Error('expected sql command');
  }
  expect(command.sql).toMatchObject({
    request_id: 'req-query-defaults',
    output: 'arrow_ipc_stream',
    delivery: 'chunked_buffers',
    browser_safe_defaults: true,
    query: {
      options: {
        result_page: {
          limit: 501,
          offset: 0,
        },
        runtime_limits: {
          max_result_rows: 501,
          max_arrow_ipc_bytes: 8 * 1024 * 1024,
          max_preview_string_bytes: 256 * 1024,
        },
      },
    },
  });

  worker.emitMessage(legacySuccessMessage('req-query-defaults', [1, 2, 3, 4]));
  await expect(resultPromise).resolves.toMatchObject({
    request_id: 'req-query-defaults',
  });
});

test('query sends single-buffer delivery only when explicitly selected', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-single-buffer', delivery: 'single_buffer' },
  );

  await Promise.resolve();
  const command = worker.commands[0];
  if (!('sql' in command)) throw new Error('expected sql command');
  expect(command.sql).toMatchObject({
    request_id: 'req-query-single-buffer',
    output: 'arrow_ipc_stream',
    delivery: 'single_buffer',
    browser_safe_defaults: true,
  });

  worker.emitMessage(legacySuccessMessage('req-query-single-buffer', [1, 2, 3, 4]));
  await expect(resultPromise).resolves.toMatchObject({
    request_id: 'req-query-single-buffer',
    result: { delivery: 'single_buffer', byte_length: 4 },
  });
});

test('query preserves stricter browser-safe limits and rejects looser SDK limits', async () => {
  const strictWorker = new FakeWorker();
  const strictClient = createAxonBrowserClient({ worker: strictWorker as unknown as Worker });

  const strictResult = strictClient.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
      options: {
        result_page: {
          limit: 20,
          offset: 40,
        },
        runtime_limits: {
          max_result_rows: 20,
          max_arrow_ipc_bytes: 1024,
          max_preview_string_bytes: 2048,
        },
      },
    },
    { requestId: 'req-query-strict-limits' },
  );
  await Promise.resolve();
  const strictCommand = strictWorker.commands[0];
  if (!('sql' in strictCommand)) {
    throw new Error('expected sql command');
  }
  expect(strictCommand.sql.query.options).toMatchObject({
    result_page: {
      limit: 20,
      offset: 40,
    },
    runtime_limits: {
      max_result_rows: 20,
      max_arrow_ipc_bytes: 1024,
      max_preview_string_bytes: 2048,
    },
  });
  strictWorker.emitMessage(legacySuccessMessage('req-query-strict-limits', [1]));
  await strictResult;

  const looseWorker = new FakeWorker();
  const looseClient = createAxonBrowserClient({ worker: looseWorker as unknown as Worker });
  const looseResult = looseClient.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
      options: {
        result_page: {
          limit: 502,
          offset: 0,
        },
      },
    },
    { requestId: 'req-query-loose-limits' },
  );
  await Promise.resolve();
  if (looseWorker.commands.length > 0) {
    looseWorker.emitMessage(legacySuccessMessage('req-query-loose-limits', [1]));
  }

  await expect(looseResult).rejects.toThrow(AxonSdkError);
  await expect(looseResult).rejects.toThrow('result_page.limit');
});

test('reassembles ordered Arrow IPC chunks before resolving success', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-chunked' },
  );

  worker.emitRawMessage(arrowIpcChunk('req-query-chunked', 0, 0, [1, 2]));
  worker.emitRawMessage(arrowIpcChunk('req-query-chunked', 1, 2, [3, 4]));
  worker.emitRawMessage(chunkedSuccessMessage('req-query-chunked', 4, 2));

  const result = await resultPromise;
  expect([...result.result.bytes]).toEqual([1, 2, 3, 4]);
  expect(result.result).toMatchObject({
    delivery: 'chunked_buffers',
    byte_length: 4,
    chunk_count: 2,
  });
});

test('accepts empty chunked Arrow IPC success without chunk events', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-empty-chunked' },
  );

  worker.emitRawMessage(chunkedSuccessMessage('req-query-empty-chunked', 0, 0));

  const result = await resultPromise;
  expect([...result.result.bytes]).toEqual([]);
});

test('rejects malformed Arrow IPC chunk streams', async () => {
  await expectChunkProtocolReject(
    [arrowIpcChunk('req-query-bad-chunks', 1, 0, [1])],
    'expected Arrow IPC chunk sequence 0',
  );
  await expectChunkProtocolReject(
    [
      arrowIpcChunk('req-query-bad-chunks', 0, 0, [1]),
      arrowIpcChunk('req-query-bad-chunks', 0, 1, [2]),
    ],
    'duplicate Arrow IPC chunk sequence 0',
  );
  await expectChunkProtocolReject(
    [arrowIpcChunk('req-query-bad-chunks', 0, 1, [1])],
    'expected Arrow IPC chunk byte_offset 0',
  );
  await expectChunkProtocolReject(
    [arrowIpcChunk('unknown-request', 0, 0, [1])],
    "unknown Arrow IPC chunk request 'unknown-request'",
  );
  await expectChunkProtocolReject(
    [
      arrowIpcChunk('req-query-bad-chunks', 0, 0, [1]),
      chunkedSuccessMessage('req-query-bad-chunks', 2, 2),
    ],
    'missing Arrow IPC chunks',
  );
  await expectChunkProtocolReject(
    [
      arrowIpcChunk('req-query-bad-chunks', 0, 0, [1]),
      chunkedSuccessMessage('req-query-bad-chunks', 2, 1),
    ],
    'Arrow IPC byte length mismatch',
  );
  await expectChunkProtocolReject(
    [
      arrowIpcChunk('req-query-bad-chunks', 0, 0, [1]),
      chunkedSuccessMessage('req-query-bad-chunks', 1, 2),
    ],
    'Arrow IPC chunk count mismatch',
  );
});

test('rejects chunks after terminal responses', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const firstResult = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-completed' },
  );
  worker.emitRawMessage(chunkedSuccessMessage('req-query-completed', 0, 0));
  await firstResult;

  const secondResult = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-query-after-completion' },
  );
  worker.emitRawMessage(arrowIpcChunk('req-query-completed', 0, 0, [1]));

  await expect(secondResult).rejects.toThrow(AxonProtocolError);
  await expect(secondResult).rejects.toThrow("after request 'req-query-completed' completed");
});

test('inspectParquet sends an inspect command and resolves the inspection summary', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const inspectionPromise = client.inspectParquet('events', 'part-000.parquet', {
    requestId: 'req-inspect',
  });

  expect(worker.commands).toEqual([
    {
      inspect_parquet: {
        request_id: 'req-inspect',
        name: 'events',
        path: 'part-000.parquet',
      },
    },
  ]);

  worker.emitMessage({
    parquet_inspection: {
      request_id: 'req-inspect',
      summary: parquetInspectionSummary(),
    },
  });

  await expect(inspectionPromise).resolves.toMatchObject({
    path: 'part-000.parquet',
    row_count: 3,
    columns: [
      {
        name: 'id',
        encodings: ['PLAIN'],
        has_offset_index: true,
      },
    ],
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

test('DeltaSharingClient imports a bearer profile and lists shares without exposing profile secrets', async () => {
  const fetcher = scriptedFetch([
    jsonResponse({
      items: [{ name: 'retail_share' }],
      nextPageToken: 'shares-page-2',
    }),
  ]);
  const client = createDeltaSharingClient({ fetch: fetcher.fetch });

  const session = await client.connect({
    source: 'json',
    value: JSON.stringify({
      shareCredentialsVersion: 1,
      endpoint: 'https://sharing.example.test/api/2.1/unity-catalog/delta-sharing/',
      bearerToken: 'secret-profile-token',
      expirationTime: '2100-01-01T00:00:00Z',
    }),
  });

  expect(session.profile).toEqual({
    kind: 'delta_sharing',
    profileSource: 'uploaded_profile',
    endpoint: 'https://sharing.example.test/api/2.1/unity-catalog/delta-sharing',
    authMode: 'bearer',
  });
  expect(JSON.stringify(session.profile)).not.toContain('secret-profile-token');

  await expect(session.listShares()).resolves.toEqual({
    items: [{ name: 'retail_share' }],
    nextPageToken: 'shares-page-2',
  });

  expect(String(fetcher.calls[0].input)).toBe(
    'https://sharing.example.test/api/2.1/unity-catalog/delta-sharing/shares',
  );
  expect(new Headers(fetcher.calls[0].init?.headers).get('authorization')).toBe(
    'Bearer secret-profile-token',
  );
});

test('DeltaSharingSession discovers schemas, tables, table version, and metadata', async () => {
  const fetcher = scriptedFetch([
    jsonResponse({ items: [{ name: 'sales' }] }),
    jsonResponse({ items: [{ name: 'orders', share: 'retail_share', schema: 'sales' }] }),
    textResponse('', { 'delta-table-version': '42' }),
    ndjsonResponse([
      { protocol: { minReaderVersion: 1 } },
      {
        metaData: {
          id: 'table-id',
          name: 'orders',
          schemaString: '{"type":"struct","fields":[]}',
        },
      },
    ]),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  await expect(session.listSchemas('retail_share')).resolves.toEqual({
    items: [{ name: 'sales' }],
  });
  await expect(session.listTables('retail_share', 'sales')).resolves.toEqual({
    items: [{ name: 'orders', share: 'retail_share', schema: 'sales' }],
  });
  await expect(
    session.getTableVersion({ share: 'retail_share', schema: 'sales', table: 'orders' }),
  ).resolves.toBe(42);
  await expect(
    session.getTableMetadata({ share: 'retail_share', schema: 'sales', table: 'orders' }),
  ).resolves.toMatchObject({
    version: undefined,
    protocol: { minReaderVersion: 1 },
    metadata: { id: 'table-id', name: 'orders' },
  });

  expect(fetcher.calls.map((call) => String(call.input))).toEqual([
    'https://sharing.example.test/delta-sharing/shares/retail_share/schemas',
    'https://sharing.example.test/delta-sharing/shares/retail_share/schemas/sales/tables',
    'https://sharing.example.test/delta-sharing/shares/retail_share/schemas/sales/tables/orders/version',
    'https://sharing.example.test/delta-sharing/shares/retail_share/schemas/sales/tables/orders/metadata',
  ]);
});

test('DeltaSharingSession resolves URL-mode parquet responses into browser descriptors', async () => {
  const fetcher = scriptedFetch([
    ndjsonResponse(deltaSharingQueryActions(), { 'delta-table-version': '12' }),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  const plan = await session.resolveTable(
    { share: 'retail_share', schema: 'sales', table: 'orders' },
    {
      version: 12,
      limitHint: 100,
      responseFormat: 'auto',
    },
  );

  expect(plan).toEqual({
    kind: 'delta_sharing_snapshot_descriptor',
    table: { share: 'retail_share', schema: 'sales', table: 'orders' },
    endpoint: 'https://sharing.example.test/delta-sharing',
    resolvedVersion: 12,
    responseFormat: 'parquet',
    descriptor: {
      table_uri:
        'delta-sharing://sharing.example.test/retail_share/sales/orders?endpoint=%2Fdelta-sharing',
      snapshot_version: 12,
      partition_column_types: { region: 'string' },
      browser_compatibility: { capabilities: { signed_url_access: 'supported' } },
      required_capabilities: { capabilities: { signed_url_access: 'supported' } },
      active_files: [
        {
          path: 'part-000.parquet',
          url: 'https://storage.example.test/retail/orders/part-000.parquet?X-Amz-Signature=abc',
          size_bytes: 128,
          partition_values: { region: 'west' },
          stats: '{"numRecords":1}',
        },
      ],
    },
    expiresAtEpochMs: Date.parse('2026-05-17T20:00:00Z'),
    warnings: [],
  } satisfies DeltaSharingReadPlan);

  expect(String(fetcher.calls[0].input)).toBe(
    'https://sharing.example.test/delta-sharing/shares/retail_share/schemas/sales/tables/orders/query',
  );
  expect(fetcher.calls[0].init?.method).toBe('POST');
  expect(fetcher.calls[0].init?.body).toBe(JSON.stringify({ version: 12, limitHint: 100 }));
  expect(new Headers(fetcher.calls[0].init?.headers).get('authorization')).toBe(
    'Bearer secret-profile-token',
  );
});

test('DeltaSharingSession carries partition column types from table metadata into descriptors', async () => {
  const fetcher = scriptedFetch([
    ndjsonResponse(
      [
        { protocol: { minReaderVersion: 1 } },
        {
          metaData: {
            partitionColumns: ['region', 'year', 'active', 'payload'],
            schemaString: JSON.stringify({
              type: 'struct',
              fields: [
                { name: 'region', type: 'string', nullable: true, metadata: {} },
                { name: 'year', type: 'long', nullable: true, metadata: {} },
                { name: 'active', type: 'boolean', nullable: true, metadata: {} },
                {
                  name: 'payload',
                  type: { type: 'struct', fields: [] },
                  nullable: true,
                  metadata: {},
                },
              ],
            }),
          },
        },
        {
          file: {
            id: 'part-000.parquet',
            url: 'https://storage.example.test/retail/orders/part-000.parquet',
            size: 128,
            partitionValues: {
              active: 'true',
              payload: 'unsupported',
              region: 'west',
              year: '2026',
            },
          },
        },
      ],
      { 'delta-table-version': '12' },
    ),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  const plan = await session.resolveTable({
    share: 'retail_share',
    schema: 'sales',
    table: 'orders',
  });

  expect(plan.descriptor.partition_column_types).toEqual({
    active: 'boolean',
    payload: 'unsupported',
    region: 'string',
    year: 'int64',
  });
});

test('openDeltaShare resolves a shared table and sends only the descriptor to the worker', async () => {
  const worker = new FakeWorker();
  const sharingFetch = scriptedFetch([
    ndjsonResponse(deltaSharingQueryActions(), { 'delta-table-version': '12' }),
  ]);
  const session = await createDeltaSharingClient({ fetch: sharingFetch.fetch }).connect({
    source: 'json',
    value: JSON.stringify({
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
      shareCredentialsVersion: 1,
    }),
  });
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const openedPromise = client.openDeltaShare('orders', {
    session,
    table: { share: 'retail_share', schema: 'sales', table: 'orders' },
    responseFormat: 'auto',
    requestId: 'req-delta-share-open',
  });

  await expect.poll(() => worker.commands.length).toBe(1);

  expect(worker.commands[0]).toMatchObject({
    open_delta_table: {
      request_id: 'req-delta-share-open',
      name: 'orders',
      snapshot: {
        snapshot_version: 12,
        active_files: [
          {
            path: 'part-000.parquet',
            url: 'https://storage.example.test/retail/orders/part-000.parquet?X-Amz-Signature=abc',
          },
        ],
      },
    },
  });
  const workerPayload = JSON.stringify(worker.commands[0]);
  expect(workerPayload).not.toContain('secret-profile-token');
  expect(workerPayload).not.toContain('shareCredentialsVersion');
  expect(workerPayload).not.toContain('Authorization');

  worker.emitMessage({ opened: { request_id: 'req-delta-share-open', name: 'orders' } });

  await expect(openedPromise).resolves.toMatchObject({
    request_id: 'req-delta-share-open',
    name: 'orders',
    deltaSharing: {
      resolvedVersion: 12,
      responseFormat: 'parquet',
      table: { share: 'retail_share', schema: 'sales', table: 'orders' },
    },
  });
});

test('DeltaSharingSession rejects directory access without a resolver', async () => {
  const fetcher = scriptedFetch([
    ndjsonResponse(
      [
        { protocol: { minReaderVersion: 1, accessModes: ['dir'] } },
        { file: { id: 'part-000.parquet', size: 128, partitionValues: {} } },
      ],
      { 'delta-table-version': '12' },
    ),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  await expect(
    session.resolveTable({ share: 'retail_share', schema: 'sales', table: 'orders' }),
  ).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'directory_access_requires_resolver',
  });
});

test('DeltaSharingClient rejects expired bearer profiles before making network requests', async () => {
  const fetcher = scriptedFetch([jsonResponse({ items: [] })]);

  await expect(
    createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
      source: 'json',
      value: {
        endpoint: 'https://sharing.example.test/delta-sharing',
        bearerToken: 'expired-profile-token',
        expirationTime: '2026-01-01T00:00:00Z',
      },
    }),
  ).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'auth_expired',
  });
  expect(fetcher.calls).toHaveLength(0);
});

test('DeltaSharingSession rejects responseformat=delta before issuing unsupported reads', async () => {
  const fetcher = scriptedFetch([ndjsonResponse(deltaSharingQueryActions())]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  await expect(
    session.resolveTable(
      { share: 'retail_share', schema: 'sales', table: 'orders' },
      { responseFormat: 'delta' },
    ),
  ).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'unsupported_response_format',
  });
  expect(fetcher.calls).toHaveLength(0);
});

test('DeltaSharingSession maps not-found responses to share/schema/table errors by endpoint', async () => {
  const fetcher = scriptedFetch([
    textResponse('missing share', {}, 404),
    textResponse('missing schema', {}, 404),
    textResponse('missing table', {}, 404),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  await expect(session.listSchemas('missing_share')).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'share_not_found',
  });
  await expect(session.listTables('retail_share', 'missing_schema')).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'schema_not_found',
  });
  await expect(
    session.getTableVersion({ share: 'retail_share', schema: 'sales', table: 'missing_table' }),
  ).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'table_not_found',
  });
});

test('DeltaSharing errors redact bearer tokens and signed URL query strings', async () => {
  const fetcher = scriptedFetch([
    textResponse(
      'failed Authorization: Bearer secret-profile-token https://storage.example.test/file.parquet?X-Amz-Signature=abc',
      {},
      500,
    ),
  ]);
  const session = await createDeltaSharingClient({ fetch: fetcher.fetch }).connect({
    source: 'json',
    value: {
      endpoint: 'https://sharing.example.test/delta-sharing',
      bearerToken: 'secret-profile-token',
    },
  });

  const result = session.listShares();

  await expect(result).rejects.toMatchObject({
    name: 'DeltaSharingError',
    code: 'server_error',
  });
  await expect(result).rejects.not.toThrow('secret-profile-token');
  await expect(result).rejects.not.toThrow('X-Amz-Signature=abc');
});

test('openDeltaLocation resolves through an injected resolver and opens only a browser descriptor', async () => {
  const worker = new FakeWorker();
  const requests: DeltaLocationResolveRequest[] = [];
  const resolverResponse = deltaLocationResponse({
    provider: 's3',
    table_uri: 's3://axon-fixtures/partitioned-table',
    requested_access_mode: 'auto',
    actual_access_mode: 'proxy',
  });
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const openedPromise = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 's3',
    tableUri: 's3://axon-fixtures/partitioned-table',
    credentialProfile: {
      id: 'prod-readonly',
      display_name: 'Production readonly',
    },
    resolveDeltaLocation: async (request) => {
      requests.push(request);
      return resolverResponse;
    },
    requestId: 'req-location-open',
  });

  await expect.poll(() => worker.commands.length).toBe(1);

  expect(requests).toEqual([
    {
      provider: 's3',
      table_uri: 's3://axon-fixtures/partitioned-table',
      credential_profile: {
        id: 'prod-readonly',
        display_name: 'Production readonly',
      },
      requested_access_mode: 'auto',
    },
  ]);
  expect(worker.commands[0]).toEqual({
    open_delta_table: {
      request_id: 'req-location-open',
      name: 'events',
      snapshot: resolverResponse.descriptor,
    },
  });

  const workerPayload = JSON.stringify(worker.commands[0]);
  expect(workerPayload).not.toContain('credential_profile');
  expect(workerPayload).not.toContain('prod-readonly');
  expect(workerPayload).not.toContain('s3://axon-fixtures/partitioned-table');

  worker.emitMessage({ opened: { request_id: 'req-location-open', name: 'events' } });

  await expect(openedPromise).resolves.toEqual({
    request_id: 'req-location-open',
    name: 'events',
    location: {
      provider: 's3',
      table_uri: 's3://axon-fixtures/partitioned-table',
      requested_snapshot_version: 7,
      resolved_snapshot_version: 7,
      requested_access_mode: 'auto',
      actual_access_mode: 'proxy',
      expires_at_epoch_ms: 4_102_444_800_000,
      warnings: [],
      refresh: {
        refresh_after_epoch_ms: 4_102_444_740_000,
        same_snapshot_required: true,
      },
    },
  });
});

test('openDeltaLocation opens trusted browser descriptors without a resolver', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const openedPromise = client.openDeltaLocation('events', {
    source: { kind: 'trusted_descriptor', descriptor: snapshot },
    requestId: 'req-browser-location-open',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  expect(worker.commands[0]).toEqual({
    open_delta_table: {
      request_id: 'req-browser-location-open',
      name: 'events',
      snapshot,
    },
  });

  worker.emitMessage({ opened: { request_id: 'req-browser-location-open', name: 'events' } });

  await expect(openedPromise).resolves.toEqual({
    request_id: 'req-browser-location-open',
    name: 'events',
    location: {
      source_kind: 'trusted_descriptor',
      resolution_mode: 'browser_local',
      table_uri: snapshot.table_uri,
      resolved_snapshot_version: snapshot.snapshot_version,
    },
  });
});

test('openDeltaLocation materializes manifest and file-action sources into descriptor handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const httpDescriptor: BrowserHttpSnapshotDescriptor = {
    ...snapshot,
    table_uri: 'https://data.example.test/events',
    snapshot_version: 9,
    active_files: snapshot.active_files.map((file) => ({
      ...file,
      url: 'https://data.example.test/events/part-000.parquet',
    })),
  };
  const brokeredDescriptor: BrowserHttpSnapshotDescriptor = {
    ...snapshot,
    table_uri: 'https://broker.example.test/materialized/events',
    snapshot_version: 10,
    active_files: snapshot.active_files.map((file) => ({
      ...file,
      url: 'https://broker.example.test/files/part-000.parquet',
    })),
  };

  fetchGlobal.fetch = (async (input: RequestInfo | URL) => {
    if (String(input).includes('/brokered-manifest.json')) {
      return new Response(JSON.stringify({ descriptor: brokeredDescriptor }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(JSON.stringify({ descriptor: httpDescriptor }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  }) as typeof fetch;

  try {
    const httpOpen = client.openDeltaLocation('http_events', {
      source: {
        kind: 'http_manifest',
        manifestUrl: 'https://manifest.example.test/http-manifest.json',
      },
      requestId: 'req-http-manifest',
    });
    await expect.poll(() => worker.commands.length).toBe(1);
    expect(worker.commands[0]).toEqual({
      open_delta_table: {
        request_id: 'req-http-manifest',
        name: 'http_events',
        snapshot: httpDescriptor,
      },
    });
    worker.emitMessage({ opened: { request_id: 'req-http-manifest', name: 'http_events' } });
    await expect(httpOpen).resolves.toMatchObject({
      location: {
        source_kind: 'http_manifest',
        resolution_mode: 'browser_local',
        table_uri: httpDescriptor.table_uri,
        resolved_snapshot_version: httpDescriptor.snapshot_version,
      },
    });

    const brokeredOpen = client.openDeltaLocation('brokered_events', {
      source: {
        kind: 'brokered_manifest',
        manifestUrl: 'https://manifest.example.test/brokered-manifest.json',
        grantId: 'grant-browser-readable',
      },
      resolutionMode: 'brokered_access',
      requestId: 'req-brokered-manifest',
    });
    await expect.poll(() => worker.commands.length).toBe(2);
    expect(worker.commands[1]).toEqual({
      open_delta_table: {
        request_id: 'req-brokered-manifest',
        name: 'brokered_events',
        snapshot: brokeredDescriptor,
      },
    });
    expect(JSON.stringify(worker.commands[1])).not.toContain('grant-browser-readable');
    worker.emitMessage({
      opened: { request_id: 'req-brokered-manifest', name: 'brokered_events' },
    });
    await expect(brokeredOpen).resolves.toMatchObject({
      location: {
        source_kind: 'brokered_manifest',
        resolution_mode: 'brokered_access',
        table_uri: brokeredDescriptor.table_uri,
        resolved_snapshot_version: brokeredDescriptor.snapshot_version,
      },
    });

    const sharingFiles = snapshot.active_files.map((file) => ({
      ...file,
      url: 'https://sharing.example.test/files/part-000.parquet?sig=short-lived',
    }));
    const sharingOpen = client.openDeltaLocation('shared_events', {
      source: { kind: 'delta_sharing_url_files', files: sharingFiles },
      requestId: 'req-delta-sharing-files',
    });
    await expect.poll(() => worker.commands.length).toBe(3);
    expect(worker.commands[2]).toEqual({
      open_delta_table: {
        request_id: 'req-delta-sharing-files',
        name: 'shared_events',
        snapshot: {
          table_uri: 'delta-sharing://browser-source',
          snapshot_version: 0,
          partition_column_types: {},
          browser_compatibility: {
            capabilities: { signed_url_access: 'supported' },
          },
          required_capabilities: {
            capabilities: { signed_url_access: 'supported' },
          },
          active_files: sharingFiles,
        },
      },
    });
    worker.emitMessage({
      opened: { request_id: 'req-delta-sharing-files', name: 'shared_events' },
    });
    await expect(sharingOpen).resolves.toMatchObject({
      location: {
        source_kind: 'delta_sharing_url_files',
        resolution_mode: 'browser_local',
        table_uri: 'delta-sharing://browser-source',
        resolved_snapshot_version: 0,
      },
    });
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('openDeltaLocation resolves table-root sources through a browser snapshot resolver', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const resolvedDescriptor: BrowserHttpSnapshotDescriptor = {
    ...snapshot,
    table_uri: 'https://data.example.test/events',
    snapshot_version: 12,
    active_files: snapshot.active_files.map((file) => ({
      ...file,
      url: 'https://data.example.test/events/part-000.parquet',
    })),
  };
  const requests: unknown[] = [];

  await expect(
    client.openDeltaLocation('events', {
      source: { kind: 'cors_http_table', tableRootUrl: 'https://data.example.test/events' },
    }),
  ).rejects.toThrow('requires list, head, get, and rangeGet capabilities');

  await expect(
    client.openDeltaLocation('events', {
      source: {
        kind: 'cors_http_table',
        tableRootUrl: 'https://data.example.test/events',
        capabilities: { list: true, head: true, rangeGet: true } as never,
      },
    }),
  ).rejects.toThrow('requires list, head, get, and rangeGet capabilities');

  await expect(
    client.openDeltaLocation('events', {
      source: {
        kind: 'cors_http_table',
        tableRootUrl: 'https://data.example.test/events',
        capabilities: { list: true, head: true, get: true, rangeGet: true },
      },
    }),
  ).rejects.toThrow('requires resolveBrowserSnapshot');

  expect(worker.commands).toHaveLength(0);

  const openedPromise = client.openDeltaLocation('events', {
    source: {
      kind: 'cors_http_table',
      tableRootUrl: 'https://data.example.test/events',
      capabilities: { list: true, head: true, get: true, rangeGet: true },
    },
    snapshotVersion: 12,
    resolveBrowserSnapshot: async (source, options) => {
      requests.push({ source, options });
      return resolvedDescriptor;
    },
    requestId: 'req-browser-reconstruct',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  expect(requests).toEqual([
    {
      source: {
        kind: 'cors_http_table',
        tableRootUrl: 'https://data.example.test/events',
        capabilities: { list: true, head: true, get: true, rangeGet: true },
      },
      options: { snapshotVersion: 12 },
    },
  ]);
  expect(worker.commands[0]).toEqual({
    open_delta_table: {
      request_id: 'req-browser-reconstruct',
      name: 'events',
      snapshot: resolvedDescriptor,
    },
  });

  worker.emitMessage({ opened: { request_id: 'req-browser-reconstruct', name: 'events' } });
  await expect(openedPromise).resolves.toMatchObject({
    location: {
      source_kind: 'cors_http_table',
      resolution_mode: 'browser_local',
      table_uri: resolvedDescriptor.table_uri,
      resolved_snapshot_version: resolvedDescriptor.snapshot_version,
    },
  });
});

test('openDeltaLocation resolves brokered object grants through a browser snapshot resolver', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const resolvedDescriptor: BrowserHttpSnapshotDescriptor = {
    ...snapshot,
    table_uri: 'https://storage.example.test/tables/events',
    snapshot_version: 13,
    active_files: snapshot.active_files.map((file) => ({
      ...file,
      url: 'https://storage.example.test/tables/events/part-000.parquet',
    })),
  };
  const grant = {
    grantId: 'grant-browser-readable',
    tableRootUrl: 'https://storage.example.test/tables/events',
    expiresAtEpochMs: 4_102_444_800_000,
    capabilities: {
      list: true,
      head: true,
      get: true,
      rangeGet: true,
      batchSign: true,
      proxyRange: false,
    },
  };

  const openedPromise = client.openDeltaLocation('events', {
    source: { kind: 'brokered_object_grants', grant },
    resolutionMode: 'brokered_access',
    resolveBrowserSnapshot: async (source, options) => {
      expect(source).toEqual({ kind: 'brokered_object_grants', grant });
      expect(options).toEqual({});
      return resolvedDescriptor;
    },
    requestId: 'req-brokered-object-grant',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  expect(worker.commands[0]).toEqual({
    open_delta_table: {
      request_id: 'req-brokered-object-grant',
      name: 'events',
      snapshot: resolvedDescriptor,
    },
  });

  worker.emitMessage({ opened: { request_id: 'req-brokered-object-grant', name: 'events' } });
  await expect(openedPromise).resolves.toMatchObject({
    location: {
      source_kind: 'brokered_object_grants',
      resolution_mode: 'brokered_access',
      table_uri: resolvedDescriptor.table_uri,
      resolved_snapshot_version: resolvedDescriptor.snapshot_version,
      expires_at_epoch_ms: 4_102_444_800_000,
    },
  });
});

test('openDeltaLocation returns resolver metadata without descriptor URLs', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const openedPromise = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    resolveDeltaLocation: async () =>
      deltaLocationResponse({
        correlation_id: 'corr-location-open',
      }),
    requestId: 'req-location-open-metadata',
  });

  await expect.poll(() => worker.commands.length).toBe(1);
  worker.emitMessage({ opened: { request_id: 'req-location-open-metadata', name: 'events' } });

  const opened = await openedPromise;
  expect(opened.location).toEqual({
    provider: 'gcs',
    table_uri: 'gs://axon-fixtures/partitioned-table',
    requested_snapshot_version: 7,
    resolved_snapshot_version: 7,
    requested_access_mode: 'auto',
    actual_access_mode: 'signed_url',
    expires_at_epoch_ms: 4_102_444_800_000,
    correlation_id: 'corr-location-open',
    warnings: [],
    refresh: {
      refresh_after_epoch_ms: 4_102_444_740_000,
      same_snapshot_required: true,
    },
  });
  expect(JSON.stringify(opened.location)).not.toContain('https://example.invalid/part-000.parquet');
});

test('openDeltaLocation rejects resolver responses that resolve a different snapshot version', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const result = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    snapshotVersion: 7,
    resolveDeltaLocation: async () =>
      deltaLocationResponse({
        descriptor: {
          ...snapshot,
          table_uri: 'axon-resolved://events/snapshot/8',
          snapshot_version: 8,
          active_files: snapshot.active_files.map((file) => ({ ...file })),
        },
        requested_snapshot_version: 7,
        resolved_snapshot_version: 8,
      }),
    requestId: 'req-location-version-mismatch',
  });
  void result.catch(() => undefined);

  await settleUnexpectedOpen(worker, 'req-location-version-mismatch', 'events');

  await expect(result).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'snapshot_version_not_found',
  } satisfies Partial<DeltaLocationResolverError>);
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects resolver responses that ignore a required access mode', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const result = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    requestedAccessMode: 'proxy',
    resolveDeltaLocation: async () =>
      deltaLocationResponse({
        requested_access_mode: 'proxy',
        actual_access_mode: 'signed_url',
      }),
    requestId: 'req-location-access-mismatch',
  });
  void result.catch(() => undefined);

  await settleUnexpectedOpen(worker, 'req-location-access-mismatch', 'events');

  await expect(result).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'access_mode_not_supported',
  } satisfies Partial<DeltaLocationResolverError>);
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation can call an HTTP resolver with the typed request body', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return new Response(JSON.stringify(deltaLocationResponse()), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  }) as typeof fetch;

  try {
    const openedPromise = client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 'gcs',
      tableUri: 'gs://axon-fixtures/partitioned-table',
      credentialProfile: { id: 'prod-readonly' },
      resolverUrl: 'https://resolver.example.test/delta/location',
      snapshotVersion: 7,
      requestId: 'req-http-location-open',
    });

    await expect.poll(() => worker.commands.length).toBe(1);

    expect(calls).toHaveLength(1);
    expect(String(calls[0].input)).toBe('https://resolver.example.test/delta/location');
    expect(calls[0].init?.method).toBe('POST');
    expect(calls[0].init?.body).toBe(
      JSON.stringify({
        provider: 'gcs',
        table_uri: 'gs://axon-fixtures/partitioned-table',
        credential_profile: { id: 'prod-readonly' },
        requested_access_mode: 'auto',
        snapshot_version: 7,
      }),
    );

    worker.emitMessage({ opened: { request_id: 'req-http-location-open', name: 'events' } });
    await expect(openedPromise).resolves.toMatchObject({ name: 'events' });
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis descriptor resolver example opens through the SDK without exposing credentials to the worker', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
  const resolverResponse = deltaLocationResponse({
    provider: 'gcs',
    table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
    requested_snapshot_version: undefined,
    resolved_snapshot_version: 42,
    requested_access_mode: 'auto',
    actual_access_mode: 'signed_url',
    expires_at_epoch_ms: 4_102_444_800_000,
    correlation_id: 'daxis-req-01JABCDEF',
    descriptor: {
      ...snapshot,
      table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
      snapshot_version: 42,
      active_files: snapshot.active_files.map((file) => ({
        ...file,
        path: 'order_date=2026-05-30/part-00000.parquet',
        url: 'https://storage.googleapis.com/daxis-prod-lakehouse/sales/orders/order_date=2026-05-30/part-00000.parquet?X-Goog-Signature=redacted',
        partition_values: { order_date: '2026-05-30' },
      })),
    },
  });

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return jsonResponse(resolverResponse);
  }) as typeof fetch;

  try {
    const openedPromise = openDaxisResolvedDeltaTable(client, {
      tableName: 'orders',
      tableUri: 'gs://daxis-prod-lakehouse/sales/orders',
      credentialProfileId: 'daxis-workspace-prod-read',
      credentialProfileDisplayName: 'Daxis workspace production read grant',
      resolverUrl: '/v1/query/delta/snapshot-descriptor',
      requestId: 'req-daxis-open-orders',
    });

    await expect.poll(() => worker.commands.length).toBe(1);

    expect(String(calls[0].input)).toBe('/v1/query/delta/snapshot-descriptor');
    expect(calls[0].init?.method).toBe('POST');
    expect(new Headers(calls[0].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-open-orders',
    );
    expect(calls[0].init?.body).toBe(
      JSON.stringify({
        provider: 'gcs',
        table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
        credential_profile: {
          id: 'daxis-workspace-prod-read',
          display_name: 'Daxis workspace production read grant',
        },
        requested_access_mode: 'auto',
      }),
    );
    expect(worker.commands[0]).toEqual({
      open_delta_table: {
        request_id: 'req-daxis-open-orders',
        name: 'orders',
        snapshot: resolverResponse.descriptor,
      },
    });
    const workerPayload = JSON.stringify(worker.commands[0]);
    expect(workerPayload).not.toContain('credential_profile');
    expect(workerPayload).not.toContain('daxis-workspace-prod-read');

    worker.emitMessage({ opened: { request_id: 'req-daxis-open-orders', name: 'orders' } });
    await expect(openedPromise).resolves.toMatchObject({
      name: 'orders',
      location: {
        provider: 'gcs',
        table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
        resolved_snapshot_version: 42,
        actual_access_mode: 'signed_url',
        correlation_id: 'daxis-req-01JABCDEF',
      },
    });
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis descriptor resolver example preserves structured Daxis resolver errors', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return jsonResponse(
      {
        code: 'storage_auth_failed',
        message: 'Daxis storage broker could not mint browser-safe URLs for this table',
        correlation_id: 'daxis-req-01JERROR',
      },
      {},
      403,
    );
  }) as typeof fetch;

  try {
    await expect(
      openDaxisResolvedDeltaTable(client, {
        tableName: 'orders',
        tableUri: 'gs://daxis-prod-lakehouse/sales/orders',
        credentialProfileId: 'daxis-workspace-prod-read',
        resolverUrl: '/v1/query/delta/snapshot-descriptor',
        requestId: 'req-daxis-open-denied',
      }),
    ).rejects.toMatchObject({
      name: 'DeltaLocationResolverError',
      code: 'storage_auth_failed',
      message: 'Daxis storage broker could not mint browser-safe URLs for this table',
      correlationId: 'daxis-req-01JERROR',
    } satisfies Partial<DeltaLocationResolverError>);

    expect(calls).toHaveLength(1);
    expect(new Headers(calls[0].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-open-denied',
    );
    expect(worker.commands).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis read-access-plan example opens brokered Delta plans without Daxis session data in the worker', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return jsonResponse(brokeredDeltaPlan());
  }) as typeof fetch;

  try {
    const openedPromise = openDaxisReadAccessPlanTable(client, {
      tableName: 'orders',
      fullName: 'main.sales.orders',
      session: { id: 'daxis-browser-session-123', displayName: 'Daxis browser session' },
      readAccessPlanUrl: '/v1/catalog/read-access-plan',
      requestId: 'req-daxis-read-plan',
      brokeredDelta: {
        resolveSnapshot: async (plan) => {
          expect(plan.grantId).toBe('grant-456');
          return brokeredSnapshot();
        },
        batchSign: async (plan, paths) => {
          expect(plan.grantId).toBe('grant-456');
          expect(paths).toEqual(['part-000.parquet']);
          return {
            signedUrls: paths.map((path) => ({
              path,
              url: `https://storage.example.test/${path}?X-Amz-Signature=abc`,
              expiresAtEpochMs: 4_102_444_800_000,
            })),
          };
        },
      },
    });

    await expect.poll(() => worker.commands.length).toBe(1);

    expect(calls).toHaveLength(1);
    expect(String(calls[0].input)).toBe('/v1/catalog/read-access-plan');
    expect(calls[0].init?.method).toBe('POST');
    expect(new Headers(calls[0].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-read-plan',
    );
    expect(calls[0].init?.body).toBe(
      JSON.stringify({
        fullName: 'main.sales.orders',
        session: {
          id: 'daxis-browser-session-123',
          displayName: 'Daxis browser session',
        },
      }),
    );
    expect(worker.commands[0]).toMatchObject({
      open_delta_table: {
        request_id: 'req-daxis-read-plan',
        name: 'orders',
        snapshot: {
          table_uri: 's3://prod-bucket/tables/orders',
          active_files: [
            {
              path: 'part-000.parquet',
              url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
            },
          ],
        },
      },
    });
    const workerPayload = JSON.stringify(worker.commands[0]);
    expect(workerPayload).not.toContain('grant-456');
    expect(workerPayload).not.toContain('daxis-browser-session-123');

    worker.emitMessage({ opened: { request_id: 'req-daxis-read-plan', name: 'orders' } });
    await expect(openedPromise).resolves.toMatchObject({
      status: 'opened',
      planType: 'brokered_delta',
      tableId: 'tbl-123',
      fullName: 'main.sales.orders',
    });
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis read-access-plan example preserves SQL fallback without worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;

  fetchGlobal.fetch = (async () =>
    jsonResponse({
      plan_type: 'sql_fallback_required',
      tableId: 'tbl-governed',
      fullName: 'main.secure.payments',
      reason: 'row_filter',
      message: 'Daxis policy requires server execution because this table has a row filter.',
      statementEndpoint: 'https://daxis.example.test/v1/query/sql/statements',
      warehouseRequired: true,
    })) as typeof fetch;

  try {
    await expect(
      openDaxisReadAccessPlanTable(client, {
        tableName: 'payments',
        fullName: 'main.secure.payments',
        readAccessPlanUrl: '/v1/catalog/read-access-plan',
      }),
    ).resolves.toMatchObject({
      status: 'sql_fallback_required',
      reason: 'row_filter',
      statementEndpoint: 'https://daxis.example.test/v1/query/sql/statements',
    });
    expect(worker.commands).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis read-access-plan example preserves structured Daxis plan errors without worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return jsonResponse(
      {
        code: 'broker_unavailable',
        message: 'Daxis read-access planning is temporarily unavailable',
        correlation_id: 'daxis-read-01JERROR',
      },
      {},
      503,
    );
  }) as typeof fetch;

  try {
    await expect(
      openDaxisReadAccessPlanTable(client, {
        tableName: 'orders',
        fullName: 'main.sales.orders',
        readAccessPlanUrl: '/v1/catalog/read-access-plan',
        requestId: 'req-daxis-read-plan-error',
      }),
    ).rejects.toMatchObject({
      name: 'DaxisReadAccessPlanError',
      code: 'broker_unavailable',
      status: 503,
      message: 'Daxis read-access planning is temporarily unavailable',
      correlationId: 'daxis-read-01JERROR',
    } satisfies Partial<DaxisReadAccessPlanError>);

    expect(calls).toHaveLength(1);
    expect(new Headers(calls[0].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-read-plan-error',
    );
    expect(worker.commands).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant adapter opens brokered Delta plans through grant-scoped batch signing', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    if (String(input) === '/v1/catalog/read-access-plan') {
      return jsonResponse(brokeredDeltaPlan());
    }
    if (String(input) === '/object-grants/grant-456/batch-sign') {
      return jsonResponse({
        signedUrls: [
          {
            path: 'part-000.parquet',
            url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
            expiresAtEpochMs: 4_102_444_800_000,
          },
        ],
      });
    }
    throw new Error(`unexpected Daxis fetch call: ${String(input)}`);
  }) as typeof fetch;

  try {
    const adapter = createDaxisObjectGrantAdapter({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-grant-open',
      resolveSnapshot: async (plan) => {
        expect(plan.grantId).toBe('grant-456');
        return brokeredSnapshot();
      },
    });

    const openedPromise = openDaxisReadAccessPlanTable(client, {
      tableName: 'orders',
      fullName: 'main.sales.orders',
      readAccessPlanUrl: '/v1/catalog/read-access-plan',
      requestId: 'req-daxis-grant-open',
      brokeredDelta: adapter,
    });

    await expect.poll(() => worker.commands.length).toBe(1);

    expect(calls).toHaveLength(2);
    expect(String(calls[1].input)).toBe('/object-grants/grant-456/batch-sign');
    expect(calls[1].init?.method).toBe('POST');
    expect(new Headers(calls[1].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-grant-open',
    );
    expect(calls[1].init?.body).toBe(JSON.stringify({ paths: ['part-000.parquet'] }));

    expect(worker.commands[0]).toMatchObject({
      open_delta_table: {
        request_id: 'req-daxis-grant-open',
        name: 'orders',
        snapshot: {
          active_files: [
            {
              path: 'part-000.parquet',
              url: 'https://storage.example.test/part-000.parquet?X-Amz-Signature=abc',
            },
          ],
        },
      },
    });
    const workerPayload = JSON.stringify(worker.commands[0]);
    expect(workerPayload).not.toContain('grant-456');
    expect(workerPayload).not.toContain('object-grants');

    worker.emitMessage({ opened: { request_id: 'req-daxis-grant-open', name: 'orders' } });
    await expect(openedPromise).resolves.toMatchObject({
      status: 'opened',
      planType: 'brokered_delta',
      tableId: 'tbl-123',
      fullName: 'main.sales.orders',
    });
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant adapter preserves structured grant errors', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    return jsonResponse(
      {
        code: 'grant_expired',
        message: 'Daxis object grant expired before batch signing completed',
        correlation_id: 'daxis-grant-01JERROR',
      },
      {},
      401,
    );
  }) as typeof fetch;

  try {
    const adapter = createDaxisObjectGrantAdapter({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-grant-error',
      resolveSnapshot: async () => brokeredSnapshot(),
    });

    await expect(
      adapter.batchSign(brokeredDeltaPlan(), ['part-000.parquet']),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'grant_expired',
      status: 401,
      message: 'Daxis object grant expired before batch signing completed',
      correlationId: 'daxis-grant-01JERROR',
    } satisfies Partial<DaxisObjectGrantError>);

    expect(calls).toHaveLength(1);
    expect(String(calls[0].input)).toBe('/object-grants/grant-456/batch-sign');
    expect(new Headers(calls[0].init?.headers).get('x-daxis-request-id')).toBe(
      'req-daxis-grant-error',
    );
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client calls grant-scoped list, head, and range routes', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
  const rangeBytes = new Uint8Array(Array.from({ length: 16 }, (_, index) => index + 1));

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    const url = String(input);
    if (url === '/object-grants/grant-456/list') {
      return jsonResponse({
        objects: [{ path: 'part-000.parquet', sizeBytes: 512, etag: '"part-000-v7"' }],
      });
    }
    if (url === '/object-grants/grant-456/head') {
      return jsonResponse({ path: 'part-000.parquet', sizeBytes: 512, etag: '"part-000-v7"' });
    }
    if (url === '/object-grants/grant-456/range?path=part-000.parquet&start=0&end=16') {
      return new Response(rangeBytes, {
        status: 206,
        headers: { 'content-type': 'application/octet-stream' },
      });
    }
    throw new Error(`unexpected Daxis object grant call: ${url}`);
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-object-grant-routes',
    });
    const plan = brokeredDeltaPlan();

    await expect(grantClient.list(plan, 'part-')).resolves.toEqual({
      objects: [{ path: 'part-000.parquet', sizeBytes: 512, etag: '"part-000-v7"' }],
    });
    await expect(grantClient.head(plan, 'part-000.parquet')).resolves.toEqual({
      path: 'part-000.parquet',
      sizeBytes: 512,
      etag: '"part-000-v7"',
    });
    await expect(
      grantClient.range(plan, { path: 'part-000.parquet', start: 0, end: 16 }),
    ).resolves.toEqual(rangeBytes);

    expect(calls).toHaveLength(3);
    expect(calls.map((call) => String(call.input))).toEqual([
      '/object-grants/grant-456/list',
      '/object-grants/grant-456/head',
      '/object-grants/grant-456/range?path=part-000.parquet&start=0&end=16',
    ]);
    expect(calls[0].init?.method).toBe('POST');
    expect(calls[0].init?.body).toBe(JSON.stringify({ prefix: 'part-' }));
    expect(calls[1].init?.method).toBe('POST');
    expect(calls[1].init?.body).toBe(JSON.stringify({ path: 'part-000.parquet' }));
    expect(calls[2].init?.method).toBe('GET');
    for (const call of calls) {
      expect(new Headers(call.init?.headers).get('x-daxis-request-id')).toBe(
        'req-daxis-object-grant-routes',
      );
    }
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client preserves structured range errors', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;

  fetchGlobal.fetch = (async () =>
    jsonResponse(
      {
        code: 'range_not_authorized',
        message: 'Daxis object grant does not authorize this byte range',
        correlation_id: 'daxis-range-01JERROR',
      },
      {},
      403,
    )) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-range-error',
    });

    await expect(
      grantClient.range(brokeredDeltaPlan(), { path: 'part-000.parquet', start: 0, end: 16 }),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'range_not_authorized',
      status: 403,
      message: 'Daxis object grant does not authorize this byte range',
      correlationId: 'daxis-range-01JERROR',
    } satisfies Partial<DaxisObjectGrantError>);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects expired grants before route calls', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    throw new Error('expired grant should not call fetch');
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-expired-grant',
    });

    await expect(
      grantClient.head({ ...brokeredDeltaPlan(), expiresAtEpochMs: 1 }, 'part-000.parquet'),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'grant_expired',
      message: 'Daxis object grant expired before the route call was issued',
    } satisfies Partial<DaxisObjectGrantError>);
    expect(calls).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects unavailable proxy-range capability before route calls', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    throw new Error('proxy range should not call fetch when the plan denies it');
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-proxy-range-denied',
    });

    await expect(
      grantClient.range(
        {
          ...brokeredDeltaPlan(),
          objectAccess: { ...brokeredDeltaPlan().objectAccess, proxyRange: false },
        },
        { path: 'part-000.parquet', start: 0, end: 16 },
      ),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'proxy_range_unavailable',
      message: 'Daxis object grant does not allow proxy range reads',
    } satisfies Partial<DaxisObjectGrantError>);
    expect(calls).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects unavailable JSON route capabilities before route calls', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    throw new Error('denied object grant capability should not call fetch');
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-route-denied',
    });
    const plan = brokeredDeltaPlan();

    await expect(
      grantClient.list({ ...plan, objectAccess: { ...plan.objectAccess, list: false } }, 'part-'),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'object_grant_list_unavailable',
      message: 'Daxis object grant does not allow list requests',
    } satisfies Partial<DaxisObjectGrantError>);

    await expect(
      grantClient.head(
        { ...plan, objectAccess: { ...plan.objectAccess, head: false } },
        'part-000.parquet',
      ),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'object_grant_head_unavailable',
      message: 'Daxis object grant does not allow head requests',
    } satisfies Partial<DaxisObjectGrantError>);

    await expect(
      grantClient.batchSign({ ...plan, objectAccess: { ...plan.objectAccess, batchSign: false } }, [
        'part-000.parquet',
      ]),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'batch_sign_unavailable',
      message: 'Daxis object grant does not allow batch signing',
    } satisfies Partial<DaxisObjectGrantError>);

    expect(calls).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects malformed successful JSON route responses', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    const url = String(input);
    if (url === '/object-grants/grant-456/list') {
      return jsonResponse({ objects: [{ path: 'part-000.parquet', sizeBytes: -1 }] });
    }
    if (url === '/object-grants/grant-456/head') {
      return jsonResponse({ path: 42, sizeBytes: 512 });
    }
    if (url === '/object-grants/grant-456/batch-sign') {
      return jsonResponse({
        signedUrls: [{ path: 'part-000.parquet', url: 'not-a-url', expiresAtEpochMs: 1 }],
      });
    }
    throw new Error(`unexpected Daxis object grant call: ${url}`);
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-malformed-grant-response',
    });
    const plan = brokeredDeltaPlan();

    await expect(grantClient.list(plan, 'part-')).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_response',
      status: 200,
    } satisfies Partial<DaxisObjectGrantError>);
    await expect(grantClient.head(plan, 'part-000.parquet')).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_response',
      status: 200,
    } satisfies Partial<DaxisObjectGrantError>);
    await expect(grantClient.batchSign(plan, ['part-000.parquet'])).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_response',
      status: 200,
    } satisfies Partial<DaxisObjectGrantError>);
    expect(calls).toHaveLength(3);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects invalid range requests before route calls', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    throw new Error('invalid range requests should not call fetch');
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-invalid-range',
    });

    await expect(
      grantClient.range(brokeredDeltaPlan(), { path: '', start: 0, end: 16 }),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_request',
      message: 'Daxis object grant range path must be a non-empty string',
    } satisfies Partial<DaxisObjectGrantError>);
    await expect(
      grantClient.range(brokeredDeltaPlan(), { path: 'part-000.parquet', start: 16, end: 16 }),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_request',
      message: 'Daxis object grant range start must be less than end',
    } satisfies Partial<DaxisObjectGrantError>);
    expect(calls).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis object-grant client rejects malformed successful range responses', async () => {
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;
  const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];

  fetchGlobal.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ input, init });
    if (calls.length === 1) {
      return new Response(new Uint8Array(Array.from({ length: 16 }, (_, index) => index)), {
        status: 206,
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response(new Uint8Array([1, 2, 3, 4]), {
      status: 206,
      headers: { 'content-type': 'application/octet-stream' },
    });
  }) as typeof fetch;

  try {
    const grantClient = createDaxisObjectGrantClient({
      objectGrantBaseUrl: '/object-grants',
      requestId: 'req-daxis-malformed-range',
    });
    const plan = brokeredDeltaPlan();

    await expect(
      grantClient.range(plan, { path: 'part-000.parquet', start: 0, end: 16 }),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_response',
      status: 206,
      message: 'Daxis object grant range response must use application/octet-stream',
    } satisfies Partial<DaxisObjectGrantError>);

    await expect(
      grantClient.range(plan, { path: 'part-000.parquet', start: 0, end: 16 }),
    ).rejects.toMatchObject({
      name: 'DaxisObjectGrantError',
      code: 'invalid_object_grant_response',
      status: 206,
      message: 'Daxis object grant range response returned 4 bytes for a 16-byte request',
    } satisfies Partial<DaxisObjectGrantError>);
    expect(calls).toHaveLength(2);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('Daxis headless query example executes an approved descriptor and returns a Daxis envelope', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const resultPromise = executeDaxisApprovedAxonRead(client, daxisApprovedAxonReadDescriptor(), {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
    workerArtifactId: 'axon-web-worker@sha256:test',
  });

  await Promise.resolve();
  expect(worker.commands[0]).toMatchObject({
    open_delta_table: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
      snapshot: {
        table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
        snapshot_version: 42,
      },
    },
  });

  worker.emitMessage({
    opened: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(worker.commands[1]).toMatchObject({
    sql: {
      request_id: 'exec-daxis-headless-saved-query',
      name: 'orders',
      query: {
        table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
        snapshot_version: 42,
        sql: 'select order_id, amount_cents from orders order by amount_cents desc limit 2',
        preferred_target: 'browser_wasm',
        options: {
          collect_metrics: true,
          result_page: {
            limit: 500,
            offset: 0,
          },
          runtime_limits: {
            max_arrow_ipc_bytes: 1048576,
            max_result_rows: 500,
            max_scan_bytes: 10485760,
          },
        },
      },
      output: 'arrow_ipc_stream',
    },
  });

  worker.emitMessage({
    success: {
      request_id: 'exec-daxis-headless-saved-query',
      response: queryResponse({
        metrics: {
          bytes_fetched: 32768,
          duration_ms: 18,
          files_touched: 1,
          files_skipped: 0,
          prebootstrap_fail_open_count: 1,
          prebootstrap_files_pruned: 0,
          footer_reads_avoided: 0,
          prebootstrap_candidate_files: 1,
          row_groups_touched: 2,
          row_groups_skipped: 3,
          footer_reads: 1,
          rows_emitted: 2,
          snapshot_bootstrap_duration_ms: 7,
          access_mode: 'browser_safe_http',
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

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'executed',
    execution_engine: 'axon_browser',
    result_transport: 'arrow_ipc',
    surface_kind: 'saved_query',
    request_id: 'req-daxis-headless-saved-query',
    execution_id: 'exec-daxis-headless-saved-query',
    metrics: {
      rows_returned: 2,
      arrow_ipc_bytes: 4,
      scan_bytes: 32768,
      duration_ms: 18,
      files_touched: 1,
      files_skipped: 0,
      prebootstrap_fail_open_count: 1,
      prebootstrap_files_pruned: 0,
      footer_reads_avoided: 0,
      prebootstrap_candidate_files: 1,
      row_groups_touched: 2,
      row_groups_skipped: 3,
      footer_reads: 1,
      snapshot_bootstrap_duration_ms: 7,
      access_mode: 'browser_safe_http',
    },
    diagnostics: {
      worker_artifact_id: 'axon-web-worker@sha256:test',
      sql_fingerprint: 'sqlfp_saved_orders_20260530',
    },
  });
  if (result.arrowIpc === null) {
    throw new Error('expected Daxis headless query to return Arrow IPC bytes');
  }
  expect(result.arrowIpc.bytes).toEqual(new Uint8Array([1, 2, 3, 4]));
});

test('Daxis headless query example withholds results over the approved output budget', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.limits.max_arrow_ipc_bytes = 3;

  const resultPromise = executeDaxisApprovedAxonRead(client, descriptor, {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
  });

  await Promise.resolve();
  worker.emitMessage({
    opened: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.emitMessage({
    success: {
      request_id: 'exec-daxis-headless-saved-query',
      response: queryResponse({
        metrics: {
          bytes_fetched: 32768,
          duration_ms: 18,
          files_touched: 1,
          files_skipped: 0,
          row_groups_touched: 2,
          row_groups_skipped: 3,
          footer_reads: 1,
          rows_emitted: 2,
          snapshot_bootstrap_duration_ms: 7,
          access_mode: 'browser_safe_http',
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

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'fallback',
    execution_engine: null,
    result_transport: null,
    fallback_reason: 'runtime_budget_overflow',
    block_reason: null,
    diagnostics: {
      budget_field: 'max_arrow_ipc_bytes',
      actual_bytes: '4',
      limit_bytes: '3',
    },
  });
  expect(result.arrowIpc).toBeNull();
});

test('Daxis headless query example withholds results over the approved scan budget', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.limits.max_scan_bytes = 32767;

  const resultPromise = executeDaxisApprovedAxonRead(client, descriptor, {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
  });

  await Promise.resolve();
  worker.emitMessage({
    opened: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.emitMessage({
    success: {
      request_id: 'exec-daxis-headless-saved-query',
      response: queryResponse({
        metrics: {
          bytes_fetched: 32768,
          duration_ms: 18,
          files_touched: 1,
          files_skipped: 0,
          row_groups_touched: 2,
          row_groups_skipped: 3,
          footer_reads: 1,
          rows_emitted: 2,
          snapshot_bootstrap_duration_ms: 7,
          access_mode: 'browser_safe_http',
        },
      }),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2],
      },
    },
  });

  const result = await resultPromise;

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'fallback',
    fallback_reason: 'runtime_budget_overflow',
    diagnostics: {
      budget_field: 'max_scan_bytes',
      actual_bytes: '32768',
      limit_bytes: '32767',
    },
    metrics: {
      rows_returned: 2,
      arrow_ipc_bytes: 2,
      scan_bytes: 32768,
      duration_ms: 18,
      files_touched: 1,
      files_skipped: 0,
      row_groups_touched: 2,
      row_groups_skipped: 3,
      footer_reads: 1,
      snapshot_bootstrap_duration_ms: 7,
      access_mode: 'browser_safe_http',
    },
  });
  expect(result.arrowIpc).toBeNull();
});

test('Daxis headless query example withholds results over the approved row budget', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.limits.max_result_rows = 1;

  const resultPromise = executeDaxisApprovedAxonRead(client, descriptor, {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
  });

  await Promise.resolve();
  worker.emitMessage({
    opened: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
    },
  });

  await new Promise((resolve) => setTimeout(resolve, 0));
  worker.emitMessage({
    success: {
      request_id: 'exec-daxis-headless-saved-query',
      response: queryResponse({
        metrics: {
          bytes_fetched: 1024,
          duration_ms: 18,
          files_touched: 1,
          files_skipped: 0,
          rows_emitted: 2,
        },
      }),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1, 2],
      },
    },
  });

  const result = await resultPromise;

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'fallback',
    fallback_reason: 'runtime_budget_overflow',
    diagnostics: {
      budget_field: 'max_result_rows',
      actual_rows: '2',
      limit_rows: '1',
    },
  });
  expect(result.arrowIpc).toBeNull();
});

test('Daxis headless query example returns fallback when execution exceeds timeout budget', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.limits.timeout_ms = 1;

  const resultPromise = executeDaxisApprovedAxonRead(client, descriptor, {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
  });

  await Promise.resolve();
  worker.emitMessage({
    opened: {
      request_id: 'req-daxis-headless-saved-query',
      name: 'orders',
    },
  });

  const result = await resultPromise;

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'fallback',
    execution_engine: null,
    result_transport: null,
    fallback_reason: 'runtime_budget_overflow',
    diagnostics: {
      budget_field: 'timeout_ms',
      limit_ms: '1',
      stage: 'query',
    },
  });
  expect(result.arrowIpc).toBeNull();
  expect(worker.commands[2]).toEqual({
    cancel: {
      request_id: 'cancel-exec-daxis-headless-saved-query',
      query_id: 'exec-daxis-headless-saved-query',
    },
  });
  expect(worker.terminated).toBe(true);
});

test('Daxis headless query example terminates the client when open exceeds timeout budget', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.limits.timeout_ms = 1;

  const result = await executeDaxisApprovedAxonRead(client, descriptor, {
    tableName: 'orders',
    nowEpochMs: 1_800_000_000_000,
  });

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'fallback',
    fallback_reason: 'runtime_budget_overflow',
    diagnostics: {
      budget_field: 'timeout_ms',
      limit_ms: '1',
      stage: 'open',
    },
  });
  expect(result.arrowIpc).toBeNull();
  expect(worker.terminated).toBe(true);
});

test('Daxis headless query example rejects unapproved descriptors before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor();
  descriptor.validated_sql.read_only = false;

  await expect(
    executeDaxisApprovedAxonRead(client, descriptor, { nowEpochMs: 1_800_000_000_000 }),
  ).rejects.toMatchObject({
    name: 'DaxisHeadlessQueryError',
    code: 'non_read_only_sql',
  } satisfies Partial<DaxisHeadlessQueryError>);
  expect(worker.commands).toHaveLength(0);
});

test('Daxis headless query example rejects unknown descriptor fields before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor() as DaxisApprovedAxonReadDescriptor & {
    tables: [
      DaxisApprovedAxonReadDescriptor['tables'][number] & {
        descriptor: DaxisApprovedAxonReadDescriptor['tables'][number]['descriptor'] & {
          access_token?: string;
        };
      },
    ];
  };
  descriptor.tables[0].descriptor.access_token = 'secret-token';

  await expect(
    executeDaxisApprovedAxonRead(client, descriptor, { nowEpochMs: 1_800_000_000_000 }),
  ).rejects.toMatchObject({
    name: 'DaxisHeadlessQueryError',
    code: 'invalid_descriptor',
  } satisfies Partial<DaxisHeadlessQueryError>);
  expect(worker.commands).toHaveLength(0);
});

test('Daxis headless query example rejects unknown active-file fields before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor() as DaxisApprovedAxonReadDescriptor & {
    tables: [
      DaxisApprovedAxonReadDescriptor['tables'][number] & {
        descriptor: DaxisApprovedAxonReadDescriptor['tables'][number]['descriptor'] & {
          active_files: Array<
            DaxisApprovedAxonReadDescriptor['tables'][number]['descriptor']['active_files'][number] & {
              raw_prompt?: string;
            }
          >;
        };
      },
    ];
  };
  descriptor.tables[0].descriptor.active_files[0].raw_prompt = 'show me customer emails';

  await expect(
    executeDaxisApprovedAxonRead(client, descriptor, { nowEpochMs: 1_800_000_000_000 }),
  ).rejects.toMatchObject({
    name: 'DaxisHeadlessQueryError',
    code: 'invalid_descriptor',
  } satisfies Partial<DaxisHeadlessQueryError>);
  expect(worker.commands).toHaveLength(0);
});

test('Daxis headless descriptor validation rejects unknown nested capability keys', () => {
  const descriptor = daxisApprovedAxonReadDescriptor() as DaxisApprovedAxonReadDescriptor & {
    tables: [
      DaxisApprovedAxonReadDescriptor['tables'][number] & {
        descriptor: DaxisApprovedAxonReadDescriptor['tables'][number]['descriptor'] & {
          required_capabilities: {
            capabilities: Record<string, string>;
          };
        };
      },
    ];
  };
  descriptor.tables[0].descriptor.required_capabilities = {
    capabilities: {
      signed_url_access: 'supported',
      access_token: 'secret-token',
    },
  };

  expect(() => validateDaxisApprovedAxonReadDescriptor(descriptor, 1_800_000_000_000)).toThrow(
    /known capability key/,
  );
});

test('Daxis headless descriptor validation rejects invalid partition column types', () => {
  const descriptor = daxisApprovedAxonReadDescriptor();
  const mutableDescriptor = descriptor as unknown as {
    tables: [{ descriptor: { partition_column_types: Record<string, string> } }];
  };
  mutableDescriptor.tables[0].descriptor.partition_column_types = {
    region: 'secret-token',
  };

  expect(() => validateDaxisApprovedAxonReadDescriptor(descriptor, 1_800_000_000_000)).toThrow(
    /partition_column_types\.region/,
  );
});

test('Daxis headless descriptor validation rejects non-string partition values', () => {
  const descriptor = daxisApprovedAxonReadDescriptor();
  const mutableDescriptor = descriptor as unknown as {
    tables: [
      { descriptor: { active_files: Array<{ partition_values: Record<string, unknown> }> } },
    ];
  };
  mutableDescriptor.tables[0].descriptor.active_files[0].partition_values = {
    region: { raw_prompt: 'show me customer emails' },
  };

  expect(() => validateDaxisApprovedAxonReadDescriptor(descriptor, 1_800_000_000_000)).toThrow(
    /partition_values\.region/,
  );
});

test('Daxis headless query example rejects multi-table descriptors before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const descriptor = daxisApprovedAxonReadDescriptor() as DaxisApprovedAxonReadDescriptor & {
    tables: Array<DaxisApprovedAxonReadDescriptor['tables'][number]>;
  };
  descriptor.tables.push({
    ...descriptor.tables[0],
    table_id: 'tbl_daxis_sales_customers',
    descriptor_id: 'desc_daxis_sales_customers_v42',
  });

  await expect(
    executeDaxisApprovedAxonRead(client, descriptor as DaxisApprovedAxonReadDescriptor, {
      nowEpochMs: 1_800_000_000_000,
    }),
  ).rejects.toMatchObject({
    name: 'DaxisHeadlessQueryError',
    code: 'invalid_descriptor',
    message: 'Daxis-approved descriptor v1 requires exactly one table descriptor',
  } satisfies Partial<DaxisHeadlessQueryError>);
  expect(worker.commands).toHaveLength(0);
});

test('Daxis headless descriptor validation rejects unknown taxonomy values', () => {
  const invalidCases: Array<{
    field: 'surface_kind' | 'intent_kind' | 'input_artifact_kind' | 'compiled_artifact_kind';
    value: string;
    code: DaxisHeadlessQueryError['code'];
    message: string;
  }> = [
    {
      field: 'surface_kind',
      value: 'notebook',
      code: 'unsupported_surface',
      message: 'Daxis-approved descriptor surface_kind must be one of',
    },
    {
      field: 'intent_kind',
      value: 'prompt',
      code: 'invalid_descriptor',
      message: 'Daxis-approved descriptor intent_kind must be one of',
    },
    {
      field: 'input_artifact_kind',
      value: 'raw_prompt',
      code: 'invalid_descriptor',
      message: 'Daxis-approved descriptor input_artifact_kind must be one of',
    },
    {
      field: 'compiled_artifact_kind',
      value: 'unvalidated_sql',
      code: 'invalid_descriptor',
      message: 'Daxis-approved descriptor compiled_artifact_kind must be one of',
    },
  ];

  for (const invalidCase of invalidCases) {
    const descriptor = daxisApprovedAxonReadDescriptor();
    const mutableDescriptor = descriptor as unknown as Record<string, unknown>;
    mutableDescriptor[invalidCase.field] = invalidCase.value;

    expect(() =>
      validateDaxisApprovedAxonReadDescriptor(descriptor, 1_800_000_000_000),
    ).toThrowError(
      expect.objectContaining({
        name: 'DaxisHeadlessQueryError',
        code: invalidCase.code,
        message: expect.stringContaining(invalidCase.message),
      }),
    );
  }
});

test('Daxis headless descriptor validation rejects incompatible surface metadata', () => {
  const invalidCases: Array<{
    surface_kind: DaxisApprovedAxonReadDescriptor['surface_kind'];
    intent_kind: DaxisApprovedAxonReadDescriptor['intent_kind'];
    input_artifact_kind: DaxisApprovedAxonReadDescriptor['input_artifact_kind'];
    message: string;
  }> = [
    {
      surface_kind: 'agent',
      intent_kind: 'sql',
      input_artifact_kind: 'raw_sql',
      message:
        'Daxis-approved descriptor surface agent must use intent_kind=semantic_query and input_artifact_kind=semantic_plan',
    },
    {
      surface_kind: 'builder',
      intent_kind: 'semantic_query',
      input_artifact_kind: 'raw_sql',
      message:
        'Daxis-approved descriptor surface builder must use intent_kind=semantic_query and input_artifact_kind=builder_plan',
    },
    {
      surface_kind: 'api',
      intent_kind: 'semantic_query',
      input_artifact_kind: 'semantic_plan',
      message:
        'Daxis-approved descriptor surface api must use intent_kind=sql and input_artifact_kind=raw_sql',
    },
    {
      surface_kind: 'dashboard_tile',
      intent_kind: 'sql',
      input_artifact_kind: 'builder_plan',
      message:
        'Daxis-approved descriptor surface dashboard_tile must use intent_kind=sql and input_artifact_kind=saved_sql',
    },
  ];

  for (const invalidCase of invalidCases) {
    const descriptor = daxisApprovedAxonReadDescriptor();
    descriptor.surface_kind = invalidCase.surface_kind;
    descriptor.intent_kind = invalidCase.intent_kind;
    descriptor.input_artifact_kind = invalidCase.input_artifact_kind;

    expect(() =>
      validateDaxisApprovedAxonReadDescriptor(descriptor, 1_800_000_000_000),
    ).toThrowError(
      expect.objectContaining({
        name: 'DaxisHeadlessQueryError',
        code: 'invalid_descriptor',
        message: invalidCase.message,
      }),
    );
  }
});

test('Daxis headless query example maps pre-handoff cancellation into a cancelled envelope', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const controller = new AbortController();
  controller.abort('user navigated away');

  const result = await executeDaxisApprovedAxonRead(client, daxisApprovedAxonReadDescriptor(), {
    nowEpochMs: 1_800_000_000_000,
    signal: controller.signal,
  });

  expect(result.envelope).toMatchObject({
    schema_version: 'daxis.query_result.v1',
    status: 'cancelled',
    execution_engine: null,
    result_transport: null,
    fallback_reason: null,
    block_reason: null,
    request_id: 'req-daxis-headless-saved-query',
    execution_id: 'exec-daxis-headless-saved-query',
  });
  expect(result.arrowIpc).toBeNull();
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects malformed successful HTTP resolver responses', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const fetchGlobal = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const previousFetch = fetchGlobal.fetch;

  fetchGlobal.fetch = (async () =>
    new Response(
      JSON.stringify({
        provider: 'gcs',
        table_uri: 'gs://axon-fixtures/partitioned-table',
        requested_snapshot_version: 7,
        resolved_snapshot_version: 7,
        requested_access_mode: 'auto',
        actual_access_mode: 'signed_url',
        expires_at_epoch_ms: 4_102_444_800_000,
      }),
      {
        status: 200,
        headers: { 'content-type': 'application/json' },
      },
    )) as typeof fetch;

  try {
    await expect(
      client.openDeltaLocation('events', {
        resolutionMode: 'server_snapshot',
        provider: 'gcs',
        tableUri: 'gs://axon-fixtures/partitioned-table',
        credentialProfile: { id: 'prod-readonly' },
        resolverUrl: 'https://resolver.example.test/delta/location',
        snapshotVersion: 7,
      }),
    ).rejects.toMatchObject({
      name: 'DeltaLocationResolverError',
      code: 'resolver_unavailable',
    } satisfies Partial<DeltaLocationResolverError>);
    expect(worker.commands).toHaveLength(0);
  } finally {
    if (previousFetch) {
      fetchGlobal.fetch = previousFetch;
    } else {
      Reflect.deleteProperty(fetchGlobal, 'fetch');
    }
  }
});

test('openDeltaLocation propagates signed URL resolver failures with redacted diagnostics', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  const result = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    requestedAccessMode: 'signed_url',
    resolveDeltaLocation: async () => {
      throw new DeltaLocationResolverError(
        'signed_url_unavailable',
        'failed to sign https://storage.example.test/object?X-Goog-Signature=secret Authorization: Bearer abc123 Authorization: Basic basic-secret {"private_key":"secret"}',
      );
    },
  });

  await expect(result).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'signed_url_unavailable',
  } satisfies Partial<DeltaLocationResolverError>);
  await expect(result).rejects.not.toThrow('X-Goog-Signature=secret');
  await expect(result).rejects.not.toThrow('Bearer abc123');
  await expect(result).rejects.not.toThrow('basic-secret');
  await expect(result).rejects.not.toThrow('private_key');
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects expired resolver descriptors before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 'gcs',
      tableUri: 'gs://axon-fixtures/partitioned-table',
      credentialProfile: { id: 'prod-readonly' },
      resolveDeltaLocation: async () =>
        deltaLocationResponse({
          expires_at_epoch_ms: 1,
        }),
    }),
  ).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'descriptor_expired',
  } satisfies Partial<DeltaLocationResolverError>);
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects malformed resolver envelopes before worker handoff', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const malformed = { ...deltaLocationResponse() } as Partial<DeltaLocationResolveResponse>;
  delete malformed.expires_at_epoch_ms;

  const openedPromise = client.openDeltaLocation('events', {
    resolutionMode: 'server_snapshot',
    provider: 'gcs',
    tableUri: 'gs://axon-fixtures/partitioned-table',
    credentialProfile: { id: 'prod-readonly' },
    resolveDeltaLocation: async () => malformed as DeltaLocationResolveResponse,
  });

  await Promise.resolve();
  expect(worker.commands).toHaveLength(0);
  await expect(openedPromise).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'resolver_unavailable',
  } satisfies Partial<DeltaLocationResolverError>);
  await expect(openedPromise).rejects.toThrow('expires_at_epoch_ms');
});

test('validateDeltaLocationResolveResponse rejects malformed resolver response fields', () => {
  const request: DeltaLocationResolveRequest = {
    provider: 'gcs',
    table_uri: 'gs://axon-fixtures/partitioned-table',
    credential_profile: { id: 'prod-readonly' },
    requested_access_mode: 'auto',
    snapshot_version: 7,
  };
  const malformed = {
    ...deltaLocationResponse(),
    actual_access_mode: 'browser_safe_http',
  };

  expect(() =>
    validateDeltaLocationResolveResponse(
      malformed as unknown as DeltaLocationResolveResponse,
      request,
      4_102_444_700_000,
    ),
  ).toThrow(DeltaLocationResolverError);
});

test('openDeltaLocation validates logical URI and credential profile before invoking resolver', async () => {
  const worker = new FakeWorker();
  const requests: DeltaLocationResolveRequest[] = [];
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 's3',
      tableUri: 'https://bucket.s3.amazonaws.com/table?X-Amz-Signature=secret',
      credentialProfile: { id: 'prod-readonly' },
      resolveDeltaLocation: async (request) => {
        requests.push(request);
        return deltaLocationResponse();
      },
    }),
  ).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'invalid_table_uri',
  } satisfies Partial<DeltaLocationResolverError>);

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 's3',
      tableUri: 's3://axon-fixtures/partitioned-table',
      credentialProfile: { id: 'AKIAIOSFODNN7EXAMPLE' },
      resolveDeltaLocation: async (request) => {
        requests.push(request);
        return deltaLocationResponse();
      },
    }),
  ).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'policy_blocked',
  } satisfies Partial<DeltaLocationResolverError>);

  expect(requests).toEqual([]);
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects unsupported provider and access mode before invoking resolver', async () => {
  const worker = new FakeWorker();
  const requests: DeltaLocationResolveRequest[] = [];
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 'r2' as never,
      tableUri: 'az://axonaccount/tables/prod-like-events',
      credentialProfile: { id: 'prod-readonly' },
      resolveDeltaLocation: async (request) => {
        requests.push(request);
        return deltaLocationResponse();
      },
    }),
  ).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'provider_not_supported',
  } satisfies Partial<DeltaLocationResolverError>);

  await expect(
    client.openDeltaLocation('events', {
      resolutionMode: 'server_snapshot',
      provider: 'gcs',
      tableUri: 'gs://axon-fixtures/partitioned-table',
      credentialProfile: { id: 'prod-readonly' },
      requestedAccessMode: 'direct' as never,
      resolveDeltaLocation: async (request) => {
        requests.push(request);
        return deltaLocationResponse();
      },
    }),
  ).rejects.toMatchObject({
    name: 'DeltaLocationResolverError',
    code: 'access_mode_not_supported',
  } satisfies Partial<DeltaLocationResolverError>);

  expect(requests).toEqual([]);
  expect(worker.commands).toHaveLength(0);
});

test('openDeltaLocation rejects invalid snapshot versions before invoking resolver', async () => {
  const worker = new FakeWorker();
  const requests: DeltaLocationResolveRequest[] = [];
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  for (const snapshotVersion of [Number.NaN, 1.5, -1]) {
    await expect(
      client.openDeltaLocation('events', {
        resolutionMode: 'server_snapshot',
        provider: 'gcs',
        tableUri: 'gs://axon-fixtures/partitioned-table',
        credentialProfile: { id: 'prod-readonly' },
        snapshotVersion,
        resolveDeltaLocation: async (request) => {
          requests.push(request);
          return deltaLocationResponse();
        },
      }),
    ).rejects.toMatchObject({
      name: 'DeltaLocationResolverError',
      code: 'invalid_snapshot_version',
    } satisfies Partial<DeltaLocationResolverError>);
  }

  expect(requests).toEqual([]);
  expect(worker.commands).toHaveLength(0);
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

test('projects coordinator and DataFusion owned-memory metrics through worker events', async () => {
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
    { requestId: 'req-owned-memory' },
  );

  worker.emitRawMessage({
    owned_memory_metrics: {
      context: {
        phase: 'query',
        request_id: 'req-owned-memory',
        query_id: 'req-owned-memory',
        table_name: 'events',
      },
      coordinator: {
        limit_bytes: 33_554_432,
        reserved_bytes: 0,
        staged_bytes: 0,
        peak_reserved_bytes: 16_777_216,
        peak_staged_bytes: 4096,
      },
      datafusion: {
        limit_bytes: 67_108_864,
        reserved_bytes: 0,
        peak_bytes: 8192,
      },
    },
  });

  await expect(settlement(resultPromise)).resolves.toEqual({ status: 'pending' });
  expect(events).toEqual([
    {
      owned_memory_metrics: {
        context: {
          phase: 'query',
          request_id: 'req-owned-memory',
          query_id: 'req-owned-memory',
          table_name: 'events',
        },
        coordinator: {
          limit_bytes: 33_554_432,
          reserved_bytes: 0,
          staged_bytes: 0,
          peak_reserved_bytes: 16_777_216,
          peak_staged_bytes: 4096,
        },
        datafusion: {
          limit_bytes: 67_108_864,
          reserved_bytes: 0,
          peak_bytes: 8192,
        },
      },
    },
  ]);

  worker.emitMessage({
    success: {
      request_id: 'req-owned-memory',
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes: [1],
      },
    },
  });
  await expect(resultPromise).resolves.toMatchObject({ request_id: 'req-owned-memory' });
});

test('rejects malformed owned-memory worker metrics', async () => {
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
      sql: 'SELECT 1',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-invalid-owned-memory' },
  );

  worker.emitRawMessage({
    owned_memory_metrics: {
      context: {
        phase: 'query',
        request_id: 'req-invalid-owned-memory',
        query_id: 'req-invalid-owned-memory',
        table_name: 'events',
      },
      coordinator: {
        limit_bytes: 33_554_432,
        reserved_bytes: 0,
        staged_bytes: 0,
        peak_reserved_bytes: 16_777_216,
        peak_staged_bytes: -1,
      },
    },
  });

  await expect(resultPromise).rejects.toThrow(AxonProtocolError);
  await expect(resultPromise).rejects.toThrow(
    'owned_memory_metrics.coordinator.peak_staged_bytes must be a non-negative integer',
  );
  expect(events).toEqual([]);
});

test('projects cache and dormant readahead metrics through events and final responses', async () => {
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
      sql: 'SELECT 1',
      preferred_target: 'browser_wasm',
    },
    { requestId: 'req-cache-metrics' },
  );
  const counters = {
    range_cache_hits: 1,
    range_cache_misses: 0,
    range_cache_bytes_reused: 800,
    range_cache_bytes_stored: 0,
    range_cache_validation_misses: 0,
    range_cache_degraded_identity_reads: 0,
    range_readahead_requests: 0,
    range_readahead_bytes_fetched: 0,
    range_readahead_bytes_used: 0,
    range_readahead_wasted_bytes: 0,
    scan_overfetch_bytes: 0,
    coordinator_peak_staged_bytes: 4_096,
    coordinator_staging_limit_bytes: 8_388_608,
    cursor_peak_pending_encoded_bytes: 2_048,
    cursor_peak_transport_chunk_bytes: 1_048_576,
  };
  worker.emitRawMessage({
    range_read_metrics: {
      context: {
        phase: 'query',
        request_id: 'req-cache-metrics',
        query_id: 'req-cache-metrics',
        table_name: 'events',
      },
      bytes_fetched: 0,
      files_touched: 1,
      files_skipped: 0,
      row_groups_touched: 1,
      row_groups_skipped: 0,
      rows_emitted: 1,
      ...counters,
    },
  });
  const response = queryResponse();
  Object.assign(response.metrics, counters);
  worker.emitMessage({
    success: {
      request_id: 'req-cache-metrics',
      response,
      result: { format: 'stream', content_type: 'application/vnd.apache.arrow.stream', bytes: [1] },
    },
  });
  await expect(resultPromise).resolves.toMatchObject({ response: { metrics: counters } });
  expect(events).toContainEqual({ range_read_metrics: expect.objectContaining(counters) });
});

for (const field of [
  'range_cache_hits',
  'range_cache_misses',
  'range_cache_bytes_reused',
  'range_cache_bytes_stored',
  'range_cache_validation_misses',
  'range_cache_degraded_identity_reads',
  'range_readahead_requests',
  'range_readahead_bytes_fetched',
  'range_readahead_bytes_used',
  'range_readahead_wasted_bytes',
  'scan_overfetch_bytes',
  'coordinator_peak_staged_bytes',
  'coordinator_staging_limit_bytes',
  'cursor_peak_pending_encoded_bytes',
  'cursor_peak_transport_chunk_bytes',
] as const) {
  test(`rejects non-finite ${field} worker metrics`, async () => {
    const worker = new FakeWorker();
    const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
    const resultPromise = client.query(
      'events',
      {
        table_uri: snapshot.table_uri,
        snapshot_version: snapshot.snapshot_version,
        sql: 'SELECT 1',
        preferred_target: 'browser_wasm',
      },
      { requestId: `req-invalid-${field}` },
    );
    worker.emitRawMessage({
      range_read_metrics: {
        context: {
          phase: 'query',
          request_id: `req-invalid-${field}`,
          query_id: `req-invalid-${field}`,
          table_name: 'events',
        },
        bytes_fetched: 0,
        files_touched: 1,
        files_skipped: 0,
        row_groups_touched: 1,
        row_groups_skipped: 0,
        rows_emitted: 1,
        [field]: Number.NaN,
      },
    });
    await expect(resultPromise).rejects.toThrow(
      `range_read_metrics.${field} must be a finite number`,
    );
  });
}

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

function brokeredDeltaPlan(): BrokeredDeltaReadAccessPlan {
  return {
    plan_type: 'brokered_delta',
    tableId: 'tbl-123',
    fullName: 'main.sales.orders',
    tableRoot: 's3://prod-bucket/tables/orders',
    grantId: 'grant-456',
    expiresAtEpochMs: 4_102_444_800_000,
    deltaAccessMode: 'delta_log',
    policyAuthority: {
      authority: 'unity_catalog',
      directExternalEngineRead: 'confirmed',
    },
    objectAccess: {
      list: true,
      head: true,
      get: false,
      rangeGet: true,
      batchSign: true,
      proxyRange: true,
    },
  };
}

function brokeredSnapshot(): ResolvedSnapshotDescriptor {
  return {
    table_uri: 's3://prod-bucket/tables/orders',
    snapshot_version: 12,
    partition_column_types: { region: 'string' },
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: [
      {
        path: 'part-000.parquet',
        size_bytes: 128,
        partition_values: { region: 'west' },
      },
    ],
  };
}

function deltaSharingAccessPlan() {
  return {
    plan_type: 'delta_sharing',
    tableId: 'tbl-share',
    fullName: 'main.analytics.events',
    sharingEndpoint: 'https://sharing.example.test/delta-sharing',
    expiresAtEpochMs: 4_102_444_800_000,
    files: [
      {
        path: 'part-000.parquet',
        url: 'https://storage.example.test/share/part-000.parquet?X-Amz-Signature=abc',
        size_bytes: 128,
        partition_values: { region: 'west' },
      },
    ],
  } as const;
}

function daxisApprovedAxonReadDescriptor(): DaxisApprovedAxonReadDescriptor {
  return {
    schema_version: 'daxis.approved_axon_read.v1',
    request_id: 'req-daxis-headless-saved-query',
    correlation_id: 'corr-daxis-headless-saved-query',
    execution_id: 'exec-daxis-headless-saved-query',
    workspace_id: 'workspace_sales',
    surface_kind: 'saved_query',
    intent_kind: 'sql',
    input_artifact_kind: 'saved_sql',
    compiled_artifact_kind: 'validated_sql',
    query_id: 'query-daxis-headless-saved-query',
    validated_sql: {
      sql: 'select order_id, amount_cents from orders order by amount_cents desc limit 2',
      dialect: 'daxis_sql_v1',
      fingerprint: 'sqlfp_saved_orders_20260530',
      validation_id: 'sqlval_saved_orders_20260530',
      read_only: true,
    },
    tables: [
      {
        catalog_id: 'catalog_main',
        table_id: 'tbl_daxis_sales_orders',
        descriptor_id: 'desc_daxis_sales_orders_v42',
        table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
        full_name: 'main.sales.orders',
        snapshot_version: 42,
        schema_hash: 'sha256:orders-schema-v7',
        descriptor_schema_hash: 'sha256:orders-schema-v7',
        descriptor: {
          table_uri: 'gs://daxis-prod-lakehouse/sales/orders',
          snapshot_version: 42,
          partition_column_types: {
            order_date: 'string',
          },
          active_files: [
            {
              path: 'order_date=2026-05-30/part-00000.parquet',
              url: 'https://storage.googleapis.com/daxis-prod-lakehouse/sales/orders/order_date=2026-05-30/part-00000.parquet?X-Goog-Signature=redacted',
              size_bytes: 32768,
              partition_values: {
                order_date: '2026-05-30',
              },
            },
          ],
        },
      },
    ],
    access_proof: {
      policy_decision_id: 'policy_decision_saved_orders_20260530',
      read_access_decision: 'approved',
      expires_at_epoch_ms: 1_800_000_060_000,
    },
    limits: {
      max_result_rows: 500,
      max_arrow_ipc_bytes: 1_048_576,
      max_scan_bytes: 10_485_760,
      timeout_ms: 15_000,
      cancellation_deadline_epoch_ms: 1_800_000_015_000,
    },
    runtime_preference: {
      preferred_engine: 'axon_browser',
      allow_remote_fallback: true,
    },
  };
}

function legacySuccessMessage(
  requestId: string,
  bytes: number[],
): WireBrowserWorkerMessageEnvelope {
  return {
    success: {
      request_id: requestId,
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        bytes,
      },
    },
  };
}

function chunkedSuccessMessage(requestId: string, byteLength: number, chunkCount: number): unknown {
  return {
    success: {
      request_id: requestId,
      response: queryResponse(),
      result: {
        format: 'stream',
        content_type: 'application/vnd.apache.arrow.stream',
        delivery: 'chunked_buffers',
        byte_length: byteLength,
        chunk_count: chunkCount,
      },
    },
  };
}

function arrowIpcChunk(
  requestId: string,
  sequence: number,
  byteOffset: number,
  bytes: number[],
): unknown {
  return {
    arrow_ipc_chunk: {
      context: {
        phase: 'query',
        request_id: requestId,
      },
      request_id: requestId,
      sequence,
      byte_offset: byteOffset,
      byte_length: bytes.length,
      bytes,
    },
  };
}

async function expectChunkProtocolReject(messages: unknown[], expectedMessage: string) {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });
  const requestId = 'req-query-bad-chunks';
  const resultPromise = client.query(
    'events',
    {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
    },
    { requestId },
  );

  for (const message of messages) {
    worker.emitRawMessage(message);
  }

  await expect(resultPromise).rejects.toThrow(AxonProtocolError);
  await expect(resultPromise).rejects.toThrow(expectedMessage);
}

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

function parquetInspectionSummary() {
  return {
    path: 'part-000.parquet',
    object_size_bytes: 128,
    footer_length_bytes: 32,
    metadata_memory_size_bytes: 512,
    created_by: 'axon test',
    file_version: 2,
    row_group_count: 1,
    row_count: 3,
    column_count: 1,
    compression: {
      compressed_size_bytes: 64,
      uncompressed_size_bytes: 128,
      ratio_basis_points: 5000,
    },
    columns: [
      {
        name: 'id',
        physical_type: 'Int64',
        repetition: 'Required',
        nullable: false,
        compressed_size_bytes: 64,
        uncompressed_size_bytes: 128,
        null_count: 0,
        encodings: ['PLAIN'],
        compressions: ['UNCOMPRESSED'],
        has_statistics: true,
        has_column_index: true,
        has_offset_index: true,
        has_bloom_filter: false,
      },
    ],
    row_groups: [
      {
        index: 0,
        row_count: 3,
        compressed_size_bytes: 64,
        uncompressed_size_bytes: 128,
        columns: [
          {
            column_name: 'id',
            compression: 'UNCOMPRESSED',
            encodings: ['PLAIN'],
            compressed_size_bytes: 64,
            uncompressed_size_bytes: 128,
            null_count: 0,
            has_statistics: true,
            has_column_index: true,
            has_offset_index: true,
            has_bloom_filter: false,
          },
        ],
      },
    ],
  };
}

type ScriptedFetchCall = {
  input: RequestInfo | URL;
  init?: RequestInit;
};

function scriptedFetch(responses: Response[]): {
  calls: ScriptedFetchCall[];
  fetch: DeltaSharingFetch;
} {
  const calls: ScriptedFetchCall[] = [];

  return {
    calls,
    fetch: async (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (!response) {
        throw new Error(`unexpected fetch call: ${String(input)}`);
      }
      return response;
    },
  };
}

function jsonResponse(body: unknown, headers: Record<string, string> = {}, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json',
      ...headers,
    },
  });
}

function textResponse(text: string, headers: Record<string, string> = {}, status = 200): Response {
  return new Response(text, { status, headers });
}

function ndjsonResponse(
  actions: Array<Record<string, unknown>>,
  headers: Record<string, string> = {},
  status = 200,
): Response {
  return textResponse(
    actions.map((action) => JSON.stringify(action)).join('\n'),
    {
      'content-type': 'application/x-ndjson',
      ...headers,
    },
    status,
  );
}

function deltaSharingQueryActions(): Array<Record<string, unknown>> {
  return [
    { protocol: { minReaderVersion: 1 } },
    {
      metaData: {
        id: 'table-id',
        format: { provider: 'parquet' },
        schemaString: '{"type":"struct","fields":[]}',
      },
    },
    {
      file: {
        id: 'part-000.parquet',
        url: 'https://storage.example.test/retail/orders/part-000.parquet?X-Amz-Signature=abc',
        partitionValues: { region: 'west' },
        size: 128,
        stats: '{"numRecords":1}',
        expirationTimestamp: '2026-05-17T20:00:00Z',
      },
    },
  ];
}

function deltaLocationResponse(
  overrides: Partial<DeltaLocationResolveResponse> = {},
): DeltaLocationResolveResponse {
  return {
    descriptor: {
      ...snapshot,
      table_uri: 'axon-resolved://events/snapshot/7',
      active_files: snapshot.active_files.map((file) => ({ ...file })),
    },
    provider: 'gcs',
    table_uri: 'gs://axon-fixtures/partitioned-table',
    requested_snapshot_version: 7,
    resolved_snapshot_version: 7,
    requested_access_mode: 'auto',
    actual_access_mode: 'signed_url',
    expires_at_epoch_ms: 4_102_444_800_000,
    warnings: [],
    refresh: {
      refresh_after_epoch_ms: 4_102_444_740_000,
      same_snapshot_required: true,
    },
    ...overrides,
  };
}

async function settleUnexpectedOpen(
  worker: FakeWorker,
  requestId: string,
  name: string,
): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (worker.commands.length > 0) {
    worker.emitMessage({ opened: { request_id: requestId, name } });
  }
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
