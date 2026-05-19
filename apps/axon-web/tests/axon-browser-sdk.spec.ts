import { expect, test } from '@playwright/test';

import {
  AxonProtocolError,
  AxonSdkError,
  AxonWorkerError,
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
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerCommand,
  type BrokeredDeltaReadAccessPlan,
  type DeltaSharingFetch,
  type DeltaSharingReadPlan,
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
      expirationTime: '2026-06-01T00:00:00Z',
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

test('openDeltaLocation gates CORS HTTP table reconstruction on list head and range capability', async () => {
  const worker = new FakeWorker();
  const client = createAxonBrowserClient({ worker: worker as unknown as Worker });

  await expect(
    client.openDeltaLocation('events', {
      source: { kind: 'cors_http_table', tableRootUrl: 'https://data.example.test/events' },
    }),
  ).rejects.toThrow('requires list, head, and rangeGet capabilities');

  await expect(
    client.openDeltaLocation('events', {
      source: {
        kind: 'cors_http_table',
        tableRootUrl: 'https://data.example.test/events',
        capabilities: { list: true, head: true, rangeGet: true },
      },
    }),
  ).rejects.toThrow('browser-local Delta snapshot reconstruction is not implemented');

  expect(worker.commands).toHaveLength(0);
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
