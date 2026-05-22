import { expect, type Page, test } from '@playwright/test';

type WorkerProbe = 'success' | 'cancellation' | 'fallback-error';

type WorkerProbeResult = {
  bytes?: number[];
  byteType?: string;
  cancellation?: SerializedWorkerError;
  disposedName?: string;
  executedOn?: string;
  fallbackError?: SerializedWorkerError;
  fallbackReason?: unknown;
  openedName: string;
};

type SerializedWorkerError = {
  fallbackReason?: unknown;
  message: string;
  name: string;
  queryError?: unknown;
};

const BINARY_STRING_INT_PARQUET_BASE64 =
  'UEFSMRUEFRgVGEwVBhUAEgAAAQAAAAIAAAADAAAAFQAVCBUILBUGFRAVBhUGAAACAyQAFQQVKhUqTBUGFQASAAADAAAAAAECAwAAAAMEBQMAAAAGBwgVABUIFQgsFQYVEBUGFQYAAAIDJAAZEgIZGAQBAAAAGRgEAwAAABUCGRYAABkSAhkYAwABAhkYAwYHCBUCGRYAABkcFjwVKhYAAAAZHBasARUqFgAAGRYSABUCGTxIBnNjaGVtYRUEABUCJQAYAmlkABUMJQAYB3BheWxvYWQAFgYZHBksJgAcFQIZNQAGEBkYAmlkFQAWBhZeFl4mPCYIHBgEAwAAABgEAQAAABYAKAQDAAAAGAQBAAAAEREAGSwVBBUAFQIAFQAVEBUCAAAWrgIVFBbWARUuACYAHBUMGTUABhAZGAdwYXlsb2FkFQAWBhZwFnAmrAEmZhw2ACgDBgcIGAMAAQIREQAZLBUEFQAVAgAVABUQFQIAPBYSAAAWwgIVHBaEAhUqABbOARYGJggWzgEUAAAoGXBhcnF1ZXQtcnMgdmVyc2lvbiA1Ny4zLjAZLBwAABwAAAADAQAAUEFSMQ==';
const BINARY_STRING_INT_PARQUET_BYTES = Buffer.from(BINARY_STRING_INT_PARQUET_BASE64, 'base64');
const BINARY_STRING_INT_PARQUET_PATH =
  '**/fixtures/browser-datafusion-runtime/binary-string-int.parquet';

test('starts a real browser Worker and handles Arrow IPC success envelopes', async ({ page }) => {
  const result = await runWorkerProbe(page, 'success');

  expect(result.openedName).toBe('events');
  expect(result.disposedName).toBe('events');
  expect(result.executedOn).toBe('native');
  expect(result.byteType).toBe('Uint8Array');
  expect(result.bytes).toEqual([255, 0, 1, 127]);
  expect(result.fallbackReason).toEqual({
    capability_gate: {
      capability: 'multi_partition_execution',
      required_state: 'native_only',
    },
  });
});

test('opens Delta Sharing URL-mode descriptors through the real browser query worker', async ({
  page,
}) => {
  await routeBinaryStringIntParquet(page);
  await page.goto('/');

  const result = await page.evaluate(async () => {
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const postedCommands: string[] = [];
    const workerEvents: string[] = [];
    const recordingWorker = {
      addEventListener: worker.addEventListener.bind(worker),
      removeEventListener: worker.removeEventListener.bind(worker),
      terminate: worker.terminate.bind(worker),
      postMessage(message: unknown): void {
        postedCommands.push(JSON.stringify(message));
        worker.postMessage(message);
      },
    };
    const client = sdk.createAxonBrowserClient({
      worker: recordingWorker,
      onEvent: (event: unknown) => workerEvents.push(JSON.stringify(event)),
    });

    const manifest = (await (
      await fetch('/fixtures/prod-like/delta-log-manifest.json')
    ).json()) as {
      data_files: Array<{
        relative_path: string;
        url_path: string;
        size_bytes: number;
        partition_values: Record<string, string>;
      }>;
      expected_latest_version: number;
    };
    const maybeActiveFiles = ['B', 'D'].map((category) =>
      manifest.data_files.findLast((file) => file.partition_values.category === category),
    );
    if (maybeActiveFiles.some((file) => file === undefined)) {
      throw new Error('expected active B and D fixture files in prod-like manifest');
    }
    const activeFiles = maybeActiveFiles as Array<NonNullable<(typeof maybeActiveFiles)[number]>>;

    let sharingRequest:
      | {
          authorization: string | null;
          body: string;
          method: string | undefined;
          url: string;
        }
      | undefined;
    const sharingFetch = async (
      input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> => {
      const headers = new Headers(init?.headers);
      sharingRequest = {
        authorization: headers.get('authorization'),
        body: String(init?.body ?? ''),
        method: init?.method,
        url: String(input),
      };
      const expiresAt = new Date(Date.now() + 600_000).toISOString();
      const lines = [
        JSON.stringify({ protocol: { minReaderVersion: 1 } }),
        JSON.stringify({ metaData: { id: 'shared-orders', partitionColumns: ['category'] } }),
        ...activeFiles.map((file) =>
          JSON.stringify({
            file: {
              id: file.relative_path,
              url: new URL(`${file.url_path}?X-Amz-Signature=signed-fixture-url`, location.href)
                .href,
              size: file.size_bytes,
              partitionValues: file.partition_values,
              expirationTimestamp: expiresAt,
            },
          }),
        ),
      ];
      return new Response(lines.join('\n'), {
        headers: {
          'content-type': 'application/x-ndjson',
          'delta-table-version': String(manifest.expected_latest_version),
        },
      });
    };

    const session = await sdk.createDeltaSharingClient({ fetch: sharingFetch }).connect({
      source: 'json',
      value: {
        endpoint: 'https://sharing.example.test/delta-sharing',
        bearerToken: 'secret-profile-token',
        expirationTime: '2026-12-31T00:00:00Z',
      },
    });

    try {
      const opened = await client.openDeltaShare('shared_orders', {
        session,
        table: { share: 'retail_share', schema: 'sales', table: 'orders' },
        responseFormat: 'auto',
        requestId: 'open-delta-sharing-real-worker',
      });
      const queryResult = await client.query(
        'shared_orders',
        'SELECT category, id, value FROM shared_orders ORDER BY id',
        { requestId: 'query-delta-sharing-real-worker' },
      );
      const runtimeFixtureUrl = new URL(
        '/fixtures/browser-datafusion-runtime/binary-string-int.parquet',
        location.href,
      ).href;
      const openedRuntime = await client.openDeltaTable(
        'worker_runtime_types',
        {
          table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href).href,
          snapshot_version: 0,
          partition_column_types: { category: 'string' },
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          active_files: [
            {
              path: 'category=runtime/part-000.parquet',
              url: runtimeFixtureUrl,
              size_bytes: 442,
              partition_values: { category: 'runtime' },
            },
          ],
        },
        { requestId: 'open-browser-datafusion-runtime-types' },
      );
      const typedResult = await client.query(
        'worker_runtime_types',
        'SELECT payload, category, id FROM worker_runtime_types ORDER BY id',
        { requestId: 'query-browser-datafusion-runtime-types' },
      );
      const openCommand =
        postedCommands.find(
          (command) => command.includes('open_delta_table') && command.includes('shared_orders'),
        ) ?? '';
      const openedSnapshot = JSON.parse(openCommand).open_delta_table.snapshot;
      const runtimeOpenCommand =
        postedCommands.find(
          (command) =>
            command.includes('open_delta_table') && command.includes('worker_runtime_types'),
        ) ?? '';

      return {
        activeFileCount: openedSnapshot.active_files.length,
        commandLog: postedCommands.join('\n'),
        deltaSharing: opened.deltaSharing,
        executedOn: queryResult.response.executed_on,
        ipcByteLength: queryResult.result.bytes.byteLength,
        ipcByteType: queryResult.result.bytes.constructor.name,
        ipcContentType: queryResult.result.content_type,
        ipcFormat: queryResult.result.format,
        openCommand,
        preview: queryResult.preview,
        runtimeOpenCommand,
        typedOpenedName: openedRuntime.name,
        typedResult: {
          contentType: typedResult.result.content_type,
          executedOn: typedResult.response.executed_on,
          format: typedResult.result.format,
          preview: typedResult.preview,
          byteLength: typedResult.result.bytes.byteLength,
          byteType: typedResult.result.bytes.constructor.name,
        },
        queryCommand: postedCommands.find((command) => command.includes('"sql"')) ?? '',
        sharingRequest,
        workerEvents,
      };
    } finally {
      client.terminate();
    }
  });

  expect(result.deltaSharing).toMatchObject({
    kind: 'delta_sharing_snapshot_descriptor',
    resolvedVersion: 3,
    responseFormat: 'parquet',
    table: { share: 'retail_share', schema: 'sales', table: 'orders' },
  });
  expect(result.sharingRequest).toMatchObject({
    authorization: 'Bearer secret-profile-token',
    method: 'POST',
    url: 'https://sharing.example.test/delta-sharing/shares/retail_share/schemas/sales/tables/orders/query',
  });
  expect(result.activeFileCount).toBe(2);
  expect(result.executedOn).toBe('browser_wasm');
  expect(result.ipcFormat).toBe('stream');
  expect(result.ipcContentType).toBe('application/vnd.apache.arrow.stream');
  expect(result.ipcByteType).toBe('Uint8Array');
  expect(result.ipcByteLength).toBeGreaterThan(0);
  expect(result.preview).toMatchObject({
    columns: ['category', 'id', 'value'],
    row_count: 4,
    truncated: false,
  });
  const sharingRows = result.preview.rows as Array<[string, number, number]>;
  expect(sharingRows).toHaveLength(4);
  expect(sharingRows.map((row) => row[0])).toEqual(['B', 'B', 'D', 'D']);
  for (const row of sharingRows) {
    expect(typeof row[1]).toBe('number');
    expect(typeof row[2]).toBe('number');
    expect(row[2]).toBe(row[1] * 10);
  }
  expect(result.typedOpenedName).toBe('worker_runtime_types');
  expect(result.typedResult).toMatchObject({
    byteType: 'Uint8Array',
    contentType: 'application/vnd.apache.arrow.stream',
    executedOn: 'browser_wasm',
    format: 'stream',
    preview: {
      columns: ['payload', 'category', 'id'],
      rows: [
        ['<unsupported Binary>', 'runtime', 1],
        ['<unsupported Binary>', 'runtime', 2],
        ['<unsupported Binary>', 'runtime', 3],
      ],
      row_count: 3,
      truncated: false,
    },
  });
  expect(result.typedResult.byteLength).toBeGreaterThan(0);
  expect(result.openCommand).toContain('/fixtures/prod-like/table/category=B/');
  expect(result.openCommand).toContain('/fixtures/prod-like/table/category=D/');
  expect(result.runtimeOpenCommand).toContain(
    '/fixtures/browser-datafusion-runtime/binary-string-int.parquet',
  );
  expect(result.commandLog).not.toContain('secret-profile-token');
  expect(result.commandLog).not.toContain('bearerToken');
  expect(result.workerEvents.join('\n')).toContain('range_read_metrics');
});

test('surfaces unsupported feature errors from the real browser query worker', async ({ page }) => {
  await page.goto('/');

  const result = await page.evaluate(async () => {
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const workerEvents: string[] = [];
    const client = sdk.createAxonBrowserClient({
      worker,
      onEvent: (event: unknown) => workerEvents.push(JSON.stringify(event)),
    });
    const manifest = (await (
      await fetch('/fixtures/prod-like/delta-log-manifest.json')
    ).json()) as {
      data_files: Array<{
        relative_path: string;
        url_path: string;
        size_bytes: number;
        partition_values: Record<string, string>;
      }>;
    };
    const activeFile = manifest.data_files.findLast(
      (file) => file.partition_values.category === 'B',
    );
    if (!activeFile) {
      throw new Error('expected active B fixture file in prod-like manifest');
    }
    const snapshot = {
      table_uri: new URL(
        '/fixtures/browser-datafusion-runtime/unsupported-partition',
        location.href,
      ).href,
      snapshot_version: 1,
      partition_column_types: { category: 'unsupported' },
      browser_compatibility: { capabilities: {} },
      required_capabilities: { capabilities: {} },
      active_files: [
        {
          path: activeFile.relative_path,
          url: new URL(activeFile.url_path, location.href).href,
          size_bytes: activeFile.size_bytes,
          partition_values: activeFile.partition_values,
        },
      ],
    };

    try {
      await client.openDeltaTable('unsupported_partition', snapshot, {
        requestId: 'open-unsupported-partition',
      });
    } catch (error) {
      const candidate = error as {
        message?: string;
        name?: string;
        queryError?: unknown;
      };
      return {
        error: {
          message: String(candidate.message),
          name: String(candidate.name),
          queryError: candidate.queryError,
        },
        workerEvents,
      };
    } finally {
      client.terminate();
    }

    throw new Error('expected unsupported partition type to fail');
  });

  expect(result.error).toMatchObject({
    name: 'AxonWorkerError',
    queryError: {
      code: 'unsupported_feature',
      target: 'browser_wasm',
    },
  });
  expect(result.error.message).toContain("partition column 'category' type");
  expect(result.workerEvents.join('\n')).toContain('unsupported_feature');
});

test('preserves cancellation errors from the real browser query worker', async ({ page }) => {
  await routeBinaryStringIntParquet(page);
  await page.goto('/');

  const result = await page.evaluate(async () => {
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const workerEvents: string[] = [];
    const client = sdk.createAxonBrowserClient({
      worker,
      onEvent: (event: unknown) => workerEvents.push(JSON.stringify(event)),
    });

    try {
      await client.openDeltaTable(
        'cancelled_runtime_types',
        {
          table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href).href,
          snapshot_version: 0,
          partition_column_types: { category: 'string' },
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          active_files: [
            {
              path: 'category=runtime/part-000.parquet',
              url: new URL(
                '/fixtures/browser-datafusion-runtime/binary-string-int.parquet',
                location.href,
              ).href,
              size_bytes: 442,
              partition_values: { category: 'runtime' },
            },
          ],
        },
        { requestId: 'open-real-worker-cancellation' },
      );
      worker.postMessage({
        cancel: {
          request_id: 'cancel-stale-real-worker-query',
          query_id: 'query-that-is-not-active',
        },
      });
      const healthyResult = await client.query(
        'cancelled_runtime_types',
        'SELECT payload, category, id FROM cancelled_runtime_types ORDER BY id',
        { requestId: 'query-after-stale-real-worker-cancellation' },
      );
      const cancellationQuery = client.query(
        'cancelled_runtime_types',
        'SELECT payload, category, id FROM cancelled_runtime_types ORDER BY id',
        { requestId: 'query-real-worker-cancellation' },
      );
      worker.postMessage({
        cancel: {
          request_id: 'cancel-real-worker-query',
          query_id: 'query-real-worker-cancellation',
        },
      });

      return {
        cancellation: await captureWorkerError(cancellationQuery),
        healthyPreview: healthyResult.preview,
        workerEvents,
      };
    } finally {
      client.terminate();
    }

    async function captureWorkerError(promise: Promise<unknown>): Promise<SerializedWorkerError> {
      try {
        await promise;
      } catch (error) {
        const candidate = error as Partial<SerializedWorkerError>;
        return {
          name: String(candidate.name),
          message: String(candidate.message),
          fallbackReason: candidate.fallbackReason,
          queryError: candidate.queryError,
        };
      }

      throw new Error('expected browser worker request to fail');
    }
  });

  expect(result.cancellation).toMatchObject({
    name: 'AxonWorkerError',
  });
  expect(result.cancellation.message).toContain(
    'experimental browser DataFusion query cancelled during',
  );
  expect(result.cancellation?.queryError).toMatchObject({
    code: 'execution_failed',
    target: 'browser_wasm',
  });
  expect(result.healthyPreview).toMatchObject({
    columns: ['payload', 'category', 'id'],
    row_count: 3,
    truncated: false,
  });
  expect(result.workerEvents.join('\n')).toContain('cancellation');
});

test('preserves fallback-required errors from browser worker envelopes', async ({ page }) => {
  const result = await runWorkerProbe(page, 'fallback-error');

  expect(result.fallbackError).toMatchObject({
    name: 'AxonWorkerError',
    message: 'native fallback required for query shape',
    fallbackReason: 'browser_runtime_constraint',
  });
  expect(result.fallbackError?.queryError).toMatchObject({
    code: 'fallback_required',
    target: 'browser_wasm',
    fallback_reason: 'browser_runtime_constraint',
  });
});

async function routeBinaryStringIntParquet(page: Page): Promise<void> {
  await page.route(BINARY_STRING_INT_PARQUET_PATH, async (route) => {
    const totalLength = BINARY_STRING_INT_PARQUET_BYTES.byteLength;
    const range = route.request().headers().range;

    if (!range) {
      await route.fulfill({
        status: 200,
        headers: {
          'accept-ranges': 'bytes',
          'content-length': String(totalLength),
          'content-type': 'application/octet-stream',
        },
        body: BINARY_STRING_INT_PARQUET_BYTES,
      });
      return;
    }

    const match = /^bytes=(\d+)-(\d*)$/.exec(range);
    const start = match ? Number(match[1]) : Number.NaN;
    const requestedEnd = match?.[2] ? Number(match[2]) : totalLength - 1;
    const end = Math.min(requestedEnd, totalLength - 1);
    if (
      !Number.isSafeInteger(start) ||
      !Number.isSafeInteger(end) ||
      start < 0 ||
      end < start ||
      start >= totalLength
    ) {
      await route.fulfill({
        status: 416,
        headers: {
          'content-range': `bytes */${totalLength}`,
        },
        body: '',
      });
      return;
    }

    await route.fulfill({
      status: 206,
      headers: {
        'accept-ranges': 'bytes',
        'content-length': String(end - start + 1),
        'content-range': `bytes ${start}-${end}/${totalLength}`,
        'content-type': 'application/octet-stream',
      },
      body: BINARY_STRING_INT_PARQUET_BYTES.subarray(start, end + 1),
    });
  });
}

async function runWorkerProbe(page: Page, probe: WorkerProbe): Promise<WorkerProbeResult> {
  await page.goto('/');

  return page.evaluate(
    async ({ selectedProbe, workerScript }) => {
      const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
      const workerUrl = URL.createObjectURL(new Blob([workerScript], { type: 'text/javascript' }));
      const worker = new Worker(workerUrl, { type: 'module' });
      const client = sdk.createAxonBrowserClient({ worker });
      const snapshot = {
        table_uri: 'gs://axon-fixtures/partitioned-table',
        snapshot_version: 7,
        partition_column_types: {},
        browser_compatibility: { capabilities: {} },
        required_capabilities: { capabilities: {} },
        active_files: [
          {
            path: 'category=B/part-000.parquet',
            url: new URL('/fixtures/prod-like/table/category=B/part-000.parquet', location.href)
              .href,
            size_bytes: 128,
            partition_values: { category: 'B' },
          },
        ],
      };

      try {
        const opened = await client.openDeltaTable('events', snapshot, {
          requestId: `open-${selectedProbe}`,
        });

        if (selectedProbe === 'success') {
          const result = await client.query('events', 'SELECT COUNT(*) AS row_count FROM events', {
            requestId: 'query-success',
          });
          const disposed = await client.dispose('events', { requestId: 'dispose-success' });

          return {
            openedName: opened.name,
            disposedName: disposed.name,
            executedOn: result.response.executed_on,
            fallbackReason: result.fallbackReason,
            byteType: result.result.bytes.constructor.name,
            bytes: Array.from(result.result.bytes),
          };
        }

        if (selectedProbe === 'cancellation') {
          return {
            openedName: opened.name,
            cancellation: await captureError(
              client.query('events', 'SELECT CANCEL FROM events', { requestId: 'query-cancel' }),
            ),
          };
        }

        return {
          openedName: opened.name,
          fallbackError: await captureError(
            client.query('events', 'SELECT FALLBACK_ERROR FROM events', {
              requestId: 'query-fallback',
            }),
          ),
        };
      } finally {
        client.terminate();
        URL.revokeObjectURL(workerUrl);
      }

      async function captureError(promise: Promise<unknown>): Promise<SerializedWorkerError> {
        try {
          await promise;
        } catch (error) {
          const candidate = error as Partial<SerializedWorkerError>;
          return {
            name: String(candidate.name),
            message: String(candidate.message),
            fallbackReason: candidate.fallbackReason,
            queryError: candidate.queryError,
          };
        }

        throw new Error('expected browser worker request to fail');
      }
    },
    { selectedProbe: probe, workerScript: workerSource() },
  );
}

function workerSource(): string {
  return String.raw`
    const responseMetrics = {
      bytes_fetched: 256,
      duration_ms: 4,
      files_touched: 1,
      files_skipped: 0,
      row_groups_touched: 1,
      row_groups_skipped: 0,
      footer_reads: 1,
      rows_emitted: 1,
      access_mode: 'browser_safe_http',
    };

    self.onmessage = (event) => {
      const command = event.data;
      try {
        if (command.open_delta_table) {
          const payload = command.open_delta_table;
          const file = payload.snapshot.active_files?.[0];
          if (!file?.url || !file.url.includes('/fixtures/prod-like/table/')) {
            throw new Error('open_delta_table must carry browser-safe active file URLs');
          }
          self.postMessage({ opened: { request_id: payload.request_id, name: payload.name } });
          return;
        }

        if (command.sql) {
          const payload = command.sql;
          if (payload.output !== 'arrow_ipc_stream') {
            throw new Error('sql commands must request Arrow IPC stream output');
          }

          if (payload.query.sql.includes('CANCEL')) {
            self.postMessage({
              error: {
                request_id: payload.request_id,
                error: {
                  code: 'execution_failed',
                  message:
                    'experimental browser DataFusion query cancelled during Arrow IPC batch encoding',
                  target: 'browser_wasm',
                },
              },
            });
            return;
          }

          if (payload.query.sql.includes('FALLBACK_ERROR')) {
            self.postMessage({
              error: {
                request_id: payload.request_id,
                error: {
                  code: 'fallback_required',
                  message: 'native fallback required for query shape',
                  target: 'browser_wasm',
                  fallback_reason: 'browser_runtime_constraint',
                },
              },
            });
            return;
          }

          self.postMessage({
            success: {
              request_id: payload.request_id,
              response: {
                executed_on: 'native',
                capabilities: { capabilities: {} },
                fallback_reason: {
                  capability_gate: {
                    capability: 'multi_partition_execution',
                    required_state: 'native_only',
                  },
                },
                metrics: responseMetrics,
              },
              result: {
                format: 'stream',
                content_type: 'application/vnd.apache.arrow.stream',
                bytes: new Uint8Array([255, 0, 1, 127]),
              },
            },
          });
          return;
        }

        if (command.dispose) {
          const payload = command.dispose;
          self.postMessage({ disposed: { request_id: payload.request_id, name: payload.name } });
          return;
        }

        throw new Error('unknown worker command');
      } catch (error) {
        const requestId =
          command.open_delta_table?.request_id ??
          command.sql?.request_id ??
          command.dispose?.request_id ??
          'unknown';
        self.postMessage({
          error: {
            request_id: requestId,
            error: {
              code: 'invalid_request',
              message: error instanceof Error ? error.message : String(error),
              target: 'browser_wasm',
            },
          },
        });
      }
    };
  `;
}
