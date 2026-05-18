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

test('opens Delta Sharing URL-mode descriptors through the real sandbox worker', async ({
  page,
}) => {
  await page.goto('/sandbox.html');

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
        'SELECT COUNT(*) AS row_count FROM shared_orders',
        { requestId: 'query-delta-sharing-real-worker' },
      );
      const openCommand =
        postedCommands.find((command) => command.includes('open_delta_table')) ?? '';
      const openedSnapshot = JSON.parse(openCommand).open_delta_table.snapshot;

      return {
        activeFileCount: openedSnapshot.active_files.length,
        deltaSharing: opened.deltaSharing,
        executedOn: queryResult.response.executed_on,
        openCommand,
        queryCommand: postedCommands.find((command) => command.includes('"sql"')) ?? '',
        rowCount: String(queryResult.preview?.rows?.[0]?.[0]),
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
  expect(result.rowCount).toBe('4');
  expect(result.openCommand).toContain('/fixtures/prod-like/table/category=B/');
  expect(result.openCommand).toContain('/fixtures/prod-like/table/category=D/');
  expect(result.openCommand).not.toContain('secret-profile-token');
  expect(result.openCommand).not.toContain('bearerToken');
  expect(result.queryCommand).not.toContain('secret-profile-token');
  expect(result.workerEvents.join('\n')).toContain('range_read_metrics');
});

test('preserves cancellation errors from browser worker envelopes', async ({ page }) => {
  const result = await runWorkerProbe(page, 'cancellation');

  expect(result.cancellation).toMatchObject({
    name: 'AxonWorkerError',
    message: 'experimental browser DataFusion query cancelled',
  });
  expect(result.cancellation?.queryError).toMatchObject({
    code: 'execution_failed',
    target: 'browser_wasm',
  });
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

async function runWorkerProbe(page: Page, probe: WorkerProbe): Promise<WorkerProbeResult> {
  await page.goto('/sandbox.html');

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
                  message: 'experimental browser DataFusion query cancelled',
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
