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
