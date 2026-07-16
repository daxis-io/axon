import { expect, type Page, test } from '@playwright/test';

const BINARY_STRING_INT_PARQUET_BASE64 =
  'UEFSMRUEFRgVGEwVBhUAEgAAAQAAAAIAAAADAAAAFQAVCBUILBUGFRAVBhUGAAACAyQAFQQVKhUqTBUGFQASAAADAAAAAAECAwAAAAMEBQMAAAAGBwgVABUIFQgsFQYVEBUGFQYAAAIDJAAZEgIZGAQBAAAAGRgEAwAAABUCGRYAABkSAhkYAwABAhkYAwYHCBUCGRYAABkcFjwVKhYAAAAZHBasARUqFgAAGRYSABUCGTxIBnNjaGVtYRUEABUCJQAYAmlkABUMJQAYB3BheWxvYWQAFgYZHBksJgAcFQIZNQAGEBkYAmlkFQAWBhZeFl4mPCYIHBgEAwAAABgEAQAAABYAKAQDAAAAGAQBAAAAEREAGSwVBBUAFQIAFQAVEBUCAAAWrgIVFBbWARUuACYAHBUMGTUABhAZGAdwYXlsb2FkFQAWBhZwFnAmrAEmZhw2ACgDBgcIGAMAAQIREQAZLBUEFQAVAgAVABUQFQIAPBYSAAAWwgIVHBaEAhUqABbOARYGJggWzgEUAAAoGXBhcnF1ZXQtcnMgdmVyc2lvbiA1Ny4zLjAZLBwAABwAAAADAQAAUEFSMQ==';
const BINARY_STRING_INT_PARQUET_BYTES = Buffer.from(BINARY_STRING_INT_PARQUET_BASE64, 'base64');
const BINARY_STRING_INT_PARQUET_PATH =
  '**/fixtures/browser-datafusion-runtime/internal-cursor.parquet**';
const NON_SETTLING_CHILD_SOURCE = `
self.postMessage({ kind: 'ready', version: 1 });
self.addEventListener('message', (event) => {
  const message = event.data;
  if (message.kind !== 'command' || !message.command.open_delta_table) return;
  const command = message.command.open_delta_table;
  self.postMessage({
    kind: 'public',
    version: 1,
    envelope: { opened: { request_id: command.request_id, name: command.name } },
  });
});
`;
const CRASH_ON_SQL_CHILD_SOURCE = `
self.postMessage({ kind: 'ready', version: 1 });
self.addEventListener('message', (event) => {
  const message = event.data;
  if (message.kind !== 'command') return;
  if (message.command.open_delta_table) {
    const command = message.command.open_delta_table;
    self.postMessage({
      kind: 'public',
      version: 1,
      envelope: { opened: { request_id: command.request_id, name: command.name } },
    });
    return;
  }
  if (message.command.sql) {
    setTimeout(() => {
      throw new Error('injected child crash after table open');
    }, 0);
  }
});
`;

test('private child blocks at zero credit and delivers cancellation and deadline terminals', async ({
  page,
}) => {
  await routeCursorParquet(page);
  await page.goto('/');

  const result = await page.evaluate(
    async ({ fixtureBytes }: { fixtureBytes: number }) => {
      type PrivateMessage = {
        kind?: string;
        query_id?: string;
        sequence?: bigint;
        phase?: string;
        fragment_index?: bigint;
        logical_batch_sequence?: bigint | null;
        rows_completed?: bigint;
        byte_length?: bigint;
        bytes?: Uint8Array;
        error?: unknown;
        sender_detached?: boolean;
        envelope?: { opened?: { request_id?: string } };
        metadata?: {
          status?: string;
          response?: { metrics?: { range_readahead_wasted_bytes?: number } };
        };
      };
      const cursorRequestStats = () =>
        (
          globalThis as unknown as {
            cursorRequestStats: () => Promise<{ requests: number; responseBytes: number }>;
          }
        ).cursorRequestStats();

      const worker = new Worker(
        new URL('/src/sandbox-query-child-test-harness.ts', location.href),
        {
          type: 'module',
        },
      );
      const inbox: PrivateMessage[] = [];
      worker.addEventListener('message', (event: MessageEvent<PrivateMessage>) => {
        inbox.push(event.data);
      });
      worker.addEventListener('error', (event) => {
        inbox.push({ kind: 'worker_error', error: event.message });
      });

      const waitFor = async (
        predicate: (message: PrivateMessage) => boolean,
      ): Promise<PrivateMessage> => {
        for (let attempt = 0; attempt < 500; attempt += 1) {
          const message = inbox.find(predicate);
          if (message) return message;
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throw new Error(`timed out waiting for private child message: ${JSON.stringify(inbox)}`);
      };
      const streamMessageCount = (queryId: string): number =>
        inbox.filter(
          (message) =>
            message.query_id === queryId &&
            (message.kind === 'stream_chunk' || message.kind === 'stream_terminal'),
        ).length;

      await waitFor((message) => message.kind === 'ready');
      worker.postMessage({
        kind: 'command',
        version: 1,
        command: {
          open_delta_table: {
            request_id: 'open-private-single-partition',
            name: 'single_events',
            snapshot: {
              table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href).href,
              snapshot_version: 0,
              partition_column_types: { category: 'string' },
              browser_compatibility: { capabilities: {} },
              required_capabilities: { capabilities: {} },
              active_files: [
                {
                  path: 'category=good/part-000.parquet',
                  url: new URL(
                    '/fixtures/browser-datafusion-runtime/internal-cursor.parquet',
                    location.href,
                  ).href,
                  size_bytes: fixtureBytes,
                  partition_values: { category: 'good' },
                },
              ],
            },
          },
        },
      });
      await waitFor(
        (message) =>
          message.kind === 'public' &&
          message.envelope?.opened?.request_id === 'open-private-single-partition',
      );
      worker.postMessage({
        kind: 'command',
        version: 1,
        command: {
          open_delta_table: {
            request_id: 'open-private-credit-probe',
            name: 'events',
            snapshot: {
              table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href).href,
              snapshot_version: 0,
              partition_column_types: { category: 'string' },
              browser_compatibility: { capabilities: {} },
              required_capabilities: { capabilities: {} },
              active_files: [
                {
                  path: 'category=0-good/part-000.parquet',
                  url: new URL(
                    '/fixtures/browser-datafusion-runtime/internal-cursor.parquet',
                    location.href,
                  ).href,
                  size_bytes: fixtureBytes,
                  partition_values: { category: 'good' },
                },
                {
                  path: 'category=1-bad/part-001.parquet',
                  url: new URL(
                    '/fixtures/browser-datafusion-runtime/internal-cursor.parquet',
                    location.href,
                  ).href,
                  size_bytes: fixtureBytes,
                  partition_values: { category: 'bad' },
                },
              ],
            },
          },
        },
      });
      await waitFor(
        (message) =>
          message.kind === 'public' &&
          message.envelope?.opened?.request_id === 'open-private-credit-probe',
      );

      const probe = async (
        queryId: string,
        reason: 'cancelled' | 'deadline_exceeded',
      ): Promise<{
        exactBuffer: boolean;
        plateaued: boolean;
        senderDetached: boolean;
        status: string | undefined;
        transportCounters: string[];
      }> => {
        worker.postMessage({
          kind: 'command',
          version: 1,
          command: {
            sql: {
              request_id: queryId,
              name: 'single_events',
              query: {
                table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href)
                  .href,
                snapshot_version: 0,
                sql: 'SELECT payload, category, id FROM single_events',
                preferred_target: 'browser_wasm',
                options: {},
              },
              output: 'arrow_ipc_stream',
              delivery: 'chunked_buffers',
              browser_safe_defaults: true,
            },
          },
        });
        const schema = await waitFor(
          (message) =>
            message.kind === 'stream_chunk' &&
            message.query_id === queryId &&
            message.phase === 'schema',
        );
        if (typeof schema.sequence !== 'bigint' || !(schema.bytes instanceof Uint8Array)) {
          throw new Error('private schema chunk omitted transport metadata');
        }
        worker.postMessage({
          kind: 'credit',
          version: 1,
          query_id: queryId,
          sequence: schema.sequence,
          credit_class: 'control',
          bytes: schema.bytes.byteLength,
        });

        const data = await waitFor(
          (message) =>
            message.kind === 'stream_chunk' &&
            message.query_id === queryId &&
            message.phase === 'data',
        );
        if (!(data.bytes instanceof Uint8Array)) {
          throw new Error('private data chunk omitted bytes');
        }
        const transferAudit = await waitFor(
          (message) =>
            message.kind === 'transfer_audit' &&
            message.query_id === queryId &&
            message.sequence === data.sequence,
        );
        const before = streamMessageCount(queryId);
        const requestsBefore = await cursorRequestStats();
        await new Promise((resolve) => setTimeout(resolve, 75));
        const requestsAfter = await cursorRequestStats();
        const plateaued =
          streamMessageCount(queryId) === before &&
          requestsAfter.requests === requestsBefore.requests &&
          requestsAfter.responseBytes === requestsBefore.responseBytes;

        worker.postMessage({
          kind: 'cancel',
          version: 1,
          query_id: queryId,
          reason,
        });
        const terminal = await waitFor(
          (message) => message.kind === 'stream_terminal' && message.query_id === queryId,
        );
        return {
          exactBuffer:
            data.bytes.byteOffset === 0 &&
            data.bytes.byteLength === data.bytes.buffer.byteLength &&
            data.byte_length === BigInt(data.bytes.byteLength),
          plateaued,
          senderDetached: transferAudit.sender_detached === true,
          status: terminal.metadata?.status,
          transportCounters: [
            typeof data.sequence,
            typeof data.fragment_index,
            typeof data.logical_batch_sequence,
            typeof data.rows_completed,
            typeof data.byte_length,
          ],
        };
      };

      const successfulSinglePartitionProbe = async (): Promise<{
        dataChunksBeforeTerminal: number;
        readaheadWasteBytes: number;
        status: string | undefined;
      }> => {
        const queryId = 'private-single-partition-success';
        worker.postMessage({
          kind: 'command',
          version: 1,
          command: {
            sql: {
              request_id: queryId,
              name: 'single_events',
              query: {
                table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href)
                  .href,
                snapshot_version: 0,
                sql: 'SELECT payload, category, id FROM single_events',
                preferred_target: 'browser_wasm',
                options: {},
              },
              output: 'arrow_ipc_stream',
              delivery: 'chunked_buffers',
              browser_safe_defaults: true,
            },
          },
        });

        const acknowledged = new Set<bigint>();
        let dataChunksBeforeTerminal = 0;
        for (;;) {
          const item = await waitFor(
            (message) =>
              message.query_id === queryId &&
              (message.kind === 'stream_terminal' ||
                (message.kind === 'stream_chunk' &&
                  typeof message.sequence === 'bigint' &&
                  !acknowledged.has(message.sequence))),
          );
          if (item.kind === 'stream_terminal') {
            const readaheadWasteBytes =
              item.metadata?.response?.metrics?.range_readahead_wasted_bytes;
            if (typeof readaheadWasteBytes !== 'number') {
              throw new Error('successful cursor terminal omitted readahead waste metrics');
            }
            return {
              dataChunksBeforeTerminal,
              readaheadWasteBytes,
              status: item.metadata?.status,
            };
          }
          if (
            typeof item.sequence !== 'bigint' ||
            !(item.bytes instanceof Uint8Array) ||
            (item.phase !== 'schema' && item.phase !== 'data' && item.phase !== 'end_of_stream')
          ) {
            throw new Error('successful single-partition query received an invalid chunk');
          }
          acknowledged.add(item.sequence);
          if (item.phase === 'data') dataChunksBeforeTerminal += 1;
          worker.postMessage({
            kind: 'credit',
            version: 1,
            query_id: queryId,
            sequence: item.sequence,
            credit_class: item.phase === 'data' ? 'data' : 'control',
            bytes: item.bytes.byteLength,
          });
        }
      };

      const lateFailureProbe = async (): Promise<{
        dataChunksBeforeFailure: number;
        status: string | undefined;
      }> => {
        const queryId = 'private-late-failure';
        worker.postMessage({
          kind: 'command',
          version: 1,
          command: {
            sql: {
              request_id: queryId,
              name: 'events',
              query: {
                table_uri: new URL('/fixtures/browser-datafusion-runtime/table', location.href)
                  .href,
                snapshot_version: 0,
                sql: [
                  "SELECT CAST(CASE WHEN category = 'good'",
                  "THEN CAST(id AS VARCHAR) ELSE 'not-an-int' END AS BIGINT) AS id",
                  'FROM events',
                ].join(' '),
                preferred_target: 'browser_wasm',
                options: {},
              },
              output: 'arrow_ipc_stream',
              delivery: 'chunked_buffers',
              browser_safe_defaults: true,
            },
          },
        });

        const acknowledged = new Set<bigint>();
        let dataChunksBeforeFailure = 0;
        for (;;) {
          const item = await waitFor(
            (message) =>
              message.query_id === queryId &&
              (message.kind === 'stream_terminal' ||
                message.kind === 'stream_start_failed' ||
                (message.kind === 'stream_chunk' &&
                  typeof message.sequence === 'bigint' &&
                  !acknowledged.has(message.sequence))),
          );
          if (item.kind === 'stream_start_failed') {
            throw new Error(
              `late-failure query did not establish a stream: ${JSON.stringify(item)}`,
            );
          }
          if (item.kind === 'stream_terminal') {
            return { dataChunksBeforeFailure, status: item.metadata?.status };
          }
          if (
            typeof item.sequence !== 'bigint' ||
            !(item.bytes instanceof Uint8Array) ||
            (item.phase !== 'schema' && item.phase !== 'data' && item.phase !== 'end_of_stream')
          ) {
            throw new Error('late-failure query received an invalid chunk');
          }
          acknowledged.add(item.sequence);
          if (item.phase === 'data') dataChunksBeforeFailure += 1;
          worker.postMessage({
            kind: 'credit',
            version: 1,
            query_id: queryId,
            sequence: item.sequence,
            credit_class: item.phase === 'data' ? 'data' : 'control',
            bytes: item.bytes.byteLength,
          });
        }
      };

      try {
        return {
          cancelled: await probe('private-zero-credit-cancel', 'cancelled'),
          deadline: await probe('private-zero-credit-deadline', 'deadline_exceeded'),
          successfulSinglePartition: await successfulSinglePartitionProbe(),
          lateFailure: await lateFailureProbe(),
        };
      } finally {
        worker.terminate();
      }
    },
    { fixtureBytes: BINARY_STRING_INT_PARQUET_BYTES.byteLength },
  );

  expect(result.cancelled).toEqual({
    exactBuffer: true,
    plateaued: true,
    senderDetached: true,
    status: 'cancelled',
    transportCounters: ['bigint', 'bigint', 'bigint', 'bigint', 'bigint'],
  });
  expect(result.deadline).toEqual({
    exactBuffer: true,
    plateaued: true,
    senderDetached: true,
    status: 'deadline_exceeded',
    transportCounters: ['bigint', 'bigint', 'bigint', 'bigint', 'bigint'],
  });
  expect(result.successfulSinglePartition.status).toBe('succeeded');
  expect(result.successfulSinglePartition.dataChunksBeforeTerminal).toBeGreaterThan(0);
  expect(result.successfulSinglePartition.readaheadWasteBytes).toBeLessThanOrEqual(512 * 1024);
  expect(result.lateFailure).toEqual({
    dataChunksBeforeFailure: 1,
    status: 'failed',
  });
});

test('one-child coordinator discards staged data after a late failure', async ({
  page,
  browserName,
}) => {
  await routeCursorParquet(page);
  let childLoads = 0;
  if (browserName === 'firefox') {
    page.on('worker', (worker) => {
      if (worker.url().includes('sandbox-query-child-worker')) childLoads += 1;
    });
  } else {
    await page.route('**/sandbox-query-child-worker.ts*', async (route) => {
      childLoads += 1;
      await route.continue();
    });
  }
  await page.goto('/');

  const result = await page.evaluate(
    async ({ fixtureBytes }: { fixtureBytes: number }) => {
      type PublicMessage = {
        arrow_ipc_chunk?: unknown;
        error?: { request_id?: string };
        success?: { request_id?: string };
      };
      type CapturedError = {
        message: string;
        name: string;
        queryError?: unknown;
      };

      const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
      const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
        type: 'module',
      });
      const rawMessages: PublicMessage[] = [];
      const publicEvents: string[] = [];
      worker.addEventListener('message', (event: MessageEvent<PublicMessage>) => {
        rawMessages.push(event.data);
      });
      const client = sdk.createAxonBrowserClient({
        worker,
        onEvent: (event: unknown) => publicEvents.push(JSON.stringify(event)),
      });
      const tableUri = new URL('/fixtures/browser-datafusion-runtime/table', location.href).href;
      const fixtureUrl = new URL(
        '/fixtures/browser-datafusion-runtime/internal-cursor.parquet',
        location.href,
      ).href;

      try {
        await client.openDeltaTable(
          'events',
          {
            table_uri: tableUri,
            snapshot_version: 0,
            partition_column_types: { category: 'string' },
            browser_compatibility: { capabilities: {} },
            required_capabilities: { capabilities: {} },
            active_files: [
              {
                path: 'category=0-good/part-000.parquet',
                url: fixtureUrl,
                size_bytes: fixtureBytes,
                partition_values: { category: 'good' },
              },
              {
                path: 'category=1-bad/part-001.parquet',
                url: fixtureUrl,
                size_bytes: fixtureBytes,
                partition_values: { category: 'bad' },
              },
            ],
          },
          { requestId: 'open-coordinator-rollback' },
        );

        let capturedError: CapturedError | undefined;
        try {
          await client.query(
            'events',
            [
              "SELECT CAST(CASE WHEN category = 'good'",
              "THEN CAST(id AS VARCHAR) ELSE 'not-an-int' END AS BIGINT) AS id",
              'FROM events',
            ].join(' '),
            { requestId: 'query-coordinator-rollback' },
          );
        } catch (error) {
          const candidate = error as Partial<CapturedError>;
          capturedError = {
            message: String(candidate.message),
            name: String(candidate.name),
            queryError: candidate.queryError,
          };
        }
        if (!capturedError)
          throw new Error('late-failure coordinator query unexpectedly succeeded');
        await new Promise((resolve) => setTimeout(resolve, 50));

        return {
          error: capturedError,
          publicArrowChunks: rawMessages.filter((message) => message.arrow_ipc_chunk).length,
          publicErrors: rawMessages.filter(
            (message) => message.error?.request_id === 'query-coordinator-rollback',
          ).length,
          publicSuccesses: rawMessages.filter(
            (message) => message.success?.request_id === 'query-coordinator-rollback',
          ).length,
          leakedPrivateProtocol: publicEvents.some(
            (event) =>
              event.includes('logical_batch_sequence') ||
              event.includes('fragment_index') ||
              event.includes('stream_chunk'),
          ),
        };
      } finally {
        client.terminate();
      }
    },
    { fixtureBytes: BINARY_STRING_INT_PARQUET_BYTES.byteLength },
  );

  expect(result).toMatchObject({
    error: {
      name: 'AxonWorkerError',
      queryError: { code: 'execution_failed', target: 'browser_wasm' },
    },
    leakedPrivateProtocol: false,
    publicArrowChunks: 0,
    publicErrors: 1,
    publicSuccesses: 0,
  });
  expect(childLoads).toBe(1);
});

test('one-child coordinator fails a forwarded command once when the child crashes', async ({
  page,
  browserName,
}) => {
  let childLoads = 0;
  const isFirefox = browserName === 'firefox';
  const childWorkerPromise = isFirefox
    ? page.waitForEvent('worker', {
        predicate: (worker) => worker.url().includes('sandbox-query-child-worker'),
      })
    : undefined;
  if (isFirefox) {
    page.on('worker', (worker) => {
      if (worker.url().includes('sandbox-query-child-worker')) childLoads += 1;
    });
  } else {
    await page.route('**/sandbox-query-child-worker.ts*', async (route) => {
      childLoads += 1;
      if (childLoads === 1) {
        await route.fulfill({
          status: 200,
          contentType: 'application/javascript',
          body: "throw new Error('injected child worker crash');",
        });
        return;
      }
      await route.continue();
    });
  }
  await page.goto('/');

  const resultPromise = page.evaluate(async () => {
    type PublicMessage = { error?: { request_id?: string } };
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const rawMessages: PublicMessage[] = [];
    worker.addEventListener('message', (event: MessageEvent<PublicMessage>) => {
      rawMessages.push(event.data);
    });
    const client = sdk.createAxonBrowserClient({ worker });

    try {
      await client.openDeltaTable(
        'child_crash_probe',
        {
          table_uri: new URL('/fixtures/browser-datafusion-runtime/child-crash', location.href)
            .href,
          snapshot_version: 0,
          partition_column_types: {},
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          active_files: [],
        },
        { requestId: 'open-child-crash-probe' },
      );
      throw new Error('child crash probe unexpectedly succeeded');
    } catch (error) {
      const candidate = error as {
        message?: string;
        name?: string;
        queryError?: unknown;
      };
      await new Promise((resolve) => setTimeout(resolve, 50));
      return {
        error: {
          message: String(candidate.message),
          name: String(candidate.name),
          queryError: candidate.queryError,
        },
        publicErrors: rawMessages.filter(
          (message) => message.error?.request_id === 'open-child-crash-probe',
        ).length,
      };
    } finally {
      client.terminate();
    }
  });
  if (childWorkerPromise) {
    const childWorker = await childWorkerPromise;
    await childWorker.evaluate(() => {
      setTimeout(() => {
        throw new Error('injected child worker crash');
      }, 0);
    });
  }
  const result = await resultPromise;

  expect(result).toMatchObject({
    error: {
      name: 'AxonWorkerError',
      queryError: { code: 'execution_failed', target: 'browser_wasm' },
    },
    publicErrors: 1,
  });
  expect(childLoads).toBeGreaterThanOrEqual(2);
});

test('one-child coordinator bounds queued SQL and recycles a non-settling child', async ({
  page,
  browserName,
}) => {
  test.skip(browserName === 'firefox', 'Firefox cannot route nested worker module requests');
  let childLoads = 0;
  await page.route('**/sandbox-query-child-worker.ts*', async (route) => {
    childLoads += 1;
    if (childLoads === 1) {
      await route.fulfill({
        status: 200,
        contentType: 'application/javascript',
        body: NON_SETTLING_CHILD_SOURCE,
      });
      return;
    }
    await route.continue();
  });
  await page.goto('/');

  const result = await page.evaluate(async () => {
    type CapturedError = {
      code?: string;
      message: string;
      name: string;
    };
    const sdk = await import(new URL('/src/axon-browser-sdk.ts', location.href).href);
    const worker = new Worker(new URL('/src/sandbox-query-worker.ts', location.href), {
      type: 'module',
    });
    const client = sdk.createAxonBrowserClient({ worker });

    try {
      await client.openDeltaTable(
        'blocked',
        {
          table_uri: new URL('/fixtures/browser-datafusion-runtime/non-settling', location.href)
            .href,
          snapshot_version: 0,
          partition_column_types: {},
          browser_compatibility: { capabilities: {} },
          required_capabilities: { capabilities: {} },
          active_files: [],
        },
        { requestId: 'open-non-settling' },
      );

      const attempts = Array.from({ length: 33 }, (_, index) =>
        client.query('blocked', 'SELECT 1', { requestId: `blocked-query-${index}` }).then(
          () => ({ ok: true as const }),
          (error: unknown) => {
            const candidate = error as {
              message?: string;
              name?: string;
              queryError?: { code?: string; message?: string };
            };
            return {
              ok: false as const,
              error: {
                code: candidate.queryError?.code,
                message: String(candidate.queryError?.message ?? candidate.message),
                name: String(candidate.name),
              } satisfies CapturedError,
            };
          },
        ),
      );
      client.cancelQuery('blocked-query-0', { requestId: 'cancel-non-settling' });
      const settlements = await Promise.race([
        Promise.all(attempts),
        new Promise<null>((resolve) => setTimeout(() => resolve(null), 3_000)),
      ]);
      if (!settlements) return { timedOut: true, errors: [] as CapturedError[] };
      return {
        timedOut: false,
        errors: settlements.flatMap((settlement) => (settlement.ok ? [] : [settlement.error])),
      };
    } finally {
      client.terminate();
    }
  });

  expect(result.timedOut).toBe(false);
  expect(result.errors).toHaveLength(33);
  expect(
    result.errors.filter(
      (error) => error.code === 'invalid_request' && error.message.includes('coordinator capacity'),
    ),
  ).toHaveLength(1);
  expect(
    result.errors.filter((error) =>
      error.message.startsWith('experimental browser DataFusion query cancelled'),
    ),
  ).toHaveLength(1);
  expect(
    result.errors.filter((error) => error.message.includes('browser query session invalidated:')),
  ).toHaveLength(32);
  expect(childLoads).toBeGreaterThanOrEqual(2);
});

test('editor query session reopens its table after a child crash', async ({
  page,
  browserName,
}) => {
  test.skip(browserName === 'firefox', 'Firefox cannot route nested worker module requests');
  let childLoads = 0;
  await page.route('**/sandbox-query-child-worker.ts*', async (route) => {
    childLoads += 1;
    if (childLoads === 1) {
      await route.fulfill({
        status: 200,
        contentType: 'application/javascript',
        body: CRASH_ON_SQL_CHILD_SOURCE,
      });
      return;
    }
    await route.continue();
  });
  await page.goto('/');

  const result = await page.evaluate(async () => {
    const query = await import(new URL('/src/services/query.ts', location.href).href);
    const sourceModule = await import(new URL('/src/services/query-source.ts', location.href).href);
    const keys = await import(new URL('/src/query/keys.ts', location.href).href);
    const source = sourceModule.SAMPLE_QUERY_SOURCE;
    const selection = {
      kind: 'sample' as const,
      ref: sourceModule.SAMPLE_QUERY_SOURCE_REF,
      source,
    };
    const sourceIdentity = keys.selectedQuerySourceIdentity(selection);
    const admission = (executionId: string) => ({
      executionId,
      sourceIdentity,
      sql: 'SELECT COUNT(*) AS value FROM events',
      target: 'browser_wasm' as const,
      deadlineAt: Date.now() + 30_000,
      budgets: {
        maxResultRows: 501,
        maxArrowIpcBytes: 8 * 1024 * 1024,
        maxPreviewStringBytes: 256 * 1024,
        maxScanBytes: 64 * 1024 * 1024,
      },
    });

    query.discardQuerySession();
    try {
      const first = await query.runQuery(
        { sql: 'SELECT COUNT(*) AS value FROM events' },
        () => undefined,
        source,
        admission('child-crash-first'),
      );
      const second = await query.runQuery(
        { sql: 'SELECT COUNT(*) AS value FROM events' },
        () => undefined,
        source,
        admission('child-crash-second'),
      );
      return { first, second };
    } finally {
      query.discardQuerySession();
    }
  });

  expect(result.first).toMatchObject({
    status: 'error',
    code: 'execution_failed',
    message: expect.stringContaining('browser query session invalidated:'),
  });
  expect(result.second, JSON.stringify(result)).toMatchObject({ status: 'done' });
  expect(childLoads).toBeGreaterThanOrEqual(3);
});

async function routeCursorParquet(page: Page): Promise<void> {
  let requests = 0;
  let responseBytes = 0;
  await page.exposeFunction('cursorRequestStats', () => ({ requests, responseBytes }));
  await page.route(BINARY_STRING_INT_PARQUET_PATH, async (route) => {
    requests += 1;
    const totalLength = BINARY_STRING_INT_PARQUET_BYTES.byteLength;
    const range = route.request().headers().range;
    if (!range) {
      responseBytes += totalLength;
      await route.fulfill({
        status: 200,
        body: BINARY_STRING_INT_PARQUET_BYTES,
        headers: {
          'accept-ranges': 'bytes',
          'content-length': String(totalLength),
          'content-type': 'application/octet-stream',
        },
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
        body: '',
        headers: { 'content-range': `bytes */${totalLength}` },
      });
      return;
    }
    responseBytes += end - start + 1;
    await route.fulfill({
      status: 206,
      body: BINARY_STRING_INT_PARQUET_BYTES.subarray(start, end + 1),
      headers: {
        'accept-ranges': 'bytes',
        'content-length': String(end - start + 1),
        'content-range': `bytes ${start}-${end}/${totalLength}`,
        'content-type': 'application/octet-stream',
      },
    });
  });
}
