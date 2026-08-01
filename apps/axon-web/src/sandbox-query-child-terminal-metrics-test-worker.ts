import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
  type PrivateTerminalMetadata,
  type PrivateTerminalStatus,
} from './sandbox-query-stream-protocol.ts';

type TestScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<PrivateCoordinatorMessage>) => void,
  ): void;
  postMessage(message: PrivateChildMessage): void;
};

const scope = self as unknown as TestScope;
let queryId: string | undefined;
let failureTimer: ReturnType<typeof setTimeout> | undefined;

scope.addEventListener('message', (event) => {
  const message = event.data;
  if (message.kind === 'command' && 'sql' in message.command) {
    queryId = message.command.sql.request_id;
    failureTimer = setTimeout(() => {
      postTerminal('failed');
    }, 100);
    return;
  }
  if (message.kind === 'cancel' && message.query_id === queryId) {
    postTerminal(message.reason);
  }
});

post({ kind: 'ready', version: PRIVATE_STREAM_PROTOCOL_VERSION });

function postTerminal(status: Exclude<PrivateTerminalStatus, 'succeeded'>): void {
  if (!queryId) return;
  if (failureTimer) clearTimeout(failureTimer);
  const currentQueryId = queryId;
  queryId = undefined;
  const metadata: PrivateTerminalMetadata = {
    metadata_version: 1,
    status,
    ...(status === 'failed'
      ? {
          error: {
            code: 'resource_exhausted',
            message: 'injected post-cleanup spill failure',
            target: 'browser_wasm',
            resource_details: {
              resource: 'spill_storage',
              reason: 'io_failure',
            },
          } as const,
        }
      : {}),
    arrow_ipc_byte_length: '0',
    row_count: '0',
    datafusion_memory: {
      limit_bytes: '67108864',
      reserved_bytes: '0',
      peak_bytes: '62914560',
    },
    external_memory: {
      backend: 'opfs',
      storage_limit_bytes: '603979776',
      bytes_written: '4096',
      bytes_read: '1024',
      files_created: '2',
      peak_active_bytes: '4096',
      active_files: '0',
      merge_passes: '1',
      cleanup_count: '1',
      abandoned_cleanup_count: '0',
      working_set_limit_bytes: '67108864',
      peak_reservation_bytes: '62914560',
      ...(status === 'failed' ? { error_reason: 'io_failure' as const } : {}),
    },
  };
  post({
    kind: 'stream_terminal',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: currentQueryId,
    metadata,
  });
}

function post(message: PrivateChildMessage): void {
  scope.postMessage(message);
}
