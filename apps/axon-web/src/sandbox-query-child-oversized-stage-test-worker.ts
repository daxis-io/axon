import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
} from './sandbox-query-stream-protocol';

type TestWorkerScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<PrivateCoordinatorMessage>) => void,
  ): void;
  postMessage(message: PrivateChildMessage, transfer?: Transferable[]): void;
};

const scope = self as unknown as TestWorkerScope;

scope.postMessage({ kind: 'ready', version: PRIVATE_STREAM_PROTOCOL_VERSION });
scope.addEventListener('message', (event) => {
  const message = event.data;
  if (message.kind !== 'command' || !('sql' in message.command)) return;
  const bytes = new Uint8Array(2);
  scope.postMessage(
    {
      kind: 'stream_chunk',
      version: PRIVATE_STREAM_PROTOCOL_VERSION,
      query_id: message.command.sql.request_id,
      sequence: 0n,
      phase: 'schema',
      logical_batch_sequence: null,
      fragment_index: 0n,
      end_of_logical_batch: true,
      rows_completed: 0n,
      byte_length: 2n,
      bytes,
    },
    [bytes.buffer],
  );
});
