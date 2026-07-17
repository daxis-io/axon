import {
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
} from './sandbox-query-stream-protocol';

type HarnessScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<PrivateCoordinatorMessage>) => void,
  ): void;
  postMessage(message: PrivateChildMessage | HarnessTransferAudit, transfer?: Transferable[]): void;
};

type HarnessTransferAudit = {
  kind: 'transfer_audit';
  query_id: string;
  sequence: bigint;
  sender_detached: boolean;
};

const scope = self as unknown as HarnessScope;
const child = new Worker(new URL('./sandbox-query-child-worker.ts', import.meta.url), {
  type: 'module',
  name: 'axon-private-child-test-harness',
});

scope.addEventListener('message', (event) => {
  child.postMessage(event.data);
});

child.addEventListener('message', (event: MessageEvent<PrivateChildMessage>) => {
  const message = event.data;
  if (message.kind !== 'stream_chunk') {
    scope.postMessage(message);
    return;
  }
  const transferredBuffer = message.bytes.buffer;
  scope.postMessage(message, [transferredBuffer]);
  scope.postMessage({
    kind: 'transfer_audit',
    query_id: message.query_id,
    sequence: message.sequence,
    sender_detached: transferredBuffer.byteLength === 0,
  });
});

child.addEventListener('error', (event) => {
  throw new Error(event.message || 'private child test harness observed a worker error');
});
