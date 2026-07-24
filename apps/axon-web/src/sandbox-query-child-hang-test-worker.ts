import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
} from './sandbox-query-stream-protocol';

type HangTestScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<PrivateCoordinatorMessage>) => void,
  ): void;
  postMessage(message: PrivateChildMessage): void;
};

const scope = self as unknown as HangTestScope;

scope.addEventListener('message', () => {
  // Intentionally ignore every command and cancellation to exercise the coordinator watchdog.
});
scope.postMessage({ kind: 'ready', version: PRIVATE_STREAM_PROTOCOL_VERSION });
