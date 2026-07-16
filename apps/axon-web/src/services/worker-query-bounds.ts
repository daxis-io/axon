import type { ExecutionTarget, QueryErrorCode } from '../axon-browser-sdk.ts';

class QueryOutputBoundsError extends Error {
  readonly code: QueryErrorCode = 'execution_failed';
  readonly target: ExecutionTarget = 'browser_wasm';
}

type ValidatedArrowIpcOutput = Readonly<{
  byteLength: number;
}>;

export function withValidatedArrowIpcOutput<TResult>(
  bytes: Uint8Array,
  reportedByteLength: number,
  maxArrowIpcBytes: number,
  publish: (output: ValidatedArrowIpcOutput) => TResult,
): TResult {
  const actualByteLength = bytes.byteLength;
  if (!Number.isSafeInteger(reportedByteLength) || reportedByteLength < 0) {
    throw new QueryOutputBoundsError(
      `reported Arrow IPC byte length must be a non-negative safe integer, got ${String(reportedByteLength)}`,
    );
  }
  if (!Number.isSafeInteger(maxArrowIpcBytes) || maxArrowIpcBytes <= 0) {
    throw new QueryOutputBoundsError(
      `runtime_limits.max_arrow_ipc_bytes must be a positive safe integer, got ${String(maxArrowIpcBytes)}`,
    );
  }
  if (reportedByteLength !== actualByteLength) {
    throw new QueryOutputBoundsError(
      `reported Arrow IPC byte length ${reportedByteLength} did not match actual ${actualByteLength}`,
    );
  }
  if (actualByteLength > maxArrowIpcBytes) {
    throw new QueryOutputBoundsError(
      `resource limit runtime_limits.max_arrow_ipc_bytes exceeded: actual ${actualByteLength}, limit ${maxArrowIpcBytes}`,
    );
  }
  return publish(Object.freeze({ byteLength: actualByteLength }));
}
