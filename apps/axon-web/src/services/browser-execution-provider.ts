import { equals } from '@bufbuild/protobuf';
import { timestampMs } from '@bufbuild/protobuf/wkt';
import {
  CanonicalResourceRefSchema,
  ResourceKind,
} from '../generated/contracts/protobuf/axon/common/v1/common_pb.ts';
import {
  BrowserAccessClass,
  type BrowserHttpSnapshotDescriptor,
  type ResolvedBrowserRead,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ExecutionRejectionReason,
  type CancelRequest,
  type CancelResponse,
  type ExecuteRequest,
  type ExecuteResponse,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import type { TableNode } from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';

export type BrowserExecuteInput = Readonly<{
  table: TableNode;
  request: ExecuteRequest;
}>;

export interface ExecutionProvider {
  execute(input: BrowserExecuteInput): AsyncIterable<ExecuteResponse>;
  cancel(request: CancelRequest): CancelResponse;
}

export class BrowserExecutionValidationError extends Error {
  readonly name = 'BrowserExecutionValidationError';

  constructor(
    readonly reason: ExecutionRejectionReason,
    message: string,
  ) {
    super(message);
  }
}

export type BrowserExecutionValidationOptions = Readonly<{
  now?: () => number;
  isCurrentLocalObjectUrl?: (registryId: string, url: string) => boolean;
}>;

export function createValidatedBrowserExecutionProvider(
  delegate: ExecutionProvider,
  options: BrowserExecutionValidationOptions = {},
): ExecutionProvider {
  return {
    async *execute(input) {
      validateBrowserExecuteInput(input, options);
      yield* delegate.execute(input);
    },
    cancel(request) {
      return delegate.cancel(request);
    },
  };
}

export function validateBrowserExecuteInput(
  input: BrowserExecuteInput,
  options: BrowserExecutionValidationOptions = {},
): ResolvedBrowserRead {
  const { table, request } = input;
  const now = options.now?.() ?? Date.now();
  if (!request.executionId.trim()) {
    invalid('execution ID is required');
  }
  if (!table.name.trim() || !table.resource) {
    invalid('table name and canonical resource are required');
  }
  if (!request.query || !request.query.sql.trim()) {
    invalid('query is required');
  }
  const deadline = requiredTimestampMs(request.deadline, 'execution deadline');
  if (deadline <= now) {
    reject(ExecutionRejectionReason.DEADLINE_EXPIRED, 'execution deadline has expired');
  }
  if (request.binding.case !== 'browserRead') {
    invalid('browser execution requires exactly one browser-read binding');
  }

  const read = request.binding.value;
  if (!read.resource || !equals(CanonicalResourceRefSchema, table.resource, read.resource)) {
    invalid('table resource and browser-read resource must match exactly');
  }
  if (
    read.resource.kind !== ResourceKind.TABLE ||
    !read.resource.connectionId.trim() ||
    !read.resource.providerNamespace.trim() ||
    read.resource.identity.case === undefined
  ) {
    invalid('browser-read resource identity is incomplete');
  }
  if (read.correlationId !== request.executionId) {
    invalid('browser-read correlation must match the execution ID');
  }
  if (!read.provenance?.resolverId.trim() || !read.provenance.resolutionId.trim()) {
    invalid('browser-read resolution provenance is required');
  }
  if (read.descriptor?.descriptor.case !== 'snapshot') {
    reject(
      ExecutionRejectionReason.UNSUPPORTED,
      'browser execution in this slice requires a Delta snapshot descriptor',
    );
  }

  const descriptor = read.descriptor.descriptor.value;
  validateSnapshotDescriptor(descriptor);
  switch (read.accessClass) {
    case BrowserAccessClass.LOCAL_HANDLE:
      validateLocalRead(read, descriptor, deadline, now, options);
      break;
    case BrowserAccessClass.PUBLIC:
      if (read.notAfter) {
        invalid('public browser reads must not carry an expiry');
      }
      break;
    default:
      reject(
        ExecutionRejectionReason.ACCESS_DENIED,
        'browser-read access class is unavailable to the local executor',
      );
  }
  return read;
}

function validateLocalRead(
  read: ResolvedBrowserRead,
  descriptor: BrowserHttpSnapshotDescriptor,
  deadline: number,
  now: number,
  options: BrowserExecutionValidationOptions,
): void {
  if (
    read.resource?.providerNamespace !== 'axon.local-delta/v1' ||
    read.resource.identity.case !== 'providerObjectId' ||
    !read.resource.identity.value.trim()
  ) {
    invalid('local browser reads require one registry-backed canonical identity');
  }
  const notAfter = requiredTimestampMs(read.notAfter, 'local access lifetime');
  if (notAfter !== deadline) {
    invalid('local access lifetime must equal the execution deadline');
  }
  if (notAfter <= now) {
    reject(ExecutionRejectionReason.ACCESS_DENIED, 'local access lifetime has expired');
  }
  const registryId = read.resource.identity.value;
  for (const file of descriptor.activeFiles) {
    if (
      !file.url.startsWith('blob:') ||
      options.isCurrentLocalObjectUrl?.(registryId, file.url) !== true
    ) {
      reject(
        ExecutionRejectionReason.ACCESS_DENIED,
        'local descriptor contains an unavailable Blob URL',
      );
    }
  }
}

function validateSnapshotDescriptor(descriptor: BrowserHttpSnapshotDescriptor): void {
  if (!descriptor.tableUri.trim() || descriptor.snapshotVersion === undefined) {
    invalid('snapshot descriptor identity is incomplete');
  }
  if (descriptor.snapshotVersion < 0n) {
    invalid('snapshot version must be non-negative');
  }
  for (const file of descriptor.activeFiles) {
    if (!file.path.trim() || !file.url.trim() || file.sizeBytes < 0n) {
      invalid('snapshot descriptor contains an invalid active file');
    }
  }
}

function requiredTimestampMs(
  timestamp: ExecuteRequest['deadline'] | ResolvedBrowserRead['notAfter'],
  field: string,
): number {
  if (!timestamp) invalid(`${field} is required`);
  const value = timestampMs(timestamp);
  if (!Number.isSafeInteger(value) || value < 0) {
    invalid(`${field} is invalid`);
  }
  return value;
}

function invalid(message: string): never {
  return reject(ExecutionRejectionReason.INVALID_REQUEST, message);
}

function reject(reason: ExecutionRejectionReason, message: string): never {
  throw new BrowserExecutionValidationError(reason, message);
}
