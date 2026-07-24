import { clone, create, equals } from '@bufbuild/protobuf';
import { timestampFromMs, timestampMs, type Timestamp } from '@bufbuild/protobuf/wkt';
import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
} from '../axon-browser-sdk.ts';
import {
  CancelRequestSchema,
  CancelResponseSchema,
  ExecuteRequestSchema,
  ExecutionAcceptedSchema,
  ExecutionAdmissionSchema,
  ExecutionCancelledSchema,
  ExecutionCompletedSchema,
  ExecutionFailedSchema,
  ExecutionLifecycleState as ContractExecutionLifecycleState,
  ExecutionRejectedSchema,
  ExecutionRejectionReason,
  ExecutionTarget,
  ExecutionTerminalFrameSchema,
  ExecutionTerminalStateSchema,
  type CancelRequest,
  type CancelResponse,
  type ExecuteRequest,
  type ExecutionAdmission,
  type ExecutionTerminalFrame,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';

export type ExecutionLifecycleState =
  | 'created'
  | 'running'
  | 'cancel_requested'
  | 'rejected'
  | 'completed'
  | 'failed'
  | 'cancelled';

export type ExecutionTerminalState = Extract<
  ExecutionLifecycleState,
  'completed' | 'failed' | 'cancelled'
>;

export type ExecutionSnapshot = Readonly<{
  executionId: string;
  state: ExecutionLifecycleState;
  request?: ExecuteRequest;
  admitted: boolean;
  rejectionReason?: ExecutionRejectionReason;
  terminalReason?: string;
  terminalFrame?: ExecutionTerminalFrame;
  invariantViolations: readonly string[];
}>;

export type ExecutionFrameDelivery = Readonly<{
  kind: 'frame';
  sequence: number;
  payload: unknown;
}>;

export type ExecutionTerminalDelivery = Readonly<{
  kind: 'terminal';
  sequence: number;
  state: ExecutionTerminalState;
  frame: ExecutionTerminalFrame;
  reason?: string;
  payload?: unknown;
}>;

export type ExecutionDelivery = ExecutionFrameDelivery | ExecutionTerminalDelivery;
export type ExecutionListener = (delivery: ExecutionDelivery) => void;
export type ExecutionCancellationHandle = () => void;

const MAX_EXECUTION_LISTENERS = 16;
const MAX_INVARIANT_VIOLATIONS = 32;

export type ExecutionRegistrationResult =
  | { kind: 'created'; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; snapshot: ExecutionSnapshot }
  | { kind: 'id_reuse'; snapshot: ExecutionSnapshot }
  | { kind: 'rejected'; reason: ExecutionRejectionReason.CAPACITY };

export type ExecutionAdmissionResult = Readonly<{
  admission: ExecutionAdmission;
  snapshot?: ExecutionSnapshot;
}>;

export type ExecutionPublishResult =
  | { kind: 'published'; sequence: number; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; snapshot: ExecutionSnapshot }
  | { kind: 'unknown' };

export type ExecutionTerminalResult =
  | { kind: 'transitioned'; delivered: true; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; delivered: false; snapshot: ExecutionSnapshot }
  | { kind: 'unknown'; delivered: false };

type ExecutionRecord = {
  executionId: string;
  deadline: Timestamp;
  request?: ExecuteRequest;
  state: ContractExecutionLifecycleState;
  admitted: boolean;
  rejectionReason?: ExecutionRejectionReason;
  terminalReason?: string;
  terminalFrame?: ExecutionTerminalFrame;
  invariantViolations: string[];
  listeners: Set<ExecutionListener>;
  cancellationHandle?: ExecutionCancellationHandle;
  cancellationInvoked: boolean;
  sequence: number;
  deadlineProcessed: boolean;
};

export type ExecutionLifecycleOptions = {
  maxRecords?: number;
};

export class ExecutionLifecycle {
  readonly #maxRecords: number;
  readonly #records = new Map<string, ExecutionRecord>();

  constructor(options: ExecutionLifecycleOptions = {}) {
    const maxRecords = options.maxRecords ?? 256;
    if (!Number.isSafeInteger(maxRecords) || maxRecords < 1) {
      throw new Error('execution lifecycle maxRecords must be a positive safe integer');
    }
    this.#maxRecords = maxRecords;
  }

  get recordCount(): number {
    return this.#records.size;
  }

  reserve(executionId: string, deadline: Timestamp): ExecutionRegistrationResult {
    const existing = this.#records.get(executionId);
    if (existing) {
      if (timestampMs(existing.deadline) !== timestampMs(deadline)) {
        return { kind: 'id_reuse', snapshot: snapshot(existing) };
      }
      return { kind: 'recorded', snapshot: snapshot(existing) };
    }

    if (this.#records.size >= this.#maxRecords) {
      return { kind: 'rejected', reason: ExecutionRejectionReason.CAPACITY };
    }

    const record: ExecutionRecord = {
      executionId,
      deadline: cloneTimestamp(deadline),
      state: ContractExecutionLifecycleState.CREATED,
      admitted: false,
      invariantViolations: [],
      listeners: new Set(),
      cancellationInvoked: false,
      sequence: 0,
      deadlineProcessed: false,
    };
    this.#records.set(executionId, record);
    return { kind: 'created', snapshot: snapshot(record) };
  }

  admit(request: ExecuteRequest, now = Date.now()): ExecutionAdmissionResult {
    let record = this.#records.get(request.executionId);
    if (!record) {
      if (!request.deadline) {
        return rejectedAdmission(
          request.executionId,
          ExecutionRejectionReason.INVALID_REQUEST,
          'execution deadline is required',
        );
      }
      const registration = this.reserve(request.executionId, request.deadline);
      if (registration.kind === 'rejected') {
        return rejectedAdmission(
          request.executionId,
          ExecutionRejectionReason.CAPACITY,
          'execution lifecycle capacity is exhausted',
        );
      }
      if (registration.kind === 'id_reuse') {
        return rejectedAdmission(
          request.executionId,
          ExecutionRejectionReason.EXECUTION_ID_REUSE,
          'execution ID was already reserved with a different deadline',
          registration.snapshot,
        );
      }
      record = this.#records.get(request.executionId);
    }
    if (!record) throw new Error('execution lifecycle lost a reserved admission');

    if (record.request && !equals(ExecuteRequestSchema, record.request, request)) {
      return rejectedAdmission(
        request.executionId,
        ExecutionRejectionReason.EXECUTION_ID_REUSE,
        'execution ID was already used with a different immutable request',
        snapshot(record),
      );
    }
    if (!request.deadline || timestampMs(record.deadline) !== timestampMs(request.deadline)) {
      return rejectedAdmission(
        request.executionId,
        ExecutionRejectionReason.EXECUTION_ID_REUSE,
        'resolved request deadline did not match its caller reservation',
        snapshot(record),
      );
    }
    record.request ??= clone(ExecuteRequestSchema, request);

    if (record.state === ContractExecutionLifecycleState.REJECTED) {
      return rejectedAdmission(
        request.executionId,
        record.rejectionReason ?? ExecutionRejectionReason.INVALID_REQUEST,
        rejectionMessage(record.rejectionReason),
        snapshot(record),
      );
    }

    const rejection = admissionRejection(request, now);
    if (record.state === ContractExecutionLifecycleState.CREATED && rejection) {
      record.state = ContractExecutionLifecycleState.REJECTED;
      record.rejectionReason = rejection.reason;
      record.cancellationHandle = undefined;
      record.listeners.clear();
      if (rejection.reason === ExecutionRejectionReason.DEADLINE_EXPIRED) {
        record.deadlineProcessed = true;
      }
      return rejectedAdmission(
        request.executionId,
        rejection.reason,
        rejection.message,
        snapshot(record),
      );
    }

    if (record.state === ContractExecutionLifecycleState.CREATED) {
      record.state = ContractExecutionLifecycleState.RUNNING;
      record.admitted = true;
      return acceptedAdmission(request.executionId, true, snapshot(record));
    }

    return acceptedAdmission(request.executionId, false, snapshot(record));
  }

  subscribe(executionId: string, listener: ExecutionListener): () => void {
    const record = this.#records.get(executionId);
    if (!record) return () => undefined;
    if (isTerminalOrRejected(record.state)) {
      recordInvariant(record, `listener attached after ${uiLifecycleState(record.state)}`);
      return () => undefined;
    }
    if (record.listeners.size >= MAX_EXECUTION_LISTENERS) {
      recordInvariant(record, 'execution listener capacity exceeded');
      return () => undefined;
    }
    record.listeners.add(listener);
    return () => {
      record.listeners.delete(listener);
    };
  }

  attachCancellation(
    executionId: string,
    handle: ExecutionCancellationHandle,
  ): ExecutionSnapshot | undefined {
    const record = this.#records.get(executionId);
    if (!record) return undefined;
    if (isTerminalOrRejected(record.state)) {
      recordInvariant(
        record,
        `cancellation handle attached after ${uiLifecycleState(record.state)}`,
      );
      return snapshot(record);
    }
    record.cancellationHandle = handle;
    if (record.state === ContractExecutionLifecycleState.CANCEL_REQUESTED) {
      this.#invokeCancellation(record);
    }
    return snapshot(record);
  }

  cancel(request: CancelRequest): CancelResponse {
    const record = this.#records.get(request.executionId);
    if (!record) {
      return create(CancelResponseSchema, {
        executionId: request.executionId,
        state: ContractExecutionLifecycleState.UNSPECIFIED,
      });
    }

    if (record.state === ContractExecutionLifecycleState.CREATED) {
      this.#invokeCancellation(record);
      record.state = ContractExecutionLifecycleState.REJECTED;
      record.rejectionReason = ExecutionRejectionReason.CANCELLED;
      record.cancellationHandle = undefined;
      record.listeners.clear();
    } else if (record.state === ContractExecutionLifecycleState.RUNNING) {
      record.state = ContractExecutionLifecycleState.CANCEL_REQUESTED;
      this.#invokeCancellation(record);
    } else if (
      record.state !== ContractExecutionLifecycleState.CANCEL_REQUESTED &&
      record.state !== ContractExecutionLifecycleState.REJECTED
    ) {
      recordInvariant(record, `cancellation requested after ${uiLifecycleState(record.state)}`);
    }

    return create(CancelResponseSchema, {
      executionId: request.executionId,
      state: record.state,
    });
  }

  reject(
    executionId: string,
    reason: ExecutionRejectionReason,
    message: string,
  ): ExecutionAdmissionResult {
    const record = this.#records.get(executionId);
    if (!record) return rejectedAdmission(executionId, reason, message);
    if (record.state === ContractExecutionLifecycleState.CREATED) {
      record.state = ContractExecutionLifecycleState.REJECTED;
      record.rejectionReason = reason;
      record.cancellationHandle = undefined;
      record.listeners.clear();
      return rejectedAdmission(executionId, reason, message, snapshot(record));
    }
    if (record.state === ContractExecutionLifecycleState.REJECTED) {
      return rejectedAdmission(
        executionId,
        record.rejectionReason ?? reason,
        rejectionMessage(record.rejectionReason),
        snapshot(record),
      );
    }
    return acceptedAdmission(executionId, false, snapshot(record));
  }

  publishFrame(executionId: string, payload: unknown): ExecutionPublishResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown' };
    if (
      record.state !== ContractExecutionLifecycleState.RUNNING &&
      record.state !== ContractExecutionLifecycleState.CANCEL_REQUESTED
    ) {
      recordInvariant(record, `frame published after ${uiLifecycleState(record.state)}`);
      return { kind: 'recorded', snapshot: snapshot(record) };
    }

    const sequence = ++record.sequence;
    this.#notify(record, Object.freeze({ kind: 'frame', sequence, payload }));
    return { kind: 'published', sequence, snapshot: snapshot(record) };
  }

  complete(executionId: string, payload?: unknown): ExecutionTerminalResult {
    return this.#terminal(executionId, 'completed', undefined, payload);
  }

  fail(executionId: string, reason: string, payload?: unknown): ExecutionTerminalResult {
    return this.#terminal(executionId, 'failed', reason, payload);
  }

  confirmCancelled(executionId: string, payload?: unknown): ExecutionTerminalResult {
    return this.#terminal(executionId, 'cancelled', undefined, payload);
  }

  processDeadline(executionId: string): ExecutionTerminalResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown', delivered: false };
    if (record.deadlineProcessed) {
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }
    record.deadlineProcessed = true;

    if (record.state === ContractExecutionLifecycleState.CREATED) {
      this.#invokeCancellation(record);
      record.state = ContractExecutionLifecycleState.REJECTED;
      record.rejectionReason = ExecutionRejectionReason.DEADLINE_EXPIRED;
      record.cancellationHandle = undefined;
      record.listeners.clear();
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }
    if (
      record.state === ContractExecutionLifecycleState.RUNNING ||
      record.state === ContractExecutionLifecycleState.CANCEL_REQUESTED
    ) {
      return this.#terminal(executionId, 'failed', 'deadline');
    }
    return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
  }

  sweep(now: number): string[] {
    const deleted: string[] = [];
    for (const [executionId, record] of this.#records) {
      if (!record.deadlineProcessed && timestampMs(record.deadline) <= now) {
        this.processDeadline(executionId);
      }
      if (!record.deadlineProcessed) continue;
      record.listeners.clear();
      record.cancellationHandle = undefined;
      this.#records.delete(executionId);
      deleted.push(executionId);
    }
    return deleted;
  }

  getSnapshot(executionId: string): ExecutionSnapshot | undefined {
    const record = this.#records.get(executionId);
    return record ? snapshot(record) : undefined;
  }

  #terminal(
    executionId: string,
    state: ExecutionTerminalState,
    reason?: string,
    payload?: unknown,
  ): ExecutionTerminalResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown', delivered: false };

    if (
      record.state !== ContractExecutionLifecycleState.RUNNING &&
      record.state !== ContractExecutionLifecycleState.CANCEL_REQUESTED
    ) {
      recordInvariant(record, `late ${state} after ${uiLifecycleState(record.state)}`);
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }

    record.state = contractLifecycleState(state);
    record.terminalReason = reason;
    if (reason === 'deadline') this.#invokeCancellation(record);
    const sequence = ++record.sequence;
    const frame = terminalFrame(record, state, sequence);
    record.terminalFrame = frame;
    const delivery: ExecutionTerminalDelivery = Object.freeze({
      kind: 'terminal',
      sequence,
      state,
      frame,
      ...(reason === undefined ? {} : { reason }),
      ...(payload === undefined ? {} : { payload }),
    });
    this.#notify(record, delivery);
    record.listeners.clear();
    record.cancellationHandle = undefined;
    return { kind: 'transitioned', delivered: true, snapshot: snapshot(record) };
  }

  #invokeCancellation(record: ExecutionRecord): void {
    if (record.cancellationInvoked || !record.cancellationHandle) return;
    record.cancellationInvoked = true;
    try {
      record.cancellationHandle();
    } catch {
      recordInvariant(record, 'cancellation handle threw');
    }
  }

  #notify(record: ExecutionRecord, delivery: ExecutionDelivery): void {
    for (const listener of [...record.listeners]) {
      try {
        listener(delivery);
      } catch {
        recordInvariant(record, 'execution listener threw');
      }
    }
  }
}

export type PrepareExecutionRequest = Readonly<{
  timeoutMs: number;
}>;

export type PreparedExecution = Readonly<{
  executionId: string;
  deadline: Timestamp;
}>;

export type PreparedExecutionResult =
  | {
      kind: 'created' | 'recorded';
      execution: PreparedExecution;
      snapshot: ExecutionSnapshot;
    }
  | {
      kind: 'rejected';
      reason: ExecutionRejectionReason.INVALID_REQUEST | ExecutionRejectionReason.CAPACITY;
    };

export type ExecutionTimerHandle = unknown;

export type ExecutionControllerDependencies = {
  lifecycle?: ExecutionLifecycle;
  idFactory?: () => string;
  now?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => ExecutionTimerHandle;
  clearTimer?: (handle: ExecutionTimerHandle) => void;
};

export class ExecutionController {
  readonly lifecycle: ExecutionLifecycle;
  readonly #idFactory: () => string;
  readonly #now: () => number;
  readonly #setTimer: (callback: () => void, delayMs: number) => ExecutionTimerHandle;
  readonly #clearTimer: (handle: ExecutionTimerHandle) => void;
  readonly #deadlineTimers = new Map<string, ExecutionTimerHandle>();

  constructor(dependencies: ExecutionControllerDependencies = {}) {
    this.lifecycle = dependencies.lifecycle ?? new ExecutionLifecycle();
    this.#idFactory = dependencies.idFactory ?? (() => crypto.randomUUID());
    this.#now = dependencies.now ?? (() => Date.now());
    this.#setTimer =
      dependencies.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer =
      dependencies.clearTimer ??
      ((handle) => globalThis.clearTimeout(handle as ReturnType<typeof globalThis.setTimeout>));
  }

  prepare(request: PrepareExecutionRequest): PreparedExecutionResult {
    const now = this.#now();
    for (const executionId of this.lifecycle.sweep(now)) {
      const handle = this.#deadlineTimers.get(executionId);
      if (handle !== undefined) this.#clearTimer(handle);
      this.#deadlineTimers.delete(executionId);
    }
    if (!Number.isSafeInteger(request.timeoutMs) || request.timeoutMs <= 0) {
      return { kind: 'rejected', reason: ExecutionRejectionReason.INVALID_REQUEST };
    }
    const execution = Object.freeze({
      executionId: this.#idFactory(),
      deadline: timestampFromMs(now + request.timeoutMs),
    });
    const registration = this.lifecycle.reserve(execution.executionId, execution.deadline);
    if (registration.kind === 'rejected') return registration;
    if (registration.kind === 'id_reuse') {
      return { kind: 'rejected', reason: ExecutionRejectionReason.INVALID_REQUEST };
    }
    this.#ensureDeadlineTimer(execution);
    return { ...registration, execution };
  }

  admit(request: ExecuteRequest): ExecutionAdmissionResult {
    return this.lifecycle.admit(request, this.#now());
  }

  subscribe(executionId: string, listener: ExecutionListener): () => void {
    return this.lifecycle.subscribe(executionId, listener);
  }

  attachCancellation(executionId: string, handle: ExecutionCancellationHandle): void {
    this.lifecycle.attachCancellation(executionId, handle);
  }

  cancel(request: CancelRequest): CancelResponse {
    return this.lifecycle.cancel(request);
  }

  reject(
    executionId: string,
    reason: ExecutionRejectionReason,
    message: string,
  ): ExecutionAdmissionResult {
    return this.lifecycle.reject(executionId, reason, message);
  }

  publishFrame(executionId: string, payload: unknown): ExecutionPublishResult {
    return this.lifecycle.publishFrame(executionId, payload);
  }

  complete(executionId: string, payload?: unknown): ExecutionTerminalResult {
    return this.lifecycle.complete(executionId, payload);
  }

  fail(executionId: string, reason: string, payload?: unknown): ExecutionTerminalResult {
    return this.lifecycle.fail(executionId, reason, payload);
  }

  confirmCancelled(executionId: string, payload?: unknown): ExecutionTerminalResult {
    return this.lifecycle.confirmCancelled(executionId, payload);
  }

  #ensureDeadlineTimer(execution: PreparedExecution): void {
    if (this.#deadlineTimers.has(execution.executionId)) return;
    const deadlineAt = timestampMs(execution.deadline);
    const handle = this.#setTimer(
      () => {
        this.#deadlineTimers.delete(execution.executionId);
        this.lifecycle.processDeadline(execution.executionId);
      },
      Math.max(0, deadlineAt - this.#now()),
    );
    this.#deadlineTimers.set(execution.executionId, handle);
  }
}

export function createExecutionController(
  dependencies: ExecutionControllerDependencies = {},
): ExecutionController {
  return new ExecutionController(dependencies);
}

export function executionRequestId(executionId: string): string {
  return executionId;
}

export function executionOpenSpanId(executionId: string, ordinal: number): string {
  return `${executionId}:open:${ordinal}`;
}

export function executionCancelSpanId(executionId: string, ordinal: number): string {
  return `${executionId}:cancel:${ordinal}`;
}

export function cancelExecutionRequest(executionId: string): CancelRequest {
  return create(CancelRequestSchema, { executionId });
}

function admissionRejection(
  request: ExecuteRequest,
  now: number,
): { reason: ExecutionRejectionReason; message: string } | undefined {
  if (!request.executionId.trim() || !request.query || request.binding.case === undefined) {
    return invalidRequest('execution ID, binding, and query are required');
  }
  if (!request.deadline) return invalidRequest('execution deadline is required');
  const deadlineAt = timestampMs(request.deadline);
  if (!Number.isSafeInteger(deadlineAt) || deadlineAt < 0) {
    return invalidRequest('execution deadline is invalid');
  }
  if (deadlineAt <= now) {
    return {
      reason: ExecutionRejectionReason.DEADLINE_EXPIRED,
      message: 'execution deadline has expired',
    };
  }
  if (request.query.preferredTarget !== ExecutionTarget.BROWSER_WASM) {
    return {
      reason: ExecutionRejectionReason.UNSUPPORTED,
      message: 'this browser executor only admits browser_wasm queries',
    };
  }

  const limits = request.query.options?.runtimeLimits;
  if (!limits) return invalidRequest('query runtime limits are required');
  const boundedBudgets: Array<
    [value: bigint | undefined, name: string, browserMaximum: number | undefined]
  > = [
    [limits.maxResultRows, 'max_result_rows', BROWSER_SAFE_RESULT_ROW_LIMIT],
    [limits.maxArrowIpcBytes, 'max_arrow_ipc_bytes', BROWSER_SAFE_ARROW_IPC_BYTES],
    [limits.maxPreviewStringBytes, 'max_preview_string_bytes', BROWSER_SAFE_PREVIEW_STRING_BYTES],
  ];
  if (limits.maxScanBytes !== undefined) {
    boundedBudgets.push([limits.maxScanBytes, 'max_scan_bytes', undefined]);
  }
  for (const [value, name, browserMaximum] of boundedBudgets) {
    if (value === undefined || value <= 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      return invalidRequest(`invalid ${name}`);
    }
    if (browserMaximum !== undefined && value > BigInt(browserMaximum)) {
      return {
        reason: ExecutionRejectionReason.RESOURCE_LIMIT,
        message: `browser-unsafe ${name}`,
      };
    }
  }
  return undefined;
}

function invalidRequest(message: string): {
  reason: ExecutionRejectionReason.INVALID_REQUEST;
  message: string;
} {
  return { reason: ExecutionRejectionReason.INVALID_REQUEST, message };
}

function acceptedAdmission(
  executionId: string,
  launch: boolean,
  executionSnapshot: ExecutionSnapshot,
): ExecutionAdmissionResult {
  return {
    admission: create(ExecutionAdmissionSchema, {
      outcome: {
        case: 'accepted',
        value: create(ExecutionAcceptedSchema, {
          executionId,
          state: executionSnapshotState(executionSnapshot.state),
          launch,
        }),
      },
    }),
    snapshot: executionSnapshot,
  };
}

function rejectedAdmission(
  executionId: string,
  reason: ExecutionRejectionReason,
  message: string,
  executionSnapshot?: ExecutionSnapshot,
): ExecutionAdmissionResult {
  return {
    admission: create(ExecutionAdmissionSchema, {
      outcome: {
        case: 'rejected',
        value: create(ExecutionRejectedSchema, { executionId, reason, message }),
      },
    }),
    snapshot: executionSnapshot,
  };
}

function rejectionMessage(reason: ExecutionRejectionReason | undefined): string {
  switch (reason) {
    case ExecutionRejectionReason.CANCELLED:
      return 'execution was cancelled before admission';
    case ExecutionRejectionReason.DEADLINE_EXPIRED:
      return 'execution deadline has expired';
    default:
      return 'execution was rejected';
  }
}

function snapshot(record: ExecutionRecord): ExecutionSnapshot {
  return Object.freeze({
    executionId: record.executionId,
    state: uiLifecycleState(record.state),
    request: record.request ? clone(ExecuteRequestSchema, record.request) : undefined,
    admitted: record.admitted,
    rejectionReason: record.rejectionReason,
    terminalReason: record.terminalReason,
    terminalFrame: record.terminalFrame
      ? clone(ExecutionTerminalFrameSchema, record.terminalFrame)
      : undefined,
    invariantViolations: Object.freeze([...record.invariantViolations]),
  });
}

function terminalFrame(
  record: ExecutionRecord,
  state: ExecutionTerminalState,
  sequence: number,
): ExecutionTerminalFrame {
  const outcome =
    state === 'completed'
      ? { case: 'completed' as const, value: create(ExecutionCompletedSchema) }
      : state === 'failed'
        ? { case: 'failed' as const, value: create(ExecutionFailedSchema) }
        : { case: 'cancelled' as const, value: create(ExecutionCancelledSchema) };
  return create(ExecutionTerminalFrameSchema, {
    executionId: record.executionId,
    sequence: BigInt(sequence),
    state: create(ExecutionTerminalStateSchema, { outcome }),
  });
}

function cloneTimestamp(timestamp: Timestamp): Timestamp {
  return {
    $typeName: 'google.protobuf.Timestamp',
    seconds: timestamp.seconds,
    nanos: timestamp.nanos,
  };
}

function isTerminalOrRejected(state: ContractExecutionLifecycleState): boolean {
  return (
    state === ContractExecutionLifecycleState.REJECTED ||
    state === ContractExecutionLifecycleState.COMPLETED ||
    state === ContractExecutionLifecycleState.FAILED ||
    state === ContractExecutionLifecycleState.CANCELLED
  );
}

function contractLifecycleState(state: ExecutionTerminalState): ContractExecutionLifecycleState {
  switch (state) {
    case 'completed':
      return ContractExecutionLifecycleState.COMPLETED;
    case 'failed':
      return ContractExecutionLifecycleState.FAILED;
    case 'cancelled':
      return ContractExecutionLifecycleState.CANCELLED;
  }
}

function executionSnapshotState(state: ExecutionLifecycleState): ContractExecutionLifecycleState {
  switch (state) {
    case 'created':
      return ContractExecutionLifecycleState.CREATED;
    case 'running':
      return ContractExecutionLifecycleState.RUNNING;
    case 'cancel_requested':
      return ContractExecutionLifecycleState.CANCEL_REQUESTED;
    case 'rejected':
      return ContractExecutionLifecycleState.REJECTED;
    case 'completed':
      return ContractExecutionLifecycleState.COMPLETED;
    case 'failed':
      return ContractExecutionLifecycleState.FAILED;
    case 'cancelled':
      return ContractExecutionLifecycleState.CANCELLED;
  }
}

function uiLifecycleState(state: ContractExecutionLifecycleState): ExecutionLifecycleState {
  switch (state) {
    case ContractExecutionLifecycleState.CREATED:
      return 'created';
    case ContractExecutionLifecycleState.RUNNING:
      return 'running';
    case ContractExecutionLifecycleState.CANCEL_REQUESTED:
      return 'cancel_requested';
    case ContractExecutionLifecycleState.REJECTED:
      return 'rejected';
    case ContractExecutionLifecycleState.COMPLETED:
      return 'completed';
    case ContractExecutionLifecycleState.FAILED:
      return 'failed';
    case ContractExecutionLifecycleState.CANCELLED:
      return 'cancelled';
    case ContractExecutionLifecycleState.UNSPECIFIED:
      throw new Error('execution lifecycle state is unspecified');
  }
}

function recordInvariant(record: ExecutionRecord, violation: string): void {
  if (record.invariantViolations.length >= MAX_INVARIANT_VIOLATIONS) return;
  record.invariantViolations.push(violation);
}
