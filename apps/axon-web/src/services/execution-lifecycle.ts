import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
  type ExecutionTarget,
} from '../axon-browser-sdk.ts';
import { selectedQuerySourceIdentity, type SelectedQuerySourceIdentity } from '../query/keys.ts';
import type { AvailableQuerySourceSelection } from './query-source.ts';

export type ExecutionBudgets = Readonly<{
  maxResultRows: number;
  maxArrowIpcBytes: number;
  maxPreviewStringBytes: number;
  maxScanBytes?: number;
}>;

export type ExecutionAdmissionInput = Readonly<{
  executionId: string;
  sourceIdentity: SelectedQuerySourceIdentity;
  sql: string;
  target: ExecutionTarget;
  deadlineAt: number;
  budgets: ExecutionBudgets;
}>;

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
  input: ExecutionAdmissionInput;
  admitted: boolean;
  rejectionReason?: string;
  terminalReason?: string;
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
  | { kind: 'rejected'; reason: 'capacity' };

export type ExecutionAdmissionResult =
  | { kind: 'accepted'; launch: boolean; snapshot: ExecutionSnapshot }
  | { kind: 'rejected'; launch: false; reason: string; snapshot: ExecutionSnapshot }
  | { kind: 'id_reuse'; launch: false; snapshot: ExecutionSnapshot }
  | { kind: 'rejected'; launch: false; reason: 'capacity' };

export type ExecutionCancellationResult =
  | { kind: 'cancelled_before_admit'; snapshot: ExecutionSnapshot }
  | { kind: 'cancel_requested'; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; snapshot: ExecutionSnapshot }
  | { kind: 'unknown' };

export type ExecutionPublishResult =
  | { kind: 'published'; sequence: number; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; snapshot: ExecutionSnapshot }
  | { kind: 'unknown' };

export type ExecutionTerminalResult =
  | { kind: 'transitioned'; delivered: true; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; delivered: false; snapshot: ExecutionSnapshot }
  | { kind: 'unknown'; delivered: false };

type ExecutionRecord = {
  input: ExecutionAdmissionInput;
  state: ExecutionLifecycleState;
  admitted: boolean;
  rejectionReason?: string;
  terminalReason?: string;
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

  create(input: ExecutionAdmissionInput): ExecutionRegistrationResult {
    const existing = this.#records.get(input.executionId);
    if (existing) {
      if (!sameAdmissionInput(existing.input, input)) {
        return { kind: 'id_reuse', snapshot: snapshot(existing) };
      }
      return { kind: 'recorded', snapshot: snapshot(existing) };
    }

    if (this.#records.size >= this.#maxRecords) {
      return { kind: 'rejected', reason: 'capacity' };
    }

    const immutableInput = immutableAdmissionInput(input);
    const record: ExecutionRecord = {
      input: immutableInput,
      state: 'created',
      admitted: false,
      invariantViolations: [],
      listeners: new Set(),
      cancellationInvoked: false,
      sequence: 0,
      deadlineProcessed: false,
    };
    this.#records.set(immutableInput.executionId, record);
    return { kind: 'created', snapshot: snapshot(record) };
  }

  admit(input: ExecutionAdmissionInput, rejectionReason?: string): ExecutionAdmissionResult {
    const registration = this.create(input);
    if (registration.kind === 'id_reuse') {
      return { kind: 'id_reuse', launch: false, snapshot: registration.snapshot };
    }
    if (registration.kind === 'rejected') {
      return { kind: 'rejected', launch: false, reason: registration.reason };
    }

    const record = this.#records.get(input.executionId);
    if (!record) throw new Error('execution lifecycle lost a registered admission');

    if (record.state === 'created') {
      if (rejectionReason) {
        record.state = 'rejected';
        record.rejectionReason = rejectionReason;
        record.cancellationHandle = undefined;
        record.listeners.clear();
        if (rejectionReason === 'invalid_deadline' || rejectionReason === 'deadline_expired') {
          record.deadlineProcessed = true;
        }
        return {
          kind: 'rejected',
          launch: false,
          reason: rejectionReason,
          snapshot: snapshot(record),
        };
      }
      record.state = 'running';
      record.admitted = true;
      return { kind: 'accepted', launch: true, snapshot: snapshot(record) };
    }

    if (record.state === 'rejected') {
      return {
        kind: 'rejected',
        launch: false,
        reason: record.rejectionReason ?? 'rejected',
        snapshot: snapshot(record),
      };
    }

    return { kind: 'accepted', launch: false, snapshot: snapshot(record) };
  }

  subscribe(executionId: string, listener: ExecutionListener): () => void {
    const record = this.#records.get(executionId);
    if (!record) return () => undefined;
    if (
      record.state === 'rejected' ||
      record.state === 'completed' ||
      record.state === 'failed' ||
      record.state === 'cancelled'
    ) {
      recordInvariant(record, `listener attached after ${record.state}`);
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
    if (
      record.state === 'rejected' ||
      record.state === 'completed' ||
      record.state === 'failed' ||
      record.state === 'cancelled'
    ) {
      recordInvariant(record, `cancellation handle attached after ${record.state}`);
      return snapshot(record);
    }
    record.cancellationHandle = handle;
    if (record.state === 'cancel_requested') this.#invokeCancellation(record);
    return snapshot(record);
  }

  requestCancellation(executionId: string): ExecutionCancellationResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown' };

    if (record.state === 'created') {
      record.state = 'rejected';
      record.rejectionReason = 'cancelled';
      record.cancellationHandle = undefined;
      record.listeners.clear();
      return { kind: 'cancelled_before_admit', snapshot: snapshot(record) };
    }

    if (record.state === 'running') {
      record.state = 'cancel_requested';
      this.#invokeCancellation(record);
      return { kind: 'cancel_requested', snapshot: snapshot(record) };
    }

    if (record.state === 'cancel_requested' || record.state === 'rejected') {
      return { kind: 'recorded', snapshot: snapshot(record) };
    }

    recordInvariant(record, `cancellation requested after ${record.state}`);
    return { kind: 'recorded', snapshot: snapshot(record) };
  }

  publishFrame(executionId: string, payload: unknown): ExecutionPublishResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown' };
    if (record.state !== 'running' && record.state !== 'cancel_requested') {
      recordInvariant(record, `frame published after ${record.state}`);
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

    if (record.state === 'created') {
      record.state = 'rejected';
      record.rejectionReason = 'deadline_expired';
      record.cancellationHandle = undefined;
      record.listeners.clear();
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }
    if (record.state === 'running' || record.state === 'cancel_requested') {
      return this.#terminal(executionId, 'failed', 'deadline');
    }
    return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
  }

  sweep(now: number): string[] {
    const deleted: string[] = [];
    for (const [executionId, record] of this.#records) {
      if (!record.deadlineProcessed && record.input.deadlineAt <= now) {
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

    if (record.state !== 'running' && record.state !== 'cancel_requested') {
      recordInvariant(record, `late ${state} after ${record.state}`);
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }

    record.state = state;
    record.terminalReason = reason;
    if (reason === 'deadline') this.#invokeCancellation(record);
    const sequence = ++record.sequence;
    const delivery: ExecutionTerminalDelivery = Object.freeze({
      kind: 'terminal',
      sequence,
      state,
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
  selection: AvailableQuerySourceSelection;
  snapshotVersion?: number;
  sql: string;
  target: ExecutionTarget;
  timeoutMs: number;
  budgets: ExecutionBudgets;
}>;

export type PreparedExecutionResult =
  | { kind: 'created'; input: ExecutionAdmissionInput; snapshot: ExecutionSnapshot }
  | { kind: 'recorded'; input: ExecutionAdmissionInput; snapshot: ExecutionSnapshot }
  | { kind: 'id_reuse'; input: ExecutionAdmissionInput; snapshot: ExecutionSnapshot }
  | { kind: 'rejected'; reason: 'capacity' };

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
    for (const executionId of this.lifecycle.sweep(this.#now())) {
      const handle = this.#deadlineTimers.get(executionId);
      if (handle !== undefined) this.#clearTimer(handle);
      this.#deadlineTimers.delete(executionId);
    }
    const input = immutableAdmissionInput({
      executionId: this.#idFactory(),
      sourceIdentity: selectedQuerySourceIdentity(request.selection, request.snapshotVersion),
      sql: request.sql,
      target: request.target,
      deadlineAt: this.#now() + request.timeoutMs,
      budgets: request.budgets,
    });
    const registration = this.lifecycle.create(input);
    if (registration.kind === 'rejected') return registration;
    this.#ensureDeadlineTimer(input);
    return { ...registration, input };
  }

  admit(input: ExecutionAdmissionInput): ExecutionAdmissionResult {
    const registration = this.lifecycle.create(input);
    if (registration.kind === 'id_reuse') {
      return { kind: 'id_reuse', launch: false, snapshot: registration.snapshot };
    }
    if (registration.kind === 'rejected') {
      return { kind: 'rejected', launch: false, reason: registration.reason };
    }

    const rejectionReason = admissionRejectionReason(input, this.#now());
    if (registration.kind === 'recorded' && rejectionReason === 'deadline_expired') {
      this.lifecycle.processDeadline(input.executionId);
    }
    return this.lifecycle.admit(input, rejectionReason);
  }

  subscribe(executionId: string, listener: ExecutionListener): () => void {
    return this.lifecycle.subscribe(executionId, listener);
  }

  attachCancellation(executionId: string, handle: ExecutionCancellationHandle): void {
    this.lifecycle.attachCancellation(executionId, handle);
  }

  requestCancellation(executionId: string): ExecutionCancellationResult {
    return this.lifecycle.requestCancellation(executionId);
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

  #ensureDeadlineTimer(input: ExecutionAdmissionInput): void {
    if (this.#deadlineTimers.has(input.executionId)) return;
    if (!Number.isSafeInteger(input.deadlineAt)) return;
    const handle = this.#setTimer(
      () => {
        this.#deadlineTimers.delete(input.executionId);
        this.lifecycle.processDeadline(input.executionId);
      },
      Math.max(0, input.deadlineAt - this.#now()),
    );
    this.#deadlineTimers.set(input.executionId, handle);
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

function immutableAdmissionInput(input: ExecutionAdmissionInput): ExecutionAdmissionInput {
  const sourceIdentity: SelectedQuerySourceIdentity = Object.freeze({
    kind: input.sourceIdentity.kind,
    ref: Object.freeze({ ...input.sourceIdentity.ref }),
    source: Object.freeze([
      ...input.sourceIdentity.source,
    ]) as SelectedQuerySourceIdentity['source'],
    snapshotVersion: input.sourceIdentity.snapshotVersion,
  });
  const budgets = Object.freeze({ ...input.budgets });
  return Object.freeze({ ...input, sourceIdentity, budgets });
}

function sameAdmissionInput(
  left: ExecutionAdmissionInput,
  right: ExecutionAdmissionInput,
): boolean {
  return (
    left.executionId === right.executionId &&
    sameSelectedSourceIdentity(left.sourceIdentity, right.sourceIdentity) &&
    left.sql === right.sql &&
    left.target === right.target &&
    Object.is(left.deadlineAt, right.deadlineAt) &&
    Object.is(left.budgets.maxResultRows, right.budgets.maxResultRows) &&
    Object.is(left.budgets.maxArrowIpcBytes, right.budgets.maxArrowIpcBytes) &&
    Object.is(left.budgets.maxPreviewStringBytes, right.budgets.maxPreviewStringBytes) &&
    Object.is(left.budgets.maxScanBytes, right.budgets.maxScanBytes)
  );
}

function sameSelectedSourceIdentity(
  left: SelectedQuerySourceIdentity,
  right: SelectedQuerySourceIdentity,
): boolean {
  return (
    left.kind === right.kind &&
    left.ref.catalogId === right.ref.catalogId &&
    left.ref.schemaName === right.ref.schemaName &&
    left.ref.tableName === right.ref.tableName &&
    Object.is(left.snapshotVersion, right.snapshotVersion) &&
    left.source.length === right.source.length &&
    left.source.every((value, index) => Object.is(value, right.source[index]))
  );
}

function admissionRejectionReason(input: ExecutionAdmissionInput, now: number): string | undefined {
  if (!Number.isSafeInteger(input.deadlineAt)) return 'invalid_deadline';
  if (input.deadlineAt <= now) return 'deadline_expired';

  const boundedBudgets: Array<[value: number, name: string, browserMaximum: number | undefined]> = [
    [input.budgets.maxResultRows, 'max_result_rows', BROWSER_SAFE_RESULT_ROW_LIMIT],
    [input.budgets.maxArrowIpcBytes, 'max_arrow_ipc_bytes', BROWSER_SAFE_ARROW_IPC_BYTES],
    [
      input.budgets.maxPreviewStringBytes,
      'max_preview_string_bytes',
      BROWSER_SAFE_PREVIEW_STRING_BYTES,
    ],
  ];
  if (input.budgets.maxScanBytes !== undefined) {
    boundedBudgets.push([input.budgets.maxScanBytes, 'max_scan_bytes', undefined]);
  }
  for (const [value, name, browserMaximum] of boundedBudgets) {
    if (!Number.isSafeInteger(value) || value <= 0) return `invalid_${name}`;
    if (browserMaximum !== undefined && value > browserMaximum) {
      return `browser_unsafe_${name}`;
    }
  }
  return undefined;
}

function snapshot(record: ExecutionRecord): ExecutionSnapshot {
  return Object.freeze({
    executionId: record.input.executionId,
    state: record.state,
    input: record.input,
    admitted: record.admitted,
    rejectionReason: record.rejectionReason,
    terminalReason: record.terminalReason,
    invariantViolations: Object.freeze([...record.invariantViolations]),
  });
}

function recordInvariant(record: ExecutionRecord, violation: string): void {
  if (record.invariantViolations.length >= MAX_INVARIANT_VIOLATIONS) return;
  record.invariantViolations.push(violation);
}
