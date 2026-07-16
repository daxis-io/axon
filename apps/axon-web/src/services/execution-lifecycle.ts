import type { ExecutionTarget } from '../axon-browser-sdk.ts';
import { querySourceIdentity, type QuerySourceIdentity } from '../query/keys.ts';
import type { QueryTableSource } from './query-source.ts';

export type ExecutionBudgets = Readonly<{
  maxResultRows: number;
  maxArrowIpcBytes: number;
  maxPreviewStringBytes: number;
  maxScanBytes?: number;
}>;

export type ExecutionAdmissionInput = Readonly<{
  executionId: string;
  sourceIdentity: QuerySourceIdentity;
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
  fingerprint: string;
  state: ExecutionLifecycleState;
  admitted: boolean;
  rejectionReason?: string;
  terminalReason?: string;
  invariantViolations: string[];
  listeners: Set<ExecutionListener>;
  cancellationHandle?: ExecutionCancellationHandle;
  cancellationInvoked: boolean;
  sequence: number;
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
      if (existing.fingerprint !== admissionFingerprint(input)) {
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
      fingerprint: admissionFingerprint(immutableInput),
      state: 'created',
      admitted: false,
      invariantViolations: [],
      listeners: new Set(),
      cancellationInvoked: false,
      sequence: 0,
    };
    this.#records.set(immutableInput.executionId, record);
    return { kind: 'created', snapshot: snapshot(record) };
  }

  admit(input: ExecutionAdmissionInput): ExecutionAdmissionResult {
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

    record.invariantViolations.push(`cancellation requested after ${record.state}`);
    return { kind: 'recorded', snapshot: snapshot(record) };
  }

  publishFrame(executionId: string, payload: unknown): ExecutionPublishResult {
    const record = this.#records.get(executionId);
    if (!record) return { kind: 'unknown' };
    if (record.state !== 'running' && record.state !== 'cancel_requested') {
      record.invariantViolations.push(`frame published after ${record.state}`);
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
      if (record.state !== state) {
        record.invariantViolations.push(`late ${state} after ${record.state}`);
      }
      return { kind: 'recorded', delivered: false, snapshot: snapshot(record) };
    }

    record.state = state;
    record.terminalReason = reason;
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
      record.invariantViolations.push('cancellation handle threw');
    }
  }

  #notify(record: ExecutionRecord, delivery: ExecutionDelivery): void {
    for (const listener of [...record.listeners]) {
      try {
        listener(delivery);
      } catch {
        record.invariantViolations.push('execution listener threw');
      }
    }
  }
}

export type PrepareExecutionRequest = Readonly<{
  source: QueryTableSource;
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
    const input = immutableAdmissionInput({
      executionId: this.#idFactory(),
      sourceIdentity: querySourceIdentity(request.source),
      sql: request.sql,
      target: request.target,
      deadlineAt: this.#now() + request.timeoutMs,
      budgets: request.budgets,
    });
    const registration = this.lifecycle.create(input);
    if (registration.kind === 'rejected') return registration;
    return { ...registration, input };
  }

  admit(input: ExecutionAdmissionInput): ExecutionAdmissionResult {
    return this.lifecycle.admit(input);
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

  scheduleDeadline(input: ExecutionAdmissionInput, callback: () => void): ExecutionTimerHandle {
    return this.#setTimer(callback, Math.max(0, input.deadlineAt - this.#now()));
  }

  clearScheduled(handle: ExecutionTimerHandle): void {
    this.#clearTimer(handle);
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
  const sourceIdentity = Object.freeze([...input.sourceIdentity]) as QuerySourceIdentity;
  const budgets = Object.freeze({ ...input.budgets });
  return Object.freeze({ ...input, sourceIdentity, budgets });
}

function admissionFingerprint(input: ExecutionAdmissionInput): string {
  return JSON.stringify([
    input.sourceIdentity,
    input.sql,
    input.target,
    input.deadlineAt,
    input.budgets.maxResultRows,
    input.budgets.maxArrowIpcBytes,
    input.budgets.maxPreviewStringBytes,
    input.budgets.maxScanBytes ?? null,
  ]);
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
