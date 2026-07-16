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

export type ExecutionSnapshot = Readonly<{
  executionId: string;
  state: ExecutionLifecycleState;
  input: ExecutionAdmissionInput;
  admitted: boolean;
  rejectionReason?: string;
  invariantViolations: readonly string[];
}>;

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

type ExecutionRecord = {
  input: ExecutionAdmissionInput;
  fingerprint: string;
  state: ExecutionLifecycleState;
  admitted: boolean;
  rejectionReason?: string;
  invariantViolations: string[];
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

  getSnapshot(executionId: string): ExecutionSnapshot | undefined {
    const record = this.#records.get(executionId);
    return record ? snapshot(record) : undefined;
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
    invariantViolations: Object.freeze([...record.invariantViolations]),
  });
}
