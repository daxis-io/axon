import { BROWSER_SAFE_ARROW_IPC_BYTES, type BrowserWorkerSqlCommand } from './axon-browser-sdk';
import type { PrivateTerminalStatus } from './sandbox-query-stream-protocol';

export const MAX_COORDINATOR_REQUESTS = 64;
export const MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES = 32 * 1024 * 1024;

export type CoordinatorMemorySnapshot = {
  limitBytes: number;
  currentReservedBytes: number;
  currentStagedBytes: number;
  peakReservedBytes: number;
  peakStagedBytes: number;
};

type CoordinatorMemoryReservation = {
  declaredBytes: number;
  stagedBytes: number;
};

export class CoordinatorMemoryBudget {
  readonly #limitBytes: number;
  readonly #reservations = new Map<string, CoordinatorMemoryReservation>();
  #currentReservedBytes = 0;
  #currentStagedBytes = 0;
  #peakReservedBytes = 0;
  #peakStagedBytes = 0;

  constructor(limitBytes = MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES) {
    requirePositiveSafeInteger(limitBytes, 'coordinator memory limit');
    this.#limitBytes = limitBytes;
  }

  tryReserve(queryId: string, declaredBytes: number): boolean {
    requireQueryId(queryId);
    requirePositiveSafeInteger(declaredBytes, 'coordinator declared result bytes');
    if (this.#reservations.has(queryId)) {
      throw new Error(`coordinator query '${queryId}' already has a memory reservation`);
    }
    const nextReservedBytes = this.#currentReservedBytes + declaredBytes;
    if (!Number.isSafeInteger(nextReservedBytes)) {
      throw new Error('coordinator reserved byte total overflowed a safe integer');
    }
    if (nextReservedBytes > this.#limitBytes) return false;
    this.#reservations.set(queryId, { declaredBytes, stagedBytes: 0 });
    this.#currentReservedBytes = nextReservedBytes;
    this.#peakReservedBytes = Math.max(this.#peakReservedBytes, nextReservedBytes);
    return true;
  }

  recordStaged(queryId: string, additionalBytes: number): void {
    requireQueryId(queryId);
    requireNonNegativeSafeInteger(additionalBytes, 'coordinator staged result bytes');
    const reservation = this.#requireReservation(queryId);
    const nextQueryStagedBytes = reservation.stagedBytes + additionalBytes;
    const nextAggregateStagedBytes = this.#currentStagedBytes + additionalBytes;
    if (
      !Number.isSafeInteger(nextQueryStagedBytes) ||
      !Number.isSafeInteger(nextAggregateStagedBytes)
    ) {
      throw new Error('coordinator staged byte total overflowed a safe integer');
    }
    if (nextQueryStagedBytes > reservation.declaredBytes) {
      throw new Error(
        `coordinator query '${queryId}' staging would exceed its declared reservation`,
      );
    }
    reservation.stagedBytes = nextQueryStagedBytes;
    this.#currentStagedBytes = nextAggregateStagedBytes;
    this.#peakStagedBytes = Math.max(this.#peakStagedBytes, nextAggregateStagedBytes);
  }

  release(queryId: string): void {
    requireQueryId(queryId);
    const reservation = this.#requireReservation(queryId);
    this.#reservations.delete(queryId);
    this.#currentReservedBytes -= reservation.declaredBytes;
    this.#currentStagedBytes -= reservation.stagedBytes;
  }

  snapshot(): CoordinatorMemorySnapshot {
    return {
      limitBytes: this.#limitBytes,
      currentReservedBytes: this.#currentReservedBytes,
      currentStagedBytes: this.#currentStagedBytes,
      peakReservedBytes: this.#peakReservedBytes,
      peakStagedBytes: this.#peakStagedBytes,
    };
  }

  #requireReservation(queryId: string): CoordinatorMemoryReservation {
    const reservation = this.#reservations.get(queryId);
    if (!reservation) {
      throw new Error(`coordinator query '${queryId}' does not have a memory reservation`);
    }
    return reservation;
  }
}

export type CoordinatorQueryState = 'active' | 'draining' | 'finished';

export class CoordinatorQueryLifecycle {
  #state: CoordinatorQueryState = 'active';
  #terminalStatus: PrivateTerminalStatus | undefined;

  get state(): CoordinatorQueryState {
    return this.#state;
  }

  get terminalStatus(): PrivateTerminalStatus | undefined {
    return this.#terminalStatus;
  }

  get canCommit(): boolean {
    return this.#state === 'active' && this.#terminalStatus === undefined;
  }

  beginDrain(status: PrivateTerminalStatus): boolean {
    if (!this.canCommit) return false;
    this.#terminalStatus = status;
    this.#state = 'draining';
    return true;
  }

  finishDrain(): void {
    if (this.#state !== 'draining') {
      throw new Error('coordinator query can finish only after entering draining state');
    }
    this.#state = 'finished';
  }
}

export function coordinatorMaxArrowIpcBytes(command: BrowserWorkerSqlCommand): number {
  const requested = command.query.options?.runtime_limits?.max_arrow_ipc_bytes;
  if (command.browser_safe_defaults === true) {
    return Math.min(
      positiveSafeIntegerOr(requested, BROWSER_SAFE_ARROW_IPC_BYTES),
      BROWSER_SAFE_ARROW_IPC_BYTES,
    );
  }
  if (!isPositiveSafeInteger(requested)) {
    throw new Error('raw sandbox SQL commands must provide a positive max_arrow_ipc_bytes');
  }
  return requested;
}

export function coordinatorHasCapacity(
  activeQueries: number,
  pendingForwardedCommands: number,
  maxRequests = MAX_COORDINATOR_REQUESTS,
): boolean {
  if (
    !Number.isSafeInteger(activeQueries) ||
    activeQueries < 0 ||
    !Number.isSafeInteger(pendingForwardedCommands) ||
    pendingForwardedCommands < 0 ||
    !Number.isSafeInteger(maxRequests) ||
    maxRequests <= 0
  ) {
    throw new Error('coordinator request counts and capacity must be non-negative safe integers');
  }
  return activeQueries + pendingForwardedCommands < maxRequests;
}

function positiveSafeIntegerOr(value: number | undefined, fallback: number): number {
  return isPositiveSafeInteger(value) ? value : fallback;
}

function isPositiveSafeInteger(value: number | undefined): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0;
}

function requireQueryId(queryId: string): void {
  if (!queryId) throw new Error('coordinator memory query id must not be empty');
}

function requirePositiveSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${field} must be a positive safe integer`);
  }
}

function requireNonNegativeSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} must be a non-negative safe integer`);
  }
}
