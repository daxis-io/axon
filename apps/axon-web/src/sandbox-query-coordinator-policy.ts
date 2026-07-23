import { BROWSER_SAFE_ARROW_IPC_BYTES, type BrowserWorkerSqlCommand } from './axon-browser-sdk';
import type { PrivateTerminalStatus } from './sandbox-query-stream-protocol';

export const MAX_COORDINATOR_REQUESTS = 64;

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
