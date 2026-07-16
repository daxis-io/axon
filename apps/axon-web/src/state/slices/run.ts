import { defaultCapabilityMatrix } from '../../services/capabilities.ts';
import {
  appendResultPage,
  sameQueryResultPageRun,
  type QueryResultPageRun,
} from '../../services/query-pagination.ts';
import type {
  CapabilityMatrixRow,
  ExecutionTarget,
  PlanSummary,
  QueryEvent,
  QueryMetricsSummary,
  QueryResultData,
} from '../../services/types.ts';

type ActiveRunUiState = {
  executionId: string;
  target: ExecutionTarget;
  elapsed: number;
};

export type RunUiState =
  | { status: 'idle' }
  | ({ status: 'created' } & ActiveRunUiState)
  | ({ status: 'running' } & ActiveRunUiState)
  | ({ status: 'cancel_requested' } & ActiveRunUiState)
  | {
      status: 'rejected';
      executionId: string;
      target: ExecutionTarget;
      ms: number;
      message: string;
      code?: string;
    }
  | {
      status: 'completed';
      executionId: string;
      target: ExecutionTarget;
      ms: number;
      rows: number;
      fallback?: { code: string; detail: string } | null;
    }
  | {
      status: 'failed';
      executionId: string;
      target: ExecutionTarget;
      ms: number;
      message: string;
      code?: string;
    }
  | {
      status: 'cancelled';
      executionId: string;
      target: ExecutionTarget;
      ms: number;
      message?: string;
    };

export type RunState = {
  runState: RunUiState;
  resultData: QueryResultData | undefined;
  resultPageRun: QueryResultPageRun | undefined;
  loadingMoreRows: boolean;
  loadingMoreExecutionId: string | undefined;
  metrics: QueryMetricsSummary | undefined;
  events: QueryEvent[];
  plan: PlanSummary | undefined;
  capabilities: CapabilityMatrixRow[];
};

export type FinishRunSuccessInput = {
  runState: Extract<RunUiState, { status: 'completed' }>;
  resultData: QueryResultData;
  resultPageRun: QueryResultPageRun;
  metrics: QueryMetricsSummary;
  plan: PlanSummary | undefined;
  capabilities: CapabilityMatrixRow[];
};

export type FinishLoadMoreRowsSuccessInput = {
  executionId: string;
  runForPage: QueryResultPageRun;
  activeRun: QueryResultPageRun | undefined;
  resultData: QueryResultData;
  metrics: QueryMetricsSummary;
  plan?: PlanSummary;
  elapsedMs: number;
};

export type FinishLoadMoreRowsResult = { discarded: true } | { discarded: false; rows: number };

export type RunActions = {
  createRun(executionId: string, target: ExecutionTarget): void;
  startRun(executionId: string): void;
  updateRunElapsed(executionId: string, elapsed: number): void;
  appendRunEvent(executionId: string, event: QueryEvent): void;
  rejectRun(runState: Extract<RunUiState, { status: 'rejected' }>): void;
  finishRunSuccess(input: FinishRunSuccessInput): void;
  finishRunError(runState: Extract<RunUiState, { status: 'failed' }>): void;
  requestRunCancellation(executionId: string): void;
  finishRunCancelled(runState: Extract<RunUiState, { status: 'cancelled' }>): void;
  resetRun(): void;
  clearForLocalAccessReselect(): void;
  startLoadMoreRows(executionId: string): void;
  finishLoadMoreRowsSuccess(input: FinishLoadMoreRowsSuccessInput): FinishLoadMoreRowsResult;
  finishLoadMoreRows(executionId: string): void;
};

export type RunSlice = {
  run: RunState;
  runActions: RunActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

type StoreGet<TState> = () => TState;

export function createDefaultRunState(): RunState {
  return {
    runState: { status: 'idle' },
    resultData: undefined,
    resultPageRun: undefined,
    loadingMoreRows: false,
    loadingMoreExecutionId: undefined,
    metrics: undefined,
    events: [],
    plan: undefined,
    capabilities: defaultCapabilityMatrix(),
  };
}

function updateRun<TState extends RunSlice>(
  set: StoreSet<TState>,
  update: (run: RunState) => RunState,
): void {
  set((state) => ({
    ...state,
    run: update(state.run),
  }));
}

function activeExecutionId(runState: RunUiState): string | undefined {
  return runState.status === 'created' ||
    runState.status === 'running' ||
    runState.status === 'cancel_requested'
    ? runState.executionId
    : undefined;
}

function isMutableExecution(runState: RunUiState, executionId: string): boolean {
  return activeExecutionId(runState) === executionId;
}

export function createRunSlice<TState extends RunSlice>(
  set: StoreSet<TState>,
  get: StoreGet<TState>,
): RunSlice {
  return {
    run: createDefaultRunState(),
    runActions: {
      createRun(executionId, target) {
        updateRun(set, (run) => {
          if (activeExecutionId(run.runState)) return run;
          return {
            ...run,
            runState: { status: 'created', executionId, target, elapsed: 0 },
            resultPageRun: undefined,
            loadingMoreRows: false,
            loadingMoreExecutionId: undefined,
            events: [],
            plan: undefined,
          };
        });
      },
      startRun(executionId) {
        updateRun(set, (run) =>
          run.runState.status === 'created' && run.runState.executionId === executionId
            ? { ...run, runState: { ...run.runState, status: 'running' } }
            : run,
        );
      },
      updateRunElapsed(executionId, elapsed) {
        updateRun(set, (run) =>
          isMutableExecution(run.runState, executionId)
            ? { ...run, runState: { ...run.runState, elapsed } }
            : run,
        );
      },
      appendRunEvent(executionId, event) {
        updateRun(set, (run) => {
          const isPrimaryExecution = isMutableExecution(run.runState, executionId);
          const isPageExecution = run.loadingMoreExecutionId === executionId;
          if (!isPrimaryExecution && !isPageExecution) return run;
          return {
            ...run,
            events: [...run.events, event],
            metrics: event.kind === 'metrics' ? event.metrics : run.metrics,
          };
        });
      },
      rejectRun(runState) {
        updateRun(set, (run) =>
          isMutableExecution(run.runState, runState.executionId)
            ? { ...run, runState, loadingMoreRows: false, loadingMoreExecutionId: undefined }
            : run,
        );
      },
      finishRunSuccess(input) {
        updateRun(set, (run) => {
          if (!isMutableExecution(run.runState, input.runState.executionId)) return run;
          return {
            ...run,
            runState: input.runState,
            resultData: input.resultData,
            resultPageRun: input.resultPageRun,
            loadingMoreRows: false,
            loadingMoreExecutionId: undefined,
            metrics: input.metrics,
            plan: input.plan,
            capabilities: input.capabilities,
          };
        });
      },
      finishRunError(runState) {
        updateRun(set, (run) =>
          isMutableExecution(run.runState, runState.executionId)
            ? {
                ...run,
                runState,
                loadingMoreRows: false,
                loadingMoreExecutionId: undefined,
                plan: undefined,
              }
            : run,
        );
      },
      requestRunCancellation(executionId) {
        updateRun(set, (run) => {
          if (
            (run.runState.status !== 'created' &&
              run.runState.status !== 'running' &&
              run.runState.status !== 'cancel_requested') ||
            run.runState.executionId !== executionId
          ) {
            return run;
          }
          if (run.runState.status === 'cancel_requested') return run;
          return { ...run, runState: { ...run.runState, status: 'cancel_requested' } };
        });
      },
      finishRunCancelled(runState) {
        updateRun(set, (run) =>
          isMutableExecution(run.runState, runState.executionId)
            ? {
                ...run,
                runState,
                loadingMoreRows: false,
                loadingMoreExecutionId: undefined,
                plan: undefined,
              }
            : run,
        );
      },
      resetRun() {
        updateRun(set, () => createDefaultRunState());
      },
      clearForLocalAccessReselect() {
        updateRun(set, (run) => {
          if (activeExecutionId(run.runState)) return run;
          return {
            ...run,
            runState: { status: 'idle' },
            resultData: undefined,
            resultPageRun: undefined,
            loadingMoreRows: false,
            loadingMoreExecutionId: undefined,
            metrics: undefined,
            events: [],
            plan: undefined,
          };
        });
      },
      startLoadMoreRows(executionId) {
        updateRun(set, (run) => {
          if (run.loadingMoreRows || run.runState.status !== 'completed') return run;
          return { ...run, loadingMoreRows: true, loadingMoreExecutionId: executionId };
        });
      },
      finishLoadMoreRowsSuccess(input) {
        const current = get().run;
        const latestResultRun = current.resultPageRun;
        if (
          current.loadingMoreExecutionId !== input.executionId ||
          !current.resultData ||
          !latestResultRun ||
          !input.activeRun ||
          !sameQueryResultPageRun(latestResultRun, input.runForPage) ||
          !sameQueryResultPageRun(input.activeRun, input.runForPage)
        ) {
          updateRun(set, (run) =>
            run.loadingMoreExecutionId === input.executionId
              ? { ...run, loadingMoreRows: false, loadingMoreExecutionId: undefined }
              : run,
          );
          return { discarded: true };
        }

        const merged = appendResultPage(current.resultData, input.resultData);
        updateRun(set, (run) => {
          if (run.loadingMoreExecutionId !== input.executionId) return run;
          return {
            ...run,
            runState:
              run.runState.status === 'completed'
                ? { ...run.runState, ms: input.elapsedMs, rows: merged.row_count }
                : run.runState,
            resultData: merged,
            resultPageRun: input.runForPage,
            loadingMoreRows: false,
            loadingMoreExecutionId: undefined,
            metrics: input.metrics,
            plan: input.plan ?? run.plan,
          };
        });
        return { discarded: false, rows: merged.row_count };
      },
      finishLoadMoreRows(executionId) {
        updateRun(set, (run) =>
          run.loadingMoreExecutionId === executionId
            ? { ...run, loadingMoreRows: false, loadingMoreExecutionId: undefined }
            : run,
        );
      },
    },
  };
}
