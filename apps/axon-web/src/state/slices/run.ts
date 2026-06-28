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

export type RunUiState =
  | { status: 'idle' }
  | { status: 'running'; target: ExecutionTarget; elapsed: number }
  | {
      status: 'done';
      target: ExecutionTarget;
      ms: number;
      rows: number;
      fallback?: { code: string; detail: string } | null;
    }
  | {
      status: 'error';
      target?: ExecutionTarget;
      ms: number;
      message: string;
      code?: string;
    };

export type RunState = {
  runState: RunUiState;
  resultData: QueryResultData | undefined;
  resultPageRun: QueryResultPageRun | undefined;
  loadingMoreRows: boolean;
  metrics: QueryMetricsSummary | undefined;
  events: QueryEvent[];
  plan: PlanSummary | undefined;
  capabilities: CapabilityMatrixRow[];
};

export type FinishRunSuccessInput = {
  runState: Extract<RunUiState, { status: 'done' }>;
  resultData: QueryResultData;
  resultPageRun: QueryResultPageRun;
  metrics: QueryMetricsSummary;
  plan: PlanSummary | undefined;
  capabilities: CapabilityMatrixRow[];
};

export type FinishLoadMoreRowsSuccessInput = {
  runForPage: QueryResultPageRun;
  activeRun: QueryResultPageRun | undefined;
  resultData: QueryResultData;
  metrics: QueryMetricsSummary;
  plan?: PlanSummary;
  elapsedMs: number;
};

export type FinishLoadMoreRowsResult = { discarded: true } | { discarded: false; rows: number };

export type RunActions = {
  startRun(target: ExecutionTarget): void;
  updateRunElapsed(elapsed: number): void;
  appendRunEvent(event: QueryEvent): void;
  finishRunSuccess(input: FinishRunSuccessInput): void;
  finishRunError(runState: Extract<RunUiState, { status: 'error' }>): void;
  cancelRun(): void;
  resetRun(): void;
  clearForLocalAccessReselect(): void;
  startLoadMoreRows(): void;
  finishLoadMoreRowsSuccess(input: FinishLoadMoreRowsSuccessInput): FinishLoadMoreRowsResult;
  finishLoadMoreRows(): void;
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

export function createRunSlice<TState extends RunSlice>(
  set: StoreSet<TState>,
  get: StoreGet<TState>,
): RunSlice {
  return {
    run: createDefaultRunState(),
    runActions: {
      startRun(target) {
        updateRun(set, (run) => ({
          ...run,
          runState: { status: 'running', target, elapsed: 0 },
          resultPageRun: undefined,
          loadingMoreRows: false,
          events: [],
          plan: undefined,
        }));
      },
      updateRunElapsed(elapsed) {
        updateRun(set, (run) =>
          run.runState.status === 'running'
            ? { ...run, runState: { ...run.runState, elapsed } }
            : run,
        );
      },
      appendRunEvent(event) {
        updateRun(set, (run) => ({
          ...run,
          events: [...run.events, event],
          metrics: event.kind === 'metrics' ? event.metrics : run.metrics,
        }));
      },
      finishRunSuccess(input) {
        updateRun(set, (run) => ({
          ...run,
          runState: input.runState,
          resultData: input.resultData,
          resultPageRun: input.resultPageRun,
          loadingMoreRows: false,
          metrics: input.metrics,
          plan: input.plan,
          capabilities: input.capabilities,
        }));
      },
      finishRunError(runState) {
        updateRun(set, (run) => ({
          ...run,
          runState,
          loadingMoreRows: false,
          plan: undefined,
        }));
      },
      cancelRun() {
        updateRun(set, (run) => ({
          ...run,
          runState: { status: 'idle' },
          resultPageRun: undefined,
          loadingMoreRows: false,
        }));
      },
      resetRun() {
        updateRun(set, () => createDefaultRunState());
      },
      clearForLocalAccessReselect() {
        updateRun(set, (run) => ({
          ...run,
          runState: { status: 'idle' },
          resultData: undefined,
          resultPageRun: undefined,
          loadingMoreRows: false,
          metrics: undefined,
          events: [],
          plan: undefined,
        }));
      },
      startLoadMoreRows() {
        updateRun(set, (run) => ({ ...run, loadingMoreRows: true }));
      },
      finishLoadMoreRowsSuccess(input) {
        const current = get().run;
        const latestResultRun = current.resultPageRun;
        if (
          !current.resultData ||
          !latestResultRun ||
          !input.activeRun ||
          !sameQueryResultPageRun(latestResultRun, input.runForPage) ||
          !sameQueryResultPageRun(input.activeRun, input.runForPage)
        ) {
          updateRun(set, (run) => ({ ...run, loadingMoreRows: false }));
          return { discarded: true };
        }

        const merged = appendResultPage(current.resultData, input.resultData);
        updateRun(set, (run) => ({
          ...run,
          runState:
            run.runState.status === 'done'
              ? { ...run.runState, ms: input.elapsedMs, rows: merged.row_count }
              : run.runState,
          resultData: merged,
          resultPageRun: input.runForPage,
          loadingMoreRows: false,
          metrics: input.metrics,
          plan: input.plan ?? run.plan,
        }));
        return { discarded: false, rows: merged.row_count };
      },
      finishLoadMoreRows() {
        updateRun(set, (run) => ({ ...run, loadingMoreRows: false }));
      },
    },
  };
}
