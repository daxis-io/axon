import { describe, expect, it, vi } from 'vitest';
import type { EngineStatus } from '../services/types.ts';
import type { QuerySourceSelection, QueryTableSource } from '../services/query-source.ts';
import { selectEngineActions, selectEngineStatus } from '../state/hooks.ts';
import { createAxonClientStore, createMemoryClientStateStorage } from '../state/store.ts';
import type { RunUiState } from '../state/slices/run.ts';
import * as AppModule from './App.tsx';

type AppEngineStatusModule = {
  subscribeAppEngineStatus?: (
    engineActions: ReturnType<typeof selectEngineActions>,
    subscribe?: (listener: (status: EngineStatus) => void) => () => void,
  ) => () => void;
};

type AppQuerySelectionModule = {
  executeQuerySelection?: <T>(
    selection: QuerySourceSelection,
    execute: (source: QueryTableSource) => Promise<T>,
  ) => Promise<{ status: 'unavailable'; reason: string } | { status: 'executed'; value: T }>;
};

type AppExecutionGuardModule = {
  executionMayUpdateUi?: (runState: RunUiState, executionId: string) => boolean;
};

function engineStatus(): EngineStatus {
  return {
    bundle: 'axon_web_wasm.wasm',
    bundle_tier: 'baseline',
    available_tiers: ['baseline'],
    active_tier: 'baseline',
    wasm_size_kb: 4096,
    cold_start_ms: 75,
    worker_mem_mb: 128,
    cache: {
      opfs_used_mb: 2,
      opfs_budget_mb: 64,
      memory_mb: 8,
      extents: 4,
      hit_ratio: 0.25,
    },
    proto: 'DataFusion · Delta Lake',
  };
}

describe('App engine status subscription', () => {
  it('feeds subscribed engine status events into the client store', () => {
    const subscribeAppEngineStatus = (AppModule as AppEngineStatusModule).subscribeAppEngineStatus;
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const status = engineStatus();
    const unsubscribe = vi.fn();
    const subscribe = vi.fn((listener: (incoming: EngineStatus) => void) => {
      listener(status);
      return unsubscribe;
    });

    expect(subscribeAppEngineStatus).toEqual(expect.any(Function));

    const cleanup = subscribeAppEngineStatus?.(selectEngineActions(store.getState()), subscribe);

    expect(selectEngineStatus(store.getState())).toEqual(status);
    expect(cleanup).toBe(unsubscribe);
    expect(subscribe).toHaveBeenCalledTimes(1);
  });
});

describe('App authoritative query selection', () => {
  it('does not invoke the lazy query path for an unavailable selection', async () => {
    const executeQuerySelection = (AppModule as AppQuerySelectionModule).executeQuerySelection;
    const execute = vi.fn();
    const selection: QuerySourceSelection = { kind: 'unavailable', reason: 'missing' };

    expect(executeQuerySelection).toEqual(expect.any(Function));
    if (!executeQuerySelection) return;

    await expect(executeQuerySelection(selection, execute)).resolves.toEqual({
      status: 'unavailable',
      reason: 'missing',
    });
    expect(execute).not.toHaveBeenCalled();
  });
});

describe('App execution callback guard', () => {
  it('allows post-await UI effects only for the current execution ID', () => {
    const executionMayUpdateUi = (AppModule as AppExecutionGuardModule).executionMayUpdateUi;
    expect(executionMayUpdateUi).toEqual(expect.any(Function));
    if (!executionMayUpdateUi) return;

    expect(
      executionMayUpdateUi(
        {
          status: 'completed',
          executionId: 'execution-1',
          target: 'browser_wasm',
          ms: 10,
          rows: 1,
        },
        'execution-1',
      ),
    ).toBe(true);
    expect(
      executionMayUpdateUi(
        {
          status: 'running',
          executionId: 'execution-2',
          target: 'browser_wasm',
          elapsed: 2,
        },
        'execution-1',
      ),
    ).toBe(false);
    expect(executionMayUpdateUi({ status: 'idle' }, 'execution-1')).toBe(false);
  });
});
