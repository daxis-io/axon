import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { ExecutionRejectionReason } from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import type { EngineStatus } from '../services/types.ts';
import { selectEngineActions, selectEngineStatus } from '../state/hooks.ts';
import { createAxonClientStore, createMemoryClientStateStorage } from '../state/store.ts';
import type { RunUiState } from '../state/slices/run.ts';
import * as AppModule from './App.tsx';

const appSource = readFileSync(new URL('./App.tsx', import.meta.url), 'utf8');

type AppEngineStatusModule = {
  subscribeAppEngineStatus?: (
    engineActions: ReturnType<typeof selectEngineActions>,
    subscribe?: (listener: (status: EngineStatus) => void) => () => void,
  ) => () => void;
};

type AppExecutionGuardModule = {
  executionMayUpdateUi?: (runState: RunUiState, executionId: string) => boolean;
  browserProviderRejectionReason?: (error: unknown) => ExecutionRejectionReason;
};

type AppTimerOwnershipModule = {
  clearOwnedRunTimer?: (
    timerRef: { current: number | null },
    ownedTimer: number,
    clearTimer: (timer: number) => void,
  ) => void;
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

describe('App connection cleanup ordering', () => {
  it.each(['const handleConnected', 'const removeConnectedCatalog'])(
    'begins connection-prefix purge before releasing runtime resources in %s',
    (marker) => {
      const start = appSource.indexOf(marker);
      const block = appSource.slice(start, start + 1_200);

      expect(start).toBeGreaterThanOrEqual(0);
      expect(block.indexOf('purgeCatalogSourcesCache(')).toBeGreaterThanOrEqual(0);
      expect(block.indexOf('purgeCatalogSourcesCache(')).toBeLessThan(
        block.indexOf('discardActiveQuerySession()'),
      );
      expect(block.indexOf('purgeCatalogSourcesCache(')).toBeLessThan(
        block.indexOf('unregisterLocalDeltaRuntimeIds('),
      );
    },
  );
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

  it('preserves provider rejection classes without accepting unrelated error fields', () => {
    const browserProviderRejectionReason = (AppModule as AppExecutionGuardModule)
      .browserProviderRejectionReason;
    expect(browserProviderRejectionReason).toEqual(expect.any(Function));
    if (!browserProviderRejectionReason) return;

    expect(
      browserProviderRejectionReason({
        rejectionReason: ExecutionRejectionReason.ACCESS_DENIED,
      }),
    ).toBe(ExecutionRejectionReason.ACCESS_DENIED);
    expect(
      browserProviderRejectionReason({
        rejectionReason: ExecutionRejectionReason.CANCELLED,
      }),
    ).toBe(ExecutionRejectionReason.UNAVAILABLE);
    expect(browserProviderRejectionReason(new Error('runtime import failed'))).toBe(
      ExecutionRejectionReason.UNAVAILABLE,
    );
  });
});

describe('App execution timer ownership', () => {
  it('clears an old execution timer without detaching a newer run timer', () => {
    const clearOwnedRunTimer = (AppModule as AppTimerOwnershipModule).clearOwnedRunTimer;
    const clearTimer = vi.fn();
    const timerRef = { current: 2 };

    expect(clearOwnedRunTimer).toEqual(expect.any(Function));
    if (!clearOwnedRunTimer) return;

    clearOwnedRunTimer(timerRef, 1, clearTimer);
    expect(clearTimer).toHaveBeenCalledWith(1);
    expect(timerRef.current).toBe(2);

    clearOwnedRunTimer(timerRef, 2, clearTimer);
    expect(clearTimer).toHaveBeenCalledWith(2);
    expect(timerRef.current).toBeNull();
  });
});
