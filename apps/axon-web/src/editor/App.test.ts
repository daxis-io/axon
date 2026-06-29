import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import type { EngineStatus } from '../services/types.ts';
import { selectEngineActions, selectEngineStatus } from '../state/hooks.ts';
import { createAxonClientStore, createMemoryClientStateStorage } from '../state/store.ts';
import * as AppModule from './App.tsx';

type AppEngineStatusModule = {
  subscribeAppEngineStatus?: (
    engineActions: ReturnType<typeof selectEngineActions>,
    subscribe?: (listener: (status: EngineStatus) => void) => () => void,
  ) => () => void;
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

describe('App catalog server-state wiring', () => {
  it('reads catalog and commit data through query adapters', () => {
    const source = readFileSync(new URL('./App.tsx', import.meta.url), 'utf8');

    expect(source).toContain('useQuery(catalogQueryOptions(querySource))');
    expect(source).toContain('useQuery(commitsQueryOptions(querySource))');
    expect(source).not.toMatch(
      /\b(loadCatalog|subscribeCatalog|snapshotCatalog|subscribeCommits)\b/,
    );
  });
});

describe('App local metadata server-state wiring', () => {
  it('reads and writes local metadata through query adapters', () => {
    const source = readFileSync(new URL('./App.tsx', import.meta.url), 'utf8');

    expect(source).toContain('useQuery(historyQueryOptions())');
    expect(source).toContain('useQuery(savedQueriesQueryOptions())');
    expect(source).toContain('appendHistoryEntry(queryClient,');
    expect(source).toContain('saveSavedQuery(queryClient,');
    expect(source).not.toMatch(/\b(loadHistory|loadSaved|appendHistory|saveQuery)\b/);
  });
});
