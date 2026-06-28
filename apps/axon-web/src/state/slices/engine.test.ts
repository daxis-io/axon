import { describe, expect, it } from 'vitest';
import type { EngineStatus } from '../../services/types.ts';
import { selectEngineActions, selectEngineStatus } from '../hooks.ts';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';

function persistedClientState(storage: ReturnType<typeof createMemoryClientStateStorage>) {
  return JSON.parse(storage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}') as {
    state?: Record<string, unknown>;
  };
}

function engineStatus(): EngineStatus {
  return {
    bundle: 'browser-engine-worker.wasm',
    bundle_tier: 'baseline',
    available_tiers: ['baseline', 'simd'],
    active_tier: 'baseline',
    wasm_size_kb: 1024,
    cold_start_ms: 42,
    worker_mem_mb: 128,
    cache: {
      opfs_used_mb: 1,
      opfs_budget_mb: 64,
      memory_mb: 8,
      extents: 3,
      hit_ratio: 0.5,
    },
    proto: 'DataFusion Delta Lake',
  };
}

describe('engine slice', () => {
  it('starts without an engine status snapshot', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(selectEngineStatus(store.getState())).toBeUndefined();
  });

  it('stores the latest complete engine status snapshot', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const status = engineStatus();

    selectEngineActions(store.getState()).setStatus(status);

    expect(selectEngineStatus(store.getState())).toEqual(status);
  });

  it('does not persist engine status with client state', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });

    selectEngineActions(first.getState()).setStatus(engineStatus());

    const persisted = persistedClientState(storage);
    expect(Object.keys(persisted.state ?? {}).sort()).toEqual(['layout', 'settings', 'tabs']);
    expect(persisted.state).not.toHaveProperty('engine');
    expect(persisted.state).not.toHaveProperty('engineActions');

    const second = createAxonClientStore({ storage });
    expect(selectEngineStatus(second.getState())).toBeUndefined();
  });
});
