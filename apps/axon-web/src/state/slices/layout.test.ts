import { describe, expect, it } from 'vitest';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';
import { DEFAULT_LAYOUT_STATE } from './layout.ts';

describe('layout slice', () => {
  it('uses the current editor layout defaults', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(store.getState().layout).toEqual(DEFAULT_LAYOUT_STATE);
    expect(store.getState().layout).toEqual({
      sidebarW: 264,
      resultsH: 360,
    });
  });

  it('clamps the sidebar width to the supported range', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    store.getState().layoutActions.setSidebarW(120);
    expect(store.getState().layout.sidebarW).toBe(220);

    store.getState().layoutActions.setSidebarW(640);
    expect(store.getState().layout.sidebarW).toBe(480);

    store.getState().layoutActions.setSidebarW(336);
    expect(store.getState().layout.sidebarW).toBe(336);
  });

  it('clamps the results height to the supported range and optional viewport', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    store.getState().layoutActions.setResultsH(20);
    expect(store.getState().layout.resultsH).toBe(80);

    store.getState().layoutActions.setResultsH(900, { viewportHeight: 1_000 });
    expect(store.getState().layout.resultsH).toBe(760);

    store.getState().layoutActions.setResultsH(260, { viewportHeight: 400 });
    expect(store.getState().layout.resultsH).toBe(160);
  });

  it('persists layout without persisting actions or server state', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });

    first.getState().layoutActions.setSidebarW(380);
    first.getState().layoutActions.setResultsH(900, { viewportHeight: 1_000 });

    const raw = storage.getItem(CLIENT_STATE_STORAGE_KEY);
    expect(raw).toBeTypeOf('string');

    const persisted = JSON.parse(raw ?? '{}') as { state?: Record<string, unknown> };
    expect(Object.keys(persisted.state ?? {}).sort()).toEqual(['layout', 'settings', 'tabs']);
    expect(persisted.state).not.toHaveProperty('layoutActions');
    expect(persisted.state).not.toHaveProperty('settingsActions');

    const second = createAxonClientStore({ storage });
    expect(second.getState().layout).toEqual({ sidebarW: 380, resultsH: 760 });
    expect(second.getState().layoutActions.setSidebarW).toEqual(expect.any(Function));
  });

  it('keeps layout updates working when persistence writes fail', () => {
    const storage = {
      getItem: () => null,
      setItem: () => {
        throw new Error('storage quota exceeded');
      },
      removeItem: () => undefined,
    };
    const store = createAxonClientStore({ storage });

    expect(() => store.getState().layoutActions.setSidebarW(320)).not.toThrow();
    expect(store.getState().layout.sidebarW).toBe(320);
  });
});
