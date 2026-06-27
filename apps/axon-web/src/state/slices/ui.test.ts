import { describe, expect, it } from 'vitest';
import { selectUi, selectUiActions } from '../hooks.ts';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';

const DEFAULT_UI_STATE = {
  saveOpen: false,
  capsOpen: false,
  toast: null,
  connectModalOpen: false,
  connectInitialStep: 1,
  connectInitialSource: null,
  connectedPanelOpen: false,
};

function persistedClientState(storage: ReturnType<typeof createMemoryClientStateStorage>) {
  return JSON.parse(storage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}') as {
    state?: Record<string, unknown>;
  };
}

describe('ui slice', () => {
  it('uses closed shell defaults without a toast or selected connect source', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(selectUi(store.getState())).toEqual(DEFAULT_UI_STATE);
  });

  it('updates shell dialog, popover, toast, connect modal, and connected-panel state only', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const initial = store.getState();
    const actions = selectUiActions(initial);

    actions.openSaveDialog();
    actions.closeSaveDialog();
    actions.openCapabilityPopover();
    actions.toggleCapabilityPopover();
    actions.showToast('Saved');
    actions.showToast('Reconnect local folder', 'warn');
    actions.clearToast();
    actions.openConnectModal();
    actions.closeConnectModal();
    actions.openConnectedPanel();
    actions.closeConnectedPanel();

    const next = store.getState();
    expect(selectUi(next)).toEqual(DEFAULT_UI_STATE);
    expect(next.layout).toBe(initial.layout);
    expect(next.settings).toBe(initial.settings);
    expect(next.connections).toBe(initial.connections);
    expect(next.tabs).toBe(initial.tabs);
  });

  it('defaults connect modal entry state and preserves local reselect entry state', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectUiActions(store.getState());

    actions.openConnectModal();

    expect(selectUi(store.getState())).toMatchObject({
      connectModalOpen: true,
      connectInitialStep: 1,
      connectInitialSource: null,
    });

    actions.closeConnectModal();
    actions.openConnectModal(2, 'local');

    expect(selectUi(store.getState())).toMatchObject({
      connectModalOpen: true,
      connectInitialStep: 2,
      connectInitialSource: 'local',
    });
  });

  it('does not persist ui shell state or actions with client state', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });
    const actions = selectUiActions(first.getState());

    actions.openSaveDialog();
    actions.openCapabilityPopover();
    actions.showToast('Connected');
    actions.openConnectModal(2, 'local');
    actions.openConnectedPanel();

    const persisted = persistedClientState(storage);
    expect(Object.keys(persisted.state ?? {}).sort()).toEqual(['layout', 'settings', 'tabs']);
    expect(persisted.state).not.toHaveProperty('ui');
    expect(persisted.state).not.toHaveProperty('uiActions');

    const second = createAxonClientStore({ storage });
    expect(selectUi(second.getState())).toEqual(DEFAULT_UI_STATE);
    expect(selectUiActions(second.getState()).openConnectModal).toEqual(expect.any(Function));
  });
});
