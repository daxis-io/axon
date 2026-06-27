import type { SourceId } from '../../editor/connect/data.ts';

export type UiToast = {
  message: string;
  kind: 'ok' | 'warn';
};

export type ConnectStep = 1 | 2 | 3;

export type UiState = {
  saveOpen: boolean;
  capsOpen: boolean;
  toast: UiToast | null;
  connectModalOpen: boolean;
  connectInitialStep: ConnectStep;
  connectInitialSource: SourceId | null;
  connectedPanelOpen: boolean;
};

export type UiActions = {
  openSaveDialog(): void;
  closeSaveDialog(): void;
  openCapabilityPopover(): void;
  closeCapabilityPopover(): void;
  toggleCapabilityPopover(): void;
  showToast(message: string, kind?: UiToast['kind']): void;
  clearToast(): void;
  openConnectModal(step?: ConnectStep, source?: SourceId | null): void;
  closeConnectModal(): void;
  openConnectedPanel(): void;
  closeConnectedPanel(): void;
};

export type UiSlice = {
  ui: UiState;
  uiActions: UiActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

export const DEFAULT_UI_STATE: UiState = {
  saveOpen: false,
  capsOpen: false,
  toast: null,
  connectModalOpen: false,
  connectInitialStep: 1,
  connectInitialSource: null,
  connectedPanelOpen: false,
};

function updateUi<TState extends UiSlice>(
  set: StoreSet<TState>,
  update: (ui: UiState) => UiState,
): void {
  set((state) => ({
    ...state,
    ui: update(state.ui),
  }));
}

export function createUiSlice<TState extends UiSlice>(set: StoreSet<TState>): UiSlice {
  return {
    ui: { ...DEFAULT_UI_STATE },
    uiActions: {
      openSaveDialog() {
        updateUi(set, (ui) => ({ ...ui, saveOpen: true }));
      },
      closeSaveDialog() {
        updateUi(set, (ui) => ({ ...ui, saveOpen: false }));
      },
      openCapabilityPopover() {
        updateUi(set, (ui) => ({ ...ui, capsOpen: true }));
      },
      closeCapabilityPopover() {
        updateUi(set, (ui) => ({ ...ui, capsOpen: false }));
      },
      toggleCapabilityPopover() {
        updateUi(set, (ui) => ({ ...ui, capsOpen: !ui.capsOpen }));
      },
      showToast(message, kind = 'ok') {
        updateUi(set, (ui) => ({ ...ui, toast: { message, kind } }));
      },
      clearToast() {
        updateUi(set, (ui) => ({ ...ui, toast: null }));
      },
      openConnectModal(step = 1, source = null) {
        updateUi(set, (ui) => ({
          ...ui,
          connectModalOpen: true,
          connectInitialStep: step,
          connectInitialSource: source,
        }));
      },
      closeConnectModal() {
        updateUi(set, (ui) => ({ ...ui, connectModalOpen: false }));
      },
      openConnectedPanel() {
        updateUi(set, (ui) => ({ ...ui, connectedPanelOpen: true }));
      },
      closeConnectedPanel() {
        updateUi(set, (ui) => ({ ...ui, connectedPanelOpen: false }));
      },
    },
  };
}
