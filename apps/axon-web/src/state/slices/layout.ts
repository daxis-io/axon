export type LayoutState = {
  sidebarW: number;
  resultsH: number;
};

export type LayoutActions = {
  setSidebarW(width: number): void;
  setResultsH(height: number, options?: { viewportHeight?: number }): void;
};

export type LayoutSlice = {
  layout: LayoutState;
  layoutActions: LayoutActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

const SIDEBAR_MIN = 220;
const SIDEBAR_MAX = 480;
const RESULTS_MIN = 80;
const RESULTS_VIEWPORT_CHROME = 240;

export const DEFAULT_LAYOUT_STATE: LayoutState = {
  sidebarW: 264,
  resultsH: 360,
};

function finiteOr(value: number, fallback: number): number {
  return Number.isFinite(value) ? value : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function clampSidebarW(width: number): number {
  return clamp(finiteOr(width, DEFAULT_LAYOUT_STATE.sidebarW), SIDEBAR_MIN, SIDEBAR_MAX);
}

export function clampResultsH(height: number, options?: { viewportHeight?: number }): number {
  const viewportHeight = options?.viewportHeight;
  const max =
    viewportHeight == null || !Number.isFinite(viewportHeight)
      ? Number.POSITIVE_INFINITY
      : Math.max(RESULTS_MIN, viewportHeight - RESULTS_VIEWPORT_CHROME);

  return clamp(finiteOr(height, DEFAULT_LAYOUT_STATE.resultsH), RESULTS_MIN, max);
}

export function createLayoutSlice<TState extends LayoutSlice>(set: StoreSet<TState>): LayoutSlice {
  return {
    layout: { ...DEFAULT_LAYOUT_STATE },
    layoutActions: {
      setSidebarW(width) {
        set((state) => ({
          ...state,
          layout: {
            ...state.layout,
            sidebarW: clampSidebarW(width),
          },
        }));
      },
      setResultsH(height, options) {
        set((state) => ({
          ...state,
          layout: {
            ...state.layout,
            resultsH: clampResultsH(height, options),
          },
        }));
      },
    },
  };
}
