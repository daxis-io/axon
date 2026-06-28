import type { EngineStatus } from '../../services/types.ts';

export type EngineState = {
  status: EngineStatus | undefined;
};

export type EngineActions = {
  setStatus(status: EngineStatus): void;
};

export type EngineSlice = {
  engine: EngineState;
  engineActions: EngineActions;
};

type StoreSet<TState> = (
  partial: Partial<TState> | ((state: TState) => Partial<TState>),
  replace?: false,
) => void;

export const DEFAULT_ENGINE_STATE: EngineState = {
  status: undefined,
};

export function createEngineSlice<TState extends EngineSlice>(set: StoreSet<TState>): EngineSlice {
  return {
    engine: { ...DEFAULT_ENGINE_STATE },
    engineActions: {
      setStatus(status) {
        set((state) => ({
          ...state,
          engine: {
            ...state.engine,
            status,
          },
        }));
      },
    },
  };
}
