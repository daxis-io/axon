import { describe, expect, it, vi } from 'vitest';
import type { ConnectionMutationResult } from '../state/slices/connections.ts';
import * as ConnectPageModule from './ConnectPage.tsx';

type ConnectPageSideEffectsModule = {
  applyConnectPageMutationSideEffects?: (
    mutation: Pick<
      ConnectionMutationResult,
      'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
    >,
    unregisterMessage: string,
    options: {
      discardActiveQuerySession?: () => void;
      unregisterLocalDeltaRuntimeIds?: (registryIds: string[], message: string) => void;
    },
  ) => void;
};

function mutation(
  patch: Partial<ConnectionMutationResult> = {},
): Pick<
  ConnectionMutationResult,
  'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
> {
  return {
    localRegistryIdsToUnregister: [],
    shouldDiscardActiveQuerySession: false,
    ...patch,
  };
}

describe('ConnectPage connection mutation side effects', () => {
  it('discards the active query session when a connection mutation asks for discard', () => {
    const applySideEffects = (ConnectPageModule as ConnectPageSideEffectsModule)
      .applyConnectPageMutationSideEffects;
    const discardActiveQuerySession = vi.fn();
    const unregisterLocalDeltaRuntimeIds = vi.fn();

    expect(applySideEffects).toEqual(expect.any(Function));

    applySideEffects?.(
      mutation({
        localRegistryIdsToUnregister: ['local-registry-id'],
        shouldDiscardActiveQuerySession: true,
      }),
      'failed to unregister local Delta catalog:',
      { discardActiveQuerySession, unregisterLocalDeltaRuntimeIds },
    );

    expect(discardActiveQuerySession).toHaveBeenCalledTimes(1);
    expect(unregisterLocalDeltaRuntimeIds).toHaveBeenCalledWith(
      ['local-registry-id'],
      'failed to unregister local Delta catalog:',
    );
  });
});
