import { describe, expect, it, vi } from 'vitest';
import type { QueryTableSource } from '../services/query-source.ts';
import type { ConnectionMutationResult } from '../state/slices/connections.ts';
import * as ConnectPageModule from './ConnectPage.tsx';

type ConnectPageSideEffectsModule = {
  applyConnectPageMutationSideEffects?: (
    mutation: Pick<
      ConnectionMutationResult,
      'discardedSources' | 'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
    >,
    unregisterMessage: string,
    options: {
      discardActiveQuerySession?: () => void;
      purgeCatalogSources?: (sources: QueryTableSource[]) => void;
      unregisterLocalDeltaRuntimeIds?: (registryIds: string[], message: string) => void;
    },
  ) => void;
};

const source: QueryTableSource = {
  kind: 'manifest',
  catalogName: 'catalog-a',
  schemaName: 'schema-a',
  tableName: 'table-a',
  manifestUrl: '/manifest-a.json',
  storage: 'gs://bucket/table-a',
  region: 'browser-local',
};

function mutation(
  patch: Partial<ConnectionMutationResult> = {},
): Pick<
  ConnectionMutationResult,
  'discardedSources' | 'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
> {
  return {
    discardedSources: [],
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
    const purgeCatalogSources = vi.fn();
    const unregisterLocalDeltaRuntimeIds = vi.fn();

    expect(applySideEffects).toEqual(expect.any(Function));

    applySideEffects?.(
      mutation({
        discardedSources: [source],
        localRegistryIdsToUnregister: ['local-registry-id'],
        shouldDiscardActiveQuerySession: true,
      }),
      'failed to unregister local Delta catalog:',
      { discardActiveQuerySession, purgeCatalogSources, unregisterLocalDeltaRuntimeIds },
    );

    expect(discardActiveQuerySession).toHaveBeenCalledTimes(1);
    expect(purgeCatalogSources).toHaveBeenCalledWith([source]);
    expect(unregisterLocalDeltaRuntimeIds).toHaveBeenCalledWith(
      ['local-registry-id'],
      'failed to unregister local Delta catalog:',
    );
  });
});
