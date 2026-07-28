import type { QueryClient } from '@tanstack/react-query';
import { purgeCatalogSourcesCache } from '../../query/catalog.ts';
import type { ConnectionMutationResult } from '../../state/slices/connections.ts';

type ConnectionLifecycleMutation = Pick<
  ConnectionMutationResult,
  'discardedSources' | 'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
>;

type ConnectionLifecycleOptions = {
  discardActiveQuerySession?: () => void | Promise<void>;
  unregisterLocalDeltaRuntime?: (registryId: string) => void | Promise<void>;
  reportError?: (message: string, error: unknown) => void;
};

export async function applyConnectionLifecycleCleanup(
  queryClient: QueryClient,
  mutation: ConnectionLifecycleMutation,
  options: ConnectionLifecycleOptions = {},
): Promise<void> {
  const report = options.reportError ?? reportError;
  try {
    await purgeCatalogSourcesCache(queryClient, mutation.discardedSources);
  } catch (error) {
    report('failed to clear catalog connection cache:', error);
  }

  if (mutation.shouldDiscardActiveQuerySession) {
    try {
      await (options.discardActiveQuerySession ?? discardActiveQuerySession)();
    } catch (error) {
      report('failed to discard query session:', error);
    }
  }

  if (mutation.localRegistryIdsToUnregister.length === 0) return;
  const unregister = options.unregisterLocalDeltaRuntime ?? unregisterLocalDeltaRuntime;
  for (const registryId of mutation.localRegistryIdsToUnregister) {
    try {
      await unregister(registryId);
    } catch (error) {
      report(`failed to unregister local Delta runtime ${registryId}:`, error);
    }
  }
}

async function discardActiveQuerySession(): Promise<void> {
  const { discardQuerySession } = await import('../../services/query.ts');
  discardQuerySession();
}

async function unregisterLocalDeltaRuntime(registryId: string): Promise<void> {
  const localDelta = await import('../../services/local-delta.ts');
  await localDelta.unregisterLocalDeltaRuntime(registryId);
}

function reportError(message: string, error: unknown): void {
  console.warn(message, error);
}
