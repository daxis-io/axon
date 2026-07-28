import type { QueryClient } from '@tanstack/react-query';
import { purgeCatalogSourcesCache } from '../../query/catalog.ts';
import type { ConnectionMutationResult } from '../../state/slices/connections.ts';

type ConnectionLifecycleMutation = Pick<
  ConnectionMutationResult,
  'discardedSources' | 'localRegistryIdsToUnregister' | 'shouldDiscardActiveQuerySession'
>;

type ConnectionLifecycleOptions = {
  cancelActiveExecution?: () => void | Promise<void>;
  discardActiveQuerySession?: () => void | Promise<void>;
  unregisterLocalDeltaRuntime?: (registryId: string) => void | Promise<void>;
  reportError?: (message: string, error: unknown) => void;
};

const mutationQueues = new WeakMap<QueryClient, Promise<void>>();
const pendingMutationCounts = new WeakMap<QueryClient, number>();
const pendingMutationListeners = new WeakMap<QueryClient, Set<() => void>>();

export function connectionMutationPending(queryClient: QueryClient): boolean {
  return (pendingMutationCounts.get(queryClient) ?? 0) > 0;
}

export function subscribeConnectionMutationPending(
  queryClient: QueryClient,
  listener: () => void,
): () => void {
  const listeners = pendingMutationListeners.get(queryClient) ?? new Set<() => void>();
  listeners.add(listener);
  pendingMutationListeners.set(queryClient, listeners);
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      pendingMutationListeners.delete(queryClient);
    }
  };
}

export function runConnectionMutationLifecycle<TMutation extends ConnectionLifecycleMutation>(
  queryClient: QueryClient,
  mutate: () => TMutation,
  options: ConnectionLifecycleOptions = {},
): Promise<TMutation> {
  updatePendingMutationCount(queryClient, 1);
  const previous = mutationQueues.get(queryClient) ?? Promise.resolve();
  const run = previous
    .catch(() => undefined)
    .then(async () => {
      const mutation = mutate();
      await applyConnectionLifecycleCleanup(queryClient, mutation, options);
      return mutation;
    })
    .finally(() => {
      updatePendingMutationCount(queryClient, -1);
    });
  const tail = run.then(
    () => undefined,
    () => undefined,
  );
  mutationQueues.set(queryClient, tail);
  void tail.then(() => {
    if (mutationQueues.get(queryClient) === tail) {
      mutationQueues.delete(queryClient);
    }
  });
  return run;
}

function updatePendingMutationCount(queryClient: QueryClient, delta: 1 | -1): void {
  const next = Math.max(0, (pendingMutationCounts.get(queryClient) ?? 0) + delta);
  if (next === 0) {
    pendingMutationCounts.delete(queryClient);
  } else {
    pendingMutationCounts.set(queryClient, next);
  }
  for (const listener of pendingMutationListeners.get(queryClient) ?? []) {
    listener();
  }
}

export async function applyConnectionLifecycleCleanup(
  queryClient: QueryClient,
  mutation: ConnectionLifecycleMutation,
  options: ConnectionLifecycleOptions = {},
): Promise<void> {
  const report = options.reportError ?? reportError;
  if (mutation.shouldDiscardActiveQuerySession && options.cancelActiveExecution) {
    try {
      await options.cancelActiveExecution();
    } catch (error) {
      report('failed to cancel active editor execution:', error);
    }
  }

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
