import { QueryClient } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import { queryKeys } from '../../query/keys.ts';
import {
  clearQueryRuntimeState,
  getQueryRuntimeState,
  publishQueryRuntimeState,
} from '../../services/query-runtime-state.ts';
import { SAMPLE_QUERY_SOURCE, type QueryTableSource } from '../../services/query-source.ts';
import type { Catalog } from '../../services/types.ts';
import type { ConnectionMutationResult } from '../../state/slices/connections.ts';
import {
  applyConnectionLifecycleCleanup,
  connectionMutationPending,
  runConnectionMutationLifecycle,
  subscribeConnectionMutationPending,
} from './connection-lifecycle.ts';

const first: QueryTableSource = {
  kind: 'object_store_table_root',
  provider: 'gcs',
  catalogName: 'workspace',
  schemaName: 'analytics',
  tableName: 'events',
  tableUri: 'gs://shared-bucket/events',
  storage: 'gs://shared-bucket/events',
  region: 'browser-local',
};

const sameConnection: QueryTableSource = {
  ...first,
  tableName: 'orders',
  tableUri: 'gs://shared-bucket/orders',
  storage: 'gs://shared-bucket/orders',
};

const otherConnection: QueryTableSource = {
  ...first,
  tableName: 'events',
  tableUri: 'gs://other-bucket/events',
  storage: 'gs://other-bucket/events',
};

function catalog(name: string): Catalog {
  return {
    name,
    region: 'browser-local',
    storage: `gs://${name}`,
    tables: [],
  };
}

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

describe('connection lifecycle cleanup', () => {
  it('awaits canonical-prefix eviction before discarding sessions and local runtimes', async () => {
    clearQueryRuntimeState();
    const client = new QueryClient();
    const order: string[] = [];
    let releaseCancellation!: () => void;
    const cancellationGate = new Promise<void>((resolve) => {
      releaseCancellation = resolve;
    });
    const originalRemove = client.removeQueries.bind(client);
    const originalInvalidate = client.invalidateQueries.bind(client);
    const cancelQueries = vi.spyOn(client, 'cancelQueries').mockImplementation(async () => {
      order.push('cancel');
      await cancellationGate;
    });
    vi.spyOn(client, 'removeQueries').mockImplementation((filters) => {
      order.push('remove');
      return originalRemove(filters);
    });
    vi.spyOn(client, 'invalidateQueries').mockImplementation(async (filters) => {
      order.push('invalidate');
      return originalInvalidate(filters);
    });
    const discardActiveQuerySession = vi.fn(async () => {
      order.push('discard');
    });
    const unregisterLocalDeltaRuntime = vi.fn(async (registryId: string) => {
      order.push(`unregister:${registryId}`);
    });

    client.setQueryData(queryKeys.catalog.tableDerived(first), catalog('first'));
    client.setQueryData(queryKeys.catalog.tableDerived(sameConnection), catalog('same-connection'));
    client.setQueryData(queryKeys.catalog.tableDerived(otherConnection), catalog('other'));
    client.setQueryData(queryKeys.catalog.tableDerived(SAMPLE_QUERY_SOURCE), catalog('sample'));
    client.setQueryData(queryKeys.local.history(), [{ id: 'history' }]);
    publishQueryRuntimeState(
      { source: sameConnection, catalog: catalog('runtime-presentation') },
      5,
    );

    const cleanup = applyConnectionLifecycleCleanup(
      client,
      mutation({
        discardedSources: [first, sameConnection],
        localRegistryIdsToUnregister: ['registry-a', 'registry-b'],
        shouldDiscardActiveQuerySession: true,
      }),
      {
        discardActiveQuerySession,
        unregisterLocalDeltaRuntime,
      },
    );
    await vi.waitFor(() => expect(cancelQueries).toHaveBeenCalledTimes(1));

    expect(order).toEqual(['cancel']);
    expect(discardActiveQuerySession).not.toHaveBeenCalled();
    expect(unregisterLocalDeltaRuntime).not.toHaveBeenCalled();

    releaseCancellation();
    await cleanup;

    expect(order).toEqual([
      'cancel',
      'remove',
      'invalidate',
      'discard',
      'unregister:registry-a',
      'unregister:registry-b',
    ]);
    expect(cancelQueries).toHaveBeenCalledWith({
      queryKey: queryKeys.catalog.connection(first),
      exact: false,
    });
    expect(client.getQueryData(queryKeys.catalog.tableDerived(first))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.tableDerived(sameConnection))).toBeUndefined();
    expect(client.getQueryData(queryKeys.catalog.tableDerived(otherConnection))).toEqual(
      catalog('other'),
    );
    expect(client.getQueryData(queryKeys.catalog.tableDerived(SAMPLE_QUERY_SOURCE))).toEqual(
      catalog('sample'),
    );
    expect(client.getQueryData(queryKeys.local.history())).toEqual([{ id: 'history' }]);
    expect(getQueryRuntimeState(sameConnection)).toBeUndefined();
  });

  it('does not touch runtime teardown when a mutation has nothing to release', async () => {
    const client = new QueryClient();
    const discardActiveQuerySession = vi.fn();
    const unregisterLocalDeltaRuntime = vi.fn();

    await applyConnectionLifecycleCleanup(client, mutation(), {
      discardActiveQuerySession,
      unregisterLocalDeltaRuntime,
    });

    expect(discardActiveQuerySession).not.toHaveBeenCalled();
    expect(unregisterLocalDeltaRuntime).not.toHaveBeenCalled();
  });

  it('serializes a reconnect behind teardown for the prior connection owner', async () => {
    const client = new QueryClient();
    const order: string[] = [];
    let releaseCancellation!: () => void;
    const cancellationGate = new Promise<void>((resolve) => {
      releaseCancellation = resolve;
    });
    vi.spyOn(client, 'cancelQueries').mockImplementation(async () => {
      order.push('cancel');
      await cancellationGate;
    });
    const discardActiveQuerySession = vi.fn(() => {
      order.push('discard');
    });
    const unregisterLocalDeltaRuntime = vi.fn((registryId: string) => {
      order.push(`unregister:${registryId}`);
    });
    const disconnect = vi.fn(() => {
      order.push('disconnect mutation');
      return mutation({
        discardedSources: [first],
        localRegistryIdsToUnregister: ['registry-a'],
        shouldDiscardActiveQuerySession: true,
      });
    });
    const reconnect = vi.fn(() => {
      order.push('reconnect mutation');
      return mutation();
    });
    const firstMountListener = vi.fn();
    const unsubscribeFirstMount = subscribeConnectionMutationPending(client, firstMountListener);

    const disconnecting = runConnectionMutationLifecycle(client, disconnect, {
      discardActiveQuerySession,
      unregisterLocalDeltaRuntime,
    });
    await vi.waitFor(() => expect(client.cancelQueries).toHaveBeenCalledTimes(1));
    const reconnecting = runConnectionMutationLifecycle(client, reconnect);

    expect(reconnect).not.toHaveBeenCalled();
    expect(connectionMutationPending(client)).toBe(true);
    expect(firstMountListener).toHaveBeenCalledTimes(2);
    unsubscribeFirstMount();
    const remountedListener = vi.fn();
    const unsubscribeRemounted = subscribeConnectionMutationPending(client, remountedListener);
    expect(connectionMutationPending(client)).toBe(true);
    const startQuery = vi.fn();
    if (!connectionMutationPending(client)) startQuery();
    expect(startQuery).not.toHaveBeenCalled();
    releaseCancellation();
    await disconnecting;
    await reconnecting;

    expect(connectionMutationPending(client)).toBe(false);
    expect(remountedListener).toHaveBeenCalledTimes(2);
    unsubscribeRemounted();
    if (!connectionMutationPending(client)) startQuery();
    expect(startQuery).toHaveBeenCalledTimes(1);
    expect(order).toEqual([
      'disconnect mutation',
      'cancel',
      'discard',
      'unregister:registry-a',
      'reconnect mutation',
    ]);
  });

  it('still evicts and tears down runtime ownership when cancellation reports an error', async () => {
    const client = new QueryClient();
    const cancellationError = new Error('cancellation failed');
    vi.spyOn(client, 'cancelQueries').mockRejectedValue(cancellationError);
    const discardActiveQuerySession = vi.fn();
    const reportError = vi.fn();
    client.setQueryData(queryKeys.catalog.tableDerived(first), catalog('first'));

    await applyConnectionLifecycleCleanup(
      client,
      mutation({
        discardedSources: [first],
        shouldDiscardActiveQuerySession: true,
      }),
      { discardActiveQuerySession, reportError },
    );

    expect(client.getQueryData(queryKeys.catalog.tableDerived(first))).toBeUndefined();
    expect(discardActiveQuerySession).toHaveBeenCalledTimes(1);
    expect(reportError).toHaveBeenCalledWith(
      'failed to clear catalog connection cache:',
      cancellationError,
    );
  });
});
