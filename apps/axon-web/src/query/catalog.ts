import type { QueryClient } from '@tanstack/react-query';
import { queryOptions } from '@tanstack/react-query';
import { loadCatalog, snapshotCatalog } from '../services/catalog.ts';
import { subscribeQueryRuntimeState } from '../services/query-runtime-state.ts';
import type { QueryTableSource } from '../services/query-source.ts';
import { loadCommits } from '../services/snapshot.ts';
import type { CommitEntry } from '../services/types.ts';
import { queryKeys } from './keys';

type BridgeRegistration = {
  refCount: number;
  unsubscribe: () => void;
};

const catalogBridgeRegistrations = new WeakMap<QueryClient, BridgeRegistration>();

export function catalogQueryOptions(source: QueryTableSource) {
  return queryOptions({
    queryKey: queryKeys.catalog.tableDerived(source),
    queryFn: () => loadCatalog(source),
    initialData: snapshotCatalog(source),
  });
}

export function commitsQueryOptions(source: QueryTableSource) {
  return queryOptions({
    queryKey: queryKeys.catalog.commits(source),
    queryFn: () => loadCommits(source),
    initialData: [] as CommitEntry[],
  });
}

export function installCatalogQueryBridge(queryClient: QueryClient): () => void {
  let registration = catalogBridgeRegistrations.get(queryClient);
  if (!registration) {
    registration = {
      refCount: 0,
      unsubscribe: subscribeQueryRuntimeState((state) => {
        queryClient.setQueryData(queryKeys.catalog.tableDerived(state.source), state.catalog);
        void queryClient.invalidateQueries({
          queryKey: queryKeys.catalog.commits(state.source),
          exact: true,
        });
      }),
    };
    catalogBridgeRegistrations.set(queryClient, registration);
  }

  registration.refCount += 1;
  let active = true;

  return () => {
    if (!active) return;
    active = false;

    const current = catalogBridgeRegistrations.get(queryClient);
    if (!current) return;

    current.refCount -= 1;
    if (current.refCount === 0) {
      current.unsubscribe();
      catalogBridgeRegistrations.delete(queryClient);
    }
  };
}
