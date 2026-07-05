import type { QueryClient } from '@tanstack/react-query';
import { queryOptions } from '@tanstack/react-query';
import { loadCatalog, snapshotCatalog } from '../services/catalog.ts';
import {
  clearQueryRuntimeState,
  subscribeQueryRuntimeState,
} from '../services/query-runtime-state.ts';
import type { QueryTableSource } from '../services/query-source.ts';
import { loadCommits } from '../services/snapshot.ts';
import type { CommitEntry } from '../services/types.ts';
import {
  AXON_CATALOG_QUERY_STALE_TIME_MS,
  AXON_COMMITS_QUERY_STALE_TIME_MS,
  AXON_QUERY_GC_TIME_MS,
  shouldRetryQuery,
} from './client';
import { queryKeys } from './keys';

type BridgeRegistration = {
  refCount: number;
  unsubscribe: () => void;
};

const CATALOG_SOURCE_DISCARD_STATUSES = new Set([401, 403, 419, 440]);
const catalogBridgeRegistrations = new WeakMap<QueryClient, BridgeRegistration>();

export function catalogQueryOptions(source: QueryTableSource) {
  return queryOptions({
    queryKey: queryKeys.catalog.tableDerived(source),
    queryFn: () => loadCatalog(source),
    initialData: snapshotCatalog(source),
    initialDataUpdatedAt: 0,
    staleTime: AXON_CATALOG_QUERY_STALE_TIME_MS,
    gcTime: AXON_QUERY_GC_TIME_MS,
    retry: shouldRetryQuery,
  });
}

export function commitsQueryOptions(source: QueryTableSource) {
  return queryOptions({
    queryKey: queryKeys.catalog.commits(source),
    queryFn: () => loadCommits(source),
    initialData: [] as CommitEntry[],
    initialDataUpdatedAt: 0,
    staleTime: AXON_COMMITS_QUERY_STALE_TIME_MS,
    gcTime: AXON_QUERY_GC_TIME_MS,
    retry: shouldRetryQuery,
  });
}

export function purgeCatalogSourceCache(queryClient: QueryClient, source: QueryTableSource): void {
  queryClient.removeQueries({
    queryKey: queryKeys.catalog.source(source),
    exact: false,
  });
  void queryClient.invalidateQueries({
    queryKey: queryKeys.catalog.source(source),
    exact: false,
  });
  clearQueryRuntimeState(source);
}

export function purgeCatalogSourcesCache(
  queryClient: QueryClient,
  sources: readonly QueryTableSource[],
): void {
  for (const source of sources) {
    purgeCatalogSourceCache(queryClient, source);
  }
}

export function purgeCatalogSourceCacheForError(
  queryClient: QueryClient,
  source: QueryTableSource,
  error: unknown,
): boolean {
  if (!isCatalogSourceDiscardError(error)) {
    return false;
  }

  purgeCatalogSourceCache(queryClient, source);
  return true;
}

export function isCatalogSourceDiscardError(error: unknown): boolean {
  const status = getErrorStatus(error);
  return status !== undefined && CATALOG_SOURCE_DISCARD_STATUSES.has(status);
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function toStatusCode(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isInteger(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isInteger(parsed)) {
      return parsed;
    }
  }

  return undefined;
}

function getErrorStatus(error: unknown, seen = new Set<unknown>()): number | undefined {
  if (!isRecord(error) || seen.has(error)) {
    return undefined;
  }
  seen.add(error);

  const ownStatus = toStatusCode(error.status) ?? toStatusCode(error.statusCode);
  if (ownStatus !== undefined) {
    return ownStatus;
  }

  if (isRecord(error.response)) {
    const responseStatus =
      toStatusCode(error.response.status) ?? toStatusCode(error.response.statusCode);
    if (responseStatus !== undefined) {
      return responseStatus;
    }
  }

  return getErrorStatus(error.cause, seen);
}
