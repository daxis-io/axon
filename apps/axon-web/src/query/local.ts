import type { QueryClient } from '@tanstack/react-query';
import { queryOptions } from '@tanstack/react-query';
import { appendHistory, loadHistory } from '../services/history.ts';
import { loadSaved, saveQuery } from '../services/saved.ts';
import type { HistoryEntry, SavedQuery } from '../services/types.ts';
import {
  AXON_LOCAL_METADATA_QUERY_STALE_TIME_MS,
  AXON_QUERY_GC_TIME_MS,
  shouldRetryQuery,
} from './client';
import { queryKeys } from './keys';

const MAX_HISTORY_ENTRIES = 100;

export type NewHistoryEntry = Parameters<typeof appendHistory>[0];
export type NewSavedQuery = Parameters<typeof saveQuery>[0];

export function historyQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.local.history(),
    queryFn: loadHistory,
    initialData: [] as HistoryEntry[],
    initialDataUpdatedAt: 0,
    staleTime: AXON_LOCAL_METADATA_QUERY_STALE_TIME_MS,
    gcTime: AXON_QUERY_GC_TIME_MS,
    retry: shouldRetryQuery,
  });
}

export function savedQueriesQueryOptions() {
  return queryOptions({
    queryKey: queryKeys.local.saved(),
    queryFn: loadSaved,
    initialData: [] as SavedQuery[],
    initialDataUpdatedAt: 0,
    staleTime: AXON_LOCAL_METADATA_QUERY_STALE_TIME_MS,
    gcTime: AXON_QUERY_GC_TIME_MS,
    retry: shouldRetryQuery,
  });
}

export async function appendHistoryEntry(
  queryClient: QueryClient,
  input: NewHistoryEntry,
): Promise<HistoryEntry> {
  const entry = await appendHistory(input);
  queryClient.setQueryData(queryKeys.local.history(), (current: HistoryEntry[] | undefined) =>
    [entry, ...(current ?? []).filter((candidate) => candidate.id !== entry.id)].slice(
      0,
      MAX_HISTORY_ENTRIES,
    ),
  );
  return entry;
}

export async function saveSavedQuery(
  queryClient: QueryClient,
  input: NewSavedQuery,
): Promise<SavedQuery> {
  const entry = await saveQuery(input);
  queryClient.setQueryData(queryKeys.local.saved(), (current: SavedQuery[] | undefined) => [
    entry,
    ...(current ?? []).filter((candidate) => candidate.name !== entry.name),
  ]);
  return entry;
}
