import { QueryClient, isCancelledError } from '@tanstack/react-query';
import { AXON_QUERY_CACHE_MAX_AGE_MS } from './persistence';

const MAX_QUERY_RETRIES = 2;

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

function isCancelledOrAborted(error: unknown): boolean {
  if (isCancelledError(error)) {
    return true;
  }

  if (!isRecord(error)) {
    return false;
  }

  const name = typeof error.name === 'string' ? error.name : undefined;
  return name === 'AbortError' || name === 'CancelledError' || name === 'CanceledError';
}

export function shouldRetryQuery(failureCount: number, error: unknown): boolean {
  if (isCancelledOrAborted(error)) {
    return false;
  }

  const status = getErrorStatus(error);
  if (status !== undefined && status >= 400 && status < 500) {
    return false;
  }

  return failureCount < MAX_QUERY_RETRIES;
}

export function createAxonQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        gcTime: AXON_QUERY_CACHE_MAX_AGE_MS,
        retry: shouldRetryQuery,
      },
    },
  });
}

export const queryClient = createAxonQueryClient();
