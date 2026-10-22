import type { QueryError } from '../axon-browser-sdk.ts';

const QUERY_SESSION_INVALIDATED_MARKER = 'browser query session invalidated:';

export function querySessionInvalidationMessage(reason: string): string {
  return `${QUERY_SESSION_INVALIDATED_MARKER} ${reason}`;
}

export function appendQuerySessionInvalidation(message: string, reason: string): string {
  return `${message}; ${querySessionInvalidationMessage(reason)}`;
}

export function isQuerySessionInvalidation(error: Pick<QueryError, 'code' | 'message'>): boolean {
  return (
    error.code === 'execution_failed' && error.message.includes(QUERY_SESSION_INVALIDATED_MARKER)
  );
}

// The editor opens its table before it queries, so a worker that reports the table missing has had
// its session replaced underneath the page. Only a reopen recovers, so treat it as invalidation.
export function isMissingOpenTableError(error: Pick<QueryError, 'code' | 'message'>): boolean {
  return (
    error.code === 'invalid_request' &&
    /^browser DataFusion session does not have an open table named '.*'$/.test(error.message)
  );
}
