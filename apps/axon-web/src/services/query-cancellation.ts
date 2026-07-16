import type { QueryError } from '../axon-browser-sdk.ts';

const BROWSER_DATAFUSION_CANCELLATION_PREFIX = 'experimental browser DataFusion query cancelled';

export function isBrowserDataFusionCancellation(
  error: Pick<QueryError, 'code' | 'message'>,
): boolean {
  return (
    error.code === 'execution_failed' &&
    error.message.startsWith(BROWSER_DATAFUSION_CANCELLATION_PREFIX)
  );
}
