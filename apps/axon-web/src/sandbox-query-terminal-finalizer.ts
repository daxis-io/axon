export type DeferredQueryOutcome<Metadata, Error> =
  | { kind: 'stream_start_failed'; error: Error }
  | { kind: 'stream_fault'; error: Error }
  | { kind: 'stream_terminal'; metadata: Metadata };

export type QueryCleanupResult<Accounting, Error> = {
  accounting?: Accounting;
  error?: Error;
};

export async function finalizeQueryOutcome<Metadata, Error, Accounting>(options: {
  outcome: DeferredQueryOutcome<Metadata, Error>;
  failureKind: 'stream_start_failed' | 'stream_fault';
  closeStream: () => Promise<void>;
  cleanup: () => Promise<QueryCleanupResult<Accounting, Error>>;
  normalizeCloseError: (error: unknown) => Error;
  publish: (
    outcome: DeferredQueryOutcome<Metadata, Error>,
    accounting: Accounting | undefined,
  ) => void;
}): Promise<void> {
  let outcome = options.outcome;
  let closeError: Error | undefined;
  try {
    await options.closeStream();
  } catch (error) {
    closeError = options.normalizeCloseError(error);
  }

  let cleanup: QueryCleanupResult<Accounting, Error>;
  try {
    cleanup = await options.cleanup();
  } catch (error) {
    cleanup = { error: options.normalizeCloseError(error) };
  }

  const finalizationError = cleanup.error ?? closeError;
  if (finalizationError) {
    outcome = { kind: options.failureKind, error: finalizationError };
  }
  options.publish(outcome, cleanup.accounting);
}
