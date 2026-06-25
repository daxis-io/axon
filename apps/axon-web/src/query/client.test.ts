import { CancelledError, QueryClient } from '@tanstack/react-query';
import { describe, expect, it } from 'vitest';
import { createAxonQueryClient, queryClient, shouldRetryQuery } from './client';

describe('shouldRetryQuery', () => {
  it('does not retry aborted requests', () => {
    const error = Object.assign(new Error('aborted'), { name: 'AbortError' });

    expect(shouldRetryQuery(0, error)).toBe(false);
  });

  it('does not retry cancelled requests', () => {
    const error = Object.assign(new Error('cancelled'), { name: 'CancelledError' });

    expect(shouldRetryQuery(0, error)).toBe(false);
  });

  it('does not retry TanStack cancelled requests', () => {
    expect(shouldRetryQuery(0, new CancelledError())).toBe(false);
  });

  it('does not retry known client errors', () => {
    expect(shouldRetryQuery(0, { status: 400 })).toBe(false);
    expect(shouldRetryQuery(0, { status: 401 })).toBe(false);
    expect(shouldRetryQuery(0, { response: { status: 403 } })).toBe(false);
    expect(shouldRetryQuery(0, { statusCode: 404 })).toBe(false);
  });

  it('retries unknown and server-style errors for the first two retry attempts', () => {
    expect(shouldRetryQuery(0, new Error('network unavailable'))).toBe(true);
    expect(shouldRetryQuery(1, { status: 503 })).toBe(true);
    expect(shouldRetryQuery(2, new Error('still unavailable'))).toBe(false);
  });
});

describe('createAxonQueryClient', () => {
  it('creates query clients with the Axon retry policy', () => {
    const client = createAxonQueryClient();

    expect(client).toBeInstanceOf(QueryClient);
    expect(client.getDefaultOptions().queries?.retry).toBe(shouldRetryQuery);
  });

  it('exports a stable default query client', () => {
    expect(queryClient).toBeInstanceOf(QueryClient);
    expect(queryClient.getDefaultOptions().queries?.retry).toBe(shouldRetryQuery);
  });
});
