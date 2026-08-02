import { describe, expect, it, vi } from 'vitest';

import {
  finalizeQueryOutcome,
  type DeferredQueryOutcome,
} from './sandbox-query-terminal-finalizer.ts';

type TestError = { code: string; resource?: string };
type TestMetadata = { status: 'succeeded' | 'cancelled' | 'deadline_exceeded' };

describe('sandbox query terminal finalizer', () => {
  it.each<{
    name: string;
    outcome: DeferredQueryOutcome<TestMetadata, TestError>;
    failureKind: 'stream_start_failed' | 'stream_fault';
  }>([
    {
      name: 'stream start failure',
      outcome: { kind: 'stream_start_failed', error: { code: 'execution_failed' } },
      failureKind: 'stream_start_failed',
    },
    {
      name: 'stream pump fault',
      outcome: { kind: 'stream_fault', error: { code: 'execution_failed' } },
      failureKind: 'stream_fault',
    },
    {
      name: 'cancellation terminal',
      outcome: { kind: 'stream_terminal', metadata: { status: 'cancelled' } },
      failureKind: 'stream_fault',
    },
    {
      name: 'deadline terminal',
      outcome: { kind: 'stream_terminal', metadata: { status: 'deadline_exceeded' } },
      failureKind: 'stream_fault',
    },
  ])('publishes $name only after stream and spill cleanup', async ({ outcome, failureKind }) => {
    const events: string[] = [];
    let namespacePresent = true;
    const publish = vi.fn(
      (
        delivered: DeferredQueryOutcome<TestMetadata, TestError>,
        accounting: { active_files: number; scopes_deleted: number } | undefined,
      ) => {
        events.push('publish');
        expect(namespacePresent).toBe(false);
        expect(delivered).toEqual(outcome);
        expect(accounting).toEqual({ active_files: 0, scopes_deleted: 1 });
      },
    );

    await finalizeQueryOutcome({
      outcome,
      failureKind,
      closeStream: async () => {
        events.push('close');
      },
      cleanup: async () => {
        events.push('cleanup');
        namespacePresent = false;
        return { accounting: { active_files: 0, scopes_deleted: 1 } };
      },
      normalizeCloseError: () => ({ code: 'execution_failed' }),
      publish,
    });

    expect(events).toEqual(['close', 'cleanup', 'publish']);
    expect(publish).toHaveBeenCalledOnce();
  });

  it('replaces a terminal outcome with structured spill cleanup failure', async () => {
    const cleanupError = { code: 'resource_exhausted', resource: 'spill_storage' };
    const publish = vi.fn();

    await finalizeQueryOutcome({
      outcome: { kind: 'stream_terminal', metadata: { status: 'succeeded' } },
      failureKind: 'stream_fault',
      closeStream: async () => undefined,
      cleanup: async () => ({
        accounting: { active_files: 1, scopes_deleted: 0 },
        error: cleanupError,
      }),
      normalizeCloseError: () => ({ code: 'execution_failed' }),
      publish,
    });

    expect(publish).toHaveBeenCalledWith(
      { kind: 'stream_fault', error: cleanupError },
      { active_files: 1, scopes_deleted: 0 },
    );
  });
});
