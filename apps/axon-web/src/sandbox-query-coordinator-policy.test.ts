import { describe, expect, it } from 'vitest';

import {
  CoordinatorMemoryBudget,
  CoordinatorQueryLifecycle,
  MAX_COORDINATOR_REQUESTS,
  MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
  coordinatorHasCapacity,
  coordinatorMaxArrowIpcBytes,
} from './sandbox-query-coordinator-policy';
import type { BrowserWorkerSqlCommand } from './axon-browser-sdk';

describe('sandbox query coordinator policy', () => {
  it('preserves a validated raw 16 MiB output budget for both delivery modes', () => {
    for (const delivery of ['single_buffer', 'chunked_buffers'] as const) {
      expect(coordinatorMaxArrowIpcBytes(sqlCommand(false, 16 * 1024 * 1024, delivery))).toBe(
        16 * 1024 * 1024,
      );
    }
  });

  it('clamps browser-safe defaults to the 8 MiB browser budget', () => {
    expect(coordinatorMaxArrowIpcBytes(sqlCommand(true, 16 * 1024 * 1024))).toBe(8 * 1024 * 1024);
  });

  it('fails closed when a raw command omits or corrupts its output budget', () => {
    expect(() => coordinatorMaxArrowIpcBytes(sqlCommand(false, undefined))).toThrow(
      'raw sandbox SQL commands must provide a positive max_arrow_ipc_bytes',
    );
    expect(() => coordinatorMaxArrowIpcBytes(sqlCommand(false, Number.NaN))).toThrow(
      'raw sandbox SQL commands must provide a positive max_arrow_ipc_bytes',
    );
  });

  it('bounds active and forwarded coordinator work together', () => {
    expect(coordinatorHasCapacity(MAX_COORDINATOR_REQUESTS - 1, 0)).toBe(true);
    expect(coordinatorHasCapacity(MAX_COORDINATOR_REQUESTS - 1, 1)).toBe(false);
    expect(coordinatorHasCapacity(0, MAX_COORDINATOR_REQUESTS)).toBe(false);
  });

  it('reserves declared output weight and tracks aggregate staging high-water marks', () => {
    const budget = new CoordinatorMemoryBudget(MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES);
    const half = MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES / 2;

    expect(budget.tryReserve('query-1', half)).toBe(true);
    expect(budget.tryReserve('query-2', half)).toBe(true);
    expect(budget.tryReserve('query-3', 1)).toBe(false);
    expect(budget.snapshot()).toEqual({
      limitBytes: MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
      currentReservedBytes: MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
      currentStagedBytes: 0,
      peakReservedBytes: MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
      peakStagedBytes: 0,
    });

    budget.recordStaged('query-1', 4 * 1024 * 1024);
    budget.recordStaged('query-2', 8 * 1024 * 1024);
    expect(budget.snapshot().currentStagedBytes).toBe(12 * 1024 * 1024);
    expect(budget.snapshot().peakStagedBytes).toBe(12 * 1024 * 1024);

    budget.release('query-1');
    expect(budget.snapshot()).toMatchObject({
      currentReservedBytes: half,
      currentStagedBytes: 8 * 1024 * 1024,
    });
    budget.release('query-2');
    expect(budget.snapshot()).toEqual({
      limitBytes: MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
      currentReservedBytes: 0,
      currentStagedBytes: 0,
      peakReservedBytes: MAX_COORDINATOR_STAGED_ARROW_IPC_BYTES,
      peakStagedBytes: 12 * 1024 * 1024,
    });
  });

  it('rejects invalid coordinator memory ownership transitions', () => {
    const budget = new CoordinatorMemoryBudget(8);

    expect(budget.tryReserve('query-1', 8)).toBe(true);
    expect(() => budget.tryReserve('query-1', 1)).toThrow('already has a memory reservation');
    expect(() => budget.recordStaged('missing-query', 1)).toThrow(
      'does not have a memory reservation',
    );
    expect(() => budget.recordStaged('query-1', 9)).toThrow(
      'would exceed its declared reservation',
    );
    budget.release('query-1');
    expect(() => budget.release('query-1')).toThrow('does not have a memory reservation');
  });

  it('makes the first terminal transition authoritative until the child drains', () => {
    const lifecycle = new CoordinatorQueryLifecycle();

    expect(lifecycle.canCommit).toBe(true);
    expect(lifecycle.beginDrain('deadline_exceeded')).toBe(true);
    expect(lifecycle.state).toBe('draining');
    expect(lifecycle.terminalStatus).toBe('deadline_exceeded');
    expect(lifecycle.canCommit).toBe(false);
    expect(lifecycle.beginDrain('succeeded')).toBe(false);

    lifecycle.finishDrain();
    expect(lifecycle.state).toBe('finished');
    expect(lifecycle.canCommit).toBe(false);
  });
});

function sqlCommand(
  browserSafeDefaults: boolean,
  maxArrowIpcBytes: number | undefined,
  delivery: BrowserWorkerSqlCommand['delivery'] = 'chunked_buffers',
): BrowserWorkerSqlCommand {
  return {
    request_id: 'query-policy',
    name: 'events',
    query: {
      table_uri: 'https://example.com/table',
      snapshot_version: 1,
      sql: 'SELECT * FROM events',
      preferred_target: 'browser_wasm',
      options: {
        result_page: { limit: 100, offset: 0 },
        runtime_limits: {
          max_result_rows: 100,
          ...(maxArrowIpcBytes === undefined ? {} : { max_arrow_ipc_bytes: maxArrowIpcBytes }),
          max_preview_string_bytes: 1024,
        },
      },
    },
    output: 'arrow_ipc_stream',
    delivery,
    browser_safe_defaults: browserSafeDefaults,
  };
}
