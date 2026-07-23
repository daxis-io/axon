import { describe, expect, it } from 'vitest';

import {
  CoordinatorQueryLifecycle,
  MAX_COORDINATOR_REQUESTS,
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
