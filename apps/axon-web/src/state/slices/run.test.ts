import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { defaultCapabilityMatrix } from '../../services/capabilities.ts';
import { SAMPLE_QUERY_SOURCE } from '../../services/query-source.ts';
import type { QueryResultPageRun } from '../../services/query-pagination.ts';
import type {
  CapabilityMatrixRow,
  ExecutionTarget,
  QueryEvent,
  QueryMetricsSummary,
  QueryResultData,
} from '../../services/types.ts';
import {
  selectRun,
  selectRunActions,
  selectRunCapabilities,
  selectRunEvents,
  selectRunIsRunning,
  selectRunLoadingMoreRows,
  selectRunMetrics,
  selectRunPlan,
  selectRunResultData,
  selectRunResultPageRun,
  selectRunState,
} from '../hooks.ts';
import {
  CLIENT_STATE_STORAGE_KEY,
  createAxonClientStore,
  createMemoryClientStateStorage,
} from '../store.ts';

function metrics(overrides: Partial<QueryMetricsSummary> = {}): QueryMetricsSummary {
  return {
    bytes_fetched: 1024,
    duration_ms: 10,
    files_touched: 2,
    files_skipped: 3,
    row_groups_touched: 4,
    row_groups_skipped: 5,
    footer_reads: 1,
    snapshot_bootstrap_duration_ms: 8,
    ...overrides,
  };
}

function result(rows: QueryResultData['rows'], hasMore = false): QueryResultData {
  return {
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'string' },
    ],
    rows,
    row_count: rows.length,
    truncated: hasMore,
    page: {
      offset: 0,
      size: 2,
      returned_rows: rows.length,
      loaded_rows: rows.length,
      has_more: hasMore,
      next_offset: hasMore ? rows.length : undefined,
    },
  };
}

function pageResult(
  offset: number,
  rows: QueryResultData['rows'],
  hasMore = false,
): QueryResultData {
  return {
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'string' },
    ],
    rows,
    row_count: offset + rows.length,
    truncated: hasMore,
    page: {
      offset,
      size: 2,
      returned_rows: rows.length,
      loaded_rows: offset + rows.length,
      has_more: hasMore,
      next_offset: hasMore ? offset + rows.length : undefined,
    },
  };
}

function runFor(
  sql: string,
  target: ExecutionTarget | 'auto' = 'browser_wasm',
): QueryResultPageRun {
  return {
    request: {
      sql,
      table_name: 'events',
      preferred_target: target,
      snapshot_version: 12,
    },
    source: SAMPLE_QUERY_SOURCE,
  };
}

function metricsEvent(summary = metrics()): QueryEvent {
  return {
    kind: 'metrics',
    metrics: summary,
    elapsed_ms: 12,
  };
}

describe('run slice', () => {
  it('guards created, running, event, and terminal mutations by execution ID', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectRunActions(store.getState());
    const rows = result([[1, 'a']]);
    const run = runFor('select * from events');

    actions.createRun('execution-1', 'browser_wasm');
    expect(selectRunState(store.getState())).toEqual({
      status: 'created',
      executionId: 'execution-1',
      target: 'browser_wasm',
      elapsed: 0,
    });

    actions.startRun('execution-1');
    actions.updateRunElapsed('stale-execution', 99);
    actions.appendRunEvent('stale-execution', metricsEvent());
    expect(selectRunState(store.getState())).toEqual({
      status: 'running',
      executionId: 'execution-1',
      target: 'browser_wasm',
      elapsed: 0,
    });
    expect(selectRunEvents(store.getState())).toEqual([]);

    actions.finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'stale-execution',
        target: 'browser_wasm',
        ms: 4,
        rows: 1,
        fallback: null,
      },
      resultData: rows,
      resultPageRun: run,
      metrics: metrics(),
      plan: undefined,
      capabilities: defaultCapabilityMatrix(),
    });
    expect(selectRunState(store.getState())).toMatchObject({
      status: 'running',
      executionId: 'execution-1',
    });

    actions.finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-1',
        target: 'browser_wasm',
        ms: 5,
        rows: 1,
        fallback: null,
      },
      resultData: rows,
      resultPageRun: run,
      metrics: metrics(),
      plan: undefined,
      capabilities: defaultCapabilityMatrix(),
    });
    expect(selectRunState(store.getState())).toMatchObject({
      status: 'completed',
      executionId: 'execution-1',
    });
  });

  it('starts with idle run state and default capabilities', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });

    expect(selectRunState(store.getState())).toEqual({ status: 'idle' });
    expect(selectRunResultData(store.getState())).toBeUndefined();
    expect(selectRunResultPageRun(store.getState())).toBeUndefined();
    expect(selectRunLoadingMoreRows(store.getState())).toBe(false);
    expect(selectRunMetrics(store.getState())).toBeUndefined();
    expect(selectRunEvents(store.getState())).toEqual([]);
    expect(selectRunPlan(store.getState())).toBeUndefined();
    expect(selectRunCapabilities(store.getState())).toEqual(defaultCapabilityMatrix());
    expect(selectRunIsRunning(store.getState())).toBe(false);
  });

  it('starts a run, updates elapsed time, appends events, and captures metrics events', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const summary = metrics({ bytes_fetched: 2048 });

    selectRunActions(store.getState()).createRun('execution-1', 'browser_wasm');
    selectRunActions(store.getState()).startRun('execution-1');
    selectRunActions(store.getState()).updateRunElapsed('execution-1', 160);
    selectRunActions(store.getState()).appendRunEvent('execution-1', {
      kind: 'progress',
      stage: 'planning',
      elapsed_ms: 4,
    });
    selectRunActions(store.getState()).appendRunEvent('execution-1', metricsEvent(summary));

    expect(selectRunState(store.getState())).toEqual({
      status: 'running',
      executionId: 'execution-1',
      target: 'browser_wasm',
      elapsed: 160,
    });
    expect(selectRunEvents(store.getState())).toHaveLength(2);
    expect(selectRunMetrics(store.getState())).toEqual(summary);
    expect(selectRunIsRunning(store.getState())).toBe(true);
  });

  it('stores successful run output and capability state', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const run = runFor('select * from events');
    const summary = metrics({ files_touched: 7 });
    const rows = result([[1, 'a']], true);
    const capabilities: CapabilityMatrixRow[] = defaultCapabilityMatrix().map((row) =>
      row.key === 'deletion_vectors' ? { ...row, browser: 'supported' } : row,
    );

    selectRunActions(store.getState()).createRun('execution-1', 'browser_wasm');
    selectRunActions(store.getState()).startRun('execution-1');
    selectRunActions(store.getState()).finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-1',
        target: 'browser_wasm',
        ms: 42,
        rows: rows.row_count,
        fallback: null,
      },
      resultData: rows,
      resultPageRun: run,
      metrics: summary,
      plan: { tree: 'ProjectionExec' },
      capabilities,
    });

    expect(selectRunState(store.getState())).toEqual({
      status: 'completed',
      executionId: 'execution-1',
      target: 'browser_wasm',
      ms: 42,
      rows: 1,
      fallback: null,
    });
    expect(selectRunResultData(store.getState())).toEqual(rows);
    expect(selectRunResultPageRun(store.getState())).toEqual(run);
    expect(selectRunMetrics(store.getState())).toEqual(summary);
    expect(selectRunPlan(store.getState())).toEqual({ tree: 'ProjectionExec' });
    expect(selectRunCapabilities(store.getState())).toEqual(capabilities);
  });

  it('stores error, cancel, reset, and local-reselect clear transitions', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectRunActions(store.getState());
    const rows = result([[1, 'a']]);
    const run = runFor('select * from events');

    actions.createRun('execution-1', 'native');
    actions.startRun('execution-1');
    actions.finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-1',
        target: 'native',
        ms: 20,
        rows: 1,
        fallback: null,
      },
      resultData: rows,
      resultPageRun: run,
      metrics: metrics(),
      plan: { tree: 'Plan' },
      capabilities: defaultCapabilityMatrix(),
    });

    actions.clearForLocalAccessReselect();
    expect(selectRunState(store.getState())).toEqual({ status: 'idle' });
    expect(selectRunResultData(store.getState())).toBeUndefined();
    expect(selectRunResultPageRun(store.getState())).toBeUndefined();
    expect(selectRunMetrics(store.getState())).toBeUndefined();
    expect(selectRunEvents(store.getState())).toEqual([]);
    expect(selectRunPlan(store.getState())).toBeUndefined();

    actions.createRun('execution-2', 'browser_wasm');
    actions.startRun('execution-2');
    actions.finishRunError({
      status: 'failed',
      executionId: 'execution-2',
      target: 'browser_wasm',
      ms: 15,
      message: 'boom',
      code: 'E_QUERY',
    });
    expect(selectRunState(store.getState())).toEqual({
      status: 'failed',
      executionId: 'execution-2',
      target: 'browser_wasm',
      ms: 15,
      message: 'boom',
      code: 'E_QUERY',
    });

    actions.createRun('execution-3', 'native');
    actions.startRun('execution-3');
    actions.requestRunCancellation('execution-3');
    expect(selectRunState(store.getState())).toMatchObject({
      status: 'cancel_requested',
      executionId: 'execution-3',
    });
    actions.finishRunCancelled({
      status: 'cancelled',
      executionId: 'execution-3',
      target: 'native',
      ms: 7,
    });
    expect(selectRunState(store.getState())).toMatchObject({
      status: 'cancelled',
      executionId: 'execution-3',
    });
    expect(selectRunLoadingMoreRows(store.getState())).toBe(false);
    expect(selectRunResultPageRun(store.getState())).toBeUndefined();

    actions.createRun('execution-4', 'native');
    actions.startRun('execution-4');
    actions.finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-4',
        target: 'native',
        ms: 20,
        rows: 1,
        fallback: null,
      },
      resultData: rows,
      resultPageRun: run,
      metrics: metrics(),
      plan: { tree: 'Plan' },
      capabilities: defaultCapabilityMatrix(),
    });
    actions.resetRun();
    expect(selectRun(store.getState())).toMatchObject({
      runState: { status: 'idle' },
      resultData: undefined,
      resultPageRun: undefined,
      loadingMoreRows: false,
      metrics: undefined,
      events: [],
      plan: undefined,
    });
  });

  it('loads more rows for the same run and discards stale batches', () => {
    const store = createAxonClientStore({ storage: createMemoryClientStateStorage() });
    const actions = selectRunActions(store.getState());
    const run = runFor('select * from events');
    const staleRun = runFor('select * from other_events');

    actions.createRun('execution-1', 'browser_wasm');
    actions.startRun('execution-1');
    actions.finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-1',
        target: 'browser_wasm',
        ms: 10,
        rows: 2,
        fallback: null,
      },
      resultData: result(
        [
          [1, 'a'],
          [2, 'b'],
        ],
        true,
      ),
      resultPageRun: run,
      metrics: metrics(),
      plan: { tree: 'InitialPlan' },
      capabilities: defaultCapabilityMatrix(),
    });

    actions.startLoadMoreRows('page-execution-1');
    expect(selectRunLoadingMoreRows(store.getState())).toBe(true);

    const stale = actions.finishLoadMoreRowsSuccess({
      executionId: 'page-execution-1',
      runForPage: run,
      activeRun: staleRun,
      resultData: pageResult(2, [[3, 'stale']]),
      metrics: metrics({ bytes_fetched: 333 }),
      elapsedMs: 19,
    });

    expect(stale).toEqual({ discarded: true });
    expect(selectRunLoadingMoreRows(store.getState())).toBe(false);
    expect(selectRunResultData(store.getState())?.rows).toEqual([
      [1, 'a'],
      [2, 'b'],
    ]);

    actions.startLoadMoreRows('page-execution-2');
    const merged = actions.finishLoadMoreRowsSuccess({
      executionId: 'page-execution-2',
      runForPage: run,
      activeRun: run,
      resultData: pageResult(2, [[3, 'c']]),
      metrics: metrics({ bytes_fetched: 4096 }),
      plan: { tree: 'NextPlan' },
      elapsedMs: 24,
    });

    expect(merged).toEqual({ discarded: false, rows: 3 });
    expect(selectRunResultData(store.getState())?.rows).toEqual([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ]);
    expect(selectRunState(store.getState())).toMatchObject({
      status: 'completed',
      ms: 24,
      rows: 3,
    });
    expect(selectRunMetrics(store.getState())?.bytes_fetched).toBe(4096);
    expect(selectRunPlan(store.getState())).toEqual({ tree: 'NextPlan' });
  });

  it('does not persist run lifecycle state with client state', () => {
    const storage = createMemoryClientStateStorage();
    const first = createAxonClientStore({ storage });

    selectRunActions(first.getState()).createRun('execution-1', 'browser_wasm');
    selectRunActions(first.getState()).startRun('execution-1');
    selectRunActions(first.getState()).finishRunSuccess({
      runState: {
        status: 'completed',
        executionId: 'execution-1',
        target: 'browser_wasm',
        ms: 10,
        rows: 1,
        fallback: null,
      },
      resultData: result([[1, 'a']]),
      resultPageRun: runFor('select * from events'),
      metrics: metrics(),
      plan: { tree: 'Plan' },
      capabilities: defaultCapabilityMatrix(),
    });

    const persisted = JSON.parse(storage.getItem(CLIENT_STATE_STORAGE_KEY) ?? '{}') as {
      state?: Record<string, unknown>;
    };
    expect(Object.keys(persisted.state ?? {}).sort()).toEqual(['layout', 'settings', 'tabs']);
    expect(persisted.state).not.toHaveProperty('run');
    expect(persisted.state).not.toHaveProperty('runActions');

    const second = createAxonClientStore({ storage });
    expect(selectRunState(second.getState())).toEqual({ status: 'idle' });
    expect(selectRunResultData(second.getState())).toBeUndefined();
  });

  it('keeps elapsed-bearing run subscriptions scoped below the app shell', () => {
    const appSource = readFileSync(new URL('../../editor/App.tsx', import.meta.url), 'utf8');
    const resultsPanelSource = readFileSync(
      new URL('../../editor/components/RunResultsPanel.tsx', import.meta.url),
      'utf8',
    );
    const subscribedRunSelectors = [
      ...appSource.matchAll(/useAxonClientStore\((selectRun[A-Za-z]+)\)/g),
    ].map((match) => match[1]);

    expect(subscribedRunSelectors.sort()).toEqual(['selectRunActions', 'selectRunIsRunning']);
    expect(resultsPanelSource).toContain('useAxonClientStore(selectRunState)');
  });
});
