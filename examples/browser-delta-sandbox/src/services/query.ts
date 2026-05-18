// Query connector. Owns the AxonBrowserClient + worker, resolves Delta snapshots
// against the configured fixture, opens the table once, and runs SQL through the
// SDK. Translates worker events + the success envelope into UI-shaped types.

import init, { resolve_delta_snapshot_from_manifest } from '../wasm/browser_delta_sandbox.js';
import {
  AxonWorkerError,
  createAxonBrowserClient,
  redactUrlSecrets,
  type AxonBrowserClient,
  type AxonQueryResult,
  type BrowserHttpFileDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerResultPreview,
  type ExecutionTarget,
  type PartitionColumnType,
  type QueryError,
} from '../axon-browser-sdk.ts';
import type {
  CatalogTable,
  QueryEvent,
  QueryExecRequest,
  QueryResultData,
  QueryRunOutcome,
  ResultCell,
  ResultColumn,
} from './types.ts';

const TABLE_NAME = 'axon_table';
const DEFAULT_FIXTURE_MANIFEST = '/fixtures/prod-like/delta-log-manifest.json';

type FixtureObject = {
  relative_path: string;
  url_path: string;
  kind?: string;
  size_bytes?: number;
  etag?: string;
};

type FixtureDataFile = {
  relative_path: string;
  url_path: string;
  size_bytes: number;
  partition_values: Record<string, string>;
};

type FixtureManifest = {
  name?: string;
  table_uri: string;
  expected_latest_version?: number;
  checkpoint_version?: number;
  objects: FixtureObject[];
  data_files?: FixtureDataFile[];
};

type ResolvedSnapshot = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  active_files: Array<{
    path: string;
    size_bytes: number;
    partition_values: Record<string, string | null>;
    stats?: string;
  }>;
};

type EventHandler = (envelope: BrowserWorkerEventEnvelope) => void;

type SessionState = {
  client: AxonBrowserClient;
  descriptor: BrowserHttpSnapshotDescriptor;
  manifest: FixtureManifest;
  snapshot: ResolvedSnapshot;
  tableOpened: boolean;
  manifestUrl: string;
};

let wasmReady: Promise<unknown> | undefined;
let session: SessionState | undefined;
let sessionInit: Promise<SessionState> | undefined;
let requestCounter = 0;
let coldStartMs: number | undefined;

// Returns the wall-clock time from module load to the first successful session
// bootstrap, in milliseconds. undefined before bootstrap completes.
export function getColdStartMs(): number | undefined {
  return coldStartMs;
}

const sessionSubscribers = new Set<(state: SessionState) => void>();
const eventListeners = new Set<EventHandler>();

// Exposed for cross-cutting consumers (engine status, etc.) that need every event,
// not just the per-query subset that runQuery() filters by request_id.
export function subscribeWorkerEvents(handler: EventHandler): () => void {
  eventListeners.add(handler);
  return () => {
    eventListeners.delete(handler);
  };
}

function ensureWasm(): Promise<unknown> {
  wasmReady ??= init();
  return wasmReady;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`failed to load ${url} (${response.status})`);
  }
  return (await response.json()) as T;
}

function browserSnapshotDescriptor(
  snapshot: ResolvedSnapshot,
  manifest: FixtureManifest,
): BrowserHttpSnapshotDescriptor {
  const fileIndex = new Map(manifest.data_files?.map((f) => [f.relative_path, f]) ?? []);
  const active: BrowserHttpFileDescriptor[] = snapshot.active_files.map((file) => {
    const data = fileIndex.get(file.path);
    if (!data) {
      throw new Error(`active file '${file.path}' missing from fixture data_files`);
    }
    return {
      path: file.path,
      url: new URL(data.url_path, window.location.href).toString(),
      size_bytes: file.size_bytes,
      partition_values: file.partition_values,
      stats: file.stats,
    };
  });
  return {
    table_uri: snapshot.table_uri,
    snapshot_version: snapshot.snapshot_version,
    partition_column_types: snapshot.partition_column_types ?? {},
    browser_compatibility: { capabilities: {} },
    required_capabilities: { capabilities: {} },
    active_files: active,
  };
}

async function buildSession(manifestUrl: string): Promise<SessionState> {
  await ensureWasm();
  const manifest = await fetchJson<FixtureManifest>(manifestUrl);
  const wasmManifest = {
    objects: manifest.objects.map((object) => ({
      relative_path: object.relative_path,
      url: new URL(object.url_path, window.location.href).toString(),
      size_bytes: object.size_bytes,
      etag: object.etag,
    })),
  };
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    manifest.table_uri,
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const descriptor = browserSnapshotDescriptor(snapshot, manifest);

  const client = createAxonBrowserClient({
    worker: () =>
      new Worker(new URL('../sandbox-query-worker.ts', import.meta.url), {
        type: 'module',
        name: 'axon-editor-query-worker',
      }),
    requestId: () => `editor-request-${++requestCounter}`,
    onEvent: (envelope) => {
      for (const handler of eventListeners) handler(envelope);
    },
  });

  return {
    client,
    descriptor,
    manifest,
    snapshot,
    tableOpened: false,
    manifestUrl,
  };
}

export async function getSession(
  manifestUrl: string = DEFAULT_FIXTURE_MANIFEST,
): Promise<SessionState> {
  if (session?.manifestUrl === manifestUrl) return session;
  if (!sessionInit) {
    const t0 = performance.now();
    sessionInit = buildSession(manifestUrl).then((s) => {
      session = s;
      coldStartMs = Math.round(performance.now() - t0);
      sessionSubscribers.forEach((fn) => fn(s));
      return s;
    });
  }
  return sessionInit;
}

export function getCurrentSession(): SessionState | undefined {
  return session;
}

export function subscribeSession(listener: (state: SessionState) => void): () => void {
  sessionSubscribers.add(listener);
  if (session) listener(session);
  return () => {
    sessionSubscribers.delete(listener);
  };
}

// ─── Run a query ────────────────────────────────────────────────────────────

function resolvePreferredTarget(input: QueryExecRequest['preferred_target']): ExecutionTarget {
  // SDK QueryRequest requires a concrete ExecutionTarget; 'auto' maps to
  // browser_wasm so the router gets the chance to keep work in-browser first.
  if (input === 'native') return 'native';
  return 'browser_wasm';
}

function previewToResultData(preview: BrowserWorkerResultPreview | undefined): QueryResultData {
  if (!preview) {
    return { columns: [], rows: [], row_count: 0, truncated: false };
  }
  const columns: ResultColumn[] = preview.columns.map((name, idx) => ({
    name,
    type: inferTypeFromColumn(preview.rows, idx),
  }));
  return {
    columns,
    rows: preview.rows.map((row) => row as ResultCell[]),
    row_count: preview.row_count,
    truncated: preview.truncated,
  };
}

function inferTypeFromColumn(rows: BrowserWorkerResultPreview['rows'], idx: number): string {
  for (const row of rows) {
    const v = row[idx];
    if (v == null) continue;
    if (typeof v === 'number') return Number.isInteger(v) ? 'integer' : 'double';
    if (typeof v === 'boolean') return 'boolean';
    return 'string';
  }
  return 'string';
}

function ensureTable(state: SessionState, signal: AbortSignal): Promise<void> {
  if (state.tableOpened) return Promise.resolve();
  const requestId = `editor-open-${++requestCounter}`;
  return state.client.openDeltaTable(TABLE_NAME, state.descriptor, { requestId }).then(() => {
    if (signal.aborted) return;
    state.tableOpened = true;
  });
}

export async function runQuery(
  req: QueryExecRequest,
  onEvent: (event: QueryEvent) => void,
  signal: AbortSignal = new AbortController().signal,
  manifestUrl: string = DEFAULT_FIXTURE_MANIFEST,
): Promise<QueryRunOutcome> {
  const startedAt = performance.now();
  const since = () => Math.round(performance.now() - startedAt);

  try {
    const state = await getSession(manifestUrl);
    if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

    await ensureTable(state, signal);
    if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

    const requestId = `editor-query-${++requestCounter}`;
    const handler: EventHandler = (envelope) => {
      if ('progress' in envelope) {
        if (envelope.progress.context.request_id !== requestId) return;
        onEvent({ kind: 'progress', stage: envelope.progress.stage, elapsed_ms: since() });
      } else if ('log' in envelope) {
        if (envelope.log.context.request_id !== requestId) return;
        onEvent({
          kind: 'log',
          level: envelope.log.level,
          message: redactUrlSecrets(envelope.log.message),
          elapsed_ms: since(),
        });
      } else if ('range_read_metrics' in envelope) {
        if (envelope.range_read_metrics.context.request_id !== requestId) return;
        const m = envelope.range_read_metrics;
        onEvent({
          kind: 'metrics',
          metrics: {
            bytes_fetched: m.bytes_fetched,
            duration_ms: since(),
            files_touched: m.files_touched,
            files_skipped: m.files_skipped,
            row_groups_touched: m.row_groups_touched,
            row_groups_skipped: m.row_groups_skipped,
            footer_reads: m.footer_reads,
            rows_emitted: m.rows_emitted,
            snapshot_bootstrap_duration_ms: m.snapshot_bootstrap_duration_ms,
            access_mode: m.access_mode,
          },
          elapsed_ms: since(),
        });
      } else if ('fallback' in envelope) {
        if (envelope.fallback.context.request_id !== requestId) return;
        onEvent({ kind: 'fallback', reason: envelope.fallback.reason, elapsed_ms: since() });
      }
    };
    eventListeners.add(handler);

    try {
      const result: AxonQueryResult = await state.client.query(
        TABLE_NAME,
        {
          table_uri: state.snapshot.table_uri,
          snapshot_version: req.snapshot_version ?? state.snapshot.snapshot_version,
          sql: req.sql,
          preferred_target: resolvePreferredTarget(req.preferred_target),
          options: { collect_metrics: true, include_explain: true },
        },
        { requestId },
      );
      if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

      return {
        status: 'done',
        result: previewToResultData(result.preview),
        metrics: result.response.metrics,
        executed_on: result.response.executed_on,
        capabilities: result.response.capabilities,
        fallback_reason: result.fallbackReason ?? result.response.fallback_reason,
        explain: result.response.explain,
        elapsed_ms: since(),
      };
    } finally {
      eventListeners.delete(handler);
    }
  } catch (err) {
    if (signal.aborted) {
      return {
        status: 'error',
        message: 'Query cancelled',
        code: 'cancelled',
        elapsed_ms: since(),
      };
    }
    if (err instanceof AxonWorkerError) {
      const qe: QueryError = err.queryError;
      return {
        status: 'error',
        message: qe.message,
        code: qe.code,
        target: qe.target,
        fallback_reason: qe.fallback_reason,
        elapsed_ms: since(),
      };
    }
    return {
      status: 'error',
      message: err instanceof Error ? err.message : String(err),
      elapsed_ms: since(),
    };
  }
}

// ─── Catalog derivation ─────────────────────────────────────────────────────
// Phase 1 supports a single table per session (matches the existing browser
// sandbox). Phase 2 will fan out to multi-table catalogs.

export function deriveCatalogTable(state: SessionState): CatalogTable {
  const { snapshot } = state;
  const partitionTypes = snapshot.partition_column_types ?? {};
  const partitionColumns = Object.entries(partitionTypes).map(([name, type]) => ({
    name,
    type: type ?? 'string',
    pruning: 'stats' as const,
  }));
  const totalBytes = snapshot.active_files.reduce((acc, f) => acc + f.size_bytes, 0);

  return {
    name: TABLE_NAME,
    uri: snapshot.table_uri,
    kind: 'delta',
    snapshot: snapshot.snapshot_version,
    size_bytes: totalBytes,
    row_count: 0, // TODO Phase 2: parse Delta log metadata for true row count
    file_count: snapshot.active_files.length,
    row_group_count: 0, // TODO Phase 2: surface from Parquet preflight
    partition_columns: partitionColumns,
    // TODO Phase 2: parse protocol action from Delta log
    protocol: { minReaderVersion: 2, minWriterVersion: 5, features: [] },
    columns: inferColumnsFromSnapshot(snapshot, partitionTypes),
  };
}

function inferColumnsFromSnapshot(
  snapshot: ResolvedSnapshot,
  partitionTypes: Partial<Record<string, PartitionColumnType>>,
): CatalogTable['columns'] {
  // Without a parsed Delta metadata action, derive a minimal column set from
  // partition columns + stats keys. Phase 2 will parse the metaData action for
  // canonical schemas.
  const cols: CatalogTable['columns'] = [];
  for (const [name, type] of Object.entries(partitionTypes)) {
    cols.push({ name, type: type ?? 'string', role: 'partition' });
  }
  const stats = snapshot.active_files[0]?.stats;
  if (stats) {
    try {
      const parsed = JSON.parse(stats) as { minValues?: Record<string, unknown> };
      for (const key of Object.keys(parsed.minValues ?? {})) {
        if (cols.some((c) => c.name === key)) continue;
        cols.push({ name: key, type: 'unknown', role: 'data' });
      }
    } catch {
      // stats may not be JSON for older protocols
    }
  }
  return cols;
}
