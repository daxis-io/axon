// Query connector. Owns the AxonBrowserClient + worker, resolves Delta snapshots
// against the configured fixture, opens the table once, and runs SQL through the
// SDK. Translates worker events + the success envelope into UI-shaped types.

import init, { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';
import {
  AxonWorkerError,
  createAxonBrowserClient,
  redactUrlSecrets,
  type AxonBrowserClient,
  type AxonQueryResult,
  type BrowserHttpFileDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerRangeReadMetricsEvent,
  type BrowserWorkerEventEnvelope,
  type ExecutionTarget,
  type PartitionColumnType,
  type QueryError,
  type QueryMetricsSummary,
} from '../axon-browser-sdk.ts';
import type { CatalogTable, QueryEvent, QueryExecRequest, QueryRunOutcome } from './types.ts';
import { loadLocalDeltaRuntime, releaseLocalDeltaObjectUrls } from './local-delta.ts';
import {
  resolvePublicObjectStorageDescriptor,
  type PublicObjectStorageDescriptorResolutionMetrics,
} from './object-storage.ts';
import {
  defaultQueryPage,
  queryResultPageRequest,
  resultPageFromPreview,
} from './query-pagination.ts';
import { SAMPLE_QUERY_SOURCE, sameQuerySource, type QueryTableSource } from './query-source.ts';

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
  manifest?: FixtureManifest;
  setupMetrics?: SessionSetupMetrics;
  setupMetricsEmitted: boolean;
  snapshot: ResolvedSnapshot;
  tableOpened: boolean;
  source: QueryTableSource;
};

export type SessionSetupMetrics = Pick<
  QueryMetricsSummary,
  | 'descriptor_resolution_count'
  | 'delta_log_manifest_list_count'
  | 'delta_log_manifest_list_duration_ms'
  | 'snapshot_resolve_count'
  | 'snapshot_resolve_duration_ms'
>;

export type SessionSetupMetricsState = {
  setupMetrics?: SessionSetupMetrics;
  setupMetricsEmitted: boolean;
};

let wasmReady: Promise<unknown> | undefined;
let session: SessionState | undefined;
let sessionInit: { source: QueryTableSource; promise: Promise<SessionState> } | undefined;
let sessionGeneration = 0;
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

async function buildSession(source: QueryTableSource): Promise<SessionState> {
  if (source.kind === 'local_delta') {
    const runtime = await loadLocalDeltaRuntime(source.localRegistryId, {
      schemaName: source.schemaName,
      tableName: source.tableName,
    });
    return {
      client: createQueryClient(),
      descriptor: runtime.descriptor,
      setupMetricsEmitted: true,
      snapshot: snapshotFromBrowserDescriptor(runtime.descriptor),
      tableOpened: false,
      source: {
        ...source,
        tableName: runtime.tableName,
        schemaName: runtime.schemaName,
        storage: runtime.storageLabel,
      },
    };
  }

  if (source.kind === 'object_store_table_root') {
    await ensureWasm();
    let setupMetrics = source.descriptorResolutionMetrics
      ? sessionSetupMetricsFromPublicObjectStorage(source.descriptorResolutionMetrics)
      : undefined;
    const descriptor = await resolvePublicObjectStorageDescriptor({
      provider: source.provider,
      tableUri: source.tableUri,
      resolveDeltaSnapshotFromManifest: resolve_delta_snapshot_from_manifest,
      onMetrics: (metrics) => {
        setupMetrics = mergeSessionSetupMetrics(
          setupMetrics,
          sessionSetupMetricsFromPublicObjectStorage(metrics),
        );
      },
    });

    return {
      client: createQueryClient(),
      descriptor,
      setupMetrics,
      setupMetricsEmitted: false,
      snapshot: snapshotFromBrowserDescriptor(descriptor),
      tableOpened: false,
      source,
    };
  }

  await ensureWasm();
  const manifest = await fetchJson<FixtureManifest>(source.manifestUrl);
  const wasmManifest = {
    objects: manifest.objects.map((object) => ({
      relative_path: object.relative_path,
      url: new URL(object.url_path, window.location.href).toString(),
      size_bytes: object.size_bytes,
      etag: object.etag,
    })),
  };
  const snapshotResolveStartedAt = performance.now();
  const snapshotJson = await resolve_delta_snapshot_from_manifest(
    JSON.stringify(wasmManifest),
    manifest.table_uri,
  );
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const descriptor = browserSnapshotDescriptor(snapshot, manifest);

  const client = createQueryClient();

  return {
    client,
    descriptor,
    manifest,
    setupMetrics: {
      descriptor_resolution_count: 1,
      snapshot_resolve_count: 1,
      snapshot_resolve_duration_ms: Math.round(performance.now() - snapshotResolveStartedAt),
    },
    setupMetricsEmitted: false,
    snapshot,
    tableOpened: false,
    source,
  };
}

function createQueryClient(): AxonBrowserClient {
  return createAxonBrowserClient({
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
}

function snapshotFromBrowserDescriptor(
  descriptor: BrowserHttpSnapshotDescriptor,
): ResolvedSnapshot {
  return {
    table_uri: descriptor.table_uri,
    snapshot_version: descriptor.snapshot_version,
    partition_column_types: descriptor.partition_column_types,
    active_files: descriptor.active_files.map((file) => ({
      path: file.path,
      size_bytes: file.size_bytes,
      partition_values: file.partition_values,
      stats: file.stats,
    })),
  };
}

function sessionSetupMetricsFromPublicObjectStorage(
  metrics: PublicObjectStorageDescriptorResolutionMetrics,
): SessionSetupMetrics {
  return {
    descriptor_resolution_count: metrics.descriptor_resolution_count,
    delta_log_manifest_list_count: metrics.delta_log_manifest_list_count,
    delta_log_manifest_list_duration_ms: metrics.delta_log_manifest_list_duration_ms,
    snapshot_resolve_count: metrics.snapshot_resolve_count,
    snapshot_resolve_duration_ms: metrics.snapshot_resolve_duration_ms,
  };
}

export function pendingSessionSetupMetrics(
  state: SessionSetupMetricsState,
): SessionSetupMetrics | undefined {
  if (state.setupMetricsEmitted) return undefined;
  return state.setupMetrics;
}

export function markSessionSetupMetricsEmitted(state: SessionSetupMetricsState): void {
  state.setupMetricsEmitted = true;
}

function addMetric(left: number | undefined, right: number | undefined): number | undefined {
  if (left === undefined) return right;
  if (right === undefined) return left;
  return left + right;
}

function sessionSetupMetricsFromQueryMetrics(
  metrics: QueryMetricsSummary,
): SessionSetupMetrics | undefined {
  if (
    metrics.descriptor_resolution_count === undefined &&
    metrics.delta_log_manifest_list_count === undefined &&
    metrics.delta_log_manifest_list_duration_ms === undefined &&
    metrics.snapshot_resolve_count === undefined &&
    metrics.snapshot_resolve_duration_ms === undefined
  ) {
    return undefined;
  }
  return {
    descriptor_resolution_count: metrics.descriptor_resolution_count,
    delta_log_manifest_list_count: metrics.delta_log_manifest_list_count,
    delta_log_manifest_list_duration_ms: metrics.delta_log_manifest_list_duration_ms,
    snapshot_resolve_count: metrics.snapshot_resolve_count,
    snapshot_resolve_duration_ms: metrics.snapshot_resolve_duration_ms,
  };
}

export function mergeSessionSetupMetrics(
  left: SessionSetupMetrics | undefined,
  right: SessionSetupMetrics | undefined,
): SessionSetupMetrics | undefined {
  if (!left) return right;
  if (!right) return left;
  return {
    descriptor_resolution_count: addMetric(
      left.descriptor_resolution_count,
      right.descriptor_resolution_count,
    ),
    delta_log_manifest_list_count: addMetric(
      left.delta_log_manifest_list_count,
      right.delta_log_manifest_list_count,
    ),
    delta_log_manifest_list_duration_ms: addMetric(
      left.delta_log_manifest_list_duration_ms,
      right.delta_log_manifest_list_duration_ms,
    ),
    snapshot_resolve_count: addMetric(left.snapshot_resolve_count, right.snapshot_resolve_count),
    snapshot_resolve_duration_ms: addMetric(
      left.snapshot_resolve_duration_ms,
      right.snapshot_resolve_duration_ms,
    ),
  };
}

function mergeQueryMetrics(
  metrics: QueryMetricsSummary,
  setupMetrics: SessionSetupMetrics | undefined,
): QueryMetricsSummary {
  const mergedSetupMetrics = mergeSessionSetupMetrics(
    sessionSetupMetricsFromQueryMetrics(metrics),
    setupMetrics,
  );
  if (!mergedSetupMetrics) return metrics;
  return {
    ...metrics,
    ...mergedSetupMetrics,
  };
}

export function queryMetricsFromRangeReadMetricsEvent(
  metrics: BrowserWorkerRangeReadMetricsEvent,
  durationMs: number,
  setupMetrics: SessionSetupMetrics | undefined,
): QueryMetricsSummary {
  return mergeQueryMetrics(
    {
      bytes_fetched: metrics.bytes_fetched,
      duration_ms: durationMs,
      files_touched: metrics.files_touched,
      files_skipped: metrics.files_skipped,
      prebootstrap_fail_open_count: metrics.prebootstrap_fail_open_count,
      prebootstrap_files_pruned: metrics.prebootstrap_files_pruned,
      footer_reads_avoided: metrics.footer_reads_avoided,
      prebootstrap_candidate_files: metrics.prebootstrap_candidate_files,
      row_groups_touched: metrics.row_groups_touched,
      row_groups_skipped: metrics.row_groups_skipped,
      footer_reads: metrics.footer_reads,
      bootstrap_footer_range_reads: metrics.bootstrap_footer_range_reads,
      scan_footer_range_reads: metrics.scan_footer_range_reads,
      scan_data_range_reads: metrics.scan_data_range_reads,
      duplicate_range_reads: metrics.duplicate_range_reads,
      footer_cache_hits: metrics.footer_cache_hits,
      footer_cache_misses: metrics.footer_cache_misses,
      footer_range_reads_avoided: metrics.footer_range_reads_avoided,
      footer_cache_degraded_identity_reads: metrics.footer_cache_degraded_identity_reads,
      identity_present_range_reads: metrics.identity_present_range_reads,
      identity_missing_range_reads: metrics.identity_missing_range_reads,
      rows_emitted: metrics.rows_emitted,
      snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms,
      access_mode: metrics.access_mode,
    },
    setupMetrics,
  );
}

export async function getSession(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<SessionState> {
  if (session && sameQuerySource(session.source, source)) return session;
  if (sessionInit && sameQuerySource(sessionInit.source, source)) return sessionInit.promise;
  discardQuerySession();
  const generation = ++sessionGeneration;
  const t0 = performance.now();
  const promise = buildSession(source)
    .then((s) => {
      if (
        generation !== sessionGeneration ||
        !sessionInit ||
        sessionInit.promise !== promise ||
        !sameQuerySource(sessionInit.source, source)
      ) {
        disposeSession(s);
        throw new DOMException('stale query session discarded', 'AbortError');
      }
      session = s;
      coldStartMs = Math.round(performance.now() - t0);
      sessionSubscribers.forEach((fn) => fn(s));
      return s;
    })
    .finally(() => {
      if (generation === sessionGeneration && sessionInit?.promise === promise) {
        sessionInit = undefined;
      }
    });
  sessionInit = { source, promise };
  return promise;
}

export function getCurrentSession(
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): SessionState | undefined {
  if (!session || !sameQuerySource(session.source, source)) return undefined;
  return session;
}

export function discardQuerySession(source?: QueryTableSource): void {
  let discarded = false;
  if (session && (!source || sameQuerySource(session.source, source))) {
    disposeSession(session);
    session = undefined;
    discarded = true;
  }
  if (sessionInit && (!source || sameQuerySource(sessionInit.source, source))) {
    sessionInit = undefined;
    discarded = true;
  }
  if (discarded) {
    sessionGeneration += 1;
  }
}

function disposeSession(state: SessionState): void {
  state.client.terminate();
  if (state.source.kind === 'local_delta') {
    releaseLocalDeltaObjectUrls(state.source.localRegistryId);
  }
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

function ensureTable(state: SessionState, signal: AbortSignal): Promise<void> {
  if (state.tableOpened) return Promise.resolve();
  const requestId = `editor-open-${++requestCounter}`;
  return state.client
    .openDeltaTable(state.source.tableName, state.descriptor, { requestId })
    .then(() => {
      if (signal.aborted) return;
      state.tableOpened = true;
    });
}

export async function runQuery(
  req: QueryExecRequest,
  onEvent: (event: QueryEvent) => void,
  signal: AbortSignal = new AbortController().signal,
  source: QueryTableSource = SAMPLE_QUERY_SOURCE,
): Promise<QueryRunOutcome> {
  const startedAt = performance.now();
  const since = () => Math.round(performance.now() - startedAt);

  const page = req.page ?? defaultQueryPage();

  try {
    const state = await getSession(source);
    const setupMetrics = pendingSessionSetupMetrics(state);
    if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

    await ensureTable(state, signal);
    if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

    const requestId = `editor-query-${++requestCounter}`;
    let emittedSetupMetricsEvent = false;
    const setupMetricsForEvent = () => {
      if (emittedSetupMetricsEvent) return undefined;
      emittedSetupMetricsEvent = true;
      return setupMetrics;
    };
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
          metrics: queryMetricsFromRangeReadMetricsEvent(m, since(), setupMetricsForEvent()),
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
        state.source.tableName,
        {
          table_uri: state.snapshot.table_uri,
          snapshot_version: req.snapshot_version ?? state.snapshot.snapshot_version,
          sql: req.sql,
          preferred_target: resolvePreferredTarget(req.preferred_target),
          options: {
            collect_metrics: true,
            include_explain: true,
            result_page: queryResultPageRequest(page),
          },
        },
        { requestId },
      );
      if (signal.aborted) throw new DOMException('cancelled', 'AbortError');

      const outcome = {
        status: 'done',
        result: resultPageFromPreview(result.preview, page),
        metrics: mergeQueryMetrics(result.response.metrics, setupMetrics),
        executed_on: result.response.executed_on,
        capabilities: result.response.capabilities,
        fallback_reason: result.fallbackReason ?? result.response.fallback_reason,
        explain: result.response.explain,
        elapsed_ms: since(),
      } satisfies QueryRunOutcome;
      markSessionSetupMetricsEmitted(state);
      return outcome;
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
    name: state.source.tableName,
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
