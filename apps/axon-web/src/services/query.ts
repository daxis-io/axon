// Query connector. Owns the AxonBrowserClient + worker, resolves Delta snapshots
// against the configured fixture, opens the table once, and runs SQL through the
// SDK. Translates worker events + the success envelope into UI-shaped types.

import { create, equals } from '@bufbuild/protobuf';
import { timestampFromMs } from '@bufbuild/protobuf/wkt';
import init, { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';
import {
  AxonWorkerError,
  createAxonBrowserClient,
  redactUrlSecrets,
  type AxonBrowserClient,
  type AxonQueryRequestOptions,
  type AxonQueryResult,
  type BrowserHttpFileDescriptor,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerRangeReadMetricsEvent,
  type BrowserWorkerEventEnvelope,
  type CapabilityKey,
  type CapabilityState,
  type PartitionColumnType,
  type QueryError,
  type QueryExecutionOptions,
  type QueryMetricsSummary,
  type QueryResultPageRequest,
} from '../axon-browser-sdk.ts';
import {
  BrowserHttpSnapshotDescriptorSchema as ContractBrowserHttpSnapshotDescriptorSchema,
  CapabilityKey as ContractCapabilityKey,
  CapabilityState as ContractCapabilityState,
  PartitionColumnType as ContractPartitionColumnType,
  type BrowserHttpSnapshotDescriptor as ContractBrowserHttpSnapshotDescriptor,
  type CapabilityReport as ContractCapabilityReport,
  type ResolvedBrowserRead,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ExecuteRequestSchema,
  ExecutionTarget as ContractExecutionTarget,
  QueryExecutionOptionsSchema,
  QueryRequestSchema,
  QueryResultPageSchema,
  QueryRuntimeLimitsSchema,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import type {
  CatalogTable,
  QueryEvent,
  QueryExecRequest,
  QueryRunError,
  QueryRunOutcome,
} from './types.ts';
import { isCurrentLocalDeltaObjectUrl } from './local-delta.ts';
import {
  lookupPublicObjectStorageRuntimeCache,
  resolvePublicObjectStorageDescriptor,
  type PublicObjectStorageDescriptorResolutionMetrics,
} from './object-storage.ts';
import {
  defaultQueryPage,
  queryResultPageRequest,
  resultPageFromPreview,
} from './query-pagination.ts';
import {
  clearQueryRuntimeState,
  publishQueryRuntimeState,
  publishWorkerEvent,
} from './query-runtime-state.ts';
import { isBrowserDataFusionCancellation } from './query-cancellation.ts';
import { isQuerySessionInvalidation } from './query-session-invalidation.ts';
import {
  sameQuerySource,
  type AvailableQuerySourceSelection,
  type QueryTableSource,
} from './query-source.ts';
import {
  canonicalTableForSelection,
  dataAccessResolverForSelection,
} from './browser-read-resolution.ts';
import {
  validateBrowserExecuteInput,
  type BrowserExecuteInput,
} from './browser-execution-provider.ts';
import {
  executionCancelSpanId,
  executionOpenSpanId,
  executionRequestId,
  type ExecutionAdmissionInput,
} from './execution-lifecycle.ts';
import type { Catalog } from './types.ts';

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
  contractDescriptor?: ContractBrowserHttpSnapshotDescriptor;
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
  | 'descriptor_cache_hit'
>;

export type SessionSetupMetricsState = {
  setupMetrics?: SessionSetupMetrics;
  setupMetricsEmitted: boolean;
};

let wasmReady: Promise<unknown> | undefined;
let session: SessionState | undefined;
let sessionInit:
  | {
      source: QueryTableSource;
      contractDescriptor?: ContractBrowserHttpSnapshotDescriptor;
      promise: Promise<SessionState>;
    }
  | undefined;
let sessionGeneration = 0;
let coldStartMs: number | undefined;

// Returns the wall-clock time from module load to the first successful session
// bootstrap, in milliseconds. undefined before bootstrap completes.
export function getColdStartMs(): number | undefined {
  return coldStartMs;
}

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
  if (!wasmReady) {
    wasmReady = init();
  }
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

async function buildSession(
  source: QueryTableSource,
  browserRead?: ResolvedBrowserRead,
): Promise<SessionState> {
  if (source.kind === 'local_delta') {
    if (browserRead?.descriptor?.descriptor.case !== 'snapshot') {
      throw new Error('local Delta session requires a resolved snapshot descriptor');
    }
    const contractDescriptor = browserRead.descriptor.descriptor.value;
    const descriptor = sdkSnapshotDescriptor(contractDescriptor);
    return {
      client: createQueryClient(),
      descriptor,
      contractDescriptor,
      setupMetricsEmitted: true,
      snapshot: snapshotFromBrowserDescriptor(descriptor),
      tableOpened: false,
      source,
    };
  }

  if (source.kind === 'object_store_table_root') {
    await ensureWasm();
    let setupMetrics = source.descriptorResolutionMetrics
      ? sessionSetupMetricsFromPublicObjectStorage(source.descriptorResolutionMetrics)
      : undefined;
    const cached = lookupPublicObjectStorageRuntimeCache({
      provider: source.provider,
      tableUri: source.tableUri,
      region: source.region,
      snapshot: { kind: 'latest' },
      expectedSnapshotVersion: source.snapshot,
    });
    let descriptor: BrowserHttpSnapshotDescriptor;
    if (cached) {
      descriptor = cached.descriptor;
      setupMetrics = mergeSessionSetupMetrics(setupMetrics, { descriptor_cache_hit: 1 });
    } else {
      descriptor = await resolvePublicObjectStorageDescriptor({
        provider: source.provider,
        tableUri: source.tableUri,
        region: source.region,
        resolveDeltaSnapshotFromManifest: resolve_delta_snapshot_from_manifest,
        onMetrics: (metrics) => {
          setupMetrics = mergeSessionSetupMetrics(
            setupMetrics,
            sessionSetupMetricsFromPublicObjectStorage(metrics),
          );
        },
      });
    }

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
    requestId: () => {
      throw new Error('editor worker commands require an execution-scoped request ID');
    },
    onEvent: (envelope) => {
      publishWorkerEvent(envelope);
      for (const handler of eventListeners) handler(envelope);
    },
  });
}

function sdkSnapshotDescriptor(
  descriptor: ContractBrowserHttpSnapshotDescriptor,
): BrowserHttpSnapshotDescriptor {
  if (descriptor.snapshotVersion === undefined) {
    throw new Error('resolved snapshot descriptor omitted snapshot_version');
  }
  return {
    table_uri: descriptor.tableUri,
    snapshot_version: safeContractInteger(descriptor.snapshotVersion, 'snapshot_version'),
    partition_column_types: Object.fromEntries(
      Object.entries(descriptor.partitionColumnTypes).map(([name, value]) => [
        name,
        sdkPartitionColumnType(value),
      ]),
    ),
    browser_compatibility: sdkCapabilityReport(descriptor.browserCompatibility),
    required_capabilities: sdkCapabilityReport(descriptor.requiredCapabilities),
    active_files: descriptor.activeFiles.map((file) => ({
      path: file.path,
      url: file.url,
      size_bytes: safeContractInteger(file.sizeBytes, 'size_bytes'),
      partition_values: Object.fromEntries(
        Object.entries(file.partitionValues).map(([name, value]) => [
          name,
          value.value.case === 'nullValue'
            ? null
            : value.value.case === 'stringValue'
              ? value.value.value
              : null,
        ]),
      ),
      stats: file.stats,
      object_etag: file.objectEtag,
    })),
  };
}

function sdkPartitionColumnType(value: ContractPartitionColumnType): PartitionColumnType {
  switch (value) {
    case ContractPartitionColumnType.INT64:
      return 'int64';
    case ContractPartitionColumnType.BOOLEAN:
      return 'boolean';
    case ContractPartitionColumnType.UNSUPPORTED:
      return 'unsupported';
    case ContractPartitionColumnType.STRING:
    case ContractPartitionColumnType.UNSPECIFIED:
      return 'string';
  }
}

function sdkCapabilityReport(report: ContractCapabilityReport | undefined): {
  capabilities: Partial<Record<CapabilityKey, CapabilityState>>;
} {
  const capabilities: Partial<Record<CapabilityKey, CapabilityState>> = {};
  for (const entry of report?.capabilities ?? []) {
    const key = sdkCapabilityKey(entry.key);
    const state = sdkCapabilityState(entry.state);
    if (key && state) capabilities[key] = state;
  }
  return { capabilities };
}

function sdkCapabilityKey(value: ContractCapabilityKey): CapabilityKey | undefined {
  switch (value) {
    case ContractCapabilityKey.CHANGE_DATA_FEED:
      return 'change_data_feed';
    case ContractCapabilityKey.COLUMN_MAPPING:
      return 'column_mapping';
    case ContractCapabilityKey.DELETION_VECTORS:
      return 'deletion_vectors';
    case ContractCapabilityKey.MULTI_PARTITION_EXECUTION:
      return 'multi_partition_execution';
    case ContractCapabilityKey.PROXY_ACCESS:
      return 'proxy_access';
    case ContractCapabilityKey.RANGE_READS:
      return 'range_reads';
    case ContractCapabilityKey.SIGNED_URL_ACCESS:
      return 'signed_url_access';
    case ContractCapabilityKey.TIME_TRAVEL:
      return 'time_travel';
    case ContractCapabilityKey.TIMESTAMP_NTZ:
      return 'timestamp_ntz';
    case ContractCapabilityKey.UNKNOWN_PROTOCOL_FEATURES:
      return 'unknown_protocol_features';
    case ContractCapabilityKey.UNSPECIFIED:
      return undefined;
  }
}

function sdkCapabilityState(value: ContractCapabilityState): CapabilityState | undefined {
  switch (value) {
    case ContractCapabilityState.SUPPORTED:
      return 'supported';
    case ContractCapabilityState.NATIVE_ONLY:
      return 'native_only';
    case ContractCapabilityState.UNSUPPORTED:
      return 'unsupported';
    case ContractCapabilityState.EXPERIMENTAL:
      return 'experimental';
    case ContractCapabilityState.UNSPECIFIED:
      return undefined;
  }
}

function safeContractInteger(value: bigint, field: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new Error(`resolved browser descriptor ${field} is outside JavaScript-safe range`);
  }
  return number;
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
    metrics.snapshot_resolve_duration_ms === undefined &&
    metrics.descriptor_cache_hit === undefined
  ) {
    return undefined;
  }
  return {
    descriptor_resolution_count: metrics.descriptor_resolution_count,
    delta_log_manifest_list_count: metrics.delta_log_manifest_list_count,
    delta_log_manifest_list_duration_ms: metrics.delta_log_manifest_list_duration_ms,
    snapshot_resolve_count: metrics.snapshot_resolve_count,
    snapshot_resolve_duration_ms: metrics.snapshot_resolve_duration_ms,
    descriptor_cache_hit: metrics.descriptor_cache_hit,
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
    descriptor_cache_hit: addMetric(left.descriptor_cache_hit, right.descriptor_cache_hit),
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
      coalesced_range_reads: metrics.coalesced_range_reads,
      coalesced_gap_bytes_fetched: metrics.coalesced_gap_bytes_fetched,
      scan_overfetch_bytes: metrics.scan_overfetch_bytes,
      coordinator_peak_staged_bytes: metrics.coordinator_peak_staged_bytes,
      coordinator_staging_limit_bytes: metrics.coordinator_staging_limit_bytes,
      cursor_peak_pending_encoded_bytes: metrics.cursor_peak_pending_encoded_bytes,
      cursor_peak_transport_chunk_bytes: metrics.cursor_peak_transport_chunk_bytes,
      footer_cache_hits: metrics.footer_cache_hits,
      footer_cache_misses: metrics.footer_cache_misses,
      footer_range_reads_avoided: metrics.footer_range_reads_avoided,
      footer_cache_degraded_identity_reads: metrics.footer_cache_degraded_identity_reads,
      identity_present_range_reads: metrics.identity_present_range_reads,
      identity_missing_range_reads: metrics.identity_missing_range_reads,
      range_cache_hits: metrics.range_cache_hits,
      range_cache_misses: metrics.range_cache_misses,
      range_cache_bytes_reused: metrics.range_cache_bytes_reused,
      range_cache_bytes_stored: metrics.range_cache_bytes_stored,
      range_cache_validation_misses: metrics.range_cache_validation_misses,
      range_cache_degraded_identity_reads: metrics.range_cache_degraded_identity_reads,
      range_readahead_requests: metrics.range_readahead_requests,
      range_readahead_bytes_fetched: metrics.range_readahead_bytes_fetched,
      range_readahead_bytes_used: metrics.range_readahead_bytes_used,
      range_readahead_wasted_bytes: metrics.range_readahead_wasted_bytes,
      descriptor_cache_hit: metrics.descriptor_cache_hit,
      session_reuse_count: metrics.session_reuse_count,
      opened_table_reuse_count: metrics.opened_table_reuse_count,
      identity_refresh_count: metrics.identity_refresh_count,
      access_envelope_refresh_count: metrics.access_envelope_refresh_count,
      rows_emitted: metrics.rows_emitted,
      snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms,
      access_mode: metrics.access_mode,
      arrow_ipc_bytes: metrics.arrow_ipc_bytes,
      arrow_ipc_chunk_count: metrics.arrow_ipc_chunk_count,
      preview_rows: metrics.preview_rows,
      preview_string_bytes: metrics.preview_string_bytes,
      planning_duration_ms: metrics.planning_duration_ms,
      arrow_ipc_encode_duration_ms: metrics.arrow_ipc_encode_duration_ms,
      preview_duration_ms: metrics.preview_duration_ms,
    },
    setupMetrics,
  );
}

export async function getSession(
  source: QueryTableSource,
  browserRead?: ResolvedBrowserRead,
): Promise<SessionState> {
  const contractDescriptor = resolvedSnapshotDescriptor(browserRead);
  if (session && sameSessionResolution(session, source, contractDescriptor)) return session;
  if (sessionInit && sameSessionResolution(sessionInit, source, contractDescriptor)) {
    return sessionInit.promise;
  }
  discardQuerySession();
  const generation = ++sessionGeneration;
  const t0 = performance.now();
  const promise = buildSession(source, browserRead)
    .then((s) => {
      if (
        generation !== sessionGeneration ||
        !sessionInit ||
        sessionInit.promise !== promise ||
        !sameSessionResolution(sessionInit, source, contractDescriptor)
      ) {
        disposeSession(s);
        throw new DOMException('stale query session discarded', 'AbortError');
      }
      session = s;
      coldStartMs = Math.round(performance.now() - t0);
      publishQueryRuntimeState(
        {
          source: s.source,
          catalog: catalogFromSession(s),
          manifest: s.manifest,
        },
        coldStartMs,
      );
      return s;
    })
    .finally(() => {
      if (generation === sessionGeneration && sessionInit?.promise === promise) {
        sessionInit = undefined;
      }
    });
  sessionInit = { source, contractDescriptor, promise };
  return promise;
}

function resolvedSnapshotDescriptor(
  browserRead: ResolvedBrowserRead | undefined,
): ContractBrowserHttpSnapshotDescriptor | undefined {
  return browserRead?.descriptor?.descriptor.case === 'snapshot'
    ? browserRead.descriptor.descriptor.value
    : undefined;
}

function sameSessionResolution(
  candidate: {
    source: QueryTableSource;
    contractDescriptor?: ContractBrowserHttpSnapshotDescriptor;
  },
  source: QueryTableSource,
  contractDescriptor: ContractBrowserHttpSnapshotDescriptor | undefined,
): boolean {
  if (!sameQuerySource(candidate.source, source)) return false;
  if (source.kind !== 'local_delta') return true;
  return (
    candidate.contractDescriptor !== undefined &&
    contractDescriptor !== undefined &&
    equals(
      ContractBrowserHttpSnapshotDescriptorSchema,
      candidate.contractDescriptor,
      contractDescriptor,
    )
  );
}

export function getCurrentSession(source: QueryTableSource): SessionState | undefined {
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
    clearQueryRuntimeState(source);
  }
}

function disposeSession(state: SessionState): void {
  state.client.terminate();
}

// ─── Run a query ────────────────────────────────────────────────────────────

function ensureTable(
  state: SessionState,
  signal: AbortSignal,
  executionId: string,
  input?: BrowserExecuteInput,
): Promise<void> {
  if (input) {
    validateBrowserExecuteInput(input, {
      isCurrentLocalObjectUrl: isCurrentLocalDeltaObjectUrl,
    });
  }
  if (state.tableOpened) return Promise.resolve();
  const requestId = executionOpenSpanId(executionId, 1);
  return state.client
    .openDeltaTable(input?.table.name ?? state.source.tableName, state.descriptor, { requestId })
    .then(() => {
      if (signal.aborted) return;
      state.tableOpened = true;
    });
}

function cancellationError(): DOMException {
  return new DOMException('cancelled', 'AbortError');
}

function throwIfCancelled(signal: AbortSignal): void {
  if (signal.aborted) throw cancellationError();
}

function waitForExecutionStage<T>(
  work: Promise<T>,
  signal: AbortSignal,
  deadlineSignal?: AbortSignal,
): Promise<T> {
  if (deadlineSignal?.aborted) {
    return Promise.reject(new ExecutionDeadlineError('execution deadline expired'));
  }
  if (signal.aborted) return Promise.reject(cancellationError());
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => {
      signal.removeEventListener('abort', cancel);
      deadlineSignal?.removeEventListener('abort', deadline);
    };
    const cancel = () => {
      cleanup();
      reject(cancellationError());
    };
    const deadline = () => {
      cleanup();
      reject(new ExecutionDeadlineError('execution deadline expired'));
    };
    signal.addEventListener('abort', cancel, { once: true });
    deadlineSignal?.addEventListener('abort', deadline, { once: true });
    work.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error: unknown) => {
        cleanup();
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

function waitForExecutionDeadline<T>(work: Promise<T>, deadlineSignal?: AbortSignal): Promise<T> {
  if (!deadlineSignal) return work;
  if (deadlineSignal.aborted) {
    return Promise.reject(new ExecutionDeadlineError('execution deadline expired'));
  }
  return new Promise<T>((resolve, reject) => {
    const deadline = () => reject(new ExecutionDeadlineError('execution deadline expired'));
    deadlineSignal.addEventListener('abort', deadline, { once: true });
    work.then(
      (value) => {
        deadlineSignal.removeEventListener('abort', deadline);
        resolve(value);
      },
      (error: unknown) => {
        deadlineSignal.removeEventListener('abort', deadline);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

export type CancelableQueryStages<TSession, TResult> = {
  signal: AbortSignal;
  deadlineSignal?: AbortSignal;
  remainingTime?: () => number;
  getSession: (remainingMs: number) => Promise<TSession>;
  openTable: (session: TSession, remainingMs: number) => Promise<void>;
  execute: (session: TSession, remainingMs: number) => Promise<TResult>;
  cancelQuery: (session: TSession) => void;
};

class ExecutionDeadlineError extends Error {}

function throwIfExecutionDeadline(signal?: AbortSignal): void {
  if (signal?.aborted) throw new ExecutionDeadlineError('execution deadline expired');
}

function stageRemainingTime(remainingTime: (() => number) | undefined): number {
  if (!remainingTime) return Number.POSITIVE_INFINITY;
  const remainingMs = remainingTime();
  if (!Number.isFinite(remainingMs) || remainingMs <= 0) {
    throw new ExecutionDeadlineError('execution deadline expired');
  }
  return remainingMs;
}

export async function runCancelableQueryStages<TSession, TResult>(
  stages: CancelableQueryStages<TSession, TResult>,
): Promise<{ session: TSession; result: TResult }> {
  throwIfExecutionDeadline(stages.deadlineSignal);
  throwIfCancelled(stages.signal);
  const session = await waitForExecutionStage(
    stages.getSession(stageRemainingTime(stages.remainingTime)),
    stages.signal,
    stages.deadlineSignal,
  );
  throwIfExecutionDeadline(stages.deadlineSignal);
  throwIfCancelled(stages.signal);
  await waitForExecutionStage(
    stages.openTable(session, stageRemainingTime(stages.remainingTime)),
    stages.signal,
    stages.deadlineSignal,
  );
  throwIfExecutionDeadline(stages.deadlineSignal);
  throwIfCancelled(stages.signal);

  let queryStarted = false;
  const cancelStartedQuery = () => {
    if (!queryStarted) return;
    try {
      stages.cancelQuery(session);
    } catch {
      // The worker outcome or lifecycle deadline remains authoritative.
    }
  };
  stages.signal.addEventListener('abort', cancelStartedQuery);
  try {
    throwIfExecutionDeadline(stages.deadlineSignal);
    throwIfCancelled(stages.signal);
    queryStarted = true;
    const result = await waitForExecutionDeadline(
      stages.execute(session, stageRemainingTime(stages.remainingTime)),
      stages.deadlineSignal,
    );
    return { session, result };
  } finally {
    stages.signal.removeEventListener('abort', cancelStartedQuery);
  }
}

export function queryExecutionOptionsForAdmission(
  admission: ExecutionAdmissionInput,
  resultPage: QueryResultPageRequest,
): QueryExecutionOptions {
  return {
    collect_metrics: true,
    include_explain: true,
    result_page: resultPage,
    runtime_limits: {
      max_result_rows: admission.budgets.maxResultRows,
      max_arrow_ipc_bytes: admission.budgets.maxArrowIpcBytes,
      max_preview_string_bytes: admission.budgets.maxPreviewStringBytes,
      ...(admission.budgets.maxScanBytes === undefined
        ? {}
        : { max_scan_bytes: admission.budgets.maxScanBytes }),
    },
  };
}

export function queryClientOptionsForAdmission(
  admission: ExecutionAdmissionInput,
): AxonQueryRequestOptions {
  return { requestId: executionRequestId(admission.executionId), delivery: 'single_buffer' };
}

class BrowserReadResolutionFailure extends Error {
  constructor(
    message: string,
    readonly code: 'access_denied' | 'unsupported_feature' | 'execution_failed',
  ) {
    super(message);
  }
}

async function localBrowserExecuteInput(
  selection: AvailableQuerySourceSelection,
  req: QueryExecRequest,
  admission: ExecutionAdmissionInput,
  signal: AbortSignal,
): Promise<BrowserExecuteInput | undefined> {
  if (selection.source.kind !== 'local_delta') return undefined;
  const table = canonicalTableForSelection(selection);
  if (!table.resource) {
    throw new BrowserReadResolutionFailure(
      'Selected table did not produce a canonical resource.',
      'execution_failed',
    );
  }
  const deadline = timestampFromMs(admission.deadlineAt);
  const resolution = await dataAccessResolverForSelection(selection).resolve(table.resource, {
    executionId: admission.executionId,
    deadline,
    snapshotVersion: req.snapshot_version ?? selection.source.snapshot,
    signal,
  });
  if (resolution.outcome.case !== 'browserRead') {
    const message =
      resolution.outcome.case === undefined
        ? 'Data access resolver returned no outcome.'
        : resolution.outcome.value.message;
    throw new BrowserReadResolutionFailure(
      message,
      resolution.outcome.case === 'denied'
        ? 'access_denied'
        : resolution.outcome.case === 'remoteRequired'
          ? 'unsupported_feature'
          : 'execution_failed',
    );
  }
  const page = req.page ?? defaultQueryPage();
  const request = create(ExecuteRequestSchema, {
    executionId: admission.executionId,
    binding: {
      case: 'browserRead',
      value: resolution.outcome.value,
    },
    query: create(QueryRequestSchema, {
      sql: req.sql,
      preferredTarget:
        admission.target === 'native'
          ? ContractExecutionTarget.NATIVE
          : ContractExecutionTarget.BROWSER_WASM,
      options: create(QueryExecutionOptionsSchema, {
        collectMetrics: true,
        includeExplain: true,
        resultPage: create(QueryResultPageSchema, {
          limit: BigInt(page.size),
          offset: BigInt(page.offset),
        }),
        runtimeLimits: create(QueryRuntimeLimitsSchema, {
          maxResultRows: BigInt(admission.budgets.maxResultRows),
          maxArrowIpcBytes: BigInt(admission.budgets.maxArrowIpcBytes),
          maxPreviewStringBytes: BigInt(admission.budgets.maxPreviewStringBytes),
          maxScanBytes:
            admission.budgets.maxScanBytes === undefined
              ? undefined
              : BigInt(admission.budgets.maxScanBytes),
        }),
      }),
    }),
    deadline,
  });
  const input = { table, request };
  validateBrowserExecuteInput(input, {
    isCurrentLocalObjectUrl: isCurrentLocalDeltaObjectUrl,
  });
  return input;
}

export async function runQuery(
  req: QueryExecRequest,
  onEvent: (event: QueryEvent) => void,
  selection: AvailableQuerySourceSelection,
  admission: ExecutionAdmissionInput,
  signal: AbortSignal = new AbortController().signal,
  deadlineSignal?: AbortSignal,
): Promise<QueryRunOutcome> {
  const source = selection.source;
  const startedAt = performance.now();
  const since = () => Math.round(performance.now() - startedAt);

  const page = req.page ?? defaultQueryPage();
  const executionId = admission.executionId;

  try {
    const browserInput = await localBrowserExecuteInput(selection, req, admission, signal);
    const browserRead =
      browserInput?.request.binding.case === 'browserRead'
        ? browserInput.request.binding.value
        : undefined;
    const requestId = executionRequestId(executionId);
    const execution = await runCancelableQueryStages({
      signal,
      deadlineSignal,
      remainingTime: () => admission.deadlineAt - Date.now(),
      getSession: () => getSession(source, browserRead),
      openTable: (state) => ensureTable(state, signal, executionId, browserInput),
      cancelQuery: (state) =>
        state.client.cancelQuery(executionId, {
          requestId: executionCancelSpanId(executionId, 1),
        }),
      execute: async (state) => {
        const setupMetrics = pendingSessionSetupMetrics(state);
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
        const removeHandler = () => eventListeners.delete(handler);
        eventListeners.add(handler);
        try {
          const result: AxonQueryResult = await state.client.query(
            state.source.tableName,
            {
              table_uri: state.snapshot.table_uri,
              snapshot_version: req.snapshot_version ?? state.snapshot.snapshot_version,
              sql: req.sql,
              preferred_target: admission.target,
              options: queryExecutionOptionsForAdmission(admission, queryResultPageRequest(page)),
            },
            queryClientOptionsForAdmission(admission),
          );
          return { result, setupMetrics };
        } finally {
          removeHandler();
        }
      },
    });
    const { result, setupMetrics } = execution.result;
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
    markSessionSetupMetricsEmitted(execution.session);
    return outcome;
  } catch (err) {
    if (queryFailureInvalidatesSession(err)) {
      discardQuerySession(source);
    }
    return queryFailureOutcome(err, since(), admission.target);
  }
}

export function queryFailureInvalidatesSession(error: unknown): boolean {
  return (
    error instanceof ExecutionDeadlineError ||
    (error instanceof AxonWorkerError && isQuerySessionInvalidation(error.queryError))
  );
}

export function queryFailureOutcome(
  error: unknown,
  elapsedMs: number,
  target: ExecutionAdmissionInput['target'],
): QueryRunError {
  if (
    (error instanceof DOMException && error.name === 'AbortError') ||
    (error instanceof AxonWorkerError && isBrowserDataFusionCancellation(error.queryError))
  ) {
    return {
      status: 'error',
      message: 'Query cancelled',
      code: 'cancelled',
      target,
      elapsed_ms: elapsedMs,
    };
  }
  if (error instanceof AxonWorkerError) {
    const queryError: QueryError = error.queryError;
    return {
      status: 'error',
      message: queryError.message,
      code: queryError.code,
      target: queryError.target,
      fallback_reason: queryError.fallback_reason,
      elapsed_ms: elapsedMs,
    };
  }
  if (error instanceof ExecutionDeadlineError) {
    return {
      status: 'error',
      message: error.message,
      code: 'deadline',
      target,
      elapsed_ms: elapsedMs,
    };
  }
  if (error instanceof BrowserReadResolutionFailure) {
    return {
      status: 'error',
      message: error.message,
      code: error.code,
      target,
      elapsed_ms: elapsedMs,
    };
  }
  return {
    status: 'error',
    message: error instanceof Error ? error.message : String(error),
    elapsed_ms: elapsedMs,
  };
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

function catalogFromSession(state: SessionState): Catalog {
  return {
    name: state.source.catalogName,
    region: state.source.region,
    storage: state.source.storage,
    tables: [deriveCatalogTable(state)],
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
