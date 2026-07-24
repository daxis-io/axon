// Query connector. Owns the AxonBrowserClient + worker, resolves Delta snapshots
// against the configured fixture, opens the table once, and runs SQL through the
// SDK. Translates worker events + the success envelope into UI-shaped types.

import { clone, create, equals } from '@bufbuild/protobuf';
import { timestampMs } from '@bufbuild/protobuf/wkt';
import init, { resolve_delta_snapshot_from_manifest } from '../wasm/axon_web_wasm.js';
import {
  AxonWorkerError,
  createAxonBrowserClient,
  redactUrlSecrets,
  type AxonBrowserClient,
  type AxonQueryRequestOptions,
  type AxonQueryResult,
  type BrowserHttpSnapshotDescriptor,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerLogLevel,
  type BrowserWorkerProgressStage,
  type BrowserWorkerRangeReadMetricsEvent,
  type CapabilityKey,
  type CapabilityState,
  type ExecutionTarget,
  type PartitionColumnType,
  type QueryError,
  type QueryExecutionOptions,
  type QueryMetricsSummary,
} from '../axon-browser-sdk.ts';
import {
  BrowserHttpFileDescriptorSchema as ContractBrowserHttpFileDescriptorSchema,
  BrowserHttpSnapshotDescriptorSchema as ContractBrowserHttpSnapshotDescriptorSchema,
  CapabilityEntrySchema,
  CapabilityKey as ContractCapabilityKey,
  CapabilityReportSchema as ContractCapabilityReportSchema,
  CapabilityState as ContractCapabilityState,
  PartitionColumnType as ContractPartitionColumnType,
  PartitionValueSchema,
  type BrowserHttpSnapshotDescriptor as ContractBrowserHttpSnapshotDescriptor,
  type CapabilityReport as ContractCapabilityReport,
  type ResolvedBrowserRead,
} from '../generated/contracts/protobuf/axon/dataaccess/v1/dataaccess_pb.ts';
import {
  ArrowIpcFormat as ContractArrowIpcFormat,
  ArrowIpcResultSchema,
  BrowserAccessMode as ContractBrowserAccessMode,
  BrowserWorkerEventContextSchema as ContractBrowserWorkerEventContextSchema,
  BrowserWorkerEventEnvelopeSchema as ContractBrowserWorkerEventEnvelopeSchema,
  BrowserWorkerEventPhase as ContractBrowserWorkerEventPhase,
  BrowserWorkerLogEventSchema as ContractBrowserWorkerLogEventSchema,
  BrowserWorkerLogLevel as ContractBrowserWorkerLogLevel,
  BrowserWorkerProgressEventSchema as ContractBrowserWorkerProgressEventSchema,
  BrowserWorkerProgressStage as ContractBrowserWorkerProgressStage,
  BrowserWorkerRangeReadMetricsEventSchema as ContractBrowserWorkerRangeReadMetricsEventSchema,
  CancelResponseSchema,
  ExecuteRequestSchema,
  ExecuteResponseSchema,
  ExecutionAcceptedSchema,
  ExecutionAdmissionSchema,
  ExecutionCancelledSchema,
  ExecutionCompletedSchema,
  ExecutionFailedSchema,
  ExecutionLifecycleState,
  ExecutionRejectionReason,
  ExecutionTerminalFrameSchema,
  ExecutionTerminalStateSchema,
  ExecutionTarget as ContractExecutionTarget,
  PreviewCellSchema,
  QueryErrorCode as ContractQueryErrorCode,
  QueryErrorSchema as ContractQueryErrorSchema,
  QueryMetricsSummarySchema as ContractQueryMetricsSummarySchema,
  QueryResponseSchema as ContractQueryResponseSchema,
  QueryRequestSchema,
  ResultPreviewRowSchema,
  ResultPreviewSchema,
  type BrowserWorkerEventEnvelope as ContractBrowserWorkerEventEnvelope,
  type BrowserWorkerRangeReadMetricsEvent as ContractBrowserWorkerRangeReadMetricsEvent,
  type CancelRequest,
  type ExecuteResponse,
  type ExecutionCompleted,
  type ExecutionTerminalState as ContractExecutionTerminalState,
  type QueryMetricsSummary as ContractQueryMetricsSummary,
  type QueryRequest as ContractQueryRequest,
} from '../generated/contracts/protobuf/axon/exec/v1/exec_pb.ts';
import {
  TableNodeSchema,
  type TableNode,
} from '../generated/contracts/protobuf/axon/catalog/v1/catalog_pb.ts';
import type {
  CatalogTable,
  QueryEvent,
  QueryPageRequest,
  QueryRunError,
  QueryRunOutcome,
} from './types.ts';
import { isCurrentLocalDeltaObjectUrl } from './local-delta.ts';
import {
  lookupPublicObjectStorageRuntimeCache,
  resolvePublicObjectStorageDescriptor,
  type PublicObjectStorageDescriptorResolutionMetrics,
} from './object-storage.ts';
import { queryPageFromRequest, resultPageFromContractPreview } from './query-pagination.ts';
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
  BrowserReadResolutionFailure,
  canonicalTableForSelection,
  dataAccessResolverForSelection,
  requireBrowserReadResolution,
} from './browser-read-resolution.ts';
import {
  BrowserExecutionValidationError,
  createValidatedBrowserExecutionProvider,
  validateBrowserExecuteInput,
  type BrowserExecuteInput,
  type ExecutionProvider,
} from './browser-execution-provider.ts';
import {
  cancelExecutionRequest,
  executionCancelSpanId,
  executionOpenSpanId,
  executionRequestId,
  type PreparedExecution,
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
const publicDescriptorSetupMetrics = new WeakMap<
  ContractBrowserHttpSnapshotDescriptor,
  SessionSetupMetrics
>();
const sampleDescriptorSetupMetrics = new WeakMap<
  ContractBrowserHttpSnapshotDescriptor,
  SessionSetupMetrics
>();
const sampleDescriptorManifests = new WeakMap<
  ContractBrowserHttpSnapshotDescriptor,
  FixtureManifest
>();

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
): ContractBrowserHttpSnapshotDescriptor {
  const fileIndex = new Map(manifest.data_files?.map((f) => [f.relative_path, f]) ?? []);
  const active = snapshot.active_files.map((file) => {
    const data = fileIndex.get(file.path);
    if (!data) {
      throw new Error(`active file '${file.path}' missing from fixture data_files`);
    }
    return create(ContractBrowserHttpFileDescriptorSchema, {
      path: file.path,
      url: new URL(data.url_path, window.location.href).toString(),
      sizeBytes: BigInt(file.size_bytes),
      partitionValues: Object.fromEntries(
        Object.entries(file.partition_values).map(([name, value]) => [
          name,
          create(PartitionValueSchema, {
            value:
              value === null ? { case: 'nullValue', value: 0 } : { case: 'stringValue', value },
          }),
        ]),
      ),
      stats: file.stats,
    });
  });
  return create(ContractBrowserHttpSnapshotDescriptorSchema, {
    tableUri: snapshot.table_uri,
    snapshotVersion: BigInt(snapshot.snapshot_version),
    partitionColumnTypes: Object.fromEntries(
      Object.entries(snapshot.partition_column_types ?? {}).map(([name, value]) => [
        name,
        contractPartitionColumnType(value),
      ]),
    ),
    activeFiles: active,
  });
}

async function buildSession(
  source: QueryTableSource,
  browserRead?: ResolvedBrowserRead,
): Promise<SessionState> {
  if (browserRead?.descriptor?.descriptor.case !== 'snapshot') {
    throw new Error('browser Delta session requires a resolved snapshot descriptor');
  }
  const contractDescriptor = browserRead.descriptor.descriptor.value;
  const descriptor = sdkSnapshotDescriptor(contractDescriptor);

  if (source.kind === 'object_store_table_root') {
    const setupMetrics = mergeSessionSetupMetrics(
      source.descriptorResolutionMetrics
        ? sessionSetupMetricsFromPublicObjectStorage(source.descriptorResolutionMetrics)
        : undefined,
      publicDescriptorSetupMetrics.get(contractDescriptor),
    );

    return {
      client: createQueryClient(),
      descriptor,
      contractDescriptor,
      setupMetrics,
      setupMetricsEmitted: false,
      snapshot: snapshotFromBrowserDescriptor(descriptor),
      tableOpened: false,
      source,
    };
  }

  return {
    client: createQueryClient(),
    descriptor,
    contractDescriptor,
    manifest: sampleDescriptorManifests.get(contractDescriptor),
    setupMetrics:
      source.kind === 'manifest' ? sampleDescriptorSetupMetrics.get(contractDescriptor) : undefined,
    setupMetricsEmitted: source.kind === 'local_delta',
    snapshot: snapshotFromBrowserDescriptor(descriptor),
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

function contractPartitionColumnType(
  value: PartitionColumnType | undefined,
): ContractPartitionColumnType {
  switch (value) {
    case 'int64':
      return ContractPartitionColumnType.INT64;
    case 'boolean':
      return ContractPartitionColumnType.BOOLEAN;
    case 'unsupported':
      return ContractPartitionColumnType.UNSUPPORTED;
    case 'string':
    case undefined:
      return ContractPartitionColumnType.STRING;
  }
}

function sdkExecutionTarget(value: ContractExecutionTarget): ExecutionTarget {
  switch (value) {
    case ContractExecutionTarget.BROWSER_WASM:
      return 'browser_wasm';
    case ContractExecutionTarget.NATIVE:
      return 'native';
    case ContractExecutionTarget.REMOTE_SERVICE:
    case ContractExecutionTarget.UNSPECIFIED:
      throw new BrowserReadResolutionFailure(
        'Selected execution target is unavailable to the browser executor.',
        'unsupported_feature',
      );
  }
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
  input: BrowserExecuteInput,
): Promise<void> {
  validateBrowserExecuteInput(input, {
    isCurrentLocalObjectUrl: isCurrentLocalDeltaObjectUrl,
  });
  if (state.tableOpened) return Promise.resolve();
  const requestId = executionOpenSpanId(executionId, 1);
  return state.client.openDeltaTable(input.table.name, state.descriptor, { requestId }).then(() => {
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

export function queryExecutionOptionsForRequest(
  request: ContractQueryRequest,
): QueryExecutionOptions {
  const options = request.options;
  const page = options?.resultPage;
  const limits = options?.runtimeLimits;
  return {
    collect_metrics: options?.collectMetrics,
    include_explain: options?.includeExplain,
    ...(page === undefined
      ? {}
      : {
          result_page: {
            limit: safeContractInteger(page.limit ?? 0n, 'result_page.limit'),
            offset: safeContractInteger(page.offset ?? 0n, 'result_page.offset'),
          },
        }),
    ...(limits === undefined
      ? {}
      : {
          runtime_limits: {
            ...(limits.maxResultRows === undefined
              ? {}
              : {
                  max_result_rows: safeContractInteger(
                    limits.maxResultRows,
                    'runtime_limits.max_result_rows',
                  ),
                }),
            ...(limits.maxArrowIpcBytes === undefined
              ? {}
              : {
                  max_arrow_ipc_bytes: safeContractInteger(
                    limits.maxArrowIpcBytes,
                    'runtime_limits.max_arrow_ipc_bytes',
                  ),
                }),
            ...(limits.maxPreviewStringBytes === undefined
              ? {}
              : {
                  max_preview_string_bytes: safeContractInteger(
                    limits.maxPreviewStringBytes,
                    'runtime_limits.max_preview_string_bytes',
                  ),
                }),
            ...(limits.maxScanBytes === undefined
              ? {}
              : {
                  max_scan_bytes: safeContractInteger(
                    limits.maxScanBytes,
                    'runtime_limits.max_scan_bytes',
                  ),
                }),
            ...(limits.maxScanOverfetchBytes === undefined
              ? {}
              : {
                  max_scan_overfetch_bytes: safeContractInteger(
                    limits.maxScanOverfetchBytes,
                    'runtime_limits.max_scan_overfetch_bytes',
                  ),
                }),
          },
        }),
  };
}

export function queryClientOptionsForRequest(
  request: Pick<BrowserExecuteInput['request'], 'executionId'>,
): AxonQueryRequestOptions {
  return { requestId: executionRequestId(request.executionId), delivery: 'single_buffer' };
}

async function loadSampleFixtureDescriptor(input: {
  snapshotVersion?: number;
  signal: AbortSignal;
}): Promise<ContractBrowserHttpSnapshotDescriptor> {
  throwIfCancelled(input.signal);
  await ensureWasm();
  throwIfCancelled(input.signal);
  const manifest = await fetchJson<FixtureManifest>('/fixtures/prod-like/delta-log-manifest.json');
  throwIfCancelled(input.signal);
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
    input.snapshotVersion,
  );
  throwIfCancelled(input.signal);
  const snapshot = JSON.parse(snapshotJson) as ResolvedSnapshot;
  const descriptor = browserSnapshotDescriptor(snapshot, manifest);
  sampleDescriptorSetupMetrics.set(descriptor, {
    descriptor_resolution_count: 1,
    snapshot_resolve_count: 1,
    snapshot_resolve_duration_ms: Math.round(performance.now() - snapshotResolveStartedAt),
  });
  sampleDescriptorManifests.set(descriptor, manifest);
  return descriptor;
}

async function loadPublicObjectStorageDescriptor(input: {
  provider: 'gcs' | 's3';
  tableUri: string;
  region?: string;
  snapshotVersion?: number;
  expectedSnapshotVersion?: number;
  signal: AbortSignal;
}): Promise<ContractBrowserHttpSnapshotDescriptor> {
  throwIfCancelled(input.signal);
  await ensureWasm();
  throwIfCancelled(input.signal);
  const snapshot =
    input.snapshotVersion === undefined
      ? ({ kind: 'latest' } as const)
      : ({ kind: 'version', version: input.snapshotVersion } as const);
  const cached = lookupPublicObjectStorageRuntimeCache({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
    snapshot,
    expectedSnapshotVersion: input.expectedSnapshotVersion,
  });
  if (cached) {
    publicDescriptorSetupMetrics.set(cached.descriptor, { descriptor_cache_hit: 1 });
    return cached.descriptor;
  }

  let setupMetrics: SessionSetupMetrics | undefined;
  const descriptor = await resolvePublicObjectStorageDescriptor({
    provider: input.provider,
    tableUri: input.tableUri,
    region: input.region,
    snapshotVersion: input.snapshotVersion,
    resolveDeltaSnapshotFromManifest: resolve_delta_snapshot_from_manifest,
    onMetrics: (metrics) => {
      setupMetrics = sessionSetupMetricsFromPublicObjectStorage(metrics);
    },
  });
  throwIfCancelled(input.signal);
  if (setupMetrics) publicDescriptorSetupMetrics.set(descriptor, setupMetrics);
  return descriptor;
}

export async function resolveBrowserExecuteInput(input: {
  selection: AvailableQuerySourceSelection;
  table?: TableNode;
  query: ContractQueryRequest;
  snapshotVersion?: number;
  execution: PreparedExecution;
  signal: AbortSignal;
}): Promise<BrowserExecuteInput> {
  const expectedTable = canonicalTableForSelection(input.selection);
  if (input.table && !equals(TableNodeSchema, input.table, expectedTable)) {
    throw new BrowserReadResolutionFailure(
      'Selected table did not match its canonical execution table.',
      'execution_failed',
      ExecutionRejectionReason.INVALID_REQUEST,
    );
  }
  const table = clone(TableNodeSchema, expectedTable);
  if (!table.resource) {
    throw new BrowserReadResolutionFailure(
      'Selected table did not produce a canonical resource.',
      'execution_failed',
      ExecutionRejectionReason.INVALID_REQUEST,
    );
  }
  const deadline = input.execution.deadline;
  const resolution = await dataAccessResolverForSelection(input.selection, {
    loadPublicObjectStorageDescriptor,
    loadSampleFixtureDescriptor,
  }).resolve(table.resource, {
    executionId: input.execution.executionId,
    deadline,
    snapshotVersion: input.snapshotVersion,
    signal: input.signal,
  });
  const browserRead = requireBrowserReadResolution(resolution);
  const request = create(ExecuteRequestSchema, {
    executionId: input.execution.executionId,
    binding: {
      case: 'browserRead',
      value: browserRead,
    },
    query: clone(QueryRequestSchema, input.query),
    deadline,
  });
  const browserInput = { table, request };
  try {
    validateBrowserExecuteInput(browserInput, {
      isCurrentLocalObjectUrl: isCurrentLocalDeltaObjectUrl,
    });
  } catch (error) {
    const rejectionReason =
      error instanceof BrowserExecutionValidationError
        ? error.reason
        : ExecutionRejectionReason.INVALID_REQUEST;
    throw new BrowserReadResolutionFailure(
      error instanceof Error ? error.message : String(error),
      rejectionReason === ExecutionRejectionReason.ACCESS_DENIED
        ? 'access_denied'
        : rejectionReason === ExecutionRejectionReason.UNSUPPORTED
          ? 'unsupported_feature'
          : 'execution_failed',
      rejectionReason,
    );
  }
  return browserInput;
}

type QueryMetricField = readonly [
  sdk: keyof QueryMetricsSummary,
  contract: keyof ContractQueryMetricsSummary,
];

const QUERY_METRIC_FIELDS: readonly QueryMetricField[] = [
  ['bytes_fetched', 'bytesFetched'],
  ['duration_ms', 'durationMs'],
  ['files_touched', 'filesTouched'],
  ['files_skipped', 'filesSkipped'],
  ['prebootstrap_fail_open_count', 'prebootstrapFailOpenCount'],
  ['prebootstrap_files_pruned', 'prebootstrapFilesPruned'],
  ['footer_reads_avoided', 'footerReadsAvoided'],
  ['prebootstrap_candidate_files', 'prebootstrapCandidateFiles'],
  ['row_groups_touched', 'rowGroupsTouched'],
  ['row_groups_skipped', 'rowGroupsSkipped'],
  ['footer_reads', 'footerReads'],
  ['bootstrap_footer_range_reads', 'bootstrapFooterRangeReads'],
  ['scan_footer_range_reads', 'scanFooterRangeReads'],
  ['scan_data_range_reads', 'scanDataRangeReads'],
  ['duplicate_range_reads', 'duplicateRangeReads'],
  ['coalesced_range_reads', 'coalescedRangeReads'],
  ['coalesced_gap_bytes_fetched', 'coalescedGapBytesFetched'],
  ['footer_cache_hits', 'footerCacheHits'],
  ['footer_cache_misses', 'footerCacheMisses'],
  ['footer_range_reads_avoided', 'footerRangeReadsAvoided'],
  ['footer_cache_degraded_identity_reads', 'footerCacheDegradedIdentityReads'],
  ['identity_present_range_reads', 'identityPresentRangeReads'],
  ['identity_missing_range_reads', 'identityMissingRangeReads'],
  ['descriptor_resolution_count', 'descriptorResolutionCount'],
  ['delta_log_manifest_list_count', 'deltaLogManifestListCount'],
  ['delta_log_manifest_list_duration_ms', 'deltaLogManifestListDurationMs'],
  ['snapshot_resolve_count', 'snapshotResolveCount'],
  ['snapshot_resolve_duration_ms', 'snapshotResolveDurationMs'],
  ['descriptor_cache_hit', 'descriptorCacheHit'],
  ['session_reuse_count', 'sessionReuseCount'],
  ['opened_table_reuse_count', 'openedTableReuseCount'],
  ['identity_refresh_count', 'identityRefreshCount'],
  ['access_envelope_refresh_count', 'accessEnvelopeRefreshCount'],
  ['rows_emitted', 'rowsEmitted'],
  ['snapshot_bootstrap_duration_ms', 'snapshotBootstrapDurationMs'],
  ['arrow_ipc_bytes', 'arrowIpcBytes'],
  ['preview_rows', 'previewRows'],
  ['preview_string_bytes', 'previewStringBytes'],
  ['planning_duration_ms', 'planningDurationMs'],
  ['arrow_ipc_encode_duration_ms', 'arrowIpcEncodeDurationMs'],
  ['preview_duration_ms', 'previewDurationMs'],
  ['range_cache_hits', 'rangeCacheHits'],
  ['range_cache_misses', 'rangeCacheMisses'],
  ['range_cache_bytes_reused', 'rangeCacheBytesReused'],
  ['range_cache_bytes_stored', 'rangeCacheBytesStored'],
  ['range_cache_validation_misses', 'rangeCacheValidationMisses'],
  ['range_cache_degraded_identity_reads', 'rangeCacheDegradedIdentityReads'],
  ['range_readahead_requests', 'rangeReadaheadRequests'],
  ['range_readahead_bytes_fetched', 'rangeReadaheadBytesFetched'],
  ['range_readahead_bytes_used', 'rangeReadaheadBytesUsed'],
  ['range_readahead_wasted_bytes', 'rangeReadaheadWastedBytes'],
  ['scan_overfetch_bytes', 'scanOverfetchBytes'],
  ['coordinator_peak_staged_bytes', 'coordinatorPeakStagedBytes'],
  ['coordinator_staging_limit_bytes', 'coordinatorStagingLimitBytes'],
  ['cursor_peak_pending_encoded_bytes', 'cursorPeakPendingEncodedBytes'],
  ['cursor_peak_transport_chunk_bytes', 'cursorPeakTransportChunkBytes'],
];

function executeResponseForWorkerEvent(
  envelope: BrowserWorkerEventEnvelope,
  executionId: string,
  elapsedMs: number,
  setupMetrics?: SessionSetupMetrics,
): ExecuteResponse | undefined {
  const eventContext = (context: BrowserWorkerEventContext) =>
    create(ContractBrowserWorkerEventContextSchema, {
      phase: contractWorkerEventPhase(context.phase),
      requestId: context.request_id,
      executionId,
      tableName: context.table_name,
    });
  if ('progress' in envelope) {
    return executeEventResponse(
      create(ContractBrowserWorkerEventEnvelopeSchema, {
        event: {
          case: 'progress',
          value: create(ContractBrowserWorkerProgressEventSchema, {
            context: eventContext(envelope.progress.context),
            stage: contractWorkerProgressStage(envelope.progress.stage),
          }),
        },
      }),
    );
  }
  if ('log' in envelope) {
    return executeEventResponse(
      create(ContractBrowserWorkerEventEnvelopeSchema, {
        event: {
          case: 'log',
          value: create(ContractBrowserWorkerLogEventSchema, {
            context: eventContext(envelope.log.context),
            level: contractWorkerLogLevel(envelope.log.level),
            message: redactUrlSecrets(envelope.log.message),
          }),
        },
      }),
    );
  }
  if ('range_read_metrics' in envelope) {
    const metrics = queryMetricsFromRangeReadMetricsEvent(
      envelope.range_read_metrics,
      elapsedMs,
      setupMetrics,
    );
    return executeEventResponse(
      create(ContractBrowserWorkerEventEnvelopeSchema, {
        event: {
          case: 'rangeReadMetrics',
          value: contractRangeReadMetricsEvent(
            envelope.range_read_metrics.context,
            executionId,
            metrics,
          ),
        },
      }),
    );
  }
  return undefined;
}

function executeEventResponse(event: ContractBrowserWorkerEventEnvelope): ExecuteResponse {
  return create(ExecuteResponseSchema, {
    item: { case: 'event', value: event },
  });
}

function contractRangeReadMetricsEvent(
  context: BrowserWorkerEventContext,
  executionId: string,
  metrics: QueryMetricsSummary,
): ContractBrowserWorkerRangeReadMetricsEvent {
  const event = create(ContractBrowserWorkerRangeReadMetricsEventSchema, {
    context: create(ContractBrowserWorkerEventContextSchema, {
      phase: contractWorkerEventPhase(context.phase),
      requestId: context.request_id,
      executionId,
      tableName: context.table_name,
    }),
  });
  const generatedMetrics = contractQueryMetrics(metrics);
  const target = event as unknown as Record<string, unknown>;
  const source = generatedMetrics as unknown as Record<string, unknown>;
  for (const [, contractName] of QUERY_METRIC_FIELDS) {
    if (contractName === 'durationMs') continue;
    const value = source[contractName];
    if (value !== undefined) target[contractName] = value;
  }
  event.accessMode = generatedMetrics.accessMode;
  return event;
}

function queryEventFromExecuteResponse(
  response: ExecuteResponse,
  elapsedMs: number,
): QueryEvent | undefined {
  if (response.item.case !== 'event') return undefined;
  const event = response.item.value.event;
  switch (event.case) {
    case 'progress':
      return {
        kind: 'progress',
        stage: sdkWorkerProgressStage(event.value.stage),
        elapsed_ms: elapsedMs,
      };
    case 'log':
      return {
        kind: 'log',
        level: sdkWorkerLogLevel(event.value.level),
        message: event.value.message,
        elapsed_ms: elapsedMs,
      };
    case 'rangeReadMetrics':
      return {
        kind: 'metrics',
        metrics: {
          ...sdkQueryMetrics(event.value),
          duration_ms: elapsedMs,
        },
        elapsed_ms: elapsedMs,
      };
    case 'cacheMetrics':
    case undefined:
      return undefined;
  }
}

function contractWorkerEventPhase(
  phase: BrowserWorkerEventContext['phase'],
): ContractBrowserWorkerEventPhase {
  switch (phase) {
    case 'instantiate':
      return ContractBrowserWorkerEventPhase.INSTANTIATE;
    case 'open':
      return ContractBrowserWorkerEventPhase.OPEN;
    case 'inspect':
      return ContractBrowserWorkerEventPhase.INSPECT;
    case 'query':
      return ContractBrowserWorkerEventPhase.QUERY;
  }
}

function contractWorkerProgressStage(
  stage: BrowserWorkerProgressStage,
): ContractBrowserWorkerProgressStage {
  switch (stage) {
    case 'started':
      return ContractBrowserWorkerProgressStage.STARTED;
    case 'planning':
      return ContractBrowserWorkerProgressStage.PLANNING;
    case 'executing':
      return ContractBrowserWorkerProgressStage.EXECUTING;
    case 'arrow_ipc_ready':
      return ContractBrowserWorkerProgressStage.ARROW_IPC_READY;
    case 'finished':
      return ContractBrowserWorkerProgressStage.FINISHED;
  }
}

function sdkWorkerProgressStage(
  stage: ContractBrowserWorkerProgressStage,
): BrowserWorkerProgressStage {
  switch (stage) {
    case ContractBrowserWorkerProgressStage.STARTED:
      return 'started';
    case ContractBrowserWorkerProgressStage.PLANNING:
      return 'planning';
    case ContractBrowserWorkerProgressStage.EXECUTING:
      return 'executing';
    case ContractBrowserWorkerProgressStage.ARROW_IPC_READY:
      return 'arrow_ipc_ready';
    case ContractBrowserWorkerProgressStage.FINISHED:
      return 'finished';
    case ContractBrowserWorkerProgressStage.UNSPECIFIED:
      throw new BrowserReadResolutionFailure(
        'Browser executor received an unspecified progress stage.',
        'execution_failed',
      );
  }
}

function contractWorkerLogLevel(level: BrowserWorkerLogLevel): ContractBrowserWorkerLogLevel {
  switch (level) {
    case 'debug':
      return ContractBrowserWorkerLogLevel.DEBUG;
    case 'info':
      return ContractBrowserWorkerLogLevel.INFO;
    case 'warn':
      return ContractBrowserWorkerLogLevel.WARN;
    case 'error':
      return ContractBrowserWorkerLogLevel.ERROR;
  }
}

function sdkWorkerLogLevel(level: ContractBrowserWorkerLogLevel): BrowserWorkerLogLevel {
  switch (level) {
    case ContractBrowserWorkerLogLevel.DEBUG:
      return 'debug';
    case ContractBrowserWorkerLogLevel.INFO:
      return 'info';
    case ContractBrowserWorkerLogLevel.WARN:
      return 'warn';
    case ContractBrowserWorkerLogLevel.ERROR:
      return 'error';
    case ContractBrowserWorkerLogLevel.UNSPECIFIED:
      throw new BrowserReadResolutionFailure(
        'Browser executor received an unspecified log level.',
        'execution_failed',
      );
  }
}

function completedExecuteResponse(
  input: BrowserExecuteInput,
  result: AxonQueryResult,
  metrics: QueryMetricsSummary,
  sequence: bigint,
): ExecuteResponse {
  if (result.result.delivery !== 'single_buffer') {
    throw new BrowserReadResolutionFailure(
      'Browser executor requires one bounded Arrow IPC buffer.',
      'execution_failed',
    );
  }
  const admittedMaximum = input.request.query?.options?.runtimeLimits?.maxArrowIpcBytes;
  if (admittedMaximum !== undefined && BigInt(result.result.byte_length) > admittedMaximum) {
    throw new BrowserReadResolutionFailure(
      'Arrow IPC result exceeded the admitted byte budget.',
      'execution_failed',
    );
  }
  const response = create(ContractQueryResponseSchema, {
    executedOn:
      result.response.executed_on === 'native'
        ? ContractExecutionTarget.NATIVE
        : ContractExecutionTarget.BROWSER_WASM,
    capabilities: contractCapabilityReport(result.response.capabilities),
    metrics: contractQueryMetrics(metrics),
    explain: result.response.explain,
  });
  const completed = create(ExecutionCompletedSchema, {
    response,
    result: create(ArrowIpcResultSchema, {
      format:
        result.result.format === 'file'
          ? ContractArrowIpcFormat.FILE
          : ContractArrowIpcFormat.STREAM,
      contentType: result.result.content_type,
      bytes: result.result.bytes,
      byteLength: BigInt(result.result.byte_length),
    }),
    preview: result.preview ? contractResultPreview(result.preview) : undefined,
  });
  return terminalExecuteResponse(input.request.executionId, sequence, {
    case: 'completed',
    value: completed,
  });
}

function failedExecuteResponse(
  executionId: string,
  outcome: QueryRunError,
  sequence: bigint,
): ExecuteResponse {
  if (outcome.code === 'cancelled') {
    return terminalExecuteResponse(executionId, sequence, {
      case: 'cancelled',
      value: create(ExecutionCancelledSchema),
    });
  }
  return terminalExecuteResponse(executionId, sequence, {
    case: 'failed',
    value: create(ExecutionFailedSchema, {
      error: create(ContractQueryErrorSchema, {
        code: contractQueryErrorCode(outcome.code),
        message: outcome.message,
        target:
          outcome.target === 'native'
            ? ContractExecutionTarget.NATIVE
            : ContractExecutionTarget.BROWSER_WASM,
      }),
    }),
  });
}

function terminalExecuteResponse(
  executionId: string,
  sequence: bigint,
  outcome: ContractExecutionTerminalState['outcome'],
): ExecuteResponse {
  return create(ExecuteResponseSchema, {
    item: {
      case: 'terminal',
      value: create(ExecutionTerminalFrameSchema, {
        executionId,
        sequence,
        state: create(ExecutionTerminalStateSchema, { outcome }),
      }),
    },
  });
}

function acceptedExecuteResponse(executionId: string): ExecuteResponse {
  return create(ExecuteResponseSchema, {
    item: {
      case: 'admission',
      value: create(ExecutionAdmissionSchema, {
        outcome: {
          case: 'accepted',
          value: create(ExecutionAcceptedSchema, {
            executionId,
            state: ExecutionLifecycleState.RUNNING,
            launch: true,
          }),
        },
      }),
    },
  });
}

function contractResultPreview(preview: NonNullable<AxonQueryResult['preview']>) {
  return create(ResultPreviewSchema, {
    columns: preview.columns,
    rows: preview.rows.map((row) =>
      create(ResultPreviewRowSchema, {
        cells: row.map((cell) =>
          create(PreviewCellSchema, {
            value:
              cell === null
                ? { case: 'nullValue', value: 0 }
                : typeof cell === 'string'
                  ? { case: 'stringValue', value: cell }
                  : typeof cell === 'number'
                    ? { case: 'numberValue', value: cell }
                    : { case: 'boolValue', value: cell },
          }),
        ),
      }),
    ),
    rowCount: BigInt(preview.row_count),
    previewRowLimit: BigInt(preview.preview_row_limit),
    truncated: preview.truncated,
  });
}

function contractCapabilityReport(report: {
  capabilities: Partial<Record<CapabilityKey, CapabilityState>>;
}): ContractCapabilityReport {
  return create(ContractCapabilityReportSchema, {
    capabilities: Object.entries(report.capabilities).flatMap(([key, state]) => {
      const contractKey = contractCapabilityKey(key as CapabilityKey);
      const contractState = state ? contractCapabilityState(state) : undefined;
      return contractKey === undefined || contractState === undefined
        ? []
        : [
            create(CapabilityEntrySchema, {
              key: contractKey,
              state: contractState,
            }),
          ];
    }),
  });
}

function contractCapabilityKey(value: CapabilityKey): ContractCapabilityKey | undefined {
  switch (value) {
    case 'change_data_feed':
      return ContractCapabilityKey.CHANGE_DATA_FEED;
    case 'column_mapping':
      return ContractCapabilityKey.COLUMN_MAPPING;
    case 'deletion_vectors':
      return ContractCapabilityKey.DELETION_VECTORS;
    case 'multi_partition_execution':
      return ContractCapabilityKey.MULTI_PARTITION_EXECUTION;
    case 'proxy_access':
      return ContractCapabilityKey.PROXY_ACCESS;
    case 'range_reads':
      return ContractCapabilityKey.RANGE_READS;
    case 'signed_url_access':
      return ContractCapabilityKey.SIGNED_URL_ACCESS;
    case 'time_travel':
      return ContractCapabilityKey.TIME_TRAVEL;
    case 'timestamp_ntz':
      return ContractCapabilityKey.TIMESTAMP_NTZ;
    case 'unknown_protocol_features':
      return ContractCapabilityKey.UNKNOWN_PROTOCOL_FEATURES;
  }
}

function contractCapabilityState(value: CapabilityState): ContractCapabilityState | undefined {
  switch (value) {
    case 'supported':
      return ContractCapabilityState.SUPPORTED;
    case 'native_only':
      return ContractCapabilityState.NATIVE_ONLY;
    case 'unsupported':
      return ContractCapabilityState.UNSUPPORTED;
    case 'experimental':
      return ContractCapabilityState.EXPERIMENTAL;
  }
}

function contractQueryMetrics(metrics: QueryMetricsSummary): ContractQueryMetricsSummary {
  const result = create(ContractQueryMetricsSummarySchema);
  const writable = result as unknown as Record<string, unknown>;
  const source = metrics as unknown as Record<string, unknown>;
  for (const [sdkName, contractName] of QUERY_METRIC_FIELDS) {
    const value = source[sdkName];
    if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) {
      writable[contractName] = BigInt(value);
    }
  }
  if (metrics.access_mode) {
    result.accessMode =
      metrics.access_mode === 'cloud_object_store'
        ? ContractBrowserAccessMode.CLOUD_OBJECT_STORE
        : ContractBrowserAccessMode.BROWSER_SAFE_HTTP;
  }
  return result;
}

function sdkQueryMetrics(
  metrics: ContractQueryMetricsSummary | ContractBrowserWorkerRangeReadMetricsEvent | undefined,
): QueryMetricsSummary {
  const result: QueryMetricsSummary = {
    bytes_fetched: 0,
    duration_ms: 0,
    files_touched: 0,
    files_skipped: 0,
  };
  if (!metrics) return result;
  const target = result as unknown as Record<string, unknown>;
  const source = metrics as unknown as Record<string, unknown>;
  for (const [sdkName, contractName] of QUERY_METRIC_FIELDS) {
    const value = source[contractName];
    if (typeof value === 'bigint') {
      target[sdkName] = safeContractInteger(value, `metrics.${String(contractName)}`);
    }
  }
  if (metrics.accessMode === ContractBrowserAccessMode.CLOUD_OBJECT_STORE) {
    result.access_mode = 'cloud_object_store';
  } else if (metrics.accessMode === ContractBrowserAccessMode.BROWSER_SAFE_HTTP) {
    result.access_mode = 'browser_safe_http';
  }
  return result;
}

function contractQueryErrorCode(code: QueryRunError['code']): ContractQueryErrorCode {
  switch (code) {
    case 'access_denied':
      return ContractQueryErrorCode.ACCESS_DENIED;
    case 'invalid_request':
      return ContractQueryErrorCode.INVALID_REQUEST;
    case 'object_not_found':
      return ContractQueryErrorCode.OBJECT_NOT_FOUND;
    case 'object_store_protocol':
      return ContractQueryErrorCode.OBJECT_STORE_PROTOCOL;
    case 'security_policy_violation':
      return ContractQueryErrorCode.SECURITY_POLICY_VIOLATION;
    case 'unsupported_feature':
      return ContractQueryErrorCode.UNSUPPORTED_FEATURE;
    case 'cancelled':
    case 'deadline':
    case 'execution_failed':
    case undefined:
      return ContractQueryErrorCode.EXECUTION_FAILED;
    default:
      return ContractQueryErrorCode.EXECUTION_FAILED;
  }
}

function queryOutcomeFromCompleted(
  completed: ExecutionCompleted,
  page: QueryPageRequest,
  fallbackReason: AxonQueryResult['fallbackReason'],
  elapsedMs: number,
): QueryRunOutcome {
  if (!completed.response || !completed.result) {
    return {
      status: 'error',
      message: 'Browser executor returned an incomplete terminal result.',
      code: 'execution_failed',
      elapsed_ms: elapsedMs,
    };
  }
  return {
    status: 'done',
    result: resultPageFromContractPreview(completed.preview, page),
    metrics: sdkQueryMetrics(completed.response.metrics),
    executed_on: sdkExecutionTarget(completed.response.executedOn),
    capabilities: sdkCapabilityReport(completed.response.capabilities),
    fallback_reason: fallbackReason,
    explain: completed.response.explain,
    elapsed_ms: elapsedMs,
  };
}

function createAsyncQueue<T>() {
  const pending: T[] = [];
  let closed = false;
  let wake: (() => void) | undefined;
  return {
    push(value: T) {
      if (closed) return;
      pending.push(value);
      const notify = wake;
      wake = undefined;
      notify?.();
    },
    close() {
      closed = true;
      const notify = wake;
      wake = undefined;
      notify?.();
    },
    async *stream(): AsyncIterable<T> {
      while (!closed || pending.length > 0) {
        if (pending.length === 0) {
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
          continue;
        }
        yield pending.shift()!;
      }
    },
  };
}

export async function runQuery(
  input: BrowserExecuteInput,
  onEvent: (event: QueryEvent) => void,
  source: QueryTableSource,
  signal: AbortSignal = new AbortController().signal,
  deadlineSignal?: AbortSignal,
): Promise<QueryRunOutcome> {
  const startedAt = performance.now();
  const since = () => Math.round(performance.now() - startedAt);
  const query = input.request.query;
  if (!query) {
    return queryFailureOutcome(
      new BrowserReadResolutionFailure('Execution request omitted its query.', 'execution_failed'),
      since(),
      undefined,
    );
  }
  const page = queryPageFromRequest(query);
  const executionId = input.request.executionId;
  const target = sdkExecutionTarget(query.preferredTarget);
  const deadlineAt = input.request.deadline ? timestampMs(input.request.deadline) : 0;

  let runtimeFailure: unknown;
  let completed: ExecutionCompleted | undefined;
  let fallbackReason: AxonQueryResult['fallbackReason'];
  let cancelActiveExecution: ((request: CancelRequest) => void) | undefined;
  const delegate: ExecutionProvider = {
    async *execute(validatedInput) {
      yield acceptedExecuteResponse(executionId);
      const events = createAsyncQueue<ExecuteResponse>();
      const terminal = (async (): Promise<ExecuteResponse> => {
        try {
          const browserRead =
            validatedInput.request.binding.case === 'browserRead'
              ? validatedInput.request.binding.value
              : undefined;
          const requestId = executionRequestId(executionId);
          const execution = await runCancelableQueryStages({
            signal,
            deadlineSignal,
            remainingTime: () => deadlineAt - Date.now(),
            getSession: () => getSession(source, browserRead),
            openTable: (state) => ensureTable(state, signal, executionId, validatedInput),
            cancelQuery: () => {
              provider.cancel(cancelExecutionRequest(executionId));
            },
            execute: async (state) => {
              const setupMetrics = pendingSessionSetupMetrics(state);
              let emittedSetupMetricsEvent = false;
              const setupMetricsForEvent = () => {
                if (emittedSetupMetricsEvent) return undefined;
                emittedSetupMetricsEvent = true;
                return setupMetrics;
              };
              const handler: EventHandler = (envelope) => {
                const context =
                  'progress' in envelope
                    ? envelope.progress.context
                    : 'log' in envelope
                      ? envelope.log.context
                      : 'range_read_metrics' in envelope
                        ? envelope.range_read_metrics.context
                        : undefined;
                if (context && context.request_id === requestId) {
                  const response = executeResponseForWorkerEvent(
                    envelope,
                    executionId,
                    since(),
                    'range_read_metrics' in envelope ? setupMetricsForEvent() : undefined,
                  );
                  if (response) events.push(response);
                  return;
                }
                // Fallback is an SDK compatibility event with no generated E9 event arm.
                if ('fallback' in envelope && envelope.fallback.context.request_id === requestId) {
                  onEvent({
                    kind: 'fallback',
                    reason: envelope.fallback.reason,
                    elapsed_ms: since(),
                  });
                }
              };
              const removeHandler = () => eventListeners.delete(handler);
              eventListeners.add(handler);
              cancelActiveExecution = (request) => {
                if (request.executionId !== executionId) return;
                state.client.cancelQuery(executionId, {
                  requestId: executionCancelSpanId(executionId, 1),
                });
              };
              try {
                const result: AxonQueryResult = await state.client.query(
                  validatedInput.table.name,
                  {
                    table_uri: state.snapshot.table_uri,
                    snapshot_version: state.snapshot.snapshot_version,
                    sql: query.sql,
                    preferred_target: target,
                    options: queryExecutionOptionsForRequest(query),
                  },
                  queryClientOptionsForRequest(validatedInput.request),
                );
                return { result, setupMetrics };
              } finally {
                cancelActiveExecution = undefined;
                removeHandler();
              }
            },
          });
          const { result, setupMetrics } = execution.result;
          const metrics = mergeQueryMetrics(result.response.metrics, setupMetrics);
          fallbackReason = result.fallbackReason ?? result.response.fallback_reason;
          markSessionSetupMetricsEmitted(execution.session);
          return completedExecuteResponse(validatedInput, result, metrics, 1n);
        } catch (error) {
          runtimeFailure = error;
          if (queryFailureInvalidatesSession(error)) {
            discardQuerySession(source);
          }
          return failedExecuteResponse(
            executionId,
            queryFailureOutcome(error, since(), target),
            1n,
          );
        } finally {
          events.close();
        }
      })();
      for await (const response of events.stream()) {
        yield response;
      }
      yield await terminal;
    },
    cancel(request) {
      const cancel = request.executionId === executionId ? cancelActiveExecution : undefined;
      cancel?.(request);
      return create(CancelResponseSchema, {
        executionId: request.executionId,
        state: cancel
          ? ExecutionLifecycleState.CANCEL_REQUESTED
          : ExecutionLifecycleState.UNSPECIFIED,
      });
    },
  };
  const provider = createValidatedBrowserExecutionProvider(delegate, {
    isCurrentLocalObjectUrl: isCurrentLocalDeltaObjectUrl,
  });

  let terminalFrameCount = 0;
  try {
    for await (const response of provider.execute(input)) {
      if (response.item.case === 'event') {
        const event = queryEventFromExecuteResponse(response, since());
        if (event) onEvent(event);
        continue;
      }
      if (response.item.case === 'terminal') {
        terminalFrameCount += 1;
        if (response.item.value.executionId !== executionId || terminalFrameCount > 1) {
          throw new BrowserReadResolutionFailure(
            'Browser executor returned an invalid terminal frame.',
            'execution_failed',
          );
        }
        if (response.item.value.state?.outcome.case === 'completed') {
          completed = response.item.value.state.outcome.value;
        }
      }
    }
  } catch (error) {
    runtimeFailure = error;
  }
  if (runtimeFailure !== undefined) {
    if (queryFailureInvalidatesSession(runtimeFailure)) {
      discardQuerySession(source);
    }
    return queryFailureOutcome(runtimeFailure, since(), target);
  }
  if (!completed || terminalFrameCount !== 1) {
    return {
      status: 'error',
      message: 'Browser executor did not return one completed terminal frame.',
      code: 'execution_failed',
      target,
      elapsed_ms: since(),
    };
  }
  return queryOutcomeFromCompleted(completed, page, fallbackReason, since());
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
  target: ExecutionTarget | undefined,
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
