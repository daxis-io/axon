import {
  ARROW_IPC_STREAM_CONTENT_TYPE,
  type BrowserWorkerCancelCommand,
  type BrowserWorkerCommand,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerLogLevel,
  type BrowserWorkerProgressStage,
  type BrowserWorkerResultPreview,
  type BrowserWorkerSqlDelivery,
  type BrowserWorkerSqlCommand,
  type FallbackReason,
  type QueryError,
  type QueryMetricsSummary,
  type QueryResponse,
  type WireBrowserWorkerResponseEnvelope,
  redactUrlSecrets,
} from './axon-browser-sdk';
import {
  CoordinatorQueryLifecycle,
  MAX_COORDINATOR_REQUESTS,
  coordinatorHasCapacity,
  coordinatorMaxArrowIpcBytes,
} from './sandbox-query-coordinator-policy';
import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  QueryStage,
  requireProtocolVersion,
  requireTerminalMetadata,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
  type PrivateTerminalMetadata,
  type PrivateTerminalStatus,
} from './sandbox-query-stream-protocol';

type DecimalString = string;

type SandboxCacheMetrics = {
  session_cached_bytes: DecimalString;
  session_table_count: DecimalString;
  max_session_cached_bytes: DecimalString;
};

type SandboxWireResultPreview = Omit<BrowserWorkerResultPreview, 'row_count'> & {
  row_count: DecimalString;
};

type SandboxTerminalMetadata = PrivateTerminalMetadata & {
  response?: QueryResponse;
  preview?: SandboxWireResultPreview;
  preview_string_bytes?: DecimalString;
  preview_duration_ms?: DecimalString;
  cache_metrics?: SandboxCacheMetrics;
};

type PublicWorkerScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<BrowserWorkerCommand>) => void,
  ): void;
  postMessage(
    message: WireBrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope,
    transfer?: Transferable[],
  ): void;
};

type ActiveCoordinatorQuery = {
  command: BrowserWorkerSqlCommand;
  context: BrowserWorkerEventContext;
  delivery: BrowserWorkerSqlDelivery;
  lifecycle: CoordinatorQueryLifecycle;
  stage: QueryStage;
  maxBytes: number;
  deadline: ReturnType<typeof setTimeout>;
  watchdog?: ReturnType<typeof setTimeout>;
};

type PendingForwardedCommand = {
  context: BrowserWorkerEventContext;
};

type CoordinatorRuntimeConfig = {
  deadlineMs: number;
  watchdogMs: number;
  maxRequests: number;
  crashOnCommandNumber?: number;
  firstChildUrl?: string;
};

type CoordinatorConfiguredScope = PublicWorkerScope & {
  __AXON_SANDBOX_QUERY_COORDINATOR_TEST_CONFIG__?: Partial<CoordinatorRuntimeConfig>;
};

const DEFAULT_PRIVATE_QUERY_DEADLINE_MS = 120_000;
const DEFAULT_PRIVATE_QUERY_WATCHDOG_MS = 5_000;
const workerScope = self as unknown as PublicWorkerScope;
const runtimeConfig = coordinatorRuntimeConfig(self as unknown as CoordinatorConfiguredScope);
const activeQueries = new Map<string, ActiveCoordinatorQuery>();
const pendingForwardedCommands = new Map<string, PendingForwardedCommand>();
let childCreationCount = 0;
let coordinatorCommandCount = 0;
let child = createChild();

workerScope.addEventListener('message', (event) => {
  const command = event.data;
  if ('cancel' in command) {
    handleCancel(command.cancel);
    return;
  }
  if ('sql' in command) {
    startCoordinatedQuery(command.sql);
    return;
  }
  forwardCommand(command);
});

function forwardCommand(command: BrowserWorkerCommand): void {
  const requestId = commandRequestId(command);
  const context = commandContext(command);
  if (activeQueries.has(requestId) || pendingForwardedCommands.has(requestId)) {
    postQueryError(
      requestId,
      context,
      queryError('invalid_request', `request '${requestId}' is already active in the coordinator`),
    );
    return;
  }
  if (
    !coordinatorHasCapacity(
      activeQueries.size,
      pendingForwardedCommands.size,
      runtimeConfig.maxRequests,
    )
  ) {
    postQueryError(
      requestId,
      context,
      queryError(
        'fallback_required',
        `browser coordinator request capacity (${runtimeConfig.maxRequests}) is exhausted`,
        'browser_runtime_constraint',
      ),
    );
    return;
  }
  pendingForwardedCommands.set(requestId, { context });
  postToChild({
    kind: 'command',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    command,
  });
}

function createChild(): Worker {
  const configuredFirstChildUrl =
    childCreationCount === 0 ? runtimeConfig.firstChildUrl : undefined;
  childCreationCount += 1;
  const childUrl = configuredFirstChildUrl
    ? new URL(configuredFirstChildUrl)
    : new URL('./sandbox-query-child-worker.ts', import.meta.url);
  const next = new Worker(childUrl, {
    type: 'module',
    name: 'axon-sandbox-query-child',
  });
  next.addEventListener('message', (event: MessageEvent<PrivateChildMessage>) => {
    if (child !== next) return;
    handleChildMessage(event.data);
  });
  next.addEventListener('error', (event) => {
    if (child !== next) return;
    handleChildCrash(event.message || 'sandbox query child worker crashed');
  });
  next.addEventListener('messageerror', () => {
    if (child !== next) return;
    handleChildCrash('sandbox query child worker emitted an invalid structured-clone message');
  });
  return next;
}

function startCoordinatedQuery(command: BrowserWorkerSqlCommand): void {
  const queryId = command.request_id;
  const context = commandContext({ sql: command });
  if (activeQueries.has(queryId) || pendingForwardedCommands.has(queryId)) {
    postQueryError(
      queryId,
      context,
      queryError('invalid_request', `query '${queryId}' is already active in the coordinator`),
    );
    return;
  }
  if (
    !coordinatorHasCapacity(
      activeQueries.size,
      pendingForwardedCommands.size,
      runtimeConfig.maxRequests,
    )
  ) {
    postQueryError(
      queryId,
      context,
      queryError(
        'fallback_required',
        `browser coordinator request capacity (${runtimeConfig.maxRequests}) is exhausted`,
        'browser_runtime_constraint',
      ),
    );
    return;
  }
  const delivery = command.delivery ?? 'single_buffer';
  if (delivery !== 'single_buffer' && delivery !== 'chunked_buffers') {
    postQueryError(
      queryId,
      context,
      queryError('invalid_request', `unsupported Arrow IPC delivery '${String(delivery)}'`),
    );
    return;
  }
  let maxBytes: number;
  try {
    maxBytes = coordinatorMaxArrowIpcBytes(command);
  } catch (error) {
    postQueryError(
      queryId,
      context,
      queryError('invalid_request', error instanceof Error ? error.message : String(error)),
    );
    return;
  }
  const deadline = setTimeout(() => {
    const active = activeQueries.get(queryId);
    if (!active) return;
    beginAuthoritativeDrain(
      active,
      'deadline_exceeded',
      queryError(
        'execution_failed',
        'browser DataFusion query deadline exceeded in the coordinator',
      ),
      'deadline_exceeded',
    );
  }, runtimeConfig.deadlineMs);
  const active: ActiveCoordinatorQuery = {
    command,
    context,
    delivery,
    lifecycle: new CoordinatorQueryLifecycle(),
    stage: new QueryStage(queryId),
    maxBytes,
    deadline,
  };
  activeQueries.set(queryId, active);
  postToChild({
    kind: 'command',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    command: { sql: command },
  });
}

function handleCancel(command: BrowserWorkerCancelCommand): void {
  const queryId = command.query_id ?? command.request_id;
  const active = activeQueries.get(queryId);
  if (!active) return;
  beginAuthoritativeDrain(
    active,
    'cancelled',
    queryError(
      'execution_failed',
      'experimental browser DataFusion query cancelled in the coordinator',
    ),
    'cancelled',
  );
}

function handleChildMessage(message: PrivateChildMessage): void {
  try {
    requireProtocolVersion(message.version);
    if (message.kind === 'ready') return;
    if (message.kind === 'public') {
      workerScope.postMessage(message.envelope);
      const requestId = responseRequestId(message.envelope);
      if (requestId) pendingForwardedCommands.delete(requestId);
      return;
    }
    const active = activeQueries.get(message.query_id);
    if (!active) return;
    if (active.lifecycle.state === 'draining') {
      if (message.kind !== 'stream_chunk') finishActiveQuery(message.query_id);
      return;
    }

    if (message.kind === 'stream_chunk') {
      const credit = active.stage.stage(message);
      if (active.stage.stagedByteLength > active.maxBytes) {
        throw queryError(
          'fallback_required',
          `browser coordinator exceeded max_arrow_ipc_bytes (${active.stage.stagedByteLength} > ${active.maxBytes})`,
          'browser_runtime_constraint',
        );
      }
      postToChild({
        kind: 'credit',
        version: PRIVATE_STREAM_PROTOCOL_VERSION,
        query_id: message.query_id,
        sequence: message.sequence,
        credit_class: credit.credit_class,
        bytes: credit.byte_length,
      });
      return;
    }
    if (message.kind === 'stream_terminal') {
      finishFromTerminal(active, message.metadata);
      return;
    }
    if (!active.lifecycle.beginDrain('failed')) return;
    active.stage.discard();
    postQueryError(message.query_id, active.context, message.error);
    finishActiveQuery(message.query_id);
  } catch (error) {
    const queryId = 'query_id' in message ? message.query_id : undefined;
    if (!queryId) return;
    const active = activeQueries.get(queryId);
    if (!active) return;
    beginAuthoritativeDrain(active, 'failed', normalizeQueryError(error), 'cancelled');
  }
}

function finishFromTerminal(
  active: ActiveCoordinatorQuery,
  metadataValue: PrivateTerminalMetadata,
): void {
  requireTerminalMetadata(metadataValue);
  const metadata = metadataValue as SandboxTerminalMetadata;
  if (metadata.status !== 'succeeded') {
    if (!active.lifecycle.beginDrain(metadata.status)) return;
    active.stage.discard();
    const error =
      metadata.error ??
      queryError(
        'execution_failed',
        metadata.status === 'deadline_exceeded'
          ? 'browser DataFusion query deadline exceeded'
          : metadata.status === 'cancelled'
            ? 'experimental browser DataFusion query cancelled during Arrow IPC cursor pull'
            : 'browser DataFusion query failed without structured error metadata',
      );
    postQueryError(active.command.request_id, active.context, error, metadata.status);
    finishActiveQuery(active.command.request_id);
    return;
  }

  let chunks: Uint8Array[];
  try {
    chunks = active.stage.commit(metadata);
    if (!active.lifecycle.beginDrain('succeeded')) return;
    commitSuccessfulQuery(active, metadata, chunks);
  } catch (error) {
    active.lifecycle.beginDrain('failed');
    active.stage.discard();
    postQueryError(active.command.request_id, active.context, normalizeQueryError(error));
  } finally {
    finishActiveQuery(active.command.request_id);
  }
}

function commitSuccessfulQuery(
  active: ActiveCoordinatorQuery,
  metadata: SandboxTerminalMetadata,
  chunks: Uint8Array[],
): void {
  if (!metadata.response || !metadata.preview || !metadata.cache_metrics) {
    throw queryError('execution_failed', 'successful child terminal omitted response metadata');
  }
  const arrowIpcByteLength = decimalNumber(metadata.arrow_ipc_byte_length);
  const preview = normalizePreview(metadata.preview);
  const previewStringBytes = decimalNumber(metadata.preview_string_bytes ?? '0');
  const previewDurationMs = decimalNumber(metadata.preview_duration_ms ?? '0');
  const publicChunkCount =
    active.delivery === 'chunked_buffers' ? chunks.length : arrowIpcByteLength === 0 ? 0 : 1;
  const response: QueryResponse = {
    ...metadata.response,
    metrics: {
      ...metadata.response.metrics,
      arrow_ipc_bytes: arrowIpcByteLength,
      arrow_ipc_chunk_count: publicChunkCount,
      preview_rows: preview.rows.length,
      preview_string_bytes: previewStringBytes,
      preview_duration_ms: previewDurationMs,
    },
  };

  emitRangeReadMetrics(active.context, response.metrics);
  if (response.fallback_reason) emitFallback(active.context, response.fallback_reason);
  emitCacheMetrics(active.context, metadata.cache_metrics);
  emitProgress(active.context, 'arrow_ipc_ready');

  if (active.delivery === 'chunked_buffers') {
    postCommittedChunks(active.context, active.command.request_id, chunks);
    emitProgress(active.context, 'finished');
    postResponse({
      success: {
        request_id: active.command.request_id,
        response,
        result: {
          format: 'stream',
          content_type: ARROW_IPC_STREAM_CONTENT_TYPE,
          delivery: 'chunked_buffers',
          byte_length: arrowIpcByteLength,
          chunk_count: publicChunkCount,
        },
        preview,
      },
    });
    return;
  }

  const bytes = checkedSingleBuffer(chunks, arrowIpcByteLength);
  emitProgress(active.context, 'finished');
  postResponse(
    {
      success: {
        request_id: active.command.request_id,
        response,
        result: {
          format: 'stream',
          content_type: ARROW_IPC_STREAM_CONTENT_TYPE,
          delivery: 'single_buffer',
          bytes,
          byte_length: arrowIpcByteLength,
          chunk_count: publicChunkCount,
        },
        preview,
      },
    },
    [bytes.buffer],
  );
}

function postCommittedChunks(
  context: BrowserWorkerEventContext,
  requestId: string,
  chunks: Uint8Array[],
): void {
  let byteOffset = 0;
  for (let sequence = 0; sequence < chunks.length; sequence += 1) {
    const chunk = chunks[sequence];
    requireExactSizedChunk(chunk);
    const chunkByteLength = chunk.byteLength;
    workerScope.postMessage(
      {
        arrow_ipc_chunk: {
          context,
          request_id: requestId,
          sequence,
          byte_offset: byteOffset,
          byte_length: chunkByteLength,
          bytes: chunk,
        },
      },
      [chunk.buffer],
    );
    byteOffset += chunkByteLength;
  }
}

function checkedSingleBuffer(chunks: Uint8Array[], expectedLength: number): Uint8Array {
  if (!Number.isSafeInteger(expectedLength) || expectedLength < 0) {
    throw queryError('execution_failed', 'Arrow IPC terminal length was not a safe integer');
  }
  const output = new Uint8Array(expectedLength);
  let offset = 0;
  for (const chunk of chunks) {
    requireExactSizedChunk(chunk);
    const next = offset + chunk.byteLength;
    if (!Number.isSafeInteger(next) || next > output.byteLength) {
      throw queryError('execution_failed', 'Arrow IPC staged chunks exceeded terminal length');
    }
    output.set(chunk, offset);
    offset = next;
  }
  if (offset !== output.byteLength) {
    throw queryError('execution_failed', 'Arrow IPC staged chunks did not fill terminal length');
  }
  return output;
}

function requireExactSizedChunk(chunk: Uint8Array): void {
  if (chunk.byteOffset !== 0 || chunk.byteLength !== chunk.buffer.byteLength) {
    throw queryError('execution_failed', 'Arrow IPC staged chunk was not exact-sized');
  }
}

function beginAuthoritativeDrain(
  active: ActiveCoordinatorQuery,
  status: PrivateTerminalStatus,
  error: QueryError,
  cancelReason: 'cancelled' | 'deadline_exceeded',
): void {
  if (!active.lifecycle.beginDrain(status)) return;
  clearTimeout(active.deadline);
  active.stage.discard();
  postQueryError(active.command.request_id, active.context, error, status);
  postToChild({
    kind: 'cancel',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: active.command.request_id,
    reason: cancelReason,
  });
  active.watchdog = setTimeout(() => {
    const current = activeQueries.get(active.command.request_id);
    if (current !== active || active.lifecycle.state !== 'draining') return;
    handleChildCrash(
      `sandbox query child did not drain '${active.command.request_id}' within ${runtimeConfig.watchdogMs}ms`,
    );
  }, runtimeConfig.watchdogMs);
}

function finishActiveQuery(queryId: string): void {
  const active = activeQueries.get(queryId);
  if (!active) return;
  clearTimeout(active.deadline);
  if (active.watchdog) clearTimeout(active.watchdog);
  if (active.lifecycle.state === 'draining') active.lifecycle.finishDrain();
  activeQueries.delete(queryId);
}

function handleChildCrash(message: string): void {
  const error = queryError('execution_failed', message);
  for (const [queryId, active] of activeQueries) {
    active.stage.discard();
    if (active.lifecycle.beginDrain('failed')) {
      postQueryError(queryId, active.context, error);
    }
    clearTimeout(active.deadline);
    if (active.watchdog) clearTimeout(active.watchdog);
    if (active.lifecycle.state === 'draining') active.lifecycle.finishDrain();
  }
  activeQueries.clear();
  for (const [requestId, pending] of pendingForwardedCommands) {
    postQueryError(requestId, pending.context, error);
  }
  pendingForwardedCommands.clear();
  child.terminate();
  child = createChild();
}

function coordinatorRuntimeConfig(scope: CoordinatorConfiguredScope): CoordinatorRuntimeConfig {
  const configured = scope.__AXON_SANDBOX_QUERY_COORDINATOR_TEST_CONFIG__;
  return {
    deadlineMs: configuredPositiveInteger(
      configured?.deadlineMs,
      DEFAULT_PRIVATE_QUERY_DEADLINE_MS,
    ),
    watchdogMs: configuredPositiveInteger(
      configured?.watchdogMs,
      DEFAULT_PRIVATE_QUERY_WATCHDOG_MS,
    ),
    maxRequests: configuredPositiveInteger(configured?.maxRequests, MAX_COORDINATOR_REQUESTS),
    ...(typeof configured?.crashOnCommandNumber === 'number'
      ? {
          crashOnCommandNumber: configuredPositiveInteger(configured.crashOnCommandNumber, 1),
        }
      : {}),
    ...(typeof configured?.firstChildUrl === 'string'
      ? { firstChildUrl: configured.firstChildUrl }
      : {}),
  };
}

function configuredPositiveInteger(value: number | undefined, fallback: number): number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

function postQueryError(
  requestId: string,
  context: BrowserWorkerEventContext,
  error: QueryError,
  status?: PrivateTerminalStatus,
): void {
  const normalized = redactQueryError(error);
  emitErrorEvents(context, normalized, status);
  postResponse({ error: { request_id: requestId, error: normalized } });
}

function postToChild(message: PrivateCoordinatorMessage): void {
  child.postMessage(message);
  if (message.kind === 'command') {
    coordinatorCommandCount += 1;
    if (runtimeConfig.crashOnCommandNumber === coordinatorCommandCount) {
      handleChildCrash('injected sandbox query child crash');
    }
  }
}

function normalizePreview(preview: SandboxWireResultPreview): BrowserWorkerResultPreview {
  return { ...preview, row_count: decimalNumber(preview.row_count) };
}

function emitProgress(context: BrowserWorkerEventContext, stage: BrowserWorkerProgressStage): void {
  postEvent({ progress: { context, stage } });
}

function emitLog(
  context: BrowserWorkerEventContext,
  level: BrowserWorkerLogLevel,
  message: string,
): void {
  postEvent({ log: { context, level, message } });
}

function emitRangeReadMetrics(
  context: BrowserWorkerEventContext,
  metrics: QueryMetricsSummary,
): void {
  postEvent({
    range_read_metrics: {
      context,
      bytes_fetched: metrics.bytes_fetched,
      files_touched: metrics.files_touched,
      files_skipped: metrics.files_skipped,
      prebootstrap_fail_open_count: metrics.prebootstrap_fail_open_count,
      prebootstrap_files_pruned: metrics.prebootstrap_files_pruned,
      footer_reads_avoided: metrics.footer_reads_avoided,
      prebootstrap_candidate_files: metrics.prebootstrap_candidate_files,
      row_groups_touched: metrics.row_groups_touched ?? 0,
      row_groups_skipped: metrics.row_groups_skipped ?? 0,
      footer_reads: metrics.footer_reads,
      bootstrap_footer_range_reads: metrics.bootstrap_footer_range_reads,
      scan_footer_range_reads: metrics.scan_footer_range_reads,
      scan_data_range_reads: metrics.scan_data_range_reads,
      duplicate_range_reads: metrics.duplicate_range_reads,
      coalesced_range_reads: metrics.coalesced_range_reads,
      coalesced_gap_bytes_fetched: metrics.coalesced_gap_bytes_fetched,
      scan_overfetch_bytes: metrics.scan_overfetch_bytes,
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
      descriptor_resolution_count: metrics.descriptor_resolution_count,
      delta_log_manifest_list_count: metrics.delta_log_manifest_list_count,
      delta_log_manifest_list_duration_ms: metrics.delta_log_manifest_list_duration_ms,
      snapshot_resolve_count: metrics.snapshot_resolve_count,
      snapshot_resolve_duration_ms: metrics.snapshot_resolve_duration_ms,
      descriptor_cache_hit: metrics.descriptor_cache_hit,
      session_reuse_count: metrics.session_reuse_count,
      opened_table_reuse_count: metrics.opened_table_reuse_count,
      identity_refresh_count: metrics.identity_refresh_count,
      access_envelope_refresh_count: metrics.access_envelope_refresh_count,
      rows_emitted: metrics.rows_emitted ?? 0,
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
  });
}

function emitCacheMetrics(context: BrowserWorkerEventContext, metrics: SandboxCacheMetrics): void {
  postEvent({
    cache_metrics: {
      context,
      session_cached_bytes: decimalNumber(metrics.session_cached_bytes),
      session_table_count: decimalNumber(metrics.session_table_count),
      max_session_cached_bytes: decimalNumber(metrics.max_session_cached_bytes),
    },
  });
}

function emitFallback(context: BrowserWorkerEventContext, reason: FallbackReason): void {
  postEvent({ fallback: { context, reason } });
}

function emitErrorEvents(
  context: BrowserWorkerEventContext,
  error: QueryError,
  status?: PrivateTerminalStatus,
): void {
  if (error.fallback_reason) emitFallback(context, error.fallback_reason);
  if (status === 'cancelled' || status === 'deadline_exceeded' || isCancellation(error)) {
    postEvent({ cancellation: { context, error } });
  }
  emitLog(context, 'error', error.message);
  postEvent({ terminal_error: { context, error } });
}

function isCancellation(error: QueryError): boolean {
  return (
    error.code === 'execution_failed' &&
    (error.message.includes('cancelled') || error.message.includes('deadline'))
  );
}

function postEvent(event: BrowserWorkerEventEnvelope): void {
  workerScope.postMessage(event);
}

function postResponse(
  response: WireBrowserWorkerResponseEnvelope,
  transfer: Transferable[] = [],
): void {
  workerScope.postMessage(response, transfer);
}

function commandContext(command: BrowserWorkerCommand): BrowserWorkerEventContext {
  if ('sql' in command) {
    return {
      phase: 'query',
      request_id: command.sql.request_id,
      query_id: command.sql.request_id,
      table_name: command.sql.name,
    };
  }
  if ('cancel' in command) {
    return {
      phase: 'query',
      request_id: command.cancel.request_id,
      query_id: command.cancel.query_id ?? command.cancel.request_id,
    };
  }
  if ('open_delta_table' in command) {
    return {
      phase: 'open',
      request_id: command.open_delta_table.request_id,
      table_name: command.open_delta_table.name,
    };
  }
  if ('open_parquet_dataset' in command) {
    return {
      phase: 'open',
      request_id: command.open_parquet_dataset.request_id,
      table_name: command.open_parquet_dataset.name,
    };
  }
  if ('inspect_parquet' in command) {
    return {
      phase: 'inspect',
      request_id: command.inspect_parquet.request_id,
      table_name: command.inspect_parquet.name,
    };
  }
  if ('open_table' in command) {
    return {
      phase: 'open',
      request_id: command.open_table.request_id,
      table_name: command.open_table.name,
    };
  }
  return {
    phase: 'open',
    request_id: command.dispose.request_id,
    table_name: command.dispose.name,
  };
}

function commandRequestId(command: BrowserWorkerCommand): string {
  if ('open_delta_table' in command) return command.open_delta_table.request_id;
  if ('open_parquet_dataset' in command) return command.open_parquet_dataset.request_id;
  if ('inspect_parquet' in command) return command.inspect_parquet.request_id;
  if ('sql' in command) return command.sql.request_id;
  if ('cancel' in command) return command.cancel.request_id;
  if ('open_table' in command) return command.open_table.request_id;
  return command.dispose.request_id;
}

function responseRequestId(
  envelope: WireBrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope,
): string | undefined {
  if ('opened' in envelope) return envelope.opened.request_id;
  if ('success' in envelope) return envelope.success.request_id;
  if ('parquet_inspection' in envelope) return envelope.parquet_inspection.request_id;
  if ('disposed' in envelope) return envelope.disposed.request_id;
  if ('error' in envelope) return envelope.error.request_id;
  return undefined;
}

function normalizeQueryError(error: unknown): QueryError {
  if (isQueryError(error)) return redactQueryError(error);
  if (error instanceof Error) return queryError('execution_failed', error.message);
  return queryError('execution_failed', String(error));
}

function isQueryError(value: unknown): value is QueryError {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<QueryError>;
  return (
    typeof candidate.code === 'string' &&
    typeof candidate.message === 'string' &&
    candidate.target === 'browser_wasm'
  );
}

function redactQueryError(error: QueryError): QueryError {
  return { ...error, message: redactUrlSecrets(error.message) };
}

function queryError(
  code: QueryError['code'],
  message: string,
  fallbackReason?: FallbackReason,
): QueryError {
  return {
    code,
    message: redactUrlSecrets(message),
    target: 'browser_wasm',
    ...(fallbackReason ? { fallback_reason: fallbackReason } : {}),
  };
}

function decimalNumber(value: DecimalString | number): number {
  const parsed = typeof value === 'number' ? value : Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw queryError('execution_failed', 'child terminal decimal exceeded a safe integer');
  }
  return parsed;
}
