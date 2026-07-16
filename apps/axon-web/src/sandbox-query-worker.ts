import {
  ARROW_IPC_STREAM_CONTENT_TYPE,
  BROWSER_SAFE_ARROW_IPC_BYTES,
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
  PRIVATE_STREAM_PROTOCOL_VERSION,
  QueryStage,
  requireProtocolVersion,
  requireTerminalMetadata,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
  type PrivateTerminalMetadata,
  type PrivateTerminalStatus,
} from './sandbox-query-stream-protocol';
import { isBrowserDataFusionCancellation } from './services/query-cancellation.ts';
import {
  appendQuerySessionInvalidation,
  querySessionInvalidationMessage,
} from './services/query-session-invalidation.ts';

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
  stage: QueryStage;
  maxBytes: number;
  deadline: ReturnType<typeof setTimeout>;
  cancellation?: {
    reason: 'cancelled' | 'deadline_exceeded';
    grace: ReturnType<typeof setTimeout>;
  };
};

type PendingForwardedCommand = {
  context: BrowserWorkerEventContext;
};

const PRIVATE_QUERY_DEADLINE_MS = 120_000;
const PRIVATE_QUERY_CANCELLATION_GRACE_MS = 1_000;
const MAX_COORDINATOR_PENDING_COMMANDS = 32;
const workerScope = self as unknown as PublicWorkerScope;
const activeQueries = new Map<string, ActiveCoordinatorQuery>();
const pendingForwardedCommands = new Map<string, PendingForwardedCommand>();
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
  if (!hasCoordinatorCapacity()) {
    postQueryError(
      requestId,
      context,
      queryError(
        'invalid_request',
        `browser query coordinator capacity ${MAX_COORDINATOR_PENDING_COMMANDS} exceeded`,
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
  const next = new Worker(new URL('./sandbox-query-child-worker.ts', import.meta.url), {
    type: 'module',
    name: 'axon-sandbox-query-child',
  });
  next.addEventListener('message', (event: MessageEvent<PrivateChildMessage>) => {
    if (child !== next) return;
    handleChildMessage(event.data);
  });
  next.addEventListener('error', (event) => {
    if (child !== next) return;
    event.preventDefault();
    handleChildCrash('child worker crashed');
  });
  next.addEventListener('messageerror', () => {
    if (child !== next) return;
    handleChildCrash('child worker emitted an invalid structured-clone message');
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
  if (!hasCoordinatorCapacity()) {
    postQueryError(
      queryId,
      context,
      queryError(
        'invalid_request',
        `browser query coordinator capacity ${MAX_COORDINATOR_PENDING_COMMANDS} exceeded`,
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
  const requestedMax = command.query.options?.runtime_limits?.max_arrow_ipc_bytes;
  const maxBytes =
    typeof requestedMax === 'number' && Number.isSafeInteger(requestedMax) && requestedMax > 0
      ? Math.min(requestedMax, BROWSER_SAFE_ARROW_IPC_BYTES)
      : BROWSER_SAFE_ARROW_IPC_BYTES;
  const deadline = setTimeout(() => {
    requestQueryCancellation(queryId, 'deadline_exceeded');
  }, PRIVATE_QUERY_DEADLINE_MS);
  activeQueries.set(queryId, {
    command,
    context,
    delivery,
    stage: new QueryStage(queryId),
    maxBytes,
    deadline,
  });
  postToChild({
    kind: 'command',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    command: { sql: command },
  });
}

function handleCancel(command: BrowserWorkerCancelCommand): void {
  const queryId = command.query_id ?? command.request_id;
  requestQueryCancellation(queryId, 'cancelled');
}

function requestQueryCancellation(
  queryId: string,
  reason: 'cancelled' | 'deadline_exceeded',
): void {
  const active = activeQueries.get(queryId);
  if (!active || active.cancellation) return;
  postToChild({
    kind: 'cancel',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: queryId,
    reason,
  });
  active.cancellation = {
    reason,
    grace: setTimeout(() => {
      const current = activeQueries.get(queryId);
      if (!current || current.cancellation?.reason !== reason) return;
      recycleAfterUnconfirmedCancellation(queryId, reason);
    }, PRIVATE_QUERY_CANCELLATION_GRACE_MS),
  };
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

    if (message.kind === 'stream_chunk') {
      const credit = active.stage.stage(message);
      if (active.stage.stagedByteLength > active.maxBytes) {
        throw queryError(
          'execution_failed',
          `resource limit runtime_limits.max_arrow_ipc_bytes exceeded: actual ${active.stage.stagedByteLength}, limit ${active.maxBytes}`,
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
    active.stage.discard();
    postQueryError(message.query_id, active.context, message.error);
    finishActiveQuery(message.query_id);
  } catch (error) {
    const queryId = 'query_id' in message ? message.query_id : undefined;
    if (!queryId) return;
    const active = activeQueries.get(queryId);
    if (!active) return;
    active.stage.discard();
    postToChild({
      kind: 'cancel',
      version: PRIVATE_STREAM_PROTOCOL_VERSION,
      query_id: queryId,
      reason: 'cancelled',
    });
    postQueryError(queryId, active.context, normalizeQueryError(error));
    finishActiveQuery(queryId);
  }
}

function finishFromTerminal(
  active: ActiveCoordinatorQuery,
  metadataValue: PrivateTerminalMetadata,
): void {
  requireTerminalMetadata(metadataValue);
  const metadata = metadataValue as SandboxTerminalMetadata;
  if (metadata.status !== 'succeeded') {
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
    commitSuccessfulQuery(active, metadata, chunks);
  } catch (error) {
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

function finishActiveQuery(queryId: string): void {
  const active = activeQueries.get(queryId);
  if (!active) return;
  clearTimeout(active.deadline);
  if (active.cancellation) clearTimeout(active.cancellation.grace);
  activeQueries.delete(queryId);
}

function recycleAfterUnconfirmedCancellation(
  queryId: string,
  reason: 'cancelled' | 'deadline_exceeded',
): void {
  const invalidationReason = 'child did not confirm cancellation before the grace period expired';
  const primaryError =
    reason === 'cancelled'
      ? queryError(
          'execution_failed',
          appendQuerySessionInvalidation(
            'experimental browser DataFusion query cancelled while awaiting worker confirmation',
            invalidationReason,
          ),
        )
      : queryError(
          'execution_failed',
          appendQuerySessionInvalidation(
            'browser DataFusion query deadline exceeded while awaiting worker confirmation',
            invalidationReason,
          ),
        );
  recycleChild(invalidationReason, {
    queryId,
    error: primaryError,
    status: reason,
  });
}

function handleChildCrash(invalidationReason: string): void {
  recycleChild(invalidationReason);
}

function recycleChild(
  invalidationReason: string,
  primary?: {
    queryId: string;
    error: QueryError;
    status: PrivateTerminalStatus;
  },
): void {
  const invalidationError = queryError(
    'execution_failed',
    querySessionInvalidationMessage(invalidationReason),
  );
  for (const [queryId, active] of activeQueries) {
    active.stage.discard();
    if (primary?.queryId === queryId) {
      postQueryError(queryId, active.context, primary.error, primary.status);
    } else {
      postQueryError(queryId, active.context, invalidationError);
    }
    clearTimeout(active.deadline);
    if (active.cancellation) clearTimeout(active.cancellation.grace);
  }
  activeQueries.clear();
  for (const [requestId, pending] of pendingForwardedCommands) {
    postQueryError(requestId, pending.context, invalidationError);
  }
  pendingForwardedCommands.clear();
  const previousChild = child;
  child = createChild();
  previousChild.terminate();
}

function hasCoordinatorCapacity(): boolean {
  return activeQueries.size + pendingForwardedCommands.size < MAX_COORDINATOR_PENDING_COMMANDS;
}

function postQueryError(
  requestId: string,
  context: BrowserWorkerEventContext,
  error: QueryError,
  status?: PrivateTerminalStatus,
): void {
  const normalized = redactQueryError(normalizeArrowOutputBudgetError(error));
  emitErrorEvents(context, normalized, status);
  postResponse({ error: { request_id: requestId, error: normalized } });
}

function postToChild(message: PrivateCoordinatorMessage): void {
  child.postMessage(message);
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
  emitLog(context, 'error', error.message);
  if (status === 'cancelled' || isBrowserDataFusionCancellation(error)) {
    postEvent({ cancellation: { context, error } });
    return;
  }
  postEvent({ terminal_error: { context, error } });
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

function normalizeArrowOutputBudgetError(error: QueryError): QueryError {
  if (!error.message.includes('max_output_ipc_bytes')) return error;
  return queryError(
    'execution_failed',
    `resource limit runtime_limits.max_arrow_ipc_bytes exceeded: ${error.message.replaceAll(
      'max_output_ipc_bytes',
      'max_arrow_ipc_bytes',
    )}`,
  );
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
