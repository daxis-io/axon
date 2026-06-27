import init, { SandboxQueryCancellation, SandboxQuerySession } from './wasm/axon_web_wasm';
import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
  ARROW_IPC_STREAM_CONTENT_TYPE,
  type BrowserWorkerCancelCommand,
  type BrowserWorkerCommand,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerInspectParquetCommand,
  type BrowserWorkerLogLevel,
  type BrowserWorkerOpenDeltaTableCommand,
  type BrowserWorkerOpenParquetDatasetCommand,
  type BrowserWorkerProgressStage,
  type BrowserWorkerResultPreview,
  type BrowserWorkerSqlDelivery,
  type BrowserWorkerSqlCommand,
  type FallbackReason,
  type ParquetInspectionSummary,
  type QueryError,
  type QueryMetricsSummary,
  type QueryResultPageRequest,
  type QueryResponse,
  type QueryRuntimeLimits,
  type WireBrowserWorkerResponseEnvelope,
  redactUrlSecrets,
} from './axon-browser-sdk';
import { QUERY_RESULT_PAGE_SIZE } from './services/query-pagination.ts';

type DecimalString = string;

type SandboxCacheMetrics = {
  session_cached_bytes: DecimalString;
  session_table_count: DecimalString;
  max_session_cached_bytes: DecimalString;
};

type SandboxOpenTableOutput = {
  cache_metrics: SandboxCacheMetrics;
};

type SandboxSqlMetadata = {
  response: QueryResponse;
  preview: SandboxWireResultPreview;
  arrow_ipc_byte_length: DecimalString;
  row_count: DecimalString;
  preview_string_bytes: DecimalString;
  preview_duration_ms: DecimalString;
  cache_metrics: SandboxCacheMetrics;
};

type SandboxWireResultPreview = Omit<BrowserWorkerResultPreview, 'row_count'> & {
  row_count: DecimalString;
};

type SandboxSqlBridgeValue = {
  metadata_json: string;
  arrow_ipc_bytes: Uint8Array;
};

type SandboxWorkerScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<BrowserWorkerCommand>) => void,
  ): void;
  postMessage(
    message: WireBrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope,
    transfer?: Transferable[],
  ): void;
};

const QUERY_PREVIEW_LIMIT = QUERY_RESULT_PAGE_SIZE + 1;
const DEFAULT_ARROW_IPC_CHUNK_BYTES = 1024 * 1024;
const workerScope = self as unknown as SandboxWorkerScope;

let sessionPromise: Promise<SandboxQuerySession> | undefined;
let cancellationToken: SandboxQueryCancellation | undefined;
let activeQueryId: string | undefined;
let commandQueue = Promise.resolve();

workerScope.addEventListener('message', (event: MessageEvent<BrowserWorkerCommand>) => {
  if ('cancel' in event.data) {
    void handleCancel(event.data.cancel);
    return;
  }

  commandQueue = commandQueue.catch(() => undefined).then(() => handleCommand(event.data));
});

async function handleCommand(command: BrowserWorkerCommand): Promise<void> {
  const context = commandContext(command);
  try {
    if ('open_delta_table' in command) {
      await handleOpenDeltaTable(command.open_delta_table, context);
      return;
    }
    if ('open_parquet_dataset' in command) {
      await handleOpenParquetDataset(command.open_parquet_dataset, context);
      return;
    }
    if ('inspect_parquet' in command) {
      await handleInspectParquet(command.inspect_parquet, context);
      return;
    }
    if ('sql' in command) {
      await handleSql(command.sql, context);
      return;
    }
    if ('dispose' in command) {
      const session = await ensureSession(context);
      await session.dispose_table(command.dispose.name);
      postResponse({
        disposed: {
          request_id: command.dispose.request_id,
          name: command.dispose.name,
        },
      });
      return;
    }

    throw queryError('invalid_request', 'unknown sandbox worker command');
  } catch (error) {
    const normalizedError = normalizeQueryError(error);
    emitErrorEvents(context, normalizedError);
    postResponse({
      error: {
        request_id: requestId(command),
        error: normalizedError,
      },
    });
  }
}

async function handleOpenDeltaTable(
  command: BrowserWorkerOpenDeltaTableCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox worker opening Delta table descriptor');
  const session = await ensureSession(context);
  const output = JSON.parse(
    await session.open_delta_table(command.name, JSON.stringify(command.snapshot)),
  ) as SandboxOpenTableOutput;

  emitCacheMetrics(context, output.cache_metrics);
  emitProgress(context, 'finished');
  postResponse({
    opened: {
      request_id: command.request_id,
      name: command.name,
    },
  });
}

async function handleOpenParquetDataset(
  command: BrowserWorkerOpenParquetDatasetCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox worker opening Parquet dataset descriptor');
  const session = await ensureSession(context);
  const output = JSON.parse(
    await session.open_parquet_dataset(command.name, JSON.stringify(command.dataset)),
  ) as SandboxOpenTableOutput;

  emitCacheMetrics(context, output.cache_metrics);
  emitProgress(context, 'finished');
  postResponse({
    opened: {
      request_id: command.request_id,
      name: command.name,
    },
  });
}

async function handleInspectParquet(
  command: BrowserWorkerInspectParquetCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox worker inspecting Parquet metadata');
  emitProgress(context, 'executing');
  const session = await ensureSession(context);
  const summary = JSON.parse(
    await session.inspect_parquet(command.name, command.path),
  ) as ParquetInspectionSummary;

  emitProgress(context, 'finished');
  postResponse({
    parquet_inspection: {
      request_id: command.request_id,
      summary,
    },
  });
}

async function handleSql(
  command: BrowserWorkerSqlCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  if (command.output !== undefined && command.output !== 'arrow_ipc_stream') {
    throw queryError('invalid_request', 'sandbox worker only supports Arrow IPC stream output');
  }
  const delivery = command.delivery ?? 'single_buffer';
  if (delivery !== 'single_buffer' && delivery !== 'chunked_buffers') {
    throw queryError('invalid_request', `unsupported Arrow IPC delivery '${String(delivery)}'`);
  }
  const query = queryWithBrowserSafeLimits(command);

  activeQueryId = command.request_id;
  try {
    emitProgress(context, 'started');
    emitLog(context, 'info', 'sandbox worker executing SQL query');
    emitProgress(context, 'planning');
    const session = await ensureSession(context);
    emitProgress(context, 'executing');

    const bridgeValue = (await session.sql(
      command.name,
      JSON.stringify(query),
      QUERY_PREVIEW_LIMIT,
    )) as SandboxSqlBridgeValue;
    const metadata = JSON.parse(bridgeValue.metadata_json) as SandboxSqlMetadata;
    const preview = normalizePreview(metadata.preview);
    const arrowIpcByteLength = decimalNumber(metadata.arrow_ipc_byte_length);
    const previewStringBytes = decimalNumber(metadata.preview_string_bytes);
    const previewDurationMs = decimalNumber(metadata.preview_duration_ms);
    const arrowIpcChunkCount = arrowIpcChunkCountForDelivery(
      delivery,
      bridgeValue.arrow_ipc_bytes.byteLength,
    );
    const response = {
      ...metadata.response,
      metrics: {
        ...metadata.response.metrics,
        arrow_ipc_bytes: arrowIpcByteLength,
        arrow_ipc_chunk_count: arrowIpcChunkCount,
        preview_rows: preview.rows.length,
        preview_string_bytes: previewStringBytes,
        preview_duration_ms: previewDurationMs,
      },
    };

    emitRangeReadMetrics(context, response.metrics);
    if (response.fallback_reason) {
      emitFallback(context, response.fallback_reason);
    }
    emitCacheMetrics(context, metadata.cache_metrics);
    emitProgress(context, 'arrow_ipc_ready');
    if (delivery === 'chunked_buffers') {
      postArrowIpcChunks(context, command.request_id, bridgeValue.arrow_ipc_bytes);
    }
    emitProgress(context, 'finished');

    if (delivery === 'chunked_buffers') {
      postResponse({
        success: {
          request_id: command.request_id,
          response,
          result: {
            format: 'stream',
            content_type: ARROW_IPC_STREAM_CONTENT_TYPE,
            delivery: 'chunked_buffers',
            byte_length: arrowIpcByteLength,
            chunk_count: arrowIpcChunkCount,
          },
          preview,
        },
      });
    } else {
      postResponse(
        {
          success: {
            request_id: command.request_id,
            response,
            result: {
              format: 'stream',
              content_type: ARROW_IPC_STREAM_CONTENT_TYPE,
              delivery: 'single_buffer',
              bytes: bridgeValue.arrow_ipc_bytes,
              byte_length: arrowIpcByteLength,
              chunk_count: arrowIpcChunkCount,
            },
            preview,
          },
        },
        [bridgeValue.arrow_ipc_bytes.buffer],
      );
    }
  } finally {
    if (activeQueryId === command.request_id) {
      activeQueryId = undefined;
    }
  }
}

async function handleCancel(command: BrowserWorkerCancelCommand): Promise<void> {
  const queryId = command.query_id ?? command.request_id;
  const context: BrowserWorkerEventContext = {
    phase: 'query',
    request_id: command.request_id,
    query_id: queryId,
  };

  try {
    if (activeQueryId !== queryId) {
      emitLog(context, 'debug', 'sandbox worker ignored cancellation for inactive query');
      return;
    }
    const token = await ensureCancellationToken(context);
    token.cancel();
    emitLog(context, 'info', 'sandbox worker cancelled running browser DataFusion queries');
  } catch (error) {
    emitErrorEvents(context, normalizeQueryError(error));
  }
}

function ensureSession(commandContext: BrowserWorkerEventContext): Promise<SandboxQuerySession> {
  if (sessionPromise) {
    return sessionPromise;
  }

  const context: BrowserWorkerEventContext = {
    ...commandContext,
    phase: 'instantiate',
  };
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox worker instantiating query bridge');
  const nextSession = init().then(() => {
    const session = new SandboxQuerySession();
    cancellationToken = session.cancellation();
    emitProgress(context, 'finished');
    return session;
  });
  sessionPromise = nextSession;
  return nextSession;
}

async function ensureCancellationToken(
  commandContext: BrowserWorkerEventContext,
): Promise<SandboxQueryCancellation> {
  await ensureSession(commandContext);
  if (!cancellationToken) {
    throw queryError('execution_failed', 'sandbox worker cancellation handle was not initialized');
  }
  return cancellationToken;
}

function normalizePreview(preview: SandboxWireResultPreview): BrowserWorkerResultPreview {
  return {
    ...preview,
    row_count: decimalNumber(preview.row_count),
  };
}

function queryWithBrowserSafeLimits(
  command: BrowserWorkerSqlCommand,
): BrowserWorkerSqlCommand['query'] {
  const options = {
    include_explain: false,
    collect_metrics: true,
    ...(command.query.options ?? {}),
  };

  if (command.browser_safe_defaults === true) {
    return {
      ...command.query,
      options: {
        ...options,
        result_page: resultPageWithBrowserSafeDefaults(options.result_page),
        runtime_limits: runtimeLimitsWithBrowserSafeDefaults(options.runtime_limits),
      },
    };
  }

  if (!options.result_page || !options.runtime_limits) {
    throw queryError(
      'invalid_request',
      'raw sandbox SQL commands must provide result_page plus runtime_limits or set browser_safe_defaults',
    );
  }
  requirePositiveInteger(options.result_page.limit, 'result_page.limit');
  requireNonNegativeInteger(options.result_page.offset, 'result_page.offset');
  requirePositiveInteger(options.runtime_limits.max_result_rows, 'runtime_limits.max_result_rows');
  requirePositiveInteger(
    options.runtime_limits.max_arrow_ipc_bytes,
    'runtime_limits.max_arrow_ipc_bytes',
  );
  requirePositiveInteger(
    options.runtime_limits.max_preview_string_bytes,
    'runtime_limits.max_preview_string_bytes',
  );

  return {
    ...command.query,
    options,
  };
}

function resultPageWithBrowserSafeDefaults(
  page: QueryResultPageRequest | undefined,
): QueryResultPageRequest {
  if (!page) {
    return {
      limit: BROWSER_SAFE_RESULT_ROW_LIMIT,
      offset: 0,
    };
  }
  return {
    limit: Math.min(
      requirePositiveInteger(page.limit, 'result_page.limit'),
      BROWSER_SAFE_RESULT_ROW_LIMIT,
    ),
    offset: requireNonNegativeInteger(page.offset, 'result_page.offset'),
  };
}

function runtimeLimitsWithBrowserSafeDefaults(
  limits: QueryRuntimeLimits | undefined,
): QueryRuntimeLimits {
  return {
    ...(limits ?? {}),
    max_result_rows: Math.min(
      optionalPositiveInteger(
        limits?.max_result_rows,
        BROWSER_SAFE_RESULT_ROW_LIMIT,
        'runtime_limits.max_result_rows',
      ),
      BROWSER_SAFE_RESULT_ROW_LIMIT,
    ),
    max_arrow_ipc_bytes: Math.min(
      optionalPositiveInteger(
        limits?.max_arrow_ipc_bytes,
        BROWSER_SAFE_ARROW_IPC_BYTES,
        'runtime_limits.max_arrow_ipc_bytes',
      ),
      BROWSER_SAFE_ARROW_IPC_BYTES,
    ),
    max_preview_string_bytes: Math.min(
      optionalPositiveInteger(
        limits?.max_preview_string_bytes,
        BROWSER_SAFE_PREVIEW_STRING_BYTES,
        'runtime_limits.max_preview_string_bytes',
      ),
      BROWSER_SAFE_PREVIEW_STRING_BYTES,
    ),
  };
}

function arrowIpcChunkCountForDelivery(
  delivery: BrowserWorkerSqlDelivery,
  byteLength: number,
): number {
  if (delivery === 'single_buffer') {
    return byteLength === 0 ? 0 : 1;
  }
  return byteLength === 0 ? 0 : Math.ceil(byteLength / DEFAULT_ARROW_IPC_CHUNK_BYTES);
}

function postArrowIpcChunks(
  context: BrowserWorkerEventContext,
  requestId: string,
  bytes: Uint8Array,
): void {
  let sequence = 0;
  for (let byteOffset = 0; byteOffset < bytes.byteLength; ) {
    const chunkLength = Math.min(DEFAULT_ARROW_IPC_CHUNK_BYTES, bytes.byteLength - byteOffset);
    const chunk = exactSizedChunk(bytes, byteOffset, chunkLength);
    workerScope.postMessage(
      {
        arrow_ipc_chunk: {
          context,
          request_id: requestId,
          sequence,
          byte_offset: byteOffset,
          byte_length: chunk.byteLength,
          bytes: chunk,
        },
      },
      [chunk.buffer],
    );
    sequence += 1;
    byteOffset += chunkLength;
  }
}

function exactSizedChunk(source: Uint8Array, byteOffset: number, byteLength: number): Uint8Array {
  const chunk = source.slice(byteOffset, byteOffset + byteLength);
  if (chunk.byteOffset !== 0 || chunk.byteLength !== chunk.buffer.byteLength) {
    throw queryError('execution_failed', 'Arrow IPC chunk buffer was not exact-sized');
  }
  return chunk;
}

function emitProgress(context: BrowserWorkerEventContext, stage: BrowserWorkerProgressStage): void {
  postEvent({
    progress: {
      context,
      stage,
    },
  });
}

function emitLog(
  context: BrowserWorkerEventContext,
  level: BrowserWorkerLogLevel,
  message: string,
): void {
  postEvent({
    log: {
      context,
      level,
      message,
    },
  });
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
  postEvent({
    fallback: {
      context,
      reason,
    },
  });
}

function emitErrorEvents(context: BrowserWorkerEventContext, error: QueryError): void {
  if (error.fallback_reason) {
    emitFallback(context, error.fallback_reason);
  }
  if (isBrowserDataFusionCancellation(error)) {
    postEvent({
      cancellation: {
        context,
        error,
      },
    });
  }
  emitLog(context, 'error', error.message);
  postEvent({
    terminal_error: {
      context,
      error,
    },
  });
}

function isBrowserDataFusionCancellation(error: QueryError): boolean {
  return (
    error.code === 'execution_failed' &&
    error.message.startsWith('experimental browser DataFusion query cancelled')
  );
}

function postEvent(event: BrowserWorkerEventEnvelope): void {
  workerScope.postMessage(event);
}

function postResponse(
  response: WireBrowserWorkerResponseEnvelope,
  transfer?: Transferable[],
): void {
  workerScope.postMessage(response, transfer ?? []);
}

function commandContext(command: BrowserWorkerCommand): BrowserWorkerEventContext {
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

function requestId(command: BrowserWorkerCommand): string {
  if ('open_delta_table' in command) {
    return command.open_delta_table.request_id;
  }
  if ('open_parquet_dataset' in command) {
    return command.open_parquet_dataset.request_id;
  }
  if ('inspect_parquet' in command) {
    return command.inspect_parquet.request_id;
  }
  if ('sql' in command) {
    return command.sql.request_id;
  }
  if ('cancel' in command) {
    return command.cancel.request_id;
  }
  if ('open_table' in command) {
    return command.open_table.request_id;
  }
  return command.dispose.request_id;
}

function normalizeQueryError(error: unknown): QueryError {
  if (isQueryError(error)) {
    return redactQueryError(error);
  }
  if (typeof error === 'string') {
    const parsed = parseQueryErrorJson(error);
    if (parsed) {
      return redactQueryError(parsed);
    }
    return queryError('execution_failed', error);
  }
  if (error instanceof Error) {
    const parsed = parseQueryErrorJson(error.message);
    if (parsed) {
      return redactQueryError(parsed);
    }
    return queryError('execution_failed', error.message);
  }

  return queryError('execution_failed', String(error));
}

function parseQueryErrorJson(value: string): QueryError | undefined {
  try {
    const parsed = JSON.parse(value) as unknown;
    return isQueryError(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

function isQueryError(value: unknown): value is QueryError {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const candidate = value as Partial<QueryError>;
  return (
    typeof candidate.code === 'string' &&
    typeof candidate.message === 'string' &&
    candidate.target === 'browser_wasm'
  );
}

function redactQueryError(error: QueryError): QueryError {
  return {
    ...error,
    message: redactUrlSecrets(error.message),
  };
}

function queryError(code: QueryError['code'], message: string): QueryError {
  return {
    code,
    message: redactUrlSecrets(message),
    target: 'browser_wasm',
  };
}

function decimalNumber(value: DecimalString | number): number {
  return typeof value === 'number' ? value : Number.parseInt(value, 10);
}

function requirePositiveInteger(value: unknown, path: string): number {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) {
    return value;
  }
  throw queryError('invalid_request', `${path} must be a positive integer`);
}

function requireNonNegativeInteger(value: unknown, path: string): number {
  if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
    return value;
  }
  throw queryError('invalid_request', `${path} must be a non-negative integer`);
}

function optionalPositiveInteger(value: unknown, defaultValue: number, path: string): number {
  return value === undefined ? defaultValue : requirePositiveInteger(value, path);
}
