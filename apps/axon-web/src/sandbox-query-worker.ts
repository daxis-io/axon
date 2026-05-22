import init, { SandboxQueryCancellation, SandboxQuerySession } from './wasm/axon_web_wasm';
import {
  ARROW_IPC_STREAM_CONTENT_TYPE,
  type BrowserWorkerCancelCommand,
  type BrowserWorkerCommand,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerInspectParquetCommand,
  type BrowserWorkerLogLevel,
  type BrowserWorkerOpenDeltaTableCommand,
  type BrowserWorkerProgressStage,
  type BrowserWorkerResultPreview,
  type BrowserWorkerResponseEnvelope,
  type BrowserWorkerSqlCommand,
  type FallbackReason,
  type ParquetInspectionSummary,
  type QueryError,
  type QueryMetricsSummary,
  type QueryResponse,
  redactUrlSecrets,
} from './axon-browser-sdk';

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
    message: BrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope,
    transfer?: Transferable[],
  ): void;
};

const QUERY_PREVIEW_LIMIT = 100;
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

  activeQueryId = command.request_id;
  try {
    emitProgress(context, 'started');
    emitLog(context, 'info', 'sandbox worker executing SQL query');
    emitProgress(context, 'planning');
    const session = await ensureSession(context);
    emitProgress(context, 'executing');

    const bridgeValue = (await session.sql(
      command.name,
      JSON.stringify(command.query),
      QUERY_PREVIEW_LIMIT,
    )) as SandboxSqlBridgeValue;
    const metadata = JSON.parse(bridgeValue.metadata_json) as SandboxSqlMetadata;
    const preview = normalizePreview(metadata.preview);

    emitRangeReadMetrics(context, metadata.response.metrics);
    if (metadata.response.fallback_reason) {
      emitFallback(context, metadata.response.fallback_reason);
    }
    emitCacheMetrics(context, metadata.cache_metrics);
    emitProgress(context, 'arrow_ipc_ready');
    emitProgress(context, 'finished');

    postResponse(
      {
        success: {
          request_id: command.request_id,
          response: metadata.response,
          result: {
            format: 'stream',
            content_type: ARROW_IPC_STREAM_CONTENT_TYPE,
            bytes: bridgeValue.arrow_ipc_bytes,
          },
          preview,
        },
      },
      [bridgeValue.arrow_ipc_bytes.buffer],
    );
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
  if (!sessionPromise) {
    const context: BrowserWorkerEventContext = {
      ...commandContext,
      phase: 'instantiate',
    };
    emitProgress(context, 'started');
    emitLog(context, 'info', 'sandbox worker instantiating query bridge');
    sessionPromise = init().then(() => {
      const session = new SandboxQuerySession();
      cancellationToken = session.cancellation();
      emitProgress(context, 'finished');
      return session;
    });
  }

  return sessionPromise;
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
      row_groups_touched: metrics.row_groups_touched ?? 0,
      row_groups_skipped: metrics.row_groups_skipped ?? 0,
      footer_reads: metrics.footer_reads,
      rows_emitted: metrics.rows_emitted ?? 0,
      snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms,
      access_mode: metrics.access_mode,
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

function postResponse(response: BrowserWorkerResponseEnvelope, transfer?: Transferable[]): void {
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
