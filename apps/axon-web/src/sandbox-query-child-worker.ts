import init, { SandboxQuerySession, SandboxSqlStream } from './wasm/axon_web_wasm';
import {
  BROWSER_SAFE_ARROW_IPC_BYTES,
  BROWSER_SAFE_PREVIEW_STRING_BYTES,
  BROWSER_SAFE_RESULT_ROW_LIMIT,
  type BrowserWorkerCommand,
  type BrowserWorkerEventContext,
  type BrowserWorkerEventEnvelope,
  type BrowserWorkerInspectParquetCommand,
  type BrowserWorkerLogLevel,
  type BrowserWorkerOpenDeltaTableCommand,
  type BrowserWorkerOpenParquetDatasetCommand,
  type BrowserWorkerProgressStage,
  type BrowserWorkerSqlCommand,
  type ParquetInspectionSummary,
  type QueryError,
  type QueryResultPageRequest,
  type QueryRuntimeLimits,
  type WireBrowserWorkerResponseEnvelope,
  redactUrlSecrets,
} from './axon-browser-sdk';
import { QUERY_RESULT_PAGE_SIZE } from './services/query-pagination.ts';
import {
  PRIVATE_STREAM_PROTOCOL_VERSION,
  QueryCreditGate,
  requireProtocolVersion,
  requireTerminalMetadata,
  type CreditReservation,
  type PrivateChildMessage,
  type PrivateCoordinatorMessage,
  type PrivateCreditClass,
  type PrivateStreamChunk,
  type PrivateStreamPhase,
} from './sandbox-query-stream-protocol';

type DecimalString = string;

type SandboxCacheMetrics = {
  session_cached_bytes: DecimalString;
  session_table_count: DecimalString;
  max_session_cached_bytes: DecimalString;
};

type SandboxOpenTableOutput = {
  cache_metrics: SandboxCacheMetrics;
};

type WasmStreamChunk = Omit<PrivateStreamChunk, 'version' | 'query_id'>;
type WasmStreamItem = { chunk: WasmStreamChunk } | { terminal: { metadata_json: string } } | null;

type PrivateChildScope = {
  addEventListener(
    type: 'message',
    listener: (event: MessageEvent<PrivateCoordinatorMessage>) => void,
  ): void;
  postMessage(message: PrivateChildMessage, transfer?: Transferable[]): void;
};

type ActiveQuery = {
  queryId: string;
  gate: QueryCreditGate;
  stream?: SandboxSqlStream;
};

const QUERY_PREVIEW_LIMIT = QUERY_RESULT_PAGE_SIZE + 1;
const childScope = self as unknown as PrivateChildScope;

let sessionPromise: Promise<SandboxQuerySession> | undefined;
let activeQuery: ActiveQuery | undefined;
let commandQueue = Promise.resolve();
const queuedQueryIds = new Set<string>();
const pendingCancellations = new Map<string, 'cancelled' | 'deadline_exceeded'>();

childScope.addEventListener('message', (event) => {
  const message = event.data;
  try {
    requireProtocolVersion(message.version);
    if (message.kind === 'credit') {
      if (activeQuery?.queryId === message.query_id) {
        activeQuery.gate.addCredit(message.credit_class, message.sequence, message.bytes);
      }
      return;
    }
    if (message.kind === 'cancel') {
      if (activeQuery?.queryId === message.query_id) {
        cancelActiveQuery(activeQuery, message.reason);
      } else if (queuedQueryIds.has(message.query_id)) {
        if (!pendingCancellations.has(message.query_id)) {
          pendingCancellations.set(message.query_id, message.reason);
        }
      }
      return;
    }
    if ('sql' in message.command) queuedQueryIds.add(message.command.sql.request_id);
    commandQueue = commandQueue.catch(() => undefined).then(() => handleCommand(message.command));
  } catch (error) {
    postStreamFault(activeQuery?.queryId ?? 'unknown', normalizeQueryError(error));
  }
});

postPrivate({ kind: 'ready', version: PRIVATE_STREAM_PROTOCOL_VERSION });

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
      try {
        await handleSql(command.sql, context);
      } finally {
        queuedQueryIds.delete(command.sql.request_id);
        pendingCancellations.delete(command.sql.request_id);
      }
      return;
    }
    if ('dispose' in command) {
      const session = await ensureSession(context);
      session.dispose_table(command.dispose.name);
      postPublic({
        disposed: {
          request_id: command.dispose.request_id,
          name: command.dispose.name,
        },
      });
      return;
    }
    throw queryError('invalid_request', 'unknown private child worker command');
  } catch (error) {
    const normalized = normalizeQueryError(error);
    emitErrorEvents(context, normalized);
    postPublic({ error: { request_id: requestId(command), error: normalized } });
  }
}

async function handleOpenDeltaTable(
  command: BrowserWorkerOpenDeltaTableCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox child opening Delta table descriptor');
  const session = await ensureSession(context);
  const output = JSON.parse(
    await session.open_delta_table(command.name, JSON.stringify(command.snapshot)),
  ) as SandboxOpenTableOutput;
  emitCacheMetrics(context, output.cache_metrics);
  emitProgress(context, 'finished');
  postPublic({ opened: { request_id: command.request_id, name: command.name } });
}

async function handleOpenParquetDataset(
  command: BrowserWorkerOpenParquetDatasetCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox child opening Parquet dataset descriptor');
  const session = await ensureSession(context);
  const output = JSON.parse(
    await session.open_parquet_dataset(command.name, JSON.stringify(command.dataset)),
  ) as SandboxOpenTableOutput;
  emitCacheMetrics(context, output.cache_metrics);
  emitProgress(context, 'finished');
  postPublic({ opened: { request_id: command.request_id, name: command.name } });
}

async function handleInspectParquet(
  command: BrowserWorkerInspectParquetCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  emitProgress(context, 'started');
  emitLog(context, 'info', 'sandbox child inspecting Parquet metadata');
  emitProgress(context, 'executing');
  const session = await ensureSession(context);
  const summary = JSON.parse(
    await session.inspect_parquet(command.name, command.path),
  ) as ParquetInspectionSummary;
  emitProgress(context, 'finished');
  postPublic({ parquet_inspection: { request_id: command.request_id, summary } });
}

async function handleSql(
  command: BrowserWorkerSqlCommand,
  context: BrowserWorkerEventContext,
): Promise<void> {
  if (command.output !== undefined && command.output !== 'arrow_ipc_stream') {
    postStreamStartFailed(
      command.request_id,
      queryError('invalid_request', 'sandbox child only supports Arrow IPC stream output'),
    );
    return;
  }
  const delivery = command.delivery ?? 'single_buffer';
  if (delivery !== 'single_buffer' && delivery !== 'chunked_buffers') {
    postStreamStartFailed(
      command.request_id,
      queryError('invalid_request', `unsupported Arrow IPC delivery '${String(delivery)}'`),
    );
    return;
  }
  let query: BrowserWorkerSqlCommand['query'];
  try {
    query = queryWithBrowserSafeLimits(command);
  } catch (error) {
    postStreamStartFailed(command.request_id, normalizeQueryError(error));
    return;
  }
  const active: ActiveQuery = {
    queryId: command.request_id,
    gate: new QueryCreditGate(),
  };
  activeQuery = active;
  const pendingCancellation = pendingCancellations.get(command.request_id);
  if (pendingCancellation) cancelActiveQuery(active, pendingCancellation);

  try {
    emitProgress(context, 'started');
    emitLog(context, 'info', 'sandbox child executing SQL query');
    emitProgress(context, 'planning');
    const session = await ensureSession(context);
    let stream: SandboxSqlStream;
    try {
      stream = await session.start_sql_stream(
        command.name,
        JSON.stringify(query),
        QUERY_PREVIEW_LIMIT,
      );
    } catch (error) {
      postStreamStartFailed(command.request_id, normalizeQueryError(error));
      return;
    }
    active.stream = stream;
    if (active.gate.cancelReason === 'deadline_exceeded') stream.cancel_for_deadline();
    if (active.gate.cancelReason === 'cancelled') stream.cancel();
    emitProgress(context, 'executing');
    await pumpStream(active, stream);
  } catch (error) {
    postStreamFault(command.request_id, normalizeQueryError(error));
  } finally {
    if (active.stream) await active.stream.close();
    if (activeQuery === active) activeQuery = undefined;
  }
}

async function pumpStream(active: ActiveQuery, stream: SandboxSqlStream): Promise<void> {
  let creditCandidates: readonly PrivateCreditClass[] = ['control'];
  for (;;) {
    const reservations = await active.gate.reserveAll(creditCandidates);
    if (active.gate.cancelReason === 'deadline_exceeded') stream.cancel_for_deadline();
    if (active.gate.cancelReason === 'cancelled') stream.cancel();
    const item = (await stream.next()) as WasmStreamItem;
    if (item === null) {
      throw queryError('execution_failed', 'Arrow IPC cursor closed without a terminal outcome');
    }
    if ('terminal' in item) {
      refundReservations(active.gate, reservations);
      const metadata = JSON.parse(item.terminal.metadata_json) as unknown;
      requireTerminalMetadata(metadata);
      postPrivate({
        kind: 'stream_terminal',
        version: PRIVATE_STREAM_PROTOCOL_VERSION,
        query_id: active.queryId,
        metadata,
      });
      return;
    }

    const chunk = requireWasmChunk(item.chunk, active.queryId);
    const actualClass: PrivateCreditClass = chunk.phase === 'data' ? 'data' : 'control';
    const actualReservation = reservations?.find(
      (reservation) => reservation.creditClass === actualClass,
    );
    if (!actualReservation) {
      throw queryError(
        'execution_failed',
        `private child pulled ${actualClass} bytes without reserving its full credit window`,
      );
    }
    refundReservations(
      active.gate,
      reservations,
      actualReservation,
      chunk.sequence,
      chunk.bytes.byteLength,
    );
    creditCandidates = nextCreditCandidates(chunk);
    postPrivate({ kind: 'stream_chunk', ...chunk }, [chunk.bytes.buffer]);
  }
}

function nextCreditCandidates(chunk: PrivateStreamChunk): readonly PrivateCreditClass[] {
  if (chunk.phase === 'schema') {
    return chunk.end_of_logical_batch ? ['data', 'control'] : ['control'];
  }
  if (chunk.phase === 'data') {
    return chunk.end_of_logical_batch ? ['data', 'control'] : ['data'];
  }
  return chunk.end_of_logical_batch ? [] : ['control'];
}

function refundReservations(
  gate: QueryCreditGate,
  reservations: readonly CreditReservation[] | undefined,
  consumed?: CreditReservation,
  sequence?: bigint,
  actualBytes = 0,
): void {
  if (!reservations) return;
  for (const reservation of reservations) {
    if (reservation === consumed) {
      if (sequence === undefined) {
        throw queryError('execution_failed', 'private child transfer omitted its chunk sequence');
      }
      gate.commitTransfer(reservation, sequence, actualBytes);
    } else {
      gate.refundUnused(reservation, 0);
    }
  }
}

function cancelActiveQuery(active: ActiveQuery, reason: 'cancelled' | 'deadline_exceeded'): void {
  active.gate.cancel(reason);
  if (reason === 'deadline_exceeded') active.stream?.cancel_for_deadline();
  else active.stream?.cancel();
}

function requireWasmChunk(chunk: WasmStreamChunk, queryId: string): PrivateStreamChunk {
  if (!(chunk.bytes instanceof Uint8Array)) {
    throw queryError('execution_failed', 'Wasm Arrow IPC chunk bytes were not a Uint8Array');
  }
  if (chunk.bytes.byteOffset !== 0 || chunk.bytes.byteLength !== chunk.bytes.buffer.byteLength) {
    throw queryError('execution_failed', 'Wasm Arrow IPC chunk buffer was not exact-sized');
  }
  if (
    typeof chunk.sequence !== 'bigint' ||
    typeof chunk.fragment_index !== 'bigint' ||
    typeof chunk.rows_completed !== 'bigint' ||
    typeof chunk.byte_length !== 'bigint' ||
    (chunk.logical_batch_sequence !== null && typeof chunk.logical_batch_sequence !== 'bigint')
  ) {
    throw queryError('execution_failed', 'Wasm Arrow IPC chunk counters were not bigint values');
  }
  if (chunk.byte_length !== BigInt(chunk.bytes.byteLength)) {
    throw queryError('execution_failed', 'Wasm Arrow IPC chunk byte count was inconsistent');
  }
  if (chunk.phase !== 'schema' && chunk.phase !== 'data' && chunk.phase !== 'end_of_stream') {
    throw queryError('execution_failed', 'Wasm Arrow IPC chunk phase was invalid');
  }
  return {
    ...chunk,
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: queryId,
    phase: chunk.phase as PrivateStreamPhase,
  };
}

function ensureSession(context: BrowserWorkerEventContext): Promise<SandboxQuerySession> {
  if (sessionPromise) return sessionPromise;
  const instantiateContext = { ...context, phase: 'instantiate' as const };
  emitProgress(instantiateContext, 'started');
  emitLog(instantiateContext, 'info', 'sandbox child instantiating query bridge');
  sessionPromise = init().then(() => {
    const session = new SandboxQuerySession();
    emitProgress(instantiateContext, 'finished');
    return session;
  });
  return sessionPromise;
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
  return { ...command.query, options };
}

function resultPageWithBrowserSafeDefaults(
  page: QueryResultPageRequest | undefined,
): QueryResultPageRequest {
  if (!page) return { limit: BROWSER_SAFE_RESULT_ROW_LIMIT, offset: 0 };
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

function emitProgress(context: BrowserWorkerEventContext, stage: BrowserWorkerProgressStage): void {
  postPublic({ progress: { context, stage } });
}

function emitLog(
  context: BrowserWorkerEventContext,
  level: BrowserWorkerLogLevel,
  message: string,
): void {
  postPublic({ log: { context, level, message } });
}

function emitCacheMetrics(context: BrowserWorkerEventContext, metrics: SandboxCacheMetrics): void {
  postPublic({
    cache_metrics: {
      context,
      session_cached_bytes: decimalNumber(metrics.session_cached_bytes),
      session_table_count: decimalNumber(metrics.session_table_count),
      max_session_cached_bytes: decimalNumber(metrics.max_session_cached_bytes),
    },
  });
}

function emitErrorEvents(context: BrowserWorkerEventContext, error: QueryError): void {
  if (error.fallback_reason) postPublic({ fallback: { context, reason: error.fallback_reason } });
  emitLog(context, 'error', error.message);
  postPublic({ terminal_error: { context, error } });
}

function postPublic(
  envelope: WireBrowserWorkerResponseEnvelope | BrowserWorkerEventEnvelope,
): void {
  postPrivate({
    kind: 'public',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    envelope,
  });
}

function postStreamStartFailed(queryId: string, error: QueryError): void {
  postPrivate({
    kind: 'stream_start_failed',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: queryId,
    error,
  });
}

function postStreamFault(queryId: string, error: QueryError): void {
  postPrivate({
    kind: 'stream_fault',
    version: PRIVATE_STREAM_PROTOCOL_VERSION,
    query_id: queryId,
    error,
  });
}

function postPrivate(message: PrivateChildMessage, transfer: Transferable[] = []): void {
  childScope.postMessage(message, transfer);
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
  if ('open_delta_table' in command) return command.open_delta_table.request_id;
  if ('open_parquet_dataset' in command) return command.open_parquet_dataset.request_id;
  if ('inspect_parquet' in command) return command.inspect_parquet.request_id;
  if ('sql' in command) return command.sql.request_id;
  if ('cancel' in command) return command.cancel.request_id;
  if ('open_table' in command) return command.open_table.request_id;
  return command.dispose.request_id;
}

function normalizeQueryError(error: unknown): QueryError {
  if (isQueryError(error)) return redactQueryError(error);
  if (typeof error === 'string') {
    const parsed = parseQueryErrorJson(error);
    return parsed ? redactQueryError(parsed) : queryError('execution_failed', error);
  }
  if (error instanceof Error) {
    const parsed = parseQueryErrorJson(error.message);
    return parsed ? redactQueryError(parsed) : queryError('execution_failed', error.message);
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

function queryError(code: QueryError['code'], message: string): QueryError {
  return { code, message: redactUrlSecrets(message), target: 'browser_wasm' };
}

function decimalNumber(value: DecimalString | number): number {
  return typeof value === 'number' ? value : Number.parseInt(value, 10);
}

function requirePositiveInteger(value: unknown, path: string): number {
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) return value;
  throw queryError('invalid_request', `${path} must be a positive integer`);
}

function requireNonNegativeInteger(value: unknown, path: string): number {
  if (typeof value === 'number' && Number.isInteger(value) && value >= 0) return value;
  throw queryError('invalid_request', `${path} must be a non-negative integer`);
}

function optionalPositiveInteger(value: unknown, defaultValue: number, path: string): number {
  return value === undefined ? defaultValue : requirePositiveInteger(value, path);
}
