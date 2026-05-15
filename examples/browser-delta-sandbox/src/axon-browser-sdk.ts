export const PREFERRED_TARGET: ExecutionTarget = 'browser_wasm';
export const ARROW_IPC_STREAM_CONTENT_TYPE = 'application/vnd.apache.arrow.stream';
export const ARROW_IPC_FILE_CONTENT_TYPE = 'application/vnd.apache.arrow.file';

export type ExecutionTarget = 'browser_wasm' | 'native';

export type CapabilityKey =
  | 'change_data_feed'
  | 'column_mapping'
  | 'deletion_vectors'
  | 'multi_partition_execution'
  | 'proxy_access'
  | 'range_reads'
  | 'signed_url_access'
  | 'time_travel'
  | 'timestamp_ntz'
  | 'unknown_protocol_features';

export type CapabilityState = 'supported' | 'native_only' | 'unsupported' | 'experimental';

export type CapabilityReport = {
  capabilities: Partial<Record<CapabilityKey, CapabilityState>>;
};

export type FallbackReason =
  | 'access_denied'
  | 'browser_runtime_constraint'
  | 'native_required'
  | 'network_failure'
  | 'range_read_unavailable'
  | 'security_policy'
  | 'signed_url_expired'
  | {
      capability_gate: {
        capability: CapabilityKey;
        required_state: CapabilityState;
      };
    };

export type QueryErrorCode =
  | 'access_denied'
  | 'execution_failed'
  | 'fallback_required'
  | 'invalid_request'
  | 'object_store_protocol'
  | 'security_policy_violation'
  | 'unsupported_feature';

export type QueryError = {
  code: QueryErrorCode;
  message: string;
  target: ExecutionTarget;
  fallback_reason?: FallbackReason;
};

export type QueryExecutionOptions = {
  include_explain?: boolean;
  collect_metrics?: boolean;
};

export type QueryRequest = {
  table_uri: string;
  snapshot_version?: number;
  sql: string;
  preferred_target: ExecutionTarget;
  options?: QueryExecutionOptions;
};

export type PartitionColumnType = 'string' | 'int64' | 'boolean' | 'unsupported';

export type BrowserHttpFileDescriptor = {
  path: string;
  url: string;
  size_bytes: number;
  partition_values: Record<string, string | null>;
  stats?: string;
};

export type BrowserHttpSnapshotDescriptor = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  browser_compatibility?: CapabilityReport;
  required_capabilities?: CapabilityReport;
  active_files: BrowserHttpFileDescriptor[];
};

export type BrowserAccessMode = 'browser_safe_http' | 'cloud_object_store';

export type QueryMetricsSummary = {
  bytes_fetched: number;
  duration_ms: number;
  files_touched: number;
  files_skipped: number;
  row_groups_touched?: number;
  row_groups_skipped?: number;
  footer_reads?: number;
  rows_emitted?: number;
  snapshot_bootstrap_duration_ms?: number;
  access_mode?: BrowserAccessMode;
};

export type QueryResponse = {
  executed_on: ExecutionTarget;
  capabilities: CapabilityReport;
  fallback_reason?: FallbackReason;
  metrics: QueryMetricsSummary;
};

export type BrowserWorkerSqlOutput = 'arrow_ipc_stream';

export type BrowserWorkerOpenTableCommand = {
  request_id: string;
  name: string;
  snapshot: BrowserHttpSnapshotDescriptor;
};

export type BrowserWorkerOpenDeltaTableCommand = {
  request_id: string;
  name: string;
  snapshot: BrowserHttpSnapshotDescriptor;
};

export type BrowserWorkerSqlCommand = {
  request_id: string;
  name: string;
  query: QueryRequest;
  output?: BrowserWorkerSqlOutput;
};

export type BrowserWorkerDisposeCommand = {
  request_id: string;
  name: string;
};

export type BrowserWorkerCommand =
  | { open_table: BrowserWorkerOpenTableCommand }
  | { open_delta_table: BrowserWorkerOpenDeltaTableCommand }
  | { sql: BrowserWorkerSqlCommand }
  | { dispose: BrowserWorkerDisposeCommand };

export type ArrowIpcFormat = 'stream' | 'file';

export type WireArrowIpcResult = {
  format: ArrowIpcFormat;
  content_type: string;
  bytes: number[] | ArrayBuffer | Uint8Array;
};

export type ArrowIpcResult = {
  format: ArrowIpcFormat;
  content_type: string;
  bytes: Uint8Array;
};

export type BrowserWorkerOpenedEnvelope = {
  request_id: string;
  name: string;
};

export type BrowserWorkerSuccessEnvelope = {
  request_id: string;
  response: QueryResponse;
  result: ArrowIpcResult;
};

export type WireBrowserWorkerSuccessEnvelope = Omit<BrowserWorkerSuccessEnvelope, 'result'> & {
  result: WireArrowIpcResult;
};

export type BrowserWorkerDisposedEnvelope = {
  request_id: string;
  name: string;
};

export type BrowserWorkerErrorEnvelope = {
  request_id: string;
  error: QueryError;
};

export type BrowserWorkerResponseEnvelope =
  | { opened: BrowserWorkerOpenedEnvelope }
  | { success: BrowserWorkerSuccessEnvelope }
  | { disposed: BrowserWorkerDisposedEnvelope }
  | { error: BrowserWorkerErrorEnvelope };

export type WireBrowserWorkerResponseEnvelope =
  | { opened: BrowserWorkerOpenedEnvelope }
  | { success: WireBrowserWorkerSuccessEnvelope }
  | { disposed: BrowserWorkerDisposedEnvelope }
  | { error: BrowserWorkerErrorEnvelope };

export type AxonQueryResult = BrowserWorkerSuccessEnvelope & {
  fallbackReason?: FallbackReason;
};

export type AxonRequestOptions = {
  requestId?: string;
};

export type AxonQueryOptions = AxonRequestOptions & {
  tableUri?: string;
  snapshotVersion?: number;
  preferredTarget?: ExecutionTarget;
  queryOptions?: QueryExecutionOptions;
};

export type AxonBrowserClientOptions =
  | {
      workerUrl: string | URL;
      workerOptions?: WorkerOptions;
      requestId?: RequestIdFactory;
    }
  | {
      worker: Worker | WorkerFactory;
      requestId?: RequestIdFactory;
    };

export type RequestIdFactory = () => string;
export type WorkerFactory = () => Worker;

export interface AxonBrowserClient {
  openDeltaTable(
    name: string,
    snapshot: BrowserHttpSnapshotDescriptor,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerOpenedEnvelope>;
  query(
    name: string,
    request: QueryRequest,
    options?: AxonRequestOptions,
  ): Promise<AxonQueryResult>;
  query(name: string, sql: string, options?: AxonQueryOptions): Promise<AxonQueryResult>;
  dispose(name: string, options?: AxonRequestOptions): Promise<BrowserWorkerDisposedEnvelope>;
  terminate(): void;
}

type PendingRequest = {
  expected: 'opened' | 'success' | 'disposed';
  resolve: (response: BrowserWorkerResponseEnvelope) => void;
  reject: (error: Error) => void;
};

export class AxonWorkerError extends Error {
  readonly name = 'AxonWorkerError';
  readonly requestId: string;
  readonly queryError: QueryError;
  readonly fallbackReason?: FallbackReason;
  readonly envelope: BrowserWorkerErrorEnvelope;

  constructor(envelope: BrowserWorkerErrorEnvelope) {
    super(envelope.error.message);
    this.requestId = envelope.request_id;
    this.queryError = envelope.error;
    this.fallbackReason = envelope.error.fallback_reason;
    this.envelope = envelope;
  }
}

export class AxonSdkError extends Error {
  readonly name = 'AxonSdkError';
}

export class AxonProtocolError extends Error {
  readonly name = 'AxonProtocolError';
}

export function createAxonBrowserClient(options: AxonBrowserClientOptions): AxonBrowserClient {
  const worker =
    'workerUrl' in options
      ? new Worker(options.workerUrl, { type: 'module', ...options.workerOptions })
      : createWorkerFromOption(options.worker);

  return new AxonBrowserWorkerClient(worker, options.requestId);
}

export function openDeltaTableCommand(
  requestId: string,
  name: string,
  snapshot: BrowserHttpSnapshotDescriptor,
): BrowserWorkerCommand {
  return {
    open_delta_table: {
      request_id: requestId,
      name,
      snapshot,
    },
  };
}

export function queryCommand(
  requestId: string,
  name: string,
  request: QueryRequest,
): BrowserWorkerCommand {
  return {
    sql: {
      request_id: requestId,
      name,
      query: request,
      output: 'arrow_ipc_stream',
    },
  };
}

export function disposeCommand(requestId: string, name: string): BrowserWorkerCommand {
  return {
    dispose: {
      request_id: requestId,
      name,
    },
  };
}

export function createQueryRequest(
  tableUri: string,
  sql: string,
  options: Omit<AxonQueryOptions, 'requestId' | 'tableUri'> = {},
): QueryRequest {
  const request: QueryRequest = {
    table_uri: tableUri,
    sql,
    preferred_target: options.preferredTarget ?? PREFERRED_TARGET,
    options: {
      include_explain: false,
      collect_metrics: true,
      ...options.queryOptions,
    },
  };

  if (options.snapshotVersion !== undefined) {
    request.snapshot_version = options.snapshotVersion;
  }

  return request;
}

export function expectedArrowIpcContentType(format: ArrowIpcFormat): string {
  switch (format) {
    case 'stream':
      return ARROW_IPC_STREAM_CONTENT_TYPE;
    case 'file':
      return ARROW_IPC_FILE_CONTENT_TYPE;
  }
}

export function normalizeArrowIpcResult(result: WireArrowIpcResult): ArrowIpcResult {
  const expectedContentType = expectedArrowIpcContentType(result.format);
  if (result.content_type !== expectedContentType) {
    throw new AxonProtocolError(
      `Arrow IPC content type '${result.content_type}' does not match expected content type '${expectedContentType}' for format '${result.format}'`,
    );
  }

  return {
    format: result.format,
    content_type: result.content_type,
    bytes: normalizeBytes(result.bytes),
  };
}

class AxonBrowserWorkerClient implements AxonBrowserClient {
  private readonly worker: Worker;
  private readonly requestId: RequestIdFactory;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly tables = new Map<string, BrowserHttpSnapshotDescriptor>();
  private terminated = false;

  private readonly handleWorkerMessage = (event: MessageEvent<unknown>): void => {
    let response: BrowserWorkerResponseEnvelope;
    try {
      response = normalizeWorkerResponse(event.data);
    } catch (error) {
      this.rejectAll(toError(error));
      return;
    }

    const requestId = responseRequestId(response);
    const pending = this.pending.get(requestId);
    if (!pending) {
      return;
    }

    this.pending.delete(requestId);

    if ('error' in response) {
      pending.reject(new AxonWorkerError(response.error));
      return;
    }

    if (!(pending.expected in response)) {
      pending.reject(
        new AxonProtocolError(
          `worker response for request '${requestId}' did not match expected '${pending.expected}' envelope`,
        ),
      );
      return;
    }

    pending.resolve(response);
  };

  private readonly handleWorkerError = (event: ErrorEvent): void => {
    const message = event.message || 'Axon worker emitted an error event';
    this.rejectAll(new AxonSdkError(message));
  };

  private readonly handleWorkerMessageError = (): void => {
    this.rejectAll(new AxonSdkError('Axon worker emitted a messageerror event'));
  };

  constructor(worker: Worker, requestId: RequestIdFactory = defaultRequestIdFactory()) {
    this.worker = worker;
    this.requestId = requestId;
    this.worker.addEventListener('message', this.handleWorkerMessage);
    this.worker.addEventListener('error', this.handleWorkerError);
    this.worker.addEventListener('messageerror', this.handleWorkerMessageError);
  }

  async openDeltaTable(
    name: string,
    snapshot: BrowserHttpSnapshotDescriptor,
    options: AxonRequestOptions = {},
  ): Promise<BrowserWorkerOpenedEnvelope> {
    const requestId = options.requestId ?? this.requestId();
    const response = await this.send(openDeltaTableCommand(requestId, name, snapshot), 'opened');
    if (!('opened' in response)) {
      throw new AxonProtocolError(
        `worker response for request '${requestId}' did not contain an opened envelope`,
      );
    }
    const opened = response.opened;
    if (opened.name !== name) {
      throw new AxonProtocolError(
        `worker opened table '${opened.name}' for request '${requestId}', but '${name}' was requested`,
      );
    }
    this.tables.set(name, snapshot);
    return opened;
  }

  query(
    name: string,
    request: QueryRequest,
    options?: AxonRequestOptions,
  ): Promise<AxonQueryResult>;
  query(name: string, sql: string, options?: AxonQueryOptions): Promise<AxonQueryResult>;
  async query(
    name: string,
    request: QueryRequest | string,
    options: AxonRequestOptions | AxonQueryOptions = {},
  ): Promise<AxonQueryResult> {
    const requestId = options.requestId ?? this.requestId();
    const queryRequest =
      typeof request === 'string' ? this.queryRequestFromSql(name, request, options) : request;
    const response = await this.send(queryCommand(requestId, name, queryRequest), 'success');
    if (!('success' in response)) {
      throw new AxonProtocolError(
        `worker response for request '${requestId}' did not contain a success envelope`,
      );
    }
    const success = response.success;
    return {
      ...success,
      fallbackReason: success.response.fallback_reason,
    };
  }

  async dispose(
    name: string,
    options: AxonRequestOptions = {},
  ): Promise<BrowserWorkerDisposedEnvelope> {
    const requestId = options.requestId ?? this.requestId();
    const response = await this.send(disposeCommand(requestId, name), 'disposed');
    if (!('disposed' in response)) {
      throw new AxonProtocolError(
        `worker response for request '${requestId}' did not contain a disposed envelope`,
      );
    }
    const disposed = response.disposed;
    if (disposed.name !== name) {
      throw new AxonProtocolError(
        `worker disposed table '${disposed.name}' for request '${requestId}', but '${name}' was requested`,
      );
    }
    this.tables.delete(name);
    return disposed;
  }

  terminate(): void {
    if (this.terminated) {
      return;
    }

    this.terminated = true;
    this.worker.removeEventListener('message', this.handleWorkerMessage);
    this.worker.removeEventListener('error', this.handleWorkerError);
    this.worker.removeEventListener('messageerror', this.handleWorkerMessageError);
    this.rejectAll(new AxonSdkError('Axon browser client was terminated'));
    this.worker.terminate();
  }

  private send(
    command: BrowserWorkerCommand,
    expected: PendingRequest['expected'],
  ): Promise<BrowserWorkerResponseEnvelope> {
    if (this.terminated) {
      return Promise.reject(new AxonSdkError('Axon browser client has been terminated'));
    }

    const requestId = commandRequestId(command);
    if (this.pending.has(requestId)) {
      return Promise.reject(new AxonSdkError(`duplicate active request id '${requestId}'`));
    }

    return new Promise((resolve, reject) => {
      this.pending.set(requestId, { expected, resolve, reject });

      try {
        this.worker.postMessage(command);
      } catch (error) {
        this.pending.delete(requestId);
        reject(toError(error));
      }
    });
  }

  private queryRequestFromSql(
    name: string,
    sql: string,
    options: AxonQueryOptions,
  ): QueryRequest {
    const openedSnapshot = this.tables.get(name);
    const tableUri = options.tableUri ?? openedSnapshot?.table_uri;
    if (!tableUri) {
      throw new AxonSdkError(
        `query('${name}', sql) requires an opened table or an explicit tableUri option`,
      );
    }

    return createQueryRequest(tableUri, sql, {
      snapshotVersion: options.snapshotVersion ?? openedSnapshot?.snapshot_version,
      preferredTarget: options.preferredTarget,
      queryOptions: options.queryOptions,
    });
  }

  private rejectAll(error: Error): void {
    for (const [requestId, pending] of this.pending) {
      this.pending.delete(requestId);
      pending.reject(error);
    }
  }
}

function createWorkerFromOption(worker: Worker | WorkerFactory): Worker {
  return typeof worker === 'function' ? worker() : worker;
}

function defaultRequestIdFactory(): RequestIdFactory {
  let next = 0;
  const prefix = `axon-${Date.now().toString(36)}`;

  return () => `${prefix}-${++next}`;
}

function commandRequestId(command: BrowserWorkerCommand): string {
  if ('open_table' in command) {
    return command.open_table.request_id;
  }
  if ('open_delta_table' in command) {
    return command.open_delta_table.request_id;
  }
  if ('sql' in command) {
    return command.sql.request_id;
  }
  return command.dispose.request_id;
}

function responseRequestId(response: BrowserWorkerResponseEnvelope): string {
  if ('opened' in response) {
    return response.opened.request_id;
  }
  if ('success' in response) {
    return response.success.request_id;
  }
  if ('disposed' in response) {
    return response.disposed.request_id;
  }
  return response.error.request_id;
}

function normalizeWorkerResponse(data: unknown): BrowserWorkerResponseEnvelope {
  if (!isObject(data)) {
    throw new AxonProtocolError('worker response must be an object');
  }

  const response = data as WireBrowserWorkerResponseEnvelope;
  if ('opened' in response) {
    return response;
  }
  if ('success' in response) {
    return {
      success: {
        ...response.success,
        result: normalizeArrowIpcResult(response.success.result),
      },
    };
  }
  if ('disposed' in response) {
    return response;
  }
  if ('error' in response) {
    return response;
  }

  throw new AxonProtocolError('worker response did not contain a known Axon envelope tag');
}

function normalizeBytes(bytes: number[] | ArrayBuffer | Uint8Array): Uint8Array {
  if (bytes instanceof Uint8Array) {
    return bytes;
  }
  if (bytes instanceof ArrayBuffer) {
    return new Uint8Array(bytes);
  }
  if (Array.isArray(bytes)) {
    return Uint8Array.from(bytes);
  }

  throw new AxonProtocolError('Arrow IPC result bytes must be a byte array');
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function toError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  return new AxonSdkError(String(error));
}
