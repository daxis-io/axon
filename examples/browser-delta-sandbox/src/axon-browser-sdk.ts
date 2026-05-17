export const PREFERRED_TARGET: ExecutionTarget = 'browser_wasm';
export const ARROW_IPC_STREAM_CONTENT_TYPE = 'application/vnd.apache.arrow.stream';
export const ARROW_IPC_FILE_CONTENT_TYPE = 'application/vnd.apache.arrow.file';

export function redactUrlSecrets(message: string): string {
  return message.replace(/https?:\/\/[^\s'"<>]+/g, (candidate) => {
    try {
      const url = new URL(candidate);
      url.username = '';
      url.password = '';
      url.search = '';
      url.hash = '';
      return url.toString();
    } catch {
      return candidate.split(/[?#]/, 1)[0];
    }
  });
}

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

export type BrowserWorkerInspectParquetCommand = {
  request_id: string;
  name: string;
  path: string;
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
  | { inspect_parquet: BrowserWorkerInspectParquetCommand }
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
  preview?: BrowserWorkerResultPreview;
};

export type WireBrowserWorkerSuccessEnvelope = Omit<BrowserWorkerSuccessEnvelope, 'result'> & {
  result: WireArrowIpcResult;
};

export type ParquetCompressionSummary = {
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  ratio_basis_points: number;
};

export type ParquetInspectionColumnChunk = {
  column_name: string;
  compression: string;
  encodings: string[];
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  null_count?: number;
  has_statistics: boolean;
  has_column_index: boolean;
  has_offset_index: boolean;
  has_bloom_filter: boolean;
};

export type ParquetInspectionRowGroup = {
  index: number;
  row_count: number;
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  columns: ParquetInspectionColumnChunk[];
};

export type ParquetInspectionColumn = {
  name: string;
  physical_type: string;
  logical_type?: string;
  converted_type?: string;
  repetition: string;
  nullable: boolean;
  compressed_size_bytes: number;
  uncompressed_size_bytes: number;
  null_count?: number;
  encodings: string[];
  compressions: string[];
  has_statistics: boolean;
  has_column_index: boolean;
  has_offset_index: boolean;
  has_bloom_filter: boolean;
};

export type ParquetInspectionSummary = {
  path: string;
  object_size_bytes: number;
  footer_length_bytes: number;
  metadata_memory_size_bytes: number;
  created_by?: string;
  file_version: number;
  row_group_count: number;
  row_count: number;
  column_count: number;
  compression: ParquetCompressionSummary;
  columns: ParquetInspectionColumn[];
  row_groups: ParquetInspectionRowGroup[];
};

export type BrowserWorkerParquetInspectionEnvelope = {
  request_id: string;
  summary: ParquetInspectionSummary;
};

export type BrowserWorkerResultPreviewCell = string | number | boolean | null;

export type BrowserWorkerResultPreview = {
  columns: string[];
  rows: BrowserWorkerResultPreviewCell[][];
  row_count: number;
  preview_row_limit: number;
  truncated: boolean;
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
  | { parquet_inspection: BrowserWorkerParquetInspectionEnvelope }
  | { disposed: BrowserWorkerDisposedEnvelope }
  | { error: BrowserWorkerErrorEnvelope };

export type WireBrowserWorkerResponseEnvelope =
  | { opened: BrowserWorkerOpenedEnvelope }
  | { success: WireBrowserWorkerSuccessEnvelope }
  | { parquet_inspection: BrowserWorkerParquetInspectionEnvelope }
  | { disposed: BrowserWorkerDisposedEnvelope }
  | { error: BrowserWorkerErrorEnvelope };

export type BrowserWorkerEventPhase = 'instantiate' | 'open' | 'inspect' | 'query';

export type BrowserWorkerEventContext = {
  phase: BrowserWorkerEventPhase;
  request_id?: string;
  query_id?: string;
  table_name?: string;
};

export type BrowserWorkerProgressStage =
  | 'started'
  | 'planning'
  | 'executing'
  | 'arrow_ipc_ready'
  | 'finished';

export type BrowserWorkerLogLevel = 'debug' | 'info' | 'warn' | 'error';

export type BrowserWorkerProgressEvent = {
  context: BrowserWorkerEventContext;
  stage: BrowserWorkerProgressStage;
};

export type BrowserWorkerLogEvent = {
  context: BrowserWorkerEventContext;
  level: BrowserWorkerLogLevel;
  message: string;
};

export type BrowserWorkerRangeReadMetricsEvent = {
  context: BrowserWorkerEventContext;
  bytes_fetched: number;
  files_touched: number;
  files_skipped: number;
  row_groups_touched: number;
  row_groups_skipped: number;
  footer_reads?: number;
  rows_emitted: number;
  snapshot_bootstrap_duration_ms?: number;
  access_mode?: BrowserAccessMode;
};

export type BrowserWorkerTransportCacheMetrics = {
  bytes_reused: number;
  validation_misses: number;
  persistent_cache_errors: number;
};

export type BrowserWorkerCacheMetricsEvent = {
  context: BrowserWorkerEventContext;
  session_cached_bytes: number;
  session_table_count: number;
  max_session_cached_bytes: number;
  transport?: BrowserWorkerTransportCacheMetrics;
};

export type BrowserWorkerFallbackEvent = {
  context: BrowserWorkerEventContext;
  reason: FallbackReason;
};

export type BrowserWorkerCancellationEvent = {
  context: BrowserWorkerEventContext;
  error: QueryError;
};

export type BrowserWorkerTerminalErrorEvent = {
  context: BrowserWorkerEventContext;
  error: QueryError;
};

export type BrowserWorkerEventEnvelope =
  | { progress: BrowserWorkerProgressEvent }
  | { log: BrowserWorkerLogEvent }
  | { range_read_metrics: BrowserWorkerRangeReadMetricsEvent }
  | { cache_metrics: BrowserWorkerCacheMetricsEvent }
  | { fallback: BrowserWorkerFallbackEvent }
  | { cancellation: BrowserWorkerCancellationEvent }
  | { terminal_error: BrowserWorkerTerminalErrorEvent };

export type WireBrowserWorkerMessageEnvelope =
  | WireBrowserWorkerResponseEnvelope
  | BrowserWorkerEventEnvelope;

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

export type PlatformFeatures = {
  crossOriginIsolated: boolean;
  wasmSIMD: boolean;
  wasmThreads: boolean;
  bigInt64Array: boolean;
};

export type PlatformFeatureKey = keyof PlatformFeatures;
export type BrowserBundleTier = 'baseline' | 'simd' | 'threaded' | 'simd_threaded';
export type BrowserBundleStatus = 'available' | 'future';

export type BrowserWorkerBundle = {
  id: string;
  tier: BrowserBundleTier;
  workerUrl: string | URL;
  wasmUrl?: string | URL;
  requiredFeatures?: Partial<Record<PlatformFeatureKey, boolean>>;
  status?: BrowserBundleStatus;
};

export type BrowserBundleManifest = {
  bundles: BrowserWorkerBundle[];
};

export type BrowserBundleSelection = {
  bundle: BrowserWorkerBundle;
  features: PlatformFeatures;
};

export type PlatformFeatureScope = Partial<{
  crossOriginIsolated: boolean;
  WebAssembly: typeof WebAssembly;
  BigInt64Array: BigInt64ArrayConstructor;
  SharedArrayBuffer: typeof SharedArrayBuffer;
  Atomics: typeof Atomics;
}>;

export const AXON_BROWSER_BUNDLE_MANIFEST: BrowserBundleManifest = {
  bundles: [
    {
      id: 'baseline',
      tier: 'baseline',
      workerUrl: '/workers/browser-engine-worker.js',
      wasmUrl: '/workers/browser_engine_worker.wasm',
    },
    {
      id: 'simd',
      tier: 'simd',
      status: 'future',
      workerUrl: '/workers/browser-engine-worker.simd.js',
      wasmUrl: '/workers/browser_engine_worker.simd.wasm',
      requiredFeatures: {
        wasmSIMD: true,
      },
    },
    {
      id: 'threaded',
      tier: 'threaded',
      status: 'future',
      workerUrl: '/workers/browser-engine-worker.threaded.js',
      wasmUrl: '/workers/browser_engine_worker.threaded.wasm',
      requiredFeatures: {
        crossOriginIsolated: true,
        wasmThreads: true,
      },
    },
    {
      id: 'simd-threaded',
      tier: 'simd_threaded',
      status: 'future',
      workerUrl: '/workers/browser-engine-worker.simd-threaded.js',
      wasmUrl: '/workers/browser_engine_worker.simd-threaded.wasm',
      requiredFeatures: {
        crossOriginIsolated: true,
        wasmSIMD: true,
        wasmThreads: true,
      },
    },
  ],
};

export type AxonBrowserClientOptions =
  | {
      workerUrl: string | URL;
      workerOptions?: WorkerOptions;
      requestId?: RequestIdFactory;
      onEvent?: BrowserWorkerEventHandler;
    }
  | {
      worker: Worker | WorkerFactory;
      requestId?: RequestIdFactory;
      onEvent?: BrowserWorkerEventHandler;
    }
  | {
      bundleManifest?: BrowserBundleManifest;
      platformFeatures?: PlatformFeatures;
      workerOptions?: WorkerOptions;
      requestId?: RequestIdFactory;
      onEvent?: BrowserWorkerEventHandler;
    };

export type RequestIdFactory = () => string;
export type WorkerFactory = () => Worker;
export type BrowserWorkerEventHandler = (event: BrowserWorkerEventEnvelope) => void;

export interface AxonBrowserClient {
  openDeltaTable(
    name: string,
    snapshot: BrowserHttpSnapshotDescriptor,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerOpenedEnvelope>;
  inspectParquet(
    name: string,
    path: string,
    options?: AxonRequestOptions,
  ): Promise<ParquetInspectionSummary>;
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
  expected: 'opened' | 'success' | 'parquet_inspection' | 'disposed';
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
      : 'worker' in options
        ? createWorkerFromOption(options.worker)
        : createWorkerFromBundleSelection(options);

  return new AxonBrowserWorkerClient(worker, options.requestId, options.onEvent);
}

export function getPlatformFeatures(scope: PlatformFeatureScope = globalThis): PlatformFeatures {
  const crossOriginIsolated = scope.crossOriginIsolated === true;

  return {
    crossOriginIsolated,
    wasmSIMD: detectWasmSIMD(scope),
    wasmThreads: detectWasmThreads(scope, crossOriginIsolated),
    bigInt64Array: typeof scope.BigInt64Array === 'function',
  };
}

export function selectBundle(
  manifest: BrowserBundleManifest,
  features: PlatformFeatures = getPlatformFeatures(),
): BrowserBundleSelection {
  let selected: { bundle: BrowserWorkerBundle; score: number } | undefined;

  for (const bundle of manifest.bundles) {
    if (bundleStatus(bundle) !== 'available' || !bundleSupportsFeatures(bundle, features)) {
      continue;
    }

    const score = bundleScore(bundle);
    if (!selected || score > selected.score) {
      selected = { bundle, score };
    }
  }

  if (!selected) {
    throw new AxonSdkError(
      `no supported Axon browser bundle found for platform features ${JSON.stringify(features)}`,
    );
  }

  return {
    bundle: selected.bundle,
    features,
  };
}

function bundleStatus(bundle: BrowserWorkerBundle): BrowserBundleStatus {
  const status = bundle.status ?? 'available';
  if (status === 'available' || status === 'future') {
    return status;
  }

  throw new AxonSdkError(
    `unknown Axon browser bundle status '${String(status)}' for bundle '${bundle.id}'`,
  );
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

export function inspectParquetCommand(
  requestId: string,
  name: string,
  path: string,
): BrowserWorkerCommand {
  return {
    inspect_parquet: {
      request_id: requestId,
      name,
      path,
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
  private readonly onEvent?: BrowserWorkerEventHandler;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly tables = new Map<string, BrowserHttpSnapshotDescriptor>();
  private terminated = false;

  private readonly handleWorkerMessage = (event: MessageEvent<unknown>): void => {
    let message: NormalizedWorkerMessage;
    try {
      message = normalizeWorkerMessage(event.data);
    } catch (error) {
      this.rejectAll(toError(error));
      return;
    }

    if (message.kind === 'event') {
      this.onEvent?.(message.event);
      return;
    }

    const response = message.response;
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

  constructor(
    worker: Worker,
    requestId: RequestIdFactory = defaultRequestIdFactory(),
    onEvent?: BrowserWorkerEventHandler,
  ) {
    this.worker = worker;
    this.requestId = requestId;
    this.onEvent = onEvent;
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

  async inspectParquet(
    name: string,
    path: string,
    options: AxonRequestOptions = {},
  ): Promise<ParquetInspectionSummary> {
    const requestId = options.requestId ?? this.requestId();
    const response = await this.send(
      inspectParquetCommand(requestId, name, path),
      'parquet_inspection',
    );
    if (!('parquet_inspection' in response)) {
      throw new AxonProtocolError(
        `worker response for request '${requestId}' did not contain a parquet_inspection envelope`,
      );
    }
    return response.parquet_inspection.summary;
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

  private queryRequestFromSql(name: string, sql: string, options: AxonQueryOptions): QueryRequest {
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

function createWorkerFromBundleSelection(options: {
  bundleManifest?: BrowserBundleManifest;
  platformFeatures?: PlatformFeatures;
  workerOptions?: WorkerOptions;
}): Worker {
  const selection = selectBundle(
    options.bundleManifest ?? AXON_BROWSER_BUNDLE_MANIFEST,
    options.platformFeatures ?? getPlatformFeatures(),
  );

  return new Worker(selection.bundle.workerUrl, { type: 'module', ...options.workerOptions });
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
  if ('inspect_parquet' in command) {
    return command.inspect_parquet.request_id;
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
  if ('parquet_inspection' in response) {
    return response.parquet_inspection.request_id;
  }
  if ('disposed' in response) {
    return response.disposed.request_id;
  }
  return response.error.request_id;
}

type NormalizedWorkerMessage =
  | { kind: 'event'; event: BrowserWorkerEventEnvelope }
  | { kind: 'response'; response: BrowserWorkerResponseEnvelope };

function normalizeWorkerMessage(data: unknown): NormalizedWorkerMessage {
  if (!isObject(data)) {
    throw new AxonProtocolError('worker message must be an object');
  }

  const tags = workerEnvelopeTags(data);
  if (tags.length > 1) {
    throw new AxonProtocolError(
      `worker message contained multiple Axon envelope tags: ${tags.join(', ')}`,
    );
  }

  if (tags.length === 1 && isWorkerEventTag(tags[0])) {
    return { kind: 'event', event: normalizeWorkerEvent(tags[0], data[tags[0]]) };
  }

  return { kind: 'response', response: normalizeWorkerResponse(data) };
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
        preview: normalizeResultPreview(response.success.preview, 'success.preview'),
      },
    };
  }
  if ('parquet_inspection' in response) {
    return {
      parquet_inspection: {
        request_id: requiredString(
          response.parquet_inspection.request_id,
          'parquet_inspection.request_id',
        ),
        summary: normalizeParquetInspectionSummary(
          response.parquet_inspection.summary,
          'parquet_inspection.summary',
        ),
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

function normalizeWorkerEvent(tag: WorkerEventTag, payload: unknown): BrowserWorkerEventEnvelope {
  if (!isObject(payload)) {
    throw new AxonProtocolError(`worker event '${tag}' payload must be an object`);
  }

  switch (tag) {
    case 'progress':
      return {
        progress: {
          context: normalizeWorkerEventContext(payload.context, 'progress.context'),
          stage: requiredEnum(payload.stage, 'progress.stage', BROWSER_WORKER_PROGRESS_STAGES),
        },
      };
    case 'log':
      return {
        log: {
          context: normalizeWorkerEventContext(payload.context, 'log.context'),
          level: requiredEnum(payload.level, 'log.level', BROWSER_WORKER_LOG_LEVELS),
          message: requiredString(payload.message, 'log.message'),
        },
      };
    case 'range_read_metrics':
      return {
        range_read_metrics: {
          context: normalizeWorkerEventContext(payload.context, 'range_read_metrics.context'),
          bytes_fetched: requiredNumber(payload.bytes_fetched, 'range_read_metrics.bytes_fetched'),
          files_touched: requiredNumber(payload.files_touched, 'range_read_metrics.files_touched'),
          files_skipped: requiredNumber(payload.files_skipped, 'range_read_metrics.files_skipped'),
          row_groups_touched: requiredNumber(
            payload.row_groups_touched,
            'range_read_metrics.row_groups_touched',
          ),
          row_groups_skipped: requiredNumber(
            payload.row_groups_skipped,
            'range_read_metrics.row_groups_skipped',
          ),
          footer_reads: optionalNumber(payload.footer_reads, 'range_read_metrics.footer_reads'),
          rows_emitted: requiredNumber(payload.rows_emitted, 'range_read_metrics.rows_emitted'),
          snapshot_bootstrap_duration_ms: optionalNumber(
            payload.snapshot_bootstrap_duration_ms,
            'range_read_metrics.snapshot_bootstrap_duration_ms',
          ),
          access_mode: optionalEnum(
            payload.access_mode,
            'range_read_metrics.access_mode',
            BROWSER_ACCESS_MODES,
          ),
        },
      };
    case 'cache_metrics':
      return {
        cache_metrics: {
          context: normalizeWorkerEventContext(payload.context, 'cache_metrics.context'),
          session_cached_bytes: requiredNumber(
            payload.session_cached_bytes,
            'cache_metrics.session_cached_bytes',
          ),
          session_table_count: requiredNumber(
            payload.session_table_count,
            'cache_metrics.session_table_count',
          ),
          max_session_cached_bytes: requiredNumber(
            payload.max_session_cached_bytes,
            'cache_metrics.max_session_cached_bytes',
          ),
          transport: normalizeOptionalTransportCacheMetrics(payload.transport),
        },
      };
    case 'fallback':
      return {
        fallback: {
          context: normalizeWorkerEventContext(payload.context, 'fallback.context'),
          reason: normalizeFallbackReason(payload.reason, 'fallback.reason'),
        },
      };
    case 'cancellation':
      return {
        cancellation: {
          context: normalizeWorkerEventContext(payload.context, 'cancellation.context'),
          error: normalizeQueryError(payload.error, 'cancellation.error'),
        },
      };
    case 'terminal_error':
      return {
        terminal_error: {
          context: normalizeWorkerEventContext(payload.context, 'terminal_error.context'),
          error: normalizeQueryError(payload.error, 'terminal_error.error'),
        },
      };
  }
}

function normalizeWorkerEventContext(value: unknown, path: string): BrowserWorkerEventContext {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    phase: requiredEnum(value.phase, `${path}.phase`, BROWSER_WORKER_EVENT_PHASES),
    request_id: optionalString(value.request_id, `${path}.request_id`),
    query_id: optionalString(value.query_id, `${path}.query_id`),
    table_name: optionalString(value.table_name, `${path}.table_name`),
  };
}

function normalizeOptionalTransportCacheMetrics(
  value: unknown,
): BrowserWorkerTransportCacheMetrics | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isObject(value)) {
    throw new AxonProtocolError('cache_metrics.transport must be an object');
  }

  return {
    bytes_reused: requiredNumber(value.bytes_reused, 'cache_metrics.transport.bytes_reused'),
    validation_misses: requiredNumber(
      value.validation_misses,
      'cache_metrics.transport.validation_misses',
    ),
    persistent_cache_errors: requiredNumber(
      value.persistent_cache_errors,
      'cache_metrics.transport.persistent_cache_errors',
    ),
  };
}

function normalizeQueryError(value: unknown, path: string): QueryError {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    code: requiredEnum(value.code, `${path}.code`, QUERY_ERROR_CODES),
    message: requiredString(value.message, `${path}.message`),
    target: requiredEnum(value.target, `${path}.target`, EXECUTION_TARGETS),
    fallback_reason:
      value.fallback_reason === undefined
        ? undefined
        : normalizeFallbackReason(value.fallback_reason, `${path}.fallback_reason`),
  };
}

function normalizeResultPreview(
  value: unknown,
  path: string,
): BrowserWorkerResultPreview | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  const columns = requiredArray(value.columns, `${path}.columns`).map((column, index) =>
    requiredString(column, `${path}.columns[${index}]`),
  );
  const rows = requiredArray(value.rows, `${path}.rows`).map((row, rowIndex) => {
    const cells = requiredArray(row, `${path}.rows[${rowIndex}]`).map((cell, cellIndex) =>
      normalizeResultPreviewCell(cell, `${path}.rows[${rowIndex}][${cellIndex}]`),
    );
    if (cells.length !== columns.length) {
      throw new AxonProtocolError(`${path}.rows[${rowIndex}] must contain ${columns.length} cells`);
    }
    return cells;
  });
  const rowCount = requiredNonNegativeInteger(value.row_count, `${path}.row_count`);
  const previewRowLimit = requiredNonNegativeInteger(
    value.preview_row_limit,
    `${path}.preview_row_limit`,
  );
  if (rows.length > rowCount) {
    throw new AxonProtocolError(`${path}.rows must not exceed ${path}.row_count`);
  }
  if (rows.length > previewRowLimit) {
    throw new AxonProtocolError(`${path}.rows must not exceed ${path}.preview_row_limit`);
  }

  return {
    columns,
    rows,
    row_count: rowCount,
    preview_row_limit: previewRowLimit,
    truncated: requiredBoolean(value.truncated, `${path}.truncated`),
  };
}

function normalizeResultPreviewCell(value: unknown, path: string): BrowserWorkerResultPreviewCell {
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  ) {
    return value;
  }

  throw new AxonProtocolError(`${path} must be a string, finite number, boolean, or null`);
}

function normalizeParquetInspectionSummary(value: unknown, path: string): ParquetInspectionSummary {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    path: requiredString(value.path, `${path}.path`),
    object_size_bytes: requiredNonNegativeInteger(
      value.object_size_bytes,
      `${path}.object_size_bytes`,
    ),
    footer_length_bytes: requiredNonNegativeInteger(
      value.footer_length_bytes,
      `${path}.footer_length_bytes`,
    ),
    metadata_memory_size_bytes: requiredNonNegativeInteger(
      value.metadata_memory_size_bytes,
      `${path}.metadata_memory_size_bytes`,
    ),
    created_by: optionalString(value.created_by, `${path}.created_by`),
    file_version: requiredNonNegativeInteger(value.file_version, `${path}.file_version`),
    row_group_count: requiredNonNegativeInteger(value.row_group_count, `${path}.row_group_count`),
    row_count: requiredNonNegativeInteger(value.row_count, `${path}.row_count`),
    column_count: requiredNonNegativeInteger(value.column_count, `${path}.column_count`),
    compression: normalizeParquetCompressionSummary(value.compression, `${path}.compression`),
    columns: requiredArray(value.columns, `${path}.columns`).map((column, index) =>
      normalizeParquetInspectionColumn(column, `${path}.columns[${index}]`),
    ),
    row_groups: requiredArray(value.row_groups, `${path}.row_groups`).map((rowGroup, index) =>
      normalizeParquetInspectionRowGroup(rowGroup, `${path}.row_groups[${index}]`),
    ),
  };
}

function normalizeParquetCompressionSummary(
  value: unknown,
  path: string,
): ParquetCompressionSummary {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    compressed_size_bytes: requiredNonNegativeInteger(
      value.compressed_size_bytes,
      `${path}.compressed_size_bytes`,
    ),
    uncompressed_size_bytes: requiredNonNegativeInteger(
      value.uncompressed_size_bytes,
      `${path}.uncompressed_size_bytes`,
    ),
    ratio_basis_points: requiredNonNegativeInteger(
      value.ratio_basis_points,
      `${path}.ratio_basis_points`,
    ),
  };
}

function normalizeParquetInspectionColumn(value: unknown, path: string): ParquetInspectionColumn {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    name: requiredString(value.name, `${path}.name`),
    physical_type: requiredString(value.physical_type, `${path}.physical_type`),
    logical_type: optionalString(value.logical_type, `${path}.logical_type`),
    converted_type: optionalString(value.converted_type, `${path}.converted_type`),
    repetition: requiredString(value.repetition, `${path}.repetition`),
    nullable: requiredBoolean(value.nullable, `${path}.nullable`),
    compressed_size_bytes: requiredNonNegativeInteger(
      value.compressed_size_bytes,
      `${path}.compressed_size_bytes`,
    ),
    uncompressed_size_bytes: requiredNonNegativeInteger(
      value.uncompressed_size_bytes,
      `${path}.uncompressed_size_bytes`,
    ),
    null_count: optionalNonNegativeInteger(value.null_count, `${path}.null_count`),
    encodings: requiredArray(value.encodings, `${path}.encodings`).map((encoding, index) =>
      requiredString(encoding, `${path}.encodings[${index}]`),
    ),
    compressions: requiredArray(value.compressions, `${path}.compressions`).map(
      (compression, index) => requiredString(compression, `${path}.compressions[${index}]`),
    ),
    has_statistics: requiredBoolean(value.has_statistics, `${path}.has_statistics`),
    has_column_index: requiredBoolean(value.has_column_index, `${path}.has_column_index`),
    has_offset_index: requiredBoolean(value.has_offset_index, `${path}.has_offset_index`),
    has_bloom_filter: requiredBoolean(value.has_bloom_filter, `${path}.has_bloom_filter`),
  };
}

function normalizeParquetInspectionRowGroup(
  value: unknown,
  path: string,
): ParquetInspectionRowGroup {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    index: requiredNonNegativeInteger(value.index, `${path}.index`),
    row_count: requiredNonNegativeInteger(value.row_count, `${path}.row_count`),
    compressed_size_bytes: requiredNonNegativeInteger(
      value.compressed_size_bytes,
      `${path}.compressed_size_bytes`,
    ),
    uncompressed_size_bytes: requiredNonNegativeInteger(
      value.uncompressed_size_bytes,
      `${path}.uncompressed_size_bytes`,
    ),
    columns: requiredArray(value.columns, `${path}.columns`).map((column, index) =>
      normalizeParquetInspectionColumnChunk(column, `${path}.columns[${index}]`),
    ),
  };
}

function normalizeParquetInspectionColumnChunk(
  value: unknown,
  path: string,
): ParquetInspectionColumnChunk {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }

  return {
    column_name: requiredString(value.column_name, `${path}.column_name`),
    compression: requiredString(value.compression, `${path}.compression`),
    encodings: requiredArray(value.encodings, `${path}.encodings`).map((encoding, index) =>
      requiredString(encoding, `${path}.encodings[${index}]`),
    ),
    compressed_size_bytes: requiredNonNegativeInteger(
      value.compressed_size_bytes,
      `${path}.compressed_size_bytes`,
    ),
    uncompressed_size_bytes: requiredNonNegativeInteger(
      value.uncompressed_size_bytes,
      `${path}.uncompressed_size_bytes`,
    ),
    null_count: optionalNonNegativeInteger(value.null_count, `${path}.null_count`),
    has_statistics: requiredBoolean(value.has_statistics, `${path}.has_statistics`),
    has_column_index: requiredBoolean(value.has_column_index, `${path}.has_column_index`),
    has_offset_index: requiredBoolean(value.has_offset_index, `${path}.has_offset_index`),
    has_bloom_filter: requiredBoolean(value.has_bloom_filter, `${path}.has_bloom_filter`),
  };
}

function normalizeFallbackReason(value: unknown, path: string): FallbackReason {
  if (typeof value === 'string' && includesString(FALLBACK_REASON_STRINGS, value)) {
    return value as FallbackReason;
  }

  const capabilityGate =
    isObject(value) && isObject(value.capability_gate) ? value.capability_gate : undefined;
  const capability = capabilityGate?.capability;
  const requiredState = capabilityGate?.required_state;
  if (
    typeof capability === 'string' &&
    includesString(CAPABILITY_KEYS, capability) &&
    typeof requiredState === 'string' &&
    includesString(CAPABILITY_STATES, requiredState)
  ) {
    return {
      capability_gate: {
        capability: capability as CapabilityKey,
        required_state: requiredState as CapabilityState,
      },
    };
  }

  throw new AxonProtocolError(`${path} must be a known fallback reason`);
}

function workerEnvelopeTags(data: object): WorkerEnvelopeTag[] {
  return WORKER_ENVELOPE_TAGS.filter((tag) => tag in data);
}

function isWorkerEventTag(tag: WorkerEnvelopeTag): tag is WorkerEventTag {
  return (WORKER_EVENT_TAGS as readonly string[]).includes(tag);
}

function requiredString(value: unknown, path: string): string {
  if (typeof value !== 'string') {
    throw new AxonProtocolError(`${path} must be a string`);
  }

  return value;
}

function optionalString(value: unknown, path: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  return requiredString(value, path);
}

function requiredNumber(value: unknown, path: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new AxonProtocolError(`${path} must be a finite number`);
  }

  return value;
}

function requiredNonNegativeInteger(value: unknown, path: string): number {
  const number = requiredNumber(value, path);
  if (!Number.isInteger(number) || number < 0) {
    throw new AxonProtocolError(`${path} must be a non-negative integer`);
  }

  return number;
}

function optionalNonNegativeInteger(value: unknown, path: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }

  return requiredNonNegativeInteger(value, path);
}

function requiredBoolean(value: unknown, path: string): boolean {
  if (typeof value !== 'boolean') {
    throw new AxonProtocolError(`${path} must be a boolean`);
  }

  return value;
}

function requiredArray(value: unknown, path: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new AxonProtocolError(`${path} must be an array`);
  }

  return value;
}

function optionalNumber(value: unknown, path: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }

  return requiredNumber(value, path);
}

function requiredEnum<T extends string>(value: unknown, path: string, allowed: readonly T[]): T {
  if (typeof value !== 'string' || !allowed.includes(value as T)) {
    throw new AxonProtocolError(`${path} must be one of ${allowed.join(', ')}`);
  }

  return value as T;
}

function includesString(values: readonly string[], value: string): boolean {
  return values.includes(value);
}

function optionalEnum<T extends string>(
  value: unknown,
  path: string,
  allowed: readonly T[],
): T | undefined {
  if (value === undefined) {
    return undefined;
  }

  return requiredEnum(value, path, allowed);
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

function bundleSupportsFeatures(bundle: BrowserWorkerBundle, features: PlatformFeatures): boolean {
  for (const [feature, required] of Object.entries(bundle.requiredFeatures ?? {}) as [
    PlatformFeatureKey,
    boolean,
  ][]) {
    if (features[feature] !== required) {
      return false;
    }
  }

  return true;
}

function bundleScore(bundle: BrowserWorkerBundle): number {
  const tierScore = BUNDLE_TIER_RANK[bundle.tier] * 100;
  const specificityScore = Object.keys(bundle.requiredFeatures ?? {}).length;
  return tierScore + specificityScore;
}

function detectWasmSIMD(scope: PlatformFeatureScope): boolean {
  const wasm = scope.WebAssembly;
  if (!wasm || typeof wasm.validate !== 'function') {
    return false;
  }

  return wasm.validate(WASM_SIMD_DETECTION_MODULE);
}

function detectWasmThreads(scope: PlatformFeatureScope, crossOriginIsolated: boolean): boolean {
  const wasm = scope.WebAssembly;
  if (
    !crossOriginIsolated ||
    !wasm ||
    typeof wasm.Memory !== 'function' ||
    typeof scope.SharedArrayBuffer !== 'function' ||
    typeof scope.Atomics !== 'object'
  ) {
    return false;
  }

  try {
    const memory = new wasm.Memory({ initial: 1, maximum: 1, shared: true });
    return memory.buffer instanceof scope.SharedArrayBuffer;
  } catch {
    return false;
  }
}

function toError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  return new AxonSdkError(String(error));
}

const BUNDLE_TIER_RANK: Record<BrowserBundleTier, number> = {
  baseline: 0,
  simd: 1,
  threaded: 2,
  simd_threaded: 3,
};

const WORKER_RESPONSE_TAGS = [
  'opened',
  'success',
  'parquet_inspection',
  'disposed',
  'error',
] as const;
const WORKER_EVENT_TAGS = [
  'progress',
  'log',
  'range_read_metrics',
  'cache_metrics',
  'fallback',
  'cancellation',
  'terminal_error',
] as const;
const WORKER_ENVELOPE_TAGS = [...WORKER_RESPONSE_TAGS, ...WORKER_EVENT_TAGS] as const;

type WorkerEnvelopeTag = (typeof WORKER_ENVELOPE_TAGS)[number];
type WorkerEventTag = (typeof WORKER_EVENT_TAGS)[number];

const EXECUTION_TARGETS = ['browser_wasm', 'native'] as const satisfies readonly ExecutionTarget[];
const CAPABILITY_KEYS = [
  'change_data_feed',
  'column_mapping',
  'deletion_vectors',
  'multi_partition_execution',
  'proxy_access',
  'range_reads',
  'signed_url_access',
  'time_travel',
  'timestamp_ntz',
  'unknown_protocol_features',
] as const satisfies readonly CapabilityKey[];
const CAPABILITY_STATES = [
  'supported',
  'native_only',
  'unsupported',
  'experimental',
] as const satisfies readonly CapabilityState[];
const FALLBACK_REASON_STRINGS = [
  'access_denied',
  'browser_runtime_constraint',
  'native_required',
  'network_failure',
  'range_read_unavailable',
  'security_policy',
  'signed_url_expired',
] as const;
const QUERY_ERROR_CODES = [
  'access_denied',
  'execution_failed',
  'fallback_required',
  'invalid_request',
  'object_store_protocol',
  'security_policy_violation',
  'unsupported_feature',
] as const satisfies readonly QueryErrorCode[];
const BROWSER_WORKER_EVENT_PHASES = [
  'instantiate',
  'open',
  'inspect',
  'query',
] as const satisfies readonly BrowserWorkerEventPhase[];
const BROWSER_WORKER_PROGRESS_STAGES = [
  'started',
  'planning',
  'executing',
  'arrow_ipc_ready',
  'finished',
] as const satisfies readonly BrowserWorkerProgressStage[];
const BROWSER_WORKER_LOG_LEVELS = [
  'debug',
  'info',
  'warn',
  'error',
] as const satisfies readonly BrowserWorkerLogLevel[];
const BROWSER_ACCESS_MODES = [
  'browser_safe_http',
  'cloud_object_store',
] as const satisfies readonly BrowserAccessMode[];

const WASM_SIMD_DETECTION_MODULE = new Uint8Array([
  0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7b, 0x03,
  0x02, 0x01, 0x00, 0x0a, 0x0a, 0x01, 0x08, 0x00, 0x41, 0x00, 0xfd, 0x0f, 0xfd, 0x62, 0x0b,
]);
