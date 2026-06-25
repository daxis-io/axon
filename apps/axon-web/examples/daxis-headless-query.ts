import {
  AxonWorkerError,
  redactUrlSecrets,
  type ArrowIpcResult,
  type AxonBrowserClient,
  type BrowserAccessMode,
  type BrowserHttpSnapshotDescriptor,
  type BrowserHttpFileDescriptor,
  type CapabilityKey,
  type CapabilityReport,
  type CapabilityState,
  type PartitionColumnType,
  type QueryMetricsSummary,
  type QueryRequest,
} from '../src/axon-browser-sdk';

const DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION = 'daxis.approved_axon_read.v1';
const DAXIS_QUERY_RESULT_SCHEMA_VERSION = 'daxis.query_result.v1';
const DAXIS_APPROVED_AXON_READ_DESCRIPTOR_FIELDS = new Set([
  'schema_version',
  'request_id',
  'correlation_id',
  'query_id',
  'execution_id',
  'workspace_id',
  'surface_kind',
  'intent_kind',
  'input_artifact_kind',
  'compiled_artifact_kind',
  'validated_sql',
  'tables',
  'access_proof',
  'limits',
  'runtime_preference',
]);
const DAXIS_VALIDATED_SQL_FIELDS = new Set([
  'sql',
  'dialect',
  'fingerprint',
  'validation_id',
  'read_only',
]);
const DAXIS_TABLE_DESCRIPTOR_FIELDS = new Set([
  'catalog_id',
  'table_id',
  'descriptor_id',
  'table_uri',
  'full_name',
  'snapshot_version',
  'schema_hash',
  'descriptor_schema_hash',
  'descriptor',
]);
const DAXIS_ACCESS_PROOF_FIELDS = new Set([
  'policy_decision_id',
  'read_access_decision',
  'expires_at_epoch_ms',
]);
const DAXIS_LIMIT_FIELDS = new Set([
  'max_result_rows',
  'max_arrow_ipc_bytes',
  'max_scan_bytes',
  'timeout_ms',
  'cancellation_deadline_epoch_ms',
]);
const DAXIS_RUNTIME_PREFERENCE_FIELDS = new Set(['preferred_engine', 'allow_remote_fallback']);
const BROWSER_HTTP_SNAPSHOT_DESCRIPTOR_FIELDS = new Set([
  'table_uri',
  'snapshot_version',
  'partition_column_types',
  'browser_compatibility',
  'required_capabilities',
  'active_files',
]);
const BROWSER_HTTP_FILE_DESCRIPTOR_FIELDS = new Set([
  'path',
  'url',
  'size_bytes',
  'partition_values',
  'stats',
]);
const CAPABILITY_REPORT_FIELDS = new Set(['capabilities']);
const PARTITION_COLUMN_TYPES = new Set(['string', 'int64', 'boolean', 'unsupported']);
const CAPABILITY_KEYS = new Set([
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
]);
const CAPABILITY_STATES = new Set(['supported', 'native_only', 'unsupported', 'experimental']);

export type DaxisSurfaceKind = 'agent' | 'dashboard_tile' | 'builder' | 'saved_query' | 'api';
export type DaxisIntentKind = 'sql' | 'semantic_query';
export type DaxisInputArtifactKind = 'raw_sql' | 'saved_sql' | 'builder_plan' | 'semantic_plan';
export type DaxisCompiledArtifactKind = 'validated_sql';
export type DaxisQueryStatus = 'executed' | 'rejected' | 'fallback' | 'failed' | 'cancelled';
export type DaxisExecutionEngine = 'axon_browser' | 'remote_worker';
export type DaxisResultTransport = 'arrow_ipc';

export type DaxisFallbackReason =
  | 'unsupported_sql'
  | 'remote_required'
  | 'estimates_over_budget'
  | 'browser_disabled'
  | 'descriptor_failure'
  | 'worker_unavailable'
  | 'worker_version_mismatch'
  | 'cancellation'
  | 'runtime_budget_overflow';

export type DaxisBlockReason =
  | 'missing_access_plan'
  | 'policy_denied'
  | 'non_read_only_sql'
  | 'unvalidated_sql'
  | 'invalid_catalog_reference'
  | 'expired_descriptor'
  | 'unsupported_surface'
  | 'unknown';

export type DaxisValidatedSql = {
  sql: string;
  dialect: 'daxis_sql_v1';
  fingerprint: string;
  validation_id: string;
  read_only: boolean;
};

export type DaxisApprovedTableDescriptor = {
  catalog_id: string;
  table_id: string;
  descriptor_id: string;
  table_uri: string;
  full_name: string;
  snapshot_version: number;
  schema_hash?: string;
  descriptor_schema_hash?: string;
  descriptor: BrowserHttpSnapshotDescriptor;
};

export type DaxisAccessProof = {
  policy_decision_id: string;
  read_access_decision: 'approved';
  expires_at_epoch_ms: number;
};

export type DaxisRuntimeLimits = {
  max_result_rows: number;
  max_arrow_ipc_bytes: number;
  max_scan_bytes: number;
  timeout_ms: number;
  cancellation_deadline_epoch_ms: number;
};

export type DaxisRuntimePreference = {
  preferred_engine: 'axon_browser';
  allow_remote_fallback: boolean;
};

const DAXIS_SURFACE_KINDS = new Set<DaxisSurfaceKind>([
  'agent',
  'dashboard_tile',
  'builder',
  'saved_query',
  'api',
]);
const DAXIS_INTENT_KINDS = new Set<DaxisIntentKind>(['sql', 'semantic_query']);
const DAXIS_INPUT_ARTIFACT_KINDS = new Set<DaxisInputArtifactKind>([
  'raw_sql',
  'saved_sql',
  'builder_plan',
  'semantic_plan',
]);
const DAXIS_COMPILED_ARTIFACT_KINDS = new Set<DaxisCompiledArtifactKind>(['validated_sql']);
const DAXIS_SURFACE_METADATA = {
  agent: {
    intentKind: 'semantic_query',
    inputArtifactKind: 'semantic_plan',
  },
  dashboard_tile: {
    intentKind: 'sql',
    inputArtifactKind: 'saved_sql',
  },
  builder: {
    intentKind: 'semantic_query',
    inputArtifactKind: 'builder_plan',
  },
  saved_query: {
    intentKind: 'sql',
    inputArtifactKind: 'saved_sql',
  },
  api: {
    intentKind: 'sql',
    inputArtifactKind: 'raw_sql',
  },
} satisfies Record<
  DaxisSurfaceKind,
  {
    intentKind: DaxisIntentKind;
    inputArtifactKind: DaxisInputArtifactKind;
  }
>;

export type DaxisApprovedAxonReadDescriptor = {
  schema_version: 'daxis.approved_axon_read.v1';
  request_id: string;
  correlation_id: string;
  query_id: string;
  execution_id: string;
  workspace_id: string;
  surface_kind: DaxisSurfaceKind;
  intent_kind: DaxisIntentKind;
  input_artifact_kind: DaxisInputArtifactKind;
  compiled_artifact_kind: DaxisCompiledArtifactKind;
  validated_sql: DaxisValidatedSql;
  tables: [DaxisApprovedTableDescriptor];
  access_proof: DaxisAccessProof;
  limits: DaxisRuntimeLimits;
  runtime_preference: DaxisRuntimePreference;
};

export type DaxisResultMetrics = {
  rows_returned?: number;
  arrow_ipc_bytes?: number;
  scan_bytes?: number;
  duration_ms?: number;
  files_touched?: number;
  files_skipped?: number;
  prebootstrap_fail_open_count?: number;
  prebootstrap_files_pruned?: number;
  footer_reads_avoided?: number;
  prebootstrap_candidate_files?: number;
  row_groups_touched?: number;
  row_groups_skipped?: number;
  footer_reads?: number;
  snapshot_bootstrap_duration_ms?: number;
  access_mode?: BrowserAccessMode;
};

export type DaxisResultEnvelope = {
  schema_version: 'daxis.query_result.v1';
  status: DaxisQueryStatus;
  execution_engine: DaxisExecutionEngine | null;
  result_transport: DaxisResultTransport | null;
  fallback_reason: DaxisFallbackReason | null;
  block_reason: DaxisBlockReason | null;
  surface_kind: DaxisSurfaceKind;
  intent_kind: DaxisIntentKind;
  input_artifact_kind: DaxisInputArtifactKind;
  compiled_artifact_kind: DaxisCompiledArtifactKind;
  request_id: string;
  correlation_id: string;
  query_id: string;
  execution_id: string;
  workspace_id: string;
  metrics: DaxisResultMetrics;
  diagnostics: Record<string, string>;
};

export type DaxisHeadlessQueryResult = {
  envelope: DaxisResultEnvelope;
  arrowIpc: ArrowIpcResult | null;
};

export type DaxisHeadlessQueryOptions = {
  tableName?: string;
  nowEpochMs?: number;
  signal?: AbortSignal;
  workerArtifactId?: string;
  terminateClientOnTimeout?: boolean;
};

type DaxisHeadlessQueryStage = 'open' | 'query';

export class DaxisHeadlessQueryError extends Error {
  readonly name = 'DaxisHeadlessQueryError';
  readonly code: DaxisBlockReason | DaxisFallbackReason | 'invalid_descriptor';

  constructor(code: DaxisHeadlessQueryError['code'], message: string) {
    super(redactUrlSecrets(message));
    this.code = code;
  }
}

class DaxisHeadlessQueryTimeout extends Error {
  readonly stage: DaxisHeadlessQueryStage;

  constructor(stage: DaxisHeadlessQueryStage) {
    super(`Daxis-approved Axon read timed out during ${stage}`);
    this.stage = stage;
  }
}

export async function executeDaxisApprovedAxonRead(
  client: AxonBrowserClient,
  input: DaxisApprovedAxonReadDescriptor,
  options: DaxisHeadlessQueryOptions = {},
): Promise<DaxisHeadlessQueryResult> {
  const nowEpochMs = options.nowEpochMs ?? Date.now();
  const descriptor = validateDaxisApprovedAxonReadDescriptor(input, nowEpochMs);

  if (options.signal?.aborted) {
    return {
      envelope: terminalEnvelope(descriptor, {
        status: 'cancelled',
        diagnostics: cancellationDiagnostics(options.signal),
      }),
      arrowIpc: null,
    };
  }

  const table = descriptor.tables[0];
  const tableName = options.tableName ?? table.full_name;

  try {
    await withDaxisTimeout(
      client.openDeltaTable(tableName, table.descriptor, {
        requestId: descriptor.request_id,
      }),
      descriptor,
      'open',
    );

    const result = await withDaxisTimeout(
      client.query(tableName, queryRequestFromDaxisDescriptor(descriptor, table), {
        requestId: descriptor.execution_id,
      }),
      descriptor,
      'query',
    );
    const budgetOverflowDiagnostics = resultBudgetOverflowDiagnostics(
      descriptor,
      result.response.metrics,
      result.result,
    );
    if (budgetOverflowDiagnostics) {
      return {
        envelope: terminalEnvelope(descriptor, {
          status: 'fallback',
          fallbackReason: 'runtime_budget_overflow',
          metrics: daxisMetricsFromQueryResult(result.response.metrics, result.result),
          diagnostics: budgetOverflowDiagnostics,
        }),
        arrowIpc: null,
      };
    }

    return {
      envelope: executedEnvelope(descriptor, result.response.metrics, result.result, {
        workerArtifactId: options.workerArtifactId,
      }),
      arrowIpc: result.result,
    };
  } catch (error) {
    if (error instanceof DaxisHeadlessQueryTimeout) {
      const diagnostics = timeoutDiagnostics(descriptor, error.stage);
      if (error.stage === 'query') {
        cancelTimedOutQuery(client, descriptor, diagnostics);
      }
      if (options.terminateClientOnTimeout ?? true) {
        terminateTimedOutClient(client, diagnostics);
      }
      return {
        envelope: terminalEnvelope(descriptor, {
          status: 'fallback',
          fallbackReason: 'runtime_budget_overflow',
          diagnostics,
        }),
        arrowIpc: null,
      };
    }

    if (error instanceof AxonWorkerError && error.fallbackReason) {
      return {
        envelope: terminalEnvelope(descriptor, {
          status: 'fallback',
          fallbackReason: mapAxonFallbackReason(error),
          diagnostics: {
            sql_fingerprint: descriptor.validated_sql.fingerprint,
            worker_error_code: error.queryError.code,
          },
        }),
        arrowIpc: null,
      };
    }

    return {
      envelope: terminalEnvelope(descriptor, {
        status: 'failed',
        diagnostics: {
          sql_fingerprint: descriptor.validated_sql.fingerprint,
          error: error instanceof Error ? redactUrlSecrets(error.message) : String(error),
        },
      }),
      arrowIpc: null,
    };
  }
}

async function withDaxisTimeout<T>(
  operation: Promise<T>,
  descriptor: DaxisApprovedAxonReadDescriptor,
  stage: DaxisHeadlessQueryStage,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<never>((_, reject) => {
    timeout = setTimeout(
      () => reject(new DaxisHeadlessQueryTimeout(stage)),
      descriptor.limits.timeout_ms,
    );
  });

  try {
    return await Promise.race([operation, timedOut]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

export function validateDaxisApprovedAxonReadDescriptor(
  descriptor: DaxisApprovedAxonReadDescriptor,
  nowEpochMs: number = Date.now(),
): DaxisApprovedAxonReadDescriptor {
  requireKnownKeys(
    descriptor,
    'Daxis-approved descriptor',
    DAXIS_APPROVED_AXON_READ_DESCRIPTOR_FIELDS,
  );
  requireKnownKeys(descriptor.validated_sql, 'validated_sql', DAXIS_VALIDATED_SQL_FIELDS);
  requireKnownKeys(descriptor.access_proof, 'access_proof', DAXIS_ACCESS_PROOF_FIELDS);
  requireKnownKeys(descriptor.limits, 'limits', DAXIS_LIMIT_FIELDS);
  requireKnownKeys(
    descriptor.runtime_preference,
    'runtime_preference',
    DAXIS_RUNTIME_PREFERENCE_FIELDS,
  );

  if (descriptor.schema_version !== DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor schema_version must be ${DAXIS_APPROVED_AXON_READ_SCHEMA_VERSION}`,
    );
  }

  for (const [field, value] of [
    ['request_id', descriptor.request_id],
    ['correlation_id', descriptor.correlation_id],
    ['query_id', descriptor.query_id],
    ['execution_id', descriptor.execution_id],
    ['workspace_id', descriptor.workspace_id],
    ['validated_sql.sql', descriptor.validated_sql.sql],
    ['validated_sql.fingerprint', descriptor.validated_sql.fingerprint],
    ['validated_sql.validation_id', descriptor.validated_sql.validation_id],
    ['access_proof.policy_decision_id', descriptor.access_proof.policy_decision_id],
  ] as const) {
    requireNonEmpty(value, field);
  }

  if (descriptor.validated_sql.dialect !== 'daxis_sql_v1') {
    throw new DaxisHeadlessQueryError(
      'unvalidated_sql',
      'Daxis-approved descriptor validated_sql.dialect must be daxis_sql_v1',
    );
  }
  if (descriptor.validated_sql.read_only !== true) {
    throw new DaxisHeadlessQueryError(
      'non_read_only_sql',
      'Daxis-approved descriptor requires validated_sql.read_only=true',
    );
  }
  if (descriptor.access_proof.read_access_decision !== 'approved') {
    throw new DaxisHeadlessQueryError(
      'policy_denied',
      'Daxis-approved descriptor access_proof.read_access_decision must be approved',
    );
  }
  if (descriptor.access_proof.expires_at_epoch_ms <= nowEpochMs) {
    throw new DaxisHeadlessQueryError(
      'expired_descriptor',
      'Daxis-approved descriptor access proof has expired',
    );
  }
  if (descriptor.runtime_preference.preferred_engine !== 'axon_browser') {
    throw new DaxisHeadlessQueryError(
      'remote_required',
      'Daxis-approved descriptor did not select axon_browser',
    );
  }
  if (!Array.isArray(descriptor.tables) || descriptor.tables.length !== 1) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      'Daxis-approved descriptor v1 requires exactly one table descriptor',
    );
  }
  const surfaceKind = requireAllowedValue(
    descriptor.surface_kind,
    'surface_kind',
    DAXIS_SURFACE_KINDS,
    'unsupported_surface',
  );
  const intentKind = requireAllowedValue(
    descriptor.intent_kind,
    'intent_kind',
    DAXIS_INTENT_KINDS,
    'invalid_descriptor',
  );
  const inputArtifactKind = requireAllowedValue(
    descriptor.input_artifact_kind,
    'input_artifact_kind',
    DAXIS_INPUT_ARTIFACT_KINDS,
    'invalid_descriptor',
  );
  const compiledArtifactKind = requireAllowedValue(
    descriptor.compiled_artifact_kind,
    'compiled_artifact_kind',
    DAXIS_COMPILED_ARTIFACT_KINDS,
    'invalid_descriptor',
  );
  validateSurfaceMetadata(surfaceKind, intentKind, inputArtifactKind);
  validateLimits(descriptor.limits, nowEpochMs);
  const table = validateApprovedTableDescriptor(descriptor.tables[0]);
  return {
    schema_version: descriptor.schema_version,
    request_id: descriptor.request_id,
    correlation_id: descriptor.correlation_id,
    query_id: descriptor.query_id,
    execution_id: descriptor.execution_id,
    workspace_id: descriptor.workspace_id,
    surface_kind: surfaceKind,
    intent_kind: intentKind,
    input_artifact_kind: inputArtifactKind,
    compiled_artifact_kind: compiledArtifactKind,
    validated_sql: { ...descriptor.validated_sql },
    tables: [table],
    access_proof: { ...descriptor.access_proof },
    limits: { ...descriptor.limits },
    runtime_preference: { ...descriptor.runtime_preference },
  };
}

function queryRequestFromDaxisDescriptor(
  descriptor: DaxisApprovedAxonReadDescriptor,
  table: DaxisApprovedTableDescriptor,
): QueryRequest {
  return {
    table_uri: table.table_uri,
    snapshot_version: table.snapshot_version,
    sql: descriptor.validated_sql.sql,
    preferred_target: 'browser_wasm',
    options: {
      include_explain: false,
      collect_metrics: true,
      result_page: {
        limit: descriptor.limits.max_result_rows,
        offset: 0,
      },
      runtime_limits: {
        max_result_rows: descriptor.limits.max_result_rows,
        max_arrow_ipc_bytes: descriptor.limits.max_arrow_ipc_bytes,
        max_scan_bytes: descriptor.limits.max_scan_bytes,
      },
    },
  };
}

function executedEnvelope(
  descriptor: DaxisApprovedAxonReadDescriptor,
  metrics: QueryMetricsSummary,
  arrowIpc: ArrowIpcResult,
  options: { workerArtifactId?: string },
): DaxisResultEnvelope {
  return terminalEnvelope(descriptor, {
    status: 'executed',
    executionEngine: 'axon_browser',
    resultTransport: 'arrow_ipc',
    metrics: daxisMetricsFromQueryResult(metrics, arrowIpc),
    diagnostics: {
      ...(options.workerArtifactId ? { worker_artifact_id: options.workerArtifactId } : {}),
      sql_fingerprint: descriptor.validated_sql.fingerprint,
    },
  });
}

function daxisMetricsFromQueryResult(
  metrics: QueryMetricsSummary,
  arrowIpc: ArrowIpcResult,
): DaxisResultMetrics {
  return {
    rows_returned: metrics.rows_emitted,
    arrow_ipc_bytes: arrowIpc.bytes.byteLength,
    scan_bytes: metrics.bytes_fetched,
    duration_ms: metrics.duration_ms,
    files_touched: metrics.files_touched,
    files_skipped: metrics.files_skipped,
    ...(metrics.prebootstrap_fail_open_count !== undefined
      ? { prebootstrap_fail_open_count: metrics.prebootstrap_fail_open_count }
      : {}),
    ...(metrics.prebootstrap_files_pruned !== undefined
      ? { prebootstrap_files_pruned: metrics.prebootstrap_files_pruned }
      : {}),
    ...(metrics.footer_reads_avoided !== undefined
      ? { footer_reads_avoided: metrics.footer_reads_avoided }
      : {}),
    ...(metrics.prebootstrap_candidate_files !== undefined
      ? { prebootstrap_candidate_files: metrics.prebootstrap_candidate_files }
      : {}),
    ...(metrics.row_groups_touched !== undefined
      ? { row_groups_touched: metrics.row_groups_touched }
      : {}),
    ...(metrics.row_groups_skipped !== undefined
      ? { row_groups_skipped: metrics.row_groups_skipped }
      : {}),
    ...(metrics.footer_reads !== undefined ? { footer_reads: metrics.footer_reads } : {}),
    ...(metrics.snapshot_bootstrap_duration_ms !== undefined
      ? { snapshot_bootstrap_duration_ms: metrics.snapshot_bootstrap_duration_ms }
      : {}),
    ...(metrics.access_mode !== undefined ? { access_mode: metrics.access_mode } : {}),
  };
}

function resultBudgetOverflowDiagnostics(
  descriptor: DaxisApprovedAxonReadDescriptor,
  metrics: QueryMetricsSummary,
  arrowIpc: ArrowIpcResult,
): Record<string, string> | null {
  const rowsEmitted = metrics.rows_emitted ?? 0;
  if (rowsEmitted > descriptor.limits.max_result_rows) {
    return {
      sql_fingerprint: descriptor.validated_sql.fingerprint,
      budget_field: 'max_result_rows',
      actual_rows: String(rowsEmitted),
      limit_rows: String(descriptor.limits.max_result_rows),
    };
  }
  if (metrics.bytes_fetched > descriptor.limits.max_scan_bytes) {
    return {
      sql_fingerprint: descriptor.validated_sql.fingerprint,
      budget_field: 'max_scan_bytes',
      actual_bytes: String(metrics.bytes_fetched),
      limit_bytes: String(descriptor.limits.max_scan_bytes),
    };
  }
  if (arrowIpc.bytes.byteLength > descriptor.limits.max_arrow_ipc_bytes) {
    return {
      sql_fingerprint: descriptor.validated_sql.fingerprint,
      budget_field: 'max_arrow_ipc_bytes',
      actual_bytes: String(arrowIpc.bytes.byteLength),
      limit_bytes: String(descriptor.limits.max_arrow_ipc_bytes),
    };
  }
  return null;
}

function timeoutDiagnostics(
  descriptor: DaxisApprovedAxonReadDescriptor,
  stage: DaxisHeadlessQueryStage,
): Record<string, string> {
  return {
    sql_fingerprint: descriptor.validated_sql.fingerprint,
    budget_field: 'timeout_ms',
    limit_ms: String(descriptor.limits.timeout_ms),
    stage,
  };
}

function cancelTimedOutQuery(
  client: AxonBrowserClient,
  descriptor: DaxisApprovedAxonReadDescriptor,
  diagnostics: Record<string, string>,
): void {
  const cancelRequestId = `cancel-${descriptor.execution_id}`;
  diagnostics.cancel_request_id = cancelRequestId;
  try {
    client.cancelQuery(descriptor.execution_id, { requestId: cancelRequestId });
  } catch (error) {
    diagnostics.cancel_error =
      error instanceof Error ? redactUrlSecrets(error.message) : String(error);
  }
}

function terminateTimedOutClient(
  client: AxonBrowserClient,
  diagnostics: Record<string, string>,
): void {
  diagnostics.client_terminated = 'true';
  try {
    client.terminate();
  } catch (error) {
    diagnostics.terminate_error =
      error instanceof Error ? redactUrlSecrets(error.message) : String(error);
  }
}

function terminalEnvelope(
  descriptor: DaxisApprovedAxonReadDescriptor,
  options: {
    status: DaxisQueryStatus;
    executionEngine?: DaxisExecutionEngine | null;
    resultTransport?: DaxisResultTransport | null;
    fallbackReason?: DaxisFallbackReason | null;
    blockReason?: DaxisBlockReason | null;
    metrics?: DaxisResultMetrics;
    diagnostics?: Record<string, string>;
  },
): DaxisResultEnvelope {
  return {
    schema_version: DAXIS_QUERY_RESULT_SCHEMA_VERSION,
    status: options.status,
    execution_engine: options.executionEngine ?? null,
    result_transport: options.resultTransport ?? null,
    fallback_reason: options.fallbackReason ?? null,
    block_reason: options.blockReason ?? null,
    surface_kind: descriptor.surface_kind,
    intent_kind: descriptor.intent_kind,
    input_artifact_kind: descriptor.input_artifact_kind,
    compiled_artifact_kind: descriptor.compiled_artifact_kind,
    request_id: descriptor.request_id,
    correlation_id: descriptor.correlation_id,
    query_id: descriptor.query_id,
    execution_id: descriptor.execution_id,
    workspace_id: descriptor.workspace_id,
    metrics: options.metrics ?? {},
    diagnostics: options.diagnostics ?? {},
  };
}

function validateApprovedTableDescriptor(
  table: DaxisApprovedTableDescriptor,
): DaxisApprovedTableDescriptor {
  requireKnownKeys(table, 'tables[]', DAXIS_TABLE_DESCRIPTOR_FIELDS);
  for (const [field, value] of [
    ['tables[].catalog_id', table.catalog_id],
    ['tables[].table_id', table.table_id],
    ['tables[].descriptor_id', table.descriptor_id],
    ['tables[].table_uri', table.table_uri],
    ['tables[].full_name', table.full_name],
  ] as const) {
    requireNonEmpty(value, field);
  }
  if (!Number.isInteger(table.snapshot_version) || table.snapshot_version < 0) {
    throw new DaxisHeadlessQueryError(
      'invalid_catalog_reference',
      'Daxis-approved descriptor table snapshot_version must be a non-negative integer',
    );
  }
  const descriptor = normalizeBrowserHttpSnapshotDescriptor(table.descriptor);
  if (descriptor.table_uri !== table.table_uri) {
    throw new DaxisHeadlessQueryError(
      'invalid_catalog_reference',
      'Daxis-approved descriptor table_uri must match descriptor.table_uri',
    );
  }
  if (descriptor.snapshot_version !== table.snapshot_version) {
    throw new DaxisHeadlessQueryError(
      'invalid_catalog_reference',
      'Daxis-approved descriptor snapshot_version must match descriptor.snapshot_version',
    );
  }
  if (
    table.schema_hash !== undefined &&
    table.descriptor_schema_hash !== undefined &&
    table.schema_hash !== table.descriptor_schema_hash
  ) {
    throw new DaxisHeadlessQueryError(
      'invalid_catalog_reference',
      'Daxis-approved descriptor schema_hash must match descriptor_schema_hash',
    );
  }
  return {
    catalog_id: table.catalog_id,
    table_id: table.table_id,
    descriptor_id: table.descriptor_id,
    table_uri: table.table_uri,
    full_name: table.full_name,
    snapshot_version: table.snapshot_version,
    ...(table.schema_hash !== undefined ? { schema_hash: table.schema_hash } : {}),
    ...(table.descriptor_schema_hash !== undefined
      ? { descriptor_schema_hash: table.descriptor_schema_hash }
      : {}),
    descriptor,
  };
}

function validateLimits(limits: DaxisRuntimeLimits, nowEpochMs: number): void {
  for (const [field, value] of [
    ['limits.max_result_rows', limits.max_result_rows],
    ['limits.max_arrow_ipc_bytes', limits.max_arrow_ipc_bytes],
    ['limits.max_scan_bytes', limits.max_scan_bytes],
    ['limits.timeout_ms', limits.timeout_ms],
  ] as const) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new DaxisHeadlessQueryError(
        'runtime_budget_overflow',
        `Daxis-approved descriptor ${field} must be a positive integer`,
      );
    }
  }
  if (limits.cancellation_deadline_epoch_ms <= nowEpochMs) {
    throw new DaxisHeadlessQueryError(
      'cancellation',
      'Daxis-approved descriptor cancellation deadline has expired',
    );
  }
}

function requireNonEmpty(value: string, field: string): void {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must not be empty`,
    );
  }
}

function requireAllowedValue<T extends string>(
  value: unknown,
  field: string,
  allowedValues: ReadonlySet<T>,
  code: DaxisHeadlessQueryError['code'],
): T {
  if (typeof value !== 'string' || !allowedValues.has(value as T)) {
    throw new DaxisHeadlessQueryError(
      code,
      `Daxis-approved descriptor ${field} must be one of ${Array.from(allowedValues).join(', ')}`,
    );
  }
  return value as T;
}

function validateSurfaceMetadata(
  surfaceKind: DaxisSurfaceKind,
  intentKind: DaxisIntentKind,
  inputArtifactKind: DaxisInputArtifactKind,
): void {
  const expected = DAXIS_SURFACE_METADATA[surfaceKind];
  if (intentKind !== expected.intentKind || inputArtifactKind !== expected.inputArtifactKind) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor surface ${surfaceKind} must use intent_kind=${expected.intentKind} and input_artifact_kind=${expected.inputArtifactKind}`,
    );
  }
}

function validateHttpsUrl(value: string, field: string): void {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch (error) {
    throw new DaxisHeadlessQueryError(
      'descriptor_failure',
      `${field} must be a valid URL: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (parsed.protocol !== 'https:') {
    throw new DaxisHeadlessQueryError('descriptor_failure', `${field} must use HTTPS`);
  }
}

function normalizeBrowserHttpSnapshotDescriptor(
  descriptor: unknown,
): BrowserHttpSnapshotDescriptor {
  const record = requireRecord(descriptor, 'tables[].descriptor');
  requireKnownKeys(record, 'tables[].descriptor', BROWSER_HTTP_SNAPSHOT_DESCRIPTOR_FIELDS);
  const activeFiles = requireArray(record.active_files, 'tables[].descriptor.active_files');

  return {
    table_uri: requireStringValue(record.table_uri, 'tables[].descriptor.table_uri'),
    snapshot_version: requireNonNegativeInteger(
      record.snapshot_version,
      'tables[].descriptor.snapshot_version',
    ),
    ...(record.partition_column_types !== undefined
      ? {
          partition_column_types: normalizePartitionColumnTypes(
            record.partition_column_types,
            'tables[].descriptor.partition_column_types',
          ),
        }
      : {}),
    ...(record.browser_compatibility !== undefined
      ? {
          browser_compatibility: normalizeCapabilityReport(
            record.browser_compatibility,
            'tables[].descriptor.browser_compatibility',
          ),
        }
      : {}),
    ...(record.required_capabilities !== undefined
      ? {
          required_capabilities: normalizeCapabilityReport(
            record.required_capabilities,
            'tables[].descriptor.required_capabilities',
          ),
        }
      : {}),
    active_files: activeFiles.map((file, index) => normalizeBrowserHttpFileDescriptor(file, index)),
  };
}

function normalizeBrowserHttpFileDescriptor(
  file: unknown,
  index: number,
): BrowserHttpFileDescriptor {
  const record = requireRecord(file, `tables[].descriptor.active_files[${index}]`);
  requireKnownKeys(
    record,
    `tables[].descriptor.active_files[${index}]`,
    BROWSER_HTTP_FILE_DESCRIPTOR_FIELDS,
  );
  const path = requireStringValue(record.path, `tables[].descriptor.active_files[${index}].path`);
  const url = requireStringValue(record.url, `tables[].descriptor.active_files[${index}].url`);
  validateHttpsUrl(url, `tables[].descriptor.active_files[${index}].url`);
  const sizeBytes = requireNonNegativeInteger(
    record.size_bytes,
    `tables[].descriptor.active_files[${index}].size_bytes`,
  );
  const partitionValues = normalizePartitionValues(
    record.partition_values,
    `tables[].descriptor.active_files[${index}].partition_values`,
  );
  const stats = optionalString(record.stats, `tables[].descriptor.active_files[${index}].stats`);
  return {
    path,
    url,
    size_bytes: sizeBytes,
    partition_values: partitionValues,
    ...(stats !== undefined ? { stats } : {}),
  };
}

function normalizePartitionColumnTypes(
  value: unknown,
  field: string,
): Partial<Record<string, PartitionColumnType>> {
  const record = requireRecord(value, field);
  const normalized: Partial<Record<string, PartitionColumnType>> = {};
  for (const [key, entry] of Object.entries(record)) {
    normalized[key] = requireEnum(
      entry,
      `${field}.${key}`,
      PARTITION_COLUMN_TYPES,
    ) as PartitionColumnType;
  }
  return normalized;
}

function normalizeCapabilityReport(value: unknown, field: string): CapabilityReport {
  const report = requireRecord(value, field);
  requireKnownKeys(report, field, CAPABILITY_REPORT_FIELDS);
  const capabilities = requireRecord(report.capabilities, `${field}.capabilities`);
  const normalized: Partial<Record<CapabilityKey, CapabilityState>> = {};
  for (const [key, entry] of Object.entries(capabilities)) {
    if (!CAPABILITY_KEYS.has(key)) {
      throw new DaxisHeadlessQueryError(
        'invalid_descriptor',
        `Daxis-approved descriptor ${field}.capabilities.${key} must be a known capability key`,
      );
    }
    normalized[key as CapabilityKey] = requireEnum(
      entry,
      `${field}.capabilities.${key}`,
      CAPABILITY_STATES,
    ) as CapabilityState;
  }
  return { capabilities: normalized };
}

function normalizePartitionValues(value: unknown, field: string): Record<string, string | null> {
  const record = requireRecord(value, field);
  const normalized: Record<string, string | null> = {};
  for (const [key, entry] of Object.entries(record)) {
    if (entry !== null && typeof entry !== 'string') {
      throw new DaxisHeadlessQueryError(
        'invalid_descriptor',
        `Daxis-approved descriptor ${field}.${key} must be a string or null`,
      );
    }
    normalized[key] = entry;
  }
  return normalized;
}

function requireKnownKeys(value: unknown, field: string, allowed: Set<string>): void {
  const record = requireRecord(value, field);
  for (const key of Object.keys(record)) {
    if (!allowed.has(key)) {
      throw new DaxisHeadlessQueryError(
        'invalid_descriptor',
        `Daxis-approved descriptor ${field} includes unsupported field '${key}'`,
      );
    }
  }
}

function requireArray(value: unknown, field: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must be an array`,
    );
  }
  return value;
}

function requireRecord(value: unknown, field: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must be an object`,
    );
  }
  return value as Record<string, unknown>;
}

function requireStringValue(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must not be empty`,
    );
  }
  return value;
}

function optionalString(value: unknown, field: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must be a string`,
    );
  }
  return value;
}

function requireNonNegativeInteger(value: unknown, field: string): number {
  if (!Number.isInteger(value) || (value as number) < 0) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must be a non-negative integer`,
    );
  }
  return value as number;
}

function requireEnum(value: unknown, field: string, allowed: Set<string>): string {
  if (typeof value !== 'string' || !allowed.has(value)) {
    throw new DaxisHeadlessQueryError(
      'invalid_descriptor',
      `Daxis-approved descriptor ${field} must be one of ${Array.from(allowed).join(', ')}`,
    );
  }
  return value;
}

function mapAxonFallbackReason(error: AxonWorkerError): DaxisFallbackReason {
  switch (error.queryError.fallback_reason) {
    case 'browser_runtime_constraint':
      return 'runtime_budget_overflow';
    case 'network_failure':
      return 'worker_unavailable';
    case 'native_required':
      return 'remote_required';
    default:
      return 'descriptor_failure';
  }
}

function cancellationDiagnostics(signal: AbortSignal): Record<string, string> {
  if (signal.reason === undefined) {
    return {};
  }
  return {
    cancellation_reason:
      signal.reason instanceof Error
        ? redactUrlSecrets(signal.reason.message)
        : String(signal.reason),
  };
}
