export const PREFERRED_TARGET: ExecutionTarget = 'browser_wasm';
export const ARROW_IPC_STREAM_CONTENT_TYPE = 'application/vnd.apache.arrow.stream';
export const ARROW_IPC_FILE_CONTENT_TYPE = 'application/vnd.apache.arrow.file';
export const BROWSER_SAFE_RESULT_ROW_LIMIT = 501;
export const BROWSER_SAFE_ARROW_IPC_BYTES = 8 * 1024 * 1024;
export const BROWSER_SAFE_PREVIEW_STRING_BYTES = 256 * 1024;

export function redactUrlSecrets(message: string): string {
  return redactCredentialSecrets(
    message.replace(/https?:\/\/[^\s'"<>]+/g, (candidate) => {
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
    }),
  );
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
  | 'object_not_found'
  | 'object_store_protocol'
  | 'security_policy_violation'
  | 'unsupported_feature';

export type QueryError = {
  code: QueryErrorCode;
  message: string;
  target: ExecutionTarget;
  fallback_reason?: FallbackReason;
};

export type QueryResultPageRequest = {
  limit: number;
  offset: number;
};

export type QueryExecutionOptions = {
  include_explain?: boolean;
  collect_metrics?: boolean;
  result_page?: QueryResultPageRequest;
  runtime_limits?: QueryRuntimeLimits;
};

export type QueryRuntimeLimits = {
  max_result_rows?: number;
  max_arrow_ipc_bytes?: number;
  max_preview_string_bytes?: number;
  max_scan_bytes?: number;
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
  object_etag?: string;
};

export type BrowserHttpSnapshotDescriptor = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  browser_compatibility?: CapabilityReport;
  required_capabilities?: CapabilityReport;
  active_files: BrowserHttpFileDescriptor[];
};

export type BrowserHttpParquetDatasetDescriptor = {
  table_uri: string;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  browser_compatibility?: CapabilityReport;
  required_capabilities?: CapabilityReport;
  files: BrowserHttpFileDescriptor[];
};

export type DeltaSharingProfileSource = 'uploaded_profile' | 'oidc_profile_url';
export type DeltaSharingAuthMode = 'bearer' | 'oidc';
export type DeltaSharingResponseFormat = 'auto' | 'parquet' | 'delta';
export type DeltaSharingReaderFeature = 'deletionvectors' | 'columnmapping';

export type DeltaSharingConnectionProfile = {
  kind: 'delta_sharing';
  profileSource: DeltaSharingProfileSource;
  endpoint: string;
  authMode: DeltaSharingAuthMode;
  displayName?: string;
};

export type DeltaSharingTableRef = {
  share: string;
  schema: string;
  table: string;
};

export type DeltaSharingWarning = {
  code: string;
  message: string;
};

export type OpenDeltaShareOptions = {
  version?: number;
  limitHint?: number;
  predicateHints?: unknown[];
  responseFormat?: DeltaSharingResponseFormat;
  readerFeatures?: DeltaSharingReaderFeature[];
};

export type DeltaSharingReadPlan = {
  kind: 'delta_sharing_snapshot_descriptor';
  table: DeltaSharingTableRef;
  endpoint: string;
  resolvedVersion: number;
  responseFormat: 'delta' | 'parquet';
  descriptor: BrowserHttpSnapshotDescriptor;
  expiresAtEpochMs?: number;
  warnings?: DeltaSharingWarning[];
};

export type DeltaSharingReadPlanMetadata = Omit<DeltaSharingReadPlan, 'descriptor'>;

export type DeltaSharingOpenedEnvelope = BrowserWorkerOpenedEnvelope & {
  deltaSharing: DeltaSharingReadPlanMetadata;
};

export type DeltaSharingProfileInput = {
  source: 'file' | 'json' | 'oidc_url';
  value: File | string | unknown;
};

export type PageOptions = {
  pageToken?: string;
  maxResults?: number;
};

export type DeltaSharingPage<T> = {
  items: T[];
  nextPageToken?: string;
};

export type DeltaShareInfo = {
  name: string;
  id?: string;
};

export type DeltaShareSchemaInfo = {
  name: string;
  share?: string;
};

export type DeltaShareTableInfo = {
  name: string;
  share?: string;
  schema?: string;
};

export type DeltaSharingTableMetadata = {
  table: DeltaSharingTableRef;
  version?: number;
  protocol?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  rawActions: Array<Record<string, unknown>>;
};

export type DeltaSharingMetadataOptions = {
  version?: number;
};

export type DeltaSharingFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export interface DeltaSharingClient {
  connect(profile: DeltaSharingProfileInput): Promise<DeltaSharingSession>;
}

export interface DeltaSharingSession {
  readonly profile: DeltaSharingConnectionProfile;
  listShares(opts?: PageOptions): Promise<DeltaSharingPage<DeltaShareInfo>>;
  listSchemas(share: string, opts?: PageOptions): Promise<DeltaSharingPage<DeltaShareSchemaInfo>>;
  listTables(
    share: string,
    schema: string,
    opts?: PageOptions,
  ): Promise<DeltaSharingPage<DeltaShareTableInfo>>;
  listAllTables(share: string, opts?: PageOptions): Promise<DeltaSharingPage<DeltaShareTableInfo>>;
  getTableVersion(table: DeltaSharingTableRef): Promise<number>;
  getTableMetadata(
    table: DeltaSharingTableRef,
    opts?: DeltaSharingMetadataOptions,
  ): Promise<DeltaSharingTableMetadata>;
  resolveTable(
    table: DeltaSharingTableRef,
    opts?: OpenDeltaShareOptions,
  ): Promise<DeltaSharingReadPlan>;
}

export type DeltaSharingClientOptions = {
  fetch?: DeltaSharingFetch;
};

export type DeltaSharingOpenOptions = AxonRequestOptions &
  OpenDeltaShareOptions & {
    session: DeltaSharingSession;
    table: DeltaSharingTableRef;
  };

export type DeltaSharingErrorCode =
  | 'invalid_profile'
  | 'profile_contains_sensitive_data_not_persisted'
  | 'auth_required'
  | 'auth_expired'
  | 'share_not_found'
  | 'schema_not_found'
  | 'table_not_found'
  | 'table_version_not_found'
  | 'unsupported_access_mode'
  | 'directory_access_requires_resolver'
  | 'unsupported_response_format'
  | 'unsupported_reader_feature'
  | 'storage_cors_failed'
  | 'signed_url_expired'
  | 'server_error'
  | 'network_error';

export type DeltaObjectStoreProvider = 's3' | 'gcs' | 'azure_blob';
export type ResolverRequestedAccessMode = 'auto' | 'signed_url' | 'proxy';
export type ResolverActualAccessMode = 'signed_url' | 'proxy';

export type CredentialProfileRef = {
  id: string;
  display_name?: string;
};

export type DeltaLocationResolveRequest = {
  provider: DeltaObjectStoreProvider;
  table_uri: string;
  credential_profile: CredentialProfileRef;
  requested_access_mode: ResolverRequestedAccessMode;
  snapshot_version?: number;
};

export type DeltaLocationRefresh = {
  refresh_url?: string;
  refresh_after_epoch_ms?: number;
  same_snapshot_required: true;
};

export type DeltaLocationResolveResponse = {
  descriptor: BrowserHttpSnapshotDescriptor;
  provider: DeltaObjectStoreProvider;
  table_uri: string;
  requested_snapshot_version?: number;
  resolved_snapshot_version: number;
  requested_access_mode?: ResolverRequestedAccessMode;
  actual_access_mode: ResolverActualAccessMode;
  expires_at_epoch_ms: number;
  correlation_id?: string;
  warnings?: string[];
  refresh?: DeltaLocationRefresh;
};

export type DeltaLocationResolverErrorCode =
  | 'invalid_table_uri'
  | 'invalid_snapshot_version'
  | 'credential_profile_required'
  | 'credential_profile_not_found'
  | 'provider_not_supported'
  | 'access_mode_not_supported'
  | 'snapshot_version_not_found'
  | 'not_a_delta_table'
  | 'resolver_unavailable'
  | 'storage_auth_failed'
  | 'storage_cors_failed'
  | 'descriptor_expired'
  | 'policy_blocked'
  | 'proxy_required'
  | 'signed_url_unavailable';

export type DeltaLocationResolver = (
  request: DeltaLocationResolveRequest,
) => Promise<DeltaLocationResolveResponse>;

export type BrowserTableRootAccessCapabilities = {
  list: boolean;
  head: boolean;
  get: boolean;
  rangeGet: boolean;
};

export type BrowserObjectGrantCapabilities = BrowserTableRootAccessCapabilities & {
  batchSign: boolean;
  proxyRange: boolean;
};

export type BrowserObjectGrantDescriptor = {
  grantId: string;
  tableRootUrl: string;
  expiresAtEpochMs: number;
  capabilities: BrowserObjectGrantCapabilities;
};

export type DeltaSharingFileAction = BrowserHttpFileDescriptor;

export type BrowserDeltaSource =
  | { kind: 'local_files'; files: File[] }
  | { kind: 'http_manifest'; manifestUrl: string }
  | {
      kind: 'cors_http_table';
      tableRootUrl: string;
      capabilities?: BrowserTableRootAccessCapabilities;
    }
  | { kind: 'brokered_manifest'; manifestUrl: string; grantId?: string }
  | { kind: 'brokered_object_grants'; grant: BrowserObjectGrantDescriptor }
  | { kind: 'delta_sharing_url_files'; files: DeltaSharingFileAction[] }
  | { kind: 'trusted_descriptor'; descriptor: BrowserHttpSnapshotDescriptor };

export type DeltaLocationResolutionMode = 'browser_local' | 'brokered_access' | 'server_snapshot';
export type BrowserDeltaLocationResolutionMode = 'browser_local' | 'brokered_access';

export type BrowserSnapshotReconstructionSource = Extract<
  BrowserDeltaSource,
  { kind: 'local_files' } | { kind: 'cors_http_table' } | { kind: 'brokered_object_grants' }
>;

export type BrowserSnapshotResolverOptions = {
  snapshotVersion?: number;
};

export type BrowserSnapshotResolver = (
  source: BrowserSnapshotReconstructionSource,
  options: BrowserSnapshotResolverOptions,
) => Promise<BrowserHttpSnapshotDescriptor>;

export type DeltaLocationServerSnapshotOpenOptions = AxonRequestOptions & {
  resolutionMode: 'server_snapshot';
  provider: DeltaObjectStoreProvider;
  tableUri: string;
  credentialProfile: CredentialProfileRef;
  requestedAccessMode?: ResolverRequestedAccessMode;
  snapshotVersion?: number;
  resolverUrl?: string | URL;
  resolveDeltaLocation?: DeltaLocationResolver;
};

export type DeltaLocationBrowserSourceOpenOptions = AxonRequestOptions & {
  source: BrowserDeltaSource;
  resolutionMode?: BrowserDeltaLocationResolutionMode;
  snapshotVersion?: number;
  resolveBrowserSnapshot?: BrowserSnapshotResolver;
};

export type DeltaLocationOpenOptions =
  | DeltaLocationServerSnapshotOpenOptions
  | DeltaLocationBrowserSourceOpenOptions;

export type DeltaLocationServerSnapshotOpenMetadata = Omit<
  DeltaLocationResolveResponse,
  'descriptor'
>;

export type DeltaLocationBrowserOpenMetadata = {
  source_kind: BrowserDeltaSource['kind'];
  resolution_mode: BrowserDeltaLocationResolutionMode;
  table_uri: string;
  resolved_snapshot_version: number;
  expires_at_epoch_ms?: number;
};

export type DeltaLocationOpenMetadata =
  | DeltaLocationServerSnapshotOpenMetadata
  | DeltaLocationBrowserOpenMetadata;

export type DeltaLocationServerSnapshotOpenedEnvelope = BrowserWorkerOpenedEnvelope & {
  location: DeltaLocationServerSnapshotOpenMetadata;
};

export type DeltaLocationBrowserOpenedEnvelope = BrowserWorkerOpenedEnvelope & {
  location: DeltaLocationBrowserOpenMetadata;
};

export type DeltaLocationOpenedEnvelope =
  | DeltaLocationServerSnapshotOpenedEnvelope
  | DeltaLocationBrowserOpenedEnvelope;

export type ResolvedFileDescriptor = {
  path: string;
  size_bytes: number;
  partition_values: Record<string, string | null>;
  stats?: string;
};

export type ResolvedSnapshotDescriptor = {
  table_uri: string;
  snapshot_version: number;
  partition_column_types?: Partial<Record<string, PartitionColumnType>>;
  browser_compatibility?: CapabilityReport;
  required_capabilities?: CapabilityReport;
  active_files: ResolvedFileDescriptor[];
};

export type BrokeredDeltaAccessMode = 'delta_log' | 'presigned_files';
export type PolicyAuthorityKind = 'unity_catalog' | 'delta_sharing' | 'mock_broker';
export type DirectExternalEngineReadSupport = 'confirmed' | 'not_confirmed';

export type BrokeredPolicyAuthority = {
  authority: PolicyAuthorityKind;
  directExternalEngineRead: DirectExternalEngineReadSupport;
};

export type ReadAccessPlanReason =
  | 'row_filter'
  | 'column_mask'
  | 'view'
  | 'unknown_policy_state'
  | 'no_direct_external_engine_read_support'
  | 'unsupported_table_type'
  | 'grant_expired'
  | 'storage_cors_blocked'
  | 'broker_unavailable';

export type BrokeredObjectAccess = {
  list: boolean;
  head: boolean;
  get: boolean;
  rangeGet: boolean;
  batchSign: boolean;
  proxyRange: boolean;
};

export type BrokeredDeltaReadPlan = {
  tableId: string;
  fullName: string;
  tableRoot: string;
  grantId: string;
  expiresAtEpochMs: number;
  deltaAccessMode: BrokeredDeltaAccessMode;
  policyAuthority: BrokeredPolicyAuthority;
  objectAccess: BrokeredObjectAccess;
};

export type DeltaSharingReadAccessPlanPayload = {
  tableId: string;
  fullName: string;
  sharingEndpoint: string;
  expiresAtEpochMs: number;
  files: BrowserHttpFileDescriptor[];
};

export type SqlFallbackRequiredPlan = {
  tableId: string;
  fullName: string;
  reason: ReadAccessPlanReason;
  message: string;
  statementEndpoint: string;
  warehouseRequired: true;
};

export type BlockedReadPlan = {
  tableId: string;
  fullName: string;
  reason: ReadAccessPlanReason;
  message: string;
};

export type BrokeredDeltaReadAccessPlan = { plan_type: 'brokered_delta' } & BrokeredDeltaReadPlan;
export type DeltaSharingReadAccessPlan = {
  plan_type: 'delta_sharing';
} & DeltaSharingReadAccessPlanPayload;
export type SqlFallbackRequiredReadAccessPlan = {
  plan_type: 'sql_fallback_required';
} & SqlFallbackRequiredPlan;
export type BlockedReadAccessPlan = { plan_type: 'blocked' } & BlockedReadPlan;

export type ReadAccessPlan =
  | BrokeredDeltaReadAccessPlan
  | DeltaSharingReadAccessPlan
  | SqlFallbackRequiredReadAccessPlan
  | BlockedReadAccessPlan;

export type ObjectGrantBatchSignResponse = {
  signedUrls: ObjectGrantSignedUrl[];
};

export type ObjectGrantSignedUrl = {
  path: string;
  url: string;
  expiresAtEpochMs: number;
};

export type UcSessionRef = {
  id: string;
  displayName?: string;
};

export type UnityCatalogReadAccessRequest = {
  fullName: string;
  session?: UcSessionRef;
};

export type UnityCatalogReadAccessResolver = (
  request: UnityCatalogReadAccessRequest,
) => Promise<unknown>;

export type BrokeredDeltaPlanAdapter = {
  resolveSnapshot(plan: BrokeredDeltaReadAccessPlan): Promise<unknown>;
  batchSign(plan: BrokeredDeltaReadAccessPlan, paths: string[]): Promise<unknown>;
};

export type SqlFallbackExecutor = (plan: SqlFallbackRequiredReadAccessPlan) => Promise<unknown>;

export type UnityCatalogOpenOptions = AxonRequestOptions & {
  fullName: string;
  session?: UcSessionRef;
  resolveReadAccessPlan: UnityCatalogReadAccessResolver;
  brokeredDelta?: BrokeredDeltaPlanAdapter;
  serverFallbackEnabled?: boolean;
  executeSqlFallback?: SqlFallbackExecutor;
};

export type UnityCatalogOpenedResult = BrowserWorkerOpenedEnvelope & {
  status: 'opened';
  planType: 'brokered_delta' | 'delta_sharing';
  tableId: string;
  fullName: string;
  expiresAtEpochMs: number;
};

export type UnityCatalogFallbackRequiredResult = {
  status: 'sql_fallback_required';
  planType: 'sql_fallback_required';
  tableId: string;
  fullName: string;
  reason: ReadAccessPlanReason;
  message: string;
  statementEndpoint: string;
  warehouseRequired: true;
};

export type UnityCatalogSqlFallbackRoutedResult = {
  status: 'sql_fallback_routed';
  planType: 'sql_fallback_required';
  tableId: string;
  fullName: string;
  reason: ReadAccessPlanReason;
  message: string;
  result: unknown;
};

export type UnityCatalogBlockedResult = {
  status: 'blocked';
  planType: 'blocked';
  tableId: string;
  fullName: string;
  reason: ReadAccessPlanReason;
  message: string;
};

export type UnityCatalogOpenResult =
  | UnityCatalogOpenedResult
  | UnityCatalogFallbackRequiredResult
  | UnityCatalogSqlFallbackRoutedResult
  | UnityCatalogBlockedResult;

export type BrowserAccessMode = 'browser_safe_http' | 'cloud_object_store';

export type QueryMetricsSummary = {
  bytes_fetched: number;
  duration_ms: number;
  files_touched: number;
  files_skipped: number;
  prebootstrap_fail_open_count?: number;
  prebootstrap_files_pruned?: number;
  footer_reads_avoided?: number;
  prebootstrap_candidate_files?: number;
  row_groups_touched?: number;
  row_groups_skipped?: number;
  footer_reads?: number;
  bootstrap_footer_range_reads?: number;
  scan_footer_range_reads?: number;
  scan_data_range_reads?: number;
  duplicate_range_reads?: number;
  coalesced_range_reads?: number;
  coalesced_gap_bytes_fetched?: number;
  footer_cache_hits?: number;
  footer_cache_misses?: number;
  footer_range_reads_avoided?: number;
  footer_cache_degraded_identity_reads?: number;
  identity_present_range_reads?: number;
  identity_missing_range_reads?: number;
  range_cache_hits?: number;
  range_cache_misses?: number;
  range_cache_bytes_reused?: number;
  range_cache_bytes_stored?: number;
  range_cache_validation_misses?: number;
  range_cache_degraded_identity_reads?: number;
  range_readahead_requests?: number;
  range_readahead_bytes_fetched?: number;
  range_readahead_bytes_used?: number;
  range_readahead_wasted_bytes?: number;
  descriptor_resolution_count?: number;
  delta_log_manifest_list_count?: number;
  delta_log_manifest_list_duration_ms?: number;
  snapshot_resolve_count?: number;
  snapshot_resolve_duration_ms?: number;
  descriptor_cache_hit?: number;
  session_reuse_count?: number;
  opened_table_reuse_count?: number;
  identity_refresh_count?: number;
  access_envelope_refresh_count?: number;
  rows_emitted?: number;
  snapshot_bootstrap_duration_ms?: number;
  access_mode?: BrowserAccessMode;
  arrow_ipc_bytes?: number;
  arrow_ipc_chunk_count?: number;
  preview_rows?: number;
  preview_string_bytes?: number;
  planning_duration_ms?: number;
  arrow_ipc_encode_duration_ms?: number;
  preview_duration_ms?: number;
};

export type QueryResponse = {
  executed_on: ExecutionTarget;
  capabilities: CapabilityReport;
  fallback_reason?: FallbackReason;
  metrics: QueryMetricsSummary;
  explain?: string;
};

export type BrowserWorkerSqlOutput = 'arrow_ipc_stream';
export type BrowserWorkerSqlDelivery = 'single_buffer' | 'chunked_buffers';

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

export type BrowserWorkerOpenParquetDatasetCommand = {
  request_id: string;
  name: string;
  dataset: BrowserHttpParquetDatasetDescriptor;
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
  delivery?: BrowserWorkerSqlDelivery;
  browser_safe_defaults?: boolean;
};

// Internal worker control message only. It is fire-and-forget and has no response envelope.
export type BrowserWorkerCancelCommand = {
  request_id: string;
  query_id?: string;
};

export type BrowserWorkerDisposeCommand = {
  request_id: string;
  name: string;
};

export type BrowserWorkerCommand =
  | { open_table: BrowserWorkerOpenTableCommand }
  | { open_delta_table: BrowserWorkerOpenDeltaTableCommand }
  | { open_parquet_dataset: BrowserWorkerOpenParquetDatasetCommand }
  | { inspect_parquet: BrowserWorkerInspectParquetCommand }
  | { sql: BrowserWorkerSqlCommand }
  | { cancel: BrowserWorkerCancelCommand }
  | { dispose: BrowserWorkerDisposeCommand };

export type ArrowIpcFormat = 'stream' | 'file';
export type ArrowIpcDelivery = 'single_buffer' | 'chunked_buffers';

export type WireArrowIpcResult = {
  format: ArrowIpcFormat;
  content_type: string;
  delivery?: ArrowIpcDelivery;
  bytes?: number[] | ArrayBuffer | Uint8Array;
  byte_length?: number;
  chunk_count?: number;
};

export type ArrowIpcResult = {
  format: ArrowIpcFormat;
  content_type: string;
  delivery: ArrowIpcDelivery;
  bytes: Uint8Array;
  byte_length: number;
  chunk_count: number;
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
  prebootstrap_fail_open_count?: number;
  prebootstrap_files_pruned?: number;
  footer_reads_avoided?: number;
  prebootstrap_candidate_files?: number;
  row_groups_touched: number;
  row_groups_skipped: number;
  footer_reads?: number;
  bootstrap_footer_range_reads?: number;
  scan_footer_range_reads?: number;
  scan_data_range_reads?: number;
  duplicate_range_reads?: number;
  coalesced_range_reads?: number;
  coalesced_gap_bytes_fetched?: number;
  footer_cache_hits?: number;
  footer_cache_misses?: number;
  footer_range_reads_avoided?: number;
  footer_cache_degraded_identity_reads?: number;
  identity_present_range_reads?: number;
  identity_missing_range_reads?: number;
  range_cache_hits?: number;
  range_cache_misses?: number;
  range_cache_bytes_reused?: number;
  range_cache_bytes_stored?: number;
  range_cache_validation_misses?: number;
  range_cache_degraded_identity_reads?: number;
  range_readahead_requests?: number;
  range_readahead_bytes_fetched?: number;
  range_readahead_bytes_used?: number;
  range_readahead_wasted_bytes?: number;
  descriptor_resolution_count?: number;
  delta_log_manifest_list_count?: number;
  delta_log_manifest_list_duration_ms?: number;
  snapshot_resolve_count?: number;
  snapshot_resolve_duration_ms?: number;
  descriptor_cache_hit?: number;
  session_reuse_count?: number;
  opened_table_reuse_count?: number;
  identity_refresh_count?: number;
  access_envelope_refresh_count?: number;
  rows_emitted: number;
  snapshot_bootstrap_duration_ms?: number;
  access_mode?: BrowserAccessMode;
  arrow_ipc_bytes?: number;
  arrow_ipc_chunk_count?: number;
  preview_rows?: number;
  preview_string_bytes?: number;
  planning_duration_ms?: number;
  arrow_ipc_encode_duration_ms?: number;
  preview_duration_ms?: number;
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

export type BrowserWorkerArrowIpcChunkEvent = {
  context: BrowserWorkerEventContext;
  request_id: string;
  sequence: number;
  byte_offset: number;
  byte_length: number;
  bytes: Uint8Array;
};

export type BrowserWorkerEventEnvelope =
  | { progress: BrowserWorkerProgressEvent }
  | { log: BrowserWorkerLogEvent }
  | { range_read_metrics: BrowserWorkerRangeReadMetricsEvent }
  | { cache_metrics: BrowserWorkerCacheMetricsEvent }
  | { fallback: BrowserWorkerFallbackEvent }
  | { cancellation: BrowserWorkerCancellationEvent }
  | { terminal_error: BrowserWorkerTerminalErrorEvent }
  | { arrow_ipc_chunk: BrowserWorkerArrowIpcChunkEvent };

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
      workerUrl: '/workers/axon-web-worker.js',
      wasmUrl: '/workers/axon_web_wasm_bg.wasm',
    },
    {
      id: 'simd',
      tier: 'simd',
      status: 'future',
      workerUrl: '/workers/axon-web-worker.simd.js',
      wasmUrl: '/workers/axon_web_wasm_bg.simd.wasm',
      requiredFeatures: {
        wasmSIMD: true,
      },
    },
    {
      id: 'threaded',
      tier: 'threaded',
      status: 'future',
      workerUrl: '/workers/axon-web-worker.threaded.js',
      wasmUrl: '/workers/axon_web_wasm_bg.threaded.wasm',
      requiredFeatures: {
        crossOriginIsolated: true,
        wasmThreads: true,
      },
    },
    {
      id: 'simd-threaded',
      tier: 'simd_threaded',
      status: 'future',
      workerUrl: '/workers/axon-web-worker.simd-threaded.js',
      wasmUrl: '/workers/axon_web_wasm_bg.simd-threaded.wasm',
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
  openDeltaLocation(
    name: string,
    options: DeltaLocationServerSnapshotOpenOptions,
  ): Promise<DeltaLocationServerSnapshotOpenedEnvelope>;
  openDeltaLocation(
    name: string,
    options: DeltaLocationBrowserSourceOpenOptions,
  ): Promise<DeltaLocationBrowserOpenedEnvelope>;
  openDeltaShare(
    name: string,
    options: DeltaSharingOpenOptions,
  ): Promise<DeltaSharingOpenedEnvelope>;
  openParquetDataset(
    name: string,
    dataset: BrowserHttpParquetDatasetDescriptor,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerOpenedEnvelope>;
  openUnityCatalogTable(
    name: string,
    options: UnityCatalogOpenOptions,
  ): Promise<UnityCatalogOpenResult>;
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
  cancelQuery(queryId: string, options?: AxonRequestOptions): void;
  dispose(name: string, options?: AxonRequestOptions): Promise<BrowserWorkerDisposedEnvelope>;
  terminate(): void;
}

type PendingRequest = {
  expected: 'opened' | 'success' | 'parquet_inspection' | 'disposed';
  resolve: (response: BrowserWorkerResponseEnvelope) => void;
  reject: (error: Error) => void;
  arrowIpcChunks?: PendingArrowIpcChunks;
};

type PendingArrowIpcChunks = {
  chunks: Uint8Array[];
  nextSequence: number;
  nextByteOffset: number;
};

type OpenedTableMetadata = {
  table_uri: string;
  snapshot_version?: number;
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

export class DeltaLocationResolverError extends Error {
  readonly name = 'DeltaLocationResolverError';
  readonly code: DeltaLocationResolverErrorCode;
  readonly correlationId?: string;

  constructor(
    code: DeltaLocationResolverErrorCode,
    message: string,
    options: { correlationId?: string } = {},
  ) {
    super(redactUrlSecrets(message));
    this.code = code;
    this.correlationId = options.correlationId;
  }
}

export class DeltaSharingError extends Error {
  readonly name = 'DeltaSharingError';
  readonly code: DeltaSharingErrorCode;
  readonly status?: number;

  constructor(code: DeltaSharingErrorCode, message: string, options: { status?: number } = {}) {
    super(redactUrlSecrets(message));
    this.code = code;
    this.status = options.status;
  }
}

export function createAxonBrowserClient(options: AxonBrowserClientOptions): AxonBrowserClient {
  if ('worker' in options && typeof options.worker !== 'function') {
    return new AxonBrowserWorkerClient(options.worker, options.requestId, options.onEvent);
  }

  const createWorker =
    'workerUrl' in options
      ? () => new Worker(options.workerUrl, { type: 'module', ...options.workerOptions })
      : 'worker' in options
        ? () => createWorkerFromOption(options.worker)
        : () => createWorkerFromBundleSelection(options);

  return new LazyAxonBrowserClient(
    () => new AxonBrowserWorkerClient(createWorker(), options.requestId, options.onEvent),
  );
}

export function createDeltaSharingClient(
  options: DeltaSharingClientOptions = {},
): DeltaSharingClient {
  return new DeltaSharingRestClient(options.fetch ?? globalDeltaSharingFetch);
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

export function openParquetDatasetCommand(
  requestId: string,
  name: string,
  dataset: BrowserHttpParquetDatasetDescriptor,
): BrowserWorkerCommand {
  return {
    open_parquet_dataset: {
      request_id: requestId,
      name,
      dataset,
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
      delivery: 'chunked_buffers',
      browser_safe_defaults: true,
    },
  };
}

function cancelQueryCommand(requestId: string, queryId: string): BrowserWorkerCommand {
  return {
    cancel: {
      request_id: requestId,
      query_id: queryId,
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

function queryRequestWithBrowserSafeDefaults(request: QueryRequest): QueryRequest {
  const options: QueryExecutionOptions = {
    include_explain: false,
    collect_metrics: true,
    ...(request.options ?? {}),
  };
  const resultPage = browserSafeResultPage(options.result_page);
  const runtimeLimits = browserSafeRuntimeLimits(options.runtime_limits);

  return {
    ...request,
    options: {
      ...options,
      result_page: resultPage,
      runtime_limits: runtimeLimits,
    },
  };
}

function browserSafeResultPage(page: QueryResultPageRequest | undefined): QueryResultPageRequest {
  if (!page) {
    return {
      limit: BROWSER_SAFE_RESULT_ROW_LIMIT,
      offset: 0,
    };
  }
  const limit = requiredPositiveInteger(page.limit, 'result_page.limit');
  if (limit > BROWSER_SAFE_RESULT_ROW_LIMIT) {
    throw new AxonSdkError(
      `result_page.limit ${limit} exceeds browser-safe maximum ${BROWSER_SAFE_RESULT_ROW_LIMIT}`,
    );
  }

  return {
    limit,
    offset: requiredNonNegativeInteger(page.offset, 'result_page.offset'),
  };
}

function browserSafeRuntimeLimits(limits: QueryRuntimeLimits | undefined): QueryRuntimeLimits {
  const runtimeLimits = limits ?? {};
  const maxResultRows = browserSafeOptionalLimit(
    runtimeLimits.max_result_rows,
    BROWSER_SAFE_RESULT_ROW_LIMIT,
    'runtime_limits.max_result_rows',
  );
  const maxArrowIpcBytes = browserSafeOptionalLimit(
    runtimeLimits.max_arrow_ipc_bytes,
    BROWSER_SAFE_ARROW_IPC_BYTES,
    'runtime_limits.max_arrow_ipc_bytes',
  );
  const maxPreviewStringBytes = browserSafeOptionalLimit(
    runtimeLimits.max_preview_string_bytes,
    BROWSER_SAFE_PREVIEW_STRING_BYTES,
    'runtime_limits.max_preview_string_bytes',
  );

  return {
    ...runtimeLimits,
    max_result_rows: maxResultRows,
    max_arrow_ipc_bytes: maxArrowIpcBytes,
    max_preview_string_bytes: maxPreviewStringBytes,
  };
}

function browserSafeOptionalLimit(value: number | undefined, max: number, path: string): number {
  if (value === undefined) {
    return max;
  }
  const limit = requiredPositiveInteger(value, path);
  if (limit > max) {
    throw new AxonSdkError(`${path} ${limit} exceeds browser-safe maximum ${max}`);
  }
  return limit;
}

export function parseReadAccessPlan(value: unknown): ReadAccessPlan {
  rejectForbiddenSecretKeys(value);
  const plan = requiredRecord(value, 'read access plan');
  const planType = requiredString(plan.plan_type, 'read access plan.plan_type');

  switch (planType) {
    case 'brokered_delta':
      assertOnlyKeys(plan, 'brokered_delta plan', [
        'plan_type',
        'tableId',
        'fullName',
        'tableRoot',
        'grantId',
        'expiresAtEpochMs',
        'deltaAccessMode',
        'policyAuthority',
        'objectAccess',
      ]);
      return {
        plan_type: 'brokered_delta',
        tableId: requiredString(plan.tableId, 'brokered_delta.tableId'),
        fullName: requiredString(plan.fullName, 'brokered_delta.fullName'),
        tableRoot: requiredString(plan.tableRoot, 'brokered_delta.tableRoot'),
        grantId: requiredString(plan.grantId, 'brokered_delta.grantId'),
        expiresAtEpochMs: requiredNonNegativeInteger(
          plan.expiresAtEpochMs,
          'brokered_delta.expiresAtEpochMs',
        ),
        deltaAccessMode: requiredEnum(
          plan.deltaAccessMode,
          'brokered_delta.deltaAccessMode',
          BROKERED_DELTA_ACCESS_MODES,
        ),
        policyAuthority: normalizeBrokeredPolicyAuthority(plan.policyAuthority),
        objectAccess: normalizeBrokeredObjectAccess(plan.objectAccess),
      };
    case 'delta_sharing': {
      assertOnlyKeys(plan, 'delta_sharing plan', [
        'plan_type',
        'tableId',
        'fullName',
        'sharingEndpoint',
        'expiresAtEpochMs',
        'files',
      ]);
      const files = requiredArray(plan.files, 'delta_sharing.files').map((file, index) => {
        const filePath = `delta_sharing.files[${index}]`;
        assertOnlyKeys(requiredRecord(file, filePath), filePath, [
          'path',
          'url',
          'size_bytes',
          'partition_values',
          'stats',
          'object_etag',
        ]);
        const normalized = normalizeBrowserHttpFileDescriptor(file, filePath);
        validateHttpsUrl(normalized.url, `delta_sharing.files[${index}].url`);
        return normalized;
      });
      const sharingEndpoint = requiredString(plan.sharingEndpoint, 'delta_sharing.sharingEndpoint');
      validateHttpsUrl(sharingEndpoint, 'delta_sharing.sharingEndpoint');
      return {
        plan_type: 'delta_sharing',
        tableId: requiredString(plan.tableId, 'delta_sharing.tableId'),
        fullName: requiredString(plan.fullName, 'delta_sharing.fullName'),
        sharingEndpoint,
        expiresAtEpochMs: requiredNonNegativeInteger(
          plan.expiresAtEpochMs,
          'delta_sharing.expiresAtEpochMs',
        ),
        files,
      };
    }
    case 'sql_fallback_required': {
      assertOnlyKeys(plan, 'sql_fallback_required plan', [
        'plan_type',
        'tableId',
        'fullName',
        'reason',
        'message',
        'statementEndpoint',
        'warehouseRequired',
      ]);
      const statementEndpoint = requiredString(
        plan.statementEndpoint,
        'sql_fallback_required.statementEndpoint',
      );
      validateHttpsUrl(statementEndpoint, 'sql_fallback_required.statementEndpoint');
      if (plan.warehouseRequired !== true) {
        throw new AxonProtocolError('sql_fallback_required.warehouseRequired must be true');
      }
      return {
        plan_type: 'sql_fallback_required',
        tableId: requiredString(plan.tableId, 'sql_fallback_required.tableId'),
        fullName: requiredString(plan.fullName, 'sql_fallback_required.fullName'),
        reason: requiredEnum(plan.reason, 'sql_fallback_required.reason', READ_ACCESS_PLAN_REASONS),
        message: requiredString(plan.message, 'sql_fallback_required.message'),
        statementEndpoint,
        warehouseRequired: true,
      };
    }
    case 'blocked':
      assertOnlyKeys(plan, 'blocked plan', [
        'plan_type',
        'tableId',
        'fullName',
        'reason',
        'message',
      ]);
      return {
        plan_type: 'blocked',
        tableId: requiredString(plan.tableId, 'blocked.tableId'),
        fullName: requiredString(plan.fullName, 'blocked.fullName'),
        reason: requiredEnum(plan.reason, 'blocked.reason', READ_ACCESS_PLAN_REASONS),
        message: requiredString(plan.message, 'blocked.message'),
      };
    default:
      throw new AxonProtocolError(`unknown read access plan_type '${planType}'`);
  }
}

export type DeltaSharingReadAccessSnapshotOptions = {
  snapshotVersion?: number;
  tableUri?: string;
};

export function snapshotFromDeltaSharingReadAccessPlan(
  plan: DeltaSharingReadAccessPlan,
  options: DeltaSharingReadAccessSnapshotOptions = {},
): BrowserHttpSnapshotDescriptor {
  const signedUrlCapabilities: CapabilityReport = {
    capabilities: {
      signed_url_access: 'supported',
    },
  };

  return {
    table_uri: options.tableUri ?? deltaSharingReadAccessTableUri(plan),
    snapshot_version: options.snapshotVersion ?? 0,
    partition_column_types: {},
    browser_compatibility: signedUrlCapabilities,
    required_capabilities: signedUrlCapabilities,
    active_files: plan.files,
  };
}

export function snapshotFromBrokeredDeltaReadPlan(
  plan: BrokeredDeltaReadPlan | BrokeredDeltaReadAccessPlan,
  resolvedSnapshot: ResolvedSnapshotDescriptor,
  signedUrlsByPath: Record<string, string>,
): BrowserHttpSnapshotDescriptor {
  if (plan.policyAuthority.directExternalEngineRead !== 'confirmed') {
    throw new AxonProtocolError(
      'brokered Delta direct read requires confirmed external-engine support',
    );
  }
  if (!plan.objectAccess.batchSign) {
    throw new AxonProtocolError(
      'brokered Delta descriptor adaptation requires batch signed object URLs',
    );
  }
  if (!plan.objectAccess.rangeGet) {
    throw new AxonProtocolError(
      'brokered Delta descriptor adaptation requires range-readable objects',
    );
  }
  if (resolvedSnapshot.table_uri !== plan.tableRoot) {
    throw new AxonProtocolError(
      'brokered Delta plan tableRoot did not match resolved snapshot table_uri',
    );
  }

  const resolvedPaths = new Set<string>();
  const duplicatePaths = new Set<string>();
  for (const file of resolvedSnapshot.active_files) {
    if (resolvedPaths.has(file.path)) {
      duplicatePaths.add(file.path);
    }
    resolvedPaths.add(file.path);
  }

  const missingPaths = resolvedSnapshot.active_files
    .filter((file) => signedUrlsByPath[file.path] === undefined)
    .map((file) => file.path);
  const unexpectedPaths = Object.keys(signedUrlsByPath).filter((path) => !resolvedPaths.has(path));
  if (duplicatePaths.size > 0 || missingPaths.length > 0 || unexpectedPaths.length > 0) {
    throw new AxonProtocolError(
      `brokered Delta signed URL coverage did not match resolved snapshot: ${[
        duplicatePaths.size > 0 ? `duplicate paths [${quotePaths([...duplicatePaths])}]` : '',
        missingPaths.length > 0 ? `missing URLs for [${quotePaths(missingPaths)}]` : '',
        unexpectedPaths.length > 0 ? `unexpected URLs for [${quotePaths(unexpectedPaths)}]` : '',
      ]
        .filter(Boolean)
        .join('; ')}`,
    );
  }

  return {
    table_uri: resolvedSnapshot.table_uri,
    snapshot_version: resolvedSnapshot.snapshot_version,
    partition_column_types: resolvedSnapshot.partition_column_types ?? {},
    browser_compatibility: resolvedSnapshot.browser_compatibility ?? { capabilities: {} },
    required_capabilities: resolvedSnapshot.required_capabilities ?? { capabilities: {} },
    active_files: resolvedSnapshot.active_files.map((file) => {
      const url = signedUrlsByPath[file.path];
      validateHttpsUrl(url, `brokered Delta signed object URL for '${file.path}'`);
      return {
        path: file.path,
        url,
        size_bytes: file.size_bytes,
        partition_values: file.partition_values,
        stats: file.stats,
      };
    }),
  };
}

export function validateDeltaLocationResolveRequest(request: DeltaLocationResolveRequest): void {
  if (!includesString(DELTA_OBJECT_STORE_PROVIDERS, request.provider)) {
    throw new DeltaLocationResolverError(
      'provider_not_supported',
      `Delta location resolver provider '${String(request.provider)}' is not supported`,
    );
  }
  if (!includesString(RESOLVER_REQUESTED_ACCESS_MODES, request.requested_access_mode)) {
    throw new DeltaLocationResolverError(
      'access_mode_not_supported',
      `Delta location resolver access mode '${String(request.requested_access_mode)}' is not supported`,
    );
  }
  validateStorageAccessProfile(request.credential_profile);
  validateLogicalDeltaTableUri(request.provider, request.table_uri);
  if (
    request.snapshot_version !== undefined &&
    (!Number.isInteger(request.snapshot_version) || request.snapshot_version < 0)
  ) {
    throw new DeltaLocationResolverError(
      'invalid_snapshot_version',
      'Delta location snapshot version must be a non-negative integer',
    );
  }
}

export function validateDeltaLocationResolveResponse(
  input: DeltaLocationResolveResponse,
  request: DeltaLocationResolveRequest,
  nowEpochMs: number = Date.now(),
): void {
  const response = normalizeDeltaLocationResolveResponseEnvelope(input);

  if (response.provider !== request.provider) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      `resolver response provider '${response.provider}' did not match request provider '${request.provider}'`,
    );
  }
  if (response.table_uri !== request.table_uri) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      'resolver response table URI did not match the requested Delta location',
    );
  }
  if (
    request.snapshot_version !== undefined &&
    response.requested_snapshot_version !== request.snapshot_version
  ) {
    throw new DeltaLocationResolverError(
      'snapshot_version_not_found',
      'resolver response did not confirm the requested Delta snapshot version',
      { correlationId: response.correlation_id },
    );
  }
  if (
    request.snapshot_version !== undefined &&
    response.resolved_snapshot_version !== request.snapshot_version
  ) {
    throw new DeltaLocationResolverError(
      'snapshot_version_not_found',
      'resolver response resolved a different Delta snapshot version',
      { correlationId: response.correlation_id },
    );
  }
  if (
    response.requested_access_mode !== undefined &&
    response.requested_access_mode !== request.requested_access_mode
  ) {
    throw new DeltaLocationResolverError(
      'access_mode_not_supported',
      'resolver response requested access mode did not match the SDK request',
      { correlationId: response.correlation_id },
    );
  }
  if (
    request.requested_access_mode !== 'auto' &&
    response.actual_access_mode !== request.requested_access_mode
  ) {
    throw new DeltaLocationResolverError(
      'access_mode_not_supported',
      'resolver response actual access mode did not satisfy the SDK request',
      { correlationId: response.correlation_id },
    );
  }
  if (response.descriptor.snapshot_version !== response.resolved_snapshot_version) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      'resolver response snapshot version did not match the descriptor snapshot version',
      { correlationId: response.correlation_id },
    );
  }
  if (response.expires_at_epoch_ms <= nowEpochMs) {
    throw new DeltaLocationResolverError(
      'descriptor_expired',
      'resolved Delta descriptor has expired',
      { correlationId: response.correlation_id },
    );
  }
  if (response.refresh && response.refresh.same_snapshot_required !== true) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      'Delta location refresh must require the same resolved snapshot',
      { correlationId: response.correlation_id },
    );
  }
  for (const file of response.descriptor.active_files) {
    validateBrowserSafeDescriptorUrl(file.url, response.correlation_id);
  }
}

function normalizeDeltaLocationResolveResponseEnvelope(
  input: unknown,
): DeltaLocationResolveResponse {
  try {
    const response = requiredObject(input, 'response');
    const descriptor = normalizeBrowserHttpSnapshotDescriptor(response.descriptor, 'descriptor');
    const normalized: DeltaLocationResolveResponse = {
      descriptor,
      provider: requiredEnum(response.provider, 'provider', DELTA_OBJECT_STORE_PROVIDERS),
      table_uri: requiredString(response.table_uri, 'table_uri'),
      resolved_snapshot_version: requiredNonNegativeInteger(
        response.resolved_snapshot_version,
        'resolved_snapshot_version',
      ),
      actual_access_mode: requiredEnum(
        response.actual_access_mode,
        'actual_access_mode',
        RESOLVER_ACTUAL_ACCESS_MODES,
      ),
      expires_at_epoch_ms: requiredNonNegativeInteger(
        response.expires_at_epoch_ms,
        'expires_at_epoch_ms',
      ),
    };

    const requestedSnapshotVersion = optionalNonNegativeInteger(
      response.requested_snapshot_version,
      'requested_snapshot_version',
    );
    if (requestedSnapshotVersion !== undefined) {
      normalized.requested_snapshot_version = requestedSnapshotVersion;
    }

    const requestedAccessMode = optionalEnum(
      response.requested_access_mode,
      'requested_access_mode',
      RESOLVER_REQUESTED_ACCESS_MODES,
    );
    if (requestedAccessMode !== undefined) {
      normalized.requested_access_mode = requestedAccessMode;
    }

    const correlationId = optionalString(response.correlation_id, 'correlation_id');
    if (correlationId !== undefined) {
      normalized.correlation_id = correlationId;
    }

    const warnings = optionalStringArray(response.warnings, 'warnings');
    if (warnings !== undefined) {
      normalized.warnings = warnings;
    }

    const refresh = normalizeDeltaLocationRefresh(response.refresh, 'refresh');
    if (refresh !== undefined) {
      normalized.refresh = refresh;
    }

    return normalized;
  } catch (error) {
    if (error instanceof DeltaLocationResolverError) {
      throw error;
    }
    throw new DeltaLocationResolverError(
      'resolver_unavailable',
      `Delta location resolver response is malformed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function normalizeBrowserHttpSnapshotDescriptor(
  input: unknown,
  path: string,
): BrowserHttpSnapshotDescriptor {
  const descriptor = requiredObject(input, path);
  const normalized: BrowserHttpSnapshotDescriptor = {
    table_uri: requiredString(descriptor.table_uri, `${path}.table_uri`),
    snapshot_version: requiredNonNegativeInteger(
      descriptor.snapshot_version,
      `${path}.snapshot_version`,
    ),
    active_files: requiredArray(descriptor.active_files, `${path}.active_files`).map(
      (file, index) => normalizeBrowserHttpFileDescriptor(file, `${path}.active_files[${index}]`),
    ),
  };

  const partitionColumnTypes = normalizePartitionColumnTypes(
    descriptor.partition_column_types,
    `${path}.partition_column_types`,
  );
  if (partitionColumnTypes !== undefined) {
    normalized.partition_column_types = partitionColumnTypes;
  }

  const browserCompatibility = normalizeCapabilityReport(
    descriptor.browser_compatibility,
    `${path}.browser_compatibility`,
  );
  if (browserCompatibility !== undefined) {
    normalized.browser_compatibility = browserCompatibility;
  }

  const requiredCapabilities = normalizeCapabilityReport(
    descriptor.required_capabilities,
    `${path}.required_capabilities`,
  );
  if (requiredCapabilities !== undefined) {
    normalized.required_capabilities = requiredCapabilities;
  }

  return normalized;
}

function normalizeBrowserHttpFileDescriptor(
  input: unknown,
  path: string,
): BrowserHttpFileDescriptor {
  const file = requiredObject(input, path);
  const normalized: BrowserHttpFileDescriptor = {
    path: requiredString(file.path, `${path}.path`),
    url: requiredString(file.url, `${path}.url`),
    size_bytes: requiredNonNegativeInteger(file.size_bytes, `${path}.size_bytes`),
    partition_values: normalizePartitionValues(file.partition_values, `${path}.partition_values`),
  };
  const stats = optionalString(file.stats, `${path}.stats`);
  if (stats !== undefined) {
    normalized.stats = stats;
  }
  const objectEtag = optionalString(file.object_etag, `${path}.object_etag`);
  if (objectEtag !== undefined) {
    normalized.object_etag = objectEtag;
  }
  return normalized;
}

function normalizeDeltaLocationRefresh(
  input: unknown,
  path: string,
): DeltaLocationRefresh | undefined {
  if (input === undefined) {
    return undefined;
  }
  const refresh = requiredObject(input, path);
  const normalized: DeltaLocationRefresh = {
    same_snapshot_required: requiredBoolean(
      refresh.same_snapshot_required,
      `${path}.same_snapshot_required`,
    ) as true,
  };
  const refreshUrl = optionalString(refresh.refresh_url, `${path}.refresh_url`);
  if (refreshUrl !== undefined) {
    normalized.refresh_url = refreshUrl;
  }
  const refreshAfter = optionalNonNegativeInteger(
    refresh.refresh_after_epoch_ms,
    `${path}.refresh_after_epoch_ms`,
  );
  if (refreshAfter !== undefined) {
    normalized.refresh_after_epoch_ms = refreshAfter;
  }
  return normalized;
}

function normalizePartitionColumnTypes(
  input: unknown,
  path: string,
): Partial<Record<string, PartitionColumnType>> | undefined {
  if (input === undefined) {
    return undefined;
  }
  const record = requiredObject(input, path);
  const normalized: Partial<Record<string, PartitionColumnType>> = {};
  for (const [key, value] of Object.entries(record)) {
    normalized[key] = requiredEnum(value, `${path}.${key}`, PARTITION_COLUMN_TYPES);
  }
  return normalized;
}

function normalizeCapabilityReport(input: unknown, path: string): CapabilityReport | undefined {
  if (input === undefined) {
    return undefined;
  }
  const report = requiredObject(input, path);
  const capabilities = requiredObject(report.capabilities, `${path}.capabilities`);
  const normalized: Partial<Record<CapabilityKey, CapabilityState>> = {};
  for (const [key, value] of Object.entries(capabilities)) {
    if (!includesString(CAPABILITY_KEYS, key)) {
      throw new AxonProtocolError(`${path}.capabilities.${key} must be a known capability key`);
    }
    normalized[key as CapabilityKey] = requiredEnum(
      value,
      `${path}.capabilities.${key}`,
      CAPABILITY_STATES,
    );
  }
  return { capabilities: normalized };
}

function normalizePartitionValues(input: unknown, path: string): Record<string, string | null> {
  const record = requiredObject(input, path);
  const normalized: Record<string, string | null> = {};
  for (const [key, value] of Object.entries(record)) {
    if (value !== null && typeof value !== 'string') {
      throw new AxonProtocolError(`${path}.${key} must be a string or null`);
    }
    normalized[key] = value;
  }
  return normalized;
}

function normalizeBrokeredPolicyAuthority(input: unknown): BrokeredPolicyAuthority {
  const authority = requiredRecord(input, 'brokered_delta.policyAuthority');
  assertOnlyKeys(authority, 'brokered_delta.policyAuthority', [
    'authority',
    'directExternalEngineRead',
  ]);
  return {
    authority: requiredEnum(
      authority.authority,
      'brokered_delta.policyAuthority.authority',
      POLICY_AUTHORITY_KINDS,
    ),
    directExternalEngineRead: requiredEnum(
      authority.directExternalEngineRead,
      'brokered_delta.policyAuthority.directExternalEngineRead',
      DIRECT_EXTERNAL_ENGINE_READ_SUPPORT,
    ),
  };
}

function normalizeBrokeredObjectAccess(input: unknown): BrokeredObjectAccess {
  const access = requiredRecord(input, 'brokered_delta.objectAccess');
  assertOnlyKeys(access, 'brokered_delta.objectAccess', [
    'list',
    'head',
    'get',
    'rangeGet',
    'batchSign',
    'proxyRange',
  ]);
  return {
    list: requiredBoolean(access.list, 'brokered_delta.objectAccess.list'),
    head: requiredBoolean(access.head, 'brokered_delta.objectAccess.head'),
    get: requiredBoolean(access.get, 'brokered_delta.objectAccess.get'),
    rangeGet: requiredBoolean(access.rangeGet, 'brokered_delta.objectAccess.rangeGet'),
    batchSign: requiredBoolean(access.batchSign, 'brokered_delta.objectAccess.batchSign'),
    proxyRange: requiredBoolean(access.proxyRange, 'brokered_delta.objectAccess.proxyRange'),
  };
}

function normalizeResolvedSnapshotDescriptor(
  input: unknown,
  path: string,
): ResolvedSnapshotDescriptor {
  const snapshot = requiredRecord(input, path);
  assertOnlyKeys(snapshot, path, [
    'table_uri',
    'snapshot_version',
    'partition_column_types',
    'browser_compatibility',
    'required_capabilities',
    'active_files',
  ]);
  const normalized: ResolvedSnapshotDescriptor = {
    table_uri: requiredString(snapshot.table_uri, `${path}.table_uri`),
    snapshot_version: requiredNonNegativeInteger(
      snapshot.snapshot_version,
      `${path}.snapshot_version`,
    ),
    active_files: requiredArray(snapshot.active_files, `${path}.active_files`).map((file, index) =>
      normalizeResolvedFileDescriptor(file, `${path}.active_files[${index}]`),
    ),
  };
  const partitionColumnTypes = normalizePartitionColumnTypes(
    snapshot.partition_column_types,
    `${path}.partition_column_types`,
  );
  if (partitionColumnTypes !== undefined) {
    normalized.partition_column_types = partitionColumnTypes;
  }
  const browserCompatibility = normalizeCapabilityReport(
    snapshot.browser_compatibility,
    `${path}.browser_compatibility`,
  );
  if (browserCompatibility !== undefined) {
    normalized.browser_compatibility = browserCompatibility;
  }
  const requiredCapabilities = normalizeCapabilityReport(
    snapshot.required_capabilities,
    `${path}.required_capabilities`,
  );
  if (requiredCapabilities !== undefined) {
    normalized.required_capabilities = requiredCapabilities;
  }
  return normalized;
}

function normalizeResolvedFileDescriptor(input: unknown, path: string): ResolvedFileDescriptor {
  const file = requiredRecord(input, path);
  assertOnlyKeys(file, path, ['path', 'size_bytes', 'partition_values', 'stats']);
  const normalized: ResolvedFileDescriptor = {
    path: requiredString(file.path, `${path}.path`),
    size_bytes: requiredNonNegativeInteger(file.size_bytes, `${path}.size_bytes`),
    partition_values: normalizePartitionValues(file.partition_values, `${path}.partition_values`),
  };
  const stats = optionalString(file.stats, `${path}.stats`);
  if (stats !== undefined) {
    normalized.stats = stats;
  }
  return normalized;
}

function normalizeObjectGrantBatchSignResponse(
  input: unknown,
  path: string,
): ObjectGrantBatchSignResponse {
  const response = requiredRecord(input, path);
  assertOnlyKeys(response, path, ['signedUrls']);
  return {
    signedUrls: requiredArray(response.signedUrls, `${path}.signedUrls`).map((signed, index) => {
      const itemPath = `${path}.signedUrls[${index}]`;
      const item = requiredRecord(signed, itemPath);
      assertOnlyKeys(item, itemPath, ['path', 'url', 'expiresAtEpochMs']);
      const url = requiredString(item.url, `${itemPath}.url`);
      validateHttpsUrl(url, `${itemPath}.url`);
      return {
        path: requiredString(item.path, `${itemPath}.path`),
        url,
        expiresAtEpochMs: requiredNonNegativeInteger(
          item.expiresAtEpochMs,
          `${itemPath}.expiresAtEpochMs`,
        ),
      };
    }),
  };
}

function validateReadAccessPlanNotExpired(
  plan: BrokeredDeltaReadAccessPlan | DeltaSharingReadAccessPlan,
  nowEpochMs: number,
): void {
  if (plan.expiresAtEpochMs <= nowEpochMs) {
    throw new AxonProtocolError(`${plan.plan_type} read access plan has expired`);
  }
}

function validateObjectGrantSignedUrlsNotExpired(
  signedUrls: readonly ObjectGrantSignedUrl[],
  nowEpochMs: number,
): void {
  for (const [index, signed] of signedUrls.entries()) {
    if (signed.expiresAtEpochMs <= nowEpochMs) {
      throw new AxonProtocolError(`brokered_delta.batchSign.signedUrls[${index}] has expired`);
    }
  }
}

function unityCatalogReadAccessRequestFromOptions(
  options: UnityCatalogOpenOptions,
): UnityCatalogReadAccessRequest {
  if (!options.fullName.trim()) {
    throw new AxonProtocolError('Unity Catalog table fullName is required');
  }
  if (containsSecretMaterial(options.fullName)) {
    throw new AxonProtocolError(
      'Unity Catalog table fullName must not contain credential material',
    );
  }
  const request: UnityCatalogReadAccessRequest = {
    fullName: options.fullName,
  };
  if (options.session !== undefined) {
    if (!options.session.id.trim()) {
      throw new AxonProtocolError(
        'Unity Catalog session id is required when a session is supplied',
      );
    }
    if (containsSecretMaterial(options.session.id)) {
      throw new AxonProtocolError('Unity Catalog session id must be an opaque session handle');
    }
    request.session = { id: options.session.id };
    if (options.session.displayName !== undefined) {
      request.session.displayName = options.session.displayName;
    }
  }
  return request;
}

function deltaSharingReadAccessTableUri(plan: DeltaSharingReadAccessPlan): string {
  let endpoint: URL;
  try {
    endpoint = new URL(plan.sharingEndpoint);
  } catch (error) {
    throw new AxonProtocolError(
      `delta_sharing.sharingEndpoint is invalid: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
  return `delta-sharing://${endpoint.host}/${plan.fullName}`;
}

function validateHttpsUrl(value: string, path: string): void {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch (error) {
    throw new AxonProtocolError(
      `${path} must be a valid URL: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (parsed.protocol !== 'https:') {
    throw new AxonProtocolError(`${path} must use HTTPS`);
  }
}

function rejectForbiddenSecretKeys(value: unknown, path = 'read access plan'): void {
  if (Array.isArray(value)) {
    value.forEach((item, index) => rejectForbiddenSecretKeys(item, `${path}[${index}]`));
    return;
  }
  if (!isObject(value)) {
    return;
  }
  for (const [key, nested] of Object.entries(value)) {
    if (FORBIDDEN_BROWSER_SECRET_KEYS.has(key.toLowerCase())) {
      throw new AxonProtocolError(`${path} contains forbidden browser secret field '${key}'`);
    }
    rejectForbiddenSecretKeys(nested, `${path}.${key}`);
  }
}

function assertOnlyKeys(
  value: Record<string, unknown>,
  path: string,
  allowed: readonly string[],
): void {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(value)) {
    if (!allowedSet.has(key)) {
      throw new AxonProtocolError(`${path} contains unknown field '${key}'`);
    }
  }
}

function requiredRecord(value: unknown, path: string): Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
  }
  return value;
}

function quotePaths(paths: string[]): string {
  return paths.map((path) => `'${path}'`).join(', ');
}

function optionalStringArray(input: unknown, path: string): string[] | undefined {
  if (input === undefined) {
    return undefined;
  }
  return requiredArray(input, path).map((value, index) =>
    requiredString(value, `${path}[${index}]`),
  );
}

function deltaLocationResolveRequestFromOptions(
  options: DeltaLocationServerSnapshotOpenOptions,
): DeltaLocationResolveRequest {
  const request: DeltaLocationResolveRequest = {
    provider: options.provider,
    table_uri: options.tableUri,
    credential_profile: {
      id: options.credentialProfile.id,
    },
    requested_access_mode: options.requestedAccessMode ?? 'auto',
  };

  if (options.credentialProfile.display_name !== undefined) {
    request.credential_profile.display_name = options.credentialProfile.display_name;
  }
  if (options.snapshotVersion !== undefined) {
    request.snapshot_version = options.snapshotVersion;
  }

  return request;
}

function deltaLocationOpenMetadata(
  response: DeltaLocationResolveResponse,
): DeltaLocationServerSnapshotOpenMetadata {
  const location: DeltaLocationServerSnapshotOpenMetadata = {
    provider: response.provider,
    table_uri: response.table_uri,
    resolved_snapshot_version: response.resolved_snapshot_version,
    actual_access_mode: response.actual_access_mode,
    expires_at_epoch_ms: response.expires_at_epoch_ms,
  };

  if (response.requested_snapshot_version !== undefined) {
    location.requested_snapshot_version = response.requested_snapshot_version;
  }
  if (response.requested_access_mode !== undefined) {
    location.requested_access_mode = response.requested_access_mode;
  }
  if (response.correlation_id !== undefined) {
    location.correlation_id = response.correlation_id;
  }
  if (response.warnings !== undefined) {
    location.warnings = response.warnings;
  }
  if (response.refresh !== undefined) {
    location.refresh = response.refresh;
  }

  return location;
}

async function resolveDeltaLocationFromOptions(
  options: DeltaLocationServerSnapshotOpenOptions,
  request: DeltaLocationResolveRequest,
): Promise<DeltaLocationResolveResponse> {
  if (options.resolveDeltaLocation && options.resolverUrl) {
    throw new DeltaLocationResolverError(
      'resolver_unavailable',
      'openDeltaLocation accepts either resolverUrl or resolveDeltaLocation, not both',
    );
  }

  if (options.resolveDeltaLocation) {
    try {
      return normalizeDeltaLocationResolveResponseEnvelope(
        await options.resolveDeltaLocation(request),
      );
    } catch (error) {
      throw normalizeDeltaLocationResolverError(error);
    }
  }

  if (options.resolverUrl) {
    return resolveDeltaLocationViaHttp(options.resolverUrl, request);
  }

  throw new DeltaLocationResolverError(
    'resolver_unavailable',
    'openDeltaLocation requires resolverUrl or resolveDeltaLocation',
  );
}

function browserDeltaLocationOpenMetadata(
  source: BrowserDeltaSource,
  descriptor: BrowserHttpSnapshotDescriptor,
  resolutionMode: BrowserDeltaLocationResolutionMode,
): DeltaLocationBrowserOpenMetadata {
  const metadata: DeltaLocationBrowserOpenMetadata = {
    source_kind: source.kind,
    resolution_mode: resolutionMode,
    table_uri: descriptor.table_uri,
    resolved_snapshot_version: descriptor.snapshot_version,
  };

  if (source.kind === 'brokered_object_grants') {
    metadata.expires_at_epoch_ms = source.grant.expiresAtEpochMs;
  }

  return metadata;
}

function browserDeltaSourceUsesSnapshotReconstruction(
  source: BrowserDeltaSource,
): source is BrowserSnapshotReconstructionSource {
  return (
    source.kind === 'local_files' ||
    source.kind === 'cors_http_table' ||
    source.kind === 'brokered_object_grants'
  );
}

async function resolveDeltaSnapshotBrowserLocal(
  source: BrowserSnapshotReconstructionSource,
  options: BrowserSnapshotResolverOptions & {
    resolveBrowserSnapshot?: BrowserSnapshotResolver;
  },
): Promise<BrowserHttpSnapshotDescriptor> {
  rejectForbiddenSecretKeys(source);

  switch (source.kind) {
    case 'local_files':
      break;
    case 'cors_http_table':
      assertBrowserTableRootCapabilities(source.capabilities, 'cors_http_table');
      validateHttpsUrl(source.tableRootUrl, 'cors_http_table.tableRootUrl');
      break;
    case 'brokered_object_grants':
      assertBrowserObjectGrantCapabilities(source.grant.capabilities);
      validateHttpsUrl(source.grant.tableRootUrl, 'brokered_object_grants.grant.tableRootUrl');
      if (source.grant.expiresAtEpochMs <= Date.now()) {
        throw new AxonProtocolError('brokered_object_grants grant is expired');
      }
      break;
  }

  if (!options.resolveBrowserSnapshot) {
    throw new AxonSdkError(
      `browser Delta source '${source.kind}' requires resolveBrowserSnapshot for browser-local Delta snapshot reconstruction`,
    );
  }

  return normalizeBrowserHttpSnapshotDescriptor(
    await options.resolveBrowserSnapshot(source, {
      snapshotVersion: options.snapshotVersion,
    }),
    `${source.kind}.resolvedSnapshot`,
  );
}

async function materializeDescriptorFromBrowserSource(
  source: BrowserDeltaSource,
): Promise<BrowserHttpSnapshotDescriptor> {
  rejectForbiddenSecretKeys(source);

  switch (source.kind) {
    case 'trusted_descriptor':
      return normalizeBrowserMaterializedDescriptor(source.descriptor, 'trusted_descriptor');
    case 'http_manifest':
      return fetchBrowserDescriptorManifest(source.manifestUrl, 'http_manifest');
    case 'brokered_manifest':
      return fetchBrowserDescriptorManifest(source.manifestUrl, 'brokered_manifest');
    case 'delta_sharing_url_files':
      return descriptorFromDeltaSharingFileActions(source.files);
    default:
      throw new AxonSdkError(`descriptor materialization does not accept '${source.kind}' sources`);
  }
}

function assertBrowserTableRootCapabilities(
  capabilities: BrowserTableRootAccessCapabilities | undefined,
  path: string,
): void {
  if (!capabilities?.list || !capabilities.head || !capabilities.get || !capabilities.rangeGet) {
    throw new AxonSdkError(
      `${path} requires list, head, get, and rangeGet capabilities for browser-local Delta snapshot reconstruction`,
    );
  }
}

function assertBrowserObjectGrantCapabilities(capabilities: BrowserObjectGrantCapabilities): void {
  assertBrowserTableRootCapabilities(capabilities, 'brokered_object_grants.grant.capabilities');
  if (!capabilities.batchSign && !capabilities.proxyRange) {
    throw new AxonSdkError(
      'brokered_object_grants.grant.capabilities requires batchSign or proxyRange for browser-local Delta snapshot reconstruction',
    );
  }
}

async function fetchBrowserDescriptorManifest(
  manifestUrl: string,
  path: string,
): Promise<BrowserHttpSnapshotDescriptor> {
  validateHttpsUrl(manifestUrl, `${path}.manifestUrl`);
  if (typeof fetch !== 'function') {
    throw new AxonSdkError('global fetch is not available for descriptor materialization');
  }

  let response: Response;
  try {
    response = await fetch(manifestUrl, { credentials: 'omit' });
  } catch (error) {
    throw new AxonSdkError(
      `failed to fetch ${path} descriptor manifest: ${
        error instanceof Error ? redactUrlSecrets(error.message) : String(error)
      }`,
    );
  }

  if (!response.ok) {
    throw new AxonSdkError(`failed to fetch ${path} descriptor manifest: HTTP ${response.status}`);
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch (error) {
    throw new AxonSdkError(
      `failed to parse ${path} descriptor manifest JSON: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  rejectForbiddenSecretKeys(payload, path);
  const record = requiredRecord(payload, path);
  return normalizeBrowserMaterializedDescriptor(
    Object.prototype.hasOwnProperty.call(record, 'descriptor') ? record.descriptor : payload,
    `${path}.descriptor`,
  );
}

function descriptorFromDeltaSharingFileActions(
  files: DeltaSharingFileAction[],
): BrowserHttpSnapshotDescriptor {
  const signedUrlCapabilities: CapabilityReport = {
    capabilities: {
      signed_url_access: 'supported',
    },
  };

  return {
    table_uri: 'delta-sharing://browser-source',
    snapshot_version: 0,
    partition_column_types: {},
    browser_compatibility: signedUrlCapabilities,
    required_capabilities: signedUrlCapabilities,
    active_files: files.map((file, index) =>
      normalizeBrowserHttpFileDescriptorWithHttpsUrl(
        file,
        `delta_sharing_url_files.files[${index}]`,
      ),
    ),
  };
}

function normalizeBrowserMaterializedDescriptor(
  input: unknown,
  path: string,
): BrowserHttpSnapshotDescriptor {
  const descriptor = normalizeBrowserHttpSnapshotDescriptor(input, path);
  descriptor.active_files.forEach((file, index) => {
    validateHttpsUrl(file.url, `${path}.active_files[${index}].url`);
  });
  return descriptor;
}

function normalizeBrowserHttpFileDescriptorWithHttpsUrl(
  input: unknown,
  path: string,
): BrowserHttpFileDescriptor {
  const file = normalizeBrowserHttpFileDescriptor(input, path);
  validateHttpsUrl(file.url, `${path}.url`);
  return file;
}

async function resolveDeltaLocationViaHttp(
  resolverUrl: string | URL,
  request: DeltaLocationResolveRequest,
): Promise<DeltaLocationResolveResponse> {
  if (typeof fetch !== 'function') {
    throw new DeltaLocationResolverError(
      'resolver_unavailable',
      'global fetch is not available for Delta location resolution',
    );
  }

  let response: Response;
  try {
    response = await fetch(resolverUrl, {
      method: 'POST',
      headers: {
        accept: 'application/json',
        'content-type': 'application/json',
      },
      body: JSON.stringify(request),
      credentials: 'same-origin',
    });
  } catch (error) {
    throw normalizeDeltaLocationResolverError(error);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch (error) {
    throw new DeltaLocationResolverError(
      'resolver_unavailable',
      `Delta location resolver returned non-JSON response (${response.status}): ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  if (!response.ok) {
    throw deltaLocationResolverErrorFromResponse(body, response.status);
  }

  return normalizeDeltaLocationResolveResponseEnvelope(body);
}

function deltaLocationResolverErrorFromResponse(
  body: unknown,
  status: number,
): DeltaLocationResolverError {
  if (isObject(body) && typeof body.code === 'string') {
    return new DeltaLocationResolverError(
      deltaLocationResolverErrorCode(body.code),
      typeof body.message === 'string'
        ? body.message
        : `Delta location resolver returned ${status}`,
      {
        correlationId: typeof body.correlation_id === 'string' ? body.correlation_id : undefined,
      },
    );
  }

  return new DeltaLocationResolverError(
    'resolver_unavailable',
    `Delta location resolver returned ${status}`,
  );
}

function normalizeDeltaLocationResolverError(error: unknown): DeltaLocationResolverError {
  if (error instanceof DeltaLocationResolverError) {
    return new DeltaLocationResolverError(error.code, error.message, {
      correlationId: error.correlationId,
    });
  }

  return new DeltaLocationResolverError(
    'resolver_unavailable',
    error instanceof Error ? error.message : String(error),
  );
}

function validateStorageAccessProfile(profile: CredentialProfileRef): void {
  if (!profile.id.trim()) {
    throw new DeltaLocationResolverError(
      'credential_profile_required',
      'storage access profile id is required',
    );
  }
  if (containsSecretMaterial(profile.id)) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      'storage access profile id must be an opaque policy handle, not cloud credential material',
    );
  }
}

function validateLogicalDeltaTableUri(provider: DeltaObjectStoreProvider, tableUri: string): void {
  if (containsSecretMaterial(tableUri)) {
    throw new DeltaLocationResolverError(
      'invalid_table_uri',
      'Delta table URI must not contain credential material or signed URL parameters',
    );
  }

  let parsed: URL;
  try {
    parsed = new URL(tableUri);
  } catch (error) {
    throw new DeltaLocationResolverError(
      'invalid_table_uri',
      `invalid Delta table URI: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  if (parsed.search || parsed.hash) {
    throw new DeltaLocationResolverError(
      'invalid_table_uri',
      `Delta table URI '${redactedUrl(parsed)}' must not include query strings or fragments`,
    );
  }

  if (provider === 's3') {
    if (
      parsed.protocol !== 's3:' ||
      !parsed.hostname ||
      !hasNonRootPath(parsed) ||
      hasUserinfo(parsed)
    ) {
      throw new DeltaLocationResolverError(
        'invalid_table_uri',
        'S3 Delta table URI must look like s3://bucket/table without userinfo',
      );
    }
    return;
  }

  if (provider === 'gcs') {
    if (
      parsed.protocol !== 'gs:' ||
      !parsed.hostname ||
      !hasNonRootPath(parsed) ||
      hasUserinfo(parsed)
    ) {
      throw new DeltaLocationResolverError(
        'invalid_table_uri',
        'GCS Delta table URI must look like gs://bucket/table without userinfo',
      );
    }
    return;
  }

  if (parsed.protocol === 'az:') {
    if (parsed.hostname && hasContainerAndTablePath(parsed) && !hasUserinfo(parsed)) {
      return;
    }
    throw new DeltaLocationResolverError(
      'invalid_table_uri',
      'Azure Blob Delta table URI must look like az://account/container/table',
    );
  }

  if (parsed.protocol === 'abfs:') {
    if (parsed.hostname && parsed.username && !parsed.password && hasNonRootPath(parsed)) {
      return;
    }
    throw new DeltaLocationResolverError(
      'invalid_table_uri',
      'ABFS Delta table URI must look like abfs://container@account.dfs.core.windows.net/table',
    );
  }

  throw new DeltaLocationResolverError(
    'invalid_table_uri',
    'Azure Blob resolver only accepts az:// or abfs:// Delta table URIs',
  );
}

function validateBrowserSafeDescriptorUrl(url: string, correlationId: string | undefined): void {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch (error) {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      `invalid browser-safe descriptor URL: ${error instanceof Error ? error.message : String(error)}`,
      { correlationId },
    );
  }

  if (parsed.protocol !== 'https:') {
    throw new DeltaLocationResolverError(
      'policy_blocked',
      `browser-safe descriptor URL '${redactedUrl(parsed)}' must use HTTPS`,
      { correlationId },
    );
  }
}

function hasUserinfo(url: URL): boolean {
  return Boolean(url.username || url.password);
}

function hasNonRootPath(url: URL): boolean {
  return url.pathname.replace(/^\/+|\/+$/g, '').length > 0;
}

function hasContainerAndTablePath(url: URL): boolean {
  const parts = url.pathname.split('/').filter(Boolean);
  return parts.length >= 2;
}

function deltaLocationResolverErrorCode(code: string): DeltaLocationResolverErrorCode {
  switch (code) {
    case 'invalid_table_uri':
    case 'invalid_snapshot_version':
    case 'credential_profile_required':
    case 'credential_profile_not_found':
    case 'provider_not_supported':
    case 'access_mode_not_supported':
    case 'snapshot_version_not_found':
    case 'not_a_delta_table':
    case 'resolver_unavailable':
    case 'storage_auth_failed':
    case 'storage_cors_failed':
    case 'descriptor_expired':
    case 'policy_blocked':
    case 'proxy_required':
    case 'signed_url_unavailable':
      return code;
    default:
      return 'resolver_unavailable';
  }
}

class DeltaSharingRestClient implements DeltaSharingClient {
  private readonly fetcher: DeltaSharingFetch;

  constructor(fetcher: DeltaSharingFetch) {
    this.fetcher = fetcher;
  }

  async connect(profile: DeltaSharingProfileInput): Promise<DeltaSharingSession> {
    const parsed = await parseDeltaSharingBearerProfile(profile);
    return new DeltaSharingBearerSession(parsed, this.fetcher);
  }
}

type ParsedDeltaSharingBearerProfile = {
  endpoint: string;
  bearerToken: string;
  displayName?: string;
  expiresAtEpochMs?: number;
};

class DeltaSharingBearerSession implements DeltaSharingSession {
  readonly profile: DeltaSharingConnectionProfile;
  readonly #token: string;
  readonly #fetcher: DeltaSharingFetch;

  constructor(parsed: ParsedDeltaSharingBearerProfile, fetcher: DeltaSharingFetch) {
    this.profile = {
      kind: 'delta_sharing',
      profileSource: 'uploaded_profile',
      endpoint: parsed.endpoint,
      authMode: 'bearer',
      ...(parsed.displayName ? { displayName: parsed.displayName } : {}),
    };
    this.#token = parsed.bearerToken;
    this.#fetcher = fetcher;
  }

  listShares(opts: PageOptions = {}): Promise<DeltaSharingPage<DeltaShareInfo>> {
    return this.getPage<DeltaShareInfo>(['shares'], opts);
  }

  listSchemas(
    share: string,
    opts: PageOptions = {},
  ): Promise<DeltaSharingPage<DeltaShareSchemaInfo>> {
    return this.getPage<DeltaShareSchemaInfo>(['shares', share, 'schemas'], opts);
  }

  listTables(
    share: string,
    schema: string,
    opts: PageOptions = {},
  ): Promise<DeltaSharingPage<DeltaShareTableInfo>> {
    return this.getPage<DeltaShareTableInfo>(['shares', share, 'schemas', schema, 'tables'], opts);
  }

  listAllTables(
    share: string,
    opts: PageOptions = {},
  ): Promise<DeltaSharingPage<DeltaShareTableInfo>> {
    return this.getPage<DeltaShareTableInfo>(['shares', share, 'all-tables'], opts);
  }

  async getTableVersion(table: DeltaSharingTableRef): Promise<number> {
    const response = await this.requestResponse(deltaSharingTablePath(table, 'version'), {
      method: 'GET',
      accept: 'application/json',
    });
    const headerVersion = parseDeltaSharingVersionHeader(response.headers);
    if (headerVersion !== undefined) {
      return headerVersion;
    }

    const text = await safeResponseText(response);
    const bodyVersion = parseDeltaSharingVersionBody(text);
    if (bodyVersion === undefined) {
      throw new DeltaSharingError(
        'table_version_not_found',
        `Delta Sharing version response did not contain a table version for ${deltaSharingTableLabel(table)}`,
      );
    }
    return bodyVersion;
  }

  async getTableMetadata(
    table: DeltaSharingTableRef,
    opts: DeltaSharingMetadataOptions = {},
  ): Promise<DeltaSharingTableMetadata> {
    const response = await this.requestResponse(deltaSharingTablePath(table, 'metadata'), {
      method: 'GET',
      accept: 'application/x-ndjson',
      query: opts.version === undefined ? undefined : { version: String(opts.version) },
    });
    const actions = parseDeltaSharingNdjson(await safeResponseText(response));
    const version = parseDeltaSharingVersionHeader(response.headers);
    return {
      table,
      version,
      protocol: firstActionPayload(actions, 'protocol'),
      metadata: firstActionPayload(actions, 'metaData') ?? firstActionPayload(actions, 'metadata'),
      rawActions: actions,
    };
  }

  async resolveTable(
    table: DeltaSharingTableRef,
    opts: OpenDeltaShareOptions = {},
  ): Promise<DeltaSharingReadPlan> {
    if (opts.responseFormat === 'delta') {
      throw new DeltaSharingError(
        'unsupported_response_format',
        'Delta Sharing responseformat=delta is not supported by this browser SDK build yet',
      );
    }

    const body: Record<string, unknown> = {};
    if (opts.version !== undefined) body.version = opts.version;
    if (opts.limitHint !== undefined) body.limitHint = opts.limitHint;
    if (opts.predicateHints !== undefined) body.predicateHints = opts.predicateHints;

    const response = await this.requestResponse(deltaSharingTablePath(table, 'query'), {
      method: 'POST',
      accept: 'application/x-ndjson',
      body,
      capabilities: deltaSharingCapabilitiesHeader(opts),
    });
    const actions = parseDeltaSharingNdjson(await safeResponseText(response));
    const resolvedVersion = parseDeltaSharingVersionHeader(response.headers) ?? opts.version;
    if (resolvedVersion === undefined) {
      throw new DeltaSharingError(
        'table_version_not_found',
        `Delta Sharing query response did not include a resolved table version for ${deltaSharingTableLabel(table)}`,
      );
    }

    return deltaSharingReadPlanFromActions({
      endpoint: this.profile.endpoint,
      table,
      requestedResponseFormat: opts.responseFormat ?? 'auto',
      resolvedVersion,
      actions,
    });
  }

  private async getPage<T>(
    path: readonly string[],
    opts: PageOptions,
  ): Promise<DeltaSharingPage<T>> {
    const body = await this.requestJson(path, {
      method: 'GET',
      accept: 'application/json',
      query: pageOptionsQuery(opts),
    });
    return normalizeDeltaSharingPage<T>(body);
  }

  private async requestJson(
    path: readonly string[],
    options: DeltaSharingRequestOptions,
  ): Promise<unknown> {
    const response = await this.requestResponse(path, options);
    try {
      return await response.json();
    } catch (error) {
      throw new DeltaSharingError(
        'server_error',
        `Delta Sharing response from ${deltaSharingUrl(this.profile.endpoint, path)} was not JSON: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private async requestResponse(
    path: readonly string[],
    options: DeltaSharingRequestOptions,
  ): Promise<Response> {
    const url = deltaSharingUrl(this.profile.endpoint, path, options.query);
    const headers = new Headers({
      accept: options.accept,
      authorization: `Bearer ${this.#token}`,
    });
    if (options.body !== undefined) {
      headers.set('content-type', 'application/json');
    }
    if (options.capabilities) {
      headers.set('delta-sharing-capabilities', options.capabilities);
    }

    let response: Response;
    try {
      response = await this.#fetcher(url, {
        method: options.method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
      });
    } catch (error) {
      throw new DeltaSharingError(
        'network_error',
        `Delta Sharing request to ${url} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    if (!response.ok) {
      const text = await safeResponseText(response);
      throw new DeltaSharingError(
        deltaSharingHttpErrorCode(response.status, path),
        `Delta Sharing request to ${url} failed (${response.status}): ${text}`,
        { status: response.status },
      );
    }

    return response;
  }
}

type DeltaSharingRequestOptions = {
  method: 'GET' | 'POST';
  accept: string;
  query?: Record<string, string>;
  body?: Record<string, unknown>;
  capabilities?: string;
};

async function parseDeltaSharingBearerProfile(
  input: DeltaSharingProfileInput,
): Promise<ParsedDeltaSharingBearerProfile> {
  if (input.source === 'oidc_url') {
    throw new DeltaSharingError(
      'auth_required',
      'Delta Sharing OIDC federation is not available in this browser SDK build yet',
    );
  }

  const value = await deltaSharingProfileInputValue(input);
  if (!isObject(value)) {
    throw new DeltaSharingError('invalid_profile', 'Delta Sharing profile must be a JSON object');
  }

  const endpoint = stringField(value, 'endpoint');
  const bearerToken =
    stringField(value, 'bearerToken') ??
    stringField(value, 'bearer_token') ??
    stringField(value, 'token');
  if (!endpoint) {
    throw new DeltaSharingError('invalid_profile', 'Delta Sharing profile endpoint is required');
  }
  if (!bearerToken) {
    throw new DeltaSharingError(
      'auth_required',
      'Delta Sharing bearer-token profile requires bearerToken',
    );
  }

  const expiresAtEpochMs = deltaSharingProfileExpirationEpochMs(value);
  if (expiresAtEpochMs !== undefined && expiresAtEpochMs <= Date.now()) {
    throw new DeltaSharingError('auth_expired', 'Delta Sharing bearer-token profile has expired');
  }

  return {
    endpoint: normalizeDeltaSharingEndpoint(endpoint),
    bearerToken,
    displayName: stringField(value, 'name') ?? stringField(value, 'displayName'),
    expiresAtEpochMs,
  };
}

async function deltaSharingProfileInputValue(input: DeltaSharingProfileInput): Promise<unknown> {
  if (input.source === 'file') {
    if (!isFileLike(input.value)) {
      throw new DeltaSharingError('invalid_profile', 'Delta Sharing profile file is not readable');
    }
    return parseProfileJson(await input.value.text());
  }
  if (input.source === 'json') {
    if (typeof input.value === 'string') {
      return parseProfileJson(input.value);
    }
    return input.value;
  }
  return input.value;
}

function parseProfileJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new DeltaSharingError(
      'invalid_profile',
      `Delta Sharing profile JSON is invalid: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

function deltaSharingProfileExpirationEpochMs(
  profile: Record<string, unknown>,
): number | undefined {
  const value =
    profile.expirationTime ?? profile.expiration_time ?? profile.expiresAt ?? profile.expires_at;
  if (value === undefined) return undefined;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  throw new DeltaSharingError(
    'invalid_profile',
    'Delta Sharing bearer-token profile expiration timestamp is invalid',
  );
}

function normalizeDeltaSharingEndpoint(endpoint: string): string {
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch (error) {
    throw new DeltaSharingError(
      'invalid_profile',
      `Delta Sharing endpoint is invalid: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  if (parsed.protocol !== 'https:') {
    throw new DeltaSharingError(
      'invalid_profile',
      `Delta Sharing endpoint '${redactedUrl(parsed)}' must use HTTPS`,
    );
  }
  parsed.username = '';
  parsed.password = '';
  parsed.search = '';
  parsed.hash = '';
  parsed.pathname = parsed.pathname.replace(/\/+$/g, '');
  return parsed.toString().replace(/\/+$/g, '');
}

function deltaSharingUrl(
  endpoint: string,
  path: readonly string[],
  query: Record<string, string> | undefined = undefined,
): string {
  const base = endpoint.endsWith('/') ? endpoint : `${endpoint}/`;
  const url = new URL(path.map((segment) => encodeURIComponent(segment)).join('/'), base);
  for (const [key, value] of Object.entries(query ?? {})) {
    url.searchParams.set(key, value);
  }
  return url.toString();
}

function deltaSharingTablePath(table: DeltaSharingTableRef, leaf: string): string[] {
  return ['shares', table.share, 'schemas', table.schema, 'tables', table.table, leaf];
}

function deltaSharingTableLabel(table: DeltaSharingTableRef): string {
  return `${table.share}.${table.schema}.${table.table}`;
}

function pageOptionsQuery(opts: PageOptions): Record<string, string> | undefined {
  const query: Record<string, string> = {};
  if (opts.pageToken !== undefined) query.pageToken = opts.pageToken;
  if (opts.maxResults !== undefined) query.maxResults = String(opts.maxResults);
  return Object.keys(query).length === 0 ? undefined : query;
}

function normalizeDeltaSharingPage<T>(body: unknown): DeltaSharingPage<T> {
  if (Array.isArray(body)) {
    return { items: body as T[] };
  }
  if (!isObject(body)) {
    throw new DeltaSharingError('server_error', 'Delta Sharing list response must be an object');
  }
  if (!Array.isArray(body.items)) {
    throw new DeltaSharingError(
      'server_error',
      'Delta Sharing list response items must be an array',
    );
  }
  return {
    items: body.items as T[],
    nextPageToken: optionalString(body.nextPageToken, 'nextPageToken'),
  };
}

function parseDeltaSharingNdjson(text: string): Array<Record<string, unknown>> {
  const actions: Array<Record<string, unknown>> = [];
  for (const [index, raw] of text.split(/\r?\n/).entries()) {
    const line = raw.trim();
    if (!line) continue;
    try {
      const parsed = JSON.parse(line);
      if (!isObject(parsed)) {
        throw new Error('action must be an object');
      }
      actions.push(parsed);
    } catch (error) {
      throw new DeltaSharingError(
        'server_error',
        `Delta Sharing NDJSON action ${index + 1} is invalid: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
  return actions;
}

function deltaSharingReadPlanFromActions(input: {
  endpoint: string;
  table: DeltaSharingTableRef;
  requestedResponseFormat: DeltaSharingResponseFormat;
  resolvedVersion: number;
  actions: Array<Record<string, unknown>>;
}): DeltaSharingReadPlan {
  const protocol = firstActionPayload(input.actions, 'protocol');
  const metadata =
    firstActionPayload(input.actions, 'metaData') ?? firstActionPayload(input.actions, 'metadata');
  const accessModes =
    actionStringArray(protocol, 'accessModes') ?? actionStringArray(metadata, 'accessModes');
  const actualResponseFormat = actualDeltaSharingResponseFormat(input.requestedResponseFormat);

  validateDeltaSharingReaderFeatures(protocol, metadata, actualResponseFormat);

  const files = input.actions.flatMap((action) => deltaSharingFileDescriptor(action));
  if (files.length === 0) {
    if (accessModes?.includes('dir')) {
      throw new DeltaSharingError(
        'directory_access_requires_resolver',
        'Delta Sharing directory access requires a trusted resolver before browser execution',
      );
    }
    throw new DeltaSharingError(
      'unsupported_access_mode',
      'Delta Sharing query response did not include URL-mode file actions',
    );
  }

  const expiresAtEpochMs = minimumDefined(files.map((file) => file.expiresAtEpochMs));
  return {
    kind: 'delta_sharing_snapshot_descriptor',
    table: input.table,
    endpoint: input.endpoint,
    resolvedVersion: input.resolvedVersion,
    responseFormat: actualResponseFormat,
    descriptor: {
      table_uri: deltaSharingDescriptorTableUri(input.endpoint, input.table),
      snapshot_version: input.resolvedVersion,
      partition_column_types: deltaSharingPartitionColumnTypes(metadata, files),
      browser_compatibility: { capabilities: { signed_url_access: 'supported' } },
      required_capabilities: { capabilities: { signed_url_access: 'supported' } },
      active_files: files.map(deltaSharingBrowserFile),
    },
    expiresAtEpochMs,
    warnings: [],
  };
}

function deltaSharingPartitionColumnTypes(
  metadata: Record<string, unknown> | undefined,
  files: Array<BrowserHttpFileDescriptor & { expiresAtEpochMs?: number }>,
): Partial<Record<string, PartitionColumnType>> {
  const partitionColumnTypes: Partial<Record<string, PartitionColumnType>> = {};
  const schemaTypes = deltaSharingSchemaPartitionColumnTypes(metadata);
  for (const [column, columnType] of Object.entries(schemaTypes)) {
    partitionColumnTypes[column] = columnType;
  }
  for (const file of files) {
    for (const column of Object.keys(file.partition_values)) {
      partitionColumnTypes[column] ??= 'string';
    }
  }
  return partitionColumnTypes;
}

function deltaSharingSchemaPartitionColumnTypes(
  metadata: Record<string, unknown> | undefined,
): Partial<Record<string, PartitionColumnType>> {
  const partitionColumns = actionStringArray(metadata, 'partitionColumns') ?? [];
  const schemaString = metadata ? stringField(metadata, 'schemaString') : undefined;
  if (partitionColumns.length === 0 || !schemaString) {
    return {};
  }

  let parsedSchema: unknown;
  try {
    parsedSchema = JSON.parse(schemaString);
  } catch {
    return {};
  }
  if (!isObject(parsedSchema) || !Array.isArray(parsedSchema.fields)) {
    return {};
  }

  const fieldsByName = new Map<string, Record<string, unknown>>();
  for (const field of parsedSchema.fields) {
    if (isObject(field)) {
      const name = stringField(field, 'name');
      if (name) fieldsByName.set(name, field);
    }
  }

  const columnTypes: Partial<Record<string, PartitionColumnType>> = {};
  for (const column of partitionColumns) {
    const field = fieldsByName.get(column);
    columnTypes[column] = deltaSharingPartitionTypeFromSchemaField(field);
  }
  return columnTypes;
}

function deltaSharingPartitionTypeFromSchemaField(
  field: Record<string, unknown> | undefined,
): PartitionColumnType {
  if (!field) return 'string';
  const type = field.type;
  if (typeof type !== 'string') return 'unsupported';
  switch (type.toLowerCase()) {
    case 'string':
      return 'string';
    case 'boolean':
      return 'boolean';
    case 'byte':
    case 'short':
    case 'integer':
    case 'int':
    case 'long':
      return 'int64';
    default:
      return 'unsupported';
  }
}

function deltaSharingBrowserFile(
  file: BrowserHttpFileDescriptor & { expiresAtEpochMs?: number },
): BrowserHttpFileDescriptor {
  return {
    path: file.path,
    url: file.url,
    size_bytes: file.size_bytes,
    partition_values: file.partition_values,
    ...(file.stats === undefined ? {} : { stats: file.stats }),
    ...(file.object_etag === undefined ? {} : { object_etag: file.object_etag }),
  };
}

function deltaSharingReadPlanMetadata(plan: DeltaSharingReadPlan): DeltaSharingReadPlanMetadata {
  return {
    kind: plan.kind,
    table: plan.table,
    endpoint: plan.endpoint,
    resolvedVersion: plan.resolvedVersion,
    responseFormat: plan.responseFormat,
    ...(plan.expiresAtEpochMs === undefined ? {} : { expiresAtEpochMs: plan.expiresAtEpochMs }),
    ...(plan.warnings === undefined ? {} : { warnings: plan.warnings }),
  };
}

function actualDeltaSharingResponseFormat(
  requested: DeltaSharingResponseFormat,
): 'delta' | 'parquet' {
  if (requested === 'delta') {
    throw new DeltaSharingError(
      'unsupported_response_format',
      'Delta Sharing responseformat=delta is not supported by this browser SDK build yet',
    );
  }
  return 'parquet';
}

function validateDeltaSharingReaderFeatures(
  protocol: Record<string, unknown> | undefined,
  metadata: Record<string, unknown> | undefined,
  responseFormat: 'delta' | 'parquet',
): void {
  const features = [
    ...(actionStringArray(protocol, 'readerFeatures') ?? []),
    ...(actionStringArray(metadata, 'readerFeatures') ?? []),
  ].map((feature) => feature.toLowerCase());
  const unsupported = features.filter(
    (feature) => feature === 'deletionvectors' || feature === 'columnmapping',
  );
  if (unsupported.length > 0 && responseFormat !== 'delta') {
    throw new DeltaSharingError(
      'unsupported_reader_feature',
      `Delta Sharing table requires reader features not supported in parquet response format: ${unsupported.join(', ')}`,
    );
  }
}

function deltaSharingFileDescriptor(
  action: Record<string, unknown>,
): Array<BrowserHttpFileDescriptor & { expiresAtEpochMs?: number }> {
  const payload = isObject(action.file)
    ? action.file
    : isObject(action.add)
      ? action.add
      : undefined;
  if (!payload) {
    return [];
  }

  const url = stringField(payload, 'url');
  if (!url) {
    return [];
  }
  validateDeltaSharingDescriptorUrl(url);
  const size = numberField(payload, 'size') ?? numberField(payload, 'size_bytes');
  if (size === undefined) {
    throw new DeltaSharingError(
      'server_error',
      `Delta Sharing file action for ${redactUrlSecrets(url)} did not include size`,
    );
  }

  return [
    {
      path: stringField(payload, 'id') ?? stringField(payload, 'path') ?? pathFromUrl(url),
      url,
      size_bytes: size,
      partition_values: partitionValues(payload.partitionValues ?? payload.partition_values),
      stats: stringField(payload, 'stats'),
      expiresAtEpochMs: expirationEpochMs(payload.expirationTimestamp ?? payload.expiration_time),
    },
  ];
}

function validateDeltaSharingDescriptorUrl(url: string): void {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch (error) {
    throw new DeltaSharingError(
      'server_error',
      `Delta Sharing file URL is invalid: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (parsed.protocol !== 'https:') {
    throw new DeltaSharingError(
      'unsupported_access_mode',
      `Delta Sharing URL-mode file '${redactedUrl(parsed)}' must use HTTPS`,
    );
  }
}

function partitionValues(value: unknown): Record<string, string | null> {
  if (value === undefined) return {};
  if (!isObject(value)) {
    throw new DeltaSharingError(
      'server_error',
      'Delta Sharing file partitionValues must be an object',
    );
  }
  const out: Record<string, string | null> = {};
  for (const [key, raw] of Object.entries(value)) {
    if (raw === null) {
      out[key] = null;
    } else if (typeof raw === 'string') {
      out[key] = raw;
    } else {
      out[key] = String(raw);
    }
  }
  return out;
}

function expirationEpochMs(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  throw new DeltaSharingError('server_error', 'Delta Sharing file expiration timestamp is invalid');
}

function pathFromUrl(url: string): string {
  const parsed = new URL(url);
  const path = parsed.pathname.split('/').filter(Boolean).pop();
  return path ?? redactedUrl(parsed);
}

function deltaSharingDescriptorTableUri(endpoint: string, table: DeltaSharingTableRef): string {
  const parsed = new URL(endpoint);
  const uri = new URL(
    `delta-sharing://${parsed.host}/${encodeURIComponent(table.share)}/${encodeURIComponent(
      table.schema,
    )}/${encodeURIComponent(table.table)}`,
  );
  const endpointPath = parsed.pathname.replace(/\/+$/g, '');
  if (endpointPath) {
    uri.searchParams.set('endpoint', endpointPath);
  }
  return uri.toString();
}

function deltaSharingCapabilitiesHeader(opts: OpenDeltaShareOptions): string | undefined {
  const parts: string[] = [];
  if (opts.responseFormat === 'parquet') {
    parts.push('responseformat=parquet');
  } else if (opts.responseFormat === 'delta') {
    parts.push('responseformat=delta');
  }
  if (opts.readerFeatures && opts.readerFeatures.length > 0) {
    parts.push(`readerfeatures=${opts.readerFeatures.join(',')}`);
  }
  return parts.length === 0 ? undefined : parts.join(';');
}

function firstActionPayload(
  actions: Array<Record<string, unknown>>,
  key: string,
): Record<string, unknown> | undefined {
  for (const action of actions) {
    if (isObject(action[key])) {
      return action[key];
    }
  }
  return undefined;
}

function actionStringArray(
  action: Record<string, unknown> | undefined,
  key: string,
): string[] | undefined {
  if (!action || !Array.isArray(action[key])) return undefined;
  const out: string[] = [];
  for (const value of action[key]) {
    if (typeof value === 'string') out.push(value);
  }
  return out;
}

function parseDeltaSharingVersionHeader(headers: Headers): number | undefined {
  const value =
    headers.get('delta-table-version') ??
    headers.get('Delta-Table-Version') ??
    headers.get('Delta-Sharing-Table-Version');
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseDeltaSharingVersionBody(text: string): number | undefined {
  const trimmed = text.trim();
  if (!trimmed) return undefined;
  const asNumber = Number.parseInt(trimmed, 10);
  if (String(asNumber) === trimmed) return asNumber;
  try {
    const parsed = JSON.parse(trimmed);
    if (isObject(parsed)) {
      return numberField(parsed, 'version') ?? numberField(parsed, 'tableVersion');
    }
  } catch {
    return undefined;
  }
  return undefined;
}

async function safeResponseText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch (error) {
    return `unreadable response body: ${error instanceof Error ? error.message : String(error)}`;
  }
}

function deltaSharingHttpErrorCode(status: number, path: readonly string[]): DeltaSharingErrorCode {
  if (status === 401 || status === 403) return 'auth_required';
  if (status === 404) return deltaSharingNotFoundErrorCode(path);
  if (status >= 500) return 'server_error';
  return 'server_error';
}

function deltaSharingNotFoundErrorCode(path: readonly string[]): DeltaSharingErrorCode {
  if (path[0] !== 'shares') return 'server_error';
  if (path.length <= 3) return 'share_not_found';
  if (path[2] !== 'schemas') return 'share_not_found';
  if (path.length <= 5) return 'schema_not_found';
  if (path[4] !== 'tables') return 'schema_not_found';
  return 'table_not_found';
}

function globalDeltaSharingFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  if (typeof fetch !== 'function') {
    return Promise.reject(new DeltaSharingError('network_error', 'global fetch is not available'));
  }
  return fetch(input, init);
}

function isFileLike(value: unknown): value is { text: () => Promise<string> } {
  return isObject(value) && typeof value.text === 'function';
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function numberField(value: Record<string, unknown>, key: string): number | undefined {
  return typeof value[key] === 'number' && Number.isFinite(value[key]) ? value[key] : undefined;
}

function minimumDefined(values: Array<number | undefined>): number | undefined {
  let min: number | undefined;
  for (const value of values) {
    if (value === undefined) continue;
    min = min === undefined ? value : Math.min(min, value);
  }
  return min;
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
  const delivery = result.delivery ?? 'single_buffer';
  if (delivery !== 'single_buffer' && delivery !== 'chunked_buffers') {
    throw new AxonProtocolError(`unknown Arrow IPC delivery '${String(delivery)}'`);
  }

  if (delivery === 'single_buffer') {
    if (result.bytes === undefined) {
      throw new AxonProtocolError('single_buffer Arrow IPC result requires bytes');
    }
    const bytes = normalizeBytes(result.bytes);
    const byteLength =
      result.byte_length === undefined
        ? bytes.byteLength
        : requiredNonNegativeInteger(result.byte_length, 'success.result.byte_length');
    if (byteLength !== bytes.byteLength) {
      throw new AxonProtocolError(
        `single_buffer Arrow IPC byte_length ${byteLength} did not match bytes length ${bytes.byteLength}`,
      );
    }
    const chunkCount =
      result.chunk_count === undefined
        ? bytes.byteLength === 0
          ? 0
          : 1
        : requiredNonNegativeInteger(result.chunk_count, 'success.result.chunk_count');
    if (chunkCount > 1) {
      throw new AxonProtocolError('single_buffer Arrow IPC chunk_count must be 0 or 1');
    }

    return {
      format: result.format,
      content_type: result.content_type,
      delivery,
      bytes,
      byte_length: byteLength,
      chunk_count: chunkCount,
    };
  }

  if (result.bytes !== undefined) {
    throw new AxonProtocolError('chunked_buffers Arrow IPC result must not include final bytes');
  }
  const byteLength = requiredNonNegativeInteger(result.byte_length, 'success.result.byte_length');
  const chunkCount = requiredNonNegativeInteger(result.chunk_count, 'success.result.chunk_count');
  if (byteLength === 0 && chunkCount !== 0) {
    throw new AxonProtocolError(
      'chunked_buffers Arrow IPC result with zero byte_length requires chunk_count 0',
    );
  }
  if (byteLength > 0 && chunkCount === 0) {
    throw new AxonProtocolError(
      'chunked_buffers Arrow IPC result with non-zero byte_length requires chunk_count > 0',
    );
  }

  return {
    format: result.format,
    content_type: result.content_type,
    delivery,
    bytes: new Uint8Array(0),
    byte_length: byteLength,
    chunk_count: chunkCount,
  };
}

type OpenPreparedDeltaTable = (
  name: string,
  descriptor: BrowserHttpSnapshotDescriptor,
  options?: AxonRequestOptions,
) => Promise<BrowserWorkerOpenedEnvelope>;

async function openDeltaLocationWithTableOpener(
  openDeltaTable: OpenPreparedDeltaTable,
  requestId: string | undefined,
  name: string,
  options: DeltaLocationOpenOptions,
): Promise<DeltaLocationOpenedEnvelope> {
  if ('source' in options) {
    const resolutionMode = options.resolutionMode ?? 'browser_local';
    const descriptor = browserDeltaSourceUsesSnapshotReconstruction(options.source)
      ? await resolveDeltaSnapshotBrowserLocal(options.source, {
          snapshotVersion: options.snapshotVersion,
          resolveBrowserSnapshot: options.resolveBrowserSnapshot,
        })
      : await materializeDescriptorFromBrowserSource(options.source);
    const opened = await openDeltaTable(name, descriptor, { requestId });
    return {
      ...opened,
      location: browserDeltaLocationOpenMetadata(options.source, descriptor, resolutionMode),
    };
  }

  const request = deltaLocationResolveRequestFromOptions(options);
  validateDeltaLocationResolveRequest(request);
  const response = await resolveDeltaLocationFromOptions(options, request);
  validateDeltaLocationResolveResponse(response, request, Date.now());
  const opened = await openDeltaTable(name, response.descriptor, { requestId });
  return {
    ...opened,
    location: deltaLocationOpenMetadata(response),
  };
}

async function openDeltaShareWithTableOpener(
  openDeltaTable: OpenPreparedDeltaTable,
  requestId: string | undefined,
  name: string,
  options: DeltaSharingOpenOptions,
): Promise<DeltaSharingOpenedEnvelope> {
  const plan = await options.session.resolveTable(options.table, {
    version: options.version,
    limitHint: options.limitHint,
    predicateHints: options.predicateHints,
    responseFormat: options.responseFormat,
    readerFeatures: options.readerFeatures,
  });
  const opened = await openDeltaTable(name, plan.descriptor, { requestId });
  return {
    ...opened,
    deltaSharing: deltaSharingReadPlanMetadata(plan),
  };
}

async function openUnityCatalogTableWithTableOpener(
  openDeltaTable: OpenPreparedDeltaTable,
  requestId: string | undefined,
  name: string,
  options: UnityCatalogOpenOptions,
): Promise<UnityCatalogOpenResult> {
  const request = unityCatalogReadAccessRequestFromOptions(options);
  const plan = parseReadAccessPlan(await options.resolveReadAccessPlan(request));
  if (plan.fullName !== options.fullName) {
    throw new AxonProtocolError(
      `Unity Catalog read plan '${plan.fullName}' did not match requested table '${options.fullName}'`,
    );
  }

  switch (plan.plan_type) {
    case 'brokered_delta': {
      validateReadAccessPlanNotExpired(plan, Date.now());
      if (!options.brokeredDelta) {
        throw new AxonSdkError('brokered Delta read plans require a brokeredDelta adapter');
      }
      const resolvedSnapshot = normalizeResolvedSnapshotDescriptor(
        await options.brokeredDelta.resolveSnapshot(plan),
        'brokered_delta.resolvedSnapshot',
      );
      const paths = resolvedSnapshot.active_files.map((file) => file.path);
      const signResponse = normalizeObjectGrantBatchSignResponse(
        await options.brokeredDelta.batchSign(plan, paths),
        'brokered_delta.batchSign',
      );
      validateObjectGrantSignedUrlsNotExpired(signResponse.signedUrls, Date.now());
      const signedUrlsByPath = Object.fromEntries(
        signResponse.signedUrls.map((signed) => [signed.path, signed.url]),
      );
      const descriptor = snapshotFromBrokeredDeltaReadPlan(
        plan,
        resolvedSnapshot,
        signedUrlsByPath,
      );
      const opened = await openDeltaTable(name, descriptor, { requestId });
      return {
        ...opened,
        status: 'opened',
        planType: 'brokered_delta',
        tableId: plan.tableId,
        fullName: plan.fullName,
        expiresAtEpochMs: plan.expiresAtEpochMs,
      };
    }
    case 'delta_sharing': {
      validateReadAccessPlanNotExpired(plan, Date.now());
      const descriptor = snapshotFromDeltaSharingReadAccessPlan(plan);
      const opened = await openDeltaTable(name, descriptor, { requestId });
      return {
        ...opened,
        status: 'opened',
        planType: 'delta_sharing',
        tableId: plan.tableId,
        fullName: plan.fullName,
        expiresAtEpochMs: plan.expiresAtEpochMs,
      };
    }
    case 'sql_fallback_required':
      if (options.serverFallbackEnabled === true && options.executeSqlFallback) {
        return {
          status: 'sql_fallback_routed',
          planType: 'sql_fallback_required',
          tableId: plan.tableId,
          fullName: plan.fullName,
          reason: plan.reason,
          message: plan.message,
          result: await options.executeSqlFallback(plan),
        };
      }
      return {
        status: 'sql_fallback_required',
        planType: 'sql_fallback_required',
        tableId: plan.tableId,
        fullName: plan.fullName,
        reason: plan.reason,
        message: plan.message,
        statementEndpoint: plan.statementEndpoint,
        warehouseRequired: true,
      };
    case 'blocked':
      return {
        status: 'blocked',
        planType: 'blocked',
        tableId: plan.tableId,
        fullName: plan.fullName,
        reason: plan.reason,
        message: plan.message,
      };
  }
}

class LazyAxonBrowserClient implements AxonBrowserClient {
  private client: AxonBrowserWorkerClient | undefined;
  private terminated = false;

  constructor(private readonly createClient: () => AxonBrowserWorkerClient) {}

  async openDeltaTable(
    name: string,
    snapshot: BrowserHttpSnapshotDescriptor,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerOpenedEnvelope> {
    return this.workerClient().openDeltaTable(name, snapshot, options);
  }

  openDeltaLocation(
    name: string,
    options: DeltaLocationServerSnapshotOpenOptions,
  ): Promise<DeltaLocationServerSnapshotOpenedEnvelope>;
  openDeltaLocation(
    name: string,
    options: DeltaLocationBrowserSourceOpenOptions,
  ): Promise<DeltaLocationBrowserOpenedEnvelope>;
  async openDeltaLocation(
    name: string,
    options: DeltaLocationOpenOptions,
  ): Promise<DeltaLocationOpenedEnvelope> {
    return openDeltaLocationWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.workerClient().openDeltaTable(tableName, descriptor, requestOptions),
      options.requestId,
      name,
      options,
    );
  }

  async openDeltaShare(
    name: string,
    options: DeltaSharingOpenOptions,
  ): Promise<DeltaSharingOpenedEnvelope> {
    return openDeltaShareWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.workerClient().openDeltaTable(tableName, descriptor, requestOptions),
      options.requestId,
      name,
      options,
    );
  }

  async openParquetDataset(
    name: string,
    dataset: BrowserHttpParquetDatasetDescriptor,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerOpenedEnvelope> {
    return this.workerClient().openParquetDataset(name, dataset, options);
  }

  async openUnityCatalogTable(
    name: string,
    options: UnityCatalogOpenOptions,
  ): Promise<UnityCatalogOpenResult> {
    return openUnityCatalogTableWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.workerClient().openDeltaTable(tableName, descriptor, requestOptions),
      options.requestId,
      name,
      options,
    );
  }

  async inspectParquet(
    name: string,
    path: string,
    options?: AxonRequestOptions,
  ): Promise<ParquetInspectionSummary> {
    return this.workerClient().inspectParquet(name, path, options);
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
    const client = this.workerClient();
    if (typeof request === 'string') {
      return client.query(name, request, options as AxonQueryOptions);
    }
    return client.query(name, request, options as AxonRequestOptions);
  }

  cancelQuery(queryId: string, options?: AxonRequestOptions): void {
    this.client?.cancelQuery(queryId, options);
  }

  async dispose(
    name: string,
    options?: AxonRequestOptions,
  ): Promise<BrowserWorkerDisposedEnvelope> {
    return this.workerClient().dispose(name, options);
  }

  terminate(): void {
    if (this.terminated) return;
    this.terminated = true;
    this.client?.terminate();
  }

  private workerClient(): AxonBrowserWorkerClient {
    if (this.terminated) {
      throw new AxonSdkError('Axon browser client has been terminated');
    }
    if (!this.client) {
      this.client = this.createClient();
    }
    return this.client;
  }
}

class AxonBrowserWorkerClient implements AxonBrowserClient {
  private readonly worker: Worker;
  private readonly requestId: RequestIdFactory;
  private readonly onEvent?: BrowserWorkerEventHandler;
  private readonly pending = new Map<string, PendingRequest>();
  private readonly tables = new Map<string, OpenedTableMetadata>();
  private readonly completedRequests = new Set<string>();
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
      try {
        if ('arrow_ipc_chunk' in message.event) {
          this.handleArrowIpcChunk(message.event.arrow_ipc_chunk);
        }
        this.onEvent?.(message.event);
      } catch (error) {
        this.rejectAll(toError(error));
      }
      return;
    }

    const response = message.response;
    const requestId = responseRequestId(response);
    const pending = this.pending.get(requestId);
    if (!pending) {
      return;
    }

    if ('error' in response) {
      this.pending.delete(requestId);
      this.rememberCompletedRequest(requestId);
      pending.reject(new AxonWorkerError(response.error));
      return;
    }

    if (!(pending.expected in response)) {
      this.pending.delete(requestId);
      this.rememberCompletedRequest(requestId);
      pending.reject(
        new AxonProtocolError(
          `worker response for request '${requestId}' did not match expected '${pending.expected}' envelope`,
        ),
      );
      return;
    }

    if ('success' in response) {
      try {
        this.finalizeArrowIpcSuccess(requestId, response.success, pending);
      } catch (error) {
        this.pending.delete(requestId);
        this.rememberCompletedRequest(requestId);
        pending.reject(toError(error));
        return;
      }
    }

    this.pending.delete(requestId);
    this.rememberCompletedRequest(requestId);
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
    this.tables.set(name, {
      table_uri: snapshot.table_uri,
      snapshot_version: snapshot.snapshot_version,
    });
    return opened;
  }

  async openParquetDataset(
    name: string,
    dataset: BrowserHttpParquetDatasetDescriptor,
    options: AxonRequestOptions = {},
  ): Promise<BrowserWorkerOpenedEnvelope> {
    const requestId = options.requestId ?? this.requestId();
    const response = await this.send(openParquetDatasetCommand(requestId, name, dataset), 'opened');
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
    this.tables.set(name, {
      table_uri: dataset.table_uri,
    });
    return opened;
  }

  async openDeltaLocation(
    name: string,
    options: DeltaLocationServerSnapshotOpenOptions,
  ): Promise<DeltaLocationServerSnapshotOpenedEnvelope>;
  async openDeltaLocation(
    name: string,
    options: DeltaLocationBrowserSourceOpenOptions,
  ): Promise<DeltaLocationBrowserOpenedEnvelope>;
  async openDeltaLocation(
    name: string,
    options: DeltaLocationOpenOptions,
  ): Promise<DeltaLocationOpenedEnvelope> {
    const requestId = options.requestId ?? this.requestId();
    return openDeltaLocationWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.openDeltaTable(tableName, descriptor, requestOptions),
      requestId,
      name,
      options,
    );
  }

  async openDeltaShare(
    name: string,
    options: DeltaSharingOpenOptions,
  ): Promise<DeltaSharingOpenedEnvelope> {
    const requestId = options.requestId ?? this.requestId();
    return openDeltaShareWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.openDeltaTable(tableName, descriptor, requestOptions),
      requestId,
      name,
      options,
    );
  }

  async openUnityCatalogTable(
    name: string,
    options: UnityCatalogOpenOptions,
  ): Promise<UnityCatalogOpenResult> {
    const requestId = options.requestId ?? this.requestId();
    return openUnityCatalogTableWithTableOpener(
      (tableName, descriptor, requestOptions) =>
        this.openDeltaTable(tableName, descriptor, requestOptions),
      requestId,
      name,
      options,
    );
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
    const response = await this.send(
      queryCommand(requestId, name, queryRequestWithBrowserSafeDefaults(queryRequest)),
      'success',
    );
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

  cancelQuery(queryId: string, options: AxonRequestOptions = {}): void {
    if (this.terminated) {
      throw new AxonSdkError('Axon browser client has been terminated');
    }

    const requestId = options.requestId ?? this.requestId();
    try {
      this.worker.postMessage(cancelQueryCommand(requestId, queryId));
    } catch (error) {
      throw toError(error);
    }
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
      this.rememberCompletedRequest(requestId);
      pending.reject(error);
    }
  }

  private handleArrowIpcChunk(chunk: BrowserWorkerArrowIpcChunkEvent): void {
    if (this.completedRequests.has(chunk.request_id)) {
      throw new AxonProtocolError(
        `received Arrow IPC chunk after request '${chunk.request_id}' completed`,
      );
    }
    const pending = this.pending.get(chunk.request_id);
    if (!pending) {
      throw new AxonProtocolError(`unknown Arrow IPC chunk request '${chunk.request_id}'`);
    }
    if (pending.expected !== 'success') {
      throw new AxonProtocolError(
        `Arrow IPC chunks are only valid for success requests, got '${pending.expected}'`,
      );
    }
    if (chunk.bytes.byteLength !== chunk.byte_length) {
      throw new AxonProtocolError(
        `Arrow IPC chunk byte_length ${chunk.byte_length} did not match bytes length ${chunk.bytes.byteLength}`,
      );
    }

    const chunks =
      pending.arrowIpcChunks ??
      (pending.arrowIpcChunks = {
        chunks: [],
        nextSequence: 0,
        nextByteOffset: 0,
      });
    if (chunk.sequence < chunks.nextSequence) {
      throw new AxonProtocolError(`duplicate Arrow IPC chunk sequence ${chunk.sequence}`);
    }
    if (chunk.sequence !== chunks.nextSequence) {
      throw new AxonProtocolError(
        `expected Arrow IPC chunk sequence ${chunks.nextSequence}, got ${chunk.sequence}`,
      );
    }
    if (chunk.byte_offset !== chunks.nextByteOffset) {
      throw new AxonProtocolError(
        `expected Arrow IPC chunk byte_offset ${chunks.nextByteOffset}, got ${chunk.byte_offset}`,
      );
    }

    chunks.chunks.push(chunk.bytes);
    chunks.nextSequence += 1;
    chunks.nextByteOffset += chunk.byte_length;
  }

  private finalizeArrowIpcSuccess(
    requestId: string,
    success: BrowserWorkerSuccessEnvelope,
    pending: PendingRequest,
  ): void {
    if (success.result.delivery === 'single_buffer') {
      if (pending.arrowIpcChunks && pending.arrowIpcChunks.nextSequence > 0) {
        throw new AxonProtocolError(
          `single_buffer Arrow IPC success for request '${requestId}' followed chunk events`,
        );
      }
      return;
    }

    if (success.result.byte_length === 0 && success.result.chunk_count === 0) {
      success.result.bytes = new Uint8Array(0);
      return;
    }

    const chunks = pending.arrowIpcChunks;
    if (!chunks || chunks.nextSequence === 0) {
      throw new AxonProtocolError(`missing Arrow IPC chunks for request '${requestId}'`);
    }
    if (
      chunks.nextByteOffset === success.result.byte_length &&
      chunks.nextSequence !== success.result.chunk_count
    ) {
      throw new AxonProtocolError(
        `Arrow IPC chunk count mismatch for request '${requestId}': got ${chunks.nextSequence}, expected ${success.result.chunk_count}`,
      );
    }
    if (chunks.nextSequence < success.result.chunk_count) {
      throw new AxonProtocolError(
        `missing Arrow IPC chunks for request '${requestId}': got ${chunks.nextSequence}, expected ${success.result.chunk_count}`,
      );
    }
    if (chunks.nextSequence !== success.result.chunk_count) {
      throw new AxonProtocolError(
        `Arrow IPC chunk count mismatch for request '${requestId}': got ${chunks.nextSequence}, expected ${success.result.chunk_count}`,
      );
    }
    if (chunks.nextByteOffset !== success.result.byte_length) {
      throw new AxonProtocolError(
        `Arrow IPC byte length mismatch for request '${requestId}': got ${chunks.nextByteOffset}, expected ${success.result.byte_length}`,
      );
    }

    const bytes = new Uint8Array(success.result.byte_length);
    let offset = 0;
    for (const chunk of chunks.chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    success.result.bytes = bytes;
  }

  private rememberCompletedRequest(requestId: string): void {
    this.completedRequests.add(requestId);
    if (this.completedRequests.size <= 128) {
      return;
    }
    const oldest = this.completedRequests.values().next().value;
    if (typeof oldest === 'string') {
      this.completedRequests.delete(oldest);
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
          prebootstrap_fail_open_count: optionalNumber(
            payload.prebootstrap_fail_open_count,
            'range_read_metrics.prebootstrap_fail_open_count',
          ),
          prebootstrap_files_pruned: optionalNumber(
            payload.prebootstrap_files_pruned,
            'range_read_metrics.prebootstrap_files_pruned',
          ),
          footer_reads_avoided: optionalNumber(
            payload.footer_reads_avoided,
            'range_read_metrics.footer_reads_avoided',
          ),
          prebootstrap_candidate_files: optionalNumber(
            payload.prebootstrap_candidate_files,
            'range_read_metrics.prebootstrap_candidate_files',
          ),
          row_groups_touched: requiredNumber(
            payload.row_groups_touched,
            'range_read_metrics.row_groups_touched',
          ),
          row_groups_skipped: requiredNumber(
            payload.row_groups_skipped,
            'range_read_metrics.row_groups_skipped',
          ),
          footer_reads: optionalNumber(payload.footer_reads, 'range_read_metrics.footer_reads'),
          bootstrap_footer_range_reads: optionalNumber(
            payload.bootstrap_footer_range_reads,
            'range_read_metrics.bootstrap_footer_range_reads',
          ),
          scan_footer_range_reads: optionalNumber(
            payload.scan_footer_range_reads,
            'range_read_metrics.scan_footer_range_reads',
          ),
          scan_data_range_reads: optionalNumber(
            payload.scan_data_range_reads,
            'range_read_metrics.scan_data_range_reads',
          ),
          duplicate_range_reads: optionalNumber(
            payload.duplicate_range_reads,
            'range_read_metrics.duplicate_range_reads',
          ),
          coalesced_range_reads: optionalNumber(
            payload.coalesced_range_reads,
            'range_read_metrics.coalesced_range_reads',
          ),
          coalesced_gap_bytes_fetched: optionalNumber(
            payload.coalesced_gap_bytes_fetched,
            'range_read_metrics.coalesced_gap_bytes_fetched',
          ),
          footer_cache_hits: optionalNumber(
            payload.footer_cache_hits,
            'range_read_metrics.footer_cache_hits',
          ),
          footer_cache_misses: optionalNumber(
            payload.footer_cache_misses,
            'range_read_metrics.footer_cache_misses',
          ),
          footer_range_reads_avoided: optionalNumber(
            payload.footer_range_reads_avoided,
            'range_read_metrics.footer_range_reads_avoided',
          ),
          footer_cache_degraded_identity_reads: optionalNumber(
            payload.footer_cache_degraded_identity_reads,
            'range_read_metrics.footer_cache_degraded_identity_reads',
          ),
          identity_present_range_reads: optionalNumber(
            payload.identity_present_range_reads,
            'range_read_metrics.identity_present_range_reads',
          ),
          identity_missing_range_reads: optionalNumber(
            payload.identity_missing_range_reads,
            'range_read_metrics.identity_missing_range_reads',
          ),
          range_cache_hits: optionalNumber(
            payload.range_cache_hits,
            'range_read_metrics.range_cache_hits',
          ),
          range_cache_misses: optionalNumber(
            payload.range_cache_misses,
            'range_read_metrics.range_cache_misses',
          ),
          range_cache_bytes_reused: optionalNumber(
            payload.range_cache_bytes_reused,
            'range_read_metrics.range_cache_bytes_reused',
          ),
          range_cache_bytes_stored: optionalNumber(
            payload.range_cache_bytes_stored,
            'range_read_metrics.range_cache_bytes_stored',
          ),
          range_cache_validation_misses: optionalNumber(
            payload.range_cache_validation_misses,
            'range_read_metrics.range_cache_validation_misses',
          ),
          range_cache_degraded_identity_reads: optionalNumber(
            payload.range_cache_degraded_identity_reads,
            'range_read_metrics.range_cache_degraded_identity_reads',
          ),
          range_readahead_requests: optionalNumber(
            payload.range_readahead_requests,
            'range_read_metrics.range_readahead_requests',
          ),
          range_readahead_bytes_fetched: optionalNumber(
            payload.range_readahead_bytes_fetched,
            'range_read_metrics.range_readahead_bytes_fetched',
          ),
          range_readahead_bytes_used: optionalNumber(
            payload.range_readahead_bytes_used,
            'range_read_metrics.range_readahead_bytes_used',
          ),
          range_readahead_wasted_bytes: optionalNumber(
            payload.range_readahead_wasted_bytes,
            'range_read_metrics.range_readahead_wasted_bytes',
          ),
          descriptor_resolution_count: optionalNumber(
            payload.descriptor_resolution_count,
            'range_read_metrics.descriptor_resolution_count',
          ),
          delta_log_manifest_list_count: optionalNumber(
            payload.delta_log_manifest_list_count,
            'range_read_metrics.delta_log_manifest_list_count',
          ),
          delta_log_manifest_list_duration_ms: optionalNumber(
            payload.delta_log_manifest_list_duration_ms,
            'range_read_metrics.delta_log_manifest_list_duration_ms',
          ),
          snapshot_resolve_count: optionalNumber(
            payload.snapshot_resolve_count,
            'range_read_metrics.snapshot_resolve_count',
          ),
          snapshot_resolve_duration_ms: optionalNumber(
            payload.snapshot_resolve_duration_ms,
            'range_read_metrics.snapshot_resolve_duration_ms',
          ),
          descriptor_cache_hit: optionalNumber(
            payload.descriptor_cache_hit,
            'range_read_metrics.descriptor_cache_hit',
          ),
          session_reuse_count: optionalNumber(
            payload.session_reuse_count,
            'range_read_metrics.session_reuse_count',
          ),
          opened_table_reuse_count: optionalNumber(
            payload.opened_table_reuse_count,
            'range_read_metrics.opened_table_reuse_count',
          ),
          identity_refresh_count: optionalNumber(
            payload.identity_refresh_count,
            'range_read_metrics.identity_refresh_count',
          ),
          access_envelope_refresh_count: optionalNumber(
            payload.access_envelope_refresh_count,
            'range_read_metrics.access_envelope_refresh_count',
          ),
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
          arrow_ipc_bytes: optionalNumber(
            payload.arrow_ipc_bytes,
            'range_read_metrics.arrow_ipc_bytes',
          ),
          arrow_ipc_chunk_count: optionalNumber(
            payload.arrow_ipc_chunk_count,
            'range_read_metrics.arrow_ipc_chunk_count',
          ),
          preview_rows: optionalNumber(payload.preview_rows, 'range_read_metrics.preview_rows'),
          preview_string_bytes: optionalNumber(
            payload.preview_string_bytes,
            'range_read_metrics.preview_string_bytes',
          ),
          planning_duration_ms: optionalNumber(
            payload.planning_duration_ms,
            'range_read_metrics.planning_duration_ms',
          ),
          arrow_ipc_encode_duration_ms: optionalNumber(
            payload.arrow_ipc_encode_duration_ms,
            'range_read_metrics.arrow_ipc_encode_duration_ms',
          ),
          preview_duration_ms: optionalNumber(
            payload.preview_duration_ms,
            'range_read_metrics.preview_duration_ms',
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
    case 'arrow_ipc_chunk':
      return {
        arrow_ipc_chunk: {
          context: normalizeWorkerEventContext(payload.context, 'arrow_ipc_chunk.context'),
          request_id: requiredString(payload.request_id, 'arrow_ipc_chunk.request_id'),
          sequence: requiredNonNegativeInteger(payload.sequence, 'arrow_ipc_chunk.sequence'),
          byte_offset: requiredNonNegativeInteger(
            payload.byte_offset,
            'arrow_ipc_chunk.byte_offset',
          ),
          byte_length: requiredNonNegativeInteger(
            payload.byte_length,
            'arrow_ipc_chunk.byte_length',
          ),
          bytes: normalizeBytes(payload.bytes as number[] | ArrayBuffer | Uint8Array),
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

function requiredObject(value: unknown, path: string): Record<string, unknown> {
  if (!isObject(value)) {
    throw new AxonProtocolError(`${path} must be an object`);
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

function requiredPositiveInteger(value: unknown, path: string): number {
  const number = requiredNumber(value, path);
  if (!Number.isInteger(number) || number <= 0) {
    throw new AxonProtocolError(`${path} must be a positive integer`);
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

function redactCredentialSecrets(message: string): string {
  return message
    .replace(
      /\bAuthorization\s*:\s*(?:[A-Za-z][A-Za-z0-9._-]*\s+)?[^\s'"<>]+/gi,
      'Authorization: [REDACTED]',
    )
    .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/gi, 'Bearer [REDACTED]')
    .replace(
      /"?\b(service_account|private_key|client_secret|refresh_token|client_email)"?\s*:\s*"[^"]*"/gi,
      '[REDACTED]',
    )
    .replace(
      /\b(AWS_SECRET_ACCESS_KEY|AWS_ACCESS_KEY_ID|AZURE_CLIENT_SECRET|GOOGLE_APPLICATION_CREDENTIALS)\s*[:=]\s*[^\s'"<>]+/gi,
      '$1=[REDACTED]',
    )
    .replace(/\b(AKIA|ASIA)[A-Z0-9]{16}\b/g, '[REDACTED]')
    .replace(
      /\b(X-Amz-Signature|X-Goog-Signature|X-Goog-Credential|AWSAccessKeyId|access_token|signature|sig|sv|se|sp|skoid|sktid|skt|ske|sks|skv)=([^&\s]+)/gi,
      '$1=[REDACTED]',
    );
}

function containsSecretMaterial(value: string): boolean {
  const lower = value.toLowerCase();
  return (
    lower.includes('service_account') ||
    lower.includes('private_key') ||
    lower.includes('client_secret') ||
    lower.includes('refresh_token') ||
    lower.includes('authorization:') ||
    lower.includes('bearer ') ||
    lower.includes('aws_secret_access_key') ||
    lower.includes('azure_client_secret') ||
    lower.includes('google_application_credentials') ||
    lower.includes('x-amz-signature') ||
    lower.includes('x-goog-signature') ||
    lower.includes('x-goog-credential') ||
    lower.includes('awsaccesskeyid') ||
    lower.includes('access_token') ||
    lower.includes('signature=') ||
    lower.includes('sig=') ||
    lower.includes('sv=') ||
    /\b(AKIA|ASIA)[A-Z0-9]{16}\b/.test(value)
  );
}

function redactedUrl(url: URL): string {
  const clone = new URL(url.toString());
  clone.username = '';
  clone.password = '';
  clone.search = '';
  clone.hash = '';
  return clone.toString();
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
  'arrow_ipc_chunk',
] as const;
const WORKER_ENVELOPE_TAGS = [...WORKER_RESPONSE_TAGS, ...WORKER_EVENT_TAGS] as const;

type WorkerEnvelopeTag = (typeof WORKER_ENVELOPE_TAGS)[number];
type WorkerEventTag = (typeof WORKER_EVENT_TAGS)[number];

const EXECUTION_TARGETS = ['browser_wasm', 'native'] as const satisfies readonly ExecutionTarget[];
const DELTA_OBJECT_STORE_PROVIDERS = [
  's3',
  'gcs',
  'azure_blob',
] as const satisfies readonly DeltaObjectStoreProvider[];
const RESOLVER_REQUESTED_ACCESS_MODES = [
  'auto',
  'signed_url',
  'proxy',
] as const satisfies readonly ResolverRequestedAccessMode[];
const RESOLVER_ACTUAL_ACCESS_MODES = [
  'signed_url',
  'proxy',
] as const satisfies readonly ResolverActualAccessMode[];
const PARTITION_COLUMN_TYPES = [
  'string',
  'int64',
  'boolean',
  'unsupported',
] as const satisfies readonly PartitionColumnType[];
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
const BROKERED_DELTA_ACCESS_MODES = [
  'delta_log',
  'presigned_files',
] as const satisfies readonly BrokeredDeltaAccessMode[];
const POLICY_AUTHORITY_KINDS = [
  'unity_catalog',
  'delta_sharing',
  'mock_broker',
] as const satisfies readonly PolicyAuthorityKind[];
const DIRECT_EXTERNAL_ENGINE_READ_SUPPORT = [
  'confirmed',
  'not_confirmed',
] as const satisfies readonly DirectExternalEngineReadSupport[];
const READ_ACCESS_PLAN_REASONS = [
  'row_filter',
  'column_mask',
  'view',
  'unknown_policy_state',
  'no_direct_external_engine_read_support',
  'unsupported_table_type',
  'grant_expired',
  'storage_cors_blocked',
  'broker_unavailable',
] as const satisfies readonly ReadAccessPlanReason[];
const FORBIDDEN_BROWSER_SECRET_KEYS = new Set([
  'aws_sts',
  'aws_access_key_id',
  'aws_secret_access_key',
  'aws_session_token',
  'azure_sas',
  'gcp_oauth_token',
  'gcp_hmac_secret',
  'databricks_bearer_token',
  'databricks_token',
  'personal_access_token',
  'refresh_token',
  'pat',
  'signing_secret',
]);
const QUERY_ERROR_CODES = [
  'access_denied',
  'execution_failed',
  'fallback_required',
  'invalid_request',
  'object_not_found',
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
