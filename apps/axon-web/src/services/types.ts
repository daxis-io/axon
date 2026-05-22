import type {
  CapabilityKey,
  CapabilityReport,
  CapabilityState,
  ExecutionTarget,
  FallbackReason,
  QueryMetricsSummary,
} from '../axon-browser-sdk.ts';

export type {
  CapabilityKey,
  CapabilityReport,
  CapabilityState,
  ExecutionTarget,
  FallbackReason,
  QueryMetricsSummary,
};

// ─── Catalog ────────────────────────────────────────────────────────────────
// UI-shaped catalog. Source of truth is the SDK's BrowserHttpSnapshotDescriptor +
// the Delta log; CatalogService normalizes both into this shape.

export type CatalogColumnRole = 'data' | 'partition' | 'system';

export type CatalogColumn = {
  name: string;
  type: string;
  role: CatalogColumnRole;
  pk?: boolean;
  fk?: string;
  nullable?: boolean;
};

export type CatalogProtocol = {
  minReaderVersion: number;
  minWriterVersion: number;
  features: string[];
};

export type CatalogPartitionColumn = {
  name: string;
  type: string;
  pruning?: 'stats' | 'none';
};

export type CatalogTable = {
  name: string;
  uri: string;
  kind: 'delta';
  snapshot: number;
  created?: string;
  last_commit?: string;
  size_bytes: number;
  row_count: number;
  file_count: number;
  row_group_count: number;
  partition_columns: CatalogPartitionColumn[];
  protocol: CatalogProtocol;
  columns: CatalogColumn[];
};

export type Catalog = {
  name: string;
  region: string;
  storage: string;
  tables: CatalogTable[];
};

// ─── Query ──────────────────────────────────────────────────────────────────

export type ResultColumn = {
  name: string;
  type: string;
};

export type ResultCell = string | number | boolean | null;

export type QueryResultPageInfo = {
  offset: number;
  size: number;
  returned_rows: number;
  loaded_rows: number;
  has_more: boolean;
  next_offset?: number;
};

export type QueryResultData = {
  columns: ResultColumn[];
  rows: ResultCell[][];
  row_count: number;
  truncated: boolean;
  page?: QueryResultPageInfo;
};

export type QueryPageRequest = {
  offset: number;
  size: number;
};

export type QueryRunStatus = 'idle' | 'running' | 'done' | 'error';

export type QueryProgressStage =
  | 'started'
  | 'planning'
  | 'executing'
  | 'arrow_ipc_ready'
  | 'finished';

export type QueryProgressEvent = {
  kind: 'progress';
  stage: QueryProgressStage;
  elapsed_ms: number;
};

export type QueryLogEvent = {
  kind: 'log';
  level: 'debug' | 'info' | 'warn' | 'error';
  message: string;
  elapsed_ms: number;
};

export type QueryMetricsEvent = {
  kind: 'metrics';
  metrics: QueryMetricsSummary;
  elapsed_ms: number;
};

export type QueryFallbackEvent = {
  kind: 'fallback';
  reason: FallbackReason;
  elapsed_ms: number;
};

export type QueryEvent =
  | QueryProgressEvent
  | QueryLogEvent
  | QueryMetricsEvent
  | QueryFallbackEvent;

export type QueryRunResult = {
  status: 'done';
  result: QueryResultData;
  metrics: QueryMetricsSummary;
  executed_on: ExecutionTarget;
  capabilities: CapabilityReport;
  fallback_reason?: FallbackReason;
  explain?: string;
  elapsed_ms: number;
};

export type QueryRunError = {
  status: 'error';
  message: string;
  code?: string;
  target?: ExecutionTarget;
  fallback_reason?: FallbackReason;
  elapsed_ms: number;
};

export type QueryRunOutcome = QueryRunResult | QueryRunError;

export type QueryExecRequest = {
  sql: string;
  table_name: string;
  preferred_target: ExecutionTarget | 'auto';
  snapshot_version?: number;
  page?: QueryPageRequest;
};

// ─── History ────────────────────────────────────────────────────────────────

export type HistoryEntry = {
  id: string;
  time: string; // HH:MM:SS local
  iso: string; // full ISO timestamp
  ms: number;
  rows: number;
  status: 'ok' | 'error';
  target: ExecutionTarget;
  fallback?: string | null;
  sql: string;
};

// ─── Capabilities ───────────────────────────────────────────────────────────

export type CapabilityMatrixRow = {
  key: CapabilityKey;
  label: string;
  desc: string;
  browser: CapabilityState;
  native: CapabilityState;
};

// ─── Engine status ──────────────────────────────────────────────────────────

export type EngineCacheStatus = {
  opfs_used_mb: number;
  opfs_budget_mb: number;
  memory_mb: number;
  extents: number;
  hit_ratio: number;
};

export type EngineStatus = {
  bundle: string;
  bundle_tier: string;
  available_tiers: string[];
  active_tier: string;
  wasm_size_kb: number;
  cold_start_ms: number;
  worker_mem_mb: number;
  cache: EngineCacheStatus;
  proto: string;
};

// ─── Snapshot / commit log ──────────────────────────────────────────────────

export type CommitOp = 'MERGE' | 'WRITE' | 'DELETE' | 'OPTIMIZE' | 'CREATE TABLE' | 'UNKNOWN';

export type CommitEntry = {
  v: number;
  ts: string;
  op: CommitOp;
  author: string;
  adds: number;
  removes: number;
  checkpoint?: boolean;
  current?: boolean;
  note: string;
};

// ─── Saved queries ──────────────────────────────────────────────────────────

export type SavedQuery = {
  id: string;
  name: string;
  owner: string;
  edited: string;
  target: ExecutionTarget;
  sql: string;
};

// ─── Plan ───────────────────────────────────────────────────────────────────

export type PlanFileStatus = 'touched' | 'partition_skipped' | 'row_group_skipped';

export type PlanFile = {
  path: string;
  status: PlanFileStatus;
  size: number;
  rows: number;
  row_groups: number;
  partitions: Record<string, string>;
  reason?: string;
};

export type PlanSummary = {
  tree: string;
  files?: PlanFile[];
};
