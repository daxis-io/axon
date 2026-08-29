// WCRPC draft reference types.
// These are intentionally framework-neutral and should be treated as a contract sketch.

export type WcrpcProtocolVersion = 1;

export type WcrpcPayloadCodec =
  | "arrow.ipc.stream.chunk"
  | "arrow.record_batch.buffers"
  | "datafusion.aggregate_state.native"
  | "shared_slab.arrow_buffers";

export type WcrpcStatusCode =
  | "ok"
  | "cancelled"
  | "deadline_exceeded"
  | "resource_exhausted"
  | "invalid_argument"
  | "failed_precondition"
  | "schema_mismatch"
  | "unsupported_codec"
  | "sequence_error"
  | "internal"
  | "transport_closed";

export type ExchangeMode =
  | { kind: "gather"; target: WorkerId }
  | { kind: "hash"; keyColumns: number[]; targets: WorkerId[] }
  | { kind: "range"; sortKeys: SortKey[]; ranges: RangeBoundary[]; targets: WorkerId[] }
  | { kind: "broadcast"; targets: WorkerId[] }
  | { kind: "round_robin"; targets: WorkerId[] }
  | { kind: "top_k"; sortKeys: SortKey[]; limit: number; offset: number; target: WorkerId };

export type WorkerId = string;
export type SortKey = { columnIndex: number; descending: boolean; nullsFirst: boolean };
export type RangeBoundary = { encodedKey: Uint8Array; inclusive: boolean };

export type BufferRef =
  | { kind: "transferable"; bufferIndex: number; byteOffset: number; byteLength: number }
  | { kind: "shared_slab"; slabId: number; offset: number; byteLength: number; generation: number };

export type WcrpcFrameKind =
  | "headers"
  | "schema"
  | "dictionary"
  | "data"
  | "aggregate_state"
  | "credit"
  | "cancel"
  | "release"
  | "metrics"
  | "trailers"
  | "error";

export type WcrpcFrameEnvelope<T = unknown> = {
  protocol: "wcrpc";
  version: WcrpcProtocolVersion;
  queryId: string;
  stageId: number;
  streamId: number;
  seq: number;
  kind: WcrpcFrameKind;
  payload: T;
};

export type StreamBudget = {
  maxScanBytes?: number;
  maxOutputBytes?: number;
  maxOutputRows?: number;
  maxIntermediateBytes?: number;
  maxIntermediateRows?: number;
  maxBatchesInFlight?: number;
  maxBytesInFlight?: number;
};

export type StreamHeadersFrame = {
  method: "OpenTable" | "ExecuteShard" | "ExecuteStage" | "Merge" | "GetMetrics";
  deadlineMs?: number;
  tableName?: string;
  originalSql?: string;
  shardSql?: string;
  planId: string;
  mergePlanId?: string;
  stage: { stageId: number; kind: "scan" | "partial_aggregate" | "merge" | "top_k" | "final" };
  partition: { inputPartitionId?: number; outputPartitionId?: number; shardId?: number };
  payloadCodec: WcrpcPayloadCodec;
  exchangeMode: ExchangeMode;
  schema?: { expectedSchemaId?: string; expectedSchemaFingerprint?: string };
  budget: StreamBudget;
};

export type SchemaFrame = {
  frameKind: "schema";
  schemaId: string;
  schemaFingerprint: string;
  arrowSchemaIpc?: BufferRef;
  fieldMetadataPolicy: "exact" | "ignore_non_semantic";
  dictionaryPolicy: "none" | "preserve" | "unify" | "reject";
};

export type RecordBatchFrame = {
  frameKind: "record_batch";
  schemaId: string;
  sourceTaskId: number;
  inputPartitionId: number;
  outputPartitionId: number;
  rows: number;
  bytes: number;
  codec: WcrpcPayloadCodec;
  buffers: BufferRef[];
};

export type AggregateStateDescriptor = {
  outputName: string;
  function: "count" | "sum" | "min" | "max" | "avg";
  inputColumns: number[];
  stateColumns: number[];
  nullSemantics: "sql";
  outputType: string;
};

export type AggregateStateBatchFrame = {
  frameKind: "aggregate_state_batch";
  schemaId: string;
  mergePlanId: string;
  sourceTaskId: number;
  inputPartitionId: number;
  outputPartitionId: number;
  rows: number;
  groupKeyColumns: number[];
  aggregateStates: AggregateStateDescriptor[];
  codec: WcrpcPayloadCodec;
  buffers: BufferRef[];
};

export type CreditFrame = {
  additionalBytes: number;
  additionalBatches: number;
  reason: "initial" | "merge_progress" | "release" | "manual";
};

export type CancelFrame = {
  queryId: string;
  reason: "user" | "deadline" | "budget" | "sibling_failed" | "coordinator_shutdown";
  message?: string;
};

export type ErrorFrame = {
  code: WcrpcStatusCode;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};

export type WcrpcStreamMetrics = {
  workerId: string;
  stageId: number;
  streamId: number;
  framesSent: number;
  framesReceived: number;
  rowsSent: number;
  bytesSent: number;
  batchesSent: number;
  transferredBytes: number;
  sharedBytes?: number;
  maxInFlightBytes: number;
  creditsGranted: number;
  creditsConsumed: number;
  blockedOnCreditMs: number;
  firstFrameMs?: number;
  lastFrameMs?: number;
  executeMs?: number;
  encodeMs?: number;
  transferMs?: number;
  decodeMs?: number;
  mergeMs?: number;
};

export type TrailersFrame = {
  status: WcrpcStatusCode;
  message?: string;
  metrics: WcrpcStreamMetrics;
  error?: ErrorFrame;
};

export type WorkerPoolDecision =
  | { kind: "worker_pool"; plan: ShardedQueryPlan }
  | { kind: "single_worker"; reason: WorkerPoolIneligibleReason }
  | { kind: "unsupported"; reason: UnsupportedBrowserQueryReason };

export type WorkerPoolIneligibleReason =
  | "too_few_files"
  | "too_few_estimated_bytes"
  | "unsupported_query_shape"
  | "estimated_output_too_large"
  | "capability_mismatch"
  | "worker_pool_disabled";

export type UnsupportedBrowserQueryReason =
  | "unsupported_sql_shape"
  | "unsupported_data_type"
  | "unsupported_merge_semantics"
  | "unsupported_browser_capability";

export type ShardedQueryPlan = {
  queryId: string;
  originalSql: string;
  finalSchema: ArrowSchemaDescriptor;
  candidateFiles: CandidateFile[];
  shardDescriptors: ShardDescriptor[];
  stages: StagePlan[];
  mergePlan?: MergePlan;
  budgets: QueryBudget;
};

export type ArrowSchemaDescriptor = { fingerprint: string; fields: Array<{ name: string; type: string; nullable: boolean }> };
export type CandidateFile = { path: string; sizeBytes?: number; partitionValues?: Record<string, string> };
export type ShardDescriptor = { shardId: number; files: CandidateFile[] };
export type QueryBudget = { maxWorkers: number; maxInputBytes?: number; maxOutputBytes?: number; maxInFlightBytes?: number; deadlineMs?: number };
export type StagePlan = { stageId: number; kind: string; tasks: TaskPlan[]; exchange: ExchangeMode; outputSchema: ArrowSchemaDescriptor; payloadCodec: WcrpcPayloadCodec };
export type TaskPlan = { taskId: number; shardId?: number; inputPartitionId: number; outputPartitionIds: number[]; budget: StreamBudget };
export type MergePlan = { mergePlanId: string; kind: "append" | "global_aggregate" | "grouped_aggregate" | "top_k" | "distinct"; inputSchema: ArrowSchemaDescriptor; outputSchema: ArrowSchemaDescriptor };
