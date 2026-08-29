# WCRPC Protocol Specification

## 1. Protocol identity

**Name:** WCRPC, Wasm Columnar RPC  
**Version:** `0.1-draft`  
**Scope:** Internal browser-local RPC and exchange protocol for Wasm analytical workers.  
**Primary transport v0:** `MessagePort` / `Worker.postMessage` with transferable `ArrayBuffer`s.  
**Future transport:** `SharedArrayBuffer` slabs with descriptor-only data frames.

## 2. Design principles

1. Control metadata is small.
2. Analytical payloads are columnar.
3. Rows are never serialized as JSON/protobuf on the hot path.
4. Every data stream is schema-first.
5. Every stream is credit-gated.
6. Every stream terminates with trailers.
7. Cancellation is idempotent and query-scoped.
8. Payload codecs are negotiated per stream.
9. Transport and payload codec are separate.
10. Public SDK protocol remains stable.

## 3. Stream lifecycle

```text
Client/coordinator                         Worker/executor
------------------                         ---------------
HEADERS ExecuteShard  ------------------->
CREDIT initial window ------------------->
                                      <--- SCHEMA
                                      <--- DICTIONARY? 
                                      <--- DATA batch 0
                                      <--- DATA batch 1
CREDIT additional window ---------------->
                                      <--- DATA batch N
                                      <--- TRAILERS ok/error
```

A stream is uniquely identified by:

```text
query_id + stage_id + stream_id
```

A frame is uniquely ordered by:

```text
query_id + stream_id + seq
```

## 4. Frame envelope

```ts
type WcrpcFrameEnvelope = {
  protocol: "wcrpc";
  version: 1;
  queryId: string;
  stageId: number;
  streamId: number;
  seq: number;
  kind:
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
  payload: unknown;
};
```

Large buffers must be passed out-of-band in the transfer list or via shared-slab references.

## 5. Headers

```ts
type StreamHeadersFrame = {
  method: "OpenTable" | "ExecuteShard" | "ExecuteStage" | "Merge" | "GetMetrics";
  deadlineMs?: number;
  tableName?: string;
  originalSql?: string;
  shardSql?: string;
  planId: string;
  mergePlanId?: string;
  stage: {
    stageId: number;
    kind: "scan" | "partial_aggregate" | "merge" | "top_k" | "final";
  };
  partition: {
    inputPartitionId?: number;
    outputPartitionId?: number;
    shardId?: number;
  };
  payloadCodec: WcrpcPayloadCodec;
  exchangeMode: ExchangeMode;
  schema?: {
    expectedSchemaId?: string;
    expectedSchemaFingerprint?: string;
  };
  budget: StreamBudget;
};
```

## 6. Schema frame

Every data stream must send a schema frame before data frames.

```ts
type SchemaFrame = {
  frameKind: "schema";
  schemaId: string;
  schemaFingerprint: string;
  arrowSchemaIpc?: BufferRef;
  fieldMetadataPolicy: "exact" | "ignore_non_semantic";
  dictionaryPolicy: "none" | "preserve" | "unify" | "reject";
};
```

v0 rules:

- projection/filter streams require exact schema fingerprint equality;
- aggregate streams require exact partial-state schema equality against the merge plan;
- dictionary arrays are rejected or normalized explicitly;
- field metadata is exact unless a narrower compatibility policy is reviewed and approved.

## 7. Data frames

### 7.1 Record batch frame

```ts
type RecordBatchFrame = {
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
```

### 7.2 Aggregate state batch frame

```ts
type AggregateStateBatchFrame = {
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
```

### 7.3 Dictionary frame

```ts
type DictionaryFrame = {
  frameKind: "dictionary";
  schemaId: string;
  dictionaryId: number;
  replacement: boolean;
  rows?: number;
  buffers: BufferRef[];
};
```

## 8. Buffer references

```ts
type BufferRef =
  | {
      kind: "transferable";
      bufferIndex: number;
      byteOffset: number;
      byteLength: number;
    }
  | {
      kind: "shared_slab";
      slabId: number;
      offset: number;
      byteLength: number;
      generation: number;
    };
```

v0 uses `transferable`. v1+ may use `shared_slab` only when browser deployment guarantees cross-origin isolation and shared memory is enabled.

## 9. Payload codecs

```ts
type WcrpcPayloadCodec =
  | "arrow.ipc.stream.chunk"
  | "arrow.record_batch.buffers"
  | "datafusion.aggregate_state.native"
  | "shared_slab.arrow_buffers";
```

v0 required:

- `arrow.ipc.stream.chunk`

v0 optional:

- `arrow.record_batch.buffers` for internal experiments behind feature flag

v1 target:

- `datafusion.aggregate_state.native`
- `shared_slab.arrow_buffers`

## 10. Credit frame

```ts
type CreditFrame = {
  additionalBytes: number;
  additionalBatches: number;
  reason: "initial" | "merge_progress" | "release" | "manual";
};
```

Rules:

1. Producer may not send data frames without byte and batch credit.
2. Schema and trailers do not consume data credit.
3. Data frame bytes are counted by payload byte length plus frame overhead estimate.
4. Credits are monotonic additions, not replacements.
5. Coordinator may set credit to zero by cancelling the stream.

## 11. Release frame

Used for shared slabs and optional transferable-buffer accounting.

```ts
type ReleaseFrame = {
  released: Array<{
    bufferId?: string;
    slabId?: number;
    offset?: number;
    byteLength?: number;
    generation?: number;
  }>;
};
```

## 12. Cancellation frame

```ts
type CancelFrame = {
  reason: "user" | "deadline" | "budget" | "sibling_failed" | "coordinator_shutdown";
  message?: string;
};
```

Rules:

- cancellation is idempotent;
- cancel applies to all streams with the same `queryId` unless scoped narrower;
- workers must stop producing frames after observing cancellation;
- late frames are ignored and counted.

## 13. Error frame

```ts
type ErrorFrame = {
  code: WcrpcStatusCode;
  message: string;
  retryable: boolean;
  details?: Record<string, unknown>;
};
```

## 14. Trailers

```ts
type TrailersFrame = {
  status: WcrpcStatusCode;
  message?: string;
  metrics: WcrpcStreamMetrics;
  error?: ErrorFrame;
};
```

Every stream must end with exactly one trailers frame unless the transport is lost. Missing trailers are reported as `transport_closed`.

## 15. Status codes

```ts
type WcrpcStatusCode =
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
```

## 16. Metrics

```ts
type WcrpcStreamMetrics = {
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
```

## 17. Sequencing

- Frames on one stream must be strictly ordered by `seq`.
- Retransmission is not supported in v0.
- Out-of-order frames are protocol errors.
- Duplicate data frames are protocol errors.
- Duplicate cancellation frames are ignored.
- Duplicate trailers are protocol errors.

## 18. Compatibility

A worker endpoint must declare capabilities:

```ts
type WcrpcCapabilities = {
  protocolVersion: 1;
  transports: Array<"message_port" | "shared_slab">;
  codecs: WcrpcPayloadCodec[];
  exchangeModes: ExchangeModeKind[];
  maxFrameBytes: number;
  maxInFlightBytes: number;
  supportsWasmMerge: boolean;
};
```

Coordinator chooses the intersection of capabilities. If no supported codec exists, the query falls back to documented single-worker execution or fails with a structured unsupported reason.
