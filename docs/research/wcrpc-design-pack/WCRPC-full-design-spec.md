# Executive Brief: WCRPC and Browser-Native Analytical Execution

## Summary

The current Browser DataFusion Worker Pool design moves parallelism above a single Wasm DataFusion instance and into a pool of browser Web Workers. A coordinator worker shards candidate files, sends shard descriptors to child workers, each child executes through a real `BrowserDataFusionSession` and `WasmDataFusionEngine`, and the coordinator merges results into the single Arrow IPC response expected by the existing worker protocol.

WCRPC extends that design by introducing a purpose-built internal exchange protocol for analytical workloads:

```text
WCRPC = gRPC-like stream semantics
      + Arrow/DataFusion-native payloads
      + browser-local transports
      + stage/partition/exchange metadata
      + budgets, credits, cancellation, status, and metrics
```

The goal is not merely to call functions in workers. The goal is to make browser workers act as a columnar analytical execution fabric.

## Why this is needed

A generic worker command protocol is good for one visible worker:

```text
sql(command) -> response envelope with Arrow IPC result
```

But multi-worker analytical execution needs a lower-level exchange layer:

```text
ExecuteShard stream
  Headers
  Schema
  RecordBatch / AggregateStateBatch
  Credit
  Cancel
  Metrics
  Trailers
```

Without this, child workers tend to produce full outputs, post one large blob back to the coordinator, and force coordinator-side buffering. That misses the core performance benefits of streaming, backpressure, early cancellation, and incremental merge.

## What WCRPC changes

| Current worker-message style | WCRPC style |
|---|---|
| Request/response command protocol | Streamed analytical exchange protocol |
| Opaque child result blob | Schema-first columnar frames |
| Merge after child completes | Merge while children stream |
| Hard to bound memory | Credit-based byte/batch windows |
| Generic Arrow IPC response | Pluggable payload codecs |
| Coordinator-only gather | Future direct worker-to-worker exchange |
| Query-level metrics only | Stream, stage, partition, codec, transfer, and merge metrics |

## Strategic positioning

WCRPC is designed from three important pieces of prior art:

1. **DuckDB Quack:** Quack shows that a database-native protocol can outperform a general interchange protocol by avoiding unnecessary conversion. The lesson is that WCRPC should separate stream semantics from payload codecs and should allow DataFusion/Arrow-native batch and aggregate-state codecs, not only Arrow IPC stream chunks.
2. **DataFusion Ballista:** Ballista treats distributed execution as stage DAGs with shuffle/exchange boundaries, scheduler/executor roles, partition IDs, and Arrow-native data movement. The lesson is that WCRPC should be a browser-local exchange protocol, not just an RPC mechanism.
3. **Arrow IPC and Dissociated IPC:** Arrow IPC gives a compatible columnar baseline. Dissociated IPC shows why separating metadata from body buffers matters for shared memory and high-performance transports. The lesson is that WCRPC should begin with transferable buffers and evolve toward shared-slab descriptors.

## North-star architecture

```text
SDK / UI
  |
  | existing public browser worker protocol
  v
Coordinator Worker
  |-- query classification
  |-- sharding and stage planning
  |-- WCRPC stream orchestration
  |-- budgets / cancellation / metrics
  |
  +--> Scan Worker 1: Wasm DataFusion + shard A
  +--> Scan Worker 2: Wasm DataFusion + shard B
  +--> Scan Worker 3: Wasm DataFusion + shard C
  |
  +--> Merge Worker: Wasm merge kernels over Arrow/DataFusion batches
  |
  v
Final Arrow IPC response through existing SDK envelope
```

## First version

The first version should support only:

- projection/filter gather;
- global mergeable aggregates: `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`;
- grouped mergeable aggregates with controlled key types;
- MessagePort/postMessage transport;
- transferable `ArrayBuffer` payloads;
- schema fingerprints;
- byte/batch credits;
- cancellation fanout;
- stream trailers and metrics;
- no public SDK protocol change.

## Future versions

Later versions can add:

- Arrow record-batch buffer codec to reduce full IPC encode/decode overhead;
- DataFusion aggregate-state native codec;
- direct worker-to-worker exchange through transferred `MessagePort`s;
- shared-memory slab transport when cross-origin isolation is available;
- top-k order/limit;
- distinct;
- hash/range exchange;
- limited distributed joins if separately designed and tested.

## Non-goals

WCRPC must not claim arbitrary distributed SQL correctness. It must not replace DataFusion. It must not require `SharedArrayBuffer` for v0. It must not leak worker-pool internals into the public SDK protocol. It must not silently route unsupported distributed shapes to native fallback and call that browser success.


---

# Research Brief: Quack, Ballista, Arrow IPC, and Dissociated IPC

## Research questions

1. What existing systems solve similar analytical exchange problems?
2. What should WCRPC copy, adapt, or explicitly avoid?
3. What performance lessons apply to browser-local Wasm workers?

## DuckDB Quack

DuckDB Quack is a DuckDB-to-DuckDB client/server protocol. The DuckDB docs describe it as an HTTP-based protocol where a DuckDB instance becomes a server and other DuckDB instances connect as clients. It uses `application/duckdb` serialization with DuckDB's internal serialization primitives, avoiding round-tripping through an interchange format. The same docs state that, after the initial connection handshake, a query needs one request-response pair; large results stream back in chunks through follow-up `FETCH` requests that can be parallelized.

### Lessons for WCRPC

Quack's most important lesson is not simply that HTTP can be fast. It is that **engine-native analytical serialization can beat a general-purpose interchange path when both sides understand the same execution format**.

For WCRPC this means:

```text
Do not freeze the internal data plane at Arrow Flight compatibility.
Do define stable stream semantics.
Do allow multiple payload codecs.
Do benchmark Arrow IPC chunks against lower-overhead record-batch buffer codecs.
Do keep the public compatibility boundary as Arrow IPC unless deliberately changed.
```

### WCRPC design impact

WCRPC should separate:

```text
Logical stream protocol:
  query_id, stream_id, headers, status, credits, cancellation, metrics

Payload codec:
  arrow.ipc.stream.chunk
  arrow.record_batch.buffers
  datafusion.aggregate_state.native
  shared_slab.arrow_buffers
```

This mirrors Quack's principle: avoid conversion when both sides are part of the same execution ecosystem.

## DataFusion Ballista

Ballista is a distributed SQL query engine built in Rust using the Apache Arrow memory model. Its architecture docs state that Ballista uses Arrow memory during execution, Arrow IPC for shuffle files and data exchange between executors, and open standards such as protobuf, gRPC, Arrow IPC, and Arrow Flight SQL for scheduler/executor APIs.

Ballista's scheduler breaks physical plans into stages separated by pipeline breakers and repartitioning boundaries. Executors run physical plan fragments and exchange intermediate results through shuffle. Ballista's tuning docs describe exchange between stages as upstream tasks writing local files that downstream tasks read either from disk when co-located or over Arrow Flight when remote. Ballista has sort-based and hash-based shuffle implementations with different tradeoffs around file count, buffering, memory use, and write latency.

### Lessons for WCRPC

Ballista's most important lesson is that worker-to-worker communication in analytical execution is not ordinary RPC. It is an **exchange/shuffle problem**.

For WCRPC, this means every frame should eventually be addressable by:

```text
query_id
stage_id
source_task_id
source_partition_id
output_partition_id
stream_id
sequence_number
schema_id
codec
```

The first browser implementation can implement only gather exchange, but the protocol should reserve the model for hash, range, broadcast, round-robin, and top-k exchanges.

### WCRPC design impact

Copy from Ballista:

- scheduler/executor separation -> coordinator/worker separation;
- stage DAG -> sharded browser execution plan;
- shuffle boundaries -> WCRPC exchange modes;
- partition IDs -> output partition identity in frames;
- Arrow-native payloads -> no row-wise JSON;
- metrics and plan visualization -> stream/stage metrics;
- spill/buffer tradeoffs -> browser credit windows and optional OPFS spill.

Do not copy literally:

- native process model;
- disk-first shuffle as v0;
- gRPC/Arrow Flight as local browser worker transport;
- cluster scheduler complexity for the first browser proof.

## Arrow IPC

Arrow IPC is the compatibility baseline. Arrow's docs describe IPC as a way to transfer record batches using the Arrow in-memory format; Arrow's FAQ notes that IPC avoids translation between on-disk and in-memory representation and can avoid deserialization cost and extra copies. Arrow C++ docs also caution that IPC is predominantly zero-copy but may allocate in some cases, such as compression, endian conversion, or alignment requirements.

### Lessons for WCRPC

Use Arrow IPC chunks in v0 because:

- existing browser DataFusion output already uses Arrow IPC;
- it gives a well-defined schema and record-batch representation;
- it is compatible with downstream SDK expectations;
- it is easier to test against existing parity/oracle infrastructure.

But do not assume Arrow IPC chunks are the final fastest internal payload:

```text
RecordBatch -> IPC encode -> transfer -> IPC decode -> merge
```

may be slower than:

```text
RecordBatch buffers + schema id -> transfer -> reconstruct view -> merge
```

## Arrow Dissociated IPC

Arrow's experimental Dissociated IPC protocol separates IPC metadata from body data. The spec explains that normal IPC requires a continuous byte stream where metadata and body buffers are packed together, and that constructing the IPC record batch message can require allocating a contiguous chunk and copying existing data buffers into it. Dissociated IPC attempts to handle shared memory, remote memory, and high-performance transports by allowing body data to be addressed separately from metadata.

### Lessons for WCRPC

Dissociated IPC validates WCRPC's future direction:

```text
metadata frame:
  schema, field nodes, buffer layout, offsets, lengths

body frame:
  transferable ArrayBuffer or shared slab descriptor
```

For v0, WCRPC can use packed Arrow IPC chunks. For v1+, it should support dissociated body references:

```ts
{ kind: "transferable", bufferIndex, byteOffset, byteLength }
{ kind: "shared_slab", slabId, offset, byteLength, generation }
```

## Synthesis

| Prior art | What to copy | What to avoid |
|---|---|---|
| Quack | Native/engine-aware payloads, minimal round trips, chunked results | Tying WCRPC to DuckDB-specific serialization or client/server assumptions |
| Ballista | Stage DAGs, exchange/shuffle model, partition IDs, Arrow-native intermediate data | Native cluster complexity and disk shuffle as v0 |
| Arrow IPC | Stable columnar compatibility boundary | Treating full IPC stream encoding as always optimal internally |
| Dissociated IPC | Metadata/body separation and shared/remote memory references | Depending on an experimental spec as a required v0 dependency |

## Research conclusion

WCRPC should be defined as:

> a browser-local, Wasm-first, columnar analytical exchange protocol with gRPC-like stream semantics, Quack-inspired payload-codec flexibility, Ballista-inspired stage and partition exchange modeling, Arrow IPC compatibility, and a future path to Dissociated-IPC-style shared-memory body references.


---

# WCRPC Architecture Specification

## 1. Context

The Browser DataFusion Worker Pool design uses a coordinator worker to fan out shard-local queries to child Web Workers. Each child worker owns its own Wasm module and `BrowserDataFusionSession`, then returns results to the coordinator, which merges them into one final Arrow IPC response.

WCRPC is the internal protocol that makes this exchange explicit, streamable, observable, and optimizable.

## 2. Architecture goals

WCRPC must:

1. preserve the existing public browser worker command/response protocol;
2. keep DataFusion responsible for shard-local execution;
3. keep Axon responsible for browser-level sharding, orchestration, merge, budgets, cancellation, and metrics;
4. move analytical data as columnar batches or aggregate state, not rows;
5. allow v0 to run without `SharedArrayBuffer` or cross-origin isolation;
6. allow v1+ to use shared-memory slabs when available;
7. support future exchange topologies beyond gather;
8. provide enough metrics to prove whether parallel execution offsets overhead.

## 3. System components

### 3.1 Public SDK / UI

The SDK continues to send existing commands:

```text
open_delta_table
open_parquet_dataset
sql
inspect
cancel
dispose
```

The SDK sees one logical worker-backed session and one final response envelope. It does not know about WCRPC, shard descriptors, exchange modes, stream IDs, or child worker lifetimes.

### 3.2 Coordinator Worker

The coordinator is the visible worker behind the public protocol. It owns:

- table descriptor cache;
- query classification;
- worker-pool decisioning;
- sharded query plan construction;
- stage and exchange planning;
- worker lifecycle;
- WCRPC stream orchestration;
- budget and credit allocation;
- cancellation fanout;
- merge worker selection;
- final response generation;
- metrics rollup.

### 3.3 BrowserDataFusionWorkerPool

A deep internal module that hides worker creation and WCRPC details.

```ts
class BrowserDataFusionWorkerPool {
  openDeltaTable(name: string, descriptor: BrowserHttpSnapshotDescriptor): Promise<void>;
  openParquetDataset(name: string, descriptor: BrowserHttpParquetDatasetDescriptor): Promise<void>;
  sql(command: BrowserWorkerSqlCommand): Promise<BrowserWorkerResponseEnvelope>;
  cancel(command: BrowserWorkerCancelCommand): Promise<void>;
  dispose(name: string): Promise<void>;
  terminate(): void;
}
```

The worker pool should not expose WCRPC as a public API. WCRPC is an internal transport/exchange contract.

### 3.4 Child Wasm DataFusion Worker

A child worker owns:

- `axon_web_wasm` module instance;
- `SandboxQuerySession`;
- `BrowserDataFusionSession`;
- `WasmDataFusionEngine`;
- shard descriptors;
- WCRPC server endpoint;
- local budgets and cancellation state;
- shard-local DataFusion execution.

Child workers are responsible for executing shard-local SQL or plan fragments and streaming result frames.

### 3.5 Merge Worker

In v0, the coordinator may also be the merge worker. In v1+, merge can be moved into one or more dedicated workers.

Merge worker responsibilities:

- validate schema fingerprints;
- consume `RecordBatchFrame` and `AggregateStateBatchFrame` streams;
- run Wasm merge kernels;
- produce final Arrow IPC-compatible output;
- report merge metrics;
- enforce final output and intermediate-state budgets.

### 3.6 WCRPC Endpoint

Each participant exposes an endpoint that can send/receive frames over a transport:

```text
MessagePortTransport
SharedSlabTransport
LoopbackTestTransport
```

Each endpoint understands:

- stream headers;
- schema frames;
- data frames;
- credit frames;
- cancellation frames;
- error frames;
- trailers.

## 4. Runtime topology

### 4.1 v0 gather topology

```text
SDK
  |
  v
Coordinator Worker
  | WCRPC ExecuteShard stream A
  +-------------------------------> Child Worker A
  | WCRPC ExecuteShard stream B
  +-------------------------------> Child Worker B
  | WCRPC ExecuteShard stream C
  +-------------------------------> Child Worker C
  |
  | receives Arrow frames and merges
  v
Final Arrow IPC response
```

### 4.2 v1 merge-worker topology

```text
Coordinator Worker
  |-- control streams --> Scan Workers
  |-- transfer ports --> Merge Worker

Scan Worker A --> WCRPC data stream --> Merge Worker
Scan Worker B --> WCRPC data stream --> Merge Worker
Scan Worker C --> WCRPC data stream --> Merge Worker

Merge Worker --> final stream --> Coordinator
```

### 4.3 v2 exchange topology

```text
Scan Workers
  |
  | hash/range/top-k exchange
  v
Reducer / Merge Workers
  |
  v
Final Gather
```

## 5. Planning layers

WCRPC relies on three planning artifacts.

### 5.1 WorkerPoolDecision

```ts
type WorkerPoolDecision =
  | { kind: "worker_pool"; plan: ShardedQueryPlan }
  | { kind: "single_worker"; reason: WorkerPoolIneligibleReason }
  | { kind: "unsupported"; reason: UnsupportedBrowserQueryReason };
```

### 5.2 ShardedQueryPlan

```ts
type ShardedQueryPlan = {
  queryId: string;
  originalSql: string;
  stages: StagePlan[];
  finalSchema: ArrowSchemaDescriptor;
  mergePlan?: MergePlan;
  budgets: QueryBudget;
  fallbackPolicy: "single_worker_or_error";
};
```

### 5.3 StagePlan

```ts
type StagePlan = {
  stageId: number;
  kind: "scan" | "partial_aggregate" | "merge" | "top_k" | "final";
  inputs: StageInput[];
  outputs: StageOutput[];
  exchange: ExchangeMode;
  payloadCodec: WcrpcPayloadCodec;
};
```

## 6. Dataflow

1. SDK sends a normal `sql` command.
2. Coordinator validates browser limits and builds a worker-pool decision.
3. For `single_worker`, coordinator runs the current single-worker path.
4. For `worker_pool`, coordinator constructs stages and shard descriptors.
5. Coordinator opens WCRPC streams to children.
6. Children execute shard-local work.
7. Children stream schema and data frames.
8. Coordinator/merge worker validates schema and credits.
9. Merge runs incrementally.
10. Coordinator emits final existing worker response.

## 7. Correctness boundaries

WCRPC must not decide SQL correctness alone. Query correctness is owned by:

- the planner/classifier;
- the sharded query plan;
- the merge plan;
- DataFusion shard-local execution;
- Wasm merge kernels;
- parity/oracle tests.

WCRPC enforces transport-level correctness:

- stream identity;
- schema identity;
- sequence ordering;
- codec compatibility;
- buffer ownership;
- credits;
- cancellation;
- terminal status.

## 8. Failure model

Default policy: **fail closed, cancel siblings, return one terminal response, no partial results**.

A stream fails on:

- schema mismatch;
- unknown schema ID;
- sequence violation;
- malformed frame;
- budget exceeded;
- deadline exceeded;
- child execution error;
- transport closed unexpectedly;
- worker termination;
- merge invariant failure.

Any child stream failure cancels all streams for the same query unless an explicit partial-results mode is separately designed.

## 9. Performance model

WCRPC is considered useful when:

```text
single_worker_scan_execute_ms
>
max(child_scan_execute_ms)
+ worker_startup_ms
+ table_open_ms
+ transfer_ms
+ merge_ms
+ final_emit_ms
```

WCRPC exists to reduce:

- row materialization;
- structured clone overhead;
- all-at-once buffering;
- full IPC encode/decode overhead where avoidable;
- TypeScript hot-loop merging;
- unbounded in-flight memory;
- opaque performance attribution.


---

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


---

# Planning and Execution Model

## 1. Planning goals

Planning must turn a public SQL command into one of three decisions:

```ts
type WorkerPoolDecision =
  | { kind: "worker_pool"; plan: ShardedQueryPlan }
  | { kind: "single_worker"; reason: WorkerPoolIneligibleReason }
  | { kind: "unsupported"; reason: UnsupportedBrowserQueryReason };
```

This is required because WCRPC can only move data efficiently; it cannot prove SQL distributability by itself.

## 2. ShardedQueryPlan

```ts
type ShardedQueryPlan = {
  queryId: string;
  originalSql: string;
  finalSchema: ArrowSchemaDescriptor;
  candidateFiles: CandidateFile[];
  shardDescriptors: ShardDescriptor[];
  stages: StagePlan[];
  mergePlan?: MergePlan;
  budgets: QueryBudget;
  metricsPlan: MetricsPlan;
};
```

The plan must be deterministic and serializable so it can be logged, tested, and replayed.

## 3. Query classification

### 3.1 Supported v0 shapes

- projection;
- filtering;
- boolean logic;
- arithmetic expressions;
- `CASE` expressions;
- global `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`;
- grouped aggregation when group key types and aggregate expressions are supported;
- no global order dependency.

### 3.2 Deferred shapes

- `ORDER BY ... LIMIT ... OFFSET ...` using local top-k plus final k-way merge;
- `DISTINCT` using local distinct plus global dedup with output-cardinality budgets;
- high-cardinality grouped aggregates using hash exchange;
- limited joins only after a separate design.

### 3.3 Unsupported v0 shapes

- joins across independently sharded tables;
- window functions;
- set operations beyond simple `UNION ALL`;
- queries referencing multiple open tables without compatible sharding;
- user-defined functions with non-deterministic or side-effectful semantics;
- SQL requiring global state not expressed in a merge plan.

## 4. Candidate file pruning

The coordinator must not shard an unpruned table blindly. It must begin from the same candidate-file set that the single-worker path would scan after partition pruning and file-stat pruning.

Required artifact:

```ts
type CandidateFileSet = {
  tableName: string;
  snapshotId?: string;
  files: CandidateFile[];
  pruning: {
    partitionPruningApplied: boolean;
    fileStatPruningApplied: boolean;
    pruningPlanId: string;
  };
};
```

Open issue to resolve before implementation: whether candidate-file pruning is exposed by a Rust/DataFusion planning API or reimplemented in TypeScript. Prefer exposing a browser-safe Rust classification/pruning API so eligibility and pruning cannot diverge from DataFusion behavior.

## 5. Sharding policy

v0 shards at descriptor-file level.

Deterministic policy:

```text
if all relevant size metadata is present and trusted:
  sort files by size_bytes descending, tie-break by stable path
  assign each file to current smallest shard, tie-break by worker index
else:
  sort files by stable path
  assign round-robin by worker index
```

Worker count:

```ts
actualWorkerCount = min(maxWorkers, candidateFileCount)
```

then apply thresholds:

```ts
if candidateFileCount < minFilesForParallelism: single_worker
if estimatedInputBytes < minBytesForParallelism: single_worker
if estimatedOutputBytesTooLarge: single_worker or unsupported_worker_pool
```

## 6. Stage model

WCRPC should model execution as stages even if v0 uses only one producer stage and one merge stage.

```ts
type StagePlan = {
  stageId: number;
  name: string;
  kind: "scan" | "partial_aggregate" | "merge" | "top_k" | "final";
  tasks: TaskPlan[];
  exchange: ExchangeMode;
  inputSchema?: ArrowSchemaDescriptor;
  outputSchema: ArrowSchemaDescriptor;
  payloadCodec: WcrpcPayloadCodec;
};
```

## 7. Task model

```ts
type TaskPlan = {
  taskId: number;
  workerId?: string;
  shardId?: number;
  inputPartitionId: number;
  outputPartitionIds: number[];
  tableDescriptor?: ShardDescriptor;
  sql?: string;
  physicalPlanFragment?: Uint8Array;
  budget: StreamBudget;
};
```

v0 can use SQL strings. Future versions may use DataFusion physical plan fragments or Substrait-like plan fragments if appropriate.

## 8. Exchange modes

```ts
type ExchangeMode =
  | { kind: "gather"; target: WorkerId }
  | { kind: "hash"; keyColumns: number[]; targets: WorkerId[] }
  | { kind: "range"; sortKeys: SortKey[]; ranges: RangeBoundary[]; targets: WorkerId[] }
  | { kind: "broadcast"; targets: WorkerId[] }
  | { kind: "round_robin"; targets: WorkerId[] }
  | { kind: "top_k"; sortKeys: SortKey[]; limit: number; offset: number; target: WorkerId };
```

v0 implements only `gather`.

## 9. MergePlan

```ts
type MergePlan = {
  mergePlanId: string;
  kind: "append" | "global_aggregate" | "grouped_aggregate" | "top_k" | "distinct";
  inputSchema: ArrowSchemaDescriptor;
  outputSchema: ArrowSchemaDescriptor;
  groupKeys?: GroupKeySpec[];
  aggregateStates?: AggregateStateDescriptor[];
  orderBy?: SortKey[];
  limit?: number;
  offset?: number;
  finalCasts: FinalCastSpec[];
};
```

MergePlan is required for anything other than pure append.

## 10. Planner invariants

1. A worker-pool decision must include an explicit reason.
2. Every worker-pool query must have a deterministic sharding record.
3. Every stage must specify output schema and payload codec.
4. Every aggregate query must specify a merge plan.
5. Every unsupported shape must route to single-worker or fail with a structured reason; never to hidden native fallback.
6. Every final response must include metrics that prove the executed path.


---

# Payload Codecs and Memory Strategy

## 1. Why payload codecs matter

WCRPC separates stream semantics from data representation. This is a Quack-inspired decision: the fastest internal representation may not be the most general interchange format. The public result boundary can remain Arrow IPC while internal exchange uses lower-overhead DataFusion/Arrow-native representations.

## 2. Codec list

```ts
type WcrpcPayloadCodec =
  | "arrow.ipc.stream.chunk"
  | "arrow.record_batch.buffers"
  | "datafusion.aggregate_state.native"
  | "shared_slab.arrow_buffers";
```

## 3. Codec: `arrow.ipc.stream.chunk`

### Description

A payload is an Arrow IPC stream chunk in a transferable `ArrayBuffer`.

### Advantages

- easiest v0 implementation;
- compatible with existing Arrow IPC result path;
- simple schema handling;
- easy parity testing;
- can be transferred between workers without structured-cloning rows.

### Costs

- may require encoding a `RecordBatch` into IPC bytes;
- may require decoding in coordinator/merge worker;
- may pack body buffers into contiguous chunks;
- may perform extra copies when created from Wasm linear memory.

### Use

Required for v0.

## 4. Codec: `arrow.record_batch.buffers`

### Description

A payload carries Arrow record-batch metadata plus separate buffer references. This avoids full IPC stream packing when both sides can reconstruct Arrow arrays from schema and buffer layout.

### Advantages

- lower internal encode/decode overhead;
- closer to Arrow's in-memory model;
- compatible with transferable buffers;
- easier future migration to shared slabs.

### Costs

- more complex schema/buffer validation;
- dictionary handling must be explicit;
- requires runtime support for constructing Arrow views from buffers;
- requires strict lifetime and release rules.

### Use

Experimental v0/v1 behind feature flag.

## 5. Codec: `datafusion.aggregate_state.native`

### Description

A payload represents partial aggregate state in a DataFusion/Wasm-native format that can be merged by a Wasm merge kernel.

### Advantages

- avoids interpreting aggregate partials as JS rows;
- allows engine-aware null, decimal, timestamp, and type semantics;
- aligns with Quack's engine-native payload lesson;
- reduces coordinator TypeScript hot-path work.

### Costs

- requires a stable aggregate-state contract;
- may need versioning by DataFusion build;
- less interoperable than Arrow IPC;
- requires robust parity tests.

### Use

Target for v1 aggregate optimization.

## 6. Codec: `shared_slab.arrow_buffers`

### Description

Payload data lives in a `SharedArrayBuffer` slab. WCRPC frames carry descriptors only:

```ts
{ slabId, offset, byteLength, generation }
```

### Advantages

- closest to zero-copy worker exchange;
- no ownership transfer/detach needed;
- supports direct producer/consumer exchange;
- enables near-native exchange topologies.

### Costs

- requires secure context and cross-origin isolation;
- requires slab allocator and release protocol;
- requires atomics or credit/ownership discipline;
- harder debugging and memory safety.

### Use

Future fast path only.

## 7. Wasm linear memory reality

A browser worker may not be able to transfer a slice of Wasm linear memory without transferring/detaching the whole memory. Therefore v0 should assume one copy may be needed from Wasm memory into a JS-owned transferable `ArrayBuffer`.

Performance goal is not magical zero-copy from scan to final response on day one. The goal is to avoid worse paths:

```text
bad:
  Arrow -> JS objects -> structured clone -> JS objects -> Arrow

v0 good:
  Arrow IPC bytes -> transferable buffer -> Arrow IPC bytes

v1 better:
  Arrow buffers -> transferable buffer refs -> Arrow batch view

v2 best:
  Arrow buffers in shared slab -> descriptor-only exchange
```

## 8. Buffer ownership rules

### Transferable mode

1. Sender owns buffer before `postMessage`.
2. Sender includes buffer in transfer list.
3. Sender must not read buffer after transfer.
4. Receiver owns buffer after delivery.
5. Receiver releases buffer after merge/forward.

### Shared slab mode

1. Producer reserves slab region using allocator.
2. Producer writes data and publishes descriptor.
3. Consumer reads after descriptor arrival.
4. Consumer sends release frame.
5. Allocator reclaims region only after release.
6. Generation number prevents stale descriptor reuse.

## 9. Codec negotiation

Worker capabilities:

```ts
type WcrpcCapabilities = {
  codecs: WcrpcPayloadCodec[];
  transports: Array<"message_port" | "shared_slab">;
  maxFrameBytes: number;
  maxInFlightBytes: number;
};
```

Coordinator chooses codec by:

```text
1. correctness support
2. endpoint capability intersection
3. feature flags
4. cost heuristic
5. fallback to arrow.ipc.stream.chunk
```

## 10. Recommended v0 defaults

```ts
const WCRPC_V0_DEFAULTS = {
  transport: "message_port",
  payloadCodec: "arrow.ipc.stream.chunk",
  batchTargetRows: 8192,
  initialCreditBytes: 32 * 1024 * 1024,
  initialCreditBatches: 8,
  dictionaryPolicy: "reject",
  schemaPolicy: "exact",
};
```


---

# Budgets, Backpressure, Cancellation, and Failure Semantics

## 1. Goals

WCRPC must make worker-pool execution safe in browser memory-constrained environments. It must fail closed, avoid tab-killing bursts, and avoid returning partial results unless a separate partial-results mode is designed.

## 2. Budget hierarchy

### 2.1 QueryBudget

```ts
type QueryBudget = {
  maxWorkers: number;
  maxInputBytes?: number;
  maxOutputBytes?: number;
  maxOutputRows?: number;
  maxIntermediateBytes?: number;
  maxIntermediateRows?: number;
  maxInFlightBytes?: number;
  deadlineMs?: number;
};
```

### 2.2 StreamBudget

```ts
type StreamBudget = {
  maxScanBytes?: number;
  maxOutputBytes?: number;
  maxOutputRows?: number;
  maxIntermediateBytes?: number;
  maxIntermediateRows?: number;
  maxBatchesInFlight?: number;
  maxBytesInFlight?: number;
};
```

## 3. Budget split

Projection/filter:

- child budget limits local output bytes/rows;
- coordinator budget limits final output bytes/rows;
- truncation is not allowed unless the query is explicitly a preview query.

Grouped aggregates:

- child output rows are intermediate aggregate-state rows;
- child intermediate-state budget failure must fail the query;
- never silently truncate local groups;
- final output budget applies after merge.

## 4. Credit-based backpressure

Every data stream is credit-gated.

```ts
type CreditFrame = {
  additionalBytes: number;
  additionalBatches: number;
  reason: "initial" | "merge_progress" | "release" | "manual";
};
```

Producer state:

```ts
type ProducerCreditState = {
  availableBytes: number;
  availableBatches: number;
  blockedSinceMs?: number;
};
```

Rules:

1. Producer may send a data frame only if bytes and batch credit are available.
2. Data frame decrements both counters.
3. Coordinator grants more credit after merge, release, or forward progress.
4. Schema/trailers do not consume data credit.
5. Metrics track blocked-on-credit time.

## 5. Cancellation

Cancellation is query-scoped by default.

```ts
type CancelFrame = {
  queryId: string;
  reason: "user" | "deadline" | "budget" | "sibling_failed" | "coordinator_shutdown";
  message?: string;
};
```

Rules:

1. Cancellation is idempotent.
2. Coordinator marks query as cancelled before sending child cancels.
3. Coordinator sends cancel to every active stream for the query.
4. Workers stop producing frames as soon as cancellation is observed.
5. Late frames are ignored and counted.
6. One terminal response is emitted to the SDK.
7. No partial results in v0.

## 6. Failure semantics

Default policy:

```text
any child failure -> cancel all siblings -> fail query with one structured error
```

Failure categories:

| Failure | Status | Retry v0? |
|---|---|---|
| Schema mismatch | `schema_mismatch` | No |
| Unsupported codec | `unsupported_codec` | No; replan with supported codec only before execution |
| Budget exceeded | `resource_exhausted` | No |
| Deadline exceeded | `deadline_exceeded` | No |
| User cancel | `cancelled` | No |
| Child worker crashed | `transport_closed` | No |
| WASM init failure | `internal` or `failed_precondition` | Maybe single-worker fallback before execution only |
| DataFusion execution error | `internal` / mapped error | No |
| Sequence violation | `sequence_error` | No |

## 7. Retry policy

v0 should not retry a shard after any sibling has started returning data. Retrying a single shard adds complexity around duplicate frames, budget accounting, and deterministic error behavior.

Allowed v0 retry:

```text
If worker startup fails before any query stream starts,
coordinator may create a replacement worker or fall back to single-worker
if the query has not yet begun distributed execution.
```

## 8. Deadlines and timeouts

Coordinator computes absolute deadline at query start.

```ts
absoluteDeadlineMs = nowMs + command.timeoutMs
```

Every stream receives the deadline. Workers should check deadline:

- before execution;
- between input batches;
- before sending data frames;
- before expensive encode/merge operations if possible.

Deadline exceeded by one child cancels all siblings.

## 9. Memory protection

Hard limits:

- maximum worker count;
- maximum in-flight bytes per stream;
- maximum in-flight bytes per query;
- maximum aggregate intermediate bytes;
- maximum output bytes;
- maximum frame bytes;
- maximum schema size;
- maximum dictionary size.

Soft tuning:

- target batch row count;
- target batch bytes;
- credit window size;
- merge coalescing threshold;
- worker warm pool size.

## 10. Browser memory kill prevention

Do not allow all child workers to emit full outputs at once. Required v0 invariant:

```text
sum(in_flight_payload_bytes across streams) <= queryBudget.maxInFlightBytes
```

The coordinator owns global credit allocation and can reduce credits for slower merge paths.

## 11. Terminal response policy

The SDK receives exactly one terminal response:

- success with final Arrow IPC payload and metrics;
- cancellation;
- structured error;
- unsupported worker-pool reason and single-worker fallback metrics, if applicable.

The SDK must never receive partial distributed results in v0.


---

# Merge Semantics Specification

## 1. Core principle

WCRPC moves data, but the merge plan defines SQL semantics. Every distributed query must have either:

```text
append-only semantics
```

or an explicit:

```text
MergePlan
```

If no merge plan can prove equivalence with single-worker DataFusion semantics, worker-pool execution is rejected.

## 2. Projection/filter append

Supported when query has no global ordering, aggregation, distinct, or grouping.

Rules:

1. All child schemas must match final schema exactly.
2. Output order is unspecified unless SQL requests an order.
3. Final output is a logical Arrow IPC stream of all child batches.
4. Do not byte-concatenate full IPC streams blindly. Decode/validate stream schema and emit a single valid final stream.
5. Dictionary arrays require explicit normalize/unify/reject policy.

## 3. Global aggregates

Supported v0 aggregate functions:

- `COUNT(*)`
- `COUNT(expr)`
- `SUM(expr)`
- `MIN(expr)`
- `MAX(expr)`
- `AVG(expr)` as sum + count

### 3.1 Truth table

| Function | Partial state | Merge | Empty/all-null behavior |
|---|---|---|---|
| `COUNT(*)` | row count | sum counts | zero on empty input |
| `COUNT(expr)` | non-null count | sum counts | zero on empty/all-null input |
| `SUM(expr)` | sum + non-null count | sum partial sums | null if total non-null count is zero |
| `MIN(expr)` | min + non-null count | min non-null partials | null if total non-null count is zero |
| `MAX(expr)` | max + non-null count | max non-null partials | null if total non-null count is zero |
| `AVG(expr)` | sum + non-null count | sum sums / sum counts | null if total non-null count is zero |

### 3.2 Unsupported v0 aggregate variants

Reject from worker pool unless separately designed:

- `COUNT(DISTINCT ...)`;
- approximate aggregates;
- non-deterministic aggregates;
- user-defined aggregates;
- decimal overflow behavior not proven equivalent;
- aggregate filter clauses unless planner rewrites them safely;
- aggregates over unsupported complex types.

## 4. Grouped aggregates

Each worker returns one row per group key with partial aggregate state.

Rules:

1. Missing group on a shard means zero rows for that group, not a group with null values.
2. Null group keys follow SQL grouping semantics: nulls group together.
3. Group keys must use canonical key encoding.
4. Aggregate-state schema must match merge plan exactly.
5. Intermediate group count budget failure fails the query, never truncates.

## 5. Canonical group-key encoding

v0 supported key types should be limited to tested primitive types.

Required behavior:

| Type | Requirement |
|---|---|
| Boolean | canonical byte representation |
| Signed/unsigned integer | width-aware representation |
| UTF-8 string | dictionary/plain encodings compare equal after normalization or dictionary rejected |
| Null | encoded explicitly and groups together |
| Date/time/timestamp | unit/timezone metadata included |
| Decimal | precision/scale included |
| Float | define/reject `NaN`, `-0.0`, `0.0` semantics |
| Binary | byte-exact, not UTF-8 converted |
| List/struct/map | reject in v0 unless explicitly supported |

Preferred v0 policy:

```text
Support booleans, integers, UTF-8 strings, dates, timestamps with exact metadata, and nulls.
Reject floating group keys, complex keys, and dictionary-encoded group keys unless normalized.
```

## 6. Final schema preservation

The final output schema is produced by the merge plan, not inferred opportunistically from child output.

```ts
type FinalCastSpec = {
  inputColumn: string;
  outputColumn: string;
  outputType: ArrowDataTypeDescriptor;
  nullable: boolean;
};
```

If final casts cannot reproduce DataFusion output exactly, reject worker-pool execution.

## 7. Order and limit

Deferred until top-k module is implemented.

Required future plan:

1. Each child runs local `ORDER BY` with `LIMIT global_limit + offset`.
2. Children emit sorted streams.
3. Merge worker performs k-way merge.
4. Apply offset and limit globally.
5. Preserve null sort semantics and collation semantics.
6. Validate parity against native oracle across skewed shards.

## 8. Distinct

Deferred until output cardinality budgets are enforced.

Potential plan:

```text
worker local distinct
  -> hash/range exchange by distinct columns
  -> reducer global distinct
  -> final gather
```

Do not implement as naive gather-and-dedup for large outputs without intermediate budgets.

## 9. Error rules

Fail closed on:

- schema mismatch;
- unsupported aggregate;
- unsupported group key type;
- ambiguous decimal/float semantics;
- aggregate intermediate budget exceeded;
- dictionary policy violation;
- final schema mismatch;
- merge kernel error.

## 10. Parity requirement

Every supported merge shape must have parity tests against:

- single-worker browser path where possible;
- native DataFusion oracle;
- host UAT corpus;
- randomized file partitioning property tests.


---

# Observability and Benchmark Plan

## 1. Observability goals

Metrics must prove:

1. the query executed in BrowserWasm;
2. worker pool was attempted or skipped with reason;
3. number of workers and shards;
4. child execution time;
5. transfer time;
6. merge time;
7. startup/open overhead;
8. output/intermediate sizes;
9. whether fallback happened;
10. whether worker-pool speedup offsets overhead.

## 2. Metrics envelope

```ts
type BrowserDataFusionMetricsExtension = {
  execution_target: "BrowserWasm";
  worker_pool: {
    attempted: boolean;
    enabled: boolean;
    worker_count: number;
    candidate_file_count: number;
    shard_file_counts: number[];
    decision_reason: string;
    fallback_reason: string | null;
    bytes_fetched: number;
    rows_emitted: number;
    child_rows_emitted: number[];
    coordinator_merge_duration_ms: number;
    worker_query_duration_ms: {
      min: number;
      max: number;
      total: number;
    };
    wcrpc?: WcrpcMetricsSummary;
  };
};
```

## 3. WCRPC metrics

```ts
type WcrpcMetricsSummary = {
  protocol_version: number;
  transport: "message_port" | "shared_slab";
  payload_codec: WcrpcPayloadCodec;
  streams: WcrpcStreamMetrics[];
  totals: {
    frames: number;
    batches: number;
    rows: number;
    payload_bytes: number;
    transferred_bytes: number;
    shared_bytes?: number;
    max_in_flight_bytes: number;
    blocked_on_credit_ms: number;
    schema_validation_ms: number;
    encode_ms: number;
    decode_ms: number;
    transfer_ms: number;
    merge_ms: number;
  };
};
```

## 4. Performance equation

Worker pool wins when:

```text
single_worker_total_ms
>
worker_startup_ms
+ table_open_ms
+ max(child_execute_ms)
+ transfer_ms
+ merge_ms
+ final_emit_ms
```

Benchmark reports must break out each term.

## 5. Benchmark dimensions

### 5.1 Query shapes

| Shape | Examples |
|---|---|
| Projection/filter | `SELECT a,b FROM t WHERE x > 10` |
| Narrow aggregate | `SELECT COUNT(*), SUM(x) FROM t` |
| Grouped low-cardinality | `SELECT k, COUNT(*) FROM t GROUP BY k` |
| Grouped high-cardinality | `SELECT id, SUM(x) FROM t GROUP BY id` |
| Local top-k future | `SELECT * FROM t ORDER BY ts DESC LIMIT 100` |
| Unsupported fallback | join/window query |

### 5.2 Data dimensions

- file count: 1, 2, 4, 8, 16, 64;
- file size skew: uniform, one huge file, Zipf-like;
- input bytes: small, medium, large;
- selectivity: 1%, 10%, 50%, 100%;
- result size: tiny, moderate, large;
- group cardinality: 10, 1k, 100k, 1M;
- cold workers vs warm workers;
- cached browser HTTP vs uncached;
- desktop Chrome, Firefox, Safari where supported.

## 6. Codec benchmarks

Compare:

```text
A. arrow.ipc.stream.chunk
B. arrow.record_batch.buffers
C. datafusion.aggregate_state.native
D. shared_slab.arrow_buffers, future only
```

Metrics:

- rows/sec;
- bytes/sec;
- time to first batch;
- time to last batch;
- encode ms;
- decode ms;
- transfer ms;
- merge ms;
- peak memory;
- copies per batch estimated;
- credit blocked time.

## 7. External reference baselines

Use external systems only as directional baselines, not pass/fail gates:

- single-worker BrowserWasm DataFusion;
- native host DataFusion oracle;
- DuckDB-Wasm where useful;
- DuckDB Quack bulk-transfer numbers as conceptual reference;
- Arrow Flight/Flight SQL as remote protocol reference, not local worker target.

## 8. Acceptance gates

### Slice 1 gate

- `worker_count >= 2` for eligible multi-file query;
- `execution_target = BrowserWasm`;
- `fallback_reason = None`;
- parity with single-worker/native oracle;
- metrics include startup, child execution, transfer, merge.

### Slice 2 gate

- cancellation stops all child workers;
- budget tests fail closed;
- credit windows bound in-flight bytes;
- schema mismatch fails closed;
- no partial results.

### Slice 3 gate

- top-k ordered results match native oracle across skewed shards;
- offset is global, not per shard;
- k-way merge metrics reported.

### Performance gate

For eligible scan-heavy workloads above thresholds:

```text
median warm worker-pool runtime <= 0.75 * median single-worker runtime
```

For small workloads:

```text
cost heuristic should choose single-worker at least 95% of the time
```

## 9. Trace events

Recommended trace event names:

```text
wcrpc.query.classify.start/end
wcrpc.plan.build.start/end
wcrpc.worker.open.start/end
wcrpc.stream.headers.sent
wcrpc.stream.schema.received
wcrpc.stream.batch.received
wcrpc.stream.credit.sent
wcrpc.stream.cancel.sent
wcrpc.stream.trailers.received
wcrpc.merge.start/end
wcrpc.final_ipc.emit.start/end
```

## 10. Report format

Every benchmark report should include:

- hardware/browser/environment;
- worker count;
- input file count and bytes;
- query shape;
- codec;
- cold/warm status;
- total runtime;
- component breakdown;
- peak memory estimate;
- correctness result;
- fallback status.


---

# Security, Compatibility, and Deployment

## 1. Security model

WCRPC runs inside the browser process, between workers controlled by the same application. It is not a public network service. Still, it must treat all frame inputs as untrusted because malformed frames can cause memory pressure, incorrect results, or crashes.

## 2. Public API compatibility

The public SDK protocol remains unchanged in v0:

```text
SDK -> visible coordinator worker -> existing BrowserWorkerResponseEnvelope
```

WCRPC is internal. Do not expose stream IDs, shard descriptors, codecs, or exchange topology in public responses except through structured metrics extensions.

## 3. Browser deployment modes

### 3.1 Portable mode

Requirements:

- Web Workers;
- `postMessage` transfer list;
- transferable `ArrayBuffer`;
- no cross-origin isolation requirement;
- no `SharedArrayBuffer` requirement.

This is the v0 target.

### 3.2 Shared-memory fast path

Requirements:

- secure context;
- cross-origin isolation;
- `SharedArrayBuffer` availability;
- optional shared `WebAssembly.Memory` support;
- strict COOP/COEP deployment posture.

This is future only and must be feature-detected at runtime.

## 4. Capability negotiation

Each worker reports:

```ts
type WcrpcCapabilities = {
  protocolVersion: 1;
  transports: Array<"message_port" | "shared_slab">;
  codecs: WcrpcPayloadCodec[];
  maxFrameBytes: number;
  maxInFlightBytes: number;
  supportsCancel: boolean;
  supportsCredit: boolean;
  supportsWasmMerge: boolean;
};
```

Coordinator chooses compatible mode or rejects worker-pool execution with a structured reason.

## 5. Frame validation

Reject frames with:

- unknown protocol version;
- unknown query/stream/stage;
- invalid sequence number;
- schema frame after data frames;
- data frame before schema;
- unsupported codec;
- payload bytes larger than max frame;
- buffer refs outside bounds;
- shared slab generation mismatch;
- dictionary policy violation;
- missing trailers.

## 6. Resource abuse protection

WCRPC must guard against accidental or malicious memory amplification:

- hard cap schema size;
- hard cap dictionary size;
- hard cap frame count;
- hard cap in-flight bytes;
- hard cap worker count;
- hard cap intermediate aggregate state;
- deadline on every query;
- cancellation on sibling failure.

## 7. Fallback policy

Worker-pool ineligibility must be explicit.

Allowed outcomes:

```text
worker_pool success:
  execution_target = BrowserWasm
  worker_pool.enabled = true
  fallback_reason = None

single_worker fallback:
  execution_target = BrowserWasm
  worker_pool.enabled = false
  fallback_reason = structured reason

unsupported browser query:
  structured unsupported error
```

Not allowed:

```text
native fallback hidden under browser worker-pool success
```

## 8. Versioning

WCRPC versioning has three layers:

1. protocol version;
2. payload codec version;
3. merge plan version.

Example:

```ts
{
  protocolVersion: 1,
  payloadCodec: "arrow.record_batch.buffers",
  payloadCodecVersion: 1,
  mergePlanVersion: 1
}
```

Version mismatch should fail before execution.

## 9. Compatibility tests

- old coordinator with new worker;
- new coordinator with old worker;
- unsupported codec negotiation;
- missing credit support;
- missing cancellation support;
- schema fingerprint mismatch;
- duplicate/late frames;
- transport close before trailers.

## 10. Operational guidance

Default production posture:

```text
WCRPC disabled unless worker-pool flag enabled.
Use portable transfer mode.
Bound workers to small fixed count.
Use cost heuristic before parallelizing.
Emit fallback reasons and metrics.
Do not enable shared-slab path until deployment supports cross-origin isolation.
```


---

# Rollout Plan

## Phase 0: Research and design hardening

Deliverables:

- WCRPC spec review;
- ADR approval;
- Quack/Ballista/Arrow research memo;
- planner and merge contract finalized;
- benchmark harness design;
- feature flags defined.

Exit gate:

- architecture review approves v0 scope;
- no unresolved correctness blockers for projection/filter and basic aggregates.

## Phase 1: WCRPC transport skeleton

Scope:

- MessagePort/postMessage transport;
- frame envelope;
- stream IDs and sequence numbers;
- schema, data, credit, cancel, trailers frames;
- TypeScript tests with loopback transport;
- no real DataFusion execution yet.

Exit gate:

- stream lifecycle unit tests pass;
- backpressure tests prove producer blocks on zero credits;
- cancellation tests prove late frames ignored.

## Phase 2: Worker-pool integration with Arrow IPC chunk codec

Scope:

- coordinator opens WCRPC streams to child workers;
- children execute shard-local projection/filter;
- children send Arrow IPC chunk frames;
- coordinator emits final Arrow IPC response;
- exact schema fingerprint validation.

Exit gate:

- Playwright proof uses at least two workers;
- `execution_target = BrowserWasm`;
- `fallback_reason = None`;
- parity with single-worker path.

## Phase 3: Aggregate-state frames and Wasm merge kernel

Scope:

- global aggregates;
- grouped aggregates with safe key types;
- aggregate-state frame schema;
- merge plan contract;
- Wasm merge kernel;
- intermediate-state budgets.

Exit gate:

- aggregate truth-table tests pass;
- grouped aggregate parity with native oracle;
- unsupported aggregate shapes fail closed.

## Phase 4: Budgets, cancellation, and metrics hardening

Scope:

- global credit allocator;
- child and coordinator budget enforcement;
- timeout/deadline propagation;
- stream trailers rollup;
- structured metrics extension;
- benchmark harness.

Exit gate:

- budget tests fail closed;
- cancellation stops all children;
- performance report separates startup, child execution, transfer, merge.

## Phase 5: Payload codec experiments

Scope:

- `arrow.record_batch.buffers` experimental codec;
- benchmark against Arrow IPC chunks;
- dictionary/metadata policy;
- Wasm-side reconstruction experiments;
- copy-count instrumentation.

Exit gate:

- data proves whether record-batch buffer codec beats IPC chunks for target workloads;
- no correctness regressions.

## Phase 6: Direct worker-to-worker exchange

Scope:

- coordinator creates `MessageChannel`s;
- scan workers send directly to merge worker;
- coordinator remains control plane;
- data plane bypasses coordinator where safe.

Exit gate:

- transfer/merge overlap improves;
- coordinator hot-path CPU decreases;
- cancellation and metrics still correct.

## Phase 7: Top-k order/limit

Scope:

- local top-k planning;
- k-way merge worker;
- global offset/limit;
- sort null semantics;
- skewed shard tests.

Exit gate:

- ordered results match native oracle;
- offset applied globally;
- memory bounded by k and batch windows.

## Phase 8: Shared slab fast path

Scope:

- feature detection;
- cross-origin isolation checks;
- `SharedArrayBuffer` slab allocator;
- descriptor-only frames;
- release protocol;
- shared path metrics.

Exit gate:

- available only under safe deployment mode;
- fallback to transferable mode when unavailable;
- clear performance win on target workloads.

## Phase 9: Expanded exchange semantics

Scope:

- hash exchange for high-cardinality group-by/distinct;
- range exchange for ordered workloads;
- optional OPFS spill research;
- distributed join design only if separately approved.

Exit gate:

- stage DAG model proven;
- memory and correctness gates pass;
- public API remains stable.


---

# Test Plan

## 1. Unit tests

### 1.1 Protocol lifecycle

- headers before data;
- schema before data;
- trailers exactly once;
- sequence numbers strictly increasing;
- duplicate data frame rejected;
- duplicate cancel ignored;
- data after trailers rejected;
- data after cancel ignored and counted.

### 1.2 Credit/backpressure

- producer cannot send without byte credit;
- producer cannot send without batch credit;
- credit grants are additive;
- blocked time metric increments;
- global in-flight bytes never exceed query budget.

### 1.3 Codec negotiation

- common codec selected;
- unsupported codec rejects before execution;
- fallback to IPC chunk when experimental codec disabled;
- shared slab only enabled under capability and deployment checks.

### 1.4 Schema validation

- exact schema match passes;
- field type mismatch fails;
- nullability mismatch fails;
- metadata mismatch fails under exact policy;
- dictionary policy reject/unify behavior tested.

## 2. Planner tests

- eligible projection/filter accepted;
- eligible aggregate accepted;
- grouped aggregate accepted only for safe key types;
- joins rejected from worker pool;
- windows rejected;
- distinct rejected in v0;
- order/limit rejected until top-k module enabled;
- fallback reasons deterministic;
- sharding deterministic across runs.

## 3. Merge tests

### 3.1 Aggregates

- `COUNT(*)` empty input;
- `COUNT(expr)` all-null input;
- `SUM` all-null returns null;
- `MIN`/`MAX` all-null returns null;
- `AVG` count zero returns null;
- decimal cases rejected or exactly matched;
- empty shard neutral behavior;
- missing group behavior.

### 3.2 Group keys

- null group keys;
- string keys;
- integer keys;
- timestamp metadata;
- decimal precision/scale;
- dictionary keys normalized or rejected;
- float keys rejected or fully specified;
- complex keys rejected.

## 4. Browser tests

Use Playwright.

Required tests:

- start coordinator worker;
- open table with at least two files;
- run eligible projection/filter;
- assert more than one child worker executed;
- assert `BrowserWasm` execution target;
- assert `fallback_reason = None`;
- assert result parity;
- run grouped aggregate;
- assert parity with host oracle;
- run unsupported query;
- assert documented single-worker fallback or structured unsupported reason;
- cancel during scan;
- cancel during merge;
- timeout one child and assert siblings cancelled.

## 5. Wasm tests

- keep existing single-worker Wasm UAT proof;
- add worker-pool contract test only when bridge is stable;
- test Wasm merge kernel directly with Arrow batches;
- test aggregate-state codec round trips.

## 6. Property tests

For supported query shapes:

```text
same table + same SQL + different deterministic shard assignments
=> same final result set, modulo unspecified order
```

Properties:

- random file partitioning;
- random null distribution;
- random group-key distribution;
- random empty shards;
- random batch sizes;
- random cancellation timing for no-partial-results invariant.

## 7. Fuzz tests

Fuzz WCRPC frames:

- malformed envelopes;
- invalid buffer refs;
- unknown schema IDs;
- sequence gaps;
- oversized frames;
- corrupted payload lengths;
- trailers before data;
- schema after data.

Expected result: structured protocol error, no crash, no memory leak.

## 8. Performance tests

Benchmark matrix:

- cold vs warm workers;
- 1/2/4/8 workers;
- 2/4/8/16/64 files;
- uniform/skewed file sizes;
- projection/filter output sizes;
- global aggregates;
- grouped low/high cardinality;
- Arrow IPC vs experimental record-batch buffer codec.

## 9. UAT integration

Add a separate UAT row:

```text
actual browser worker-pool WCRPC query slice
```

Keep it separate from:

- host-engine UAT query corpus;
- actual wasm32 single-worker query slice;
- native oracle corpora;
- browser/native parity;
- performance smoke.

## 10. Definition of done

A feature slice is done only when:

- correctness tests pass;
- browser proof passes;
- no hidden native fallback;
- metrics prove worker-pool behavior;
- budgets fail closed;
- cancellation is deterministic;
- performance report explains overhead and benefit.


---

# Risk Register

| Risk | Impact | Likelihood | Mitigation | Owner |
|---|---:|---:|---|---|
| Distributed SQL merge correctness bug | High | Medium | Limit v0 shapes, require MergePlan, parity/property tests | Engine |
| Worker startup overhead erases speedup | Medium | High | Opt-in, warm pool, cost heuristic, benchmark gates | Runtime |
| Wasm module duplication causes high memory use | High | Medium | Bound worker count, measure memory, consider shared module strategies later | Runtime |
| Browser memory spike from parallel outputs | High | High | Credit-based backpressure and global in-flight budget | Runtime |
| Schema mismatch discovered late | Medium | Medium | Schema-first streams and fingerprints | Protocol |
| Dictionary arrays merge incorrectly | High | Medium | Reject or normalize explicitly in v0 | Engine |
| Aggregate state truncation corrupts results | High | Medium | Separate final output and intermediate-state budgets; fail closed | Engine |
| Hidden native fallback masks browser failure | High | Medium | Structured fallback reasons, metrics, test assertions | Runtime |
| Transport protocol leaks into public SDK | Medium | Low | Keep WCRPC internal; expose only metrics extension | SDK |
| SharedArrayBuffer deployment unavailable | Medium | High | Portable transferable mode v0; shared slab only optional | Platform |
| Record-batch buffer codec diverges from Arrow semantics | High | Medium | Keep IPC as baseline; feature flag experiments; extensive schema tests | Protocol |
| Coordinator becomes bottleneck | Medium | Medium | Direct worker-to-worker exchange and merge worker in later phases | Runtime |
| Top-k ordering semantics differ from DataFusion | High | Medium | Defer until focused top-k module and oracle tests | Engine |
| Group-key canonicalization bug | High | Medium | Restrict key types; canonical encoder tests | Engine |
| Cancellation race returns partial data | High | Medium | Query state machine, late-frame rejection, no partial mode | Runtime |
| Browser differences across Chrome/Firefox/Safari | Medium | Medium | Playwright matrix, feature detection, conservative defaults | Platform |
| Quack-inspired native codec overfits internals | Medium | Medium | Version codecs, keep Arrow IPC fallback | Protocol |
| Ballista-inspired stage model adds too much v0 complexity | Medium | Low | Implement only gather v0; reserve fields for future | Architecture |
