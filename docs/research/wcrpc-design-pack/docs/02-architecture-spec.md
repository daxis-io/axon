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
