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
