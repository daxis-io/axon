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
