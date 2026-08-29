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
