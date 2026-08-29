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
