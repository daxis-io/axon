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
