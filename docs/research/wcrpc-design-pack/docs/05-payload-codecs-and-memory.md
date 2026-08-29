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
