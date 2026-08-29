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
