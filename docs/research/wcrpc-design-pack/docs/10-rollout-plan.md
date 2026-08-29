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
