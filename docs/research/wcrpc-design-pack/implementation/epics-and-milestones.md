# Implementation Epics and Milestones

## Epic 1: Protocol core

Tasks:

- implement frame envelope types;
- implement stream state machine;
- implement loopback test transport;
- implement MessagePort transport;
- implement sequence validation;
- implement trailers and status codes;
- implement error mapping.

Deliverable: WCRPC streams can run in tests without DataFusion.

## Epic 2: Credit/backpressure

Tasks:

- implement credit allocator;
- implement producer blocking;
- implement in-flight byte tracking;
- implement blocked-on-credit metrics;
- test global query budget.

Deliverable: producer cannot exceed coordinator-granted windows.

## Epic 3: Worker-pool integration

Tasks:

- instantiate WCRPC endpoint in coordinator;
- instantiate WCRPC endpoint in child workers;
- route ExecuteShard through WCRPC;
- support Arrow IPC chunk codec;
- emit schema and data frames;
- merge projection/filter outputs.

Deliverable: two-worker browser projection/filter proof.

## Epic 4: Planner contract

Tasks:

- implement WorkerPoolDecision;
- implement eligibility classifier;
- expose or implement candidate-file pruning;
- implement deterministic sharding;
- add structured fallback reasons;
- log sharded query plan.

Deliverable: worker-pool execution only for eligible shapes.

## Epic 5: Aggregate merge

Tasks:

- define MergePlan;
- implement aggregate-state frame;
- implement Wasm merge kernel;
- implement grouped key canonicalization;
- enforce intermediate-state budgets;
- add parity tests.

Deliverable: global and grouped aggregate proof.

## Epic 6: Observability

Tasks:

- define browser metrics extension;
- roll up stream metrics;
- add trace events;
- update UAT output;
- build benchmark report generator.

Deliverable: performance reports separate startup, scan, transfer, merge.

## Epic 7: Payload codec experiments

Tasks:

- prototype record-batch buffer codec;
- measure encode/decode overhead;
- evaluate dictionary handling;
- compare to Arrow IPC chunks;
- decide whether to promote to default.

Deliverable: codec recommendation backed by data.

## Epic 8: Direct exchange and merge worker

Tasks:

- use MessageChannel for direct worker-to-worker data path;
- add merge worker endpoint;
- coordinator remains control plane;
- implement direct cancellation propagation;
- compare coordinator CPU usage.

Deliverable: scan workers can stream directly to merge worker.

## Epic 9: Shared slab fast path

Tasks:

- add capability detection;
- implement slab allocator;
- implement shared_slab buffer refs;
- implement release protocol;
- benchmark shared vs transferable.

Deliverable: optional near-native transport under cross-origin isolation.
